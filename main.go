package main

import (
	"bufio"
	"fmt"
	"net/http"
	"strings"

	redisbloom "github.com/RedisBloom/redisbloom-go"
	"github.com/gomodule/redigo/redis"
	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	pool = &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", "localhost:6379", redis.DialPassword(""))
	}}
	client = redisbloom.NewClientFromPool(pool, "")
	json   = jsoniter.ConfigFastest
)

func deduplicateHandler(w http.ResponseWriter, r *http.Request) {
	var sb strings.Builder
	var lines []string

	dedupIndex := make(map[int]struct{})
	file, _, err := r.FormFile("file")
	if err != nil {
		log.Info().Err(err).Msg("Error opening file")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	keyParam := r.Form.Get("key")
	keysParam := strings.Split(keyParam, ",")
	for _, key := range keysParam {
		if key != "main" && key != "clipped" && key != "urls" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		result, err := client.BfExistsMulti(key, lines)
		if err != nil {
			log.Info().Err(err).Msg("Redis Error BF.MEXISTS")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		for index, existNum := range result {
			if existNum == 0 {
				if _, ok := dedupIndex[index]; !ok {
					dedupIndex[index] = struct{}{}
					sb.WriteString(lines[index])
					sb.WriteString("\n")
				}
			}
		}
	}
	fmt.Fprint(w, sb.String())
}

func addHandler(w http.ResponseWriter, r *http.Request) {
	var lines []string

	file, _, err := r.FormFile("file")
	if err != nil {
		log.Info().Err(err).Msg("Error opening file")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	addCount := 0
	keyParam := r.Form.Get("key")
	keysParam := strings.Split(keyParam, ",")
	for _, key := range keysParam {
		if key != "main" && key != "clipped" && key != "urls" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		result, err := client.BfAddMulti(key, lines)
		if err != nil {
			log.Info().Err(err).Msg("Redis Error BF.MADD")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		for _, existNum := range result {
			if existNum == 1 {
				addCount += 1
			}
		}
	}
	fmt.Fprint(w, addCount)
}

func infoHandler(w http.ResponseWriter, r *http.Request) {
	key := r.Form.Get("key")
	if key != "main" && key != "clipped" && key != "urls" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	result, err := client.Info(key)
	if err != nil {
		log.Info().Err(err).Msg("Redis Error BF.INFO")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	resultMarshal, err := json.Marshal(result)
	if err != nil {
		log.Info().Err(err).Msg("Error marshalling result")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(resultMarshal)
}

func makeHandler(fn func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.ParseMultipartForm(1 * 1024 * 1024)
		log.Info().Str("ip_address", r.RemoteAddr).Str("url", r.URL.Path).Msg("")
		fn(w, r)
	}
}

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	http.HandleFunc("/deduplicate/", makeHandler(deduplicateHandler))
	http.HandleFunc("/add/", makeHandler(addHandler))
	http.HandleFunc("/info/", makeHandler(infoHandler))

	log.Info().Msg("Server started")
	if err := http.ListenAndServe(":8000", nil); err != nil {
		log.Info().Err(err).Msg("Startup failed")
	}
}
