package main

import (
	"bufio"
	"fmt"
	"net/http"
	_ "net/http/pprof"
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

	key := r.Form.Get("key")
	if key != "main" && key != "clipped" && key != "urls" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	file, _, err := r.FormFile("file")
	if err != nil {
		log.Info().Err(err).Msg("Error opening file")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// Possible optimization
		lines = append(lines, scanner.Text())
	}

	result, err := client.BfExistsMulti(key, lines)
	if err != nil {
		log.Info().Err(err).Msg("Redis Error BF.MEXISTS")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	for index, existNum := range result {
		if existNum == 0 {
			sb.WriteString(lines[index] + "\n")
		}
	}
	fmt.Fprint(w, sb.String())
}

func addHandler(w http.ResponseWriter, r *http.Request) {
	var lines []string

	key := r.Form.Get("key")
	if key != "main" && key != "clipped" && key != "urls" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	file, _, err := r.FormFile("file")
	if err != nil {
		log.Info().Err(err).Msg("Error opening file")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// Possible optimization
		lines = append(lines, scanner.Text())
	}

	result, err := client.BfAddMulti(key, lines)
	if err != nil {
		log.Info().Err(err).Msg("Redis Error BF.MADD")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	deduplicated := 0
	for _, existNum := range result {
		if existNum == 0 {
			deduplicated += 1
		}
	}
	w.Write([]byte(fmt.Sprintf("%d", deduplicated)))
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

	go func() {
		http.ListenAndServe(":6060", nil)
	}()

	http.HandleFunc("/deduplicate/", makeHandler(deduplicateHandler))
	http.HandleFunc("/add/", makeHandler(addHandler))
	http.HandleFunc("/info/", makeHandler(infoHandler))

	log.Info().Msg("Server started")
	if err := http.ListenAndServe(":8000", nil); err != nil {
		log.Info().Err(err).Msg("Startup failed")
	}
}
