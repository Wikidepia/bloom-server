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

	r.ParseMultipartForm(1 * 1024 * 1024)
	file, _, err := r.FormFile("file")
	defer file.Close()
	if err != nil {
		log.Info().Err(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// Possible optimization
		lines = append(lines, scanner.Text())
	}

	result, err := client.BfExistsMulti("urls", lines)
	if err != nil {
		log.Info().Err(err)
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
	r.ParseMultipartForm(1 * 1024 * 1024)
	file, _, err := r.FormFile("file")
	defer file.Close()
	if err != nil {
		log.Info().Err(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// Possible optimization
		lines = append(lines, scanner.Text())
	}

	_, err = client.BfAddMulti("urls", lines)
	if err != nil {
		log.Info().Err(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func infoHandler(w http.ResponseWriter, r *http.Request) {
	result, err := client.Info("urls")
	if err != nil {
		log.Info().Err(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	fmt.Println(result["Capacity"])
	resultMarshal, err := json.Marshal(result)
	if err != nil {
		log.Info().Err(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(resultMarshal)
}

func makeHandler(fn func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fn(w, r)
	}
}

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	go func() {
		http.ListenAndServe(":6060", nil)
	}()

	http.HandleFunc("/deduplicate/", deduplicateHandler)
	http.HandleFunc("/add/", addHandler)
	http.HandleFunc("/info/", infoHandler)

	log.Info().Msg("Server started")
	if err := http.ListenAndServe(":8000", nil); err != nil {
		log.Info().Err(err).Msg("Startup failed")
	}
}
