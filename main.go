package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"unsafe"

	redisbloom "github.com/RedisBloom/redisbloom-go"
	"github.com/gomodule/redigo/redis"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	pool = &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", "localhost:6379", redis.DialPassword(""))
	}}
	client   = redisbloom.NewClientFromPool(pool, "bloom-cah")
	newLines = []byte("\n")
)

// https://github.com/golang/go/issues/25484#issuecomment-391415660
func ByteSlice2String(bs []byte) string {
	return *(*string)(unsafe.Pointer(&bs))
}

func filter(file io.Reader) string {
	var sb strings.Builder
	var lines []string
	var existURLs []string

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, ByteSlice2String(scanner.Bytes()))
	}
	result, err := client.BfExistsMulti("urls", lines)
	if err != nil {
		log.Info().Err(err)
	}

	for i := 0; i < len(result); i++ {
		if result[i] == 0 {
			sb.WriteString(lines[i])
			sb.Write(newLines)
			existURLs = append(existURLs, lines[i])
		}
	}
	_, err = client.BfAddMulti("urls", existURLs)
	if err != nil {
		log.Info().Err(err)
	}
	return sb.String()
}

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	go func() {
		http.ListenAndServe(":6060", nil)
	}()

	http.HandleFunc("/bloom/", func(w http.ResponseWriter, r *http.Request) {
		r.ParseMultipartForm(1 * 1024 * 1024)
		file, _, err := r.FormFile("file")
		if err != nil {
			log.Info().Err(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, filter(file))
	})
	log.Info().Msg("Server started")
	if err := http.ListenAndServe(":8000", nil); err != nil {
		log.Info().Err(err).Msg("Startup failed")
	}
}
