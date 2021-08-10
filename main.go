package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"strings"

	"github.com/greatroar/blobloom"
	"github.com/klauspost/compress/gzip"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zeebo/xxh3"
)

var bloomFilter = blobloom.NewSyncOptimized(blobloom.Config{
	Capacity: 10_000_000_000,
	FPRate:   0.05,
})
var newLines = []byte("\n")

func filter(file io.Reader) string {
	var sb strings.Builder
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		hashText := xxh3.Hash(scanner.Bytes())
		if !bloomFilter.Has(hashText) {
			bloomFilter.Add(hashText)
			sb.Write(append(scanner.Bytes(), newLines...))
		}
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
		gunzip, err := gzip.NewReader(file)
		if err != nil {
			log.Info().Err(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, filter(gunzip))
	})
	log.Info().Msg("Server started")
	if err := http.ListenAndServe(":8000", nil); err != nil {
		log.Info().Err(err).Msg("Startup failed")
	}
}
