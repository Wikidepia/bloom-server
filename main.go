package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"strings"

	"github.com/greatroar/blobloom"
	"github.com/klauspost/compress/gzip"
	"github.com/zeebo/xxh3"
)

var bloom_filter = blobloom.NewSyncOptimized(blobloom.Config{
	Capacity: 10_000_000_000,
	FPRate:   0.05,
})

func filter(file io.Reader) string {
	var sb strings.Builder
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		hashText := xxh3.Hash(scanner.Bytes())
		if !bloom_filter.Has(hashText) {
			bloom_filter.Add(hashText)
			sb.WriteString(scanner.Text() + "\n")
		}
	}
	return sb.String()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	go func() {
		http.ListenAndServe(":6060", nil)
	}()
	http.HandleFunc("/bloom/", func(w http.ResponseWriter, r *http.Request) {
		file, _, err := r.FormFile("file")
		if err != nil {
			log.Fatal(err)
		}
		gunzip, err := gzip.NewReader(file)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprint(w, filter(gunzip))
	})
	println("Server started")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
