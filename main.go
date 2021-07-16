package main

import (
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/greatroar/blobloom"
	jsoniter "github.com/json-iterator/go"
	"github.com/valyala/fasthttp"
	"github.com/zeebo/xxh3"
)

var json = jsoniter.ConfigFastest
var bloom_filter = blobloom.NewSyncOptimized(blobloom.Config{
	Capacity: 10_000_000_000,
	FPRate:   0.05,
})

type JSONData [][]string

func filter(file io.Reader) string {
	var data JSONData
	var ret JSONData

	json.NewDecoder(file).Decode(&data)
	for _, value := range data {
		hashText := xxh3.HashString(value[0])
		if !bloom_filter.Has(hashText) {
			bloom_filter.Add(hashText)
			ret = append(ret, value)
		}
	}
	jsonResult, err := json.MarshalToString(ret)
	if err != nil {
		log.Fatal(err)
	}
	return jsonResult
}

func filterHandler(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("application/json")
	ctx.SetStatusCode(http.StatusOK)
	header, err := ctx.FormFile("file")
	if err != nil {
		log.Fatal(err)
	}
	file, err := header.Open()
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Fprint(ctx, filter(file))
}

func main() {
	handler := fasthttp.CompressHandler(filterHandler)

	server := &fasthttp.Server{
		Handler:            handler,
		MaxRequestBodySize: 32 << 20,
	}
	println("Starting server...")
	if err := server.ListenAndServe(":8000"); err != nil {
		log.Fatalf("Error in ListenAndServe: %s", err)
	}
}
