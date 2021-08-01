package main

import (
	"compress/gzip"
	"io"
	"net/http"
	_ "net/http/pprof"
	"runtime"

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

type FilterJSONData [][]string

func filter(file io.Reader) []byte {
	var data FilterJSONData

	json.NewDecoder(file).Decode(&data)
	ret := make(FilterJSONData, 0, len(data))
	for _, value := range data {
		hashText := xxh3.HashString(value[0])
		if !bloom_filter.Has(hashText) {
			bloom_filter.Add(hashText)
			ret = append(ret, value)
		}
	}
	jsonResult, err := json.Marshal(ret)
	if err != nil {
		return []byte(err.Error())
	}
	return jsonResult
}

func filterHandler(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("application/json")
	ctx.SetStatusCode(http.StatusOK)

	multipartFormBoundary := ctx.Request.Header.MultipartFormBoundary()
	if len(multipartFormBoundary) == 0 || string(ctx.Method()) != "POST" {
		ctx.SetStatusCode(http.StatusBadRequest)
		return
	}

	header, err := ctx.FormFile("file")
	if err != nil {
		ctx.SetStatusCode(http.StatusBadRequest)
		return
	}
	file, err := header.Open()
	defer file.Close()
	if err != nil {
		ctx.SetStatusCode(http.StatusBadRequest)
		return
	}
	gunzip, err := gzip.NewReader(file)
	defer gunzip.Close()
	if err != nil {
		ctx.SetStatusCode(http.StatusBadRequest)
		return
	}
	ctx.Write(filter(gunzip))
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	go func() {
		http.ListenAndServe(":6060", nil)
	}()

	m := func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/bloom":
			filterHandler(ctx)
		case "/bloom/":
			filterHandler(ctx)
		default:
			ctx.Error("Unsupported path", fasthttp.StatusNotFound)
		}
	}
	server := &fasthttp.Server{
		Handler:                      m,
		MaxRequestBodySize:           8 << 20,
		ReduceMemoryUsage:            true,
		DisablePreParseMultipartForm: true,
	}
	println("Starting server...")
	if err := server.ListenAndServe(":8000"); err != nil {
		println(err.Error())
	}
}
