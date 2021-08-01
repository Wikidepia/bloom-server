package main

import (
	"bufio"
	"io"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/greatroar/blobloom"
	"github.com/klauspost/compress/gzip"
	"github.com/valyala/fasthttp"
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
	ctx.WriteString(filter(gunzip))
	debug.FreeOSMemory()
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
