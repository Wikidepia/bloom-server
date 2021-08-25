package main

import (
	"bufio"
	"strings"
	"unsafe"

	redisbloom "github.com/RedisBloom/redisbloom-go"
	"github.com/gomodule/redigo/redis"
	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/reuseport"
)

var (
	pool = &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", "localhost:6379", redis.DialPassword(""))
	}}
	client = redisbloom.NewClientFromPool(pool, "")
	json   = jsoniter.ConfigFastest
)

func b2s(b []byte) string {
	/* #nosec G103 */
	return *(*string)(unsafe.Pointer(&b))
}

func ctx2strings(ctx *fasthttp.RequestCtx) ([]string, error) {
	var lines []string

	header, err := ctx.FormFile("file")
	if err != nil {
		return nil, err
	}

	file, err := header.Open()
	defer file.Close()
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return lines, nil
}

func deduplicateHandlerFunc(ctx *fasthttp.RequestCtx) {
	var sb strings.Builder

	key := b2s(ctx.FormValue("key"))
	lines, err := ctx2strings(ctx)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		log.Info().Err(err).Msg("")
		return
	}

	result, err := client.BfExistsMulti(key, lines)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		log.Info().Err(err).Msg("")
		return
	}

	for index, existNum := range result {
		if existNum == 0 {
			sb.WriteString(lines[index] + "\n")
		}
	}

	ctx.SetContentType("text/plain; charset=utf-8")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBodyString(sb.String())
}

func addHandlerFunc(ctx *fasthttp.RequestCtx) {
	key := b2s(ctx.FormValue("key"))
	lines, err := ctx2strings(ctx)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		log.Info().Err(err).Msg("")
		return
	}

	_, err = client.BfAddMulti(key, lines)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		log.Info().Err(err).Msg("")
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
}

func infoHandlerFunc(ctx *fasthttp.RequestCtx) {
	key := b2s(ctx.FormValue("key"))
	result, err := client.Info(key)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		log.Info().Err(err).Msg("")
		return
	}

	resultMarshal, err := json.Marshal(result)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		log.Info().Err(err).Msg("")
		return
	}
	ctx.SetBody(resultMarshal)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	m := func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/deduplicate":
			deduplicateHandlerFunc(ctx)
		case "/add":
			addHandlerFunc(ctx)
		case "/info":
			infoHandlerFunc(ctx)
		default:
			ctx.Error("not found", fasthttp.StatusNotFound)
		}
	}

	ln, err := reuseport.Listen("tcp4", ":8000")
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	server := &fasthttp.Server{
		Handler:                      m,
		MaxRequestBodySize:           32 * 1024 * 1024,
		ReduceMemoryUsage:            true,
		DisablePreParseMultipartForm: true,
	}
	server.Serve(ln)
}
