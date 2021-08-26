package main

import (
	"bufio"
	"encoding/hex"
	"strings"
	"unsafe"

	"crypto/sha256"

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

func NewSHA256(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
}

func ctx2strings(ctx *fasthttp.RequestCtx) ([]string, error) {
	var lines []string

	header, err := ctx.FormFile("file")
	defer ctx.Request.Reset()
	defer ctx.Response.Reset()

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

func isMember(key string, value string) (bool, error) {
	c, err := pool.Dial()
	if err != nil {
		return false, err
	}
	defer c.Close()

	hashMember, err := redis.Int(c.Do("SISMEMBER", key, value))
	if err != nil {
		return false, err
	}
	return hashMember == 1, nil
}

func deduplicateHandlerFunc(ctx *fasthttp.RequestCtx) {
	var sb strings.Builder
	var ip []byte

	ip = append(ip, ctx.RemoteIP().String()...)
	key := b2s(ctx.FormValue("key"))
	if key != "main" && key != "clipped" && key != "urls" {
		ctx.Error("key is not main, clipped or urls", fasthttp.StatusBadRequest)
		return
	}
	hash := hex.EncodeToString(NewSHA256(ip))
	hashMember, err := isMember("whitelist-deduplicate", hash)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		log.Info().Err(err).Msg("")
		return
	}
	if !hashMember {
		ctx.Error("hash is not in whitelist-deduplicate", fasthttp.StatusBadRequest)
		return
	}

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
	var ip []byte

	ip = append(ip, ctx.RemoteIP().String()...)
	key := b2s(ctx.FormValue("key"))
	if key != "main" && key != "clipped" && key != "urls" {
		ctx.Error("key is not main, clipped or urls", fasthttp.StatusBadRequest)
		return
	}

	hash := hex.EncodeToString(NewSHA256(ip))
	hashMember, err := isMember("whitelist-add", hash)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		log.Info().Err(err).Msg("")
		return
	}
	if !hashMember {
		ctx.Error("hash is not in whitelist-add", fasthttp.StatusBadRequest)
		return
	}

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
	if key != "main" && key != "clipped" && key != "urls" {
		ctx.Error("key is not main, clipped or urls", fasthttp.StatusBadRequest)
		return
	}

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

func whitelistHandlerFunc(ctx *fasthttp.RequestCtx) {
	if ctx.RemoteIP().String() != "135.181.14.59" || ctx.RemoteIP().String() != "178.63.68.247" {
		ctx.Error("unathorized", fasthttp.StatusUnauthorized)
		return
	}
	hash := ctx.FormValue("hash")
	key := ctx.FormValue("key")

	c, err := pool.Dial()
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		log.Info().Err(err).Msg("")
		return
	}
	defer c.Close()

	if string(key) == "add" {
		_, err = c.Do("SADD", "whitelist-add", hash)
	} else if string(key) == "deduplicate" {
		_, err = c.Do("SADD", "whitelist-deduplicate", hash)
	} else {
		ctx.Error("key is not valid", fasthttp.StatusBadRequest)
		return
	}

	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		log.Info().Err(err).Msg("")
		return
	}
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
		case "/whitelist":
			whitelistHandlerFunc(ctx)
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
