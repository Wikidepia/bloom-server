package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	xxhash "github.com/cespare/xxhash/v2"
	jsoniter "github.com/json-iterator/go"

	"github.com/greatroar/blobloom"
)

var json = jsoniter.ConfigFastest

func filter(f *blobloom.SyncFilter, file io.Reader) string {
	var data []interface{}
	var ret []interface{}
	value, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal(err)
	}
	json.Unmarshal([]byte(value), &data)
	for _, value := range data {
		data_in := value.([]interface{})
		text := data_in[0].(string)
		bytesText := []byte(text)
		hashText := xxhash.Sum64(bytesText)
		if !f.Has(hashText) {
			f.Add(hashText)
			ret = append(ret, data_in)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	jsonResult, err := json.Marshal(ret)
	if err != nil {
		log.Fatal(err)
	}
	return string(jsonResult)
}

func main() {
	f := blobloom.NewSyncOptimized(blobloom.Config{
		Capacity: 10_000_000_000, // Expected number of keys.
		FPRate:   0.01,           // One in 10000 false positives is acceptable.
	})
	for i := 1; i < 10_000_000_000; i++ {
		go func() {
			f.Add(xxhash.Sum64([]byte(strconv.Itoa(i))))
			if i%100_000 == 0 {
				println(i)
			}
		}()
	}

	http.HandleFunc("/filter_url/", func(w http.ResponseWriter, r *http.Request) {
		file, _, err := r.FormFile("file")
		if err != nil {
			log.Fatal(err)
		}
		result := filter(f, file)
		fmt.Fprint(w, result)
	})
	println("Server started")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
