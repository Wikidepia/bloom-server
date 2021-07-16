package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	jsoniter "github.com/json-iterator/go"
	"github.com/zeebo/xxh3"

	"github.com/greatroar/blobloom"
)

var json = jsoniter.ConfigFastest

func filter(f *blobloom.Filter, file io.Reader) string {
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
		hashText := xxh3.Hash([]byte(text))
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
	f := blobloom.NewOptimized(blobloom.Config{
		Capacity: 10_000_000_000,
		FPRate:   0.05,
	})

	http.HandleFunc("/bloom/", func(w http.ResponseWriter, r *http.Request) {
		file, _, err := r.FormFile("file")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprint(w, filter(f, file))
	})
	println("Server started")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
