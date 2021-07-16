package main

import (
	"fmt"
	"io"
	"log"
	"net/http"

	jsoniter "github.com/json-iterator/go"
	"github.com/zeebo/xxh3"

	"github.com/greatroar/blobloom"
)

var json = jsoniter.ConfigFastest

func filter(f *blobloom.SyncFilter, file io.Reader) string {
	var data []interface{}
	var ret []interface{}

	json.NewDecoder(file).Decode(&data)
	for _, value := range data {
		data_in := value.([]interface{})
		hashText := xxh3.Hash([]byte(data_in[0].(string)))
		if !f.Has(hashText) {
			f.Add(hashText)
			ret = append(ret, data_in)
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
