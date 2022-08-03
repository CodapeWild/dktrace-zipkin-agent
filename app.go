package main

import (
	"encoding/json"
	"log"
	"os"
)

type config struct {
	DkAgent   string  `json:"dk_agent"`
	Threads   int     `json:"threads"`
	SendCount int     `json:"send_count"`
	Service   string  `json:"service"`
	Trace     []*span `json:"trace"`
}

type tag struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

type span struct {
	Resource  string  `json:"resource"`
	Operation string  `json:"operation"`
	SpanType  string  `json:"span_type"`
	Duration  int64   `json:"duration"`
	Error     string  `json:"error"`
	Tags      []tag   `json:"tags"`
	Children  []*span `json:"children"`
}

func main() {
	data, err := os.ReadFile("./config.json")
	if err != nil {
		log.Fatalln(err.Error())
	}

	config := &config{}
	if err = json.Unmarshal(data, config); err != nil {
		log.Fatalln(err.Error())
	}

}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(os.Stdout)
}
