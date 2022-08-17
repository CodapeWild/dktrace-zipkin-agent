package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	zipkin "github.com/openzipkin/zipkin-go"
	pbserializer "github.com/openzipkin/zipkin-go/proto/zipkin_proto3"
	"github.com/openzipkin/zipkin-go/reporter"
	httpreporter "github.com/openzipkin/zipkin-go/reporter/http"
)

var (
	cfg          *config
	globalCloser = make(chan struct{})
	agentAddress = "127.0.0.1:"
	zipv2        = "/api/v2/spans"
)

type sender struct {
	Threads      int `json:"threads"`
	SendCount    int `json:"send_count"`
	SendInterval int `json:"send_interval"`
}

type config struct {
	DkAgent    string  `json:"dk_agent"`
	Sender     *sender `json:"sender"`
	Service    string  `json:"service"`
	Encode     string  `json:"encode"`
	DumpSize   int     `json:"dump_size"`
	RandomDump bool    `json:"random_dump"`
	Trace      []*span `json:"trace"`
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
	dumpSize  int64
}

func main() {
	go startAgent()

	var serializer reporter.SpanSerializer
	if cfg.Encode == "json" {
		serializer = reporter.JSONSerializer{}
	} else {
		serializer = pbserializer.SpanSerializer{}
	}

	reporter := NewReporter("http://"+agentAddress+zipv2, httpreporter.Serializer(serializer), httpreporter.BatchSize(1000))
	defer reporter.Close()

	tracer, err := zipkin.NewTracer(reporter)
	if err != nil {
		log.Fatalln(err.Error())
	}

	spanCount := countSpans(cfg.Trace, 0)
	log.Printf("### span count: %d\n", spanCount)
	log.Printf("### random dump: %v", cfg.RandomDump)
	if cfg.RandomDump {
		log.Printf("### dump size: 0kb~%dkb", cfg.DumpSize)
	} else {
		log.Printf("### dump size: %dkb", cfg.DumpSize)
	}

	var fillup int64
	if cfg.DumpSize > 0 {
		fillup = int64(cfg.DumpSize / spanCount)
		fillup <<= 10
		setPerDumpSize(cfg.Trace, fillup, cfg.RandomDump)
	}

	root, children := startRootSpan(tracer, cfg.Trace)
	orchestrator(tracer, zipkin.NewContext(context.Background(), root), children)
	root.Finish()

	reporter.(*HTTPReporter).flush()

	<-globalCloser
}

func countSpans(trace []*span, c int) int {
	c += len(trace)
	for i := range trace {
		if len(trace[i].Children) != 0 {
			c = countSpans(trace[i].Children, c)
		}
	}

	return c
}

func setPerDumpSize(trace []*span, fillup int64, isRandom bool) {
	for i := range trace {
		if isRandom {
			trace[i].dumpSize = rand.Int63n(fillup)
		} else {
			trace[i].dumpSize = fillup
		}
		if len(trace[i].Children) != 0 {
			setPerDumpSize(trace[i].Children, fillup, isRandom)
		}
	}
}

func startRootSpan(tracer *zipkin.Tracer, trace []*span) (root zipkin.Span, children []*span) {
	var (
		d   int64
		err error
	)
	if len(trace) == 1 {
		root = tracer.StartSpan(trace[0].Operation)
		root.Tag("resource.name", trace[0].Resource)
		root.Tag("span.type", trace[0].SpanType)
		for _, tag := range trace[0].Tags {
			root.Tag(tag.Key, fmt.Sprintf("%v", tag.Value))
		}
		d = trace[0].Duration * int64(time.Millisecond)
		children = trace[0].Children
		if len(trace[0].Error) != 0 {
			err = errors.New(trace[0].Error)
		}
	} else {
		root = tracer.StartSpan("start_root_span")
		d = int64(time.Duration(60+rand.Intn(300)) * time.Millisecond)
		children = trace
	}

	time.Sleep(time.Duration(d) / 2)
	go func(root zipkin.Span, d int64, err error) {
		time.Sleep(time.Duration(d) / 2)
		if err != nil {
			root.Tag("err", err.Error())
		}
	}(root, d, err)

	return
}

func orchestrator(tracer *zipkin.Tracer, ctx context.Context, children []*span) {
	if len(children) == 1 {
		ctx = startSpanFromContext(tracer, ctx, children[0])
		if len(children[0].Children) != 0 {
			orchestrator(tracer, ctx, children[0].Children)
		}
	} else {
		wg := sync.WaitGroup{}
		wg.Add(len(children))
		for k := range children {
			go func(ctx context.Context, span *span) {
				defer wg.Done()

				ctx = startSpanFromContext(tracer, ctx, span)
				if len(span.Children) != 0 {
					orchestrator(tracer, ctx, span.Children)
				}
			}(ctx, children[k])
		}
		wg.Wait()
	}
}

func getRandomHexString(n int64) string {
	buf := make([]byte, n)
	rand.Read(buf)

	return hex.EncodeToString(buf)
}

func startSpanFromContext(tracer *zipkin.Tracer, ctx context.Context, span *span) context.Context {
	zipspan, ctx := tracer.StartSpanFromContext(ctx, span.Operation)

	zipspan.Tag("resource.name", span.Resource)
	zipspan.Tag("span.type", span.SpanType)
	for _, tag := range span.Tags {
		zipspan.Tag(tag.Key, fmt.Sprintf("%v", tag.Value))
	}

	if span.dumpSize != 0 {
		zipspan.Tag("_dump_data", getRandomHexString(span.dumpSize))
	}

	if len(span.Error) != 0 {
		zipspan.Tag("err", span.Error)
	}

	time.Sleep(time.Duration(span.Duration * int64(time.Millisecond)))

	zipspan.Finish()

	return ctx
}

func init() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	data, err := os.ReadFile("./config.json")
	if err != nil {
		log.Fatalln(err.Error())
	}

	cfg = &config{}
	if err = json.Unmarshal(data, cfg); err != nil {
		log.Fatalln(err.Error())
	}

	rand.Seed(time.Now().UnixNano())
	agentAddress += strconv.Itoa(30000 + rand.Intn(10000))
}
