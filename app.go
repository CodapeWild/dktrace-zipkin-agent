package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	zipkin "github.com/openzipkin/zipkin-go"
	zpkprotov2 "github.com/openzipkin/zipkin-go/proto/v2"
	"github.com/openzipkin/zipkin-go/reporter"
	httpreporter "github.com/openzipkin/zipkin-go/reporter/http"
)

var (
	cfg          *config
	globalCloser chan struct{}
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
	Resource  string        `json:"resource"`
	Operation string        `json:"operation"`
	SpanType  string        `json:"span_type"`
	Duration  time.Duration `json:"duration"`
	Error     string        `json:"error"`
	Tags      []tag         `json:"tags"`
	Children  []*span       `json:"children"`
	dumpSize  int64
}

func (sp *span) startSpanFromContext(tracer *zipkin.Tracer, ctx context.Context) (zipkin.Span, context.Context) {
	zpkspan, ctx := tracer.StartSpanFromContext(ctx, sp.Operation)

	zpkspan.Tag("resource.name", sp.Resource)
	zpkspan.Tag("span.type", sp.SpanType)
	for _, tag := range sp.Tags {
		zpkspan.Tag(tag.Key, fmt.Sprintf("%v", tag.Value))
	}

	if sp.dumpSize != 0 {
		zpkspan.Tag("_dump_data", getRandomHexString(sp.dumpSize))
	}

	if len(sp.Error) != 0 {
		zipkin.TagError.Set(zpkspan, sp.Error)
	}

	total := int64(sp.Duration * time.Millisecond)
	d := rand.Int63n(total)
	time.Sleep(time.Duration(d))
	go func() {
		time.Sleep(time.Duration(total - d))
		zpkspan.Finish()
	}()

	return zpkspan, ctx
}

func main() {
	go startAgent()

	var serializer reporter.SpanSerializer
	if cfg.Encode == "json" {
		serializer = reporter.JSONSerializer{}
	} else {
		serializer = zpkprotov2.SpanSerializer{}
	}

	reporter := NewReporter("http://"+agentAddress+zipv2, httpreporter.Serializer(serializer), httpreporter.BatchSize(1000))
	defer reporter.Close()

	endpoint, err := zipkin.NewEndpoint(cfg.Service, agentAddress)
	if err != nil {
		log.Fatalln(err.Error())
	}

	tracer, err := zipkin.NewTracer(reporter, zipkin.WithLocalEndpoint(endpoint))
	if err != nil {
		log.Fatalln(err.Error())
	}

	spanCount := countSpans(cfg.Trace, 0)
	log.Printf("### span count: %d\n", spanCount)
	log.Printf("### random dump: %v", cfg.RandomDump)
	if cfg.RandomDump {
		if cfg.DumpSize <= 0 {
			cfg.DumpSize = rand.Intn(924) + 100
		}
		log.Printf("### dump size: 0kb~%dkb", cfg.DumpSize)
	} else {
		log.Printf("### dump size: %dkb", cfg.DumpSize)
	}

	if cfg.DumpSize > 0 || cfg.DumpSize > 0 {
		setPerDumpSize(cfg.Trace, int64(cfg.DumpSize/spanCount)<<10, cfg.RandomDump)
	}

	root, children := startRootSpan(tracer, cfg.Trace)
	orchestrator(tracer, zipkin.NewContext(context.Background(), root), children)
	time.Sleep(3 * time.Second)
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
	var d time.Duration
	if len(trace) == 1 {
		root = tracer.StartSpan(trace[0].Operation)
		root.Tag("resource.name", trace[0].Resource)
		root.Tag("span.type", trace[0].SpanType)
		for _, tag := range trace[0].Tags {
			root.Tag(tag.Key, fmt.Sprintf("%v", tag.Value))
		}
		d = trace[0].Duration * time.Millisecond
		children = trace[0].Children
		if len(trace[0].Error) != 0 {
			root.Tag("error", trace[0].Error)
		}
	} else {
		root = tracer.StartSpan("startRootSpan")
		root.Tag("span.type", "web")
		d = time.Duration(60+rand.Intn(300)) * time.Millisecond
		children = trace
	}

	time.Sleep(d / 2)
	go func(root zipkin.Span, d time.Duration) {
		time.Sleep(d / 2)
	}(root, d)

	return
}

func orchestrator(tracer *zipkin.Tracer, ctx context.Context, children []*span) {
	if len(children) == 1 {
		_, ctx = children[0].startSpanFromContext(tracer, ctx)
		if len(children[0].Children) != 0 {
			orchestrator(tracer, ctx, children[0].Children)
		}
	} else {
		for k := range children {
			go func(ctx context.Context, span *span) {
				_, ctx = span.startSpanFromContext(tracer, ctx)
				if len(span.Children) != 0 {
					orchestrator(tracer, ctx, span.Children)
				}
			}(ctx, children[k])
		}
	}
}

func getRandomHexString(n int64) string {
	buf := make([]byte, n)
	rand.Read(buf)

	return hex.EncodeToString(buf)
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
	if cfg.Sender == nil || cfg.Sender.Threads <= 0 || cfg.Sender.SendCount <= 0 {
		log.Fatalln("sender not configured properly")
	}
	if len(cfg.Trace) == 0 {
		log.Fatalln("empty trace")
	}

	globalCloser = make(chan struct{})

	rand.Seed(time.Now().UnixNano())
	agentAddress += strconv.Itoa(30000 + rand.Intn(10000))
}
