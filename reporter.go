package main

import (
	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/reporter"
	"github.com/openzipkin/zipkin-go/reporter/http"
)

type HTTPReporter struct {
	spans    []*model.SpanModel
	reporter reporter.Reporter
}

func NewReporter(url string, opts ...http.ReporterOption) reporter.Reporter {
	return &HTTPReporter{
		reporter: http.NewReporter(url, opts...),
	}
}

func (r *HTTPReporter) Send(span model.SpanModel) {
	r.spans = append(r.spans, &span)
}

func (r *HTTPReporter) Close() error {
	return r.reporter.Close()
}

func (r *HTTPReporter) flush() {
	for i := range r.spans {
		r.reporter.Send(*r.spans[i])
	}
}
