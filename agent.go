package main

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	zpkmodel "github.com/openzipkin/zipkin-go/model"
	zpkprotov2 "github.com/openzipkin/zipkin-go/proto/v2"
	"github.com/openzipkin/zipkin-go/reporter"
)

func startAgent() {
	log.Printf("### start Zipkin agent %s\n", agentAddress)

	svr := getTimeoutServer(agentAddress, http.HandlerFunc(handleZipkinData))
	if err := svr.ListenAndServe(); err != nil {
		log.Fatalln(err.Error())
	}
}

func getTimeoutServer(address string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Millisecond,
		ReadTimeout:       time.Second,
		WriteTimeout:      3 * time.Second,
		IdleTimeout:       10 * time.Second,
	}
}

func handleZipkinData(resp http.ResponseWriter, req *http.Request) {
	log.Println("### Zipkin original headers:")
	for k, v := range req.Header {
		log.Printf("%s: %v\n", k, v)
	}

	resp.WriteHeader(http.StatusOK)

	if req.Header.Get("Content-Length") == "0" {
		return
	}

	zpktrace, err := parseZipkinTraceV2(req.Header.Get("Content-Encoding"), req.Header.Get("Content-Type"), req.Body)
	if err != nil {
		log.Fatalln(err.Error())
	}

	go sendZipkinTask(cfg.Sender, zpktrace, "http://"+cfg.DkAgent+zipv2, req.Header)
}

func sendZipkinTask(sender *sender, zpktrace []*zpkmodel.SpanModel, endpoint string, headers http.Header) {
	var serializer reporter.SpanSerializer
	if cfg.Encode == "json" {
		serializer = reporter.JSONSerializer{}
	} else {
		serializer = zpkprotov2.SpanSerializer{}
	}

	wg := sync.WaitGroup{}
	wg.Add(sender.Threads)
	for i := 0; i < sender.Threads; i++ {
		go func(zpktrace []*zpkmodel.SpanModel) {
			defer wg.Done()

			for j := 0; j < sender.SendCount; j++ {
				zpktrace = modifyTraceID(zpktrace)
				buf, err := serializer.Serialize(zpktrace)
				if err != nil {
					log.Fatalln(err.Error())
				}

				req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(buf))
				if err != nil {
					log.Println(err.Error())
					continue
				}
				req.Header = headers

				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					log.Println(err.Error())
					continue
				}
				log.Println(resp.Status)
				resp.Body.Close()
			}
		}(shallowCopyZipkinModels(zpktrace))
	}
	wg.Wait()

	close(globalCloser)
}

func randZipkinTraceID() zpkmodel.TraceID {
	return zpkmodel.TraceID{
		High: rand.Uint64(),
		Low:  rand.Uint64(),
	}
}

func shallowCopyZipkinModels(zpktrace []*zpkmodel.SpanModel) []*zpkmodel.SpanModel {
	trace := make([]*zpkmodel.SpanModel, len(zpktrace))
	for i := range zpktrace {
		trace[i] = &zpkmodel.SpanModel{
			SpanContext:    zpktrace[i].SpanContext,
			Name:           zpktrace[i].Name,
			Kind:           zpktrace[i].Kind,
			Timestamp:      zpktrace[i].Timestamp,
			Duration:       zpktrace[i].Duration,
			Shared:         zpktrace[i].Shared,
			LocalEndpoint:  zpktrace[i].LocalEndpoint,
			RemoteEndpoint: zpktrace[i].RemoteEndpoint,
			Annotations:    zpktrace[i].Annotations,
			Tags:           zpktrace[i].Tags,
		}
	}

	return trace
}

func modifyTraceID(zpktrace []*zpkmodel.SpanModel) []*zpkmodel.SpanModel {
	randTraceID := randZipkinTraceID()
	for i := range zpktrace {
		zpktrace[i].SpanContext.TraceID = randTraceID
	}

	return zpktrace
}

func zipkinTraceIDFromHex(h string) (t zpkmodel.TraceID, err error) {
	if len(h) > 16 {
		if t.High, err = strconv.ParseUint(h[0:len(h)-16], 16, 64); err != nil {
			return
		}
		t.Low, err = strconv.ParseUint(h[len(h)-16:], 16, 64)
		return
	}
	t.Low, err = strconv.ParseUint(h, 16, 64)

	return
}

func parseZipkinTraceV2(encoding string, contentType string, body io.ReadCloser) ([]*zpkmodel.SpanModel, error) {
	var (
		buf []byte
		err error
	)
	if encoding == "gzip" {
		gziprd, err := gzip.NewReader(body)
		if err != nil {
			return nil, err
		}
		defer gziprd.Close()

		if buf, err = io.ReadAll(gziprd); err != nil {
			return nil, err
		}
	} else {
		if buf, err = io.ReadAll(body); err != nil {
			return nil, err
		}
		defer body.Close()
	}

	var zpkmodels []*zpkmodel.SpanModel
	switch contentType {
	case "application/x-protobuf":
		zpkmodels, err = parseZipkinProtobuf3(buf)
	case "application/json":
		err = json.Unmarshal(buf, &zpkmodels)
	default:
		err = fmt.Errorf("### zipkin V2 unsupported Content-Type: %s", contentType)
	}

	return zpkmodels, err
}

func parseZipkinProtobuf3(body []byte) (zss []*zpkmodel.SpanModel, err error) {
	var listOfSpans zpkprotov2.ListOfSpans
	if err := proto.Unmarshal(body, &listOfSpans); err != nil {
		return nil, err
	}

	for _, zps := range listOfSpans.Spans {
		traceID, err := zipkinTraceIDFromHex(hex.EncodeToString(zps.TraceId))
		if err != nil {
			return nil, fmt.Errorf("invalid TraceID: %w", err)
		}

		parentSpanID, _, err := zipkinSpanIDToModelSpanID(zps.ParentId)
		if err != nil {
			return nil, fmt.Errorf("invalid ParentID: %w", err)
		}
		spanIDPtr, spanIDBlank, err := zipkinSpanIDToModelSpanID(zps.Id) //nolint:stylecheck
		if err != nil {
			return nil, fmt.Errorf("invalid SpanID: %w", err)
		}
		if spanIDBlank || spanIDPtr == nil {
			// This is a logical error
			return nil, fmt.Errorf("expected a non-nil SpanID")
		}

		zmsc := zpkmodel.SpanContext{
			TraceID:  traceID,
			ID:       *spanIDPtr,
			ParentID: parentSpanID,
			Debug:    false,
		}
		zms := &zpkmodel.SpanModel{
			SpanContext:    zmsc,
			Name:           zps.Name,
			Kind:           zpkmodel.Kind(zps.Kind.String()),
			Timestamp:      microsToTime(zps.Timestamp),
			Tags:           zps.Tags,
			Duration:       time.Duration(zps.Duration) * time.Microsecond,
			LocalEndpoint:  protoEndpointToModelEndpoint(zps.LocalEndpoint),
			RemoteEndpoint: protoEndpointToModelEndpoint(zps.RemoteEndpoint),
			Shared:         zps.Shared,
			Annotations:    protoAnnotationsToModelAnnotations(zps.Annotations),
		}
		zss = append(zss, zms)
	}

	return zss, nil
}

func protoEndpointToModelEndpoint(zpe *zpkprotov2.Endpoint) *zpkmodel.Endpoint {
	if zpe == nil {
		return nil
	}

	return &zpkmodel.Endpoint{
		ServiceName: zpe.ServiceName,
		IPv4:        net.IP(zpe.Ipv4),
		IPv6:        net.IP(zpe.Ipv6),
		Port:        uint16(zpe.Port),
	}
}

func zipkinSpanIDToModelSpanID(spanID []byte) (zid *zpkmodel.ID, blank bool, err error) {
	if len(spanID) == 0 {
		return nil, true, nil
	}
	if len(spanID) != 8 {
		return nil, true, fmt.Errorf("has length %d yet wanted length 8", len(spanID))
	}

	// Converting [8]byte --> uint64
	u64 := binary.BigEndian.Uint64(spanID)
	zid_ := zpkmodel.ID(u64)

	return &zid_, false, nil
}

func protoAnnotationsToModelAnnotations(zpa []*zpkprotov2.Annotation) (zma []zpkmodel.Annotation) {
	for _, za := range zpa {
		if za != nil {
			zma = append(zma, zpkmodel.Annotation{
				Timestamp: microsToTime(za.Timestamp),
				Value:     za.Value,
			})
		}
	}
	if len(zma) == 0 {
		return nil
	}

	return zma
}

func microsToTime(us uint64) time.Time {
	return time.Unix(0, int64(us*1e3)).UTC()
}
