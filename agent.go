package main

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

var (
	globalCloser = make(chan struct{})
	zipv2        = "/api/v2/spans"
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

	buf, err := io.ReadAll(req.Body)
	if err != nil {
		log.Fatalln(err.Error())
	}
	if len(buf) <= 1 {
		return
	}

	if cfg.Sender.Threads > 0 && cfg.Sender.SendCount > 0 {
		go sendZipkinTask(cfg.Sender, buf, "http://"+cfg.DkAgent+zipv2, req.Header)
	} else {
		close(globalCloser)
	}
}

func sendZipkinTask(sender *sender, buf []byte, endpoint string, headers http.Header) {
	wg := sync.WaitGroup{}
	wg.Add(sender.Threads)
	for i := 0; i < sender.Threads; i++ {
		go func(buf []byte) {
			defer wg.Done()

			for j := 0; j < sender.SendCount; j++ {
				req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBuffer(buf))
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
				resp.Body.Close()
				log.Println(resp.Status)
			}
		}(buf)
	}
	wg.Wait()

	close(globalCloser)
}
