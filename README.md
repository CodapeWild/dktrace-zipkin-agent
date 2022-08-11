# Zipkin Agent for Datakit

**Notice:** THIS PROJECT IS STILL IN PROGRESS

This tool used to send standard Zipkin tracing data to Datakit.

The features include:

- build with standard Zipkin Golang lib
- customized Span data
- configurable multi-thread pressure test

## Config

```json
{
  "dk_agent": "127.0.0.1:9529",
  "sender": {
    "threads": 1,
    "send_count": 1
  },
  "service": "dktrace-zipkin-agent",
  "encode": "protobuf3",
  "dump_size": 1024,
  "random_dump": false,
  "trace": []
}
```

- `dk_agent`: Datakit host address
- `sender.threads`: how many threads will start to send `trace` simultaneously
- `sender.send_count`: how many times `trace` will be send in one `thread`
- `service`: service name
- `encode`: trace data serializing type, `json` and `protobuf3` are supported
- `dump_size`: the data size in kb used to fillup the trace, 0: no extra data
- `random_dump`: whether to fillup the span with random size extra data
- `trace`: represents a Trace consists of Spans

## Span Structure

`trace`\[span...\]

```json
{
  "resource": "/get/user/name",
  "operation": "user.getUserName",
  "span_type": "",
  "duration": 1000,
  "error": "access deny, status code 100010",
  "tags": [
    {
      "key": "original_id",
      "value": "7898-7897-3456788"
    }
  ],
  "children": []
}
```

**Note:** Spans list in `trace` or `children` will generate concurrency Spans, nested in `trace` or `children` will show up as calling chain.

- `resource`: resource name
- `operation`: operation name
- `span_type`: Span type [app cache custom db web]
- `duration`: how long an operation process will last
- `error`: error string
- `tags`: Span meta data, imitate client tags
- `children`: child Spans represent a subsequent function calling from current `operation`
