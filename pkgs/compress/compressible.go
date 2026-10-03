package compress

import "strings"

// Compressible reports whether a Content-Type is worth compressing.
// Streaming framings (SSE, event streams, gRPC, Connect streams) are excluded.
func Compressible(contentType string) bool {
	mt, _, _ := strings.Cut(contentType, ";")

	mt = strings.ToLower(strings.TrimSpace(mt))
	if mt == "" {
		return false
	}

	switch {
	case mt == "text/event-stream",
		strings.HasPrefix(mt, "application/grpc"),
		strings.HasPrefix(mt, "application/connect"),
		strings.HasPrefix(mt, "application/vnd.amazon.eventstream"):
		return false
	case strings.HasPrefix(mt, "text/"),
		strings.HasPrefix(mt, "application/x-amz-json-"),
		strings.HasSuffix(mt, "+json"),
		strings.HasSuffix(mt, "+xml"):
		return true
	}

	switch mt {
	case "application/json", "application/javascript", "application/x-javascript",
		"application/ecmascript", "application/xml", "application/wasm",
		"application/cbor", "application/yaml", "application/x-ndjson", "application/x-yaml":
		return true
	}

	return false
}
