package ociloadbalancer

import (
	"crypto/tls"
	"net/http"
	"strings"
	"time"
)

const (
	logCategoryAccess = "access"
	logCategoryError  = "error"
	logTimeFormat     = "2006-01-02T15:04:05-07:00"
)

// LogSink receives load balancer log records: category is access or error.
type LogSink func(lbID, category string, at time.Time, data map[string]any)

// SetLogSink wires access and error logs to the OCI Logging service; nil disables them.
func (b *InMemoryBackend) SetLogSink(fn LogSink) {
	b.mu.Lock("SetLogSink")
	b.logSink = fn
	b.mu.Unlock()
}

func (b *InMemoryBackend) sink() LogSink {
	b.mu.RLock("sink")
	defer b.mu.RUnlock()

	return b.logSink
}

type accessInfo struct {
	listener string
	backend  string
}

type countingWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (c *countingWriter) WriteHeader(code int) {
	if c.status == 0 {
		c.status = code
	}

	c.ResponseWriter.WriteHeader(code)
}

func (c *countingWriter) Write(p []byte) (int, error) {
	if c.status == 0 {
		c.status = http.StatusOK
	}

	n, err := c.ResponseWriter.Write(p)
	c.bytes += int64(n)

	return n, err //nolint:wrapcheck // pass-through of the underlying writer
}

func (c *countingWriter) Unwrap() http.ResponseWriter { return c.ResponseWriter }

func tlsName(r *http.Request) (string, string) {
	if r.TLS == nil {
		return "", ""
	}

	proto := strings.TrimPrefix(tls.VersionName(r.TLS.Version), "TLS ")
	if proto != "" {
		proto = "TLSv" + proto
	}

	return tls.CipherSuiteName(r.TLS.CipherSuite), proto
}

// accessRecord builds an access log entry with the fields of the Logging service reference
// (docs.oracle.com Content/Logging/Reference/details_for_load_balancer_logs.htm).
func accessRecord(
	r *http.Request, cw *countingWriter, in accessInfo, lb *LoadBalancer, start, end time.Time,
) map[string]any {
	cipher, proto := tlsName(r)
	elapsed := end.Sub(start).Seconds()

	rec := map[string]any{
		"timestamp":                end.UTC().Format(logTimeFormat),
		"clientAdd":                r.RemoteAddr,
		"forwardedForAddr":         clientAddr(r).String(),
		"host":                     hostOnly(r.Host),
		"backendAddr":              in.backend,
		"requestProcessingTime":    elapsed,
		"backendConnectTime":       0.0,
		"backendProcessingTime":    elapsed,
		"lbStatusCode":             cw.status,
		"receivedBytes":            max(r.ContentLength, 0),
		"sentBytes":                cw.bytes,
		"request":                  r.Method + " " + r.URL.RequestURI() + " " + r.Proto,
		"sslCipher":                cipher,
		"sslProtocol":              proto,
		"listenerName":             in.listener,
		"userAgent":                r.UserAgent(),
		"routingRulesMatchedRule":  "",
		"routingRulesRuleHits":     0,
		"routingRulesRuleMisses":   0,
		"routingRulesEngineErrors": 0,
		"requestId":                "",
		"responseProvider":         in.backend,
	}

	if lb.RequestIDOn {
		rec["requestId"] = r.Header.Get(lb.RequestIDHeader)
	}

	return rec
}

func errorRecord(at time.Time, kind, details string) map[string]any {
	return map[string]any{
		"timestamp": at.UTC().Format(logTimeFormat),
		"errorLog":  map[string]any{"type": kind, "errorDetails": details},
	}
}
