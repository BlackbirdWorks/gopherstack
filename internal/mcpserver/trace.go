package mcpserver

import (
	"net/url"
	"strings"
	"time"
)

type capturedRequest struct {
	Timestamp    time.Time         `json:"timestamp"`
	Headers      map[string]string `json:"headers"`
	ID           string            `json:"id"`
	Method       string            `json:"method"`
	Path         string            `json:"path"`
	Query        string            `json:"query"`
	Service      string            `json:"service"`
	ErrorCode    string            `json:"error_code"`
	ErrorMessage string            `json:"error_message"`
	Status       int               `json:"status"`
	Duration     time.Duration     `json:"duration_ms"`
}

func isSensitiveQueryKey(k string) bool {
	switch strings.ToLower(k) {
	case "x-amz-signature", "x-amz-credential", "x-amz-security-token",
		"signature", "awsaccesskeyid", "token", "sig":
		return true
	}

	return false
}

func (r *capturedRequest) trace() Trace {
	return Trace{
		Timestamp:    r.Timestamp,
		ID:           r.ID,
		Method:       r.Method,
		Path:         r.Path,
		Query:        redactQuery(r.Query),
		Service:      r.Service,
		Operation:    r.operation(),
		ErrorCode:    r.ErrorCode,
		ErrorMessage: r.ErrorMessage,
		Status:       r.Status,
		DurationMS:   r.Duration.Milliseconds(),
	}
}

func (r *capturedRequest) operation() string {
	for k, v := range r.Headers {
		if strings.EqualFold(k, "X-Amz-Target") {
			if _, op, ok := strings.Cut(v, "."); ok {
				return op
			}

			return v
		}
	}

	vals, err := url.ParseQuery(r.Query)
	if err != nil {
		return ""
	}

	return vals.Get("Action")
}

func redactQuery(raw string) string {
	if raw == "" {
		return ""
	}

	vals, err := url.ParseQuery(raw)
	if err != nil {
		return redactedValue
	}

	for k := range vals {
		if isSensitiveQueryKey(k) {
			vals.Set(k, redactedValue)
		}
	}

	return vals.Encode()
}
