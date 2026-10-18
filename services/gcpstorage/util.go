package gcpstorage

import (
	"encoding/json"
	"strconv"
	"time"
)

func itoa(n int) string { return strconv.Itoa(n) }

func marshalRaw(v any) json.RawMessage {
	raw, _ := json.Marshal(v) //nolint:errchkjson // strings and string maps always marshal

	return raw
}

// fmtTime renders t in the RFC 3339 millisecond form GCS emits.
func fmtTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

func itoa64(n int64) string { return strconv.FormatInt(n, 10) }
