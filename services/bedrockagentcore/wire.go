package bedrockagentcore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

type doc = map[string]any

const (
	keyCreatedAt    = "createdAt"
	keyStatus       = "status"
	keyRecordID     = "memoryRecordId"
	keyActorID      = "actorId"
	keyText         = "text"
	keyEvent        = "event"
	keyFailed       = "failedRecords"
	statusSucceeded = "SUCCEEDED"
	opAfter         = "AFTER"
	opBefore        = "BEFORE"
	keyType         = "type"
	keySessionID    = "sessionId"
	keySucceeded    = "successfulRecords"
)

func decodeDoc(req *restroute.Request) (doc, error) {
	d := doc{}
	if err := req.Decode(&d); err != nil {
		return nil, err
	}

	return d, nil
}

func strOf(m doc, k string) string {
	s, _ := m[k].(string)

	return s
}

func objOf(m doc, k string) doc {
	o, _ := m[k].(map[string]any)

	return o
}

func listOf(m doc, k string) []any {
	l, _ := m[k].([]any)

	return l
}

func intOf(m doc, k string) int {
	f, _ := m[k].(float64)

	return int(f)
}

func boolOf(m doc, k string, def bool) bool {
	if b, ok := m[k].(bool); ok {
		return b
	}

	return def
}

func strList(m doc, k string) []string {
	var out []string

	for _, v := range listOf(m, k) {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}

	return out
}

func epoch(t time.Time) float64 { return awstime.Epoch(t) }

func isoTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func fromEpoch(v any) (time.Time, bool) {
	f, ok := v.(float64)
	if !ok {
		return time.Time{}, false
	}

	sec := int64(f)

	return time.Unix(sec, int64((f-float64(sec))*float64(time.Second))).UTC(), true
}

func exactlyOne(m doc, keys ...string) (string, bool) {
	found := ""

	for _, k := range keys {
		if _, ok := m[k]; ok {
			if found != "" {
				return "", false
			}

			found = k
		}
	}

	return found, found != ""
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	return keys
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)

	return b
}

type bytesBuffer = bytes.Buffer

func sprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }

func readJSONBody(r *http.Request) (doc, error) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		return nil, invalid("unable to read the request body")
	}

	d := doc{}
	if len(body) == 0 {
		return d, nil
	}

	if err = json.Unmarshal(body, &d); err != nil {
		return nil, invalid("request body is not valid JSON")
	}

	return d, nil
}
