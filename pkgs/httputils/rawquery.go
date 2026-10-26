package httputils

import (
	"net/url"
	"strings"
)

// RawQueryLookup returns the first value for name in rawQuery and whether the
// key is present, matching url.ParseQuery semantics without allocating when no
// percent-escapes or '+' are involved. Segments containing ';' or with invalid
// escapes are skipped, as url.ParseQuery does.
func RawQueryLookup(rawQuery, name string) (string, bool) {
	for rawQuery != "" {
		var seg string

		seg, rawQuery, _ = strings.Cut(rawQuery, "&")
		if seg == "" || strings.Contains(seg, ";") {
			continue
		}

		key, val, _ := strings.Cut(seg, "=")
		if !queryKeyEquals(key, name) {
			continue
		}

		if !strings.ContainsAny(val, "%+") {
			return val, true
		}

		if dec, err := url.QueryUnescape(val); err == nil {
			return dec, true
		}
	}

	return "", false
}

// RawQueryGet is RawQueryLookup without the presence flag.
func RawQueryGet(rawQuery, name string) string {
	v, _ := RawQueryLookup(rawQuery, name)

	return v
}

// RawQueryHas reports whether name is a key in rawQuery.
func RawQueryHas(rawQuery, name string) bool {
	_, ok := RawQueryLookup(rawQuery, name)

	return ok
}

func queryKeyEquals(key, name string) bool {
	if !strings.ContainsAny(key, "%+") {
		return key == name
	}

	dec, err := url.QueryUnescape(key)

	return err == nil && dec == name
}
