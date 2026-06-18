// Package awstime provides helpers for emitting timestamps in the wire format
// expected by AWS JSON-protocol SDK deserializers.
//
// The AWS "json" and "rest-json" protocols default to the unixTimestamp
// timestamp format, which serializes a point in time as a JSON number of
// seconds since the Unix epoch (with optional fractional milliseconds). The
// SDK deserializers reject RFC3339 strings for these shapes with an error of
// the form "expected Timestamp to be a JSON Number, got string instead".
//
// Use Epoch to convert a time.Time into a value that json.Marshal renders as
// the correct numeric wire form.
package awstime

import "time"

// Epoch converts t into seconds since the Unix epoch, preserving
// sub-second precision as a fractional component. The returned float64 is
// rendered by encoding/json as a JSON number, matching the unixTimestamp
// format used by the AWS json and rest-json protocols.
//
// A zero time.Time returns 0, matching AWS behavior of omitting unset
// timestamps (callers that must omit the field entirely should guard on
// t.IsZero() before adding it to the response).
func Epoch(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}

	return float64(t.UnixNano()) / float64(time.Second)
}
