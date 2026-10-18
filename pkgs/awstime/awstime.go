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

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

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

// EpochTime is a time.Time that marshals to and from epoch-seconds JSON numbers, so a model struct can be
// persisted and sent on the wire in the unixTimestamp form without a separate DTO.
type EpochTime struct{ time.Time }

// NewEpochTime wraps t.
func NewEpochTime(t time.Time) EpochTime { return EpochTime{Time: t} }

// MarshalJSON renders the time as epoch seconds.
func (t EpochTime) MarshalJSON() ([]byte, error) {
	return strconv.AppendFloat(nil, Epoch(t.Time), 'f', -1, float64Bits), nil
}

// UnmarshalJSON parses an epoch-seconds number; null leaves the zero time.
func (t *EpochTime) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}

	f, err := strconv.ParseFloat(string(data), float64Bits)
	if err != nil {
		return fmt.Errorf("awstime: epoch timestamp must be a JSON number: %w", err)
	}

	sec, frac := math.Modf(f)
	t.Time = time.Unix(int64(sec), int64(math.Round(frac*float64(time.Second)))).UTC()

	return nil
}

const float64Bits = 64
