package pipes

import (
	"encoding/json"
	"time"
)

// epochSecondsPtr converts *time.Time into the epoch-seconds-with-millisecond-
// precision wire format real AWS SDK clients use for
// KinesisStreamSourceParameters.StartingPositionTimestamp (restjson1;
// aws-sdk-go-v2's serializers.go: ok.Double(smithytime.FormatEpochSeconds(...))).
// Nil-safe, pairs with timeFromEpochSecondsPtr.
func epochSecondsPtr(t *time.Time) *float64 {
	if t == nil {
		return nil
	}

	v := epochMillis(*t)

	return &v
}

const millisPerSecond = 1000

// timeFromEpochSecondsPtr is the inverse of epochSecondsPtr.
func timeFromEpochSecondsPtr(f *float64) *time.Time {
	if f == nil {
		return nil
	}

	t := time.UnixMilli(int64(*f * millisPerSecond)).UTC()

	return &t
}

// MarshalJSON encodes StartingPositionTimestamp as the epoch-seconds JSON
// number real clients expect, not encoding/json's default RFC3339 string.
func (k KinesisStreamSourceParameters) MarshalJSON() ([]byte, error) {
	type alias KinesisStreamSourceParameters

	return json.Marshal(struct {
		StartingPositionTimestamp *float64 `json:"StartingPositionTimestamp,omitempty"`
		alias
	}{
		StartingPositionTimestamp: epochSecondsPtr(k.StartingPositionTimestamp),
		alias:                     alias(k),
	})
}

// UnmarshalJSON is the inverse of MarshalJSON. Without it, a real client's
// StartingPositionTimestamp (a JSON number on the wire) fails
// time.Time.UnmarshalJSON ("input is not a JSON string"), which rejects the
// entire CreatePipe/UpdatePipe request body, not just this field.
func (k *KinesisStreamSourceParameters) UnmarshalJSON(data []byte) error {
	type alias KinesisStreamSourceParameters

	aux := struct {
		StartingPositionTimestamp *float64 `json:"StartingPositionTimestamp,omitempty"`
		*alias
	}{alias: (*alias)(k)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	k.StartingPositionTimestamp = timeFromEpochSecondsPtr(aux.StartingPositionTimestamp)

	return nil
}
