package eventbridge

import (
	"encoding/json"
	"time"
)

// parseEpochSecondsPtr converts an AWS json-1.1 wire-format epoch-seconds
// JSON number (optionally fractional, e.g. 1700000000.5) into a *time.Time,
// returning nil for an absent/null value. This mirrors aws-sdk-go-v2's
// smithytime.ParseEpochSeconds, the format real AWS SDK clients use to
// serialize *time.Time request fields (see EventEntry.UnmarshalJSON and
// StartReplayInput.UnmarshalJSON below) -- plain time.Time only accepts a
// quoted RFC3339 string via its own UnmarshalJSON, which a real client's
// epoch-seconds request body would fail to satisfy.
func parseEpochSecondsPtr(raw *float64) *time.Time {
	if raw == nil {
		return nil
	}

	sec := int64(*raw)
	nsec := int64((*raw - float64(sec)) * float64(time.Second))
	t := time.Unix(sec, nsec).UTC()

	return &t
}

// wireEventEntry is EventEntry's wire shape: identical field-for-field except
// Time, which the AWS json-1.1 protocol serializes as an epoch-seconds JSON
// number (PutEventsRequestEntry.Time in aws-sdk-go-v2's serializers.go uses
// smithytime.FormatEpochSeconds), not an RFC3339 string.
type wireEventEntry struct {
	Time         *float64 `json:"Time,omitempty"`
	Source       string   `json:"Source"`
	DetailType   string   `json:"DetailType"`
	Detail       string   `json:"Detail"`
	EventBusName string   `json:"EventBusName,omitempty"`
	Resources    []string `json:"Resources,omitempty"`
}

// UnmarshalJSON parses EventEntry (PutEvents/PutPartnerEvents request
// entries) from the AWS json-1.1 wire format. A default struct-tag-driven
// json.Unmarshal would reject any real AWS SDK client's request because Time
// is a JSON number on the wire but time.Time.UnmarshalJSON only accepts a
// quoted RFC3339 string.
func (e *EventEntry) UnmarshalJSON(data []byte) error {
	var w wireEventEntry
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}

	e.Time = parseEpochSecondsPtr(w.Time)
	e.Source = w.Source
	e.DetailType = w.DetailType
	e.Detail = w.Detail
	e.EventBusName = w.EventBusName
	e.Resources = w.Resources

	return nil
}

// wireStartReplayInput is StartReplayInput's wire shape: identical
// field-for-field except EventStartTime/EventEndTime, which the AWS json-1.1
// protocol serializes as epoch-seconds JSON numbers
// (StartReplayInput.EventStartTime/EventEndTime in aws-sdk-go-v2's
// serializers.go use smithytime.FormatEpochSeconds), not RFC3339 strings.
type wireStartReplayInput struct {
	Destination    *ReplayDestination `json:"Destination,omitempty"`
	EventEndTime   *float64           `json:"EventEndTime,omitempty"`
	EventStartTime *float64           `json:"EventStartTime,omitempty"`
	Description    string             `json:"Description,omitempty"`
	EventSourceArn string             `json:"EventSourceArn"`
	ReplayName     string             `json:"ReplayName"`
}

// UnmarshalJSON parses StartReplayInput from the AWS json-1.1 wire format --
// see wireStartReplayInput's doc comment for why this can't be the default
// struct-tag-driven json.Unmarshal.
func (in *StartReplayInput) UnmarshalJSON(data []byte) error {
	var w wireStartReplayInput
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}

	in.Destination = w.Destination
	in.Description = w.Description
	in.EventSourceArn = w.EventSourceArn
	in.ReplayName = w.ReplayName
	if t := parseEpochSecondsPtr(w.EventEndTime); t != nil {
		in.EventEndTime = *t
	}
	if t := parseEpochSecondsPtr(w.EventStartTime); t != nil {
		in.EventStartTime = *t
	}

	return nil
}
