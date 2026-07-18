package memorydb

import (
	"encoding/json"
	"time"
)

// Event represents a MemoryDB event.
type Event struct {
	Date       time.Time `json:"date"`
	SourceName string    `json:"sourceName"`
	SourceType string    `json:"sourceType"`
	Message    string    `json:"message"`
}

// describeEventsRequest.StartTime/EndTime use json.Number, not time.Time:
// real aws-sdk-go-v2 clients serialize these TStamp shapes as a JSON number
// of epoch seconds (awsjson1.1), and json.Unmarshal into a time.Time field
// requires a quoted RFC3339 string -- decoding a real client's request would
// fail with a SerializationException. See parseEpochRequestTime (backend.go).
type describeEventsRequest struct {
	MaxResults *int32      `json:"MaxResults,omitempty"`
	Duration   *int32      `json:"Duration,omitempty"`
	StartTime  json.Number `json:"StartTime,omitempty"`
	EndTime    json.Number `json:"EndTime,omitempty"`
	SourceName string      `json:"SourceName,omitempty"`
	SourceType string      `json:"SourceType,omitempty"`
	NextToken  string      `json:"NextToken,omitempty"`
}

// eventObject.Date is epoch seconds (float64), matching the real
// Event.Date TStamp shape -- see toEventObject / awstime.Epoch.
type eventObject struct {
	SourceName string  `json:"SourceName,omitempty"`
	SourceType string  `json:"SourceType,omitempty"`
	Message    string  `json:"Message,omitempty"`
	Date       float64 `json:"Date,omitempty"`
}

type describeEventsResponse struct {
	NextToken string        `json:"NextToken,omitempty"`
	Events    []eventObject `json:"Events"`
}

// -- MultiRegionCluster request/response types --------------------------------
