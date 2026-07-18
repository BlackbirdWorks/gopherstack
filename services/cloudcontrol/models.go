package cloudcontrol

import (
	"encoding/json"
	"time"
)

const (
	opStatusSuccess        = "SUCCESS"
	opStatusCancelComplete = "CANCEL_COMPLETE"
)

// defaultListMaxResults is the default page size for list operations.
const defaultListMaxResults = 100

// unixEpochTime wraps [time.Time] and marshals to/from a JSON number (Unix seconds),
// which is the format expected by the AWS CloudControl SDK v2 client.
type unixEpochTime struct {
	time.Time
}

func (t unixEpochTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Unix())
}

func (t *unixEpochTime) UnmarshalJSON(b []byte) error {
	var epoch int64
	if err := json.Unmarshal(b, &epoch); err != nil {
		return err
	}

	t.Time = time.Unix(epoch, 0)

	return nil
}

// Resource represents an in-memory CloudControl managed resource.
type Resource struct {
	TypeName   string
	Identifier string
	Properties string // JSON string of current properties
}

// ProgressEvent represents the status of a CloudControl resource operation.
type ProgressEvent struct {
	EventTime       unixEpochTime `json:"EventTime"`
	TypeName        string        `json:"TypeName"`
	Identifier      string        `json:"Identifier,omitempty"`
	RequestToken    string        `json:"RequestToken"`
	Operation       string        `json:"Operation"`
	OperationStatus string        `json:"OperationStatus"`
	StatusMessage   string        `json:"StatusMessage,omitempty"`
	// ResourceModel is a JSON string containing the resource model -- each
	// resource property and its current value -- per the real ProgressEvent
	// shape. Populated on SUCCESS so callers can read the resource straight
	// off the ProgressEvent without a follow-up GetResource call, matching
	// real AWS CLI/SDK/IaC-tool usage of this field.
	ResourceModel string `json:"ResourceModel,omitempty"`
}
