package cloudtrail

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// eventVersion is the CloudTrail record schema version emitted in the
// CloudTrailEvent detail JSON. AWS currently emits 1.08 for management events.
const eventVersion = "1.08"

// managementEventDetail mirrors the JSON object AWS embeds (as a string) in
// the CloudTrailEvent field of a LookupEvents result: a full, self-contained
// record of the API call, independent of the top-level Event summary fields.
type managementEventDetail struct {
	UserIdentity       managementEventIdentity `json:"userIdentity"`
	EventVersion       string                  `json:"eventVersion"`
	EventTime          string                  `json:"eventTime"`
	EventSource        string                  `json:"eventSource"`
	EventName          string                  `json:"eventName"`
	AwsRegion          string                  `json:"awsRegion"`
	RequestID          string                  `json:"requestID"`
	EventID            string                  `json:"eventID"`
	EventType          string                  `json:"eventType"`
	RecipientAccountID string                  `json:"recipientAccountId,omitempty"`
	EventCategory      string                  `json:"eventCategory"`
	ReadOnly           bool                    `json:"readOnly"`
	ManagementEvent    bool                    `json:"managementEvent"`
}

// managementEventIdentity is the "userIdentity" block of a CloudTrail record.
type managementEventIdentity struct {
	Type        string `json:"type"`
	PrincipalID string `json:"principalId,omitempty"`
	AccessKeyID string `json:"accessKeyId,omitempty"`
	AccountID   string `json:"accountId,omitempty"`
}

// RecordManagementEvent implements service.CloudTrailRecorder. It is invoked
// by the central service registry (pkgs/service.Registry) after every
// mutating API call made against any registered emulator service. This is
// what turns the previously-unused RecordEvent path into a real, globally
// wired CloudTrail capture point (the LocalStack model: CloudTrail records
// mutating control-plane calls regardless of which service handled them),
// so LookupEvents returns genuine activity instead of always being empty.
func (b *InMemoryBackend) RecordManagementEvent(ev service.CloudTrailEventInput) {
	now := time.Now().UTC()
	eventID := uuid.NewString()

	b.mu.RLock("RecordManagementEvent:accountID")
	accountID := b.accountID
	b.mu.RUnlock()

	principalType := "AWSAccount"
	if ev.AccessKeyID != "" {
		principalType = "IAMUser"
	}

	detail := managementEventDetail{
		UserIdentity: managementEventIdentity{
			Type:        principalType,
			PrincipalID: ev.AccessKeyID,
			AccessKeyID: ev.AccessKeyID,
			AccountID:   accountID,
		},
		EventVersion:       eventVersion,
		EventTime:          now.Format(time.RFC3339Nano),
		EventSource:        ev.EventSource,
		EventName:          ev.EventName,
		AwsRegion:          ev.AwsRegion,
		RequestID:          uuid.NewString(),
		EventID:            eventID,
		EventType:          "AwsApiCall",
		RecipientAccountID: accountID,
		EventCategory:      eventCategoryManagement,
		ReadOnly:           false,
		ManagementEvent:    true,
	}

	var cloudTrailEventJSON string
	if blob, err := json.Marshal(detail); err == nil {
		cloudTrailEventJSON = string(blob)
	}

	username := ev.Username
	if username == "" {
		username = ev.AccessKeyID
	}

	var resources []EventResource
	if ev.ResourceName != "" {
		resources = []EventResource{{ResourceName: ev.ResourceName}}
	}

	b.RecordEvent(Event{
		EventID:         eventID,
		EventTime:       now,
		EventName:       ev.EventName,
		EventSource:     ev.EventSource,
		Username:        username,
		ReadOnly:        "false",
		AccessKeyID:     ev.AccessKeyID,
		EventCategory:   eventCategoryManagement,
		CloudTrailEvent: cloudTrailEventJSON,
		Resources:       resources,
	})
}
