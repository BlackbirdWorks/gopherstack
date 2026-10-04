package cloudtrail

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// renderBufSize is the initial capacity for a rendered management event record.
const renderBufSize = 512

// eventVersion is the CloudTrail record schema version emitted in the
// CloudTrailEvent detail JSON. AWS currently emits 1.08 for management events.
const eventVersion = "1.08"

// managementEventDetail mirrors the JSON object AWS embeds (as a string) in
// the CloudTrailEvent field of a LookupEvents result: a full, self-contained
// record of the API call, independent of the top-level Event summary fields.
type managementEventDetail struct {
	UserIdentity managementEventIdentity `json:"userIdentity"`
	EventVersion string                  `json:"eventVersion"`
	EventTime    string                  `json:"eventTime"`
	EventSource  string                  `json:"eventSource"`
	EventName    string                  `json:"eventName"`
	AwsRegion    string                  `json:"awsRegion"`
	// ErrorCode/ErrorMessage: "The AWS service error if the request returns
	// an error" / "If the request returns an error, the description of the
	// error" -- both documented, top-level, "Since: 1.0" record fields
	// (docs.aws.amazon.com/awscloudtrail/latest/userguide/
	// cloudtrail-event-reference-record-contents.html), populated only for
	// a failed call -- see pkgs/service/cloudtrail_capture.go's
	// extractErrorInfo for how (and how completely) that's detected.
	ErrorCode          string `json:"errorCode,omitempty"`
	ErrorMessage       string `json:"errorMessage,omitempty"`
	RequestID          string `json:"requestID"`
	EventID            string `json:"eventID"`
	EventType          string `json:"eventType"`
	RecipientAccountID string `json:"recipientAccountId,omitempty"`
	EventCategory      string `json:"eventCategory"`
	ReadOnly           bool   `json:"readOnly"`
	ManagementEvent    bool   `json:"managementEvent"`
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
		ErrorCode:          ev.ErrorCode,
		ErrorMessage:       ev.ErrorMessage,
		RequestID:          uuid.NewString(),
		EventID:            eventID,
		EventType:          "AwsApiCall",
		RecipientAccountID: accountID,
		EventCategory:      eventCategoryManagement,
		ReadOnly:           false,
		ManagementEvent:    true,
	}

	cloudTrailEventJSON := renderManagementEvent(&detail)

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

// renderManagementEvent returns json.Marshal(*d) as a string, hand-built for the
// common case where every string is plain printable ASCII needing no escaping.
func renderManagementEvent(d *managementEventDetail) string {
	if !d.plainStrings() {
		blob, err := json.Marshal(d)
		if err != nil {
			return ""
		}

		return string(blob)
	}

	b := make([]byte, 0, renderBufSize)
	b = append(b, `{"userIdentity":{"type":`...)
	b = appendPlain(b, d.UserIdentity.Type)
	b = appendOptional(b, `,"principalId":`, d.UserIdentity.PrincipalID)
	b = appendOptional(b, `,"accessKeyId":`, d.UserIdentity.AccessKeyID)
	b = appendOptional(b, `,"accountId":`, d.UserIdentity.AccountID)
	b = append(b, `},"eventVersion":`...)
	b = appendPlain(b, d.EventVersion)
	b = append(b, `,"eventTime":`...)
	b = appendPlain(b, d.EventTime)
	b = append(b, `,"eventSource":`...)
	b = appendPlain(b, d.EventSource)
	b = append(b, `,"eventName":`...)
	b = appendPlain(b, d.EventName)
	b = append(b, `,"awsRegion":`...)
	b = appendPlain(b, d.AwsRegion)
	b = appendOptional(b, `,"errorCode":`, d.ErrorCode)
	b = appendOptional(b, `,"errorMessage":`, d.ErrorMessage)
	b = append(b, `,"requestID":`...)
	b = appendPlain(b, d.RequestID)
	b = append(b, `,"eventID":`...)
	b = appendPlain(b, d.EventID)
	b = append(b, `,"eventType":`...)
	b = appendPlain(b, d.EventType)
	b = appendOptional(b, `,"recipientAccountId":`, d.RecipientAccountID)
	b = append(b, `,"eventCategory":`...)
	b = appendPlain(b, d.EventCategory)
	b = append(b, `,"readOnly":`...)
	b = strconv.AppendBool(b, d.ReadOnly)
	b = append(b, `,"managementEvent":`...)
	b = strconv.AppendBool(b, d.ManagementEvent)
	b = append(b, '}')

	return string(b)
}

func (d *managementEventDetail) plainStrings() bool {
	for _, v := range [...]string{
		d.UserIdentity.Type, d.UserIdentity.PrincipalID, d.UserIdentity.AccessKeyID, d.UserIdentity.AccountID,
		d.EventVersion, d.EventTime, d.EventSource, d.EventName, d.AwsRegion, d.ErrorCode, d.ErrorMessage,
		d.RequestID, d.EventID, d.EventType, d.RecipientAccountID, d.EventCategory,
	} {
		if !isPlainJSON(v) {
			return false
		}
	}

	return true
}

func isPlainJSON(s string) bool {
	for i := range len(s) {
		if c := s[i]; c < 0x20 || c > 0x7e || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			return false
		}
	}

	return true
}

func appendPlain(b []byte, s string) []byte {
	b = append(b, '"')
	b = append(b, s...)

	return append(b, '"')
}

func appendOptional(b []byte, key, v string) []byte {
	if v == "" {
		return b
	}

	return appendPlain(append(b, key...), v)
}
