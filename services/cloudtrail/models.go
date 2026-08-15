package cloudtrail

import (
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// AdvancedFieldSelector represents a filter condition in an advanced event selector.
// Each field selector specifies a field name and one or more comparison operators.
type AdvancedFieldSelector struct {
	Field         string   `json:"Field"`
	Equals        []string `json:"Equals,omitempty"`
	StartsWith    []string `json:"StartsWith,omitempty"`
	EndsWith      []string `json:"EndsWith,omitempty"`
	NotEquals     []string `json:"NotEquals,omitempty"`
	NotStartsWith []string `json:"NotStartsWith,omitempty"`
	NotEndsWith   []string `json:"NotEndsWith,omitempty"`
}

// AdvancedEventSelector represents an advanced event selector that filters events
// based on field-level conditions. Mutually exclusive with basic EventSelectors.
type AdvancedEventSelector struct {
	Name           string                  `json:"Name,omitempty"`
	FieldSelectors []AdvancedFieldSelector `json:"FieldSelectors"`
}

// LookupAttribute represents a filter attribute for LookupEvents.
type LookupAttribute struct {
	AttributeKey   string `json:"AttributeKey"`
	AttributeValue string `json:"AttributeValue"`
}

// Event represents a recorded management or data event.
type Event struct {
	EventTime   time.Time `json:"EventTime"`
	EventID     string    `json:"EventId"`
	EventName   string    `json:"EventName"`
	EventSource string    `json:"EventSource"`
	Username    string    `json:"Username,omitempty"`
	ReadOnly    string    `json:"ReadOnly,omitempty"`
	AccessKeyID string    `json:"AccessKeyId,omitempty"`
	// EventCategory mirrors the CloudTrail record's eventCategory field
	// ("Management" or "Insight"). Every event this backend records via
	// RecordManagementEvent is a management-plane API call, so it is always
	// "Management" -- this backend never synthesizes Insight events. Used to
	// filter LookupEvents by the LookupEventsInput.EventCategory input field
	// (real AWS: omit it and only Management events are returned; pass
	// "insight" and only Insight events are returned). The real
	// LookupEventsOutput Event shape has no top-level EventCategory field (it
	// is only present nested in the CloudTrailEvent JSON string), but this
	// backend's Event type is shared between the wire response and the
	// internal/persisted record, so this extra key rides along on the wire.
	// Not yet re-verified against the pinned SDK deserializer; see PARITY.md
	// for the dashboard shared-helper leak this pass fixed instead (a
	// different bug -- Status/Name leaking across Create/Get/Update, not
	// this Event/EventCategory field).
	EventCategory string `json:"EventCategory,omitempty"`
	// CloudTrailEvent is the full JSON-encoded event record (eventVersion,
	// userIdentity, eventTime, eventSource, eventName, awsRegion, requestID,
	// eventID, readOnly, eventType, managementEvent, eventCategory, ...),
	// matching the shape AWS embeds as a JSON string in LookupEvents results.
	CloudTrailEvent string          `json:"CloudTrailEvent,omitempty"`
	Resources       []EventResource `json:"Resources,omitempty"`
}

// MarshalJSON renders Event in the AWS JSON-protocol wire format. LookupEvents
// is a JSON-protocol operation whose EventTime shape is a unixTimestamp, so
// the SDK deserializer requires a JSON number of seconds since the epoch
// (see pkgs/awstime) rather than encoding/json's default RFC3339 string.
func (e Event) MarshalJSON() ([]byte, error) {
	type alias Event

	return json.Marshal(struct {
		alias
		EventTime float64 `json:"EventTime"`
	}{
		alias:     alias(e),
		EventTime: awstime.Epoch(e.EventTime),
	})
}

// UnmarshalJSON is the inverse of MarshalJSON: it decodes the epoch-seconds
// EventTime this type emits back into a time.Time. Without this, Event could
// be marshaled (e.g. into a Snapshot) but never restored -- encoding/json's
// default time.Time decoder rejects a JSON number ("parsing time ... as
// RFC3339: cannot parse ..."), so any snapshot containing a recorded event
// would fail Restore entirely. See persistence.go/backendSnapshot.Events.
func (e *Event) UnmarshalJSON(data []byte) error {
	type alias Event

	aux := struct {
		*alias
		EventTime float64 `json:"EventTime"`
	}{alias: (*alias)(e)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if aux.EventTime != 0 {
		e.EventTime = time.Unix(0, int64(aux.EventTime*float64(time.Second))).UTC()
	}

	return nil
}

// EventResource represents a resource associated with a CloudTrail event.
type EventResource struct {
	ResourceName string `json:"ResourceName,omitempty"`
	ResourceType string `json:"ResourceType,omitempty"`
}

// Channel represents a CloudTrail channel resource.
type Channel struct {
	Tags         *tags.Tags    `json:"tags,omitempty"`
	ChannelID    string        `json:"channelId"`
	ChannelARN   string        `json:"channelArn"`
	Name         string        `json:"name"`
	Source       string        `json:"source"`
	Destinations []Destination `json:"destinations,omitempty"`
}

// Destination represents a channel destination.
type Destination struct {
	Type     string `json:"Type"`
	Location string `json:"Location"`
}

// Dashboard represents a CloudTrail dashboard resource.
type Dashboard struct {
	CreatedTimestamp             time.Time        `json:"createdTimestamp"`
	UpdatedTimestamp             time.Time        `json:"updatedTimestamp"`
	Tags                         *tags.Tags       `json:"tags,omitempty"`
	RefreshSchedule              *RefreshSchedule `json:"refreshSchedule,omitempty"`
	DashboardID                  string           `json:"dashboardId"`
	DashboardARN                 string           `json:"dashboardArn"`
	Name                         string           `json:"name"`
	Type                         string           `json:"type"`
	Status                       string           `json:"status"`
	LastRefreshID                string           `json:"lastRefreshId,omitempty"`
	LastRefreshFailureReason     string           `json:"lastRefreshFailureReason,omitempty"`
	Widgets                      []Widget         `json:"widgets,omitempty"`
	TerminationProtectionEnabled bool             `json:"terminationProtectionEnabled"`
}

// RefreshSchedule is the refresh-schedule configuration for a dashboard.
type RefreshSchedule struct {
	Frequency *RefreshScheduleFrequency `json:"Frequency,omitempty"`
	Status    string                    `json:"Status,omitempty"`
	TimeOfDay string                    `json:"TimeOfDay,omitempty"`
}

// RefreshScheduleFrequency specifies how often a dashboard refresh runs.
type RefreshScheduleFrequency struct {
	Unit  string `json:"Unit,omitempty"`
	Value int32  `json:"Value,omitempty"`
}

// Widget represents a widget on a CloudTrail Lake dashboard.
type Widget struct {
	ViewProperties  map[string]string `json:"ViewProperties,omitempty"`
	QueryAlias      string            `json:"QueryAlias,omitempty"`
	QueryStatement  string            `json:"QueryStatement,omitempty"`
	QueryParameters []string          `json:"QueryParameters,omitempty"`
}

// EventDataStore represents a CloudTrail event data store resource.
type EventDataStore struct {
	Tags                   *tags.Tags              `json:"tags,omitempty"`
	CreatedTimestamp       time.Time               `json:"createdTimestamp"`
	UpdatedTimestamp       time.Time               `json:"updatedTimestamp"`
	EventDataStoreID       string                  `json:"eventDataStoreId"`
	EventDataStoreARN      string                  `json:"eventDataStoreArn"`
	Name                   string                  `json:"name"`
	Status                 string                  `json:"status"`
	FederationStatus       string                  `json:"federationStatus,omitempty"`
	FederationRoleArn      string                  `json:"federationRoleArn,omitempty"`
	BillingMode            string                  `json:"billingMode,omitempty"`
	KMSKeyID               string                  `json:"kmsKeyId,omitempty"`
	AdvancedEventSelectors []AdvancedEventSelector `json:"advancedEventSelectors,omitempty"`
	InsightSelectors       []InsightSelector       `json:"insightSelectors,omitempty"`
	RetentionPeriod        int32                   `json:"retentionPeriod"`
	MultiRegionEnabled     bool                    `json:"multiRegionEnabled"`
	OrganizationEnabled    bool                    `json:"organizationEnabled"`
	TerminationProtected   bool                    `json:"terminationProtectionEnabled"`
}

// Query represents a CloudTrail query resource.
//
// QueryResultRows/EventsScanned/EventsMatched/BytesScanned/ExecutionTimeInMillis
// are populated lazily: StartQuery leaves the query QUEUED and unexecuted so
// it stays cancellable (matching AWS's async model), and the first
// GetQueryResults or DescribeQuery call against it runs materializeQueryLocked,
// which executes the recognized SELECT/FROM/WHERE/LIMIT subset of the
// QueryStatement against the backend's recorded events and flips QueryStatus
// to FINISHED. See query_exec.go.
type Query struct {
	CreationTime          time.Time             `json:"creationTime"`
	QueryResultRows       [][]map[string]string `json:"queryResultRows,omitempty"`
	QueryID               string                `json:"queryId"`
	EventDataStoreARN     string                `json:"eventDataStoreArn"`
	QueryString           string                `json:"queryString"`
	QueryStatus           string                `json:"queryStatus"`
	DeliveryS3URI         string                `json:"deliveryS3Uri,omitempty"`
	ErrorMessage          string                `json:"errorMessage,omitempty"`
	QueryAlias            string                `json:"queryAlias,omitempty"`
	EventDataStoreOwnerID string                `json:"eventDataStoreOwnerId,omitempty"`
	DeliveryStatus        string                `json:"deliveryStatus,omitempty"`
	QueryParameters       []string              `json:"queryParameters,omitempty"`
	EventsScanned         int64                 `json:"eventsScanned,omitempty"`
	EventsMatched         int64                 `json:"eventsMatched,omitempty"`
	BytesScanned          int64                 `json:"bytesScanned,omitempty"`
	ExecutionTimeInMillis int32                 `json:"executionTimeInMillis,omitempty"`
}

// ResourcePolicy represents a resource-based policy attached to a CloudTrail resource.
type ResourcePolicy struct {
	ResourceARN    string `json:"resourceArn"`
	ResourcePolicy string `json:"resourcePolicy"`
}

// DataResource represents a resource type for event selector data resources.
type DataResource struct {
	Type   string   `json:"Type"`
	Values []string `json:"Values"`
}

// EventSelector represents a CloudTrail event selector.
type EventSelector struct {
	ReadWriteType           string         `json:"ReadWriteType"`
	DataResources           []DataResource `json:"DataResources"`
	IncludeManagementEvents bool           `json:"IncludeManagementEvents"`
}

// Trail represents an AWS CloudTrail trail.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via AddTags / CreateTrail.
type Trail struct {
	CreationTime               time.Time               `json:"creationTime"`
	StartLoggingTime           *time.Time              `json:"startLoggingTime,omitempty"`
	StopLoggingTime            *time.Time              `json:"stopLoggingTime,omitempty"`
	LatestDeliveryTime         *time.Time              `json:"latestDeliveryTime,omitempty"`
	Tags                       *tags.Tags              `json:"tags,omitempty"`
	KMSKeyID                   string                  `json:"kmsKeyId,omitempty"`
	TrailARN                   string                  `json:"trailArn"`
	S3BucketName               string                  `json:"s3BucketName"`
	S3KeyPrefix                string                  `json:"s3KeyPrefix,omitempty"`
	SnsTopicName               string                  `json:"snsTopicName,omitempty"`
	SnsTopicARN                string                  `json:"snsTopicArn,omitempty"`
	CloudWatchLogsLogGroupARN  string                  `json:"cloudWatchLogsLogGroupArn,omitempty"`
	CloudWatchLogsRoleARN      string                  `json:"cloudWatchLogsRoleArn,omitempty"`
	Region                     string                  `json:"region"`
	Name                       string                  `json:"name"`
	HomeRegion                 string                  `json:"homeRegion"`
	AccountID                  string                  `json:"accountId"`
	EventSelectors             []EventSelector         `json:"eventSelectors,omitempty"`
	AdvancedEventSelectors     []AdvancedEventSelector `json:"advancedEventSelectors,omitempty"`
	InsightSelectors           []InsightSelector       `json:"insightSelectors,omitempty"`
	IncludeGlobalServiceEvents bool                    `json:"includeGlobalServiceEvents"`
	IsMultiRegionTrail         bool                    `json:"isMultiRegionTrail"`
	LogFileValidationEnabled   bool                    `json:"logFileValidationEnabled"`
	IsLogging                  bool                    `json:"isLogging"`
	HasCustomEventSelectors    bool                    `json:"hasCustomEventSelectors"`
	HasInsightSelectors        bool                    `json:"hasInsightSelectors"`
	IsOrganizationTrail        bool                    `json:"isOrganizationTrail"`
}

// Import represents a CloudTrail import resource.
type Import struct {
	CreatedTimestamp time.Time     `json:"createdTimestamp"`
	UpdatedTimestamp time.Time     `json:"updatedTimestamp"`
	ImportSource     *ImportSource `json:"importSource,omitempty"`
	ImportID         string        `json:"importId"`
	ImportStatus     string        `json:"importStatus"`
	Destinations     []string      `json:"destinations,omitempty"`
}

// ImportSource is the S3 source location for a StartImport request, matching
// the real ImportSource{S3: *S3ImportSource} wire shape (S3LocationUri,
// S3BucketRegion, S3BucketAccessRoleArn are all required fields on the real
// S3ImportSource -- previously only S3LocationUri was modeled/echoed).
type ImportSource struct {
	S3 *S3ImportSource `json:"S3,omitempty"`
}

// S3ImportSource is the S3 bucket location and access role for an import.
type S3ImportSource struct {
	S3LocationURI         string `json:"S3LocationUri,omitempty"`
	S3BucketRegion        string `json:"S3BucketRegion,omitempty"`
	S3BucketAccessRoleArn string `json:"S3BucketAccessRoleArn,omitempty"`
}

// InsightSelector represents a CloudTrail insight selector.
type InsightSelector struct {
	InsightType string `json:"InsightType"`
}

// GeneratedQuery holds the result of a GenerateQuery call: a synthesized
// CloudTrail Lake SQL statement (and alias) for a natural-language prompt.
// Unlike StartQuery, GenerateQuery does not create a persisted, runnable
// query record -- AWS only returns the generated statement text.
type GeneratedQuery struct {
	QueryStatement string
	QueryAlias     string
	OwnerAccountID string
}

// EventConfiguration holds the event-aggregation and enriched-context
// settings for a single trail or event data store (keyed by its ARN).
// AggregationConfigurations and ContextKeySelectors are stored as generic
// maps (rather than fully modeled types) because the backend only needs to
// persist and echo back exactly what the caller configured -- CloudTrail
// itself does not evaluate aggregation templates or context key matches.
type EventConfiguration struct {
	MaxEventSize              string           `json:"maxEventSize,omitempty"`
	AggregationConfigurations []map[string]any `json:"aggregationConfigurations,omitempty"`
	ContextKeySelectors       []map[string]any `json:"contextKeySelectors,omitempty"`
}

// LookupEventsInput holds parameters for a LookupEvents call.
type LookupEventsInput struct {
	StartTime        *time.Time
	EndTime          *time.Time
	NextToken        string
	EventCategory    string
	LookupAttributes []LookupAttribute
	MaxResults       int32
}

// LookupEventsOutput holds the result of a LookupEvents call.
type LookupEventsOutput struct {
	NextToken string
	Events    []Event
}
