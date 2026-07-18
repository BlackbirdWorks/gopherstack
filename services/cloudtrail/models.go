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
	Tags         *tags.Tags `json:"tags,omitempty"`
	DashboardID  string     `json:"dashboardId"`
	DashboardARN string     `json:"dashboardArn"`
	Name         string     `json:"name"`
	Type         string     `json:"type"`
	Status       string     `json:"status"`
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
type Query struct {
	CreationTime      time.Time `json:"creationTime"`
	QueryID           string    `json:"queryId"`
	EventDataStoreARN string    `json:"eventDataStoreArn"`
	QueryString       string    `json:"queryString"`
	QueryStatus       string    `json:"queryStatus"`
	DeliveryS3URI     string    `json:"deliveryS3Uri,omitempty"`
	ErrorMessage      string    `json:"errorMessage,omitempty"`
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
	CreatedTimestamp time.Time `json:"createdTimestamp"`
	UpdatedTimestamp time.Time `json:"updatedTimestamp"`
	ImportID         string    `json:"importId"`
	ImportSource     string    `json:"importSource,omitempty"`
	ImportStatus     string    `json:"importStatus"`
	Destinations     []string  `json:"destinations,omitempty"`
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
	LookupAttributes []LookupAttribute
	MaxResults       int32
}

// LookupEventsOutput holds the result of a LookupEvents call.
type LookupEventsOutput struct {
	NextToken string
	Events    []Event
}
