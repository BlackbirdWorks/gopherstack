package pinpoint

import "time"

// Shared domain vocabulary constants used by both the backend (store.go and
// friends) and the HTTP handler (handler.go and friends) to represent
// resource states/types on the wire and in stored records.
const (
	journeyStateActive    = "ACTIVE"
	journeyStatePaused    = "PAUSED"
	journeyStateCancelled = "CANCELLED"
	journeyStateCompleted = "COMPLETED"
	journeyStateClosed    = "CLOSED"
	journeyStateDraft     = "DRAFT"

	jobStatusCreated = "CREATED"

	segmentTypeDimensional = "DIMENSIONAL"
	segmentTypeImport      = "IMPORT"

	campaignStatusScheduled = "SCHEDULED"
	campaignStatusPaused    = "PAUSED"
	campaignStatusCompleted = "COMPLETED"

	templateTypeEmail = "email"
	templateTypeInApp = "inapp"
	templateTypePush  = "push"
	templateTypeSMS   = "sms"
	templateTypeVoice = "voice"
)

// App represents an Amazon Pinpoint application.
type App struct {
	Tags         map[string]string `json:"tags,omitempty"`
	ARN          string            `json:"Arn"`
	ID           string            `json:"Id"`
	Name         string            `json:"Name"`
	CreationDate string            `json:"CreationDate,omitempty"`
}

// Campaign represents a Pinpoint campaign.
type Campaign struct {
	Tags                        map[string]string `json:"tags,omitempty"`
	MessageConfiguration        map[string]any    `json:"MessageConfiguration,omitempty"`
	Schedule                    map[string]any    `json:"Schedule,omitempty"`
	Hook                        map[string]any    `json:"Hook,omitempty"`
	Limits                      map[string]any    `json:"Limits,omitempty"`
	TemplateConfiguration       map[string]any    `json:"TemplateConfiguration,omitempty"`
	CustomDeliveryConfiguration map[string]any    `json:"CustomDeliveryConfiguration,omitempty"`
	ApplicationID               string            `json:"ApplicationId"`
	ARN                         string            `json:"Arn,omitempty"`
	ID                          string            `json:"Id"`
	Name                        string            `json:"Name"`
	SegmentID                   string            `json:"SegmentId,omitempty"`
	TreatmentDescription        string            `json:"TreatmentDescription,omitempty"`
	TreatmentName               string            `json:"TreatmentName,omitempty"`
	CreationDate                string            `json:"CreationDate,omitempty"`
	LastModifiedDate            string            `json:"LastModifiedDate,omitempty"`
	Status                      string            `json:"Status,omitempty"`
	AdditionalTreatments        []map[string]any  `json:"AdditionalTreatments,omitempty"`
	SegmentVersion              int               `json:"SegmentVersion,omitempty"`
	Version                     int               `json:"Version,omitempty"`
	Priority                    int               `json:"Priority,omitempty"`
	IsPaused                    bool              `json:"IsPaused,omitempty"`
}

// EmailTemplate represents a Pinpoint email template.
type EmailTemplate struct {
	ARN                  string            `json:"Arn,omitempty"`
	CreationDate         string            `json:"CreationDate,omitempty"`
	DefaultSubstitutions map[string]any    `json:"DefaultSubstitutions,omitempty"`
	HTMLPart             string            `json:"HtmlPart,omitempty"`
	LastModifiedDate     string            `json:"LastModifiedDate,omitempty"`
	RecommenderID        string            `json:"RecommenderId,omitempty"`
	Subject              string            `json:"Subject,omitempty"`
	Tags                 map[string]string `json:"tags,omitempty"`
	TemplateDescription  string            `json:"TemplateDescription,omitempty"`
	TemplateName         string            `json:"TemplateName"`
	TextPart             string            `json:"TextPart,omitempty"`
	Version              string            `json:"Version,omitempty"`
}

// ExportJob represents a Pinpoint export job.
type ExportJob struct {
	ARN           string `json:"Arn,omitempty"`
	ApplicationID string `json:"ApplicationId"`
	ID            string `json:"Id"`
	RoleArn       string `json:"RoleArn,omitempty"`
	S3UrlPrefix   string `json:"S3UrlPrefix,omitempty"`
	JobStatus     string `json:"JobStatus"`
	CreationDate  string `json:"CreationDate,omitempty"`
}

// ImportJob represents a Pinpoint import job.
type ImportJob struct {
	ARN           string `json:"Arn,omitempty"`
	ApplicationID string `json:"ApplicationId"`
	ID            string `json:"Id"`
	RoleArn       string `json:"RoleArn,omitempty"`
	S3Url         string `json:"S3Url,omitempty"`
	Format        string `json:"Format,omitempty"`
	JobStatus     string `json:"JobStatus"`
	SegmentID     string `json:"SegmentId,omitempty"`
	CreationDate  string `json:"CreationDate,omitempty"`
}

// InAppTemplate represents a Pinpoint in-app template.
type InAppTemplate struct {
	Tags                map[string]string `json:"tags,omitempty"`
	ARN                 string            `json:"Arn,omitempty"`
	CreationDate        string            `json:"CreationDate,omitempty"`
	LastModifiedDate    string            `json:"LastModifiedDate,omitempty"`
	Layout              string            `json:"Layout,omitempty"`
	TemplateDescription string            `json:"TemplateDescription,omitempty"`
	TemplateName        string            `json:"TemplateName"`
	Version             string            `json:"Version,omitempty"`
	Content             []map[string]any  `json:"Content,omitempty"`
}

// Journey represents a Pinpoint journey.
type Journey struct {
	Tags                   map[string]string         `json:"tags,omitempty"`
	Activities             map[string]map[string]any `json:"Activities,omitempty"`
	StartCondition         map[string]any            `json:"StartCondition,omitempty"`
	Schedule               map[string]any            `json:"Schedule,omitempty"`
	Limits                 map[string]any            `json:"Limits,omitempty"`
	QuietTime              map[string]any            `json:"QuietTime,omitempty"`
	OpenHours              map[string]any            `json:"OpenHours,omitempty"`
	ClosedDays             map[string]any            `json:"ClosedDays,omitempty"`
	ApplicationID          string                    `json:"ApplicationId"`
	ARN                    string                    `json:"Arn,omitempty"`
	ID                     string                    `json:"Id"`
	Name                   string                    `json:"Name"`
	StartActivity          string                    `json:"StartActivity,omitempty"`
	RefreshFrequency       string                    `json:"RefreshFrequency,omitempty"`
	CreationDate           string                    `json:"CreationDate,omitempty"`
	LastModifiedDate       string                    `json:"LastModifiedDate,omitempty"`
	State                  string                    `json:"State"`
	LocalTime              bool                      `json:"LocalTime,omitempty"`
	WaitForQuietTime       bool                      `json:"WaitForQuietTime,omitempty"`
	RefreshOnSegmentUpdate bool                      `json:"RefreshOnSegmentUpdate,omitempty"`
}

// PushTemplate represents a Pinpoint push notification template.
type PushTemplate struct {
	APNS                map[string]any    `json:"APNS,omitempty"`
	Default             map[string]any    `json:"Default,omitempty"`
	GCM                 map[string]any    `json:"GCM,omitempty"`
	Tags                map[string]string `json:"tags,omitempty"`
	ARN                 string            `json:"Arn,omitempty"`
	Body                string            `json:"Body,omitempty"`
	CreationDate        string            `json:"CreationDate,omitempty"`
	LastModifiedDate    string            `json:"LastModifiedDate,omitempty"`
	TemplateDescription string            `json:"TemplateDescription,omitempty"`
	TemplateName        string            `json:"TemplateName"`
	Title               string            `json:"Title,omitempty"`
	Version             string            `json:"Version,omitempty"`
}

// RecommenderConfiguration represents a Pinpoint recommender configuration.
type RecommenderConfiguration struct {
	Attributes                    map[string]string `json:"Attributes,omitempty"`
	ID                            string            `json:"Id"`
	Name                          string            `json:"Name"`
	Description                   string            `json:"Description,omitempty"`
	RecommendationProviderIDType  string            `json:"RecommendationProviderIdType,omitempty"`
	RecommendationProviderRoleARN string            `json:"RecommendationProviderRoleArn,omitempty"`
	RecommendationProviderURI     string            `json:"RecommendationProviderUri,omitempty"`
	CreationDate                  string            `json:"CreationDate,omitempty"`
	LastModifiedDate              string            `json:"LastModifiedDate,omitempty"`
	RecommendationsPerMessage     int               `json:"RecommendationsPerMessage,omitempty"`
}

// Segment represents a Pinpoint segment.
type Segment struct {
	Tags             map[string]string `json:"tags,omitempty"`
	Dimensions       map[string]any    `json:"Dimensions,omitempty"`
	SegmentGroups    map[string]any    `json:"SegmentGroups,omitempty"`
	ImportDefinition map[string]any    `json:"ImportDefinition,omitempty"`
	ApplicationID    string            `json:"ApplicationId"`
	ARN              string            `json:"Arn,omitempty"`
	ID               string            `json:"Id"`
	Name             string            `json:"Name"`
	CreationDate     string            `json:"CreationDate,omitempty"`
	LastModifiedDate string            `json:"LastModifiedDate,omitempty"`
	SegmentType      string            `json:"SegmentType"`
	Version          int               `json:"Version,omitempty"`
}

// SmsTemplate represents a Pinpoint SMS template.
type SmsTemplate struct {
	ARN                 string            `json:"Arn,omitempty"`
	Body                string            `json:"Body,omitempty"`
	CreationDate        string            `json:"CreationDate,omitempty"`
	LastModifiedDate    string            `json:"LastModifiedDate,omitempty"`
	SenderID            string            `json:"SenderId,omitempty"`
	Tags                map[string]string `json:"tags,omitempty"`
	TemplateDescription string            `json:"TemplateDescription,omitempty"`
	TemplateName        string            `json:"TemplateName"`
	Version             string            `json:"Version,omitempty"`
}

// nowRFC3339 returns the current UTC time formatted as RFC 3339.
func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}
