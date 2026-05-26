package pinpoint

import "time"

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

// ──────────────────────────────────────────────────
// Wire-format request types
// ──────────────────────────────────────────────────

// createAppRequest is the request body for creating a Pinpoint app.
type createAppRequest struct {
	Tags map[string]string `json:"tags,omitempty"`
	Name string            `json:"Name"`
}

// createCampaignRequest is the request body for CreateCampaign.
type createCampaignRequest struct {
	Tags                        map[string]string `json:"tags,omitempty"`
	MessageConfiguration        map[string]any    `json:"MessageConfiguration,omitempty"`
	Schedule                    map[string]any    `json:"Schedule,omitempty"`
	Hook                        map[string]any    `json:"Hook,omitempty"`
	Limits                      map[string]any    `json:"Limits,omitempty"`
	TemplateConfiguration       map[string]any    `json:"TemplateConfiguration,omitempty"`
	CustomDeliveryConfiguration map[string]any    `json:"CustomDeliveryConfiguration,omitempty"`
	Name                        string            `json:"Name"`
	SegmentID                   string            `json:"SegmentId,omitempty"`
	TreatmentDescription        string            `json:"TreatmentDescription,omitempty"`
	TreatmentName               string            `json:"TreatmentName,omitempty"`
	AdditionalTreatments        []map[string]any  `json:"AdditionalTreatments,omitempty"`
	SegmentVersion              int               `json:"SegmentVersion,omitempty"`
	Priority                    int               `json:"Priority,omitempty"`
	IsPaused                    bool              `json:"IsPaused,omitempty"`
}

// createEmailTemplateRequest is the request body for CreateEmailTemplate.
type createEmailTemplateRequest struct {
	DefaultSubstitutions map[string]any    `json:"DefaultSubstitutions,omitempty"`
	Tags                 map[string]string `json:"tags,omitempty"`
	HTMLPart             string            `json:"HtmlPart,omitempty"`
	RecommenderID        string            `json:"RecommenderId,omitempty"`
	Subject              string            `json:"Subject,omitempty"`
	TemplateDescription  string            `json:"TemplateDescription,omitempty"`
	TextPart             string            `json:"TextPart,omitempty"`
}

// createExportJobRequest is the request body for CreateExportJob.
type createExportJobRequest struct {
	RoleArn     string `json:"RoleArn"`
	S3UrlPrefix string `json:"S3UrlPrefix"`
}

// createImportJobRequest is the request body for CreateImportJob.
type createImportJobRequest struct {
	RoleArn     string `json:"RoleArn"`
	Format      string `json:"Format"`
	S3Url       string `json:"S3Url"`
	SegmentName string `json:"SegmentName,omitempty"`
}

// createInAppTemplateRequest is the request body for CreateInAppTemplate.
type createInAppTemplateRequest struct {
	Tags                map[string]string `json:"tags,omitempty"`
	Layout              string            `json:"Layout,omitempty"`
	TemplateDescription string            `json:"TemplateDescription,omitempty"`
	Content             []map[string]any  `json:"Content,omitempty"`
}

// createJourneyRequest is the request body for CreateJourney.
type createJourneyRequest struct {
	Tags                   map[string]string         `json:"tags,omitempty"`
	Activities             map[string]map[string]any `json:"Activities,omitempty"`
	StartCondition         map[string]any            `json:"StartCondition,omitempty"`
	Schedule               map[string]any            `json:"Schedule,omitempty"`
	Limits                 map[string]any            `json:"Limits,omitempty"`
	QuietTime              map[string]any            `json:"QuietTime,omitempty"`
	OpenHours              map[string]any            `json:"OpenHours,omitempty"`
	ClosedDays             map[string]any            `json:"ClosedDays,omitempty"`
	Name                   string                    `json:"Name"`
	StartActivity          string                    `json:"StartActivity,omitempty"`
	RefreshFrequency       string                    `json:"RefreshFrequency,omitempty"`
	LocalTime              bool                      `json:"LocalTime,omitempty"`
	WaitForQuietTime       bool                      `json:"WaitForQuietTime,omitempty"`
	RefreshOnSegmentUpdate bool                      `json:"RefreshOnSegmentUpdate,omitempty"`
}

// createPushTemplateRequest is the request body for CreatePushTemplate.
type createPushTemplateRequest struct {
	APNS                map[string]any    `json:"APNS,omitempty"`
	Default             map[string]any    `json:"Default,omitempty"`
	GCM                 map[string]any    `json:"GCM,omitempty"`
	Tags                map[string]string `json:"tags,omitempty"`
	Body                string            `json:"Body,omitempty"`
	TemplateDescription string            `json:"TemplateDescription,omitempty"`
	Title               string            `json:"Title,omitempty"`
}

// createRecommenderConfigRequest is the request body for CreateRecommenderConfiguration.
type createRecommenderConfigRequest struct {
	Attributes                    map[string]string `json:"Attributes,omitempty"`
	Name                          string            `json:"Name"`
	Description                   string            `json:"Description,omitempty"`
	RecommendationProviderIDType  string            `json:"RecommendationProviderIdType,omitempty"`
	RecommendationProviderRoleArn string            `json:"RecommendationProviderRoleArn,omitempty"`
	RecommendationProviderURI     string            `json:"RecommendationProviderUri,omitempty"`
	RecommendationsPerMessage     int               `json:"RecommendationsPerMessage,omitempty"`
}

// createSegmentRequest is the request body for CreateSegment.
type createSegmentRequest struct {
	Tags             map[string]string `json:"tags,omitempty"`
	Dimensions       map[string]any    `json:"Dimensions,omitempty"`
	SegmentGroups    map[string]any    `json:"SegmentGroups,omitempty"`
	ImportDefinition map[string]any    `json:"ImportDefinition,omitempty"`
	Name             string            `json:"Name"`
}

// createSmsTemplateRequest is the request body for CreateSmsTemplate.
type createSmsTemplateRequest struct {
	Body                string            `json:"Body,omitempty"`
	SenderID            string            `json:"SenderId,omitempty"`
	Tags                map[string]string `json:"tags,omitempty"`
	TemplateDescription string            `json:"TemplateDescription,omitempty"`
}

// ──────────────────────────────────────────────────
// Wire-format response types
// ──────────────────────────────────────────────────

// campaignState is the State sub-object in a CampaignResponse.
type campaignState struct {
	CampaignStatus string `json:"CampaignStatus"`
}

// campaignResponse is the JSON wire format of CampaignResponse.
type campaignResponse struct {
	Tags                        map[string]string `json:"tags,omitempty"`
	MessageConfiguration        map[string]any    `json:"MessageConfiguration,omitempty"`
	Schedule                    map[string]any    `json:"Schedule,omitempty"`
	Hook                        map[string]any    `json:"Hook,omitempty"`
	Limits                      map[string]any    `json:"Limits,omitempty"`
	TemplateConfiguration       map[string]any    `json:"TemplateConfiguration,omitempty"`
	CustomDeliveryConfiguration map[string]any    `json:"CustomDeliveryConfiguration,omitempty"`
	Name                        string            `json:"Name"`
	CreationDate                string            `json:"CreationDate,omitempty"`
	ID                          string            `json:"Id"`
	ApplicationID               string            `json:"ApplicationId"`
	SegmentID                   string            `json:"SegmentId,omitempty"`
	TreatmentDescription        string            `json:"TreatmentDescription,omitempty"`
	TreatmentName               string            `json:"TreatmentName,omitempty"`
	ARN                         string            `json:"Arn,omitempty"`
	LastModifiedDate            string            `json:"LastModifiedDate,omitempty"`
	State                       campaignState     `json:"State"`
	AdditionalTreatments        []map[string]any  `json:"AdditionalTreatments,omitempty"`
	Version                     int               `json:"Version,omitempty"`
	SegmentVersion              int               `json:"SegmentVersion,omitempty"`
	Priority                    int               `json:"Priority,omitempty"`
	IsPaused                    bool              `json:"IsPaused,omitempty"`
}

// createTemplateMessageBody is the create-template response returned by
// CreateEmailTemplate, CreatePushTemplate, CreateSmsTemplate, and CreateInAppTemplate.
type createTemplateMessageBody struct {
	ARN       string `json:"Arn,omitempty"`
	Message   string `json:"Message"`
	RequestID string `json:"RequestID,omitempty"`
}

// exportJobResponse is the JSON wire format of ExportJobResponse.
type exportJobResponse struct {
	ARN           string `json:"Arn,omitempty"`
	ApplicationID string `json:"ApplicationId"`
	ID            string `json:"Id"`
	RoleArn       string `json:"RoleArn,omitempty"`
	S3UrlPrefix   string `json:"S3UrlPrefix,omitempty"`
	JobStatus     string `json:"JobStatus"`
	Type          string `json:"Type"`
	CreationDate  string `json:"CreationDate,omitempty"`
}

// importJobResponse is the JSON wire format of ImportJobResponse.
type importJobResponse struct {
	ARN           string `json:"Arn,omitempty"`
	ApplicationID string `json:"ApplicationId"`
	ID            string `json:"Id"`
	RoleArn       string `json:"RoleArn,omitempty"`
	S3Url         string `json:"S3Url,omitempty"`
	Format        string `json:"Format,omitempty"`
	JobStatus     string `json:"JobStatus"`
	Type          string `json:"Type"`
	CreationDate  string `json:"CreationDate,omitempty"`
}

// journeyResponse is the JSON wire format of JourneyResponse.
type journeyResponse struct {
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
	State                  string                    `json:"State"`
	CreationDate           string                    `json:"CreationDate,omitempty"`
	LastModifiedDate       string                    `json:"LastModifiedDate,omitempty"`
	LocalTime              bool                      `json:"LocalTime,omitempty"`
	WaitForQuietTime       bool                      `json:"WaitForQuietTime,omitempty"`
	RefreshOnSegmentUpdate bool                      `json:"RefreshOnSegmentUpdate,omitempty"`
}

// recommenderConfigResponse is the JSON wire format of RecommenderConfigurationResponse.
type recommenderConfigResponse struct {
	Attributes                    map[string]string `json:"Attributes,omitempty"`
	ID                            string            `json:"Id"`
	Name                          string            `json:"Name"`
	Description                   string            `json:"Description,omitempty"`
	RecommendationProviderIDType  string            `json:"RecommendationProviderIdType,omitempty"`
	RecommendationProviderRoleArn string            `json:"RecommendationProviderRoleArn,omitempty"`
	RecommendationProviderURI     string            `json:"RecommendationProviderUri,omitempty"`
	CreationDate                  string            `json:"CreationDate,omitempty"`
	LastModifiedDate              string            `json:"LastModifiedDate,omitempty"`
	RecommendationsPerMessage     int               `json:"RecommendationsPerMessage,omitempty"`
}

// segmentResponse is the JSON wire format of SegmentResponse.
type segmentResponse struct {
	Tags             map[string]string `json:"tags,omitempty"`
	Dimensions       map[string]any    `json:"Dimensions,omitempty"`
	SegmentGroups    map[string]any    `json:"SegmentGroups,omitempty"`
	ImportDefinition map[string]any    `json:"ImportDefinition,omitempty"`
	ApplicationID    string            `json:"ApplicationId"`
	ARN              string            `json:"Arn,omitempty"`
	ID               string            `json:"Id"`
	Name             string            `json:"Name"`
	SegmentType      string            `json:"SegmentType"`
	CreationDate     string            `json:"CreationDate,omitempty"`
	LastModifiedDate string            `json:"LastModifiedDate,omitempty"`
	Version          int               `json:"Version,omitempty"`
}

// appResponse is the JSON wire format of ApplicationResponse.
type appResponse struct {
	Tags         map[string]string `json:"tags,omitempty"`
	ARN          string            `json:"Arn"`
	ID           string            `json:"Id"`
	Name         string            `json:"Name"`
	CreationDate string            `json:"CreationDate,omitempty"`
}

// appsResponse is the JSON wire format of ApplicationsResponse (GetApps).
type appsResponse struct {
	NextToken *string       `json:"NextToken,omitempty"`
	Item      []appResponse `json:"Item"`
}

// tagsModel is the JSON wire format of TagsModel.
type tagsModel struct {
	Tags map[string]string `json:"tags"`
}

// tagResourceRequest is the request body for TagResource.
type tagResourceRequest struct {
	Tags map[string]string `json:"tags"`
}

// appSettingsResponse is the JSON wire format of ApplicationSettingsResource.
// CampaignHook, Limits, and QuietTime must be non-nil empty objects so the
// Terraform provider's flatten helpers do not dereference nil pointers.
type appSettingsResponse struct {
	CampaignHook             map[string]any `json:"CampaignHook"`
	Limits                   map[string]any `json:"Limits"`
	QuietTime                map[string]any `json:"QuietTime"`
	ApplicationID            string         `json:"ApplicationId"`
	LastModifiedDate         string         `json:"LastModifiedDate,omitempty"`
	CloudWatchMetricsEnabled bool           `json:"CloudWatchMetricsEnabled"`
	EventTaggingEnabled      bool           `json:"EventTaggingEnabled"`
}

// nowRFC3339 returns the current UTC time formatted as RFC 3339.
func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// ──────────────────────────────────────────────────
// New request types for additional operations
// ──────────────────────────────────────────────────

// createVoiceTemplateRequest is the request body for CreateVoiceTemplate.
type createVoiceTemplateRequest struct {
	Tags map[string]string `json:"tags,omitempty"`
	Body string            `json:"Body,omitempty"`
}

// updateCampaignRequest is the request body for UpdateCampaign.
type updateCampaignRequest struct {
	Tags                        map[string]string `json:"tags,omitempty"`
	MessageConfiguration        map[string]any    `json:"MessageConfiguration,omitempty"`
	Schedule                    map[string]any    `json:"Schedule,omitempty"`
	Hook                        map[string]any    `json:"Hook,omitempty"`
	Limits                      map[string]any    `json:"Limits,omitempty"`
	TemplateConfiguration       map[string]any    `json:"TemplateConfiguration,omitempty"`
	CustomDeliveryConfiguration map[string]any    `json:"CustomDeliveryConfiguration,omitempty"`
	Name                        string            `json:"Name,omitempty"`
	SegmentID                   string            `json:"SegmentId,omitempty"`
	TreatmentDescription        string            `json:"TreatmentDescription,omitempty"`
	TreatmentName               string            `json:"TreatmentName,omitempty"`
	AdditionalTreatments        []map[string]any  `json:"AdditionalTreatments,omitempty"`
	SegmentVersion              int               `json:"SegmentVersion,omitempty"`
	Priority                    int               `json:"Priority,omitempty"`
	IsPaused                    bool              `json:"IsPaused,omitempty"`
}

// updateSegmentRequest is the request body for UpdateSegment.
type updateSegmentRequest struct {
	Tags             map[string]string `json:"tags,omitempty"`
	Dimensions       map[string]any    `json:"Dimensions,omitempty"`
	SegmentGroups    map[string]any    `json:"SegmentGroups,omitempty"`
	ImportDefinition map[string]any    `json:"ImportDefinition,omitempty"`
	Name             string            `json:"Name,omitempty"`
}

// updateJourneyRequest is the request body for UpdateJourney.
type updateJourneyRequest struct {
	Tags                   map[string]string         `json:"tags,omitempty"`
	Activities             map[string]map[string]any `json:"Activities,omitempty"`
	StartCondition         map[string]any            `json:"StartCondition,omitempty"`
	Schedule               map[string]any            `json:"Schedule,omitempty"`
	Limits                 map[string]any            `json:"Limits,omitempty"`
	QuietTime              map[string]any            `json:"QuietTime,omitempty"`
	OpenHours              map[string]any            `json:"OpenHours,omitempty"`
	ClosedDays             map[string]any            `json:"ClosedDays,omitempty"`
	Name                   string                    `json:"Name,omitempty"`
	StartActivity          string                    `json:"StartActivity,omitempty"`
	RefreshFrequency       string                    `json:"RefreshFrequency,omitempty"`
	LocalTime              bool                      `json:"LocalTime,omitempty"`
	WaitForQuietTime       bool                      `json:"WaitForQuietTime,omitempty"`
	RefreshOnSegmentUpdate bool                      `json:"RefreshOnSegmentUpdate,omitempty"`
}

// updateJourneyStateRequest is the request body for UpdateJourneyState.
type updateJourneyStateRequest struct {
	State string `json:"State"`
}

// updateEndpointRequest is the request body for UpdateEndpoint.
type updateEndpointRequest struct {
	User           endpointUser        `json:"User"`
	Attributes     map[string][]string `json:"Attributes,omitempty"`
	Metrics        map[string]float64  `json:"Metrics,omitempty"`
	Demographic    map[string]any      `json:"Demographic,omitempty"`
	Location       map[string]any      `json:"Location,omitempty"`
	ChannelType    string              `json:"ChannelType,omitempty"`
	Address        string              `json:"Address,omitempty"`
	EffectiveDate  string              `json:"EffectiveDate,omitempty"`
	EndpointStatus string              `json:"EndpointStatus,omitempty"`
	OptOut         string              `json:"OptOut,omitempty"`
	RequestID      string              `json:"RequestId,omitempty"`
}

// endpointUser is a sub-object in updateEndpointRequest.
type endpointUser struct {
	UserAttributes map[string][]string `json:"UserAttributes,omitempty"`
	UserID         string              `json:"UserId,omitempty"`
}

// updateEndpointsBatchRequest is the request body for UpdateEndpointsBatch.
type updateEndpointsBatchRequest struct {
	Item map[string]updateEndpointRequest `json:"Item"`
}

// putEventStreamRequest is the request body for PutEventStream.
type putEventStreamRequest struct {
	DestinationStreamArn string `json:"DestinationStreamArn"`
	RoleArn              string `json:"RoleArn"`
}

// updateChannelRequest is a generic channel update request body.
type updateChannelRequest struct {
	Enabled bool `json:"Enabled"`
}

// updateGCMChannelRequest is the request body for UpdateGcmChannel.
type updateGCMChannelRequest struct {
	APIKey                      string `json:"ApiKey,omitempty"`
	ServiceJSON                 string `json:"ServiceJson,omitempty"`
	DefaultAuthenticationMethod string `json:"DefaultAuthenticationMethod,omitempty"`
	Enabled                     bool   `json:"Enabled"`
}

// updateAPNSChannelRequest is the request body for UpdateApnsChannel (and sandbox/voip variants).
type updateAPNSChannelRequest struct {
	BundleID          string `json:"BundleId,omitempty"`
	Certificate       string `json:"Certificate,omitempty"`
	DefaultAuthMethod string `json:"DefaultAuthMethod,omitempty"`
	PrivateKey        string `json:"PrivateKey,omitempty"`
	TeamID            string `json:"TeamId,omitempty"`
	TokenKey          string `json:"TokenKey,omitempty"`
	TokenKeyID        string `json:"TokenKeyId,omitempty"`
	Enabled           bool   `json:"Enabled"`
}

// updateEmailChannelRequest is the request body for UpdateEmailChannel.
type updateEmailChannelRequest struct {
	ConfigurationSet string `json:"ConfigurationSet,omitempty"`
	FromAddress      string `json:"FromAddress,omitempty"`
	Identity         string `json:"Identity,omitempty"`
	RoleArn          string `json:"RoleArn,omitempty"`
	Enabled          bool   `json:"Enabled"`
}

// updateSMSChannelRequest is the request body for UpdateSmsChannel.
type updateSMSChannelRequest struct {
	SenderID  string `json:"SenderId,omitempty"`
	ShortCode string `json:"ShortCode,omitempty"`
	Enabled   bool   `json:"Enabled"`
}

// updateADMChannelRequest is the request body for UpdateAdmChannel.
type updateADMChannelRequest struct {
	ClientID     string `json:"ClientId,omitempty"`
	ClientSecret string `json:"ClientSecret,omitempty"`
	Enabled      bool   `json:"Enabled"`
}

// updateBaiduChannelRequest is the request body for UpdateBaiduChannel.
type updateBaiduChannelRequest struct {
	APIKey    string `json:"ApiKey,omitempty"`
	SecretKey string `json:"SecretKey,omitempty"`
	Enabled   bool   `json:"Enabled"`
}

// sendMessagesRequest is the request body for SendMessages.
type sendMessagesRequest struct {
	MessageRequest struct {
		Addresses map[string]addressConfig `json:"Addresses,omitempty"`
	} `json:"MessageRequest"`
}

// addressConfig is a per-address configuration in a SendMessages request.
type addressConfig struct {
	ChannelType string `json:"ChannelType,omitempty"`
}

// putEventsRequest is the request body for PutEvents.
type putEventsRequest struct {
	EventsRequest struct {
		BatchItem map[string]endpointEvents `json:"BatchItem,omitempty"`
	} `json:"EventsRequest"`
}

// endpointEvents is a per-endpoint event batch in PutEvents.
type endpointEvents struct {
	Events   map[string]eventItem `json:"Events,omitempty"`
	Endpoint endpointEventItem    `json:"Endpoint"`
}

// endpointEventItem is the endpoint portion of a PutEvents request.
type endpointEventItem struct {
	ChannelType string `json:"ChannelType,omitempty"`
}

// eventItem is a single event in a PutEvents request.
type eventItem struct {
	EventType string `json:"EventType,omitempty"`
	Timestamp string `json:"Timestamp,omitempty"`
}

// phoneNumberValidateRequest is the request body for PhoneNumberValidate.
type phoneNumberValidateRequest struct {
	NumberValidateRequest struct {
		PhoneNumber string `json:"PhoneNumber"`
		IsoCountry  string `json:"IsoCountryCode,omitempty"`
	} `json:"NumberValidateRequest"`
}

// ──────────────────────────────────────────────────
// New response types for additional operations
// ──────────────────────────────────────────────────

// voiceTemplateResponse is the JSON wire format of VoiceTemplateResponse.
type voiceTemplateResponse struct {
	Tags         map[string]string `json:"tags,omitempty"`
	ARN          string            `json:"Arn,omitempty"`
	TemplateName string            `json:"TemplateName"`
	Body         string            `json:"Body,omitempty"`
	CreationDate string            `json:"CreationDate,omitempty"`
}

// templateListItem is one entry in the ListTemplates response.
type templateListItem struct {
	TemplateName string `json:"TemplateName"`
	TemplateType string `json:"TemplateType"`
	ARN          string `json:"Arn,omitempty"`
	CreationDate string `json:"CreationDate,omitempty"`
}

// templateVersionItem is one entry in the ListTemplateVersions response.
type templateVersionItem struct {
	TemplateName    string `json:"TemplateName"`
	TemplateType    string `json:"TemplateType"`
	TemplateVersion string `json:"Version"`
}

// channelResponse is the JSON wire format of a channel response.
type channelResponse struct {
	ExtraFields       map[string]any `json:"-"`
	ApplicationID     string         `json:"ApplicationId"`
	ChannelType       string         `json:"ChannelType"`
	Platform          string         `json:"Platform,omitempty"`
	CreationDate      string         `json:"CreationDate,omitempty"`
	LastModifiedDate  string         `json:"LastModifiedDate,omitempty"`
	Version           int            `json:"Version,omitempty"`
	MessagesPerSecond int            `json:"MessagesPerSecond,omitempty"`
	Enabled           bool           `json:"Enabled"`
	IsArchived        bool           `json:"IsArchived"`
	HasCredential     bool           `json:"HasCredential,omitempty"`
	HasTokenKey       bool           `json:"HasTokenKey,omitempty"`
}

// channelsResponse is the JSON wire format of GetChannels response.
type channelsResponse struct {
	Channels map[string]channelResponse `json:"Channels"`
}

// endpointUser response embeds user info in endpoint responses.
type endpointUserResponse struct {
	UserAttributes map[string][]string `json:"UserAttributes,omitempty"`
	UserID         string              `json:"UserId,omitempty"`
}

// endpointResponse is the JSON wire format of an endpoint.
type endpointResponse struct {
	Attributes     map[string][]string  `json:"Attributes,omitempty"`
	Metrics        map[string]float64   `json:"Metrics,omitempty"`
	Demographic    map[string]any       `json:"Demographic,omitempty"`
	Location       map[string]any       `json:"Location,omitempty"`
	User           endpointUserResponse `json:"User"`
	ApplicationID  string               `json:"ApplicationId"`
	ID             string               `json:"Id"`
	CohortID       string               `json:"CohortId,omitempty"`
	ChannelType    string               `json:"ChannelType,omitempty"`
	Address        string               `json:"Address,omitempty"`
	EffectiveDate  string               `json:"EffectiveDate,omitempty"`
	CreationDate   string               `json:"CreationDate,omitempty"`
	EndpointStatus string               `json:"EndpointStatus,omitempty"`
	OptOut         string               `json:"OptOut,omitempty"`
	RequestID      string               `json:"RequestId,omitempty"`
}

// endpointsResponse is the JSON wire format of EndpointsResponse.
type endpointsResponse struct {
	Item []endpointResponse `json:"Item"`
}

// eventStreamResponse is the JSON wire format of EventStreamResponse.
type eventStreamResponse struct {
	ApplicationID        string `json:"ApplicationId"`
	DestinationStreamArn string `json:"DestinationStreamArn"`
	RoleArn              string `json:"RoleArn"`
	LastModifiedDate     string `json:"LastModifiedDate,omitempty"`
}

// campaignActivitiesResponse is the JSON wire format of ActivitiesResponse.
type campaignActivitiesResponse struct {
	Item []campaignActivity `json:"Item"`
}

// campaignActivity is a single campaign activity.
type campaignActivity struct {
	ApplicationID string `json:"ApplicationId"`
	CampaignID    string `json:"CampaignId"`
	ID            string `json:"Id"`
}

// campaignsListResponse is the JSON wire format of CampaignsResponse.
type campaignsListResponse struct {
	Item []campaignResponse `json:"Item"`
}

// segmentsListResponse is the JSON wire format of SegmentsResponse.
type segmentsListResponse struct {
	Item []segmentResponse `json:"Item"`
}

// journeysListResponse is the JSON wire format of JourneysResponse.
type journeysListResponse struct {
	Item []journeyResponse `json:"Item"`
}

// kpiResult is the KPI response structure.
type kpiResult struct {
	ApplicationID string  `json:"ApplicationId"`
	CampaignID    string  `json:"CampaignId,omitempty"`
	JourneyID     string  `json:"JourneyId,omitempty"`
	KpiName       string  `json:"KpiName"`
	KpiResult     kpiRows `json:"KpiResult"`
}

// kpiRows is the rows object in KPI responses.
type kpiRows struct {
	Rows []kpiRow `json:"Rows"`
}

// kpiRow is a single KPI data row.
type kpiRow struct {
	GroupedBys []kpiGroupedBy  `json:"GroupedBys,omitempty"`
	Values     []kpiResultItem `json:"Values,omitempty"`
}

// kpiGroupedBy is a grouping key in a KPI row.
type kpiGroupedBy struct {
	Key   string `json:"Key"`
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

// kpiResultItem is a value in a KPI row.
type kpiResultItem struct {
	Key   string `json:"Key"`
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

// messageResponse is the JSON wire format of MessageResponse.
type messageResponse struct {
	Result map[string]messageResult `json:"Result"`
}

// messageResult is a per-address message result.
type messageResult struct {
	DeliveryStatus string `json:"DeliveryStatus"`
	MessageID      string `json:"MessageId,omitempty"`
	StatusCode     int    `json:"StatusCode"`
}

// usersMessageResponse is the JSON wire format of SendUsersMessageResponse.
type usersMessageResponse struct {
	Result map[string]map[string]messageResult `json:"Result"`
}

// sendOTPMessageResponse is the response for SendOTPMessage.
type sendOTPMessageResponse struct {
	MessageResponse messageResponse `json:"MessageResponse"`
}

// verifyOTPMessageResponse is the response for VerifyOTPMessage.
type verifyOTPMessageResponse struct {
	Valid bool `json:"Valid"`
}

// phoneNumberValidateResponse is the response for PhoneNumberValidate.
type phoneNumberValidateResponse struct {
	NumberValidateResponse numberValidateResponse `json:"NumberValidateResponse"`
}

// numberValidateResponse is the inner response for PhoneNumberValidate.
type numberValidateResponse struct {
	Carrier                 string `json:"Carrier,omitempty"`
	PhoneType               string `json:"PhoneType,omitempty"`
	CleansedPhoneNumberE164 string `json:"CleansedPhoneNumberE164,omitempty"`
	PhoneTypeCode           int    `json:"PhoneTypeCode"`
}

// attributesResource is the response for RemoveAttributes.
type attributesResource struct {
	ApplicationID string   `json:"ApplicationId"`
	AttributeType string   `json:"AttributeType"`
	Attributes    []string `json:"Attributes"`
}

// inAppMessagesResponse is the response for GetInAppMessages.
type inAppMessagesResponse struct {
	InAppMessageCampaigns []inAppMessageCampaign `json:"InAppMessageCampaigns"`
}

// inAppMessageCampaign is a single in-app message campaign.
type inAppMessageCampaign struct {
	CampaignID string `json:"CampaignId"`
}

// journeyExecutionMetricsResponse is the response for GetJourneyExecutionMetrics.
type journeyExecutionMetricsResponse struct {
	Metrics       map[string]string `json:"Metrics"`
	ApplicationID string            `json:"ApplicationId"`
	JourneyID     string            `json:"JourneyId"`
}

// journeyExecutionActivityMetricsResponse is the response for GetJourneyExecutionActivityMetrics.
type journeyExecutionActivityMetricsResponse struct {
	Metrics       map[string]string `json:"Metrics"`
	ApplicationID string            `json:"ApplicationId"`
	JourneyID     string            `json:"JourneyId"`
	ActivityID    string            `json:"ActivityId"`
}

// journeyRunsResponse is the response for GetJourneyRuns.
type journeyRunsResponse struct {
	Item []journeyRun `json:"Item"`
}

// journeyRun is a single journey run.
type journeyRun struct {
	RunID         string `json:"RunId"`
	JourneyID     string `json:"JourneyId"`
	ApplicationID string `json:"ApplicationId"`
	Status        string `json:"Status"`
}

// journeyRunExecutionMetricsResponse is the response for GetJourneyRunExecutionMetrics.
type journeyRunExecutionMetricsResponse struct {
	Metrics       map[string]string `json:"Metrics"`
	ApplicationID string            `json:"ApplicationId"`
	JourneyID     string            `json:"JourneyId"`
	RunID         string            `json:"RunId"`
}

// journeyRunExecutionActivityMetricsResponse is the response for GetJourneyRunExecutionActivityMetrics.
type journeyRunExecutionActivityMetricsResponse struct {
	Metrics       map[string]string `json:"Metrics"`
	ApplicationID string            `json:"ApplicationId"`
	JourneyID     string            `json:"JourneyId"`
	RunID         string            `json:"RunId"`
	ActivityID    string            `json:"ActivityId"`
}

// templatesListResponse is the JSON wire format of TemplatesResponse (ListTemplates).
type templatesListResponse struct {
	Item []templateListItem `json:"Item"`
}

// templateVersionsListResponse is the JSON wire format of TemplateVersionsResponse.
type templateVersionsListResponse struct {
	Item []templateVersionItem `json:"Item"`
}

// recommenderConfigsListResponse is the JSON wire format of ListRecommenderConfigurationsResponse.
type recommenderConfigsListResponse struct {
	Item []recommenderConfigResponse `json:"Item"`
}

// exportJobsListResponse is the JSON wire format of ExportJobsResponse.
type exportJobsListResponse struct {
	Item []exportJobResponse `json:"Item"`
}

// importJobsListResponse is the JSON wire format of ImportJobsResponse.
type importJobsListResponse struct {
	Item []importJobResponse `json:"Item"`
}

// campaignVersionsResponse is the JSON wire format of CampaignVersionsResponse.
type campaignVersionsResponse struct {
	Item []campaignResponse `json:"Item"`
}

// segmentVersionsResponse is the JSON wire format of SegmentVersionsResponse.
type segmentVersionsResponse struct {
	Item []segmentResponse `json:"Item"`
}

// messageBodyResponse is a simple message/arn response for update ops.
type messageBodyResponse struct {
	Message string `json:"Message"`
	ARN     string `json:"Arn,omitempty"`
}

// storedPinpointEvent is a single persisted event from PutEvents.
type storedPinpointEvent struct {
	EventType string `json:"EventType"`
	Timestamp string `json:"Timestamp"`
}
