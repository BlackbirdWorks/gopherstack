package pinpoint

import "time"

// App represents an Amazon Pinpoint application.
type App struct {
	Tags         map[string]string
	ARN          string
	ID           string
	Name         string
	CreationDate string
}

// Campaign represents a Pinpoint campaign.
type Campaign struct {
	Tags             map[string]string
	ApplicationID    string
	ARN              string
	ID               string
	Name             string
	SegmentID        string
	CreationDate     string
	LastModifiedDate string
	SegmentVersion   int
}

// EmailTemplate represents a Pinpoint email template.
type EmailTemplate struct {
	Tags         map[string]string
	ARN          string
	TemplateName string
	CreationDate string
}

// ExportJob represents a Pinpoint export job.
type ExportJob struct {
	ApplicationID string
	ID            string
	JobStatus     string
	CreationDate  string
}

// ImportJob represents a Pinpoint import job.
type ImportJob struct {
	ApplicationID string
	ID            string
	JobStatus     string
	CreationDate  string
}

// InAppTemplate represents a Pinpoint in-app template.
type InAppTemplate struct {
	Tags         map[string]string
	ARN          string
	TemplateName string
	CreationDate string
}

// Journey represents a Pinpoint journey.
type Journey struct {
	Tags             map[string]string
	ApplicationID    string
	ID               string
	Name             string
	CreationDate     string
	LastModifiedDate string
	State            string
}

// PushTemplate represents a Pinpoint push notification template.
type PushTemplate struct {
	Tags         map[string]string
	ARN          string
	TemplateName string
	CreationDate string
}

// RecommenderConfiguration represents a Pinpoint recommender configuration.
type RecommenderConfiguration struct {
	Attributes                    map[string]string
	ID                            string
	Name                          string
	Description                   string
	RecommendationProviderIDType  string
	RecommendationProviderRoleARN string
	RecommendationProviderURI     string
	CreationDate                  string
	LastModifiedDate              string
	RecommendationsPerMessage     int
}

// Segment represents a Pinpoint segment.
type Segment struct {
	Tags          map[string]string
	ApplicationID string
	ARN           string
	ID            string
	Name          string
	CreationDate  string
	SegmentType   string
}

// SmsTemplate represents a Pinpoint SMS template.
type SmsTemplate struct {
	Tags         map[string]string
	ARN          string
	TemplateName string
	CreationDate string
}

// createAppRequest is the request body for creating a Pinpoint app.
type createAppRequest struct {
	Tags map[string]string `json:"tags,omitempty"`
	Name string            `json:"Name"`
}

// createCampaignRequest is the request body for CreateCampaign.
type createCampaignRequest struct {
	Tags           map[string]string `json:"tags,omitempty"`
	Name           string            `json:"Name"`
	SegmentID      string            `json:"SegmentId,omitempty"`
	SegmentVersion int               `json:"SegmentVersion,omitempty"`
}

// campaignState is the state sub-object in a CampaignResponse.
type campaignState struct {
	CampaignStatus string `json:"CampaignStatus"`
}

// campaignResponse is the JSON representation of CampaignResponse.
type campaignResponse struct {
	Tags             map[string]string `json:"tags,omitempty"`
	ApplicationID    string            `json:"ApplicationId"`
	ARN              string            `json:"Arn,omitempty"`
	ID               string            `json:"Id"`
	Name             string            `json:"Name"`
	SegmentID        string            `json:"SegmentId,omitempty"`
	CreationDate     string            `json:"CreationDate,omitempty"`
	LastModifiedDate string            `json:"LastModifiedDate,omitempty"`
	State            campaignState     `json:"State"`
	SegmentVersion   int               `json:"SegmentVersion,omitempty"`
}

// createTemplateMessageBody is the create-template response returned by
// CreateEmailTemplate, CreatePushTemplate, and CreateSmsTemplate.
type createTemplateMessageBody struct {
	ARN       string `json:"Arn,omitempty"`
	Message   string `json:"Message"`
	RequestID string `json:"RequestID,omitempty"`
}

// createEmailTemplateRequest is the request body for CreateEmailTemplate.
type createEmailTemplateRequest struct {
	Tags     map[string]string `json:"tags,omitempty"`
	Subject  string            `json:"Subject,omitempty"`
	HTMLPart string            `json:"HtmlPart,omitempty"`
	TextPart string            `json:"TextPart,omitempty"`
}

// createExportJobRequest is the request body for CreateExportJob.
type createExportJobRequest struct {
	RoleArn     string `json:"RoleArn"`
	S3UrlPrefix string `json:"S3UrlPrefix"`
}

// exportJobResponse is the JSON representation of ExportJobResponse.
type exportJobResponse struct {
	ApplicationID string `json:"ApplicationId"`
	ID            string `json:"Id"`
	JobStatus     string `json:"JobStatus"`
	Type          string `json:"Type"`
	CreationDate  string `json:"CreationDate,omitempty"`
}

// createImportJobRequest is the request body for CreateImportJob.
type createImportJobRequest struct {
	RoleArn     string `json:"RoleArn"`
	Format      string `json:"Format"`
	S3Url       string `json:"S3Url"`
	SegmentName string `json:"SegmentName,omitempty"`
}

// importJobResponse is the JSON representation of ImportJobResponse.
type importJobResponse struct {
	ApplicationID string `json:"ApplicationId"`
	ID            string `json:"Id"`
	JobStatus     string `json:"JobStatus"`
	Type          string `json:"Type"`
	CreationDate  string `json:"CreationDate,omitempty"`
}

// createInAppTemplateRequest is the request body for CreateInAppTemplate.
type createInAppTemplateRequest struct {
	Tags map[string]string `json:"tags,omitempty"`
}

// createJourneyRequest is the request body for CreateJourney.
type createJourneyRequest struct {
	Tags map[string]string `json:"tags,omitempty"`
	Name string            `json:"Name"`
}

// journeyResponse is the JSON representation of JourneyResponse.
type journeyResponse struct {
	Tags             map[string]string `json:"tags,omitempty"`
	ApplicationID    string            `json:"ApplicationId"`
	ID               string            `json:"Id"`
	Name             string            `json:"Name"`
	State            string            `json:"State"`
	CreationDate     string            `json:"CreationDate,omitempty"`
	LastModifiedDate string            `json:"LastModifiedDate,omitempty"`
}

// createPushTemplateRequest is the request body for CreatePushTemplate.
type createPushTemplateRequest struct {
	Tags map[string]string `json:"tags,omitempty"`
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

// recommenderConfigResponse is the JSON representation of RecommenderConfigurationResponse.
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

// createSegmentRequest is the request body for CreateSegment.
type createSegmentRequest struct {
	Tags map[string]string `json:"tags,omitempty"`
	Name string            `json:"Name"`
}

// segmentResponse is the JSON representation of SegmentResponse.
type segmentResponse struct {
	Tags          map[string]string `json:"tags,omitempty"`
	ApplicationID string            `json:"ApplicationId"`
	ARN           string            `json:"Arn,omitempty"`
	ID            string            `json:"Id"`
	Name          string            `json:"Name"`
	SegmentType   string            `json:"SegmentType"`
	CreationDate  string            `json:"CreationDate,omitempty"`
}

// createSmsTemplateRequest is the request body for CreateSmsTemplate.
type createSmsTemplateRequest struct {
	Tags map[string]string `json:"tags,omitempty"`
	Body string            `json:"Body,omitempty"`
}

// appResponse is the JSON representation of an ApplicationResponse.
type appResponse struct {
	Tags         map[string]string `json:"tags,omitempty"`
	ARN          string            `json:"Arn"`
	ID           string            `json:"Id"`
	Name         string            `json:"Name"`
	CreationDate string            `json:"CreationDate,omitempty"`
}

// appsResponse is the JSON representation of ApplicationsResponse (GetApps).
type appsResponse struct {
	NextToken *string       `json:"NextToken,omitempty"`
	Item      []appResponse `json:"Item"`
}

// tagsModel is the JSON representation of TagsModel.
type tagsModel struct {
	Tags map[string]string `json:"tags"`
}

// tagResourceRequest is the request body for TagResource.
type tagResourceRequest struct {
	Tags map[string]string `json:"tags"`
}

// appSettingsResponse is the JSON representation of ApplicationSettingsResource.
// CampaignHook, Limits, and QuietTime must be non-nil empty objects so the
// Terraform provider's flatten helpers do not dereference nil pointers.
type appSettingsResponse struct {
	CampaignHook     map[string]any `json:"CampaignHook"`
	Limits           map[string]any `json:"Limits"`
	QuietTime        map[string]any `json:"QuietTime"`
	ApplicationID    string         `json:"ApplicationId"`
	LastModifiedDate string         `json:"LastModifiedDate,omitempty"`
}

// newAppSettingsResponse builds an ApplicationSettingsResource response with
// empty (non-nil) nested objects for CampaignHook, Limits, and QuietTime.
// The Terraform provider's flatten helpers dereference these pointers directly
// and panic if they are nil, so we must always return non-nil empty structs.
func newAppSettingsResponse(appID string) appSettingsResponse {
	return appSettingsResponse{
		ApplicationID:    appID,
		LastModifiedDate: nowRFC3339(),
		CampaignHook:     map[string]any{},
		Limits:           map[string]any{},
		QuietTime:        map[string]any{},
	}
}

// nowRFC3339 returns the current UTC time formatted as RFC 3339.
func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}
