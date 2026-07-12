package pinpoint

import "context"

// StorageBackend defines the interface for Pinpoint backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// App operations
	CreateApp(region, accountID, name string, tags map[string]string) (*App, error)
	GetApp(appID string) (*App, error)
	DeleteApp(appID string) (*App, error)
	GetApps() ([]*App, error)
	GetApplicationSettings(appID string) (*storedAppSettings, error)
	UpdateApplicationSettings(appID string, settings *storedAppSettings) (*storedAppSettings, error)

	// Tag operations
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	// Campaign operations
	CreateCampaign(region, accountID, appID string, req createCampaignRequest) (*Campaign, error)
	GetCampaign(appID, campaignID string) (*Campaign, error)
	GetCampaigns(appID string) ([]*Campaign, error)
	UpdateCampaign(appID, campaignID string, req updateCampaignRequest) (*Campaign, error)
	DeleteCampaign(appID, campaignID string) (*Campaign, error)
	GetCampaignActivities(appID, campaignID string) (*campaignActivitiesResponse, error)
	GetCampaignDateRangeKpi(appID, campaignID, kpiName string) (*kpiResult, error)
	GetCampaignVersion(appID, campaignID string, version int) (*Campaign, error)
	GetCampaignVersions(appID, campaignID string) ([]*Campaign, error)

	// Template operations
	CreateEmailTemplate(region, accountID, templateName string, req createEmailTemplateRequest) (*EmailTemplate, error)
	GetEmailTemplate(templateName string) (*EmailTemplate, error)
	UpdateEmailTemplate(templateName string, req createEmailTemplateRequest) (*EmailTemplate, error)
	DeleteEmailTemplate(templateName string) (*EmailTemplate, error)
	CreateInAppTemplate(region, accountID, templateName string, req createInAppTemplateRequest) (*InAppTemplate, error)
	GetInAppTemplate(templateName string) (*InAppTemplate, error)
	UpdateInAppTemplate(templateName string, req createInAppTemplateRequest) (*InAppTemplate, error)
	DeleteInAppTemplate(templateName string) (*InAppTemplate, error)
	CreatePushTemplate(region, accountID, templateName string, req createPushTemplateRequest) (*PushTemplate, error)
	GetPushTemplate(templateName string) (*PushTemplate, error)
	UpdatePushTemplate(templateName string, req createPushTemplateRequest) (*PushTemplate, error)
	DeletePushTemplate(templateName string) (*PushTemplate, error)
	CreateSmsTemplate(region, accountID, templateName string, req createSmsTemplateRequest) (*SmsTemplate, error)
	GetSmsTemplate(templateName string) (*SmsTemplate, error)
	UpdateSmsTemplate(templateName string, req createSmsTemplateRequest) (*SmsTemplate, error)
	DeleteSmsTemplate(templateName string) (*SmsTemplate, error)
	CreateVoiceTemplate(region, accountID, templateName string, req createVoiceTemplateRequest) (*VoiceTemplate, error)
	GetVoiceTemplate(templateName string) (*VoiceTemplate, error)
	UpdateVoiceTemplate(templateName string, req createVoiceTemplateRequest) (*VoiceTemplate, error)
	DeleteVoiceTemplate(templateName string) (*VoiceTemplate, error)
	ListTemplates() ([]*templateListItem, error)
	ListTemplateVersions(templateName, templateType string) ([]*templateVersionItem, error)
	UpdateTemplateActiveVersion(templateName, templateType string) error

	// Job operations
	CreateExportJob(region, accountID, appID string, req createExportJobRequest) (*ExportJob, error)
	GetExportJob(appID, jobID string) (*ExportJob, error)
	GetExportJobs(appID string) ([]*ExportJob, error)
	GetSegmentExportJobs(appID, segmentID string) ([]*ExportJob, error)
	CreateImportJob(region, accountID, appID string, req createImportJobRequest) (*ImportJob, error)
	GetImportJob(appID, jobID string) (*ImportJob, error)
	GetImportJobs(appID string) ([]*ImportJob, error)
	GetSegmentImportJobs(appID, segmentID string) ([]*ImportJob, error)

	// Journey operations
	CreateJourney(region, accountID, appID string, req createJourneyRequest) (*Journey, error)
	GetJourney(appID, journeyID string) (*Journey, error)
	GetJourneys(appID string) ([]*Journey, error)
	UpdateJourney(appID, journeyID string, req updateJourneyRequest) (*Journey, error)
	UpdateJourneyState(appID, journeyID, state string) (*Journey, error)
	DeleteJourney(appID, journeyID string) (*Journey, error)
	GetJourneyDateRangeKpi(appID, journeyID, kpiName string) (*kpiResult, error)
	GetJourneyExecutionMetrics(appID, journeyID string) (*journeyExecutionMetricsResponse, error)
	GetJourneyExecutionActivityMetrics(
		appID, journeyID, activityID string,
	) (*journeyExecutionActivityMetricsResponse, error)
	GetJourneyRuns(appID, journeyID string) (*journeyRunsResponse, error)
	GetJourneyRunExecutionMetrics(appID, journeyID, runID string) (*journeyRunExecutionMetricsResponse, error)
	GetJourneyRunExecutionActivityMetrics(
		appID, journeyID, runID, activityID string,
	) (*journeyRunExecutionActivityMetricsResponse, error)

	// Recommender operations
	CreateRecommenderConfiguration(req createRecommenderConfigRequest) (*RecommenderConfiguration, error)
	GetRecommenderConfiguration(recommenderID string) (*RecommenderConfiguration, error)
	GetRecommenderConfigurations() ([]*RecommenderConfiguration, error)
	UpdateRecommenderConfiguration(
		recommenderID string,
		req createRecommenderConfigRequest,
	) (*RecommenderConfiguration, error)
	DeleteRecommenderConfiguration(recommenderID string) (*RecommenderConfiguration, error)

	// Segment operations
	CreateSegment(region, accountID, appID string, req createSegmentRequest) (*Segment, error)
	GetSegment(appID, segmentID string) (*Segment, error)
	GetSegments(appID string) ([]*Segment, error)
	UpdateSegment(appID, segmentID string, req updateSegmentRequest) (*Segment, error)
	DeleteSegment(appID, segmentID string) (*Segment, error)
	GetSegmentVersion(appID, segmentID string, version int) (*Segment, error)
	GetSegmentVersions(appID, segmentID string) ([]*Segment, error)

	// Endpoint operations
	GetEndpoint(appID, endpointID string) (*Endpoint, error)
	UpdateEndpoint(appID, endpointID string, req updateEndpointRequest) (*Endpoint, error)
	DeleteEndpoint(appID, endpointID string) (*Endpoint, error)
	GetUserEndpoints(appID, userID string) ([]*Endpoint, error)
	DeleteUserEndpoints(appID, userID string) error
	UpdateEndpointsBatch(appID string, endpoints map[string]updateEndpointRequest) error

	// EventStream operations
	GetEventStream(appID string) (*EventStream, error)
	PutEventStream(appID string, req putEventStreamRequest) (*EventStream, error)
	DeleteEventStream(appID string) (*EventStream, error)

	// Channel operations
	GetChannel(appID, channelType string) *Channel
	UpsertChannel(appID, channelType string, enabled bool, extra map[string]any) *Channel
	DeleteChannel(appID, channelType string) *Channel
	GetAllChannels(appID string) map[string]*Channel

	// Analytics
	GetApplicationDateRangeKpi(appID, kpiName string) (*kpiResult, error)

	// Messaging
	SendMessages(appID string, req sendMessagesRequest) (*messageResponse, error)
	SendUsersMessages(appID string, req sendUsersMessagesRequest) (*usersMessageResponse, error)
	SendOTPMessage(appID string) (*sendOTPMessageResponse, error)
	VerifyOTPMessage(appID, code string) (*verifyOTPMessageResponse, error)
	PutEvents(appID string, req putEventsRequest) (*eventsResponse, error)
	PhoneNumberValidate(phoneNumber string) (*phoneNumberValidateResponse, error)
	RemoveAttributes(appID, attributeType string, blacklist []string) (*attributesResource, error)
	GetInAppMessages(appID, endpointID string) (*inAppMessagesResponse, error)

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
