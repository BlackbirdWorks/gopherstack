package pinpoint

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
//
// DefaultSubstitutions is a JSON-encoded string on the wire (confirmed
// against EmailTemplateRequest's serializer -- object.Key("DefaultSubstitutions").String(*v)),
// not a nested object; the caller supplies an already-serialized JSON string.
type createEmailTemplateRequest struct {
	Tags                 map[string]string `json:"tags,omitempty"`
	DefaultSubstitutions string            `json:"DefaultSubstitutions,omitempty"`
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
	CustomConfig        map[string]string `json:"CustomConfig,omitempty"`
	Layout              string            `json:"Layout,omitempty"`
	TemplateDescription string            `json:"TemplateDescription,omitempty"`
	Content             []map[string]any  `json:"Content,omitempty"`
}

// createJourneyRequest is the request body for CreateJourney.
type createJourneyRequest struct {
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
//
// There is no top-level Body/Title on the real PushNotificationTemplateRequest
// (confirmed against awsRestjson1_serializeDocumentPushNotificationTemplateRequest) --
// per-platform body/title live inside ADM/APNS/Baidu/Default/GCM.
type createPushTemplateRequest struct {
	ADM                  map[string]any    `json:"ADM,omitempty"`
	APNS                 map[string]any    `json:"APNS,omitempty"`
	Baidu                map[string]any    `json:"Baidu,omitempty"`
	Default              map[string]any    `json:"Default,omitempty"`
	GCM                  map[string]any    `json:"GCM,omitempty"`
	Tags                 map[string]string `json:"tags,omitempty"`
	DefaultSubstitutions string            `json:"DefaultSubstitutions,omitempty"`
	RecommenderID        string            `json:"RecommenderId,omitempty"`
	TemplateDescription  string            `json:"TemplateDescription,omitempty"`
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
	Tags          map[string]string `json:"tags,omitempty"`
	Dimensions    map[string]any    `json:"Dimensions,omitempty"`
	SegmentGroups map[string]any    `json:"SegmentGroups,omitempty"`
	Name          string            `json:"Name"`
}

// createSmsTemplateRequest is the request body for CreateSmsTemplate.
//
// There is no SenderId field on the real SMSTemplateRequest (confirmed
// against awsRestjson1_serializeDocumentSMSTemplateRequest); a prior pass had
// invented it. SMS sender ID is configured on the SMS *channel*, not the
// template.
type createSmsTemplateRequest struct {
	Body                 string            `json:"Body,omitempty"`
	DefaultSubstitutions string            `json:"DefaultSubstitutions,omitempty"`
	RecommenderID        string            `json:"RecommenderId,omitempty"`
	Tags                 map[string]string `json:"tags,omitempty"`
	TemplateDescription  string            `json:"TemplateDescription,omitempty"`
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

// exportJobDefinition is the nested Definition sub-object of ExportJobResponse
// (types.ExportJobResource), confirmed against pinpoint@v1.42.4
// deserializers.go's awsRestjson1_deserializeDocumentExportJobResource. A
// prior version of exportJobResponse emitted RoleArn/S3UrlPrefix at the top
// level instead of nested here, which a real client's deserializer silently
// drops since ExportJobResponse itself has no such top-level members.
type exportJobDefinition struct {
	RoleArn        string `json:"RoleArn,omitempty"`
	S3UrlPrefix    string `json:"S3UrlPrefix,omitempty"`
	SegmentID      string `json:"SegmentId,omitempty"`
	SegmentVersion int    `json:"SegmentVersion,omitempty"`
}

// exportJobResponse is the JSON wire format of ExportJobResponse. There is no
// top-level Arn member on the real type (confirmed: absent from both
// types.ExportJobResponse and the deserializer's case list) -- a prior
// version fabricated one.
type exportJobResponse struct {
	ApplicationID string              `json:"ApplicationId"`
	ID            string              `json:"Id"`
	JobStatus     string              `json:"JobStatus"`
	Type          string              `json:"Type"`
	CreationDate  string              `json:"CreationDate,omitempty"`
	Definition    exportJobDefinition `json:"Definition"`
}

// importJobDefinition is the nested Definition sub-object of
// ImportJobResponse (types.ImportJobResource), same nesting bug as
// exportJobDefinition above.
type importJobDefinition struct {
	RoleArn   string `json:"RoleArn,omitempty"`
	S3Url     string `json:"S3Url,omitempty"`
	Format    string `json:"Format,omitempty"`
	SegmentID string `json:"SegmentId,omitempty"`
}

// importJobResponse is the JSON wire format of ImportJobResponse. No
// top-level Arn member on the real type, same as exportJobResponse.
type importJobResponse struct {
	ApplicationID string              `json:"ApplicationId"`
	ID            string              `json:"Id"`
	Definition    importJobDefinition `json:"Definition"`
	JobStatus     string              `json:"JobStatus"`
	Type          string              `json:"Type"`
	CreationDate  string              `json:"CreationDate,omitempty"`
}

// journeyResponse is the JSON wire format of JourneyResponse.
type journeyResponse struct {
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
// CampaignHook, Limits, QuietTime, and JourneyLimits must be non-nil empty
// objects so the Terraform provider's flatten helpers do not dereference nil
// pointers. JourneyLimits is a real member (types.ApplicationSettingsResource,
// pinpoint@v1.42.4 types/types.go) that a prior version never emitted at all.
// CloudWatchMetricsEnabled/EventTaggingEnabled are NOT real members of this
// type (confirmed: absent from both types.ApplicationSettingsResource and the
// deserializer's case list) -- kept here since they're harmless extra JSON
// fields a real client simply ignores, not worth an unrelated behavior change.
type appSettingsResponse struct {
	CampaignHook             map[string]any `json:"CampaignHook"`
	Limits                   map[string]any `json:"Limits"`
	QuietTime                map[string]any `json:"QuietTime"`
	JourneyLimits            map[string]any `json:"JourneyLimits"`
	ApplicationID            string         `json:"ApplicationId"`
	LastModifiedDate         string         `json:"LastModifiedDate,omitempty"`
	CloudWatchMetricsEnabled bool           `json:"CloudWatchMetricsEnabled"`
	EventTaggingEnabled      bool           `json:"EventTaggingEnabled"`
}

// New request types for additional operations
// ──────────────────────────────────────────────────

// createVoiceTemplateRequest is the request body for CreateVoiceTemplate,
// field-diffed against VoiceTemplateRequest's serializer
// (awsRestjson1_serializeDocumentVoiceTemplateRequest).
type createVoiceTemplateRequest struct {
	Tags                 map[string]string `json:"tags,omitempty"`
	Body                 string            `json:"Body,omitempty"`
	DefaultSubstitutions string            `json:"DefaultSubstitutions,omitempty"`
	LanguageCode         string            `json:"LanguageCode,omitempty"`
	TemplateDescription  string            `json:"TemplateDescription,omitempty"`
	VoiceID              string            `json:"VoiceId,omitempty"`
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
	Tags          map[string]string `json:"tags,omitempty"`
	Dimensions    map[string]any    `json:"Dimensions,omitempty"`
	SegmentGroups map[string]any    `json:"SegmentGroups,omitempty"`
	Name          string            `json:"Name,omitempty"`
}

// updateJourneyRequest is the request body for UpdateJourney.
type updateJourneyRequest struct {
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
	BundleID                    string `json:"BundleId,omitempty"`
	Certificate                 string `json:"Certificate,omitempty"`
	DefaultAuthenticationMethod string `json:"DefaultAuthenticationMethod,omitempty"`
	PrivateKey                  string `json:"PrivateKey,omitempty"`
	TeamID                      string `json:"TeamId,omitempty"`
	TokenKey                    string `json:"TokenKey,omitempty"`
	TokenKeyID                  string `json:"TokenKeyId,omitempty"`
	Enabled                     bool   `json:"Enabled"`
}

// updateEmailChannelRequest is the request body for UpdateEmailChannel.
type updateEmailChannelRequest struct {
	ConfigurationSet            string `json:"ConfigurationSet,omitempty"`
	FromAddress                 string `json:"FromAddress,omitempty"`
	Identity                    string `json:"Identity,omitempty"`
	OrchestrationSendingRoleArn string `json:"OrchestrationSendingRoleArn,omitempty"`
	RoleArn                     string `json:"RoleArn,omitempty"`
	Enabled                     bool   `json:"Enabled"`
}

// updateSMSChannelRequest is the request body for UpdateSmsChannel.
//
// PromotionalMessagesPerSecond/TransactionalMessagesPerSecond are NOT present
// here: they exist only on SMSChannelResponse (AWS-computed account
// throughput), not on SMSChannelRequest (confirmed against
// aws-sdk-go-v2/service/pinpoint/types) -- a real SDK client has no field to
// send them through. A prior pass accepted them on write and echoed them
// back, which isn't a real bug (harmless when no real client sends them) but
// is wire-shape noise; removed for hygiene.
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

// sendMessagesRequest is the JSON wire format of SendMessagesInput's
// MessageRequest member. gopherstack-lffs: pinpoint@v1.42.4
// awsRestjson1_serializeOpSendMessages serializes input.MessageRequest's own
// fields directly as the request body (serializers.go) -- there is no
// top-level "MessageRequest" wrapper key on the wire, same flat shape as the
// response side.
type sendMessagesRequest struct {
	Addresses map[string]addressConfig `json:"Addresses,omitempty"`
}

// addressConfig is a per-address configuration in a SendMessages request.
type addressConfig struct {
	ChannelType string `json:"ChannelType,omitempty"`
}

// putEventsRequest is the JSON wire format of PutEventsInput's EventsRequest
// member. Flat on the wire like sendMessagesRequest above -- confirmed
// against awsRestjson1_serializeOpPutEvents, no "EventsRequest" wrapper key.
type putEventsRequest struct {
	BatchItem map[string]endpointEvents `json:"BatchItem,omitempty"`
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

// phoneNumberValidateRequest is the JSON wire format of
// PhoneNumberValidateInput's NumberValidateRequest member. Flat on the wire,
// same as sendMessagesRequest above -- confirmed against
// awsRestjson1_serializeOpPhoneNumberValidate, no "NumberValidateRequest"
// wrapper key.
type phoneNumberValidateRequest struct {
	PhoneNumber string `json:"PhoneNumber"`
	IsoCountry  string `json:"IsoCountryCode,omitempty"`
}

// ──────────────────────────────────────────────────
// New response types for additional operations
// ──────────────────────────────────────────────────

// voiceTemplateResponse is the JSON wire format of VoiceTemplateResponse.
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

// kpiResult is the KPI response structure, shared by
// ApplicationDateRangeKpiResponse/CampaignDateRangeKpiResponse/
// JourneyDateRangeKpiResponse. StartTime/EndTime are "This member is
// required." on all three real types (pinpoint@v1.42.4 types/types.go) even
// though the request's start-time/end-time query params are optional --
// a prior version never emitted either.
type kpiResult struct {
	ApplicationID string  `json:"ApplicationId"`
	CampaignID    string  `json:"CampaignId,omitempty"`
	JourneyID     string  `json:"JourneyId,omitempty"`
	KpiName       string  `json:"KpiName"`
	StartTime     string  `json:"StartTime"`
	EndTime       string  `json:"EndTime"`
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
	Result        map[string]messageResult `json:"Result"`
	ApplicationID string                   `json:"ApplicationId"`
	RequestID     string                   `json:"RequestId,omitempty"`
}

// messageResult is a per-address message result.
type messageResult struct {
	DeliveryStatus string `json:"DeliveryStatus"`
	MessageID      string `json:"MessageId,omitempty"`
	StatusCode     int    `json:"StatusCode"`
}

// usersMessageResponse is the JSON wire format of SendUsersMessageResponse.
// SendMessages/SendUsersMessages/SendOTPMessage/PhoneNumberValidate are all
// flat/payload ops (pinpoint@v1.42.4 deserializers.go's
// awsRestjson1_deserializeOp{SendMessages,SendUsersMessages,SendOTPMessage,
// PhoneNumberValidate} decode the whole body directly into
// output.{MessageResponse,SendUsersMessageResponse,MessageResponse,
// NumberValidateResponse} via awsRestjson1_deserializeDocument*; the
// generated deserializeOpDocument*Output wrapper functions, whose case lists
// switch on "MessageResponse"/"SendUsersMessageResponse"/
// "NumberValidateResponse", are dead code never reached by HandleDeserialize)
// -- there is no top-level wrapper key on the wire, these are the response
// bodies themselves.
type usersMessageResponse struct {
	Result        map[string]map[string]messageResult `json:"Result"`
	ApplicationID string                              `json:"ApplicationId"`
	RequestID     string                              `json:"RequestId,omitempty"`
}

// verifyOTPMessageResponse is the response for VerifyOTPMessage.
type verifyOTPMessageResponse struct {
	Valid bool `json:"Valid"`
}

// numberValidateResponse is the JSON wire format of PhoneNumberValidateOutput.
type numberValidateResponse struct {
	Carrier                           string `json:"Carrier,omitempty"`
	City                              string `json:"City,omitempty"`
	CleansedPhoneNumberE164           string `json:"CleansedPhoneNumberE164,omitempty"`
	CleansedPhoneNumberNationalFormat string `json:"CleansedPhoneNumberNationalFormat,omitempty"`
	Country                           string `json:"Country,omitempty"`
	CountryCodeIso2                   string `json:"CountryCodeIso2,omitempty"`
	CountryCodeNumeric                string `json:"CountryCodeNumeric,omitempty"`
	OriginalCountryCodeIso2           string `json:"OriginalCountryCodeIso2,omitempty"`
	OriginalPhoneNumber               string `json:"OriginalPhoneNumber,omitempty"`
	PhoneType                         string `json:"PhoneType,omitempty"`
	Timezone                          string `json:"Timezone,omitempty"`
	ZipCode                           string `json:"ZipCode,omitempty"`
	PhoneTypeCode                     int    `json:"PhoneTypeCode"`
}

// attributesResource is the response for RemoveAttributes.
type attributesResource struct {
	ApplicationID string   `json:"ApplicationId"`
	AttributeType string   `json:"AttributeType"`
	Attributes    []string `json:"Attributes"`
}

// removeAttributesRequest is the request body for RemoveAttributes. The wire
// payload IS the UpdateAttributesRequest shape directly (no wrapper key).
type removeAttributesRequest struct {
	Blacklist []string `json:"Blacklist"`
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
// LastEvaluatedTime is "This member is required." on the real
// JourneyExecutionMetricsResponse (pinpoint@v1.42.4 types/types.go); a prior
// version never emitted it.
type journeyExecutionMetricsResponse struct {
	Metrics           map[string]string `json:"Metrics"`
	ApplicationID     string            `json:"ApplicationId"`
	JourneyID         string            `json:"JourneyId"`
	LastEvaluatedTime string            `json:"LastEvaluatedTime"`
}

// journeyExecutionActivityMetricsResponse is the response for GetJourneyExecutionActivityMetrics.
// Same missing-required-member shape as journeyExecutionMetricsResponse.
type journeyExecutionActivityMetricsResponse struct {
	Metrics           map[string]string `json:"Metrics"`
	ApplicationID     string            `json:"ApplicationId"`
	JourneyID         string            `json:"JourneyId"`
	ActivityID        string            `json:"ActivityId"`
	LastEvaluatedTime string            `json:"LastEvaluatedTime"`
}

// journeyRunsResponse is the response for GetJourneyRuns.
type journeyRunsResponse struct {
	Item []journeyRun `json:"Item"`
}

// journeyRun is a single journey run. CreationTime/LastUpdateTime are "This
// member is required." on the real JourneyRunResponse (pinpoint@v1.42.4
// types/types.go); a prior version never emitted either. ApplicationId/
// JourneyId are NOT real members of the per-item shape (confirmed:
// JourneyRunResponse's own field set is only CreationTime/LastUpdateTime/
// RunId/Status -- the app/journey identity comes from the URL path), kept
// here only as internal bookkeeping since they're omitted from JSON below.
type journeyRun struct {
	RunID          string `json:"RunId"`
	JourneyID      string `json:"-"`
	ApplicationID  string `json:"-"`
	Status         string `json:"Status"`
	CreationTime   string `json:"CreationTime"`
	LastUpdateTime string `json:"LastUpdateTime"`
}

// journeyRunExecutionMetricsResponse is the response for GetJourneyRunExecutionMetrics.
type journeyRunExecutionMetricsResponse struct {
	Metrics           map[string]string `json:"Metrics"`
	ApplicationID     string            `json:"ApplicationId"`
	JourneyID         string            `json:"JourneyId"`
	RunID             string            `json:"RunId"`
	LastEvaluatedTime string            `json:"LastEvaluatedTime"`
}

// journeyRunExecutionActivityMetricsResponse is the response for GetJourneyRunExecutionActivityMetrics.
type journeyRunExecutionActivityMetricsResponse struct {
	Metrics           map[string]string `json:"Metrics"`
	ApplicationID     string            `json:"ApplicationId"`
	JourneyID         string            `json:"JourneyId"`
	RunID             string            `json:"RunId"`
	ActivityID        string            `json:"ActivityId"`
	LastEvaluatedTime string            `json:"LastEvaluatedTime"`
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

// verifyOTPMessageRequest is the JSON wire format of VerifyOTPMessageInput's
// VerifyOTPMessageRequestParameters member. Flat on the wire, same as
// sendMessagesRequest above -- confirmed against
// awsRestjson1_serializeOpVerifyOTPMessage, no
// "VerifyOTPMessageRequestParameters" wrapper key. A prior version's wrapper
// meant a real client's Otp value never reached the backend, so any
// verification always fell back to the no-code "was an OTP ever sent?" path
// regardless of what code the caller actually sent.
type verifyOTPMessageRequest struct {
	DestinationIdentity string `json:"DestinationIdentity,omitempty"`
	Otp                 string `json:"Otp"`
	ReferenceID         string `json:"ReferenceId,omitempty"`
}

// sendUsersMessagesRequest is the JSON wire format of
// SendUsersMessagesInput's SendUsersMessageRequest member. Flat on the wire,
// same as sendMessagesRequest above -- confirmed against
// awsRestjson1_serializeOpSendUsersMessages, no "SendUsersMessageRequest"
// wrapper key.
type sendUsersMessagesRequest struct {
	Users                 map[string]endpointSendConfig `json:"Users,omitempty"`
	MessageConfiguration  map[string]any                `json:"MessageConfiguration,omitempty"`
	TemplateConfiguration map[string]any                `json:"TemplateConfiguration,omitempty"`
	TraceID               string                        `json:"TraceId,omitempty"`
}

// endpointSendConfig is per-endpoint config in SendUsersMessages.
type endpointSendConfig struct {
	Context       map[string]string   `json:"Context,omitempty"`
	Substitutions map[string][]string `json:"Substitutions,omitempty"`
	BodyOverride  string              `json:"BodyOverride,omitempty"`
	TitleOverride string              `json:"TitleOverride,omitempty"`
	RawContent    string              `json:"RawContent,omitempty"`
}

// pagedCampaignsResponse is the JSON wire format of CampaignsResponse with pagination.
type pagedCampaignsResponse struct {
	NextToken *string            `json:"NextToken,omitempty"`
	Item      []campaignResponse `json:"Item"`
}

// pagedSegmentsResponse is the JSON wire format of SegmentsResponse with pagination.
type pagedSegmentsResponse struct {
	NextToken *string           `json:"NextToken,omitempty"`
	Item      []segmentResponse `json:"Item"`
}

// pagedJourneysResponse is the JSON wire format of JourneysResponse with pagination.
type pagedJourneysResponse struct {
	NextToken *string           `json:"NextToken,omitempty"`
	Item      []journeyResponse `json:"Item"`
}

// eventsResponse is the response for PutEvents.
type eventsResponse struct {
	Results map[string]endpointItemResponse `json:"Results"`
}

// endpointItemResponse is a per-endpoint result in the PutEvents response.
type endpointItemResponse struct {
	EventsItemResponse map[string]itemEventResponse `json:"EventsItemResponse"`
}

// itemEventResponse is the per-event acknowledgment in a PutEvents response.
type itemEventResponse struct {
	Message    string `json:"Message"`
	StatusCode int    `json:"StatusCode"`
}
