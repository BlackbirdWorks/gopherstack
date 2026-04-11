package pinpoint

// StorageBackend defines the interface for Pinpoint backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// App operations
	CreateApp(region, accountID, name string, tags map[string]string) (*App, error)
	GetApp(appID string) (*App, error)
	DeleteApp(appID string) (*App, error)
	GetApps() ([]*App, error)

	// Tag operations
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	// Campaign operations
	CreateCampaign(region, accountID, appID string, req createCampaignRequest) (*Campaign, error)

	// Template operations
	CreateEmailTemplate(region, accountID, templateName string, req createEmailTemplateRequest) (*EmailTemplate, error)
	CreateInAppTemplate(region, accountID, templateName string, req createInAppTemplateRequest) (*InAppTemplate, error)
	CreatePushTemplate(region, accountID, templateName string, req createPushTemplateRequest) (*PushTemplate, error)
	CreateSmsTemplate(region, accountID, templateName string, req createSmsTemplateRequest) (*SmsTemplate, error)

	// Job operations
	CreateExportJob(region, accountID, appID string, req createExportJobRequest) (*ExportJob, error)
	CreateImportJob(region, accountID, appID string, req createImportJobRequest) (*ImportJob, error)

	// Journey operations
	CreateJourney(region, accountID, appID string, req createJourneyRequest) (*Journey, error)

	// Recommender operations
	CreateRecommenderConfiguration(req createRecommenderConfigRequest) (*RecommenderConfiguration, error)

	// Segment operations
	CreateSegment(region, accountID, appID string, req createSegmentRequest) (*Segment, error)

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
