package personalize

import "time"

// DatasetGroup stores an Amazon Personalize dataset group.
type DatasetGroup struct {
	CreationDateTime    time.Time
	LastUpdatedDateTime time.Time
	DatasetGroupArn     string
	Name                string
	Domain              string
	KmsKeyArn           string
	RoleArn             string
	FailureReason       string
	Status              string
}

// Dataset stores an Amazon Personalize dataset.
type Dataset struct {
	CreationDateTime    time.Time
	LastUpdatedDateTime time.Time
	DatasetArn          string
	DatasetGroupArn     string
	SchemaArn           string
	Name                string
	DatasetType         string
	Status              string
}

// Schema stores an Amazon Personalize schema.
type Schema struct {
	CreationDateTime    time.Time
	LastUpdatedDateTime time.Time
	SchemaArn           string
	Name                string
	Schema              string
	Domain              string
}

// Solution stores an Amazon Personalize solution.
type Solution struct {
	LastUpdatedDateTime      time.Time
	CreationDateTime         time.Time
	SolutionConfig           map[string]any
	AutoMLResult             map[string]any
	LatestSolutionUpdate     map[string]any
	DatasetGroupArn          string
	EventType                string
	Status                   string
	RecipeArn                string
	Name                     string
	SolutionArn              string
	PerformAutoML            bool
	PerformHPO               bool
	PerformAutoTraining      bool
	PerformIncrementalUpdate bool
}

// SolutionVersion stores a trained Amazon Personalize solution version.
type SolutionVersion struct {
	CreationDateTime    time.Time
	LastUpdatedDateTime time.Time
	SolutionConfig      map[string]any
	SolutionVersionArn  string
	SolutionArn         string
	Status              string
	TrainingMode        string
	TrainingHours       float64
}

// Campaign stores an Amazon Personalize campaign.
type Campaign struct {
	CreationDateTime     time.Time
	LastUpdatedDateTime  time.Time
	CampaignConfig       map[string]any
	LatestCampaignUpdate map[string]any
	CampaignArn          string
	SolutionVersionArn   string
	Name                 string
	Status               string
	MinProvisionedTPS    int32
}

// DatasetImportJob stores an async dataset import job.
type DatasetImportJob struct {
	CreationDateTime    time.Time
	LastUpdatedDateTime time.Time
	DataSource          map[string]any
	DatasetImportJobArn string
	DatasetArn          string
	JobName             string
	RoleArn             string
	Status              string
}

// DatasetExportJob stores an async dataset export job.
type DatasetExportJob struct {
	CreationDateTime    time.Time
	LastUpdatedDateTime time.Time
	JobOutput           map[string]any
	DatasetExportJobArn string
	DatasetArn          string
	JobName             string
	RoleArn             string
	Status              string
}

// BatchInferenceJob stores an async batch inference job.
type BatchInferenceJob struct {
	CreationDateTime     time.Time
	LastUpdatedDateTime  time.Time
	JobInput             map[string]any
	JobOutput            map[string]any
	BatchInferenceJobArn string
	SolutionVersionArn   string
	JobName              string
	RoleArn              string
	Status               string
}

// BatchSegmentJob stores an async batch segment job.
type BatchSegmentJob struct {
	CreationDateTime    time.Time
	LastUpdatedDateTime time.Time
	JobInput            map[string]any
	JobOutput           map[string]any
	BatchSegmentJobArn  string
	SolutionVersionArn  string
	JobName             string
	RoleArn             string
	Status              string
}

// EventTracker stores an Amazon Personalize event tracker.
type EventTracker struct {
	CreationDateTime    time.Time
	LastUpdatedDateTime time.Time
	EventTrackerArn     string
	DatasetGroupArn     string
	Name                string
	TrackingID          string
	Status              string
}

// Filter stores an Amazon Personalize filter.
type Filter struct {
	CreationDateTime    time.Time
	LastUpdatedDateTime time.Time
	FilterArn           string
	DatasetGroupArn     string
	Name                string
	FilterExpression    string
	Status              string
}

// Recommender stores an Amazon Personalize recommender.
type Recommender struct {
	CreationDateTime                   time.Time
	LastUpdatedDateTime                time.Time
	RecommenderConfig                  map[string]any
	LatestRecommenderUpdate            map[string]any
	RecommenderArn                     string
	DatasetGroupArn                    string
	RecipeArn                          string
	Name                               string
	Status                             string
	MinRecommendationRequestsPerSecond int32
}

// MetricAttribute describes a single tracked metric within a metric
// attribution: an event type and the expression (SUM()/SAMPLECOUNT()) used to
// compute it.
type MetricAttribute struct {
	EventType  string
	Expression string
	MetricName string
}

// MetricAttribution stores an Amazon Personalize metric attribution.
type MetricAttribution struct {
	CreationDateTime     time.Time
	LastUpdatedDateTime  time.Time
	MetricsOutputConfig  map[string]any
	MetricAttributionArn string
	DatasetGroupArn      string
	Name                 string
	Status               string
	Metrics              []MetricAttribute
}

// DataDeletionJob stores an async data deletion job.
type DataDeletionJob struct {
	CreationDateTime    time.Time
	LastUpdatedDateTime time.Time
	DataSource          map[string]any
	DataDeletionJobArn  string
	DatasetGroupArn     string
	JobName             string
	RoleArn             string
	Status              string
	NumDeleted          int32
}
