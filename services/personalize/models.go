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
	SolutionConfig           *SolutionConfig
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
//
// DatasetGroupArn/EventType/PerformAutoML/PerformHPO/PerformIncrementalUpdate/
// RecipeArn are copied from the parent Solution at CreateSolutionVersion
// time (types.SolutionVersion, types.go:2074) -- a later UpdateSolution call
// must not retroactively change an already-created version's values, so
// these are snapshotted plain fields, not a live lookup through SolutionArn.
type SolutionVersion struct {
	CreationDateTime         time.Time
	LastUpdatedDateTime      time.Time
	SolutionConfig           *SolutionConfig
	SolutionVersionArn       string
	SolutionArn              string
	DatasetGroupArn          string
	EventType                string
	RecipeArn                string
	FailureReason            string
	Status                   string
	TrainingMode             string
	Name                     string
	TrainingHours            float64
	PerformAutoML            bool
	PerformHPO               bool
	PerformIncrementalUpdate bool
}

// Campaign stores an Amazon Personalize campaign.
type Campaign struct {
	CreationDateTime     time.Time
	LastUpdatedDateTime  time.Time
	CampaignConfig       *CampaignConfig
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
	RecommenderConfig                  *RecommenderConfig
	LatestRecommenderUpdate            map[string]any
	RecommenderArn                     string
	DatasetGroupArn                    string
	RecipeArn                          string
	Name                               string
	Status                             string
	MinRecommendationRequestsPerSecond int32
}

// recommenderModelMetrics returns deterministic, ARN-derived evaluation
// metrics for a recommender -- the real Recommender.ModelMetrics member
// (types.go:1697, deserializers.go:14660, a plain map[string]float64 with
// no fixed key set) had no source in this backend at all (no real training
// pipeline computes recommender performance), following the same ARN-hash
// deterministic-mock convention already established for SolutionVersion
// metrics (solutions.go's svMetric, GetSolutionMetrics).
func recommenderModelMetrics(recommenderArn string) map[string]float64 {
	return map[string]float64{
		"coverage":        svMetric(recommenderArn, "coverage"),
		"precision_at_5":  svMetric(recommenderArn, "p@5"),
		"precision_at_10": svMetric(recommenderArn, "p@10"),
		"precision_at_25": svMetric(recommenderArn, "p@25"),
	}
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
