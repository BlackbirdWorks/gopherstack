package comprehend

import "time"

const (
	statusSubmitted                  = "SUBMITTED"
	statusInProgress                 = "IN_PROGRESS"
	statusCompleted                  = "COMPLETED"
	statusFailed                     = "FAILED"
	statusStopRequested              = "STOP_REQUESTED"
	statusStopped                    = "STOPPED"
	statusTrained                    = "TRAINED"
	statusReady                      = "READY"
	statusActive                     = "ACTIVE"
	defaultLanguageCode              = "en"
	defaultScore                     = 0.99
	failedMarker                     = "[fail]"
	resourceTypeEndpoint             = "endpoint"
	resourceTypeFlywheel             = "flywheel"
	resourceTypeDataset              = "dataset"
	resourceTypeDocClassifier        = "document-classifier"
	resourceTypeDocClassifierVersion = "document-classifier-version"
	resourceTypeEntityRecognizer     = "entity-recognizer"
	resourceTypeEntityRecognizerVer  = "entity-recognizer-version"
)

// Tag is a Comprehend resource tag.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// Job represents an asynchronous document-analysis job.
type Job struct {
	SubmitTime            time.Time
	EndTime               time.Time
	JobID                 string
	JobArn                string
	JobName               string
	JobType               string
	JobStatus             string
	LanguageCode          string
	FailureReason         string
	InputDataConfig       map[string]any
	OutputDataConfig      map[string]any
	DataAccessRoleArn     string
	DocumentClassifierArn string
	EntityRecognizerArn   string
	TargetEventTypes      []string
	polls                 int
	stopRequested         bool
	shouldFail            bool
}

// Resource stores a Comprehend trainable or hosted resource.
type Resource struct {
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Name          string
	Arn           string
	Type          string
	Status        string
	VersionName   string
	ModelArn      string
	FlywheelArn   string
	EndpointArn   string
	DatasetArn    string
	Configuration map[string]any
	FailureReason string
}

// FlywheelIteration represents one model training iteration.
type FlywheelIteration struct {
	CreationTime            time.Time
	EndTime                 time.Time
	FlywheelArn             string
	FlywheelIterationID     string
	FlywheelIterationStatus string
	Message                 string
	polls                   int
}
