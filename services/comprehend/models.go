package comprehend

import "time"

const (
	statusSubmitted              = "SUBMITTED"
	statusInProgress             = "IN_PROGRESS"
	statusCompleted              = "COMPLETED"
	statusFailed                 = "FAILED"
	statusStopRequested          = "STOP_REQUESTED"
	statusStopped                = "STOPPED"
	statusTrained                = "TRAINED"
	statusReady                  = "READY"
	statusActive                 = "ACTIVE"
	defaultLanguageCode          = "en"
	defaultScore                 = 0.99
	failedMarker                 = "[fail]"
	resourceTypeEndpoint         = "endpoint"
	resourceTypeFlywheel         = "flywheel"
	resourceTypeDataset          = "dataset"
	resourceTypeDocClassifier    = "document-classifier"
	resourceTypeEntityRecognizer = "entity-recognizer"
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
	Configuration         map[string]any
	OutputDataConfig      map[string]any
	InputDataConfig       map[string]any
	FailureReason         string
	DocumentClassifierArn string
	LanguageCode          string
	JobType               string
	JobName               string
	JobArn                string
	DataAccessRoleArn     string
	JobStatus             string
	EntityRecognizerArn   string
	JobID                 string
	TargetEventTypes      []string
	polls                 int
	stopRequested         bool
	shouldFail            bool
}

// Resource stores a Comprehend trainable or hosted resource.
type Resource struct {
	CreatedAt time.Time
	UpdatedAt time.Time
	// TrainingStartTime/TrainingEndTime back DocumentClassifierProperties/
	// EntityRecognizerProperties' TrainingStartTime/TrainingEndTime fields
	// (distinct from SubmitTime/EndTime: real AWS bills the interval between
	// these two separately from the submit-to-completion interval). Left
	// zero for resource types that don't train (endpoint/flywheel/dataset).
	TrainingStartTime time.Time
	TrainingEndTime   time.Time
	Name              string
	Arn               string
	Type              string
	Status            string
	VersionName       string
	ModelArn          string
	FlywheelArn       string
	EndpointArn       string
	DatasetArn        string
	Configuration     map[string]any
	FailureReason     string
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
