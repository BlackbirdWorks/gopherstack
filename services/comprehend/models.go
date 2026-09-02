package comprehend

import "time"

const (
	statusSubmitted  = "SUBMITTED"
	statusInProgress = "IN_PROGRESS"
	statusCompleted  = "COMPLETED"
	statusFailed     = "FAILED"

	statusStopRequested = "STOP_REQUESTED"
	statusStopped       = "STOPPED"
	statusTrained       = "TRAINED"

	// statusActive is types.FlywheelStatusActive -- a freshly created
	// Flywheel's steady-state value (types/enums.go:352-360). It is NOT a
	// valid types.EndpointStatus value (see statusEndpointInService).
	statusActive = "ACTIVE"

	// statusEndpointInService is types.EndpointStatusInService, the real
	// steady-state value for a freshly created Endpoint (types/enums.go:
	// 248-256). EndpointProperties.Status's doc comment mentions "Ready" but
	// the generated enum has no such value.
	statusEndpointInService = "IN_SERVICE"

	// statusFlywheelIterationTraining/Evaluating are
	// types.FlywheelIterationStatusTraining/Evaluating (types/enums.go:
	// 325-334) -- distinct from the generic SUBMITTED/IN_PROGRESS vocabulary
	// above, which FlywheelIterationStatus does not share.
	statusFlywheelIterationTraining   = "TRAINING"
	statusFlywheelIterationEvaluating = "EVALUATING"

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
