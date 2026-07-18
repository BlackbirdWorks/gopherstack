package translate

import "time"

// Job status values, matching aws-sdk-go-v2/service/translate/types.JobStatus.
// A freshly started job begins at jobStatusSubmitted and is advanced one step
// per DescribeTextTranslationJob call (see advanceJob), mirroring the
// SUBMITTED -> IN_PROGRESS -> COMPLETED/FAILED lifecycle real Translate jobs
// go through and the pattern established by services/comprehend.
const (
	jobStatusSubmitted     = "SUBMITTED"
	jobStatusInProgress    = "IN_PROGRESS"
	jobStatusCompleted     = "COMPLETED"
	jobStatusFailed        = "FAILED"
	jobStatusStopRequested = "STOP_REQUESTED"
	jobStatusStopped       = "STOPPED"

	// failedJobNameMarker lets tests deterministically drive a job to FAILED
	// by including this marker in JobName, matching services/comprehend's
	// failedMarker convention.
	failedJobNameMarker = "[fail]"
)

// Directionality values, matching aws-sdk-go-v2/service/translate/types.Directionality.
const (
	directionalityUni   = "UNI"
	directionalityMulti = "MULTI"
)

// TerminologyData holds imported terminology file bytes.
type TerminologyData struct {
	Format         string
	Directionality string
	File           []byte
}

// EncryptionKey holds optional KMS encryption key details.
type EncryptionKey struct {
	Type string
	ID   string
}

// Terminology stores a custom terminology resource.
type Terminology struct {
	TerminologyData *TerminologyData
	EncryptionKey   *EncryptionKey
	Tags            map[string]string
	CreatedAt       time.Time
	LastUpdatedAt   time.Time
	ARN             string
	Name            string
	Description     string
	SourceLanguage  string
	Directionality  string
	Format          string
	TargetLanguages []string
	SizeBytes       int
	TermCount       int
}

// ParallelDataConfig holds S3 data config for parallel data.
type ParallelDataConfig struct {
	S3URI  string
	Format string
}

// ParallelData stores a parallel data resource.
type ParallelData struct {
	ParallelDataConfig *ParallelDataConfig
	EncryptionKey      *EncryptionKey
	Tags               map[string]string
	CreatedAt          time.Time
	LastUpdatedAt      time.Time
	ARN                string
	Name               string
	Description        string
	SourceLanguage     string
	Status             string
	TargetLanguages    []string
}

// TranslationJob stores an async translation job.
type TranslationJob struct {
	InputDataConfig   map[string]any
	OutputDataConfig  map[string]any
	Settings          map[string]any
	Tags              map[string]string
	SubmittedAt       time.Time
	EndAt             time.Time
	JobID             string
	JobName           string
	JobStatus         string
	DataAccessRoleARN string
	SourceLanguage    string
	Message           string
	TargetLanguages   []string
	TerminologyNames  []string
	ParallelDataNames []string
	stopRequested     bool
	shouldFail        bool
}
