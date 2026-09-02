package forecast

import "time"

const (
	statusCreatePending = "CREATE_PENDING"
	statusActive        = "ACTIVE"
	statusCreateFailed  = "CREATE_FAILED"
	// statusStopped is the emulator's convention for a resource that
	// StopResource has transitioned (see UpdateResourceStatus in store.go).
	// Real Amazon Forecast models this per-kind as ACTIVE_STOPPED/
	// CREATE_STOPPED, but this emulator uses a single STOPPED value across
	// every stoppable kind.
	statusStopped = "STOPPED"

	defaultAccountID = "000000000000"
	defaultRegion    = "us-east-1"
)

// fieldPredictorArn is the Amazon Forecast API field name shared by every
// operation that references a Predictor by ARN (CreateForecast,
// CreatePredictorBacktestExportJob, CreateMonitor's alias, ...).
const fieldPredictorArn = "PredictorArn"

// maxResourceNameLen is the maximum number of characters allowed in any Amazon
// Forecast resource name (DatasetName, PredictorName, etc.).
const maxResourceNameLen = 256

type resourceKind string

const (
	kindDatasetGroup            resourceKind = "dataset-group"
	kindDataset                 resourceKind = "dataset"
	kindDatasetImportJob        resourceKind = "dataset-import-job"
	kindPredictor               resourceKind = "predictor"
	kindPredictorBacktestExport resourceKind = "predictor-backtest-export-job"
	kindForecast                resourceKind = "forecast"
	kindForecastExport          resourceKind = "forecast-export-job"
	kindExplainabilityExport    resourceKind = "explainability-export"
	kindWhatIfAnalysis          resourceKind = "what-if-analysis"
	kindWhatIfForecast          resourceKind = "what-if-forecast"
	kindWhatIfForecastExport    resourceKind = "what-if-forecast-export"
	kindMonitor                 resourceKind = "monitor"
	kindExplainability          resourceKind = "explainability"
)

// Resource stores one Forecast API object with AWS response-shaped attributes.
type Resource struct {
	CreatedAt time.Time
	UpdatedAt time.Time
	Data      map[string]any
	ARN       string
	Name      string
	Status    string
	Message   string
	Kind      resourceKind
}

// MonitorEvaluation represents one completed evaluation emitted by a predictor monitor.
type MonitorEvaluation struct {
	CreationTime    time.Time `json:"CreationTime"`
	EvaluationTime  time.Time `json:"EvaluationTime"`
	PredictorEvent  any       `json:"PredictorEvent,omitempty"`
	Message         string    `json:"Message,omitempty"`
	MonitorArn      string    `json:"MonitorArn"`
	MonitorName     string    `json:"MonitorName"`
	ResourceArn     string    `json:"ResourceArn,omitempty"`
	Status          string    `json:"Status"`
	EvaluationState string    `json:"EvaluationState"`
	MetricResults   []any     `json:"MetricResults"`
}

// arnEntry locates a resource by kind and name — used for cross-kind ARN lookups.
type arnEntry struct {
	kind resourceKind
	name string
}
