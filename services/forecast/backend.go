package forecast

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	statusCreatePending = "CREATE_PENDING"
	statusActive        = "ACTIVE"
	statusCreateFailed  = "CREATE_FAILED"
	statusDeleting      = "DELETING"

	defaultAccountID = "000000000000"
	defaultRegion    = "us-east-1"
)

var (
	// ErrNotFound is returned when a requested Forecast resource is absent.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a Forecast resource name already exists.
	ErrAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrConflict)
	// ErrValidation is returned when a Forecast request is invalid.
	ErrValidation = awserr.New("InvalidInputException", awserr.ErrInvalidParameter)
)

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
)

// Resource stores one Forecast API object with AWS response-shaped attributes.
type Resource struct {
	CreatedAt time.Time
	UpdatedAt time.Time
	Data      map[string]any
	ARN       string
	Name      string
	Status    string
	Kind      resourceKind
}

// MonitorEvaluation represents one completed evaluation emitted by a predictor monitor.
type MonitorEvaluation struct {
	MetricResults   []any     `json:"MetricResults"`
	PredictorEvent  any       `json:"PredictorEvent,omitempty"`
	Message         string    `json:"Message,omitempty"`
	MonitorArn      string    `json:"MonitorArn"`
	MonitorName     string    `json:"MonitorName"`
	ResourceArn     string    `json:"ResourceArn,omitempty"`
	Status          string    `json:"Status"`
	EvaluationState string    `json:"EvaluationState"`
	CreationTime    time.Time `json:"CreationTime"`
	EvaluationTime  time.Time `json:"EvaluationTime"`
}

// InMemoryBackend stores Amazon Forecast state with concurrency-safe transitions.
type InMemoryBackend struct {
	resources   map[resourceKind]map[string]*Resource
	evaluations map[string][]MonitorEvaluation
	accountID   string
	region      string
	mu          sync.RWMutex
}

// NewInMemoryBackend returns a stateful Amazon Forecast backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	if accountID == "" {
		accountID = defaultAccountID
	}
	if region == "" {
		region = defaultRegion
	}

	return &InMemoryBackend{
		resources:   make(map[resourceKind]map[string]*Resource),
		evaluations: make(map[string][]MonitorEvaluation),
		accountID:   accountID,
		region:      region,
	}
}

// Region returns backend region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns backend account.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

func (b *InMemoryBackend) create(kind resourceKind, name string, data map[string]any, failed bool) (*Resource, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: resource name is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	items := b.ensureKind(kind)
	if _, ok := items[name]; ok {
		return nil, fmt.Errorf("%w: %s %q", ErrAlreadyExists, kind, name)
	}

	now := time.Now().UTC()
	status := statusCreatePending
	if failed {
		status = statusCreateFailed
	}

	resource := &Resource{
		CreatedAt: now,
		UpdatedAt: now,
		Data:      cloneMap(data),
		ARN:       arn.Build("forecast", b.region, b.accountID, string(kind)+"/"+name),
		Name:      name,
		Status:    status,
		Kind:      kind,
	}
	items[name] = resource
	if kind == kindMonitor {
		b.evaluations[resource.ARN] = []MonitorEvaluation{newEvaluation(resource)}
	}

	return cloneResource(resource), nil
}

func (b *InMemoryBackend) describe(kind resourceKind, nameOrARN string) (*Resource, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	resource, ok := b.lookupLocked(kind, nameOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: %s %q", ErrNotFound, kind, nameOrARN)
	}

	result := cloneResource(resource)
	if resource.Status == statusCreatePending {
		resource.Status = statusActive
		resource.UpdatedAt = time.Now().UTC()
	}

	return result, nil
}

func (b *InMemoryBackend) update(kind resourceKind, nameOrARN string, data map[string]any) (*Resource, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	resource, ok := b.lookupLocked(kind, nameOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: %s %q", ErrNotFound, kind, nameOrARN)
	}

	for key, value := range data {
		resource.Data[key] = cloneValue(value)
	}
	resource.Status = statusActive
	resource.UpdatedAt = time.Now().UTC()

	return cloneResource(resource), nil
}

func (b *InMemoryBackend) delete(kind resourceKind, nameOrARN string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	resource, ok := b.lookupLocked(kind, nameOrARN)
	if !ok {
		return fmt.Errorf("%w: %s %q", ErrNotFound, kind, nameOrARN)
	}

	resource.Status = statusDeleting
	resource.UpdatedAt = time.Now().UTC()

	return nil
}

func (b *InMemoryBackend) list(kind resourceKind) []*Resource {
	b.mu.RLock()
	defer b.mu.RUnlock()

	items := b.resources[kind]
	result := make([]*Resource, 0, len(items))
	for _, resource := range items {
		result = append(result, cloneResource(resource))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })

	return result
}

func (b *InMemoryBackend) listMonitorEvaluations(monitorARN string) ([]MonitorEvaluation, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.lookupLocked(kindMonitor, monitorARN); !ok {
		return nil, fmt.Errorf("%w: monitor %q", ErrNotFound, monitorARN)
	}

	evaluations := b.evaluations[monitorARN]
	result := make([]MonitorEvaluation, len(evaluations))
	copy(result, evaluations)

	return result, nil
}

func (b *InMemoryBackend) ensureKind(kind resourceKind) map[string]*Resource {
	if b.resources[kind] == nil {
		b.resources[kind] = make(map[string]*Resource)
	}

	return b.resources[kind]
}

func (b *InMemoryBackend) lookupLocked(kind resourceKind, nameOrARN string) (*Resource, bool) {
	for name, resource := range b.resources[kind] {
		if name == nameOrARN || resource.ARN == nameOrARN {
			return resource, true
		}
	}

	return nil, false
}

func newEvaluation(monitor *Resource) MonitorEvaluation {
	return MonitorEvaluation{
		CreationTime:    monitor.CreatedAt,
		EvaluationTime:  monitor.CreatedAt,
		MonitorArn:      monitor.ARN,
		MonitorName:     monitor.Name,
		ResourceArn:     stringValue(monitor.Data["ResourceArn"]),
		Status:          statusActive,
		MetricResults:   []any{},
		EvaluationState: "SUCCESS",
	}
}

func cloneResource(resource *Resource) *Resource {
	copyResource := *resource
	copyResource.Data = cloneMap(resource.Data)

	return &copyResource
}

func cloneMap(data map[string]any) map[string]any {
	if data == nil {
		return make(map[string]any)
	}

	encoded, err := json.Marshal(data)
	if err != nil {
		return make(map[string]any)
	}

	var result map[string]any
	if unmarshalErr := json.Unmarshal(encoded, &result); unmarshalErr != nil {
		return make(map[string]any)
	}

	return result
}

func cloneValue(value any) any {
	return cloneMap(map[string]any{"value": value})["value"]
}

func stringValue(value any) string {
	result, _ := value.(string)

	return result
}
