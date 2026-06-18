package forecast

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"maps"
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

	// backtestWindowDuration is the synthetic span between a backtest window's
	// start and end in GetAccuracyMetrics responses.
	backtestWindowDuration = 24 * time.Hour

	// Synthetic accuracy-metric generation. The metrics returned by
	// GetAccuracyMetrics are deterministic, derived from a per-window seed so
	// the same resource always yields the same values. The constants below
	// name the otherwise-magic numbers used in that derivation.

	// windowSeedPrime is a prime multiplier mixed into the seed to vary
	// metrics between backtest windows.
	windowSeedPrime = 7919

	// Per-metric base values and the modulus/scale used to spread the seed
	// across a small synthetic range.
	rmseBase       = 10.0
	rmseSeedMod    = 500
	rmseSeedScale  = 10.0
	wapeBase       = 0.05
	wapeSeedMod    = 200
	wapeSeedScale  = 1000.0
	mapeBase       = 0.10
	mapeSeedMod    = 150
	mapeSeedScale  = 1000.0
	maseBase       = 0.50
	maseSeedMod    = 300
	maseSeedScale  = 1000.0
	lossValueBase  = 0.02
	lossValueMod   = 100
	lossValueScale = 1000.0
	itemCountBase  = 100
	itemCountMod   = 900
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

// InMemoryBackend stores Amazon Forecast state with concurrency-safe transitions.
type InMemoryBackend struct {
	resources   map[resourceKind]map[string]*Resource
	evaluations map[string][]MonitorEvaluation
	tags        map[string]map[string]string
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
		tags:        make(map[string]map[string]string),
		accountID:   accountID,
		region:      region,
	}
}

// Reset clears all in-memory Forecast state. It supports the
// /_gopherstack/reset test hook so suites start from a clean slate.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.resources = make(map[resourceKind]map[string]*Resource)
	b.evaluations = make(map[string][]MonitorEvaluation)
	b.tags = make(map[string]map[string]string)
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

	if resource.Status == statusCreatePending {
		resource.Status = statusActive
		resource.UpdatedAt = time.Now().UTC()
	}

	return cloneResource(resource), nil
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
	items := b.resources[kind]

	// Fast path: the map is keyed by name, so a name lookup is O(1).
	if resource, ok := items[nameOrARN]; ok {
		return resource, true
	}

	// ARN lookup: every ARN is built deterministically as
	// arn:...:forecast:region:account:<kind>/<name>, so reverse it to the name
	// and look that up directly rather than scanning the whole kind map.
	if name, ok := b.nameFromARN(kind, nameOrARN); ok {
		if resource, found := items[name]; found && resource.ARN == nameOrARN {
			return resource, true
		}
	}

	return nil, false
}

// nameFromARN extracts the resource name from a Forecast ARN of the form
// arn:...:forecast:region:account:<kind>/<name>. It returns false if the string
// is not an ARN with the expected "<kind>/" resource prefix.
func (b *InMemoryBackend) nameFromARN(kind resourceKind, candidate string) (string, bool) {
	prefix := arn.Build("forecast", b.region, b.accountID, string(kind)+"/")

	name, found := strings.CutPrefix(candidate, prefix)
	if !found || name == "" {
		return "", false
	}

	return name, true
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

// UpdateResourceStatus handles StopResource and ResumeResource.
func (b *InMemoryBackend) UpdateResourceStatus(arn, newStatus string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, kinds := range b.resources {
		for _, resource := range kinds {
			if resource.ARN == arn {
				resource.Status = newStatus
				resource.UpdatedAt = time.Now().UTC()

				return nil
			}
		}
	}

	return fmt.Errorf("%w: resource %q", ErrNotFound, arn)
}

// DeleteResourceTree deletes a resource and its children (mock implementation deleting just the resource).
func (b *InMemoryBackend) DeleteResourceTree(arn string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, kinds := range b.resources {
		for _, resource := range kinds {
			if resource.ARN == arn {
				resource.Status = statusDeleting
				resource.UpdatedAt = time.Now().UTC()

				return nil
			}
		}
	}

	return fmt.Errorf("%w: resource %q", ErrNotFound, arn)
}

// GetAccuracyMetrics returns deterministic backtest accuracy metrics for a
// predictor, modeled on the AWS Forecast GetAccuracyMetrics response shape
// (PredictorEvaluationResults -> TestWindows -> Metrics with RMSE, weighted
// quantile losses, and WAPE/MAPE/MASE error metrics). Values are derived from a
// stable hash of the predictor ARN so repeated calls return identical numbers,
// which is what a Terraform/SDK client comparing state expects. This exceeds
// LocalStack, which returns no evaluation results at all.
func (b *InMemoryBackend) GetAccuracyMetrics(predictorArn string) (map[string]any, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	resource, ok := b.lookupLocked(kindPredictor, predictorArn)
	if !ok {
		return nil, fmt.Errorf("%w: predictor %q", ErrNotFound, predictorArn)
	}

	quantiles := predictorQuantiles(resource)
	seed := stableSeed(resource.ARN)

	// Two backtest windows is AWS's default (NumberOfBacktestWindows defaults to 1,
	// but the response always carries at least the configured count).
	numWindows := backtestWindowCount(resource)
	windows := make([]map[string]any, 0, numWindows)

	for w := range numWindows {
		windowSeed := seed + uint32(w)*windowSeedPrime

		rmse := rmseBase + float64(windowSeed%rmseSeedMod)/rmseSeedScale
		wape := wapeBase + float64(windowSeed%wapeSeedMod)/wapeSeedScale
		mape := mapeBase + float64(windowSeed%mapeSeedMod)/mapeSeedScale
		mase := maseBase + float64(windowSeed%maseSeedMod)/maseSeedScale

		quantileLosses := make([]map[string]any, 0, len(quantiles))
		for i, q := range quantiles {
			quantileLosses = append(quantileLosses, map[string]any{
				"Quantile":  q,
				"LossValue": lossValueBase + float64((windowSeed+uint32(i))%lossValueMod)/lossValueScale,
			})
		}

		windows = append(windows, map[string]any{
			"EvaluationType":  evaluationTypeForWindow(w),
			"ItemCount":       int64(itemCountBase + windowSeed%itemCountMod),
			"TestWindowStart": resource.CreatedAt.UTC().Format(time.RFC3339),
			"TestWindowEnd":   resource.CreatedAt.UTC().Add(backtestWindowDuration).Format(time.RFC3339),
			"Metrics": map[string]any{
				"RMSE":                   rmse,
				"WeightedQuantileLosses": quantileLosses,
				"ErrorMetrics": []map[string]any{
					{
						"ForecastType": "mean",
						"WAPE":         wape,
						"MAPE":         mape,
						"MASE":         mase,
						"RMSE":         rmse,
					},
				},
				"AverageWeightedQuantileLoss": averageQuantileLoss(quantileLosses),
			},
		})
	}

	return map[string]any{
		"PredictorEvaluationResults": []map[string]any{
			{
				"AlgorithmArn": "arn:aws:forecast:::algorithm/CNN-QR",
				"TestWindows":  windows,
			},
		},
		"IsAutoPredictor": true,
	}, nil
}

// stableSeed returns a deterministic 32-bit value derived from s.
func stableSeed(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))

	return h.Sum32()
}

// predictorQuantiles returns the forecast quantiles configured on the predictor,
// defaulting to AWS's default set when none were provided.
func predictorQuantiles(r *Resource) []string {
	if raw, ok := r.Data["ForecastTypes"].([]any); ok && len(raw) > 0 {
		out := make([]string, 0, len(raw))

		for _, v := range raw {
			if s, isStr := v.(string); isStr && s != "" {
				out = append(out, s)
			}
		}

		if len(out) > 0 {
			return out
		}
	}

	return []string{"0.1", "0.5", "0.9"}
}

// backtestWindowCount returns the configured number of backtest windows
// (defaulting to 1, AWS's default).
func backtestWindowCount(r *Resource) int {
	if eval, ok := r.Data["EvaluationParameters"].(map[string]any); ok {
		if n, isNum := eval["NumberOfBacktestWindows"].(float64); isNum && n >= 1 {
			return int(n)
		}
	}

	return 1
}

func evaluationTypeForWindow(window int) string {
	if window == 0 {
		return "SUMMARY"
	}

	return "COMPUTED"
}

func averageQuantileLoss(losses []map[string]any) float64 {
	if len(losses) == 0 {
		return 0
	}

	var sum float64

	for _, l := range losses {
		if v, ok := l["LossValue"].(float64); ok {
			sum += v
		}
	}

	return sum / float64(len(losses))
}

// TagResource adds tags to a resource.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.tags[arn] == nil {
		b.tags[arn] = make(map[string]string)
	}
	maps.Copy(b.tags[arn], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.tags[arn] != nil {
		for _, k := range tagKeys {
			delete(b.tags[arn], k)
		}
	}

	return nil
}

// ListTagsForResource lists tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make(map[string]string)
	maps.Copy(result, b.tags[arn])

	return result, nil
}
