package forecast

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"maps"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	statusCreatePending = "CREATE_PENDING"
	statusActive        = "ACTIVE"
	statusCreateFailed  = "CREATE_FAILED"

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

// maxResourceNameLen is the maximum number of characters allowed in any Amazon
// Forecast resource name (DatasetName, PredictorName, etc.).
const maxResourceNameLen = 256

// resourceNameRegex matches valid Amazon Forecast resource names:
// only alphanumeric characters, underscores, and hyphens are allowed.
var resourceNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

var (
	// ErrNotFound is returned when a requested Forecast resource is absent.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a Forecast resource name already exists.
	ErrAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrConflict)
	// ErrValidation is returned when a Forecast request is invalid.
	ErrValidation = awserr.New("InvalidInputException", awserr.ErrInvalidParameter)
	// ErrInvalidNextToken is returned when a List* NextToken cannot be decoded.
	// Real Amazon Forecast models InvalidNextTokenException on every List operation.
	ErrInvalidNextToken = awserr.New("InvalidNextTokenException", awserr.ErrInvalidParameter)
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

// arnEntry locates a resource by kind and name — used for cross-kind ARN lookups.
type arnEntry struct {
	kind resourceKind
	name string
}

// InMemoryBackend stores Amazon Forecast state with concurrency-safe transitions.
type InMemoryBackend struct {
	resources   map[resourceKind]*store.Table[Resource]
	evaluations map[string][]MonitorEvaluation
	tags        map[string]map[string]string
	arnIndex    map[string]arnEntry // ARN → (kind, name) for O(1) cross-kind lookup
	registry    *store.Registry
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

	b := &InMemoryBackend{
		evaluations: make(map[string][]MonitorEvaluation),
		tags:        make(map[string]map[string]string),
		arnIndex:    make(map[string]arnEntry),
		registry:    store.NewRegistry(),
		accountID:   accountID,
		region:      region,
	}
	registerAllTables(b)

	return b
}

// Reset clears all in-memory Forecast state. It supports the
// /_gopherstack/reset test hook so suites start from a clean slate.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.evaluations = make(map[string][]MonitorEvaluation)
	b.tags = make(map[string]map[string]string)
	b.arnIndex = make(map[string]arnEntry)
}

// Region returns backend region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns backend account.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

func (b *InMemoryBackend) create(kind resourceKind, name string, data map[string]any, failed bool) (*Resource, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: resource name is required", ErrValidation)
	}

	if len(name) > maxResourceNameLen {
		return nil, fmt.Errorf(
			"%w: resource name must not exceed %d characters; got %d",
			ErrValidation, maxResourceNameLen, len(name),
		)
	}

	if !resourceNameRegex.MatchString(name) {
		return nil, fmt.Errorf(
			"%w: resource name %q contains invalid characters "+
				"(only alphanumeric, underscore, and hyphen are allowed)",
			ErrValidation, name,
		)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	table := b.resources[kind]
	if table.Has(name) {
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
	table.Put(resource)
	b.arnIndex[resource.ARN] = arnEntry{kind: kind, name: name}
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

	b.resources[kind].Delete(resource.Name)
	delete(b.arnIndex, resource.ARN)
	delete(b.evaluations, resource.ARN)
	delete(b.tags, resource.ARN)

	return nil
}

func (b *InMemoryBackend) list(kind resourceKind) []*Resource {
	b.mu.RLock()
	defer b.mu.RUnlock()

	items := b.resources[kind].All()
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

func (b *InMemoryBackend) lookupLocked(kind resourceKind, nameOrARN string) (*Resource, bool) {
	table := b.resources[kind]

	// Fast path: the table is keyed by name, so a name lookup is O(1).
	if resource, ok := table.Get(nameOrARN); ok {
		return resource, true
	}

	// ARN lookup: every ARN is built deterministically as
	// arn:...:forecast:region:account:<kind>/<name>, so reverse it to the name
	// and look that up directly rather than scanning the whole kind's table.
	if name, ok := b.nameFromARN(kind, nameOrARN); ok {
		if resource, found := table.Get(name); found && resource.ARN == nameOrARN {
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
func (b *InMemoryBackend) UpdateResourceStatus(resourceARN, newStatus string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	entry, ok := b.arnIndex[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %q", ErrNotFound, resourceARN)
	}

	resource, _ := b.resources[entry.kind].Get(entry.name)
	resource.Status = newStatus
	resource.UpdatedAt = time.Now().UTC()

	return nil
}

// DeleteResourceTree deletes a resource and all dependent child resources
// transitively, mirroring AWS Forecast behavior.
func (b *InMemoryBackend) DeleteResourceTree(targetARN string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.arnIndex[targetARN]; !ok {
		return fmt.Errorf("%w: resource %q", ErrNotFound, targetARN)
	}

	b.deleteTreeLocked(targetARN)

	return nil
}

// deleteTreeLocked performs a BFS from targetARN to collect all resources that
// directly or indirectly reference it, then removes them all.
// Must be called with b.mu held for write.
func (b *InMemoryBackend) deleteTreeLocked(targetARN string) {
	toDelete := map[string]struct{}{targetARN: {}}
	queue := []string{targetARN}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		for _, table := range b.resources {
			table.Range(func(r *Resource) bool {
				if _, already := toDelete[r.ARN]; already {
					return true
				}

				if arnReferencedBy(r, current) {
					toDelete[r.ARN] = struct{}{}
					queue = append(queue, r.ARN)
				}

				return true
			})
		}
	}

	for arnToDelete := range toDelete {
		if entry, ok := b.arnIndex[arnToDelete]; ok {
			b.resources[entry.kind].Delete(entry.name)
			delete(b.arnIndex, arnToDelete)
			delete(b.evaluations, arnToDelete)
			delete(b.tags, arnToDelete)
		}
	}
}

// arnReferencedBy returns true if any string value in r.Data equals targetARN.
func arnReferencedBy(r *Resource, targetARN string) bool {
	for _, v := range r.Data {
		if s, ok := v.(string); ok && s == targetARN {
			return true
		}
	}

	return false
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

// TagResource adds tags to a resource. Real Amazon Forecast returns
// ResourceNotFoundException when resourceARN does not identify an existing
// resource -- TagResource does not silently create tag state for ARNs no
// resource ever owned.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.arnIndex[resourceARN]; !ok {
		return fmt.Errorf("%w: resource %q", ErrNotFound, resourceARN)
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}
	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a resource. Real Amazon Forecast returns
// ResourceNotFoundException when resourceARN does not identify an existing
// resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.arnIndex[resourceARN]; !ok {
		return fmt.Errorf("%w: resource %q", ErrNotFound, resourceARN)
	}

	if b.tags[resourceARN] != nil {
		for _, k := range tagKeys {
			delete(b.tags[resourceARN], k)
		}
	}

	return nil
}

// ListTagsForResource lists tags for a resource. Real Amazon Forecast returns
// ResourceNotFoundException when resourceARN does not identify an existing
// resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.arnIndex[resourceARN]; !ok {
		return nil, fmt.Errorf("%w: resource %q", ErrNotFound, resourceARN)
	}

	result := make(map[string]string)
	maps.Copy(result, b.tags[resourceARN])

	return result, nil
}
