package personalize

import (
	"fmt"
	"hash/fnv"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	statusActive         = "ACTIVE"
	statusCreatePending  = "CREATE PENDING"
	statusCreateProgress = "CREATE IN_PROGRESS"
	statusDeletePending  = "DELETE PENDING"
	statusCreateFailed   = "CREATE FAILED"
	statusUpdatePending  = "UPDATE PENDING"
	statusUpdateProgress = "UPDATE IN_PROGRESS"
	statusStopped        = "STOPPED"
	statusStopPending    = "STOP PENDING"

	defaultAccountID = "000000000000"
	defaultRegion    = "us-east-1"

	mockMetricValue = 0.5
)

var (
	// ErrNotFound is returned when a requested resource is absent.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource name already exists.
	ErrAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when a request is invalid.
	ErrValidation = awserr.New("InvalidInputException", awserr.ErrInvalidParameter)
	// ErrInUse is returned when a resource cannot be deleted because it is in use.
	ErrInUse = awserr.New("ResourceInUseException", awserr.ErrConflict)
)

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
	CreationDateTime    time.Time
	LastUpdatedDateTime time.Time
	SolutionArn         string
	DatasetGroupArn     string
	Name                string
	RecipeArn           string
	Status              string
	PerformAutoML       bool
	PerformHPO          bool
}

// SolutionVersion stores a trained Amazon Personalize solution version.
type SolutionVersion struct {
	CreationDateTime    time.Time
	LastUpdatedDateTime time.Time
	SolutionVersionArn  string
	SolutionArn         string
	Status              string
	TrainingMode        string
	TrainingHours       float64
}

// Campaign stores an Amazon Personalize campaign.
type Campaign struct {
	CreationDateTime    time.Time
	LastUpdatedDateTime time.Time
	CampaignArn         string
	SolutionVersionArn  string
	Name                string
	Status              string
	MinProvisionedTPS   int32
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
	RecommenderArn                     string
	DatasetGroupArn                    string
	RecipeArn                          string
	Name                               string
	Status                             string
	MinRecommendationRequestsPerSecond int32
}

// MetricAttribution stores an Amazon Personalize metric attribution.
type MetricAttribution struct {
	CreationDateTime     time.Time
	LastUpdatedDateTime  time.Time
	MetricAttributionArn string
	DatasetGroupArn      string
	Name                 string
	MetricsOutputConfig  map[string]any
	Status               string
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

// storedFeatureTransformation represents a built-in feature transformation.
type storedFeatureTransformation struct {
	CreationTime  time.Time
	LastUpdated   time.Time
	DefaultValues map[string]string
	ARN           string
	Name          string
	Status        string
}

// InMemoryBackend stores Amazon Personalize state.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	registry *store.Registry

	datasetGroups          *store.Table[DatasetGroup]
	datasets               *store.Table[Dataset]
	schemas                *store.Table[Schema]
	solutions              *store.Table[Solution]
	solutionVersions       *store.Table[SolutionVersion]
	campaigns              *store.Table[Campaign]
	datasetImportJobs      *store.Table[DatasetImportJob]
	datasetExportJobs      *store.Table[DatasetExportJob]
	batchInferenceJobs     *store.Table[BatchInferenceJob]
	batchSegmentJobs       *store.Table[BatchSegmentJob]
	eventTrackers          *store.Table[EventTracker]
	filters                *store.Table[Filter]
	recommenders           *store.Table[Recommender]
	metricAttributions     *store.Table[MetricAttribution]
	dataDeletionJobs       *store.Table[DataDeletionJob]
	featureTransformations *store.Table[storedFeatureTransformation]

	// tags is left as a plain map: its value type is map[string]string, not
	// *T, so it does not fit store.Table's keyed-by-identity-value shape.
	// See persistence.go.
	tags map[string]map[string]string

	accountID string
	region    string
}

// NewInMemoryBackend returns a stateful Amazon Personalize backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	if accountID == "" {
		accountID = defaultAccountID
	}
	if region == "" {
		region = defaultRegion
	}

	b := &InMemoryBackend{
		tags:      make(map[string]map[string]string),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("personalize"),
		registry:  store.NewRegistry(),
	}
	registerAllTables(b)
	b.seedBuiltinFeatureTransformations()

	return b
}

// seedBuiltinFeatureTransformations (re-)populates featureTransformations
// with the read-only builtins. Called from NewInMemoryBackend and from Reset
// (since registry.ResetAll() clears the table along with every other one).
func (b *InMemoryBackend) seedBuiltinFeatureTransformations() {
	epoch := time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, name := range []string{
		"aws-feature-transformation",
		"aws-explicit-contextual-bandits-feature-transformation",
	} {
		ftArn := arn.Build("personalize", b.region, b.accountID, "feature-transformation/"+name)
		b.featureTransformations.Put(&storedFeatureTransformation{
			ARN:          ftArn,
			Name:         name,
			Status:       statusActive,
			CreationTime: epoch,
			LastUpdated:  epoch,
		})
	}
}

// GetFeatureTransformation looks up a feature transformation by ARN or name.
func (b *InMemoryBackend) GetFeatureTransformation(arnOrName string) (*storedFeatureTransformation, error) {
	b.mu.RLock("GetFeatureTransformation")
	defer b.mu.RUnlock()

	for _, ft := range b.featureTransformations.All() {
		if ft.ARN == arnOrName || ft.Name == arnOrName {
			return ft, nil
		}
	}

	return nil, fmt.Errorf("%w: feature transformation %q not found", ErrNotFound, arnOrName)
}

// Reset clears all in-memory Personalize state for the /_gopherstack/reset
// test hook so suites start from a clean slate.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.tags = make(map[string]map[string]string)
	// featureTransformations are read-only builtins; re-seed to restore after
	// registry.ResetAll() cleared the table along with every other one.
	b.seedBuiltinFeatureTransformations()
}

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

func (b *InMemoryBackend) personalizeARN(resource, name string) string {
	return arn.Build("personalize", b.region, b.accountID, resource+"/"+name)
}

// --- DatasetGroup ---

// CreateDatasetGroup creates a new dataset group.
func (b *InMemoryBackend) CreateDatasetGroup(
	name, domain, kmsKeyArn, roleArn string,
	tags map[string]string,
) (*DatasetGroup, error) {
	b.mu.Lock("CreateDatasetGroup")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}
	if b.datasetGroups.Has(name) {
		return nil, fmt.Errorf("%w: dataset group %q already exists", ErrAlreadyExists, name)
	}

	now := time.Now().UTC()
	dg := &DatasetGroup{
		DatasetGroupArn:     b.personalizeARN("dataset-group", name),
		Name:                name,
		Domain:              domain,
		KmsKeyArn:           kmsKeyArn,
		RoleArn:             roleArn,
		Status:              statusActive,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.datasetGroups.Put(dg)
	if len(tags) > 0 {
		b.tags[dg.DatasetGroupArn] = copyStringMap(tags)
	}

	return dg, nil
}

// DescribeDatasetGroup returns a dataset group by name or ARN.
func (b *InMemoryBackend) DescribeDatasetGroup(nameOrArn string) (*DatasetGroup, error) {
	b.mu.RLock("DescribeDatasetGroup")
	defer b.mu.RUnlock()

	if dg := b.findDatasetGroup(nameOrArn); dg != nil {
		return dg, nil
	}

	return nil, fmt.Errorf("%w: dataset group %q not found", ErrNotFound, nameOrArn)
}

// DeleteDatasetGroup removes a dataset group.
func (b *InMemoryBackend) DeleteDatasetGroup(nameOrArn string) error {
	b.mu.Lock("DeleteDatasetGroup")
	defer b.mu.Unlock()

	dg := b.findDatasetGroup(nameOrArn)
	if dg == nil {
		return fmt.Errorf("%w: dataset group %q not found", ErrNotFound, nameOrArn)
	}
	b.datasetGroups.Delete(dg.Name)
	delete(b.tags, dg.DatasetGroupArn)

	return nil
}

// ListDatasetGroups returns all dataset groups.
func (b *InMemoryBackend) ListDatasetGroups(maxResults int, nextToken string) ([]*DatasetGroup, string) {
	b.mu.RLock("ListDatasetGroups")
	defer b.mu.RUnlock()

	return paginateItems(b.datasetGroups.Snapshot(), datasetGroupKeyFn, maxResults, nextToken)
}

func (b *InMemoryBackend) findDatasetGroup(nameOrArn string) *DatasetGroup {
	if dg, ok := b.datasetGroups.Get(nameOrArn); ok {
		return dg
	}
	for _, dg := range b.datasetGroups.All() {
		if dg.DatasetGroupArn == nameOrArn {
			return dg
		}
	}

	return nil
}

// --- Dataset ---

// CreateDataset creates a new dataset.
func (b *InMemoryBackend) CreateDataset(
	name, datasetGroupArn, datasetType, schemaArn string,
	tags map[string]string,
) (*Dataset, error) {
	b.mu.Lock("CreateDataset")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}
	if b.datasets.Has(name) {
		return nil, fmt.Errorf("%w: dataset %q already exists", ErrAlreadyExists, name)
	}

	now := time.Now().UTC()
	ds := &Dataset{
		DatasetArn:          b.personalizeARN("dataset", name),
		Name:                name,
		DatasetGroupArn:     datasetGroupArn,
		DatasetType:         datasetType,
		SchemaArn:           schemaArn,
		Status:              statusActive,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.datasets.Put(ds)
	if len(tags) > 0 {
		b.tags[ds.DatasetArn] = copyStringMap(tags)
	}

	return ds, nil
}

// DescribeDataset returns a dataset by name or ARN.
func (b *InMemoryBackend) DescribeDataset(nameOrArn string) (*Dataset, error) {
	b.mu.RLock("DescribeDataset")
	defer b.mu.RUnlock()

	if ds := b.findDataset(nameOrArn); ds != nil {
		return ds, nil
	}

	return nil, fmt.Errorf("%w: dataset %q not found", ErrNotFound, nameOrArn)
}

// UpdateDataset updates a dataset's schema.
func (b *InMemoryBackend) UpdateDataset(nameOrArn, schemaArn string) (*Dataset, error) {
	b.mu.Lock("UpdateDataset")
	defer b.mu.Unlock()

	ds := b.findDataset(nameOrArn)
	if ds == nil {
		return nil, fmt.Errorf("%w: dataset %q not found", ErrNotFound, nameOrArn)
	}
	if schemaArn != "" {
		ds.SchemaArn = schemaArn
	}
	ds.LastUpdatedDateTime = time.Now().UTC()

	return ds, nil
}

// DeleteDataset removes a dataset.
func (b *InMemoryBackend) DeleteDataset(nameOrArn string) error {
	b.mu.Lock("DeleteDataset")
	defer b.mu.Unlock()

	ds := b.findDataset(nameOrArn)
	if ds == nil {
		return fmt.Errorf("%w: dataset %q not found", ErrNotFound, nameOrArn)
	}
	b.datasets.Delete(ds.Name)
	delete(b.tags, ds.DatasetArn)

	return nil
}

// ListDatasets returns datasets, optionally filtered by dataset group ARN.
func (b *InMemoryBackend) ListDatasets(datasetGroupArn string, maxResults int, nextToken string) ([]*Dataset, string) {
	b.mu.RLock("ListDatasets")
	defer b.mu.RUnlock()

	all := b.datasets.Snapshot()
	filtered := make([]*Dataset, 0, len(all))
	for _, ds := range all {
		if datasetGroupArn == "" || ds.DatasetGroupArn == datasetGroupArn {
			filtered = append(filtered, ds)
		}
	}

	return paginateItems(filtered, datasetKeyFn, maxResults, nextToken)
}

func (b *InMemoryBackend) findDataset(nameOrArn string) *Dataset {
	if ds, ok := b.datasets.Get(nameOrArn); ok {
		return ds
	}
	for _, ds := range b.datasets.All() {
		if ds.DatasetArn == nameOrArn {
			return ds
		}
	}

	return nil
}

// --- Schema ---

// CreateSchema creates a new schema.
func (b *InMemoryBackend) CreateSchema(name, schema, domain string) (*Schema, error) {
	b.mu.Lock("CreateSchema")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}
	if b.schemas.Has(name) {
		return nil, fmt.Errorf("%w: schema %q already exists", ErrAlreadyExists, name)
	}

	now := time.Now().UTC()
	s := &Schema{
		SchemaArn:           b.personalizeARN("schema", name),
		Name:                name,
		Schema:              schema,
		Domain:              domain,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.schemas.Put(s)

	return s, nil
}

// DescribeSchema returns a schema by name or ARN.
func (b *InMemoryBackend) DescribeSchema(nameOrArn string) (*Schema, error) {
	b.mu.RLock("DescribeSchema")
	defer b.mu.RUnlock()

	if s := b.findSchema(nameOrArn); s != nil {
		return s, nil
	}

	return nil, fmt.Errorf("%w: schema %q not found", ErrNotFound, nameOrArn)
}

// DeleteSchema removes a schema.
func (b *InMemoryBackend) DeleteSchema(nameOrArn string) error {
	b.mu.Lock("DeleteSchema")
	defer b.mu.Unlock()

	s := b.findSchema(nameOrArn)
	if s == nil {
		return fmt.Errorf("%w: schema %q not found", ErrNotFound, nameOrArn)
	}
	b.schemas.Delete(s.Name)

	return nil
}

// ListSchemas returns all schemas.
func (b *InMemoryBackend) ListSchemas(maxResults int, nextToken string) ([]*Schema, string) {
	b.mu.RLock("ListSchemas")
	defer b.mu.RUnlock()

	return paginateItems(b.schemas.Snapshot(), schemaKeyFn, maxResults, nextToken)
}

func (b *InMemoryBackend) findSchema(nameOrArn string) *Schema {
	if s, ok := b.schemas.Get(nameOrArn); ok {
		return s
	}
	for _, s := range b.schemas.All() {
		if s.SchemaArn == nameOrArn {
			return s
		}
	}

	return nil
}

// --- Solution ---

// CreateSolution creates a new solution.
func (b *InMemoryBackend) CreateSolution(
	name, datasetGroupArn, recipeArn string,
	performAutoML, performHPO bool,
	tags map[string]string,
) (*Solution, error) {
	b.mu.Lock("CreateSolution")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}
	if b.solutions.Has(name) {
		return nil, fmt.Errorf("%w: solution %q already exists", ErrAlreadyExists, name)
	}

	now := time.Now().UTC()
	sol := &Solution{
		SolutionArn:         b.personalizeARN("solution", name),
		Name:                name,
		DatasetGroupArn:     datasetGroupArn,
		RecipeArn:           recipeArn,
		PerformAutoML:       performAutoML,
		PerformHPO:          performHPO,
		Status:              statusActive,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.solutions.Put(sol)
	if len(tags) > 0 {
		b.tags[sol.SolutionArn] = copyStringMap(tags)
	}

	return sol, nil
}

// DescribeSolution returns a solution by name or ARN.
func (b *InMemoryBackend) DescribeSolution(nameOrArn string) (*Solution, error) {
	b.mu.RLock("DescribeSolution")
	defer b.mu.RUnlock()

	if sol := b.findSolution(nameOrArn); sol != nil {
		return sol, nil
	}

	return nil, fmt.Errorf("%w: solution %q not found", ErrNotFound, nameOrArn)
}

// UpdateSolution updates solution configuration.
func (b *InMemoryBackend) UpdateSolution(nameOrArn string, performAutoML, performHPO bool) (*Solution, error) {
	b.mu.Lock("UpdateSolution")
	defer b.mu.Unlock()

	sol := b.findSolution(nameOrArn)
	if sol == nil {
		return nil, fmt.Errorf("%w: solution %q not found", ErrNotFound, nameOrArn)
	}
	sol.PerformAutoML = performAutoML
	sol.PerformHPO = performHPO
	sol.LastUpdatedDateTime = time.Now().UTC()

	return sol, nil
}

// DeleteSolution removes a solution.
func (b *InMemoryBackend) DeleteSolution(nameOrArn string) error {
	b.mu.Lock("DeleteSolution")
	defer b.mu.Unlock()

	sol := b.findSolution(nameOrArn)
	if sol == nil {
		return fmt.Errorf("%w: solution %q not found", ErrNotFound, nameOrArn)
	}
	b.solutions.Delete(sol.Name)
	delete(b.tags, sol.SolutionArn)

	return nil
}

// ListSolutions returns solutions, optionally filtered by dataset group ARN.
func (b *InMemoryBackend) ListSolutions(
	datasetGroupArn string,
	maxResults int,
	nextToken string,
) ([]*Solution, string) {
	b.mu.RLock("ListSolutions")
	defer b.mu.RUnlock()

	all := b.solutions.Snapshot()
	filtered := make([]*Solution, 0, len(all))
	for _, sol := range all {
		if datasetGroupArn == "" || sol.DatasetGroupArn == datasetGroupArn {
			filtered = append(filtered, sol)
		}
	}

	return paginateItems(filtered, solutionKeyFn, maxResults, nextToken)
}

func (b *InMemoryBackend) findSolution(nameOrArn string) *Solution {
	if sol, ok := b.solutions.Get(nameOrArn); ok {
		return sol
	}
	for _, sol := range b.solutions.All() {
		if sol.SolutionArn == nameOrArn {
			return sol
		}
	}

	return nil
}

// --- SolutionVersion ---

// CreateSolutionVersion creates a new solution version.
func (b *InMemoryBackend) CreateSolutionVersion(
	solutionArn, trainingMode string,
	tags map[string]string,
) (*SolutionVersion, error) {
	b.mu.Lock("CreateSolutionVersion")
	defer b.mu.Unlock()

	if solutionArn == "" {
		return nil, fmt.Errorf("%w: solutionArn is required", ErrValidation)
	}

	versionID := uuid.New().String()
	now := time.Now().UTC()
	sv := &SolutionVersion{
		SolutionVersionArn:  solutionArn + "/" + versionID,
		SolutionArn:         solutionArn,
		Status:              statusActive,
		TrainingMode:        trainingMode,
		TrainingHours:       mockMetricValue,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.solutionVersions.Put(sv)
	if len(tags) > 0 {
		b.tags[sv.SolutionVersionArn] = copyStringMap(tags)
	}

	return sv, nil
}

// DescribeSolutionVersion returns a solution version by ARN.
func (b *InMemoryBackend) DescribeSolutionVersion(svArn string) (*SolutionVersion, error) {
	b.mu.RLock("DescribeSolutionVersion")
	defer b.mu.RUnlock()

	sv, ok := b.solutionVersions.Get(svArn)
	if !ok {
		return nil, fmt.Errorf("%w: solution version %q not found", ErrNotFound, svArn)
	}

	return sv, nil
}

// DeleteSolutionVersion removes a solution version.
func (b *InMemoryBackend) DeleteSolutionVersion(svArn string) error {
	b.mu.Lock("DeleteSolutionVersion")
	defer b.mu.Unlock()

	if !b.solutionVersions.Has(svArn) {
		return fmt.Errorf("%w: solution version %q not found", ErrNotFound, svArn)
	}
	b.solutionVersions.Delete(svArn)
	delete(b.tags, svArn)

	return nil
}

// ListSolutionVersions returns solution versions for a given solution ARN.
func (b *InMemoryBackend) ListSolutionVersions(
	solutionArn string,
	maxResults int,
	nextToken string,
) ([]*SolutionVersion, string) {
	b.mu.RLock("ListSolutionVersions")
	defer b.mu.RUnlock()

	all := b.solutionVersions.Snapshot()
	filtered := make([]*SolutionVersion, 0, len(all))
	for _, sv := range all {
		if solutionArn == "" || sv.SolutionArn == solutionArn {
			filtered = append(filtered, sv)
		}
	}

	return paginateItems(filtered, solutionVersionKeyFn, maxResults, nextToken)
}

// StopSolutionVersionCreation transitions a solution version to STOPPED.
func (b *InMemoryBackend) StopSolutionVersionCreation(svArn string) error {
	b.mu.Lock("StopSolutionVersionCreation")
	defer b.mu.Unlock()

	sv, ok := b.solutionVersions.Get(svArn)
	if !ok {
		return fmt.Errorf("%w: solution version %q not found", ErrNotFound, svArn)
	}
	sv.Status = statusStopped
	sv.LastUpdatedDateTime = time.Now().UTC()

	return nil
}

// GetSolutionMetrics returns deterministic accuracy metrics for a solution version.
// Values are derived from the ARN hash so each solution version gets distinct (but stable) metrics.
func (b *InMemoryBackend) GetSolutionMetrics(svArn string) (map[string]any, error) {
	b.mu.RLock("GetSolutionMetrics")
	defer b.mu.RUnlock()

	if !b.solutionVersions.Has(svArn) {
		return nil, fmt.Errorf("%w: solution version %q not found", ErrNotFound, svArn)
	}

	return map[string]any{
		keySolutionVersionArn: svArn,
		"metrics": map[string]any{
			"coverage":                   svMetric(svArn, "coverage"),
			"mean_reciprocal_rank_at_25": svMetric(svArn, "mrr@25"),
			"normalized_discounted_cumulative_gain_at_5":  svMetric(svArn, "ndcg@5"),
			"normalized_discounted_cumulative_gain_at_10": svMetric(svArn, "ndcg@10"),
			"normalized_discounted_cumulative_gain_at_25": svMetric(svArn, "ndcg@25"),
			"precision_at_5":  svMetric(svArn, "p@5"),
			"precision_at_10": svMetric(svArn, "p@10"),
			"precision_at_25": svMetric(svArn, "p@25"),
		},
	}, nil
}

// svMetric returns a stable [0.01, 0.99] metric value derived from the solution version ARN and metric name.
func svMetric(svArn, metricName string) float64 {
	const buckets = 98 // maps hash into [0.01, 0.99]
	h := fnv.New32a()
	_, _ = h.Write([]byte(svArn + "|" + metricName))

	return float64(h.Sum32()%buckets+1) / 100.0 //nolint:mnd // 100.0 converts integer percent to float ratio
}

// --- Campaign ---

// CreateCampaign creates a new campaign.
func (b *InMemoryBackend) CreateCampaign(
	name, solutionVersionArn string,
	minProvisionedTPS int32,
	tags map[string]string,
) (*Campaign, error) {
	b.mu.Lock("CreateCampaign")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}
	if b.campaigns.Has(name) {
		return nil, fmt.Errorf("%w: campaign %q already exists", ErrAlreadyExists, name)
	}

	now := time.Now().UTC()
	c := &Campaign{
		CampaignArn:         b.personalizeARN("campaign", name),
		Name:                name,
		SolutionVersionArn:  solutionVersionArn,
		MinProvisionedTPS:   minProvisionedTPS,
		Status:              statusActive,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.campaigns.Put(c)
	if len(tags) > 0 {
		b.tags[c.CampaignArn] = copyStringMap(tags)
	}

	return c, nil
}

// DescribeCampaign returns a campaign by name or ARN.
func (b *InMemoryBackend) DescribeCampaign(nameOrArn string) (*Campaign, error) {
	b.mu.RLock("DescribeCampaign")
	defer b.mu.RUnlock()

	if c := b.findCampaign(nameOrArn); c != nil {
		return c, nil
	}

	return nil, fmt.Errorf("%w: campaign %q not found", ErrNotFound, nameOrArn)
}

// UpdateCampaign updates a campaign's solution version or TPS.
func (b *InMemoryBackend) UpdateCampaign(
	nameOrArn, solutionVersionArn string,
	minProvisionedTPS int32,
) (*Campaign, error) {
	b.mu.Lock("UpdateCampaign")
	defer b.mu.Unlock()

	c := b.findCampaign(nameOrArn)
	if c == nil {
		return nil, fmt.Errorf("%w: campaign %q not found", ErrNotFound, nameOrArn)
	}
	if solutionVersionArn != "" {
		c.SolutionVersionArn = solutionVersionArn
	}
	if minProvisionedTPS > 0 {
		c.MinProvisionedTPS = minProvisionedTPS
	}
	c.LastUpdatedDateTime = time.Now().UTC()

	return c, nil
}

// DeleteCampaign removes a campaign.
func (b *InMemoryBackend) DeleteCampaign(nameOrArn string) error {
	b.mu.Lock("DeleteCampaign")
	defer b.mu.Unlock()

	c := b.findCampaign(nameOrArn)
	if c == nil {
		return fmt.Errorf("%w: campaign %q not found", ErrNotFound, nameOrArn)
	}
	b.campaigns.Delete(c.Name)
	delete(b.tags, c.CampaignArn)

	return nil
}

// ListCampaigns returns campaigns, optionally filtered by solution ARN.
func (b *InMemoryBackend) ListCampaigns(solutionArn string, maxResults int, nextToken string) ([]*Campaign, string) {
	b.mu.RLock("ListCampaigns")
	defer b.mu.RUnlock()

	all := b.campaigns.Snapshot()
	filtered := make([]*Campaign, 0, len(all))
	for _, c := range all {
		if solutionArn == "" || c.SolutionVersionArn == solutionArn {
			filtered = append(filtered, c)
		}
	}

	return paginateItems(filtered, campaignKeyFn, maxResults, nextToken)
}

func (b *InMemoryBackend) findCampaign(nameOrArn string) *Campaign {
	if c, ok := b.campaigns.Get(nameOrArn); ok {
		return c
	}
	for _, c := range b.campaigns.All() {
		if c.CampaignArn == nameOrArn {
			return c
		}
	}

	return nil
}

// --- EventTracker ---

// CreateEventTracker creates a new event tracker.
func (b *InMemoryBackend) CreateEventTracker(
	name, datasetGroupArn string,
	tags map[string]string,
) (*EventTracker, error) {
	b.mu.Lock("CreateEventTracker")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}
	if b.eventTrackers.Has(name) {
		return nil, fmt.Errorf("%w: event tracker %q already exists", ErrAlreadyExists, name)
	}

	now := time.Now().UTC()
	et := &EventTracker{
		EventTrackerArn:     b.personalizeARN("event-tracker", name),
		Name:                name,
		DatasetGroupArn:     datasetGroupArn,
		TrackingID:          uuid.New().String(),
		Status:              statusActive,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.eventTrackers.Put(et)
	if len(tags) > 0 {
		b.tags[et.EventTrackerArn] = copyStringMap(tags)
	}

	return et, nil
}

// DescribeEventTracker returns an event tracker by name or ARN.
func (b *InMemoryBackend) DescribeEventTracker(nameOrArn string) (*EventTracker, error) {
	b.mu.RLock("DescribeEventTracker")
	defer b.mu.RUnlock()

	if et := b.findEventTracker(nameOrArn); et != nil {
		return et, nil
	}

	return nil, fmt.Errorf("%w: event tracker %q not found", ErrNotFound, nameOrArn)
}

// DeleteEventTracker removes an event tracker.
func (b *InMemoryBackend) DeleteEventTracker(nameOrArn string) error {
	b.mu.Lock("DeleteEventTracker")
	defer b.mu.Unlock()

	et := b.findEventTracker(nameOrArn)
	if et == nil {
		return fmt.Errorf("%w: event tracker %q not found", ErrNotFound, nameOrArn)
	}
	b.eventTrackers.Delete(et.Name)
	delete(b.tags, et.EventTrackerArn)

	return nil
}

// ListEventTrackers returns event trackers, optionally filtered by dataset group ARN.
func (b *InMemoryBackend) ListEventTrackers(
	datasetGroupArn string,
	maxResults int,
	nextToken string,
) ([]*EventTracker, string) {
	b.mu.RLock("ListEventTrackers")
	defer b.mu.RUnlock()

	all := b.eventTrackers.Snapshot()
	filtered := make([]*EventTracker, 0, len(all))
	for _, et := range all {
		if datasetGroupArn == "" || et.DatasetGroupArn == datasetGroupArn {
			filtered = append(filtered, et)
		}
	}

	return paginateItems(filtered, eventTrackerKeyFn, maxResults, nextToken)
}

func (b *InMemoryBackend) findEventTracker(nameOrArn string) *EventTracker {
	if et, ok := b.eventTrackers.Get(nameOrArn); ok {
		return et
	}
	for _, et := range b.eventTrackers.All() {
		if et.EventTrackerArn == nameOrArn {
			return et
		}
	}

	return nil
}

// --- Filter ---

// CreateFilter creates a new filter.
func (b *InMemoryBackend) CreateFilter(
	name, datasetGroupArn, filterExpression string,
	tags map[string]string,
) (*Filter, error) {
	b.mu.Lock("CreateFilter")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}
	if b.filters.Has(name) {
		return nil, fmt.Errorf("%w: filter %q already exists", ErrAlreadyExists, name)
	}

	now := time.Now().UTC()
	f := &Filter{
		FilterArn:           b.personalizeARN("filter", name),
		Name:                name,
		DatasetGroupArn:     datasetGroupArn,
		FilterExpression:    filterExpression,
		Status:              statusActive,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.filters.Put(f)
	if len(tags) > 0 {
		b.tags[f.FilterArn] = copyStringMap(tags)
	}

	return f, nil
}

// DescribeFilter returns a filter by name or ARN.
func (b *InMemoryBackend) DescribeFilter(nameOrArn string) (*Filter, error) {
	b.mu.RLock("DescribeFilter")
	defer b.mu.RUnlock()

	if f := b.findFilter(nameOrArn); f != nil {
		return f, nil
	}

	return nil, fmt.Errorf("%w: filter %q not found", ErrNotFound, nameOrArn)
}

// DeleteFilter removes a filter.
func (b *InMemoryBackend) DeleteFilter(nameOrArn string) error {
	b.mu.Lock("DeleteFilter")
	defer b.mu.Unlock()

	f := b.findFilter(nameOrArn)
	if f == nil {
		return fmt.Errorf("%w: filter %q not found", ErrNotFound, nameOrArn)
	}
	b.filters.Delete(f.Name)
	delete(b.tags, f.FilterArn)

	return nil
}

// ListFilters returns filters, optionally filtered by dataset group ARN.
func (b *InMemoryBackend) ListFilters(datasetGroupArn string, maxResults int, nextToken string) ([]*Filter, string) {
	b.mu.RLock("ListFilters")
	defer b.mu.RUnlock()

	all := b.filters.Snapshot()
	filtered := make([]*Filter, 0, len(all))
	for _, f := range all {
		if datasetGroupArn == "" || f.DatasetGroupArn == datasetGroupArn {
			filtered = append(filtered, f)
		}
	}

	return paginateItems(filtered, filterKeyFn, maxResults, nextToken)
}

func (b *InMemoryBackend) findFilter(nameOrArn string) *Filter {
	if f, ok := b.filters.Get(nameOrArn); ok {
		return f
	}
	for _, f := range b.filters.All() {
		if f.FilterArn == nameOrArn {
			return f
		}
	}

	return nil
}

// --- Recommender ---

// CreateRecommender creates a new recommender.
func (b *InMemoryBackend) CreateRecommender(
	name, datasetGroupArn, recipeArn string,
	minRPS int32,
	tags map[string]string,
) (*Recommender, error) {
	b.mu.Lock("CreateRecommender")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}
	if b.recommenders.Has(name) {
		return nil, fmt.Errorf("%w: recommender %q already exists", ErrAlreadyExists, name)
	}

	now := time.Now().UTC()
	r := &Recommender{
		RecommenderArn:                     b.personalizeARN("recommender", name),
		Name:                               name,
		DatasetGroupArn:                    datasetGroupArn,
		RecipeArn:                          recipeArn,
		Status:                             statusActive,
		MinRecommendationRequestsPerSecond: minRPS,
		CreationDateTime:                   now,
		LastUpdatedDateTime:                now,
	}
	b.recommenders.Put(r)
	if len(tags) > 0 {
		b.tags[r.RecommenderArn] = copyStringMap(tags)
	}

	return r, nil
}

// DescribeRecommender returns a recommender by name or ARN.
func (b *InMemoryBackend) DescribeRecommender(nameOrArn string) (*Recommender, error) {
	b.mu.RLock("DescribeRecommender")
	defer b.mu.RUnlock()

	if r := b.findRecommender(nameOrArn); r != nil {
		return r, nil
	}

	return nil, fmt.Errorf("%w: recommender %q not found", ErrNotFound, nameOrArn)
}

// UpdateRecommender updates recommender configuration.
func (b *InMemoryBackend) UpdateRecommender(nameOrArn string, minRPS int32) (*Recommender, error) {
	b.mu.Lock("UpdateRecommender")
	defer b.mu.Unlock()

	r := b.findRecommender(nameOrArn)
	if r == nil {
		return nil, fmt.Errorf("%w: recommender %q not found", ErrNotFound, nameOrArn)
	}
	if minRPS > 0 {
		r.MinRecommendationRequestsPerSecond = minRPS
	}
	r.LastUpdatedDateTime = time.Now().UTC()

	return r, nil
}

// DeleteRecommender removes a recommender.
func (b *InMemoryBackend) DeleteRecommender(nameOrArn string) error {
	b.mu.Lock("DeleteRecommender")
	defer b.mu.Unlock()

	r := b.findRecommender(nameOrArn)
	if r == nil {
		return fmt.Errorf("%w: recommender %q not found", ErrNotFound, nameOrArn)
	}
	b.recommenders.Delete(r.Name)
	delete(b.tags, r.RecommenderArn)

	return nil
}

// ListRecommenders returns recommenders, optionally filtered by dataset group ARN.
func (b *InMemoryBackend) ListRecommenders(
	datasetGroupArn string,
	maxResults int,
	nextToken string,
) ([]*Recommender, string) {
	b.mu.RLock("ListRecommenders")
	defer b.mu.RUnlock()

	all := b.recommenders.Snapshot()
	filtered := make([]*Recommender, 0, len(all))
	for _, r := range all {
		if datasetGroupArn == "" || r.DatasetGroupArn == datasetGroupArn {
			filtered = append(filtered, r)
		}
	}

	return paginateItems(filtered, recommenderKeyFn, maxResults, nextToken)
}

// StartRecommender transitions a recommender to ACTIVE.
func (b *InMemoryBackend) StartRecommender(recommenderArn string) (*Recommender, error) {
	b.mu.Lock("StartRecommender")
	defer b.mu.Unlock()

	r := b.findRecommender(recommenderArn)
	if r == nil {
		return nil, fmt.Errorf("%w: recommender %q not found", ErrNotFound, recommenderArn)
	}
	r.Status = statusActive
	r.LastUpdatedDateTime = time.Now().UTC()

	return r, nil
}

// StopRecommender transitions a recommender to INACTIVE.
func (b *InMemoryBackend) StopRecommender(recommenderArn string) (*Recommender, error) {
	b.mu.Lock("StopRecommender")
	defer b.mu.Unlock()

	r := b.findRecommender(recommenderArn)
	if r == nil {
		return nil, fmt.Errorf("%w: recommender %q not found", ErrNotFound, recommenderArn)
	}
	r.Status = "INACTIVE"
	r.LastUpdatedDateTime = time.Now().UTC()

	return r, nil
}

func (b *InMemoryBackend) findRecommender(nameOrArn string) *Recommender {
	if r, ok := b.recommenders.Get(nameOrArn); ok {
		return r
	}
	for _, r := range b.recommenders.All() {
		if r.RecommenderArn == nameOrArn {
			return r
		}
	}

	return nil
}

// --- MetricAttribution ---

// CreateMetricAttribution creates a new metric attribution.
func (b *InMemoryBackend) CreateMetricAttribution(
	name, datasetGroupArn string,
	metricsOutputConfig map[string]any,
	tags map[string]string,
) (*MetricAttribution, error) {
	b.mu.Lock("CreateMetricAttribution")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}
	if b.metricAttributions.Has(name) {
		return nil, fmt.Errorf("%w: metric attribution %q already exists", ErrAlreadyExists, name)
	}

	now := time.Now().UTC()
	ma := &MetricAttribution{
		MetricAttributionArn: b.personalizeARN("metric-attribution", name),
		Name:                 name,
		DatasetGroupArn:      datasetGroupArn,
		MetricsOutputConfig:  metricsOutputConfig,
		Status:               statusActive,
		CreationDateTime:     now,
		LastUpdatedDateTime:  now,
	}
	b.metricAttributions.Put(ma)
	if len(tags) > 0 {
		b.tags[ma.MetricAttributionArn] = copyStringMap(tags)
	}

	return ma, nil
}

// DescribeMetricAttribution returns a metric attribution by name or ARN.
func (b *InMemoryBackend) DescribeMetricAttribution(nameOrArn string) (*MetricAttribution, error) {
	b.mu.RLock("DescribeMetricAttribution")
	defer b.mu.RUnlock()

	if ma := b.findMetricAttribution(nameOrArn); ma != nil {
		return ma, nil
	}

	return nil, fmt.Errorf("%w: metric attribution %q not found", ErrNotFound, nameOrArn)
}

// UpdateMetricAttribution updates a metric attribution's output config.
func (b *InMemoryBackend) UpdateMetricAttribution(
	nameOrArn string,
	metricsOutputConfig map[string]any,
) (*MetricAttribution, error) {
	b.mu.Lock("UpdateMetricAttribution")
	defer b.mu.Unlock()

	ma := b.findMetricAttribution(nameOrArn)
	if ma == nil {
		return nil, fmt.Errorf("%w: metric attribution %q not found", ErrNotFound, nameOrArn)
	}
	if metricsOutputConfig != nil {
		ma.MetricsOutputConfig = metricsOutputConfig
	}
	ma.LastUpdatedDateTime = time.Now().UTC()

	return ma, nil
}

// DeleteMetricAttribution removes a metric attribution.
func (b *InMemoryBackend) DeleteMetricAttribution(nameOrArn string) error {
	b.mu.Lock("DeleteMetricAttribution")
	defer b.mu.Unlock()

	ma := b.findMetricAttribution(nameOrArn)
	if ma == nil {
		return fmt.Errorf("%w: metric attribution %q not found", ErrNotFound, nameOrArn)
	}
	b.metricAttributions.Delete(ma.Name)
	delete(b.tags, ma.MetricAttributionArn)

	return nil
}

// ListMetricAttributions returns metric attributions, optionally filtered by dataset group ARN.
func (b *InMemoryBackend) ListMetricAttributions(
	datasetGroupArn string,
	maxResults int,
	nextToken string,
) ([]*MetricAttribution, string) {
	b.mu.RLock("ListMetricAttributions")
	defer b.mu.RUnlock()

	all := b.metricAttributions.Snapshot()
	filtered := make([]*MetricAttribution, 0, len(all))
	for _, ma := range all {
		if datasetGroupArn == "" || ma.DatasetGroupArn == datasetGroupArn {
			filtered = append(filtered, ma)
		}
	}

	return paginateItems(filtered, metricAttributionKeyFn, maxResults, nextToken)
}

// ListMetricAttributionMetrics returns metrics for a metric attribution with pagination.
func (b *InMemoryBackend) ListMetricAttributionMetrics(
	metricAttributionArn string,
	maxResults int,
	nextToken string,
) ([]map[string]any, string, error) {
	b.mu.RLock("ListMetricAttributionMetrics")
	defer b.mu.RUnlock()

	if b.findMetricAttribution(metricAttributionArn) == nil {
		return nil, "", fmt.Errorf("%w: metric attribution %q not found", ErrNotFound, metricAttributionArn)
	}

	allMetrics := []map[string]any{
		{
			"eventType":             "click",
			"expression":            "SUM(DataSource.EVENT_VALUE)",
			keyMetricAttributionArn: metricAttributionArn,
			"metricName":            "sum-of-event-value",
		},
		{
			"eventType":             "purchase",
			"expression":            "SUM(DataSource.EVENT_VALUE)",
			keyMetricAttributionArn: metricAttributionArn,
			"metricName":            "sum-purchase-value",
		},
	}

	// Build string keys for pagination helper (use metricName as key).
	keys := make([]string, len(allMetrics))
	byKey := make(map[string]map[string]any, len(allMetrics))
	for i, m := range allMetrics {
		k := m["metricName"].(string) //nolint:errcheck // metricName is always string; set by this func above
		keys[i] = k
		byKey[k] = m
	}
	sort.Strings(keys)

	paged, outToken := paginate(keys, func(k string) map[string]any { return byKey[k] }, maxResults, nextToken)

	return paged, outToken, nil
}

func (b *InMemoryBackend) findMetricAttribution(nameOrArn string) *MetricAttribution {
	if ma, ok := b.metricAttributions.Get(nameOrArn); ok {
		return ma
	}
	for _, ma := range b.metricAttributions.All() {
		if ma.MetricAttributionArn == nameOrArn {
			return ma
		}
	}

	return nil
}

// --- DatasetImportJob ---

// CreateDatasetImportJob creates a new dataset import job.
func (b *InMemoryBackend) CreateDatasetImportJob(
	jobName, datasetArn, roleArn string,
	dataSource map[string]any,
	tags map[string]string,
) (*DatasetImportJob, error) {
	b.mu.Lock("CreateDatasetImportJob")
	defer b.mu.Unlock()

	if jobName == "" {
		return nil, fmt.Errorf("%w: jobName is required", ErrValidation)
	}

	now := time.Now().UTC()
	jobArn := b.personalizeARN("dataset-import-job", jobName)
	job := &DatasetImportJob{
		DatasetImportJobArn: jobArn,
		JobName:             jobName,
		DatasetArn:          datasetArn,
		RoleArn:             roleArn,
		DataSource:          dataSource,
		Status:              statusActive,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.datasetImportJobs.Put(job)
	if len(tags) > 0 {
		b.tags[jobArn] = copyStringMap(tags)
	}

	return job, nil
}

// DescribeDatasetImportJob returns a dataset import job by ARN.
func (b *InMemoryBackend) DescribeDatasetImportJob(jobArn string) (*DatasetImportJob, error) {
	b.mu.RLock("DescribeDatasetImportJob")
	defer b.mu.RUnlock()

	job, ok := b.datasetImportJobs.Get(jobArn)
	if !ok {
		return nil, fmt.Errorf("%w: dataset import job %q not found", ErrNotFound, jobArn)
	}

	return job, nil
}

// ListDatasetImportJobs returns import jobs, optionally filtered by dataset ARN.
func (b *InMemoryBackend) ListDatasetImportJobs(
	datasetArn string,
	maxResults int,
	nextToken string,
) ([]*DatasetImportJob, string) {
	b.mu.RLock("ListDatasetImportJobs")
	defer b.mu.RUnlock()

	all := b.datasetImportJobs.Snapshot()
	filtered := make([]*DatasetImportJob, 0, len(all))
	for _, job := range all {
		if datasetArn == "" || job.DatasetArn == datasetArn {
			filtered = append(filtered, job)
		}
	}

	return paginateItems(filtered, datasetImportJobKeyFn, maxResults, nextToken)
}

// --- DatasetExportJob ---

// CreateDatasetExportJob creates a new dataset export job.
func (b *InMemoryBackend) CreateDatasetExportJob(
	jobName, datasetArn, roleArn string,
	jobOutput map[string]any,
	tags map[string]string,
) (*DatasetExportJob, error) {
	b.mu.Lock("CreateDatasetExportJob")
	defer b.mu.Unlock()

	if jobName == "" {
		return nil, fmt.Errorf("%w: jobName is required", ErrValidation)
	}

	now := time.Now().UTC()
	jobArn := b.personalizeARN("dataset-export-job", jobName)
	job := &DatasetExportJob{
		DatasetExportJobArn: jobArn,
		JobName:             jobName,
		DatasetArn:          datasetArn,
		RoleArn:             roleArn,
		JobOutput:           jobOutput,
		Status:              statusActive,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.datasetExportJobs.Put(job)
	if len(tags) > 0 {
		b.tags[jobArn] = copyStringMap(tags)
	}

	return job, nil
}

// DescribeDatasetExportJob returns a dataset export job by ARN.
func (b *InMemoryBackend) DescribeDatasetExportJob(jobArn string) (*DatasetExportJob, error) {
	b.mu.RLock("DescribeDatasetExportJob")
	defer b.mu.RUnlock()

	job, ok := b.datasetExportJobs.Get(jobArn)
	if !ok {
		return nil, fmt.Errorf("%w: dataset export job %q not found", ErrNotFound, jobArn)
	}

	return job, nil
}

// ListDatasetExportJobs returns export jobs, optionally filtered by dataset ARN.
func (b *InMemoryBackend) ListDatasetExportJobs(
	datasetArn string,
	maxResults int,
	nextToken string,
) ([]*DatasetExportJob, string) {
	b.mu.RLock("ListDatasetExportJobs")
	defer b.mu.RUnlock()

	all := b.datasetExportJobs.Snapshot()
	filtered := make([]*DatasetExportJob, 0, len(all))
	for _, job := range all {
		if datasetArn == "" || job.DatasetArn == datasetArn {
			filtered = append(filtered, job)
		}
	}

	return paginateItems(filtered, datasetExportJobKeyFn, maxResults, nextToken)
}

// --- BatchInferenceJob ---

// CreateBatchInferenceJob creates a new batch inference job.
func (b *InMemoryBackend) CreateBatchInferenceJob(
	jobName, solutionVersionArn, roleArn string,
	jobInput, jobOutput map[string]any,
	tags map[string]string,
) (*BatchInferenceJob, error) {
	b.mu.Lock("CreateBatchInferenceJob")
	defer b.mu.Unlock()

	if jobName == "" {
		return nil, fmt.Errorf("%w: jobName is required", ErrValidation)
	}

	now := time.Now().UTC()
	jobArn := b.personalizeARN("batch-inference-job", jobName)
	job := &BatchInferenceJob{
		BatchInferenceJobArn: jobArn,
		JobName:              jobName,
		SolutionVersionArn:   solutionVersionArn,
		RoleArn:              roleArn,
		JobInput:             jobInput,
		JobOutput:            jobOutput,
		Status:               statusActive,
		CreationDateTime:     now,
		LastUpdatedDateTime:  now,
	}
	b.batchInferenceJobs.Put(job)
	if len(tags) > 0 {
		b.tags[jobArn] = copyStringMap(tags)
	}

	return job, nil
}

// DescribeBatchInferenceJob returns a batch inference job by ARN.
func (b *InMemoryBackend) DescribeBatchInferenceJob(jobArn string) (*BatchInferenceJob, error) {
	b.mu.RLock("DescribeBatchInferenceJob")
	defer b.mu.RUnlock()

	job, ok := b.batchInferenceJobs.Get(jobArn)
	if !ok {
		return nil, fmt.Errorf("%w: batch inference job %q not found", ErrNotFound, jobArn)
	}

	return job, nil
}

// ListBatchInferenceJobs returns batch inference jobs, optionally filtered by solution version ARN.
func (b *InMemoryBackend) ListBatchInferenceJobs(
	solutionVersionArn string,
	maxResults int,
	nextToken string,
) ([]*BatchInferenceJob, string) {
	b.mu.RLock("ListBatchInferenceJobs")
	defer b.mu.RUnlock()

	all := b.batchInferenceJobs.Snapshot()
	filtered := make([]*BatchInferenceJob, 0, len(all))
	for _, job := range all {
		if solutionVersionArn == "" || job.SolutionVersionArn == solutionVersionArn {
			filtered = append(filtered, job)
		}
	}

	return paginateItems(filtered, batchInferenceJobKeyFn, maxResults, nextToken)
}

// --- BatchSegmentJob ---

// CreateBatchSegmentJob creates a new batch segment job.
func (b *InMemoryBackend) CreateBatchSegmentJob(
	jobName, solutionVersionArn, roleArn string,
	jobInput, jobOutput map[string]any,
	tags map[string]string,
) (*BatchSegmentJob, error) {
	b.mu.Lock("CreateBatchSegmentJob")
	defer b.mu.Unlock()

	if jobName == "" {
		return nil, fmt.Errorf("%w: jobName is required", ErrValidation)
	}

	now := time.Now().UTC()
	jobArn := b.personalizeARN("batch-segment-job", jobName)
	job := &BatchSegmentJob{
		BatchSegmentJobArn:  jobArn,
		JobName:             jobName,
		SolutionVersionArn:  solutionVersionArn,
		RoleArn:             roleArn,
		JobInput:            jobInput,
		JobOutput:           jobOutput,
		Status:              statusActive,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.batchSegmentJobs.Put(job)
	if len(tags) > 0 {
		b.tags[jobArn] = copyStringMap(tags)
	}

	return job, nil
}

// DescribeBatchSegmentJob returns a batch segment job by ARN.
func (b *InMemoryBackend) DescribeBatchSegmentJob(jobArn string) (*BatchSegmentJob, error) {
	b.mu.RLock("DescribeBatchSegmentJob")
	defer b.mu.RUnlock()

	job, ok := b.batchSegmentJobs.Get(jobArn)
	if !ok {
		return nil, fmt.Errorf("%w: batch segment job %q not found", ErrNotFound, jobArn)
	}

	return job, nil
}

// ListBatchSegmentJobs returns batch segment jobs, optionally filtered by solution version ARN.
func (b *InMemoryBackend) ListBatchSegmentJobs(
	solutionVersionArn string,
	maxResults int,
	nextToken string,
) ([]*BatchSegmentJob, string) {
	b.mu.RLock("ListBatchSegmentJobs")
	defer b.mu.RUnlock()

	all := b.batchSegmentJobs.Snapshot()
	filtered := make([]*BatchSegmentJob, 0, len(all))
	for _, job := range all {
		if solutionVersionArn == "" || job.SolutionVersionArn == solutionVersionArn {
			filtered = append(filtered, job)
		}
	}

	return paginateItems(filtered, batchSegmentJobKeyFn, maxResults, nextToken)
}

// --- DataDeletionJob ---

// CreateDataDeletionJob creates a new data deletion job.
func (b *InMemoryBackend) CreateDataDeletionJob(
	jobName, datasetGroupArn, roleArn string,
	dataSource map[string]any,
	tags map[string]string,
) (*DataDeletionJob, error) {
	b.mu.Lock("CreateDataDeletionJob")
	defer b.mu.Unlock()

	if jobName == "" {
		return nil, fmt.Errorf("%w: jobName is required", ErrValidation)
	}

	now := time.Now().UTC()
	jobArn := b.personalizeARN("data-deletion-job", jobName)
	job := &DataDeletionJob{
		DataDeletionJobArn:  jobArn,
		JobName:             jobName,
		DatasetGroupArn:     datasetGroupArn,
		RoleArn:             roleArn,
		DataSource:          dataSource,
		Status:              statusActive,
		NumDeleted:          0,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.dataDeletionJobs.Put(job)
	if len(tags) > 0 {
		b.tags[jobArn] = copyStringMap(tags)
	}

	return job, nil
}

// DescribeDataDeletionJob returns a data deletion job by ARN.
func (b *InMemoryBackend) DescribeDataDeletionJob(jobArn string) (*DataDeletionJob, error) {
	b.mu.RLock("DescribeDataDeletionJob")
	defer b.mu.RUnlock()

	job, ok := b.dataDeletionJobs.Get(jobArn)
	if !ok {
		return nil, fmt.Errorf("%w: data deletion job %q not found", ErrNotFound, jobArn)
	}

	return job, nil
}

// ListDataDeletionJobs returns data deletion jobs, optionally filtered by dataset group ARN.
func (b *InMemoryBackend) ListDataDeletionJobs(
	datasetGroupArn string,
	maxResults int,
	nextToken string,
) ([]*DataDeletionJob, string) {
	b.mu.RLock("ListDataDeletionJobs")
	defer b.mu.RUnlock()

	all := b.dataDeletionJobs.Snapshot()
	filtered := make([]*DataDeletionJob, 0, len(all))
	for _, job := range all {
		if datasetGroupArn == "" || job.DatasetGroupArn == datasetGroupArn {
			filtered = append(filtered, job)
		}
	}

	return paginateItems(filtered, dataDeletionJobKeyFn, maxResults, nextToken)
}

// --- Runtime validation ---

// ValidateCampaignOrRecommender returns nil if either campaignArn or recommenderArn resolves
// to an existing resource. Returns ErrNotFound if neither exists.
func (b *InMemoryBackend) ValidateCampaignOrRecommender(campaignArn, recommenderArn string) error {
	b.mu.RLock("ValidateCampaignOrRecommender")
	defer b.mu.RUnlock()

	if campaignArn != "" {
		if b.findCampaign(campaignArn) != nil {
			return nil
		}
	}
	if recommenderArn != "" {
		if b.findRecommender(recommenderArn) != nil {
			return nil
		}
	}

	ref := campaignArn
	if ref == "" {
		ref = recommenderArn
	}

	return fmt.Errorf("%w: campaign or recommender %q not found", ErrNotFound, ref)
}

// ValidateCampaign returns nil if campaignArn resolves to an existing campaign.
func (b *InMemoryBackend) ValidateCampaign(campaignArn string) error {
	b.mu.RLock("ValidateCampaign")
	defer b.mu.RUnlock()

	if b.findCampaign(campaignArn) != nil {
		return nil
	}

	return fmt.Errorf("%w: campaign %q not found", ErrNotFound, campaignArn)
}

// --- Tags ---

// TagResource adds tags to a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceArn string, newTags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceArn) {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceArn)
	}
	if b.tags[resourceArn] == nil {
		b.tags[resourceArn] = make(map[string]string)
	}
	maps.Copy(b.tags[resourceArn], newTags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceArn) {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceArn)
	}
	for _, k := range keys {
		delete(b.tags[resourceArn], k)
	}

	return nil
}

// ListTagsForResource returns tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.arnExists(resourceArn) {
		return nil, fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceArn)
	}

	return copyStringMap(b.tags[resourceArn]), nil
}

func (b *InMemoryBackend) arnExists(resourceArn string) bool {
	return b.arnExistsInCoreEntities(resourceArn) ||
		b.arnExistsInTrackingEntities(resourceArn) ||
		b.arnExistsInJobs(resourceArn)
}

func (b *InMemoryBackend) arnExistsInCoreEntities(resourceArn string) bool {
	for _, dg := range b.datasetGroups.All() {
		if dg.DatasetGroupArn == resourceArn {
			return true
		}
	}
	for _, ds := range b.datasets.All() {
		if ds.DatasetArn == resourceArn {
			return true
		}
	}
	for _, s := range b.schemas.All() {
		if s.SchemaArn == resourceArn {
			return true
		}
	}
	for _, sol := range b.solutions.All() {
		if sol.SolutionArn == resourceArn {
			return true
		}
	}

	return b.solutionVersions.Has(resourceArn)
}

func (b *InMemoryBackend) arnExistsInTrackingEntities(resourceArn string) bool {
	for _, c := range b.campaigns.All() {
		if c.CampaignArn == resourceArn {
			return true
		}
	}
	for _, et := range b.eventTrackers.All() {
		if et.EventTrackerArn == resourceArn {
			return true
		}
	}
	for _, f := range b.filters.All() {
		if f.FilterArn == resourceArn {
			return true
		}
	}
	for _, r := range b.recommenders.All() {
		if r.RecommenderArn == resourceArn {
			return true
		}
	}
	for _, ma := range b.metricAttributions.All() {
		if ma.MetricAttributionArn == resourceArn {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) arnExistsInJobs(resourceArn string) bool {
	return b.datasetImportJobs.Has(resourceArn) ||
		b.datasetExportJobs.Has(resourceArn) ||
		b.batchInferenceJobs.Has(resourceArn) ||
		b.batchSegmentJobs.Has(resourceArn) ||
		b.dataDeletionJobs.Has(resourceArn)
}

// --- Helpers ---

func copyStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	maps.Copy(out, m)

	return out
}

// paginateItems is the store.Table-backed counterpart of paginate: items must
// already be in the table's key-sorted order (as returned by
// [store.Table.Snapshot]), which paginateItems relies on for both the page
// slice and the nextToken continuation semantics.
func paginateItems[T any](items []*T, keyOf func(*T) string, maxResults int, nextToken string) ([]*T, string) {
	const defaultPageSize = 100

	if maxResults <= 0 {
		maxResults = defaultPageSize
	}

	start := 0
	if nextToken != "" {
		for i, v := range items {
			if keyOf(v) == nextToken {
				start = i

				break
			}
		}
	}

	end := start + maxResults
	var outToken string
	if end < len(items) {
		outToken = keyOf(items[end])
	} else {
		end = len(items)
	}

	return items[start:end], outToken
}

// paginate is used only by ListMetricAttributionMetrics, which pages over a
// synthetic, non-map-backed []map[string]any and so is out of scope for the
// store.Table conversion above.
func paginate[T any](keys []string, get func(string) T, maxResults int, nextToken string) ([]T, string) {
	const defaultPageSize = 100

	if maxResults <= 0 {
		maxResults = defaultPageSize
	}

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := start + maxResults
	var outToken string
	if end < len(keys) {
		outToken = keys[end]
	} else {
		end = len(keys)
	}

	items := make([]T, 0, end-start)
	for _, k := range keys[start:end] {
		items = append(items, get(k))
	}

	return items, outToken
}
