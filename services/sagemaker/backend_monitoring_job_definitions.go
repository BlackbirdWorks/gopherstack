package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

var (
	// ErrDataQualityJobDefNotFound is returned when a data quality job definition does not exist.
	ErrDataQualityJobDefNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrDataQualityJobDefExists is returned when creating a data quality job definition whose name is taken.
	ErrDataQualityJobDefExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrModelBiasJobDefNotFound is returned when a model bias job definition does not exist.
	ErrModelBiasJobDefNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrModelBiasJobDefExists is returned when creating a model bias job definition whose name is taken.
	ErrModelBiasJobDefExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrModelQualityJobDefNotFound is returned when a model quality job definition does not exist.
	ErrModelQualityJobDefNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrModelQualityJobDefExists is returned when creating a model quality job definition whose name is taken.
	ErrModelQualityJobDefExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrModelExplainJobDefNotFound is returned when a model explainability job definition does not exist.
	ErrModelExplainJobDefNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrModelExplainJobDefExists is returned when creating a model explainability job definition whose name is taken.
	ErrModelExplainJobDefExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// ---------------------------------------------------------------------------
// JobDefinition — shared struct for Data Quality / Model Bias / Quality / Explainability
// ---------------------------------------------------------------------------

// JobDefinition is the shared backend representation for the four SageMaker
// Model Monitor job definition types (DataQuality, ModelBias, ModelQuality,
// ModelExplainability). Each type sends its AppSpecification/JobInput/
// JobOutputConfig blocks under differently-named wire fields (e.g.
// "DataQualityAppSpecification" vs "ModelBiasAppSpecification"); Config
// captures those verbatim, plus the shared JobResources/NetworkConfig/
// StoppingCondition/BaselineConfig blocks, so Describe echoes back exactly
// what Create received.
type JobDefinition struct {
	CreationTime      time.Time                  `json:"CreationTime"`
	Tags              map[string]string          `json:"Tags,omitempty"`
	Config            map[string]json.RawMessage `json:"Config,omitempty"`
	JobDefinitionName string                     `json:"JobDefinitionName"`
	JobDefinitionArn  string                     `json:"JobDefinitionArn"`
	JobDefinitionType string                     `json:"JobDefinitionType"`
	RoleArn           string                     `json:"RoleArn,omitempty"`
	EndpointName      string                     `json:"EndpointName,omitempty"`
}

func cloneJobDefinition(j *JobDefinition) *JobDefinition {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)
	cp.Config = maps.Clone(j.Config)

	return &cp
}

// createJobDefinition is the shared Create implementation for all four job
// definition types. storeFn is called only while b.mu is held, so the
// per-region map's lazy initialisation stays race-free.
func (b *InMemoryBackend) createJobDefinition(
	ctx context.Context,
	storeFn func(string) *store.Table[JobDefinition],
	defType, name, roleArn, endpointName string,
	config map[string]json.RawMessage,
	tags map[string]string,
	resourceType string,
	alreadyExists error,
) (*JobDefinition, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("createJobDefinition")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: %sJobDefinitionName is required", ErrValidation, defType)
	}

	tbl := storeFn(region)
	if _, ok := tbl.Get(name); ok {
		return nil, fmt.Errorf("%w: %s job definition %q already exists", alreadyExists, defType, name)
	}

	defARN := arn.Build("sagemaker", region, b.accountID, resourceType+"/"+name)

	j := &JobDefinition{
		JobDefinitionName: name,
		JobDefinitionArn:  defARN,
		JobDefinitionType: defType,
		RoleArn:           roleArn,
		EndpointName:      endpointName,
		Tags:              mergeTags(nil, tags),
		Config:            config,
		CreationTime:      time.Now(),
	}
	tbl.Put(j)

	return cloneJobDefinition(j), nil
}

func (b *InMemoryBackend) describeJobDefinition(
	ctx context.Context,
	storeFn func(string) *store.Table[JobDefinition],
	name string,
	notFound error,
) (*JobDefinition, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("describeJobDefinition")
	defer b.mu.RUnlock()

	j, ok := storeFn(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: job definition %q not found", notFound, name)
	}

	return cloneJobDefinition(j), nil
}

func (b *InMemoryBackend) deleteJobDefinition(
	ctx context.Context,
	storeFn func(string) *store.Table[JobDefinition],
	name string,
	notFound error,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("deleteJobDefinition")
	defer b.mu.Unlock()

	tbl := storeFn(region)
	if _, ok := tbl.Get(name); !ok {
		return fmt.Errorf("%w: job definition %q not found", notFound, name)
	}

	tbl.Delete(name)

	return nil
}

// JobDefinitionFilter narrows the four List*JobDefinitions operations.
type JobDefinitionFilter struct {
	CreationTimeAfter  *time.Time
	CreationTimeBefore *time.Time
	EndpointName       string
	NameContains       string
	SortBy             string // "Name" | "CreationTime" (default)
	SortOrder          string // "Ascending" | "Descending" (default)
	MaxResults         int32
}

func matchesJobDefinitionFilter(j *JobDefinition, f JobDefinitionFilter) bool {
	if f.EndpointName != "" && j.EndpointName != f.EndpointName {
		return false
	}
	if f.NameContains != "" &&
		!strings.Contains(strings.ToLower(j.JobDefinitionName), strings.ToLower(f.NameContains)) {
		return false
	}
	if f.CreationTimeAfter != nil && !j.CreationTime.After(*f.CreationTimeAfter) {
		return false
	}
	if f.CreationTimeBefore != nil && !j.CreationTime.Before(*f.CreationTimeBefore) {
		return false
	}

	return true
}

func sortJobDefinitions(list []*JobDefinition, sortBy, sortOrder string) {
	byName := strings.EqualFold(sortBy, "Name")
	descending := !strings.EqualFold(sortOrder, "Ascending") // AWS default sort order is Descending

	sort.SliceStable(list, func(i, k int) bool {
		var cmp int
		if byName {
			cmp = strings.Compare(list[i].JobDefinitionName, list[k].JobDefinitionName)
		} else {
			cmp = compareTimes(list[i].CreationTime, list[k].CreationTime)
		}

		if descending {
			return cmp > 0
		}

		return cmp < 0
	})
}

// listJobDefinitions is the shared List implementation for all four job
// definition types. storeFn is called only while b.mu is held (by the
// per-type List* wrapper).
func (b *InMemoryBackend) listJobDefinitions(
	storeFn func(string) *store.Table[JobDefinition],
	region, nextToken string,
	f JobDefinitionFilter,
) ([]*JobDefinition, string) {
	items := storeFn(region).All()

	list := make([]*JobDefinition, 0, len(items))

	for _, j := range items {
		if matchesJobDefinitionFilter(j, f) {
			list = append(list, cloneJobDefinition(j))
		}
	}

	sortJobDefinitions(list, f.SortBy, f.SortOrder)

	return paginateSlice(list, nextToken, f.MaxResults)
}

// ---------------------------------------------------------------------------
// DataQualityJobDefinition
// ---------------------------------------------------------------------------

// CreateDataQualityJobDefinition creates a data quality job definition.
func (b *InMemoryBackend) CreateDataQualityJobDefinition(
	ctx context.Context,
	name, roleArn, endpointName string,
	config map[string]json.RawMessage,
	tags map[string]string,
) (*JobDefinition, error) {
	return b.createJobDefinition(
		ctx, b.dataQualityJobDefsStore, "DataQuality", name, roleArn, endpointName, config, tags,
		"data-quality-job-definition", ErrDataQualityJobDefExists,
	)
}

// DescribeDataQualityJobDefinition returns a data quality job definition by name.
func (b *InMemoryBackend) DescribeDataQualityJobDefinition(ctx context.Context, name string) (*JobDefinition, error) {
	return b.describeJobDefinition(ctx, b.dataQualityJobDefsStoreRO, name, ErrDataQualityJobDefNotFound)
}

// DeleteDataQualityJobDefinition removes a data quality job definition by name.
func (b *InMemoryBackend) DeleteDataQualityJobDefinition(ctx context.Context, name string) error {
	return b.deleteJobDefinition(ctx, b.dataQualityJobDefsStore, name, ErrDataQualityJobDefNotFound)
}

// ListDataQualityJobDefinitions returns data quality job definitions matching f.
func (b *InMemoryBackend) ListDataQualityJobDefinitions(
	ctx context.Context, nextToken string, f JobDefinitionFilter,
) ([]*JobDefinition, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListDataQualityJobDefinitions")
	defer b.mu.RUnlock()

	return b.listJobDefinitions(b.dataQualityJobDefsStoreRO, region, nextToken, f)
}

// ---------------------------------------------------------------------------
// ModelBiasJobDefinition
// ---------------------------------------------------------------------------

// CreateModelBiasJobDefinition creates a model bias job definition.
func (b *InMemoryBackend) CreateModelBiasJobDefinition(
	ctx context.Context,
	name, roleArn, endpointName string,
	config map[string]json.RawMessage,
	tags map[string]string,
) (*JobDefinition, error) {
	return b.createJobDefinition(
		ctx, b.modelBiasJobDefsStore, "ModelBias", name, roleArn, endpointName, config, tags,
		"model-bias-job-definition", ErrModelBiasJobDefExists,
	)
}

// DescribeModelBiasJobDefinition returns a model bias job definition by name.
func (b *InMemoryBackend) DescribeModelBiasJobDefinition(ctx context.Context, name string) (*JobDefinition, error) {
	return b.describeJobDefinition(ctx, b.modelBiasJobDefsStoreRO, name, ErrModelBiasJobDefNotFound)
}

// DeleteModelBiasJobDefinition removes a model bias job definition by name.
func (b *InMemoryBackend) DeleteModelBiasJobDefinition(ctx context.Context, name string) error {
	return b.deleteJobDefinition(ctx, b.modelBiasJobDefsStore, name, ErrModelBiasJobDefNotFound)
}

// ListModelBiasJobDefinitions returns model bias job definitions matching f.
func (b *InMemoryBackend) ListModelBiasJobDefinitions(
	ctx context.Context, nextToken string, f JobDefinitionFilter,
) ([]*JobDefinition, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListModelBiasJobDefinitions")
	defer b.mu.RUnlock()

	return b.listJobDefinitions(b.modelBiasJobDefsStoreRO, region, nextToken, f)
}

// ---------------------------------------------------------------------------
// ModelQualityJobDefinition
// ---------------------------------------------------------------------------

// CreateModelQualityJobDefinition creates a model quality job definition.
func (b *InMemoryBackend) CreateModelQualityJobDefinition(
	ctx context.Context,
	name, roleArn, endpointName string,
	config map[string]json.RawMessage,
	tags map[string]string,
) (*JobDefinition, error) {
	return b.createJobDefinition(
		ctx, b.modelQualityJobDefsStore, "ModelQuality", name, roleArn, endpointName, config, tags,
		"model-quality-job-definition", ErrModelQualityJobDefExists,
	)
}

// DescribeModelQualityJobDefinition returns a model quality job definition by name.
func (b *InMemoryBackend) DescribeModelQualityJobDefinition(ctx context.Context, name string) (*JobDefinition, error) {
	return b.describeJobDefinition(ctx, b.modelQualityJobDefsStoreRO, name, ErrModelQualityJobDefNotFound)
}

// DeleteModelQualityJobDefinition removes a model quality job definition by name.
func (b *InMemoryBackend) DeleteModelQualityJobDefinition(ctx context.Context, name string) error {
	return b.deleteJobDefinition(ctx, b.modelQualityJobDefsStore, name, ErrModelQualityJobDefNotFound)
}

// ListModelQualityJobDefinitions returns model quality job definitions matching f.
func (b *InMemoryBackend) ListModelQualityJobDefinitions(
	ctx context.Context, nextToken string, f JobDefinitionFilter,
) ([]*JobDefinition, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListModelQualityJobDefinitions")
	defer b.mu.RUnlock()

	return b.listJobDefinitions(b.modelQualityJobDefsStoreRO, region, nextToken, f)
}

// ---------------------------------------------------------------------------
// ModelExplainabilityJobDefinition
// ---------------------------------------------------------------------------

// CreateModelExplainabilityJobDefinition creates a model explainability job definition.
func (b *InMemoryBackend) CreateModelExplainabilityJobDefinition(
	ctx context.Context,
	name, roleArn, endpointName string,
	config map[string]json.RawMessage,
	tags map[string]string,
) (*JobDefinition, error) {
	return b.createJobDefinition(
		ctx, b.modelExplainJobDefsStore, "ModelExplainability", name, roleArn, endpointName, config, tags,
		"model-explainability-job-definition", ErrModelExplainJobDefExists,
	)
}

// DescribeModelExplainabilityJobDefinition returns a model explainability job definition by name.
func (b *InMemoryBackend) DescribeModelExplainabilityJobDefinition(
	ctx context.Context,
	name string,
) (*JobDefinition, error) {
	return b.describeJobDefinition(ctx, b.modelExplainJobDefsStoreRO, name, ErrModelExplainJobDefNotFound)
}

// DeleteModelExplainabilityJobDefinition removes a model explainability job definition by name.
func (b *InMemoryBackend) DeleteModelExplainabilityJobDefinition(ctx context.Context, name string) error {
	return b.deleteJobDefinition(ctx, b.modelExplainJobDefsStore, name, ErrModelExplainJobDefNotFound)
}

// ListModelExplainabilityJobDefinitions returns model explainability job definitions matching f.
func (b *InMemoryBackend) ListModelExplainabilityJobDefinitions(
	ctx context.Context, nextToken string, f JobDefinitionFilter,
) ([]*JobDefinition, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListModelExplainabilityJobDefinitions")
	defer b.mu.RUnlock()

	return b.listJobDefinitions(b.modelExplainJobDefsStoreRO, region, nextToken, f)
}
