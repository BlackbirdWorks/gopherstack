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

// ---------------------------------------------------------------------------
// AIWorkloadConfig — a named, reusable benchmark-tool/workload specification
// referenced by name or ARN from CreateAIBenchmarkJob and
// CreateAIRecommendationJob (AIWorkloadConfigIdentifier). Unlike the three
// job families added alongside it, AIWorkloadConfig has no status/lifecycle
// of its own — DescribeAIWorkloadConfigOutput carries no status field at
// all, matching a static configuration resource rather than a running job.
// ---------------------------------------------------------------------------

// ErrAIWorkloadConfigNotFound is returned when an AI workload configuration
// does not exist. Field-diffed against aws-sdk-go-v2/service/sagemaker's
// deserializers.go: DescribeAIWorkloadConfig's error deserializer only
// recognizes a "ResourceNotFound" wire exception, so this wraps the shared
// [ErrResourceNotFound] sentinel rather than the generic ValidationException
// path the rest of the service's older CRUD families take.
var ErrAIWorkloadConfigNotFound = awserr.New("ResourceNotFound", ErrResourceNotFound)

// ErrAIWorkloadConfigAlreadyExists is returned on a duplicate
// AIWorkloadConfigName. AWS resource-creation conflicts uniformly surface as
// ResourceInUse, which handleError already maps correctly for
// awserr.ErrConflict.
var ErrAIWorkloadConfigAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)

// AIWorkloadConfig represents a SageMaker AI workload configuration.
// WorkloadSpec and DatasetConfig are stored as opaque JSON (mirroring the
// json.RawMessage passthrough convention already used by algorithms.go for
// deeply-nested union/config shapes) — the client's Create payload is
// echoed back verbatim on Describe, which is wire-accurate for every field
// the client actually sent (both are optional in the real API and have no
// server-synthesized sub-fields).
type AIWorkloadConfig struct {
	CreationTime         time.Time         `json:"CreationTime"`
	Tags                 map[string]string `json:"Tags,omitempty"`
	AIWorkloadConfigName string            `json:"AIWorkloadConfigName"`
	AIWorkloadConfigArn  string            `json:"AIWorkloadConfigArn"`
	WorkloadSpec         json.RawMessage   `json:"AIWorkloadConfigs,omitempty"`
	DatasetConfig        json.RawMessage   `json:"DatasetConfig,omitempty"`
}

// MarshalJSON emits CreationTime as an AWS awsjson1.1 epoch-seconds number.
func (c *AIWorkloadConfig) MarshalJSON() ([]byte, error) {
	type alias AIWorkloadConfig

	return json.Marshal(struct {
		*alias
		CreationTime float64 `json:"CreationTime"`
	}{
		alias:        (*alias)(c),
		CreationTime: epochSeconds(c.CreationTime),
	})
}

// UnmarshalJSON is the inverse of [AIWorkloadConfig.MarshalJSON], used by
// persistence.go's snapshot restore path.
func (c *AIWorkloadConfig) UnmarshalJSON(data []byte) error {
	type alias AIWorkloadConfig

	aux := struct {
		*alias
		CreationTime float64 `json:"CreationTime"`
	}{alias: (*alias)(c)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	c.CreationTime = timeFromEpochSeconds(aux.CreationTime)

	return nil
}

func cloneAIWorkloadConfig(c *AIWorkloadConfig) *AIWorkloadConfig {
	cp := *c
	cp.Tags = maps.Clone(c.Tags)
	cp.WorkloadSpec = append(json.RawMessage(nil), c.WorkloadSpec...)
	cp.DatasetConfig = append(json.RawMessage(nil), c.DatasetConfig...)

	return &cp
}

func (b *InMemoryBackend) aiWorkloadConfigsStore(r string) *store.Table[AIWorkloadConfig] {
	if b.aiWorkloadConfigs[r] == nil {
		b.aiWorkloadConfigs[r] = store.Register(
			b.registry,
			"aiWorkloadConfigs:"+r,
			store.New(func(v *AIWorkloadConfig) string { return v.AIWorkloadConfigName }),
		)
	}

	return b.aiWorkloadConfigs[r]
}

// aiWorkloadConfigsStoreRO returns the region-scoped aiWorkloadConfigs table
// for r without mutating the outer map. Safe to call while holding only
// b.mu.RLock(): if the region has not been observed yet, it returns a
// fresh, unregistered, empty view instead of lazily creating (and
// persisting) an entry.
func (b *InMemoryBackend) aiWorkloadConfigsStoreRO(r string) *store.Table[AIWorkloadConfig] {
	if v := b.aiWorkloadConfigs[r]; v != nil {
		return v
	}

	return store.New(func(v *AIWorkloadConfig) string { return v.AIWorkloadConfigName })
}

// resolveAIWorkloadConfigLocked looks up an AI workload configuration by
// name or ARN (must be called with b.mu held, read or write). Used both by
// DescribeAIWorkloadConfig and by CreateAIBenchmarkJob/
// CreateAIRecommendationJob to validate their AIWorkloadConfigIdentifier
// input actually refers to an existing configuration.
func (b *InMemoryBackend) resolveAIWorkloadConfigLocked(region, nameOrArn string) (*AIWorkloadConfig, error) {
	tbl := b.aiWorkloadConfigsStoreRO(region)

	if c, ok := tbl.Get(nameOrArn); ok {
		return c, nil
	}

	if name, ok := b.aiWorkloadConfigARNIndexStoreRO(region)[nameOrArn]; ok {
		if c, found := tbl.Get(name); found {
			return c, nil
		}
	}

	return nil, fmt.Errorf(
		"%w: AI workload configuration %q not found",
		ErrAIWorkloadConfigNotFound,
		nameOrArn,
	)
}

func (b *InMemoryBackend) aiWorkloadConfigARNIndexStore(r string) map[string]string {
	if b.aiWorkloadConfigARNIndex[r] == nil {
		b.aiWorkloadConfigARNIndex[r] = make(map[string]string)
	}

	return b.aiWorkloadConfigARNIndex[r]
}

func (b *InMemoryBackend) aiWorkloadConfigARNIndexStoreRO(r string) map[string]string {
	if v := b.aiWorkloadConfigARNIndex[r]; v != nil {
		return v
	}

	return nil
}

// CreateAIWorkloadConfigOptions holds input fields for CreateAIWorkloadConfig.
type CreateAIWorkloadConfigOptions struct {
	Tags                 map[string]string
	AIWorkloadConfigName string
	WorkloadSpec         json.RawMessage
	DatasetConfig        json.RawMessage
}

// CreateAIWorkloadConfig creates an AI workload configuration.
func (b *InMemoryBackend) CreateAIWorkloadConfig(
	ctx context.Context,
	opts CreateAIWorkloadConfigOptions,
) (*AIWorkloadConfig, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateAIWorkloadConfig")
	defer b.mu.Unlock()

	if opts.AIWorkloadConfigName == "" {
		return nil, fmt.Errorf("%w: AIWorkloadConfigName is required", ErrValidation)
	}

	tbl := b.aiWorkloadConfigsStore(region)
	if _, ok := tbl.Get(opts.AIWorkloadConfigName); ok {
		return nil, fmt.Errorf(
			"%w: AI workload configuration %q already exists",
			ErrAIWorkloadConfigAlreadyExists,
			opts.AIWorkloadConfigName,
		)
	}

	configARN := arn.Build("sagemaker", region, b.accountID, "ai-workload-configuration/"+opts.AIWorkloadConfigName)
	now := time.Now()

	c := &AIWorkloadConfig{
		AIWorkloadConfigName: opts.AIWorkloadConfigName,
		AIWorkloadConfigArn:  configARN,
		WorkloadSpec:         opts.WorkloadSpec,
		DatasetConfig:        opts.DatasetConfig,
		Tags:                 mergeTags(nil, opts.Tags),
		CreationTime:         now,
	}
	tbl.Put(c)
	b.aiWorkloadConfigARNIndexStore(region)[configARN] = opts.AIWorkloadConfigName

	return cloneAIWorkloadConfig(c), nil
}

// DescribeAIWorkloadConfig returns an AI workload configuration by name or ARN.
func (b *InMemoryBackend) DescribeAIWorkloadConfig(ctx context.Context, nameOrArn string) (*AIWorkloadConfig, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeAIWorkloadConfig")
	defer b.mu.RUnlock()

	c, err := b.resolveAIWorkloadConfigLocked(region, nameOrArn)
	if err != nil {
		return nil, err
	}

	return cloneAIWorkloadConfig(c), nil
}

// DeleteAIWorkloadConfig removes an AI workload configuration by name,
// returning its ARN.
func (b *InMemoryBackend) DeleteAIWorkloadConfig(ctx context.Context, name string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteAIWorkloadConfig")
	defer b.mu.Unlock()

	tbl := b.aiWorkloadConfigsStore(region)

	c, ok := tbl.Get(name)
	if !ok {
		return "", fmt.Errorf("%w: AI workload configuration %q not found", ErrAIWorkloadConfigNotFound, name)
	}

	tbl.Delete(name)
	delete(b.aiWorkloadConfigARNIndexStore(region), c.AIWorkloadConfigArn)

	return c.AIWorkloadConfigArn, nil
}

// ListAIWorkloadConfigsParams bundles the filter/sort criteria for ListAIWorkloadConfigs.
type ListAIWorkloadConfigsParams struct {
	CreationTimeAfter  *time.Time
	CreationTimeBefore *time.Time
	NameContains       string
	SortBy             string
	SortOrder          string
	NextToken          string
	MaxResults         int32
}

// ListAIWorkloadConfigs lists AI workload configurations, optionally
// filtered and sorted.
func (b *InMemoryBackend) ListAIWorkloadConfigs(
	ctx context.Context,
	params ListAIWorkloadConfigsParams,
) ([]*AIWorkloadConfig, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListAIWorkloadConfigs")
	defer b.mu.RUnlock()

	tbl := b.aiWorkloadConfigsStoreRO(region)
	list := make([]*AIWorkloadConfig, 0, tbl.Len())

	for _, c := range tbl.All() {
		if params.NameContains != "" &&
			!strings.Contains(strings.ToLower(c.AIWorkloadConfigName), strings.ToLower(params.NameContains)) {
			continue
		}

		if params.CreationTimeAfter != nil && !c.CreationTime.After(*params.CreationTimeAfter) {
			continue
		}

		if params.CreationTimeBefore != nil && !c.CreationTime.Before(*params.CreationTimeBefore) {
			continue
		}

		list = append(list, cloneAIWorkloadConfig(c))
	}

	desc := params.SortOrder == sortOrderDescending
	sort.Slice(list, func(i, j int) bool {
		var less bool

		switch params.SortBy {
		case keyCreationTime:
			less = list[i].CreationTime.Before(list[j].CreationTime)
		default:
			less = list[i].AIWorkloadConfigName < list[j].AIWorkloadConfigName
		}

		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}
