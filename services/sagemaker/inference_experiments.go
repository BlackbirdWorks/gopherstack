package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrInferenceExperimentNotFound is returned when an inference experiment does not exist.
var ErrInferenceExperimentNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// InferenceExperiment
// ---------------------------------------------------------------------------

// RealTimeInferenceConfig mirrors types.RealTimeInferenceConfig
// (types/types.go), both members required when present.
type RealTimeInferenceConfig struct {
	InstanceType  string `json:"InstanceType,omitempty"`
	InstanceCount int32  `json:"InstanceCount,omitempty"`
}

// ModelInfrastructureConfig mirrors types.ModelInfrastructureConfig
// (types/types.go). Only one InfrastructureType exists today
// ("RealTimeInference", types/enums.go:5969), so RealTimeInferenceConfig is
// the sole variant modeled.
type ModelInfrastructureConfig struct {
	RealTimeInferenceConfig *RealTimeInferenceConfig `json:"RealTimeInferenceConfig,omitempty"`
	InfrastructureType      string                   `json:"InfrastructureType,omitempty"`
}

// ModelVariantConfig mirrors types.ModelVariantConfig
// (types/types.go), the request-side shape used by
// CreateInferenceExperimentInput/UpdateInferenceExperimentInput/
// StopInferenceExperimentInput.DesiredModelVariants.
type ModelVariantConfig struct {
	InfrastructureConfig *ModelInfrastructureConfig `json:"InfrastructureConfig,omitempty"`
	ModelName            string                     `json:"ModelName"`
	VariantName          string                     `json:"VariantName"`
}

// ModelVariantConfigSummary mirrors types.ModelVariantConfigSummary
// (types/types.go) — the response-side shape DescribeInferenceExperimentOutput
// actually emits under the same "ModelVariants" wire key that
// CreateInferenceExperimentInput uses for the request-side ModelVariantConfig
// shape above. Same key, different shape: the response additionally
// requires Status, which has no request-side counterpart to source from, so
// it is synthesized (see modelVariantConfigSummaries).
type ModelVariantConfigSummary struct {
	InfrastructureConfig *ModelInfrastructureConfig `json:"InfrastructureConfig,omitempty"`
	ModelName            string                     `json:"ModelName"`
	VariantName          string                     `json:"VariantName"`
	Status               string                     `json:"Status"`
}

// modelVariantConfigSummaries projects the stored request-shaped
// ModelVariantConfig list into the response-shaped ModelVariantConfigSummary
// list DescribeInferenceExperimentOutput/ModelVariantConfigSummary
// (types/types.go) requires. Status is synthesized as "InService"
// (types.ModelVariantStatusInService, types/enums.go:6239): this backend has
// no per-variant deployment FSM, and every variant a real client can
// currently observe here was created synchronously.
func modelVariantConfigSummaries(variants []ModelVariantConfig) []ModelVariantConfigSummary {
	out := make([]ModelVariantConfigSummary, 0, len(variants))
	for _, v := range variants {
		out = append(out, ModelVariantConfigSummary{
			ModelName:            v.ModelName,
			VariantName:          v.VariantName,
			InfrastructureConfig: v.InfrastructureConfig,
			Status:               "InService",
		})
	}

	return out
}

// ShadowModelVariantConfig mirrors types.ShadowModelVariantConfig
// (types/types.go), both members required.
type ShadowModelVariantConfig struct {
	ShadowModelVariantName string `json:"ShadowModelVariantName"`
	SamplingPercentage     int32  `json:"SamplingPercentage"`
}

// ShadowModeConfig mirrors types.ShadowModeConfig (types/types.go), both
// members required.
type ShadowModeConfig struct {
	SourceModelVariantName string                     `json:"SourceModelVariantName"`
	ShadowModelVariants    []ShadowModelVariantConfig `json:"ShadowModelVariants"`
}

// CaptureContentTypeHeader mirrors types.CaptureContentTypeHeader
// (types/types.go), both members optional.
type CaptureContentTypeHeader struct {
	CsvContentTypes  []string `json:"CsvContentTypes,omitempty"`
	JSONContentTypes []string `json:"JsonContentTypes,omitempty"`
}

// InferenceExperimentDataStorageConfig mirrors
// types.InferenceExperimentDataStorageConfig (types/types.go): Destination
// required, ContentType/KmsKey optional.
type InferenceExperimentDataStorageConfig struct {
	ContentType *CaptureContentTypeHeader `json:"ContentType,omitempty"`
	Destination string                    `json:"Destination"`
	KmsKey      string                    `json:"KmsKey,omitempty"`
}

// InferenceExperimentSchedule mirrors types.InferenceExperimentSchedule
// (types/types.go: EndTime/StartTime, both optional *time.Time). Stored as
// *float64 directly: awsjson1.1 serializes timestamps as epoch-second
// numbers (this campaign's repo-spanning time-decode bug, parity-16), and
// since these two fields are never computed against — only stored and
// echoed back — there is no need for a *time.Time-typed intermediate form
// or a custom Marshal/UnmarshalJSON pair.
type InferenceExperimentSchedule struct {
	StartTime *float64 `json:"StartTime,omitempty"`
	EndTime   *float64 `json:"EndTime,omitempty"`
}

// InferenceExperimentEndpointMetadata mirrors types.EndpointMetadata
// (types/types.go): EndpointName required, the rest optional. Always
// recomputed from the live Endpoint store at Describe time (see
// inferenceExperimentEndpointMetadata) — never itself persisted — so it can
// never go stale relative to the endpoint's real current state.
type InferenceExperimentEndpointMetadata struct {
	EndpointName       string `json:"EndpointName"`
	EndpointConfigName string `json:"EndpointConfigName,omitempty"`
	EndpointStatus     string `json:"EndpointStatus,omitempty"`
	FailureReason      string `json:"FailureReason,omitempty"`
}

// InferenceExperiment represents a SageMaker inference experiment.
type InferenceExperiment struct {
	CreationTime      time.Time                             `json:"CreationTime"`
	LastModifiedTime  time.Time                             `json:"LastModifiedTime"`
	DataStorageConfig *InferenceExperimentDataStorageConfig `json:"DataStorageConfig,omitempty"`
	Schedule          *InferenceExperimentSchedule          `json:"Schedule,omitempty"`
	ShadowModeConfig  *ShadowModeConfig                     `json:"ShadowModeConfig,omitempty"`
	EndpointMetadata  *InferenceExperimentEndpointMetadata  `json:"EndpointMetadata,omitempty"`
	Tags              map[string]string                     `json:"Tags,omitempty"`
	Name              string                                `json:"Name"`
	Arn               string                                `json:"Arn"`
	Status            string                                `json:"Status"`
	Type              string                                `json:"Type,omitempty"`
	RoleArn           string                                `json:"RoleArn,omitempty"`
	Description       string                                `json:"Description,omitempty"`
	EndpointName      string                                `json:"EndpointName,omitempty"`
	KmsKey            string                                `json:"KmsKey,omitempty"`
	StatusReason      string                                `json:"StatusReason,omitempty"`
	ModelVariants     []ModelVariantConfig                  `json:"ModelVariantConfigs,omitempty"`
}

func cloneInferenceExperiment(e *InferenceExperiment) *InferenceExperiment {
	cp := *e
	cp.Tags = maps.Clone(e.Tags)
	cp.ModelVariants = append([]ModelVariantConfig(nil), e.ModelVariants...)

	if e.DataStorageConfig != nil {
		dsc := *e.DataStorageConfig
		cp.DataStorageConfig = &dsc
	}

	if e.Schedule != nil {
		sc := *e.Schedule
		cp.Schedule = &sc
	}

	if e.ShadowModeConfig != nil {
		smc := *e.ShadowModeConfig
		smc.ShadowModelVariants = append([]ShadowModelVariantConfig(nil), e.ShadowModeConfig.ShadowModelVariants...)
		cp.ShadowModeConfig = &smc
	}

	return &cp
}

// MarshalJSON emits CreationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings, and
// projects the stored request-shaped ModelVariants into the
// response-shaped "ModelVariants" wire key (see [ModelVariantConfigSummary]'s
// doc comment) — this struct is marshaled directly by
// handleDescribeInferenceExperiment.
func (e *InferenceExperiment) MarshalJSON() ([]byte, error) {
	type alias InferenceExperiment

	return json.Marshal(struct {
		*alias
		ModelVariants    []ModelVariantConfigSummary `json:"ModelVariants"`
		CreationTime     float64                     `json:"CreationTime"`
		LastModifiedTime float64                     `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(e),
		CreationTime:     epochSeconds(e.CreationTime),
		LastModifiedTime: epochSeconds(e.LastModifiedTime),
		ModelVariants:    modelVariantConfigSummaries(e.ModelVariants),
	})
}

// UnmarshalJSON is the inverse of [InferenceExperiment.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (e *InferenceExperiment) UnmarshalJSON(data []byte) error {
	type alias InferenceExperiment

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(e)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	e.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	e.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
}

// inferenceExperimentEndpointMetadata builds the required EndpointMetadata
// response member (api_op_DescribeInferenceExperiment.go:32-38) by looking
// up endpointName in the live Endpoint store. A not-yet-existing (or
// already-deleted) endpoint still yields a valid EndpointMetadata carrying
// only the required EndpointName — this backend does not require the
// referenced endpoint to exist at CreateInferenceExperiment time.
func (b *InMemoryBackend) inferenceExperimentEndpointMetadata(
	region, endpointName string,
) *InferenceExperimentEndpointMetadata {
	meta := &InferenceExperimentEndpointMetadata{EndpointName: endpointName}

	if ep, ok := b.endpointsStoreRO(region).Get(endpointName); ok {
		meta.EndpointConfigName = ep.EndpointConfigName
		meta.EndpointStatus = ep.EndpointStatus
		meta.FailureReason = ep.FailureReason
	}

	return meta
}

// CreateInferenceExperimentOptions holds input fields for
// CreateInferenceExperiment, mirroring CreateInferenceExperimentInput
// (api_op_CreateInferenceExperiment.go:29-90).
type CreateInferenceExperimentOptions struct {
	DataStorageConfig *InferenceExperimentDataStorageConfig
	Schedule          *InferenceExperimentSchedule
	ShadowModeConfig  *ShadowModeConfig
	Tags              map[string]string
	Name              string
	Type              string
	RoleArn           string
	EndpointName      string
	KmsKey            string
	ModelVariants     []ModelVariantConfig
}

// CreateInferenceExperiment creates an inference experiment. EndpointName,
// ModelVariants, and ShadowModeConfig are all required by the real API — a
// previous version of this backend never read any of the three, so no real
// client's endpoint association, variant infrastructure, or shadow-traffic
// split was ever stored.
func (b *InMemoryBackend) CreateInferenceExperiment(
	ctx context.Context,
	opts CreateInferenceExperimentOptions,
) (*InferenceExperiment, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if opts.EndpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", ErrValidation)
	}

	if len(opts.ModelVariants) == 0 {
		return nil, fmt.Errorf("%w: ModelVariants is required", ErrValidation)
	}

	if opts.ShadowModeConfig == nil {
		return nil, fmt.Errorf("%w: ShadowModeConfig is required", ErrValidation)
	}

	return sagemakerCreate(ctx, b,
		"CreateInferenceExperiment", opts.Name, "inference-experiment",
		b.inferenceExperimentsStore,
		func(n string) error { return sagemakerDupErr("inference experiment", n) },
		func(arnStr string, now time.Time) *InferenceExperiment {
			return &InferenceExperiment{
				Name:              opts.Name,
				Arn:               arnStr,
				Status:            statusRunning,
				Type:              opts.Type,
				RoleArn:           opts.RoleArn,
				EndpointName:      opts.EndpointName,
				ModelVariants:     opts.ModelVariants,
				ShadowModeConfig:  opts.ShadowModeConfig,
				DataStorageConfig: opts.DataStorageConfig,
				Schedule:          opts.Schedule,
				KmsKey:            opts.KmsKey,
				Tags:              mergeTags(nil, opts.Tags),
				CreationTime:      now,
				LastModifiedTime:  now,
			}
		},
		cloneInferenceExperiment,
	)
}

// DescribeInferenceExperiment returns an inference experiment by name, with
// EndpointMetadata freshly computed from the live Endpoint store.
func (b *InMemoryBackend) DescribeInferenceExperiment(ctx context.Context, name string) (*InferenceExperiment, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeInferenceExperiment")
	defer b.mu.RUnlock()

	e, ok := b.inferenceExperimentsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: inference experiment %q not found", ErrInferenceExperimentNotFound, name)
	}

	out := cloneInferenceExperiment(e)
	out.EndpointMetadata = b.inferenceExperimentEndpointMetadata(region, e.EndpointName)

	return out, nil
}

// StopInferenceExperimentOptions holds input fields for
// StopInferenceExperiment, mirroring StopInferenceExperimentInput
// (api_op_StopInferenceExperiment.go:27-56).
type StopInferenceExperimentOptions struct {
	ModelVariantActions  map[string]string
	DesiredState         string
	Reason               string
	DesiredModelVariants []ModelVariantConfig
}

// StopInferenceExperiment stops an inference experiment, setting its status
// to DesiredState (defaulting to "Cancelled" when unset, matching this
// backend's pre-existing behavior) and applying ModelVariantActions
// (required by the real API, previously never read at all) to the stored
// variant list: "Remove" drops a variant, "Promote" keeps only that variant,
// "Retain" is a no-op. DesiredModelVariants, when supplied, replaces the
// variant list outright instead.
func (b *InMemoryBackend) StopInferenceExperiment(
	ctx context.Context,
	name string,
	opts StopInferenceExperimentOptions,
) (*InferenceExperiment, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StopInferenceExperiment")
	defer b.mu.Unlock()

	e, ok := b.inferenceExperimentsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: inference experiment %q not found", ErrInferenceExperimentNotFound, name)
	}

	status := "Cancelled"
	if opts.DesiredState != "" {
		status = opts.DesiredState
	}

	e.Status = status
	e.StatusReason = opts.Reason
	e.LastModifiedTime = time.Now()

	switch {
	case len(opts.DesiredModelVariants) > 0:
		e.ModelVariants = opts.DesiredModelVariants
	case len(opts.ModelVariantActions) > 0:
		e.ModelVariants = applyModelVariantActions(e.ModelVariants, opts.ModelVariantActions)
	}

	return cloneInferenceExperiment(e), nil
}

// applyModelVariantActions applies a StopInferenceExperimentInput
// ModelVariantActions map (types.ModelVariantAction: Retain/Remove/Promote,
// types/enums.go:6216-6218) to variants.
func applyModelVariantActions(variants []ModelVariantConfig, actions map[string]string) []ModelVariantConfig {
	for _, v := range variants {
		if actions[v.VariantName] == "Promote" {
			return []ModelVariantConfig{v}
		}
	}

	out := make([]ModelVariantConfig, 0, len(variants))

	for _, v := range variants {
		if actions[v.VariantName] == "Remove" {
			continue
		}

		out = append(out, v)
	}

	return out
}

// StartInferenceExperiment transitions an inference experiment to "Running".
func (b *InMemoryBackend) StartInferenceExperiment(ctx context.Context, name string) (*InferenceExperiment, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StartInferenceExperiment")
	defer b.mu.Unlock()

	e, ok := b.inferenceExperimentsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: inference experiment %q not found", ErrInferenceExperimentNotFound, name)
	}

	e.Status = statusRunning
	e.LastModifiedTime = time.Now()

	return cloneInferenceExperiment(e), nil
}

// UpdateInferenceExperimentOptions holds input fields for
// UpdateInferenceExperiment, mirroring UpdateInferenceExperimentInput
// (api_op_UpdateInferenceExperiment.go:28-63); every member besides Name is
// optional and applied only when non-nil/non-empty.
type UpdateInferenceExperimentOptions struct {
	DataStorageConfig *InferenceExperimentDataStorageConfig
	Schedule          *InferenceExperimentSchedule
	ShadowModeConfig  *ShadowModeConfig
	Description       string
	ModelVariants     []ModelVariantConfig
}

// UpdateInferenceExperiment updates an inference experiment's mutable
// fields. DataStorageConfig/ModelVariants/Schedule/ShadowModeConfig were
// previously never read at all — every real client's update to any of them
// was silently dropped.
func (b *InMemoryBackend) UpdateInferenceExperiment(
	ctx context.Context,
	name string,
	opts UpdateInferenceExperimentOptions,
) (*InferenceExperiment, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateInferenceExperiment")
	defer b.mu.Unlock()

	e, ok := b.inferenceExperimentsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: inference experiment %q not found", ErrInferenceExperimentNotFound, name)
	}

	if opts.Description != "" {
		e.Description = opts.Description
	}

	if opts.DataStorageConfig != nil {
		e.DataStorageConfig = opts.DataStorageConfig
	}

	if opts.Schedule != nil {
		e.Schedule = opts.Schedule
	}

	if opts.ShadowModeConfig != nil {
		e.ShadowModeConfig = opts.ShadowModeConfig
	}

	if len(opts.ModelVariants) > 0 {
		e.ModelVariants = opts.ModelVariants
	}

	e.LastModifiedTime = time.Now()

	return cloneInferenceExperiment(e), nil
}

// DeleteInferenceExperiment removes an inference experiment by name.
func (b *InMemoryBackend) DeleteInferenceExperiment(ctx context.Context, name string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteInferenceExperiment")
	defer b.mu.Unlock()

	store := b.inferenceExperimentsStore(region)

	e, ok := store.Get(name)
	if !ok {
		return "", fmt.Errorf("%w: inference experiment %q not found", ErrInferenceExperimentNotFound, name)
	}

	arn := e.Arn
	store.Delete(name)

	return arn, nil
}

// ListInferenceExperimentsParams bundles the filter/sort criteria for
// ListInferenceExperiments, mirroring ListInferenceExperimentsInput
// (api_op_ListInferenceExperiments.go:29-59).
type ListInferenceExperimentsParams struct {
	CreationTimeAfter      *time.Time
	CreationTimeBefore     *time.Time
	LastModifiedTimeAfter  *time.Time
	LastModifiedTimeBefore *time.Time
	NameContains           string
	SortBy                 string
	SortOrder              string
	StatusEquals           string
	Type                   string
	NextToken              string
	MaxResults             int32
}

// ListInferenceExperiments returns inference experiments, optionally
// filtered and sorted per params. Neither SortBy nor SortOrder has a
// documented default; both are kept as the disclosed undocumented-default
// fallback (CreationTime / Ascending) already used elsewhere in this
// campaign for equally undocumented cases.
func (b *InMemoryBackend) ListInferenceExperiments(
	ctx context.Context,
	params ListInferenceExperimentsParams,
) ([]*InferenceExperiment, string) {
	b.mu.RLock("ListInferenceExperiments")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*InferenceExperiment, 0)

	for _, e := range b.inferenceExperimentsStoreRO(region).All() {
		if !matchesInferenceExperimentListParams(e, params) {
			continue
		}

		list = append(list, cloneInferenceExperiment(e))
	}

	sortInferenceExperimentsByParams(list, params)

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

func matchesInferenceExperimentListParams(e *InferenceExperiment, params ListInferenceExperimentsParams) bool {
	if params.StatusEquals != "" && e.Status != params.StatusEquals {
		return false
	}

	if params.Type != "" && e.Type != params.Type {
		return false
	}

	if params.NameContains != "" &&
		!strings.Contains(strings.ToLower(e.Name), strings.ToLower(params.NameContains)) {
		return false
	}

	return matchesInferenceExperimentTimeParams(e, params)
}

func matchesInferenceExperimentTimeParams(e *InferenceExperiment, params ListInferenceExperimentsParams) bool {
	if params.CreationTimeAfter != nil && !e.CreationTime.After(*params.CreationTimeAfter) {
		return false
	}

	if params.CreationTimeBefore != nil && !e.CreationTime.Before(*params.CreationTimeBefore) {
		return false
	}

	if params.LastModifiedTimeAfter != nil && !e.LastModifiedTime.After(*params.LastModifiedTimeAfter) {
		return false
	}

	if params.LastModifiedTimeBefore != nil && !e.LastModifiedTime.Before(*params.LastModifiedTimeBefore) {
		return false
	}

	return true
}

func sortInferenceExperimentsByParams(list []*InferenceExperiment, params ListInferenceExperimentsParams) {
	desc := strings.EqualFold(params.SortOrder, sortOrderDescending)

	sort.Slice(list, func(i, j int) bool {
		var less bool

		switch params.SortBy {
		case keyGenericName:
			less = list[i].Name < list[j].Name
		case keyStatus:
			less = list[i].Status < list[j].Status
		default:
			less = list[i].CreationTime.Before(list[j].CreationTime)
		}

		if desc {
			return !less
		}

		return less
	})
}
