package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrFlowDefinitionNotFound is returned when a flow definition does not exist.
var ErrFlowDefinitionNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// FlowDefinition
// ---------------------------------------------------------------------------

// HumanLoopConfig mirrors types.HumanLoopConfig (types/types.go:9457-9560).
// PublicWorkforceTaskPrice remains unmodeled -- this backend never simulates
// paid Mechanical Turk work, disclosed.
type HumanLoopConfig struct {
	HumanTaskUIArn                    string   `json:"HumanTaskUiArn"`
	WorkteamArn                       string   `json:"WorkteamArn"`
	TaskTitle                         string   `json:"TaskTitle"`
	TaskDescription                   string   `json:"TaskDescription"`
	TaskKeywords                      []string `json:"TaskKeywords,omitempty"`
	TaskCount                         int32    `json:"TaskCount"`
	TaskAvailabilityLifetimeInSeconds int32    `json:"TaskAvailabilityLifetimeInSeconds,omitempty"`
	TaskTimeLimitInSeconds            int32    `json:"TaskTimeLimitInSeconds,omitempty"`
}

// FlowDefinitionOptions bundles the optional/required configuration
// CreateFlowDefinitionInput declares beyond FlowDefinitionName/RoleArn/Tags
// (api_op_CreateFlowDefinition.go:28-66). Previously OutputConfig (required)
// and every HumanLoop* field were entirely absent from decode.
type FlowDefinitionOptions struct {
	S3OutputPath                     string
	KmsKeyID                         string
	HumanLoopActivationConditions    string
	HumanLoopConfig                  *HumanLoopConfig
	AwsManagedHumanLoopRequestSource string
}

// FlowDefinition represents a SageMaker Augmented AI flow definition.
type FlowDefinition struct {
	CreationTime                     time.Time         `json:"CreationTime"`
	Tags                             map[string]string `json:"Tags,omitempty"`
	FlowDefinitionName               string            `json:"FlowDefinitionName"`
	FlowDefinitionArn                string            `json:"FlowDefinitionArn"`
	FlowDefinitionStatus             string            `json:"FlowDefinitionStatus"`
	RoleArn                          string            `json:"RoleArn,omitempty"`
	S3OutputPath                     string            `json:"-"`
	KmsKeyID                         string            `json:"-"`
	HumanLoopActivationConditions    string            `json:"-"`
	HumanLoopConfig                  *HumanLoopConfig  `json:"-"`
	AwsManagedHumanLoopRequestSource string            `json:"-"`
}

func cloneFlowDefinition(f *FlowDefinition) *FlowDefinition {
	cp := *f
	cp.Tags = maps.Clone(f.Tags)

	if f.HumanLoopConfig != nil {
		hlc := *f.HumanLoopConfig
		hlc.TaskKeywords = append([]string(nil), f.HumanLoopConfig.TaskKeywords...)
		cp.HumanLoopConfig = &hlc
	}

	return &cp
}

// flowDefinitionOutputConfig mirrors types.FlowDefinitionOutputConfig
// (types/types.go:9102-9117).
type flowDefinitionOutputConfig struct {
	S3OutputPath string `json:"S3OutputPath"`
	KmsKeyID     string `json:"KmsKeyId,omitempty"`
}

// flowDefinitionHumanLoopActivationConfig mirrors
// types.HumanLoopActivationConfig/HumanLoopActivationConditionsConfig
// (types/types.go:9425-9454).
type flowDefinitionHumanLoopActivationConfig struct {
	HumanLoopActivationConditionsConfig struct {
		HumanLoopActivationConditions string `json:"HumanLoopActivationConditions"`
	} `json:"HumanLoopActivationConditionsConfig"`
}

// flowDefinitionHumanLoopRequestSource mirrors types.HumanLoopRequestSource
// (types/types.go:9722-9732).
type flowDefinitionHumanLoopRequestSource struct {
	AwsManagedHumanLoopRequestSource string `json:"AwsManagedHumanLoopRequestSource"`
}

// MarshalJSON emits CreationTime as an AWS awsjson1.1 epoch-seconds number
// rather than Go's default RFC3339 string, and nests OutputConfig (always
// present -- required on Create) and the optional HumanLoopActivationConfig/
// HumanLoopConfig/HumanLoopRequestSource -- this struct is marshaled
// directly by handleDescribeFlowDefinition.
func (f *FlowDefinition) MarshalJSON() ([]byte, error) {
	type alias FlowDefinition

	aux := struct {
		*alias
		HumanLoopActivationConfig *flowDefinitionHumanLoopActivationConfig `json:"HumanLoopActivationConfig,omitempty"`
		HumanLoopConfig           *HumanLoopConfig                         `json:"HumanLoopConfig,omitempty"`
		HumanLoopRequestSource    *flowDefinitionHumanLoopRequestSource    `json:"HumanLoopRequestSource,omitempty"`
		OutputConfig              flowDefinitionOutputConfig               `json:"OutputConfig"`
		CreationTime              float64                                  `json:"CreationTime"`
	}{
		alias:           (*alias)(f),
		CreationTime:    epochSeconds(f.CreationTime),
		OutputConfig:    flowDefinitionOutputConfig{S3OutputPath: f.S3OutputPath, KmsKeyID: f.KmsKeyID},
		HumanLoopConfig: f.HumanLoopConfig,
	}

	if f.HumanLoopActivationConditions != "" {
		hlac := &flowDefinitionHumanLoopActivationConfig{}
		hlac.HumanLoopActivationConditionsConfig.HumanLoopActivationConditions = f.HumanLoopActivationConditions
		aux.HumanLoopActivationConfig = hlac
	}

	if f.AwsManagedHumanLoopRequestSource != "" {
		aux.HumanLoopRequestSource = &flowDefinitionHumanLoopRequestSource{
			AwsManagedHumanLoopRequestSource: f.AwsManagedHumanLoopRequestSource,
		}
	}

	return json.Marshal(aux)
}

// UnmarshalJSON is the inverse of [FlowDefinition.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (f *FlowDefinition) UnmarshalJSON(data []byte) error {
	type alias FlowDefinition

	aux := struct {
		*alias
		HumanLoopActivationConfig *flowDefinitionHumanLoopActivationConfig `json:"HumanLoopActivationConfig"`
		HumanLoopConfig           *HumanLoopConfig                         `json:"HumanLoopConfig"`
		HumanLoopRequestSource    *flowDefinitionHumanLoopRequestSource    `json:"HumanLoopRequestSource"`
		OutputConfig              flowDefinitionOutputConfig               `json:"OutputConfig"`
		CreationTime              float64                                  `json:"CreationTime"`
	}{alias: (*alias)(f)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	f.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	f.S3OutputPath = aux.OutputConfig.S3OutputPath
	f.KmsKeyID = aux.OutputConfig.KmsKeyID
	f.HumanLoopConfig = aux.HumanLoopConfig

	if aux.HumanLoopActivationConfig != nil {
		cond := aux.HumanLoopActivationConfig.HumanLoopActivationConditionsConfig
		f.HumanLoopActivationConditions = cond.HumanLoopActivationConditions
	}

	if aux.HumanLoopRequestSource != nil {
		f.AwsManagedHumanLoopRequestSource = aux.HumanLoopRequestSource.AwsManagedHumanLoopRequestSource
	}

	return nil
}

// CreateFlowDefinition creates a flow definition.
func (b *InMemoryBackend) CreateFlowDefinition(
	ctx context.Context,
	name, roleArn string,
	tags map[string]string,
	opts FlowDefinitionOptions,
) (*FlowDefinition, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateFlowDefinition")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: FlowDefinitionName is required", ErrValidation)
	}

	if opts.S3OutputPath == "" {
		return nil, fmt.Errorf("%w: OutputConfig.S3OutputPath is required", ErrValidation)
	}

	store := b.flowDefinitionsStore(region)

	if _, ok := store.Get(name); ok {
		return nil, fmt.Errorf("%w: flow definition %q already exists", ErrValidation, name)
	}

	flowARN := arn.Build("sagemaker", region, b.accountID, "flow-definition/"+name)

	f := &FlowDefinition{
		FlowDefinitionName:               name,
		FlowDefinitionArn:                flowARN,
		FlowDefinitionStatus:             statusActive,
		RoleArn:                          roleArn,
		Tags:                             mergeTags(nil, tags),
		CreationTime:                     time.Now(),
		S3OutputPath:                     opts.S3OutputPath,
		KmsKeyID:                         opts.KmsKeyID,
		HumanLoopActivationConditions:    opts.HumanLoopActivationConditions,
		HumanLoopConfig:                  opts.HumanLoopConfig,
		AwsManagedHumanLoopRequestSource: opts.AwsManagedHumanLoopRequestSource,
	}
	store.Put(f)

	return cloneFlowDefinition(f), nil
}

// DescribeFlowDefinition returns a flow definition by name.
func (b *InMemoryBackend) DescribeFlowDefinition(ctx context.Context, name string) (*FlowDefinition, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeFlowDefinition")
	defer b.mu.RUnlock()

	f, ok := b.flowDefinitionsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: flow definition %q not found", ErrFlowDefinitionNotFound, name)
	}

	return cloneFlowDefinition(f), nil
}

// DeleteFlowDefinition removes a flow definition by name.
func (b *InMemoryBackend) DeleteFlowDefinition(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteFlowDefinition")
	defer b.mu.Unlock()

	store := b.flowDefinitionsStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf("%w: flow definition %q not found", ErrFlowDefinitionNotFound, name)
	}

	store.Delete(name)

	return nil
}

// ListFlowDefinitionsFilter bundles the filter/sort criteria for
// ListFlowDefinitions (api_op_ListFlowDefinitions.go:30-52). Previously this
// decoded only NextToken and dropped CreationTimeAfter/Before, MaxResults
// and SortOrder entirely.
type ListFlowDefinitionsFilter struct {
	CreationTimeAfter  *time.Time
	CreationTimeBefore *time.Time
	SortOrder          string
	MaxResults         int32
}

// ListFlowDefinitions returns flow definitions matching f, paginated. Like
// ListHumanTaskUIs, the op declares no SortBy field and its own doc states
// no default order, so SortOrder is applied to this backend's pre-existing
// default order (ascending by name) rather than an invented dimension.
func (b *InMemoryBackend) ListFlowDefinitions(
	ctx context.Context,
	nextToken string,
	f ListFlowDefinitionsFilter,
) ([]*FlowDefinition, string) {
	b.mu.RLock("ListFlowDefinitions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return filterSortPaginateByNameWindow(
		b.flowDefinitionsStoreRO(region).All(),
		nextToken,
		nameWindowSortParams(f),
		func(v *FlowDefinition) time.Time { return v.CreationTime },
		func(v *FlowDefinition) string { return v.FlowDefinitionName },
		cloneFlowDefinition,
	)
}
