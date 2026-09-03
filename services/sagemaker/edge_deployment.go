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
)

// Edge deployment stage status values, mirroring the StageStatus /
// DeviceDeploymentStatus enums used by SageMaker Edge Manager. This emulator
// does not simulate per-device deployment progress, so a stage moves directly
// between these states rather than transiting the transient CREATING/STARTING
// values a real device fleet would pass through.
const (
	edgeStageReadyToDeploy = "READYTODEPLOY"
	edgeStageInProgress    = "INPROGRESS"
	edgeStageStopped       = "STOPPED"
)

var (
	// ErrEdgeDeploymentPlanNotFound is returned when an edge deployment plan does not exist.
	ErrEdgeDeploymentPlanNotFound = awserr.New("ResourceNotFound", ErrResourceNotFound)
	// ErrEdgeDeploymentPlanAlreadyExists is returned when an edge deployment plan already exists.
	ErrEdgeDeploymentPlanAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrEdgeDeploymentStageNotFound is returned when a stage does not exist within a plan.
	ErrEdgeDeploymentStageNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
)

// EdgeDeploymentModelConfig describes one model packaged for edge deployment.
type EdgeDeploymentModelConfig struct {
	ModelHandle          string `json:"ModelHandle"`
	EdgePackagingJobName string `json:"EdgePackagingJobName"`
}

// EdgeDeploymentStage represents one stage of an edge deployment plan. The
// device selection and deployment configs are stored opaquely (as submitted)
// since this emulator does not act on them beyond round-tripping.
type EdgeDeploymentStage struct {
	StageName             string          `json:"StageName"`
	DeploymentStatus      string          `json:"DeploymentStatus"`
	DeviceSelectionConfig json.RawMessage `json:"DeviceSelectionConfig,omitempty"`
	DeploymentConfig      json.RawMessage `json:"DeploymentConfig,omitempty"`
}

func cloneEdgeDeploymentStage(s *EdgeDeploymentStage) *EdgeDeploymentStage {
	cp := *s
	if s.DeviceSelectionConfig != nil {
		cp.DeviceSelectionConfig = append(json.RawMessage(nil), s.DeviceSelectionConfig...)
	}

	if s.DeploymentConfig != nil {
		cp.DeploymentConfig = append(json.RawMessage(nil), s.DeploymentConfig...)
	}

	return &cp
}

// EdgeDeploymentPlan represents a SageMaker edge deployment plan.
// EdgeDeploymentSuccess/Pending/Failed (both DescribeEdgeDeploymentPlanOutput
// and EdgeDeploymentPlanSummary) are not modeled as stored counters: like
// per-stage DeploymentStatus above, this emulator does not simulate
// per-device deployment progress, so every real client sees zero for all
// three regardless of stage state.
type EdgeDeploymentPlan struct {
	CreationTime           time.Time                   `json:"CreationTime"`
	LastModifiedTime       time.Time                   `json:"LastModifiedTime"`
	Tags                   map[string]string           `json:"Tags,omitempty"`
	EdgeDeploymentPlanName string                      `json:"EdgeDeploymentPlanName"`
	EdgeDeploymentPlanArn  string                      `json:"EdgeDeploymentPlanArn"`
	DeviceFleetName        string                      `json:"DeviceFleetName"`
	ModelConfigs           []EdgeDeploymentModelConfig `json:"ModelConfigs,omitempty"`
	Stages                 []*EdgeDeploymentStage      `json:"Stages,omitempty"`
}

func cloneEdgeDeploymentPlan(p *EdgeDeploymentPlan) *EdgeDeploymentPlan {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)
	cp.ModelConfigs = append([]EdgeDeploymentModelConfig(nil), p.ModelConfigs...)
	cp.Stages = make([]*EdgeDeploymentStage, len(p.Stages))

	for i, s := range p.Stages {
		cp.Stages[i] = cloneEdgeDeploymentStage(s)
	}

	return &cp
}

// EdgeDeploymentStageInput is a single stage supplied to
// CreateEdgeDeploymentPlan or CreateEdgeDeploymentStage.
type EdgeDeploymentStageInput struct {
	StageName             string
	DeviceSelectionConfig json.RawMessage
	DeploymentConfig      json.RawMessage
}

func buildEdgeDeploymentStages(inputs []EdgeDeploymentStageInput) []*EdgeDeploymentStage {
	stages := make([]*EdgeDeploymentStage, 0, len(inputs))
	for _, in := range inputs {
		stages = append(stages, &EdgeDeploymentStage{
			StageName:             in.StageName,
			DeviceSelectionConfig: in.DeviceSelectionConfig,
			DeploymentConfig:      in.DeploymentConfig,
			DeploymentStatus:      edgeStageReadyToDeploy,
		})
	}

	return stages
}

// findEdgeDeploymentStage returns the stage with the given name in p, or nil.
func findEdgeDeploymentStage(p *EdgeDeploymentPlan, stageName string) *EdgeDeploymentStage {
	for _, s := range p.Stages {
		if s.StageName == stageName {
			return s
		}
	}

	return nil
}

// CreateEdgeDeploymentPlanOptions holds input fields for CreateEdgeDeploymentPlan.
type CreateEdgeDeploymentPlanOptions struct {
	Tags                   map[string]string
	EdgeDeploymentPlanName string
	DeviceFleetName        string
	ModelConfigs           []EdgeDeploymentModelConfig
	Stages                 []EdgeDeploymentStageInput
}

// CreateEdgeDeploymentPlan creates a SageMaker edge deployment plan.
func (b *InMemoryBackend) CreateEdgeDeploymentPlan(
	ctx context.Context,
	opts CreateEdgeDeploymentPlanOptions,
) (*EdgeDeploymentPlan, error) {
	b.mu.Lock("CreateEdgeDeploymentPlan")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if opts.EdgeDeploymentPlanName == "" {
		return nil, fmt.Errorf("%w: EdgeDeploymentPlanName is required", ErrValidation)
	}

	if opts.DeviceFleetName == "" {
		return nil, fmt.Errorf("%w: DeviceFleetName is required", ErrValidation)
	}

	if _, ok := b.deviceFleetsStore(region).Get(opts.DeviceFleetName); !ok {
		return nil, fmt.Errorf("%w: device fleet %q", ErrDeviceFleetNotFound, opts.DeviceFleetName)
	}

	store := b.edgeDeploymentPlansStore(region)
	if _, ok := store.Get(opts.EdgeDeploymentPlanName); ok {
		return nil, fmt.Errorf(
			"%w: edge deployment plan %q already exists",
			ErrEdgeDeploymentPlanAlreadyExists,
			opts.EdgeDeploymentPlanName,
		)
	}

	planARN := arn.Build("sagemaker", region, b.accountID, "edge-deployment-plan/"+opts.EdgeDeploymentPlanName)
	now := time.Now()

	p := &EdgeDeploymentPlan{
		EdgeDeploymentPlanName: opts.EdgeDeploymentPlanName,
		EdgeDeploymentPlanArn:  planARN,
		DeviceFleetName:        opts.DeviceFleetName,
		ModelConfigs:           append([]EdgeDeploymentModelConfig(nil), opts.ModelConfigs...),
		Stages:                 buildEdgeDeploymentStages(opts.Stages),
		Tags:                   mergeTags(nil, opts.Tags),
		CreationTime:           now,
		LastModifiedTime:       now,
	}
	store.Put(p)

	return cloneEdgeDeploymentPlan(p), nil
}

// DescribeEdgeDeploymentPlan returns an edge deployment plan by name, with
// Stages paginated per maxResults/nextToken
// (api_op_DescribeEdgeDeploymentPlan.go:39-41: "If the edge deployment plan
// has enough stages to require tokening, then this is the response from the
// last list of stages returned").
func (b *InMemoryBackend) DescribeEdgeDeploymentPlan(
	ctx context.Context,
	name, nextToken string,
	maxResults int32,
) (*EdgeDeploymentPlan, string, error) {
	b.mu.RLock("DescribeEdgeDeploymentPlan")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	p, ok := b.edgeDeploymentPlansStoreRO(region).Get(name)
	if !ok {
		return nil, "", fmt.Errorf("%w: edge deployment plan %q", ErrEdgeDeploymentPlanNotFound, name)
	}

	cp := cloneEdgeDeploymentPlan(p)
	stages, next := paginateSlice(cp.Stages, nextToken, maxResults)
	cp.Stages = stages

	return cp, next, nil
}

// DeleteEdgeDeploymentPlan deletes an edge deployment plan by name.
func (b *InMemoryBackend) DeleteEdgeDeploymentPlan(ctx context.Context, name string) error {
	b.mu.Lock("DeleteEdgeDeploymentPlan")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.edgeDeploymentPlansStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf("%w: edge deployment plan %q", ErrEdgeDeploymentPlanNotFound, name)
	}

	store.Delete(name)

	return nil
}

// ListEdgeDeploymentPlansParams bundles ListEdgeDeploymentPlans'
// filter/sort/pagination criteria (api_op_ListEdgeDeploymentPlans.go:30-65,
// sagemaker@v1.263.2).
type ListEdgeDeploymentPlansParams struct {
	CreationTimeAfter       *time.Time
	CreationTimeBefore      *time.Time
	LastModifiedTimeAfter   *time.Time
	LastModifiedTimeBefore  *time.Time
	DeviceFleetNameContains string
	NameContains            string
	NextToken               string
	SortBy                  string
	SortOrder               string
	MaxResults              int32
}

// ListEdgeDeploymentPlans returns edge deployment plans matching params. The
// real op's doc (api_op_ListEdgeDeploymentPlans.go:57-61) states no default
// SortBy/SortOrder; CreationTime/Ascending are kept as the disclosed
// fallback, this campaign's recurring ListHubs/ListPipelines precedent for an
// undocumented default.
func (b *InMemoryBackend) ListEdgeDeploymentPlans(
	ctx context.Context,
	params ListEdgeDeploymentPlansParams,
) ([]*EdgeDeploymentPlan, string) {
	b.mu.RLock("ListEdgeDeploymentPlans")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	tbl := b.edgeDeploymentPlansStoreRO(region)
	list := make([]*EdgeDeploymentPlan, 0, tbl.Len())

	for _, p := range tbl.All() {
		if !matchesEdgeDeploymentPlanListParams(p, params) {
			continue
		}

		list = append(list, cloneEdgeDeploymentPlan(p))
	}

	desc := strings.EqualFold(params.SortOrder, sortOrderDescending)
	sort.Slice(list, func(i, j int) bool {
		less := edgeDeploymentPlanSortLess(list[i], list[j], params.SortBy)
		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

// matchesEdgeDeploymentPlanListParams reports whether p satisfies every filter in params.
func matchesEdgeDeploymentPlanListParams(p *EdgeDeploymentPlan, params ListEdgeDeploymentPlansParams) bool {
	if params.NameContains != "" && !strings.Contains(p.EdgeDeploymentPlanName, params.NameContains) {
		return false
	}

	if params.DeviceFleetNameContains != "" && !strings.Contains(p.DeviceFleetName, params.DeviceFleetNameContains) {
		return false
	}

	if params.CreationTimeAfter != nil && !p.CreationTime.After(*params.CreationTimeAfter) {
		return false
	}

	if params.CreationTimeBefore != nil && !p.CreationTime.Before(*params.CreationTimeBefore) {
		return false
	}

	if params.LastModifiedTimeAfter != nil && !p.LastModifiedTime.After(*params.LastModifiedTimeAfter) {
		return false
	}

	if params.LastModifiedTimeBefore != nil && !p.LastModifiedTime.Before(*params.LastModifiedTimeBefore) {
		return false
	}

	return true
}

// edgeDeploymentPlanSortLess orders two plans by sortBy — one of
// ListEdgeDeploymentPlansSortBy's real values (NAME/DEVICE_FLEET_NAME/
// CREATION_TIME/LAST_MODIFIED_TIME, types/enums.go:5312-5315). The op's own
// doc comment (api_op_ListEdgeDeploymentPlans.go:57-58) lists these without
// underscores ("DEVICEFLEETNAME", "CREATIONTIME", "LASTMODIFIEDTIME") — the
// enum constants are the real wire values, the doc prose is wrong.
func edgeDeploymentPlanSortLess(a, b *EdgeDeploymentPlan, sortBy string) bool {
	switch sortBy {
	case "DEVICE_FLEET_NAME":
		if a.DeviceFleetName != b.DeviceFleetName {
			return a.DeviceFleetName < b.DeviceFleetName
		}
	case sortByLastModifiedTime:
		if !a.LastModifiedTime.Equal(b.LastModifiedTime) {
			return a.LastModifiedTime.Before(b.LastModifiedTime)
		}
	case sortByName:
		if a.EdgeDeploymentPlanName != b.EdgeDeploymentPlanName {
			return a.EdgeDeploymentPlanName < b.EdgeDeploymentPlanName
		}
	default:
		if !a.CreationTime.Equal(b.CreationTime) {
			return a.CreationTime.Before(b.CreationTime)
		}
	}

	return a.EdgeDeploymentPlanName < b.EdgeDeploymentPlanName
}

// CreateEdgeDeploymentStage appends new stages to an existing edge deployment plan.
func (b *InMemoryBackend) CreateEdgeDeploymentStage(
	ctx context.Context,
	planName string,
	stages []EdgeDeploymentStageInput,
) error {
	b.mu.Lock("CreateEdgeDeploymentStage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.edgeDeploymentPlansStore(region).Get(planName)
	if !ok {
		return fmt.Errorf("%w: edge deployment plan %q", ErrEdgeDeploymentPlanNotFound, planName)
	}

	p.Stages = append(p.Stages, buildEdgeDeploymentStages(stages)...)
	p.LastModifiedTime = time.Now()

	return nil
}

// DeleteEdgeDeploymentStage removes a stage from an edge deployment plan.
func (b *InMemoryBackend) DeleteEdgeDeploymentStage(ctx context.Context, planName, stageName string) error {
	b.mu.Lock("DeleteEdgeDeploymentStage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.edgeDeploymentPlansStore(region).Get(planName)
	if !ok {
		return fmt.Errorf("%w: edge deployment plan %q", ErrEdgeDeploymentPlanNotFound, planName)
	}

	idx := -1

	for i, s := range p.Stages {
		if s.StageName == stageName {
			idx = i

			break
		}
	}

	if idx == -1 {
		return fmt.Errorf("%w: stage %q in plan %q", ErrEdgeDeploymentStageNotFound, stageName, planName)
	}

	p.Stages = append(p.Stages[:idx], p.Stages[idx+1:]...)
	p.LastModifiedTime = time.Now()

	return nil
}

// setEdgeDeploymentStageStatus transitions a stage's DeploymentStatus, used by
// StartEdgeDeploymentStage/StopEdgeDeploymentStage.
func (b *InMemoryBackend) setEdgeDeploymentStageStatus(ctx context.Context, planName, stageName, status string) error {
	b.mu.Lock("SetEdgeDeploymentStageStatus")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.edgeDeploymentPlansStore(region).Get(planName)
	if !ok {
		return fmt.Errorf("%w: edge deployment plan %q", ErrEdgeDeploymentPlanNotFound, planName)
	}

	s := findEdgeDeploymentStage(p, stageName)
	if s == nil {
		return fmt.Errorf("%w: stage %q in plan %q", ErrEdgeDeploymentStageNotFound, stageName, planName)
	}

	s.DeploymentStatus = status
	p.LastModifiedTime = time.Now()

	return nil
}

// StartEdgeDeploymentStage starts a deployment stage.
func (b *InMemoryBackend) StartEdgeDeploymentStage(ctx context.Context, planName, stageName string) error {
	return b.setEdgeDeploymentStageStatus(ctx, planName, stageName, edgeStageInProgress)
}

// StopEdgeDeploymentStage stops a deployment stage.
func (b *InMemoryBackend) StopEdgeDeploymentStage(ctx context.Context, planName, stageName string) error {
	return b.setEdgeDeploymentStageStatus(ctx, planName, stageName, edgeStageStopped)
}

// GetDeviceFleetReport returns the device fleet along with its current
// registered device count.
func (b *InMemoryBackend) GetDeviceFleetReport(ctx context.Context, fleetName string) (*DeviceFleet, int, error) {
	b.mu.RLock("GetDeviceFleetReport")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	f, ok := b.deviceFleetsStoreRO(region).Get(fleetName)
	if !ok {
		return nil, 0, fmt.Errorf("%w: device fleet %q", ErrDeviceFleetNotFound, fleetName)
	}

	registered := 0

	for _, d := range b.devicesStoreRO(region).All() {
		if d.DeviceFleetName == fleetName {
			registered++
		}
	}

	return cloneDeviceFleet(f), registered, nil
}

// ListStageDevices returns the edge deployment plan, the devices in its
// device fleet, and the named stage's current deployment status, paginated.
// excludeOtherStage (ExcludeDevicesDeployedInOtherStage,
// api_op_ListStageDevices.go:42-43) is accepted for wire shape but is a
// real no-op: this backend tracks one DeploymentStatus per stage, not a
// per-device per-stage assignment, so there is no "deployed in another
// stage" fact to exclude on.
func (b *InMemoryBackend) ListStageDevices(
	ctx context.Context,
	planName, stageName, nextToken string,
	maxResults int32,
	_ bool,
) (*EdgeDeploymentPlan, []*Device, string, string, error) {
	b.mu.RLock("ListStageDevices")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	p, ok := b.edgeDeploymentPlansStoreRO(region).Get(planName)
	if !ok {
		return nil, nil, "", "", fmt.Errorf("%w: edge deployment plan %q", ErrEdgeDeploymentPlanNotFound, planName)
	}

	s := findEdgeDeploymentStage(p, stageName)
	if s == nil {
		return nil, nil, "", "", fmt.Errorf(
			"%w: stage %q in plan %q", ErrEdgeDeploymentStageNotFound, stageName, planName,
		)
	}

	devices, next := devicesInFleetPaged(b.devicesStoreRO(region), p.DeviceFleetName, nextToken, maxResults)

	return cloneEdgeDeploymentPlan(p), devices, s.DeploymentStatus, next, nil
}
