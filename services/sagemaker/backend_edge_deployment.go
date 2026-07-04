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
	ErrEdgeDeploymentPlanNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
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

	if _, ok := b.deviceFleetsStore(region)[opts.DeviceFleetName]; !ok {
		return nil, fmt.Errorf("%w: device fleet %q", ErrDeviceFleetNotFound, opts.DeviceFleetName)
	}

	store := b.edgeDeploymentPlansStore(region)
	if _, ok := store[opts.EdgeDeploymentPlanName]; ok {
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
	store[opts.EdgeDeploymentPlanName] = p

	return cloneEdgeDeploymentPlan(p), nil
}

// DescribeEdgeDeploymentPlan returns an edge deployment plan by name.
func (b *InMemoryBackend) DescribeEdgeDeploymentPlan(ctx context.Context, name string) (*EdgeDeploymentPlan, error) {
	b.mu.RLock("DescribeEdgeDeploymentPlan")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	p, ok := b.edgeDeploymentPlansStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: edge deployment plan %q", ErrEdgeDeploymentPlanNotFound, name)
	}

	return cloneEdgeDeploymentPlan(p), nil
}

// DeleteEdgeDeploymentPlan deletes an edge deployment plan by name.
func (b *InMemoryBackend) DeleteEdgeDeploymentPlan(ctx context.Context, name string) error {
	b.mu.Lock("DeleteEdgeDeploymentPlan")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.edgeDeploymentPlansStore(region)

	if _, ok := store[name]; !ok {
		return fmt.Errorf("%w: edge deployment plan %q", ErrEdgeDeploymentPlanNotFound, name)
	}

	delete(store, name)

	return nil
}

// ListEdgeDeploymentPlans returns all edge deployment plans with pagination.
func (b *InMemoryBackend) ListEdgeDeploymentPlans(
	ctx context.Context,
	nextToken string,
) ([]*EdgeDeploymentPlan, string) {
	b.mu.RLock("ListEdgeDeploymentPlans")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(b.edgeDeploymentPlansStore(region), nextToken, cloneEdgeDeploymentPlan)
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

	p, ok := b.edgeDeploymentPlansStore(region)[planName]
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

	p, ok := b.edgeDeploymentPlansStore(region)[planName]
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

	p, ok := b.edgeDeploymentPlansStore(region)[planName]
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

	f, ok := b.deviceFleetsStore(region)[fleetName]
	if !ok {
		return nil, 0, fmt.Errorf("%w: device fleet %q", ErrDeviceFleetNotFound, fleetName)
	}

	registered := 0

	for k := range b.devicesStore(region) {
		if k.fleetName == fleetName {
			registered++
		}
	}

	return cloneDeviceFleet(f), registered, nil
}

// ListStageDevices returns the edge deployment plan, the devices in its
// device fleet, and the named stage's current deployment status, paginated.
func (b *InMemoryBackend) ListStageDevices(
	ctx context.Context,
	planName, stageName, nextToken string,
) (*EdgeDeploymentPlan, []*Device, string, string, error) {
	b.mu.RLock("ListStageDevices")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	p, ok := b.edgeDeploymentPlansStore(region)[planName]
	if !ok {
		return nil, nil, "", "", fmt.Errorf("%w: edge deployment plan %q", ErrEdgeDeploymentPlanNotFound, planName)
	}

	s := findEdgeDeploymentStage(p, stageName)
	if s == nil {
		return nil, nil, "", "", fmt.Errorf(
			"%w: stage %q in plan %q", ErrEdgeDeploymentStageNotFound, stageName, planName,
		)
	}

	devices, next := devicesInFleetPaged(b.devicesStore(region), p.DeviceFleetName, nextToken)

	return cloneEdgeDeploymentPlan(p), devices, s.DeploymentStatus, next, nil
}
