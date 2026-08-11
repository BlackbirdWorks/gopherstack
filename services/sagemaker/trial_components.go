package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrTrialComponentNotFound is returned when a trial component does not exist.
	ErrTrialComponentNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrTrialComponentAlreadyExists is returned when a trial component already exists.
	ErrTrialComponentAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// TrialComponent represents a SageMaker Trial Component.
type TrialComponent struct {
	CreationTime       time.Time                         `json:"CreationTime"`
	LastModifiedTime   time.Time                         `json:"LastModifiedTime"`
	StartTime          *time.Time                        `json:"StartTime,omitempty"`
	EndTime            *time.Time                        `json:"EndTime,omitempty"`
	Status             *TrialComponentStatus             `json:"Status,omitempty"`
	Tags               map[string]string                 `json:"Tags,omitempty"`
	Parameters         map[string]TrialComponentValue    `json:"Parameters,omitempty"`
	InputArtifacts     map[string]TrialComponentArtifact `json:"InputArtifacts,omitempty"`
	OutputArtifacts    map[string]TrialComponentArtifact `json:"OutputArtifacts,omitempty"`
	TrialComponentName string                            `json:"TrialComponentName"`
	TrialComponentArn  string                            `json:"TrialComponentArn"`
	DisplayName        string                            `json:"DisplayName,omitempty"`
}

// TrialComponentStatus reports a trial component's lifecycle state.
// SDK ref: aws-sdk-go-v2/service/sagemaker/types.TrialComponentStatus
// ({PrimaryStatus, Message}) — DescribeTrialComponentOutput.Status is this
// struct on the wire, never a bare string.
type TrialComponentStatus struct {
	PrimaryStatus string `json:"PrimaryStatus,omitempty"`
	Message       string `json:"Message,omitempty"`
}

// TrialComponentValue is a number or string parameter value.
type TrialComponentValue struct {
	NumberValue *float64 `json:"NumberValue,omitempty"`
	StringValue string   `json:"StringValue,omitempty"`
}

// TrialComponentArtifact is a URI/media-type artifact reference.
type TrialComponentArtifact struct {
	Value     string `json:"Value"`
	MediaType string `json:"MediaType,omitempty"`
}

func cloneTrialComponent(tc *TrialComponent) *TrialComponent {
	cp := *tc
	cp.Tags = maps.Clone(tc.Tags)
	cp.Parameters = maps.Clone(tc.Parameters)
	cp.InputArtifacts = maps.Clone(tc.InputArtifacts)
	cp.OutputArtifacts = maps.Clone(tc.OutputArtifacts)

	if tc.StartTime != nil {
		st := *tc.StartTime
		cp.StartTime = &st
	}

	if tc.EndTime != nil {
		et := *tc.EndTime
		cp.EndTime = &et
	}

	if tc.Status != nil {
		st := *tc.Status
		cp.Status = &st
	}

	return &cp
}

// CreateTrialComponentOptions holds the parameters CreateTrialComponent accepts.
type CreateTrialComponentOptions struct {
	StartTime          *time.Time
	EndTime            *time.Time
	Status             *TrialComponentStatus
	Parameters         map[string]TrialComponentValue
	InputArtifacts     map[string]TrialComponentArtifact
	OutputArtifacts    map[string]TrialComponentArtifact
	Tags               map[string]string
	TrialComponentName string
	DisplayName        string
}

// CreateTrialComponent creates a new trial component.
func (b *InMemoryBackend) CreateTrialComponent(
	ctx context.Context,
	opts CreateTrialComponentOptions,
) (*TrialComponent, error) {
	b.mu.Lock("CreateTrialComponent")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.trialComponentsStore(region).Get(opts.TrialComponentName); ok {
		return nil, fmt.Errorf(
			"%w: trial component %s already exists",
			ErrTrialComponentAlreadyExists,
			opts.TrialComponentName,
		)
	}

	tcArn := arn.Build(
		"sagemaker", region, b.accountID, "experiment-trial-component/"+opts.TrialComponentName,
	)
	now := time.Now()

	tc := &TrialComponent{
		TrialComponentName: opts.TrialComponentName,
		TrialComponentArn:  tcArn,
		DisplayName:        opts.DisplayName,
		StartTime:          opts.StartTime,
		EndTime:            opts.EndTime,
		Status:             opts.Status,
		Parameters:         maps.Clone(opts.Parameters),
		InputArtifacts:     maps.Clone(opts.InputArtifacts),
		OutputArtifacts:    maps.Clone(opts.OutputArtifacts),
		CreationTime:       now,
		LastModifiedTime:   now,
		Tags:               mergeTags(nil, opts.Tags),
	}
	b.trialComponentsStore(region).Put(tc)

	return cloneTrialComponent(tc), nil
}

// DescribeTrialComponent returns a trial component by name.
func (b *InMemoryBackend) DescribeTrialComponent(ctx context.Context, name string) (*TrialComponent, error) {
	b.mu.RLock("DescribeTrialComponent")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	tc, ok := b.trialComponentsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: trial component %q not found", ErrTrialComponentNotFound, name)
	}

	return cloneTrialComponent(tc), nil
}

// DeleteTrialComponent deletes a trial component.
func (b *InMemoryBackend) DeleteTrialComponent(ctx context.Context, name string) (*TrialComponent, error) {
	b.mu.Lock("DeleteTrialComponent")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.trialComponentsStore(region)

	tc, ok := store.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: trial component %q not found", ErrTrialComponentNotFound, name)
	}

	cp := cloneTrialComponent(tc)
	store.Delete(name)

	return cp, nil
}

// UpdateTrialComponentOptions holds optional fields for UpdateTrialComponent.
type UpdateTrialComponentOptions struct {
	StartTime       *time.Time
	EndTime         *time.Time
	Status          *TrialComponentStatus
	Parameters      map[string]TrialComponentValue    `json:"Parameters,omitempty"`
	InputArtifacts  map[string]TrialComponentArtifact `json:"InputArtifacts,omitempty"`
	OutputArtifacts map[string]TrialComponentArtifact `json:"OutputArtifacts,omitempty"`
	DisplayName     string                            `json:"DisplayName,omitempty"`
}

// UpdateTrialComponent mutates DisplayName, Parameters, and Artifacts on a trial component.
func (b *InMemoryBackend) UpdateTrialComponent(
	ctx context.Context,
	name string,
	opts UpdateTrialComponentOptions,
) (*TrialComponent, error) {
	b.mu.Lock("UpdateTrialComponent")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	tc, ok := b.trialComponentsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: trial component %q not found", ErrTrialComponentNotFound, name)
	}

	if opts.DisplayName != "" {
		tc.DisplayName = opts.DisplayName
	}
	if opts.Status != nil {
		st := *opts.Status
		tc.Status = &st
	}
	if opts.StartTime != nil {
		st := *opts.StartTime
		tc.StartTime = &st
	}
	if opts.EndTime != nil {
		et := *opts.EndTime
		tc.EndTime = &et
	}
	if len(opts.Parameters) > 0 {
		if tc.Parameters == nil {
			tc.Parameters = make(map[string]TrialComponentValue)
		}
		maps.Copy(tc.Parameters, opts.Parameters)
	}
	if len(opts.InputArtifacts) > 0 {
		if tc.InputArtifacts == nil {
			tc.InputArtifacts = make(map[string]TrialComponentArtifact)
		}
		maps.Copy(tc.InputArtifacts, opts.InputArtifacts)
	}
	if len(opts.OutputArtifacts) > 0 {
		if tc.OutputArtifacts == nil {
			tc.OutputArtifacts = make(map[string]TrialComponentArtifact)
		}
		maps.Copy(tc.OutputArtifacts, opts.OutputArtifacts)
	}
	tc.LastModifiedTime = time.Now()

	return cloneTrialComponent(tc), nil
}

// DisassociateTrialComponent removes the association between a trial and a
// trial component, if one exists. It is idempotent: disassociating a
// component that was never associated succeeds and simply returns the
// resources' ARNs (mirroring AssociateTrialComponent's non-strict existence
// checks).
func (b *InMemoryBackend) DisassociateTrialComponent(
	ctx context.Context,
	trialName, trialComponentName string,
) (string, string, error) {
	b.mu.Lock("DisassociateTrialComponent")
	defer b.mu.Unlock()

	if trialName == "" {
		return "", "", fmt.Errorf("%w: TrialName is required", ErrValidation)
	}

	if trialComponentName == "" {
		return "", "", fmt.Errorf("%w: TrialComponentName is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)

	trialArn := arn.Build("sagemaker", region, b.accountID, "experiment-trial/"+trialName)
	trialComponentArn := arn.Build(
		"sagemaker", region, b.accountID, "experiment-trial-component/"+trialComponentName,
	)

	key := trialComponentKey(trialName, trialComponentName)
	b.trialComponentAssociationsStore(region).Delete(key)

	return trialArn, trialComponentArn, nil
}

// ListTrialComponents returns trial components, optionally filtered by the
// trial they're associated with or the experiment their trial belongs to.
func (b *InMemoryBackend) ListTrialComponents(
	ctx context.Context,
	experimentName, trialName, nextToken string,
) ([]*TrialComponent, string) {
	b.mu.RLock("ListTrialComponents")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	allowed := b.trialComponentNameFilterLocked(region, experimentName, trialName)

	list := make([]*TrialComponent, 0, b.trialComponentsStoreRO(region).Len())

	for _, tc := range b.trialComponentsStoreRO(region).All() {
		if allowed != nil {
			if _, ok := allowed[tc.TrialComponentName]; !ok {
				continue
			}
		}

		list = append(list, cloneTrialComponent(tc))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].TrialComponentName < list[j].TrialComponentName })

	return paginateSlice(list, nextToken, 0)
}

// trialComponentNameFilterLocked returns the set of trial component names
// allowed by the given TrialName/ExperimentName filters, or nil if neither
// filter is set (meaning: no filtering). Callers must hold b.mu.
func (b *InMemoryBackend) trialComponentNameFilterLocked(
	region, experimentName, trialName string,
) map[string]bool {
	if trialName == "" && experimentName == "" {
		return nil
	}

	trialNames := map[string]bool{}

	if trialName != "" {
		trialNames[trialName] = true
	} else {
		for _, t := range b.trialsStoreRO(region).All() {
			if t.ExperimentName == experimentName {
				trialNames[t.TrialName] = true
			}
		}
	}

	allowed := map[string]bool{}

	for _, assoc := range b.trialComponentAssociationsStoreRO(region).All() {
		if trialNames[assoc.TrialName] {
			allowed[assoc.TrialComponentName] = true
		}
	}

	return allowed
}
