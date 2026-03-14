package cloudformation

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

const (
	driftStatusInSync  = "IN_SYNC"
	detectionComplete  = "DETECTION_COMPLETE"
	cfnEstimateCostURL = "https://calculator.s3.amazonaws.com/calc5.html?key=mock-estimate"
)

// DetectStackDrift initiates drift detection for all resources in a stack.
// The mock immediately marks detection as DETECTION_COMPLETE with IN_SYNC status.
func (b *InMemoryBackend) DetectStackDrift(nameOrID string) (string, error) {
	b.mu.Lock("DetectStackDrift")
	defer b.mu.Unlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return "", ErrStackNotFound
	}

	detectionID := uuid.New().String()
	b.driftDetections[detectionID] = &DriftDetectionStatus{
		StackID:                   stack.StackID,
		StackDriftDetectionID:     detectionID,
		StackDriftStatus:          driftStatusInSync,
		DetectionStatus:           detectionComplete,
		DriftedStackResourceCount: 0,
		Timestamp:                 time.Now(),
	}

	return detectionID, nil
}

// DetectStackResourceDrift initiates drift detection for a specific resource in a stack.
// The mock immediately marks detection as DETECTION_COMPLETE with IN_SYNC status.
func (b *InMemoryBackend) DetectStackResourceDrift(nameOrID, logicalID string) (string, error) {
	b.mu.Lock("DetectStackResourceDrift")
	defer b.mu.Unlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return "", ErrStackNotFound
	}

	if _, exists := b.resources[stack.StackID][logicalID]; !exists {
		return "", ErrResourceNotFound
	}

	detectionID := uuid.New().String()
	b.driftDetections[detectionID] = &DriftDetectionStatus{
		StackID:                   stack.StackID,
		StackDriftDetectionID:     detectionID,
		StackDriftStatus:          driftStatusInSync,
		DetectionStatus:           detectionComplete,
		DriftedStackResourceCount: 0,
		Timestamp:                 time.Now(),
	}

	return detectionID, nil
}

// DescribeStackDriftDetectionStatus returns the status of a drift detection operation.
func (b *InMemoryBackend) DescribeStackDriftDetectionStatus(detectionID string) (*DriftDetectionStatus, error) {
	b.mu.RLock("DescribeStackDriftDetectionStatus")
	defer b.mu.RUnlock()

	status, ok := b.driftDetections[detectionID]
	if !ok {
		return nil, ErrDriftDetectionNotFound
	}

	return status, nil
}

// DescribeStackResourceDrifts returns drift information for all resources in a stack.
// In the mock all resources are reported as IN_SYNC.
func (b *InMemoryBackend) DescribeStackResourceDrifts(nameOrID string) ([]StackResourceDrift, error) {
	b.mu.RLock("DescribeStackResourceDrifts")
	defer b.mu.RUnlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return nil, ErrStackNotFound
	}

	resMap := b.resources[stack.StackID]
	drifts := make([]StackResourceDrift, 0, len(resMap))

	for _, res := range resMap {
		drifts = append(drifts, StackResourceDrift{
			StackID:                  stack.StackID,
			LogicalResourceID:        res.LogicalID,
			PhysicalResourceID:       res.PhysicalID,
			ResourceType:             res.Type,
			StackResourceDriftStatus: driftStatusInSync,
			Timestamp:                res.Timestamp,
		})
	}

	sort.Slice(drifts, func(i, j int) bool {
		return drifts[i].LogicalResourceID < drifts[j].LogicalResourceID
	})

	return drifts, nil
}

// SetStackPolicy sets the stack policy for the given stack.
func (b *InMemoryBackend) SetStackPolicy(nameOrID, policy string) error {
	b.mu.Lock("SetStackPolicy")
	defer b.mu.Unlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return ErrStackNotFound
	}

	b.stackPolicies[stack.StackID] = policy

	return nil
}

// GetStackPolicy returns the stack policy for the given stack.
// Returns an empty string if no policy has been set.
func (b *InMemoryBackend) GetStackPolicy(nameOrID string) (string, error) {
	b.mu.RLock("GetStackPolicy")
	defer b.mu.RUnlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return "", ErrStackNotFound
	}

	return b.stackPolicies[stack.StackID], nil
}

// GetTemplateSummary returns summary information about a template body or an existing stack's template.
func (b *InMemoryBackend) GetTemplateSummary(templateBody, stackName string) (*TemplateSummary, error) {
	b.mu.RLock("GetTemplateSummary")
	defer b.mu.RUnlock()

	if templateBody == "" && stackName != "" {
		stack, ok := b.resolveStack(stackName)
		if !ok {
			return nil, ErrStackNotFound
		}

		templateBody = stack.TemplateBody
	}

	if templateBody == "" {
		return &TemplateSummary{}, nil
	}

	tmpl, err := ParseTemplate(templateBody)
	if err != nil {
		return nil, err
	}

	params := make([]ParameterDeclaration, 0, len(tmpl.Parameters))
	for key, pd := range tmpl.Parameters {
		defaultVal := ""
		if pd.Default != nil {
			defaultVal = fmt.Sprintf("%v", pd.Default)
		}

		params = append(params, ParameterDeclaration{
			ParameterKey:  key,
			ParameterType: pd.Type,
			DefaultValue:  defaultVal,
			Description:   pd.Description,
		})
	}

	sort.Slice(params, func(i, j int) bool { return params[i].ParameterKey < params[j].ParameterKey })

	typesSet := make(map[string]struct{}, len(tmpl.Resources))
	for _, res := range tmpl.Resources {
		typesSet[res.Type] = struct{}{}
	}

	resourceTypes := make([]string, 0, len(typesSet))
	for t := range typesSet {
		resourceTypes = append(resourceTypes, t)
	}

	sort.Strings(resourceTypes)

	return &TemplateSummary{
		Description:   tmpl.Description,
		Parameters:    params,
		ResourceTypes: resourceTypes,
	}, nil
}

// EstimateTemplateCost returns a mock cost estimation URL.
func (b *InMemoryBackend) EstimateTemplateCost(_ string, _ []Parameter) (string, error) {
	return cfnEstimateCostURL, nil
}

// ContinueUpdateRollback continues the rollback for a stack that is in ROLLBACK_IN_PROGRESS
// or UPDATE_ROLLBACK_IN_PROGRESS state.
func (b *InMemoryBackend) ContinueUpdateRollback(_ context.Context, nameOrID string) error {
	b.mu.Lock("ContinueUpdateRollback")
	defer b.mu.Unlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return ErrStackNotFound
	}

	switch stack.StackStatus {
	case statusRollbackInProgress:
		stack.StackStatus = statusRollbackComplete
		b.addEvent(stack.StackID, stack.StackName, stack.StackName, stack.StackID,
			cfnStackType, statusRollbackComplete, "")
	case statusUpdateRollbackInProgress:
		stack.StackStatus = statusUpdateRollbackComplete
		b.addEvent(stack.StackID, stack.StackName, stack.StackName, stack.StackID,
			cfnStackType, statusUpdateRollbackComplete, "")
	}

	return nil
}

// CancelUpdateStack cancels an in-progress stack update.
// If the stack is in UPDATE_IN_PROGRESS state, it transitions to UPDATE_ROLLBACK_COMPLETE.
func (b *InMemoryBackend) CancelUpdateStack(_ context.Context, nameOrID string) error {
	b.mu.Lock("CancelUpdateStack")
	defer b.mu.Unlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return ErrStackNotFound
	}

	if stack.StackStatus == statusUpdateInProgress {
		stack.StackStatus = statusUpdateRollbackInProgress
		b.addEvent(stack.StackID, stack.StackName, stack.StackName, stack.StackID,
			cfnStackType, statusUpdateRollbackInProgress, reasonUserInitiated)
		stack.StackStatus = statusUpdateRollbackComplete
		b.addEvent(stack.StackID, stack.StackName, stack.StackName, stack.StackID,
			cfnStackType, statusUpdateRollbackComplete, "")
	}

	return nil
}

const cfnDefaultAccountLimitCount = 200

// DescribeAccountLimits returns the CloudFormation account limits for this mock.
func (b *InMemoryBackend) DescribeAccountLimits() []AccountLimit {
	return []AccountLimit{
		{Name: "stackCount", Value: cfnDefaultAccountLimitCount},
		{Name: "stackOutputsCount", Value: cfnDefaultAccountLimitCount},
	}
}
