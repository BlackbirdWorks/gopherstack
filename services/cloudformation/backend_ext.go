package cloudformation

import (
	"context"
	"encoding/json"
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
// It compares deployed resource state against the current template and returns
// DRIFTED/MODIFIED/DELETED when divergence is found (#12).
func (b *InMemoryBackend) DetectStackDrift(nameOrID string) (string, error) {
	b.mu.Lock("DetectStackDrift")
	defer b.mu.Unlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return "", ErrStackNotFound
	}

	resourceStatuses := b.compareStackResources(stack)

	driftedCount := 0
	overallStatus := driftStatusInSync
	for _, status := range resourceStatuses {
		if status != driftStatusInSync {
			driftedCount++
			overallStatus = driftStatusDrifted
		}
	}

	b.resourceDriftStatus[stack.StackID] = resourceStatuses

	detectionID := uuid.New().String()
	b.driftDetections[detectionID] = &DriftDetectionStatus{
		StackID:                   stack.StackID,
		StackDriftDetectionID:     detectionID,
		StackDriftStatus:          overallStatus,
		DetectionStatus:           detectionComplete,
		DriftedStackResourceCount: driftedCount,
		Timestamp:                 time.Now(),
	}

	return detectionID, nil
}

// DetectStackResourceDrift initiates drift detection for a specific resource in a stack.
// It compares the resource's deployed properties against the template (#12).
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

	resourceStatuses := b.compareStackResources(stack)
	status, ok2 := resourceStatuses[logicalID]
	if !ok2 {
		status = driftStatusInSync
	}

	if b.resourceDriftStatus[stack.StackID] == nil {
		b.resourceDriftStatus[stack.StackID] = make(map[string]string)
	}
	b.resourceDriftStatus[stack.StackID][logicalID] = status

	overallStatus := driftStatusInSync
	driftedCount := 0
	if status != driftStatusInSync {
		overallStatus = driftStatusDrifted
		driftedCount = 1
	}

	detectionID := uuid.New().String()
	b.driftDetections[detectionID] = &DriftDetectionStatus{
		StackID:                   stack.StackID,
		StackDriftDetectionID:     detectionID,
		StackDriftStatus:          overallStatus,
		DetectionStatus:           detectionComplete,
		DriftedStackResourceCount: driftedCount,
		Timestamp:                 time.Now(),
	}

	return detectionID, nil
}

// compareStackResources compares deployed resources against the current template.
// Returns a map of logicalID → drift status (IN_SYNC, MODIFIED, DELETED).
// Must be called with b.mu held.
func (b *InMemoryBackend) compareStackResources(stack *Stack) map[string]string {
	statuses := make(map[string]string)

	deployedResources := b.resources[stack.StackID]
	if len(deployedResources) == 0 {
		return statuses
	}

	tmpl, err := ParseTemplate(stack.TemplateBody)
	if err != nil {
		// If template can't be parsed, mark all as in-sync (best effort)
		for logicalID := range deployedResources {
			statuses[logicalID] = driftStatusInSync
		}

		return statuses
	}

	// Resources in deployed state but absent from template → DELETED
	for logicalID := range deployedResources {
		if _, inTemplate := tmpl.Resources[logicalID]; !inTemplate {
			statuses[logicalID] = driftStatusDeleted
		}
	}

	// Resources in template: compare stored properties
	for logicalID, tmplRes := range tmpl.Resources {
		deployedRes, deployed := deployedResources[logicalID]
		if !deployed {
			// In template but not deployed (shouldn't normally happen post-create)
			continue
		}

		if propertiesDiffer(deployedRes.Properties, tmplRes.Properties) {
			statuses[logicalID] = driftStatusModified
		} else {
			statuses[logicalID] = driftStatusInSync
		}
	}

	return statuses
}

// propertiesDiffer reports whether two property maps differ using JSON serialization.
func propertiesDiffer(a, b map[string]any) bool {
	aBytes, err1 := json.Marshal(a)
	bBytes, err2 := json.Marshal(b)
	if err1 != nil || err2 != nil {
		return false
	}

	return string(aBytes) != string(bBytes)
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

// DescribeStackResourceDrifts is implemented in backend_parity.go with drift simulation support.

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
			ParameterKey:          key,
			ParameterType:         pd.Type,
			DefaultValue:          defaultVal,
			Description:           pd.Description,
			AllowedValues:         pd.AllowedValues,
			ConstraintDescription: pd.ConstraintDescription,
			AllowedPattern:        pd.AllowedPattern,
			NoEcho:                pd.NoEcho,
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
