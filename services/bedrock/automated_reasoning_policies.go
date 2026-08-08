package bedrock

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// newARPID generates a unique automated reasoning policy ID.
func (b *InMemoryBackend) newARPID() string {
	b.arpCounter++

	return fmt.Sprintf("arp-%07d", b.arpCounter)
}

// newARPTestCaseID generates a unique test case ID.
func (b *InMemoryBackend) newARPTestCaseID() string {
	b.arpTestCaseCounter++

	return fmt.Sprintf("tc-%07d", b.arpTestCaseCounter)
}

// newARPVersionNum generates a monotonically increasing version number for a policy.
// Must be called with the write lock held.
func (b *InMemoryBackend) newARPVersionNum(policyARN string) string {
	b.arpVersionCountByPolicy[policyARN]++

	return strconv.Itoa(b.arpVersionCountByPolicy[policyARN])
}

// CreateAutomatedReasoningPolicy creates a new Automated Reasoning policy.
func (b *InMemoryBackend) CreateAutomatedReasoningPolicy(
	name, description string,
	tags []Tag,
) (*AutomatedReasoningPolicy, error) {
	b.mu.Lock("CreateAutomatedReasoningPolicy")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if _, exists := b.arpByName[name]; exists {
		return nil, fmt.Errorf(
			"%w: automated reasoning policy %s already exists",
			ErrAlreadyExists,
			name,
		)
	}

	id := b.newARPID()
	policyARN := arn.Build("bedrock", b.region, b.accountID, "automated-reasoning-policy/"+id)
	now := time.Now().UTC()

	policy := &AutomatedReasoningPolicy{
		PolicyArn:   policyARN,
		Name:        name,
		Description: description,
		Status:      "ACTIVE",
		CreatedAt:   now,
		UpdatedAt:   now,
		Tags:        copyTags(tags),
	}
	b.automatedReasoningPolicies.Put(policy)
	b.arpByName[name] = policyARN
	cp := *policy
	cp.Tags = copyTags(policy.Tags)

	return &cp, nil
}

// CancelAutomatedReasoningPolicyBuildWorkflow cancels a running build workflow.
func (b *InMemoryBackend) CancelAutomatedReasoningPolicyBuildWorkflow(
	policyARN, workflowID string,
) error {
	b.mu.Lock("CancelAutomatedReasoningPolicyBuildWorkflow")
	defer b.mu.Unlock()

	if _, ok := b.automatedReasoningPolicies.Get(policyARN); !ok {
		return fmt.Errorf("%w: automated reasoning policy %s not found", ErrNotFound, policyARN)
	}

	wf, ok := b.arpBuildWorkflows.Get(workflowID)
	if !ok {
		return fmt.Errorf("%w: build workflow %s not found", ErrNotFound, workflowID)
	}

	if wf.PolicyArn != policyARN {
		return fmt.Errorf(
			"%w: build workflow %s does not belong to policy %s",
			ErrNotFound,
			workflowID,
			policyARN,
		)
	}

	wf.Status = "Cancelled"

	return nil
}

// CreateAutomatedReasoningPolicyTestCase creates a test case for an Automated Reasoning policy.
func (b *InMemoryBackend) CreateAutomatedReasoningPolicyTestCase(
	policyARN string,
) (*AutomatedReasoningPolicyTestCase, error) {
	b.mu.Lock("CreateAutomatedReasoningPolicyTestCase")
	defer b.mu.Unlock()

	if _, ok := b.automatedReasoningPolicies.Get(policyARN); !ok {
		return nil, fmt.Errorf(
			"%w: automated reasoning policy %s not found",
			ErrNotFound,
			policyARN,
		)
	}

	id := b.newARPTestCaseID()
	now := time.Now().UTC()
	tc := &AutomatedReasoningPolicyTestCase{
		TestCaseID: id,
		PolicyArn:  policyARN,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	b.arpTestCases.Put(tc)
	cp := *tc

	return &cp, nil
}

// CreateAutomatedReasoningPolicyVersion creates a new version of an Automated Reasoning policy.
func (b *InMemoryBackend) CreateAutomatedReasoningPolicyVersion(
	policyARN, definitionHash string,
) (*AutomatedReasoningPolicyVersion, error) {
	b.mu.Lock("CreateAutomatedReasoningPolicyVersion")
	defer b.mu.Unlock()

	policy, ok := b.automatedReasoningPolicies.Get(policyARN)
	if !ok {
		return nil, fmt.Errorf(
			"%w: automated reasoning policy %s not found",
			ErrNotFound,
			policyARN,
		)
	}

	// Use per-policy version counter for realistic monotonic versioning.
	versionNum := b.newARPVersionNum(policyARN)
	versionedARN := policyARN + "/version/" + versionNum

	version := &AutomatedReasoningPolicyVersion{
		PolicyArn:      versionedARN,
		Name:           policy.Name,
		DefinitionHash: definitionHash,
		Version:        versionNum,
		CreatedAt:      time.Now().UTC(),
	}
	b.arpVersions.Put(version)
	cp := *version

	return &cp, nil
}

const statusRunning = "Running"

// GetAutomatedReasoningPolicy returns a single ARP by ARN.
func (b *InMemoryBackend) GetAutomatedReasoningPolicy(policyARN string) (*AutomatedReasoningPolicy, error) {
	b.mu.RLock("GetAutomatedReasoningPolicy")
	defer b.mu.RUnlock()

	policy, ok := b.automatedReasoningPolicies.Get(policyARN)
	if !ok {
		return nil, fmt.Errorf("%w: automated reasoning policy %s not found", ErrNotFound, policyARN)
	}

	cp := *policy
	cp.Tags = copyTags(policy.Tags)

	return &cp, nil
}

// ListAutomatedReasoningPolicies returns all policies.
func (b *InMemoryBackend) ListAutomatedReasoningPolicies() []*AutomatedReasoningPolicy {
	b.mu.RLock("ListAutomatedReasoningPolicies")
	defer b.mu.RUnlock()

	policies := make([]*AutomatedReasoningPolicy, 0, b.automatedReasoningPolicies.Len())
	for _, p := range b.automatedReasoningPolicies.All() {
		cp := *p
		cp.Tags = copyTags(p.Tags)
		policies = append(policies, &cp)
	}

	sort.Slice(policies, func(i, k int) bool {
		return policies[i].Name < policies[k].Name
	})

	return policies
}

// UpdateAutomatedReasoningPolicy updates description (and other mutable fields) of a policy.
func (b *InMemoryBackend) UpdateAutomatedReasoningPolicy(
	policyARN, description string,
) (*AutomatedReasoningPolicy, error) {
	b.mu.Lock("UpdateAutomatedReasoningPolicy")
	defer b.mu.Unlock()

	policy, ok := b.automatedReasoningPolicies.Get(policyARN)
	if !ok {
		return nil, fmt.Errorf("%w: automated reasoning policy %s not found", ErrNotFound, policyARN)
	}

	policy.Description = description
	policy.UpdatedAt = time.Now().UTC()

	cp := *policy
	cp.Tags = copyTags(policy.Tags)

	return &cp, nil
}

// DeleteAutomatedReasoningPolicy removes a policy and its related resources.
func (b *InMemoryBackend) DeleteAutomatedReasoningPolicy(policyARN string) error {
	b.mu.Lock("DeleteAutomatedReasoningPolicy")
	defer b.mu.Unlock()

	policy, ok := b.automatedReasoningPolicies.Get(policyARN)
	if !ok {
		return fmt.Errorf("%w: automated reasoning policy %s not found", ErrNotFound, policyARN)
	}

	delete(b.arpByName, policy.Name)
	b.automatedReasoningPolicies.Delete(policyARN)

	// Remove associated workflows and test cases.
	for _, wf := range b.arpBuildWorkflows.All() {
		if wf.PolicyArn == policyARN {
			b.arpBuildWorkflows.Delete(wf.BuildWorkflowID)
		}
	}

	for _, tc := range b.arpTestCases.All() {
		if tc.PolicyArn == policyARN {
			b.arpTestCases.Delete(tc.TestCaseID)
		}
	}

	return nil
}

// StartAutomatedReasoningPolicyBuildWorkflow creates a new build workflow for a policy.
func (b *InMemoryBackend) StartAutomatedReasoningPolicyBuildWorkflow(
	policyARN string,
) (*AutomatedReasoningPolicyBuildWorkflow, error) {
	b.mu.Lock("StartAutomatedReasoningPolicyBuildWorkflow")
	defer b.mu.Unlock()

	if _, ok := b.automatedReasoningPolicies.Get(policyARN); !ok {
		return nil, fmt.Errorf("%w: automated reasoning policy %s not found", ErrNotFound, policyARN)
	}

	b.arpWorkflowCounter++
	id := "bw-" + strconv.Itoa(b.arpWorkflowCounter)

	wf := &AutomatedReasoningPolicyBuildWorkflow{
		BuildWorkflowID: id,
		PolicyArn:       policyARN,
		Status:          statusRunning,
	}
	b.arpBuildWorkflows.Put(wf)
	cp := *wf

	return &cp, nil
}

// GetAutomatedReasoningPolicyBuildWorkflow returns a workflow by policy ARN and workflow ID.
func (b *InMemoryBackend) GetAutomatedReasoningPolicyBuildWorkflow(
	policyARN, workflowID string,
) (*AutomatedReasoningPolicyBuildWorkflow, error) {
	b.mu.RLock("GetAutomatedReasoningPolicyBuildWorkflow")
	defer b.mu.RUnlock()

	wf, ok := b.arpBuildWorkflows.Get(workflowID)
	if !ok || wf.PolicyArn != policyARN {
		return nil, fmt.Errorf("%w: build workflow %s not found", ErrNotFound, workflowID)
	}

	cp := *wf

	return &cp, nil
}

// ListAutomatedReasoningPolicyBuildWorkflows returns all workflows for a policy.
func (b *InMemoryBackend) ListAutomatedReasoningPolicyBuildWorkflows(
	policyARN string,
) []*AutomatedReasoningPolicyBuildWorkflow {
	b.mu.RLock("ListAutomatedReasoningPolicyBuildWorkflows")
	defer b.mu.RUnlock()

	var workflows []*AutomatedReasoningPolicyBuildWorkflow
	for _, wf := range b.arpBuildWorkflows.All() {
		if wf.PolicyArn == policyARN {
			cp := *wf
			workflows = append(workflows, &cp)
		}
	}

	sort.Slice(workflows, func(i, k int) bool {
		return workflows[i].BuildWorkflowID < workflows[k].BuildWorkflowID
	})

	return workflows
}

// DeleteAutomatedReasoningPolicyBuildWorkflow removes a build workflow.
func (b *InMemoryBackend) DeleteAutomatedReasoningPolicyBuildWorkflow(policyARN, workflowID string) error {
	b.mu.Lock("DeleteAutomatedReasoningPolicyBuildWorkflow")
	defer b.mu.Unlock()

	wf, ok := b.arpBuildWorkflows.Get(workflowID)
	if !ok || wf.PolicyArn != policyARN {
		return fmt.Errorf("%w: build workflow %s not found", ErrNotFound, workflowID)
	}

	b.arpBuildWorkflows.Delete(workflowID)

	return nil
}

// GetAutomatedReasoningPolicyTestCase returns a test case by ID.
func (b *InMemoryBackend) GetAutomatedReasoningPolicyTestCase(
	policyARN, testCaseID string,
) (*AutomatedReasoningPolicyTestCase, error) {
	b.mu.RLock("GetAutomatedReasoningPolicyTestCase")
	defer b.mu.RUnlock()

	tc, ok := b.arpTestCases.Get(testCaseID)
	if !ok || tc.PolicyArn != policyARN {
		return nil, fmt.Errorf("%w: test case %s not found", ErrNotFound, testCaseID)
	}

	cp := *tc

	return &cp, nil
}

// ListAutomatedReasoningPolicyTestCases returns all test cases for a policy.
func (b *InMemoryBackend) ListAutomatedReasoningPolicyTestCases(policyARN string) []*AutomatedReasoningPolicyTestCase {
	b.mu.RLock("ListAutomatedReasoningPolicyTestCases")
	defer b.mu.RUnlock()

	var cases []*AutomatedReasoningPolicyTestCase
	for _, tc := range b.arpTestCases.All() {
		if tc.PolicyArn == policyARN {
			cp := *tc
			cases = append(cases, &cp)
		}
	}

	sort.Slice(cases, func(i, k int) bool {
		return cases[i].TestCaseID < cases[k].TestCaseID
	})

	return cases
}

// UpdateAutomatedReasoningPolicyTestCase updates a test case's content, query,
// expected result, and confidence threshold
// (aws-sdk-go-v2 api_op_UpdateAutomatedReasoningPolicyTestCase.go:34-71).
func (b *InMemoryBackend) UpdateAutomatedReasoningPolicyTestCase(
	policyARN, testCaseID, guardContent, queryContent, expectedResult string,
	confidenceThreshold *float64,
) (*AutomatedReasoningPolicyTestCase, error) {
	b.mu.Lock("UpdateAutomatedReasoningPolicyTestCase")
	defer b.mu.Unlock()

	tc, ok := b.arpTestCases.Get(testCaseID)
	if !ok || tc.PolicyArn != policyARN {
		return nil, fmt.Errorf("%w: test case %s not found", ErrNotFound, testCaseID)
	}

	if guardContent == "" {
		return nil, fmt.Errorf("%w: guardContent is required", ErrValidation)
	}

	if expectedResult == "" {
		return nil, fmt.Errorf("%w: expectedAggregatedFindingsResult is required", ErrValidation)
	}

	tc.GuardContent = guardContent
	tc.QueryContent = queryContent
	tc.ExpectedAggregatedFindingsResult = expectedResult
	tc.ConfidenceThreshold = confidenceThreshold
	tc.UpdatedAt = time.Now().UTC()

	cp := *tc

	return &cp, nil
}

// DeleteAutomatedReasoningPolicyTestCase removes a test case.
func (b *InMemoryBackend) DeleteAutomatedReasoningPolicyTestCase(policyARN, testCaseID string) error {
	b.mu.Lock("DeleteAutomatedReasoningPolicyTestCase")
	defer b.mu.Unlock()

	tc, ok := b.arpTestCases.Get(testCaseID)
	if !ok || tc.PolicyArn != policyARN {
		return fmt.Errorf("%w: test case %s not found", ErrNotFound, testCaseID)
	}

	b.arpTestCases.Delete(testCaseID)

	return nil
}

// GetAutomatedReasoningPolicyAnnotations returns a placeholder annotations response.
func (b *InMemoryBackend) GetAutomatedReasoningPolicyAnnotations(policyARN string) (map[string]any, error) {
	b.mu.RLock("GetAutomatedReasoningPolicyAnnotations")
	defer b.mu.RUnlock()

	if _, ok := b.automatedReasoningPolicies.Get(policyARN); !ok {
		return nil, fmt.Errorf("%w: automated reasoning policy %s not found", ErrNotFound, policyARN)
	}

	anns := b.arpAnnotations[policyARN]
	if anns == nil {
		return map[string]any{"annotations": []any{}}, nil
	}

	return map[string]any{"annotations": anns}, nil
}

// UpdateAutomatedReasoningPolicyAnnotations stores annotations for a policy.
func (b *InMemoryBackend) UpdateAutomatedReasoningPolicyAnnotations(policyARN string) error {
	b.mu.Lock("UpdateAutomatedReasoningPolicyAnnotations")
	defer b.mu.Unlock()

	if _, ok := b.automatedReasoningPolicies.Get(policyARN); !ok {
		return fmt.Errorf("%w: automated reasoning policy %s not found", ErrNotFound, policyARN)
	}

	if b.arpAnnotations == nil {
		b.arpAnnotations = make(map[string][]any)
	}

	if b.arpAnnotations[policyARN] == nil {
		b.arpAnnotations[policyARN] = []any{}
	}

	return nil
}

// GetAutomatedReasoningPolicyNextScenario returns the next scenario for active-learning.
func (b *InMemoryBackend) GetAutomatedReasoningPolicyNextScenario(policyARN string) (map[string]any, error) {
	b.mu.RLock("GetAutomatedReasoningPolicyNextScenario")
	defer b.mu.RUnlock()

	if _, ok := b.automatedReasoningPolicies.Get(policyARN); !ok {
		return nil, fmt.Errorf("%w: automated reasoning policy %s not found", ErrNotFound, policyARN)
	}

	return map[string]any{"scenario": nil, keyPolicyArn: policyARN}, nil
}

// GetAutomatedReasoningPolicyBuildWorkflowResultAssets returns result asset URLs for a workflow.
func (b *InMemoryBackend) GetAutomatedReasoningPolicyBuildWorkflowResultAssets(
	policyARN, workflowID string,
) (map[string]any, error) {
	b.mu.RLock("GetAutomatedReasoningPolicyBuildWorkflowResultAssets")
	defer b.mu.RUnlock()

	wf, ok := b.arpBuildWorkflows.Get(workflowID)
	if !ok || wf.PolicyArn != policyARN {
		return nil, fmt.Errorf("%w: build workflow %s not found", ErrNotFound, workflowID)
	}

	return map[string]any{keyBuildWorkflowID: workflowID, "resultAssets": []any{}}, nil
}

// ExportAutomatedReasoningPolicyVersion exports a policy version definition.
func (b *InMemoryBackend) ExportAutomatedReasoningPolicyVersion(policyARN, version string) (map[string]any, error) {
	b.mu.RLock("ExportAutomatedReasoningPolicyVersion")
	defer b.mu.RUnlock()

	key := policyARN + ":" + version
	v, ok := b.arpVersions.Get(key)

	if !ok {
		return nil, fmt.Errorf("%w: version %s of policy %s not found", ErrNotFound, version, policyARN)
	}

	return map[string]any{
		keyPolicyArn:     v.PolicyArn,
		"version":        v.Version,
		"definitionHash": v.DefinitionHash,
		keyCreatedAt:     v.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}, nil
}

// StartAutomatedReasoningPolicyTestWorkflow starts a test workflow for a test case.
func (b *InMemoryBackend) StartAutomatedReasoningPolicyTestWorkflow(
	policyARN, testCaseID string,
) (map[string]any, error) {
	b.mu.Lock("StartAutomatedReasoningPolicyTestWorkflow")
	defer b.mu.Unlock()

	tc, ok := b.arpTestCases.Get(testCaseID)
	if !ok || tc.PolicyArn != policyARN {
		return nil, fmt.Errorf("%w: test case %s not found", ErrNotFound, testCaseID)
	}

	return map[string]any{keyTestCaseID: testCaseID, keyPolicyArn: policyARN, keyStatus: statusRunning}, nil
}

// GetAutomatedReasoningPolicyTestResult returns the result for a test case execution.
func (b *InMemoryBackend) GetAutomatedReasoningPolicyTestResult(policyARN, testCaseID string) (map[string]any, error) {
	b.mu.RLock("GetAutomatedReasoningPolicyTestResult")
	defer b.mu.RUnlock()

	tc, ok := b.arpTestCases.Get(testCaseID)
	if !ok || tc.PolicyArn != policyARN {
		return nil, fmt.Errorf("%w: test case %s not found", ErrNotFound, testCaseID)
	}

	return map[string]any{keyTestCaseID: testCaseID, keyPolicyArn: policyARN, keyStatus: statusCompleted}, nil
}

// ListAutomatedReasoningPolicyTestResults returns test results for a policy.
func (b *InMemoryBackend) ListAutomatedReasoningPolicyTestResults(policyARN string) []map[string]any {
	b.mu.RLock("ListAutomatedReasoningPolicyTestResults")
	defer b.mu.RUnlock()

	var results []map[string]any
	for _, tc := range b.arpTestCases.All() {
		if tc.PolicyArn == policyARN {
			results = append(results, map[string]any{
				keyTestCaseID: tc.TestCaseID,
				keyPolicyArn:  policyARN,
				keyStatus:     statusCompleted,
			})
		}
	}

	sort.Slice(results, func(i, k int) bool {
		a, _ := results[i][keyTestCaseID].(string)
		b, _ := results[k][keyTestCaseID].(string)

		return a < b
	})

	return results
}
