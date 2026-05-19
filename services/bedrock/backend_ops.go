package bedrock

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const statusRunning = "Running"

// --- ModelInvocationJob ---

// ModelInvocationJob represents a batch model invocation job.
type ModelInvocationJob struct {
	CreationTime     time.Time `json:"creationTime"`
	LastModifiedTime time.Time `json:"lastModifiedTime"`
	JobArn           string    `json:"jobArn"`
	JobName          string    `json:"jobName"`
	Status           string    `json:"status"`
	Tags             []Tag     `json:"tags,omitempty"`
}

// PromptRouter represents a prompt router resource.
type PromptRouter struct {
	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
	PromptRouterArn string    `json:"promptRouterArn"`
	PromptRouterName string   `json:"promptRouterName"`
	Status          string    `json:"status"`
	Tags            []Tag     `json:"tags,omitempty"`
}

// EnforcedGuardrailConfig represents an enforced guardrail configuration entry.
type EnforcedGuardrailConfig struct {
	GuardrailID      string `json:"guardrailId"`
	GuardrailVersion string `json:"guardrailVersion"`
}

// --- ID generators ---

func (b *InMemoryBackend) newModelInvocationJobID() string {
	b.modelInvocationJobCounter++
	return "mij-" + strconv.Itoa(b.modelInvocationJobCounter)
}

func (b *InMemoryBackend) newPromptRouterID() string {
	b.promptRouterCounter++
	return "pr-" + strconv.Itoa(b.promptRouterCounter)
}

// --- EvaluationJob read/stop ---

// GetEvaluationJob returns a single evaluation job by ARN.
func (b *InMemoryBackend) GetEvaluationJob(jobARN string) (*EvaluationJob, error) {
	b.mu.RLock("GetEvaluationJob")
	defer b.mu.RUnlock()

	job, ok := b.evaluationJobs[jobARN]
	if !ok {
		return nil, fmt.Errorf("%w: evaluation job %s not found", ErrNotFound, jobARN)
	}

	cp := *job
	cp.Tags = copyTags(job.Tags)

	return &cp, nil
}

// ListEvaluationJobs returns all evaluation jobs.
func (b *InMemoryBackend) ListEvaluationJobs() []*EvaluationJob {
	b.mu.RLock("ListEvaluationJobs")
	defer b.mu.RUnlock()

	jobs := make([]*EvaluationJob, 0, len(b.evaluationJobs))
	for _, j := range b.evaluationJobs {
		cp := *j
		cp.Tags = copyTags(j.Tags)
		jobs = append(jobs, &cp)
	}

	sort.Slice(jobs, func(i, k int) bool {
		return jobs[i].CreationTime.Before(jobs[k].CreationTime)
	})

	return jobs
}

// StopEvaluationJob marks an evaluation job as stopped.
func (b *InMemoryBackend) StopEvaluationJob(jobARN string) error {
	b.mu.Lock("StopEvaluationJob")
	defer b.mu.Unlock()

	job, ok := b.evaluationJobs[jobARN]
	if !ok {
		return fmt.Errorf("%w: evaluation job %s not found", ErrNotFound, jobARN)
	}

	job.Status = statusStopped
	job.LastModifiedTime = time.Now().UTC()

	return nil
}

// --- AutomatedReasoningPolicy read/update/delete ---

// GetAutomatedReasoningPolicy returns a single ARP by ARN.
func (b *InMemoryBackend) GetAutomatedReasoningPolicy(policyARN string) (*AutomatedReasoningPolicy, error) {
	b.mu.RLock("GetAutomatedReasoningPolicy")
	defer b.mu.RUnlock()

	policy, ok := b.automatedReasoningPolicies[policyARN]
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

	policies := make([]*AutomatedReasoningPolicy, 0, len(b.automatedReasoningPolicies))
	for _, p := range b.automatedReasoningPolicies {
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
func (b *InMemoryBackend) UpdateAutomatedReasoningPolicy(policyARN, description string) (*AutomatedReasoningPolicy, error) {
	b.mu.Lock("UpdateAutomatedReasoningPolicy")
	defer b.mu.Unlock()

	policy, ok := b.automatedReasoningPolicies[policyARN]
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

	policy, ok := b.automatedReasoningPolicies[policyARN]
	if !ok {
		return fmt.Errorf("%w: automated reasoning policy %s not found", ErrNotFound, policyARN)
	}

	delete(b.arpByName, policy.Name)
	delete(b.automatedReasoningPolicies, policyARN)

	// Remove associated workflows and test cases.
	for id, wf := range b.arpBuildWorkflows {
		if wf.PolicyArn == policyARN {
			delete(b.arpBuildWorkflows, id)
		}
	}

	for id, tc := range b.arpTestCases {
		if tc.PolicyArn == policyARN {
			delete(b.arpTestCases, id)
		}
	}

	return nil
}

// --- ARP Build Workflow ---

// StartAutomatedReasoningPolicyBuildWorkflow creates a new build workflow for a policy.
func (b *InMemoryBackend) StartAutomatedReasoningPolicyBuildWorkflow(policyARN string) (*AutomatedReasoningPolicyBuildWorkflow, error) {
	b.mu.Lock("StartAutomatedReasoningPolicyBuildWorkflow")
	defer b.mu.Unlock()

	if _, ok := b.automatedReasoningPolicies[policyARN]; !ok {
		return nil, fmt.Errorf("%w: automated reasoning policy %s not found", ErrNotFound, policyARN)
	}

	b.arpWorkflowCounter++
	id := "bw-" + strconv.Itoa(b.arpWorkflowCounter)

	wf := &AutomatedReasoningPolicyBuildWorkflow{
		BuildWorkflowID: id,
		PolicyArn:       policyARN,
		Status:          statusRunning,
	}
	b.arpBuildWorkflows[id] = wf
	cp := *wf

	return &cp, nil
}

// GetAutomatedReasoningPolicyBuildWorkflow returns a workflow by policy ARN and workflow ID.
func (b *InMemoryBackend) GetAutomatedReasoningPolicyBuildWorkflow(policyARN, workflowID string) (*AutomatedReasoningPolicyBuildWorkflow, error) {
	b.mu.RLock("GetAutomatedReasoningPolicyBuildWorkflow")
	defer b.mu.RUnlock()

	wf, ok := b.arpBuildWorkflows[workflowID]
	if !ok || wf.PolicyArn != policyARN {
		return nil, fmt.Errorf("%w: build workflow %s not found", ErrNotFound, workflowID)
	}

	cp := *wf

	return &cp, nil
}

// ListAutomatedReasoningPolicyBuildWorkflows returns all workflows for a policy.
func (b *InMemoryBackend) ListAutomatedReasoningPolicyBuildWorkflows(policyARN string) []*AutomatedReasoningPolicyBuildWorkflow {
	b.mu.RLock("ListAutomatedReasoningPolicyBuildWorkflows")
	defer b.mu.RUnlock()

	var workflows []*AutomatedReasoningPolicyBuildWorkflow
	for _, wf := range b.arpBuildWorkflows {
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

	wf, ok := b.arpBuildWorkflows[workflowID]
	if !ok || wf.PolicyArn != policyARN {
		return fmt.Errorf("%w: build workflow %s not found", ErrNotFound, workflowID)
	}

	delete(b.arpBuildWorkflows, workflowID)

	return nil
}

// --- ARP Test Cases ---

// GetAutomatedReasoningPolicyTestCase returns a test case by ID.
func (b *InMemoryBackend) GetAutomatedReasoningPolicyTestCase(policyARN, testCaseID string) (*AutomatedReasoningPolicyTestCase, error) {
	b.mu.RLock("GetAutomatedReasoningPolicyTestCase")
	defer b.mu.RUnlock()

	tc, ok := b.arpTestCases[testCaseID]
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
	for _, tc := range b.arpTestCases {
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

// UpdateAutomatedReasoningPolicyTestCase updates a test case (idempotent touch).
func (b *InMemoryBackend) UpdateAutomatedReasoningPolicyTestCase(policyARN, testCaseID string) (*AutomatedReasoningPolicyTestCase, error) {
	b.mu.Lock("UpdateAutomatedReasoningPolicyTestCase")
	defer b.mu.Unlock()

	tc, ok := b.arpTestCases[testCaseID]
	if !ok || tc.PolicyArn != policyARN {
		return nil, fmt.Errorf("%w: test case %s not found", ErrNotFound, testCaseID)
	}

	cp := *tc

	return &cp, nil
}

// DeleteAutomatedReasoningPolicyTestCase removes a test case.
func (b *InMemoryBackend) DeleteAutomatedReasoningPolicyTestCase(policyARN, testCaseID string) error {
	b.mu.Lock("DeleteAutomatedReasoningPolicyTestCase")
	defer b.mu.Unlock()

	tc, ok := b.arpTestCases[testCaseID]
	if !ok || tc.PolicyArn != policyARN {
		return fmt.Errorf("%w: test case %s not found", ErrNotFound, testCaseID)
	}

	delete(b.arpTestCases, testCaseID)

	return nil
}

// --- ARP ancillary ops (annotations, next scenario, export, results) ---

// GetAutomatedReasoningPolicyAnnotations returns a placeholder annotations response.
func (b *InMemoryBackend) GetAutomatedReasoningPolicyAnnotations(policyARN string) (map[string]any, error) {
	b.mu.RLock("GetAutomatedReasoningPolicyAnnotations")
	defer b.mu.RUnlock()

	if _, ok := b.automatedReasoningPolicies[policyARN]; !ok {
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

	if _, ok := b.automatedReasoningPolicies[policyARN]; !ok {
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

	if _, ok := b.automatedReasoningPolicies[policyARN]; !ok {
		return nil, fmt.Errorf("%w: automated reasoning policy %s not found", ErrNotFound, policyARN)
	}

	return map[string]any{"scenario": nil, keyPolicyArn: policyARN}, nil
}

// GetAutomatedReasoningPolicyBuildWorkflowResultAssets returns result asset URLs for a workflow.
func (b *InMemoryBackend) GetAutomatedReasoningPolicyBuildWorkflowResultAssets(policyARN, workflowID string) (map[string]any, error) {
	b.mu.RLock("GetAutomatedReasoningPolicyBuildWorkflowResultAssets")
	defer b.mu.RUnlock()

	wf, ok := b.arpBuildWorkflows[workflowID]
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
	v, ok := b.arpVersions[key]

	if !ok {
		return nil, fmt.Errorf("%w: version %s of policy %s not found", ErrNotFound, version, policyARN)
	}

	return map[string]any{
		keyPolicyArn:   v.PolicyArn,
		"version":      v.Version,
		"definitionHash": v.DefinitionHash,
		keyCreatedAt:   v.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}, nil
}

// StartAutomatedReasoningPolicyTestWorkflow starts a test workflow for a test case.
func (b *InMemoryBackend) StartAutomatedReasoningPolicyTestWorkflow(policyARN, testCaseID string) (map[string]any, error) {
	b.mu.Lock("StartAutomatedReasoningPolicyTestWorkflow")
	defer b.mu.Unlock()

	tc, ok := b.arpTestCases[testCaseID]
	if !ok || tc.PolicyArn != policyARN {
		return nil, fmt.Errorf("%w: test case %s not found", ErrNotFound, testCaseID)
	}

	return map[string]any{"testCaseId": testCaseID, keyPolicyArn: policyARN, keyStatus: statusRunning}, nil
}

// GetAutomatedReasoningPolicyTestResult returns the result for a test case execution.
func (b *InMemoryBackend) GetAutomatedReasoningPolicyTestResult(policyARN, testCaseID string) (map[string]any, error) {
	b.mu.RLock("GetAutomatedReasoningPolicyTestResult")
	defer b.mu.RUnlock()

	tc, ok := b.arpTestCases[testCaseID]
	if !ok || tc.PolicyArn != policyARN {
		return nil, fmt.Errorf("%w: test case %s not found", ErrNotFound, testCaseID)
	}

	return map[string]any{"testCaseId": testCaseID, keyPolicyArn: policyARN, keyStatus: statusCompleted}, nil
}

// ListAutomatedReasoningPolicyTestResults returns test results for a policy.
func (b *InMemoryBackend) ListAutomatedReasoningPolicyTestResults(policyARN string) []map[string]any {
	b.mu.RLock("ListAutomatedReasoningPolicyTestResults")
	defer b.mu.RUnlock()

	var results []map[string]any
	for _, tc := range b.arpTestCases {
		if tc.PolicyArn == policyARN {
			results = append(results, map[string]any{
				"testCaseId": tc.TestCaseID,
				keyPolicyArn: policyARN,
				keyStatus:    statusCompleted,
			})
		}
	}

	sort.Slice(results, func(i, k int) bool {
		a, _ := results[i]["testCaseId"].(string)
		b, _ := results[k]["testCaseId"].(string)
		return a < b
	})

	return results
}

// --- ModelInvocationJob ---

// CreateModelInvocationJob creates a new batch model invocation job.
func (b *InMemoryBackend) CreateModelInvocationJob(name string, tags []Tag) (*ModelInvocationJob, error) {
	b.mu.Lock("CreateModelInvocationJob")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: jobName is required", ErrValidation)
	}

	for _, j := range b.modelInvocationJobs {
		if j.JobName == name {
			return nil, fmt.Errorf("%w: model invocation job %s already exists", ErrAlreadyExists, name)
		}
	}

	id := b.newModelInvocationJobID()
	jobARN := arn.Build("bedrock", b.region, b.accountID, "model-invocation-job/"+id)
	now := time.Now().UTC()

	job := &ModelInvocationJob{
		JobArn:           jobARN,
		JobName:          name,
		Status:           statusInProgress,
		CreationTime:     now,
		LastModifiedTime: now,
		Tags:             copyTags(tags),
	}
	b.modelInvocationJobs[jobARN] = job
	cp := *job
	cp.Tags = copyTags(job.Tags)

	return &cp, nil
}

// GetModelInvocationJob returns a single invocation job by ARN.
func (b *InMemoryBackend) GetModelInvocationJob(jobARN string) (*ModelInvocationJob, error) {
	b.mu.RLock("GetModelInvocationJob")
	defer b.mu.RUnlock()

	job, ok := b.modelInvocationJobs[jobARN]
	if !ok {
		return nil, fmt.Errorf("%w: model invocation job %s not found", ErrNotFound, jobARN)
	}

	cp := *job
	cp.Tags = copyTags(job.Tags)

	return &cp, nil
}

// ListModelInvocationJobs returns all invocation jobs.
func (b *InMemoryBackend) ListModelInvocationJobs() []*ModelInvocationJob {
	b.mu.RLock("ListModelInvocationJobs")
	defer b.mu.RUnlock()

	jobs := make([]*ModelInvocationJob, 0, len(b.modelInvocationJobs))
	for _, j := range b.modelInvocationJobs {
		cp := *j
		cp.Tags = copyTags(j.Tags)
		jobs = append(jobs, &cp)
	}

	sort.Slice(jobs, func(i, k int) bool {
		return jobs[i].CreationTime.Before(jobs[k].CreationTime)
	})

	return jobs
}

// StopModelInvocationJob marks an invocation job as stopped.
func (b *InMemoryBackend) StopModelInvocationJob(jobARN string) error {
	b.mu.Lock("StopModelInvocationJob")
	defer b.mu.Unlock()

	job, ok := b.modelInvocationJobs[jobARN]
	if !ok {
		return fmt.Errorf("%w: model invocation job %s not found", ErrNotFound, jobARN)
	}

	job.Status = statusStopped
	job.LastModifiedTime = time.Now().UTC()

	return nil
}

// --- ImportedModel (backed by ModelImportJob) ---

// GetImportedModel returns the import job whose importedModelArn matches.
func (b *InMemoryBackend) GetImportedModel(modelARN string) (*ModelImportJob, error) {
	b.mu.RLock("GetImportedModel")
	defer b.mu.RUnlock()

	for _, j := range b.modelImportJobs {
		if j.ImportedModelArn == modelARN {
			cp := *j
			cp.Tags = copyTags(j.Tags)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: imported model %s not found", ErrNotFound, modelARN)
}

// ListImportedModels returns the import jobs that have completed and have an imported model ARN.
func (b *InMemoryBackend) ListImportedModels() []*ModelImportJob {
	b.mu.RLock("ListImportedModels")
	defer b.mu.RUnlock()

	var models []*ModelImportJob
	for _, j := range b.modelImportJobs {
		if j.ImportedModelArn != "" {
			cp := *j
			cp.Tags = copyTags(j.Tags)
			models = append(models, &cp)
		}
	}

	sort.Slice(models, func(i, k int) bool {
		return models[i].CreationTime.Before(models[k].CreationTime)
	})

	return models
}

// DeleteImportedModel removes the import job whose importedModelArn matches.
func (b *InMemoryBackend) DeleteImportedModel(modelARN string) error {
	b.mu.Lock("DeleteImportedModel")
	defer b.mu.Unlock()

	for jobARN, j := range b.modelImportJobs {
		if j.ImportedModelArn == modelARN {
			delete(b.modelImportJobs, jobARN)

			return nil
		}
	}

	return fmt.Errorf("%w: imported model %s not found", ErrNotFound, modelARN)
}

// --- PromptRouter ---

// CreatePromptRouter creates a new prompt router.
func (b *InMemoryBackend) CreatePromptRouter(name string, tags []Tag) (*PromptRouter, error) {
	b.mu.Lock("CreatePromptRouter")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: promptRouterName is required", ErrValidation)
	}

	if _, exists := b.promptRoutersByName[name]; exists {
		return nil, fmt.Errorf("%w: prompt router %s already exists", ErrAlreadyExists, name)
	}

	id := b.newPromptRouterID()
	routerARN := arn.Build("bedrock", b.region, b.accountID, "prompt-router/"+id)
	now := time.Now().UTC()

	router := &PromptRouter{
		PromptRouterArn:  routerARN,
		PromptRouterName: name,
		Status:           "AVAILABLE",
		CreatedAt:        now,
		UpdatedAt:        now,
		Tags:             copyTags(tags),
	}
	b.promptRouters[routerARN] = router
	b.promptRoutersByName[name] = routerARN
	cp := *router
	cp.Tags = copyTags(router.Tags)

	return &cp, nil
}

// GetPromptRouter returns a single prompt router by ARN.
func (b *InMemoryBackend) GetPromptRouter(routerARN string) (*PromptRouter, error) {
	b.mu.RLock("GetPromptRouter")
	defer b.mu.RUnlock()

	router, ok := b.promptRouters[routerARN]
	if !ok {
		return nil, fmt.Errorf("%w: prompt router %s not found", ErrNotFound, routerARN)
	}

	cp := *router
	cp.Tags = copyTags(router.Tags)

	return &cp, nil
}

// ListPromptRouters returns all prompt routers.
func (b *InMemoryBackend) ListPromptRouters() []*PromptRouter {
	b.mu.RLock("ListPromptRouters")
	defer b.mu.RUnlock()

	routers := make([]*PromptRouter, 0, len(b.promptRouters))
	for _, r := range b.promptRouters {
		cp := *r
		cp.Tags = copyTags(r.Tags)
		routers = append(routers, &cp)
	}

	sort.Slice(routers, func(i, k int) bool {
		return routers[i].PromptRouterName < routers[k].PromptRouterName
	})

	return routers
}

// DeletePromptRouter removes a prompt router.
func (b *InMemoryBackend) DeletePromptRouter(routerARN string) error {
	b.mu.Lock("DeletePromptRouter")
	defer b.mu.Unlock()

	router, ok := b.promptRouters[routerARN]
	if !ok {
		return fmt.Errorf("%w: prompt router %s not found", ErrNotFound, routerARN)
	}

	delete(b.promptRoutersByName, router.PromptRouterName)
	delete(b.promptRouters, routerARN)

	return nil
}

// --- CustomModelDeployment read/update/delete ---

// GetCustomModelDeployment returns a deployment by ARN.
func (b *InMemoryBackend) GetCustomModelDeployment(deployARN string) (*CustomModelDeployment, error) {
	b.mu.RLock("GetCustomModelDeployment")
	defer b.mu.RUnlock()

	d, ok := b.customModelDeployments[deployARN]
	if !ok {
		return nil, fmt.Errorf("%w: custom model deployment %s not found", ErrNotFound, deployARN)
	}

	cp := *d
	cp.Tags = copyTags(d.Tags)

	return &cp, nil
}

// ListCustomModelDeployments returns all deployments.
func (b *InMemoryBackend) ListCustomModelDeployments() []*CustomModelDeployment {
	b.mu.RLock("ListCustomModelDeployments")
	defer b.mu.RUnlock()

	deployments := make([]*CustomModelDeployment, 0, len(b.customModelDeployments))
	for _, d := range b.customModelDeployments {
		cp := *d
		cp.Tags = copyTags(d.Tags)
		deployments = append(deployments, &cp)
	}

	sort.Slice(deployments, func(i, k int) bool {
		return deployments[i].CreationTime.Before(deployments[k].CreationTime)
	})

	return deployments
}

// UpdateCustomModelDeployment updates mutable fields of a deployment.
func (b *InMemoryBackend) UpdateCustomModelDeployment(deployARN string) (*CustomModelDeployment, error) {
	b.mu.Lock("UpdateCustomModelDeployment")
	defer b.mu.Unlock()

	d, ok := b.customModelDeployments[deployARN]
	if !ok {
		return nil, fmt.Errorf("%w: custom model deployment %s not found", ErrNotFound, deployARN)
	}

	d.LastModifiedTime = time.Now().UTC()

	cp := *d
	cp.Tags = copyTags(d.Tags)

	return &cp, nil
}

// DeleteCustomModelDeployment removes a deployment.
func (b *InMemoryBackend) DeleteCustomModelDeployment(deployARN string) error {
	b.mu.Lock("DeleteCustomModelDeployment")
	defer b.mu.Unlock()

	d, ok := b.customModelDeployments[deployARN]
	if !ok {
		return fmt.Errorf("%w: custom model deployment %s not found", ErrNotFound, deployARN)
	}

	delete(b.customModelDeployByName, d.ModelDeploymentName)
	delete(b.customModelDeployments, deployARN)

	return nil
}

// --- FoundationModelAgreement read/delete ---

// ListFoundationModelAgreementOffers returns all foundation model agreements.
func (b *InMemoryBackend) ListFoundationModelAgreementOffers() []*FoundationModelAgreement {
	b.mu.RLock("ListFoundationModelAgreementOffers")
	defer b.mu.RUnlock()

	agreements := make([]*FoundationModelAgreement, 0, len(b.foundationModelAgreements))
	for _, a := range b.foundationModelAgreements {
		cp := *a
		agreements = append(agreements, &cp)
	}

	sort.Slice(agreements, func(i, k int) bool {
		return agreements[i].ModelID < agreements[k].ModelID
	})

	return agreements
}

// DeleteFoundationModelAgreement removes an agreement by model ID.
func (b *InMemoryBackend) DeleteFoundationModelAgreement(modelID string) error {
	b.mu.Lock("DeleteFoundationModelAgreement")
	defer b.mu.Unlock()

	if _, ok := b.foundationModelAgreements[modelID]; !ok {
		return fmt.Errorf("%w: foundation model agreement for %s not found", ErrNotFound, modelID)
	}

	delete(b.foundationModelAgreements, modelID)

	return nil
}

// --- UseCaseForModelAccess ---

// GetUseCaseForModelAccess returns the current use case configuration.
func (b *InMemoryBackend) GetUseCaseForModelAccess() map[string]any {
	b.mu.RLock("GetUseCaseForModelAccess")
	defer b.mu.RUnlock()

	return map[string]any{
		"useCaseType":        b.useCaseType,
		"useCaseDescription": b.useCaseDescription,
	}
}

// PutUseCaseForModelAccess stores the use case configuration.
func (b *InMemoryBackend) PutUseCaseForModelAccess(useCaseType, description string) {
	b.mu.Lock("PutUseCaseForModelAccess")
	defer b.mu.Unlock()

	b.useCaseType = useCaseType
	b.useCaseDescription = description
}

// --- EnforcedGuardrailConfiguration ---

// ListEnforcedGuardrailsConfiguration returns all enforced guardrail configs.
func (b *InMemoryBackend) ListEnforcedGuardrailsConfiguration() []*EnforcedGuardrailConfig {
	b.mu.RLock("ListEnforcedGuardrailsConfiguration")
	defer b.mu.RUnlock()

	configs := make([]*EnforcedGuardrailConfig, 0, len(b.enforcedGuardrailConfigs))
	for _, c := range b.enforcedGuardrailConfigs {
		cp := *c
		configs = append(configs, &cp)
	}

	sort.Slice(configs, func(i, k int) bool {
		return configs[i].GuardrailID < configs[k].GuardrailID
	})

	return configs
}

// PutEnforcedGuardrailConfiguration stores or updates an enforced guardrail config.
func (b *InMemoryBackend) PutEnforcedGuardrailConfiguration(guardrailID, version string) {
	b.mu.Lock("PutEnforcedGuardrailConfiguration")
	defer b.mu.Unlock()

	if b.enforcedGuardrailConfigs == nil {
		b.enforcedGuardrailConfigs = make(map[string]*EnforcedGuardrailConfig)
	}

	b.enforcedGuardrailConfigs[guardrailID] = &EnforcedGuardrailConfig{
		GuardrailID:      guardrailID,
		GuardrailVersion: version,
	}
}

// DeleteEnforcedGuardrailConfiguration removes an enforced guardrail config.
func (b *InMemoryBackend) DeleteEnforcedGuardrailConfiguration(guardrailID string) error {
	b.mu.Lock("DeleteEnforcedGuardrailConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.enforcedGuardrailConfigs[guardrailID]; !ok {
		return fmt.Errorf("%w: enforced guardrail configuration for %s not found", ErrNotFound, guardrailID)
	}

	delete(b.enforcedGuardrailConfigs, guardrailID)

	return nil
}
