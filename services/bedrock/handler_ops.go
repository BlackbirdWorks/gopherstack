package bedrock

import (
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// --- ARP path helpers ---

// isARPBuildWorkflowsPath matches /automated-reasoning-policies/{arn}/build-workflows (no trailing segment).
func isARPBuildWorkflowsPath(path string) bool {
	rest, ok := strings.CutPrefix(path, automatedReasoningPrefix+"/")
	if !ok {
		return false
	}

	return strings.HasSuffix(rest, "/build-workflows")
}

// isARPBuildWorkflowSubPath matches /automated-reasoning-policies/{arn}/build-workflows/{id}
// but NOT sub-paths like /cancel or /result-assets.
func isARPBuildWorkflowSubPath(path string) bool {
	rest, ok := strings.CutPrefix(path, automatedReasoningPrefix+"/")
	if !ok {
		return false
	}

	_, after, ok := strings.Cut(rest, "/build-workflows/")
	if !ok {
		return false
	}

	segment := after

	return !strings.Contains(segment, "/")
}

// isARPBuildWorkflowResultAssetsPath matches /automated-reasoning-policies/{arn}/build-workflows/{id}/result-assets.
func isARPBuildWorkflowResultAssetsPath(path string) bool {
	rest, ok := strings.CutPrefix(path, automatedReasoningPrefix+"/")
	if !ok {
		return false
	}

	return strings.Contains(rest, "/build-workflows/") && strings.HasSuffix(rest, "/result-assets")
}

// isARPTestCasesResultsPath matches /automated-reasoning-policies/{arn}/test-cases/results.
func isARPTestCasesResultsPath(path string) bool {
	rest, ok := strings.CutPrefix(path, automatedReasoningPrefix+"/")
	if !ok {
		return false
	}

	return strings.HasSuffix(rest, "/test-cases/results")
}

// isARPTestCaseSubPath matches /automated-reasoning-policies/{arn}/test-cases/{id}
// but NOT /test-cases, /test-cases/results, /run, or /result sub-paths.
func isARPTestCaseSubPath(path string) bool {
	rest, ok := strings.CutPrefix(path, automatedReasoningPrefix+"/")
	if !ok {
		return false
	}

	_, after, ok := strings.Cut(rest, "/test-cases/")
	if !ok {
		return false
	}

	segment := after
	// Exclude known sub-paths: results (the collection), and further nesting.
	return segment != "results" && !strings.Contains(segment, "/")
}

// isARPTestCaseRunPath matches /automated-reasoning-policies/{arn}/test-cases/{id}/run.
func isARPTestCaseRunPath(path string) bool {
	rest, ok := strings.CutPrefix(path, automatedReasoningPrefix+"/")
	if !ok {
		return false
	}

	return strings.Contains(rest, "/test-cases/") && strings.HasSuffix(rest, "/run")
}

// isARPTestCaseResultPath matches /automated-reasoning-policies/{arn}/test-cases/{id}/result.
func isARPTestCaseResultPath(path string) bool {
	rest, ok := strings.CutPrefix(path, automatedReasoningPrefix+"/")
	if !ok {
		return false
	}

	return strings.Contains(rest, "/test-cases/") && strings.HasSuffix(rest, "/result")
}

// isARPVersionExportPath matches /automated-reasoning-policies/{arn}/versions/{v}/export.
func isARPVersionExportPath(path string) bool {
	rest, ok := strings.CutPrefix(path, automatedReasoningPrefix+"/")
	if !ok {
		return false
	}

	return strings.Contains(rest, "/versions/") && strings.HasSuffix(rest, "/export")
}

// isARPAnnotationsPath matches /automated-reasoning-policies/{arn}/annotations.
func isARPAnnotationsPath(path string) bool {
	rest, ok := strings.CutPrefix(path, automatedReasoningPrefix+"/")
	if !ok {
		return false
	}

	return strings.HasSuffix(rest, "/annotations")
}

// isARPNextScenarioPath matches /automated-reasoning-policies/{arn}/next-scenario.
func isARPNextScenarioPath(path string) bool {
	rest, ok := strings.CutPrefix(path, automatedReasoningPrefix+"/")
	if !ok {
		return false
	}

	return strings.HasSuffix(rest, "/next-scenario")
}

// extractARPPolicyARN extracts the policyARN from a path that has a known suffix.
func extractARPPolicyARN(path, suffix string) string {
	rest, _ := strings.CutPrefix(path, automatedReasoningPrefix+"/")

	return decodePath(strings.TrimSuffix(rest, suffix))
}

// extractARPWorkflowIDs extracts policyARN and workflowID from a build-workflow path.
func extractARPWorkflowIDs(path string) (string, string) {
	rest, _ := strings.CutPrefix(path, automatedReasoningPrefix+"/")

	before, after, ok := strings.Cut(rest, "/build-workflows/")
	if !ok {
		return "", ""
	}

	policyARN := decodePath(before)
	// Strip any trailing sub-path like /result-assets, /cancel.
	workflowID, _, _ := strings.Cut(after, "/")

	return policyARN, workflowID
}

// extractARPTestCaseIDs extracts policyARN and testCaseID from a test-case path.
func extractARPTestCaseIDs(path string) (string, string) {
	rest, _ := strings.CutPrefix(path, automatedReasoningPrefix+"/")

	before, after, ok := strings.Cut(rest, "/test-cases/")
	if !ok {
		return "", ""
	}

	policyARN := decodePath(before)
	testCaseID, _, _ := strings.Cut(after, "/")

	return policyARN, testCaseID
}

// extractARPVersionIDs extracts policyARN and version from a versions path.
func extractARPVersionIDs(path string) (string, string) {
	rest, _ := strings.CutPrefix(path, automatedReasoningPrefix+"/")

	before, after, ok := strings.Cut(rest, "/versions/")
	if !ok {
		return "", ""
	}

	policyARN := decodePath(before)
	version, _, _ := strings.Cut(after, "/")

	return policyARN, version
}

// --- EvaluationJob handlers ---

func (h *Handler) handleGetEvaluationJob(c *echo.Context, jobARN string) error {
	job, err := h.Backend.GetEvaluationJob(jobARN)
	if err != nil {
		return h.writeError(c, err)
	}

	resp := map[string]any{
		keyJobArn:           job.JobArn,
		keyJobName:          job.JobName,
		keyStatus:           job.Status,
		keyCreationTime:     job.CreationTime.Format(time.RFC3339),
		keyLastModifiedTime: job.LastModifiedTime.Format(time.RFC3339),
	}
	if job.JobDescription != "" {
		resp["jobDescription"] = job.JobDescription
	}
	if job.RoleArn != "" {
		resp["roleArn"] = job.RoleArn
	}
	if job.EvaluatorConfig != nil {
		resp["evaluatorConfig"] = job.EvaluatorConfig
	}
	if job.InferenceConfig != nil {
		resp["inferenceConfig"] = job.InferenceConfig
	}
	if len(job.EvaluationConfig) > 0 {
		resp["evaluationConfig"] = job.EvaluationConfig
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListEvaluationJobs(c *echo.Context) error {
	jobs := h.Backend.ListEvaluationJobs()
	summaries := make([]map[string]any, 0, len(jobs))

	for _, j := range jobs {
		summaries = append(summaries, map[string]any{
			keyJobArn:           j.JobArn,
			keyJobName:          j.JobName,
			keyStatus:           j.Status,
			keyCreationTime:     j.CreationTime.Format(time.RFC3339),
			keyLastModifiedTime: j.LastModifiedTime.Format(time.RFC3339),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"jobSummaries": summaries})
}

func (h *Handler) handleStopEvaluationJob(c *echo.Context, jobARN string) error {
	if err := h.Backend.StopEvaluationJob(jobARN); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- ARP CRUD handlers ---

func (h *Handler) handleGetAutomatedReasoningPolicy(c *echo.Context, policyARN string) error {
	policy, err := h.Backend.GetAutomatedReasoningPolicy(policyARN)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyPolicyArn:  policy.PolicyArn,
		keyName:       policy.Name,
		"description": policy.Description,
		keyStatus:     policy.Status,
		keyCreatedAt:  isoTime{policy.CreatedAt},
		keyUpdatedAt:  isoTime{policy.UpdatedAt},
	})
}

func (h *Handler) handleListAutomatedReasoningPolicies(c *echo.Context) error {
	policies := h.Backend.ListAutomatedReasoningPolicies()
	summaries := make([]map[string]any, 0, len(policies))

	for _, p := range policies {
		summaries = append(summaries, map[string]any{
			keyPolicyArn: p.PolicyArn,
			keyName:      p.Name,
			keyStatus:    p.Status,
			keyCreatedAt: isoTime{p.CreatedAt},
			keyUpdatedAt: isoTime{p.UpdatedAt},
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"automatedReasoningPolicies": summaries})
}

type updateARPInput struct {
	Description string `json:"description,omitempty"`
}

func (h *Handler) handleUpdateAutomatedReasoningPolicy(c *echo.Context, policyARN string, body []byte) error {
	in, err := parseBody[updateARPInput](body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	policy, opErr := h.Backend.UpdateAutomatedReasoningPolicy(policyARN, in.Description)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyPolicyArn: policy.PolicyArn,
		keyName:      policy.Name,
		keyStatus:    policy.Status,
		keyUpdatedAt: isoTime{policy.UpdatedAt},
	})
}

func (h *Handler) handleDeleteAutomatedReasoningPolicy(c *echo.Context, policyARN string) error {
	if err := h.Backend.DeleteAutomatedReasoningPolicy(policyARN); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- ARP Build Workflow handlers ---

func (h *Handler) handleStartARPBuildWorkflow(c *echo.Context, path string) error {
	policyARN := extractARPPolicyARN(path, "/build-workflows")

	wf, err := h.Backend.StartAutomatedReasoningPolicyBuildWorkflow(policyARN)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{
		keyBuildWorkflowID: wf.BuildWorkflowID,
		keyPolicyArn:       wf.PolicyArn,
		keyStatus:          wf.Status,
	})
}

func (h *Handler) handleGetARPBuildWorkflow(c *echo.Context, path string) error {
	policyARN, workflowID := extractARPWorkflowIDs(path)

	wf, err := h.Backend.GetAutomatedReasoningPolicyBuildWorkflow(policyARN, workflowID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyBuildWorkflowID: wf.BuildWorkflowID,
		keyPolicyArn:       wf.PolicyArn,
		keyStatus:          wf.Status,
	})
}

func (h *Handler) handleListARPBuildWorkflows(c *echo.Context, path string) error {
	policyARN := extractARPPolicyARN(path, "/build-workflows")
	workflows := h.Backend.ListAutomatedReasoningPolicyBuildWorkflows(policyARN)
	summaries := make([]map[string]any, 0, len(workflows))

	for _, wf := range workflows {
		summaries = append(summaries, map[string]any{
			keyBuildWorkflowID: wf.BuildWorkflowID,
			keyPolicyArn:       wf.PolicyArn,
			keyStatus:          wf.Status,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"buildWorkflows": summaries})
}

func (h *Handler) handleDeleteARPBuildWorkflow(c *echo.Context, path string) error {
	policyARN, workflowID := extractARPWorkflowIDs(path)

	if err := h.Backend.DeleteAutomatedReasoningPolicyBuildWorkflow(policyARN, workflowID); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetARPBuildWorkflowResultAssets(c *echo.Context, path string) error {
	policyARN, workflowID := extractARPWorkflowIDs(path)

	result, err := h.Backend.GetAutomatedReasoningPolicyBuildWorkflowResultAssets(policyARN, workflowID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, result)
}

// --- ARP Test Case handlers ---

func (h *Handler) handleGetARPTestCase(c *echo.Context, path string) error {
	policyARN, testCaseID := extractARPTestCaseIDs(path)

	tc, err := h.Backend.GetAutomatedReasoningPolicyTestCase(policyARN, testCaseID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyTestCaseID: tc.TestCaseID,
		keyPolicyArn:  tc.PolicyArn,
	})
}

func (h *Handler) handleListARPTestCases(c *echo.Context, path string) error {
	policyARN := extractARPPolicyARN(path, "/test-cases")
	cases := h.Backend.ListAutomatedReasoningPolicyTestCases(policyARN)
	summaries := make([]map[string]any, 0, len(cases))

	for _, tc := range cases {
		summaries = append(summaries, map[string]any{
			keyTestCaseID: tc.TestCaseID,
			keyPolicyArn:  tc.PolicyArn,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"testCases": summaries})
}

func (h *Handler) handleUpdateARPTestCase(c *echo.Context, path string) error {
	policyARN, testCaseID := extractARPTestCaseIDs(path)

	tc, err := h.Backend.UpdateAutomatedReasoningPolicyTestCase(policyARN, testCaseID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyTestCaseID: tc.TestCaseID,
		keyPolicyArn:  tc.PolicyArn,
	})
}

func (h *Handler) handleDeleteARPTestCase(c *echo.Context, path string) error {
	policyARN, testCaseID := extractARPTestCaseIDs(path)

	if err := h.Backend.DeleteAutomatedReasoningPolicyTestCase(policyARN, testCaseID); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleStartARPTestWorkflow(c *echo.Context, path string) error {
	policyARN, testCaseID := extractARPTestCaseIDs(path)

	result, err := h.Backend.StartAutomatedReasoningPolicyTestWorkflow(policyARN, testCaseID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusCreated, result)
}

func (h *Handler) handleGetARPTestResult(c *echo.Context, path string) error {
	policyARN, testCaseID := extractARPTestCaseIDs(path)

	result, err := h.Backend.GetAutomatedReasoningPolicyTestResult(policyARN, testCaseID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleListARPTestResults(c *echo.Context, path string) error {
	policyARN := extractARPPolicyARN(path, "/test-cases/results")
	results := h.Backend.ListAutomatedReasoningPolicyTestResults(policyARN)

	if results == nil {
		results = []map[string]any{}
	}

	return c.JSON(http.StatusOK, map[string]any{"testResults": results})
}

// --- ARP Version / Annotation / Scenario handlers ---

func (h *Handler) handleExportARPVersion(c *echo.Context, path string) error {
	policyARN, version := extractARPVersionIDs(path)

	result, err := h.Backend.ExportAutomatedReasoningPolicyVersion(policyARN, version)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleGetARPAnnotations(c *echo.Context, path string) error {
	policyARN := extractARPPolicyARN(path, "/annotations")

	result, err := h.Backend.GetAutomatedReasoningPolicyAnnotations(policyARN)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleUpdateARPAnnotations(c *echo.Context, path string) error {
	policyARN := extractARPPolicyARN(path, "/annotations")

	if err := h.Backend.UpdateAutomatedReasoningPolicyAnnotations(policyARN); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetARPNextScenario(c *echo.Context, path string) error {
	policyARN := extractARPPolicyARN(path, "/next-scenario")

	result, err := h.Backend.GetAutomatedReasoningPolicyNextScenario(policyARN)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, result)
}

// --- ModelInvocationJob handlers ---

type createModelInvocationJobInput struct {
	JobName string `json:"jobName"`
	Tags    []Tag  `json:"tags,omitempty"`
}

func (h *Handler) handleCreateModelInvocationJob(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	in, parseErr := parseBody[createModelInvocationJobInput](body)
	if parseErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	job, opErr := h.Backend.CreateModelInvocationJob(in.JobName, in.Tags)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyJobArn: job.JobArn})
}

func (h *Handler) handleGetModelInvocationJob(c *echo.Context, jobARN string) error {
	job, err := h.Backend.GetModelInvocationJob(jobARN)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyJobArn:           job.JobArn,
		keyJobName:          job.JobName,
		keyStatus:           job.Status,
		keyCreationTime:     job.CreationTime.Format(time.RFC3339),
		keyLastModifiedTime: job.LastModifiedTime.Format(time.RFC3339),
	})
}

// parseListModelInvocationJobsQuery builds the backend filter/sort/pagination input from
// the real ListModelInvocationJobs query-string bindings (nameContains, statusEquals,
// sortBy, sortOrder, nextToken, submitTimeAfter, submitTimeBefore). The previous
// implementation discarded all of these — a disguised no-op, since the backend already
// implements the filter/sort logic in full.
func parseListModelInvocationJobsQuery(c *echo.Context) *ListModelInvocationJobsInput {
	q := c.Request().URL.Query()

	in := &ListModelInvocationJobsInput{
		StatusEquals: q.Get("statusEquals"),
		NameContains: q.Get("nameContains"),
		SortBy:       q.Get("sortBy"),
		SortOrder:    q.Get("sortOrder"),
		NextToken:    q.Get("nextToken"),
	}

	if v := q.Get("submitTimeAfter"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			in.SubmitTimeAfter = &t
		}
	}

	if v := q.Get("submitTimeBefore"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			in.SubmitTimeBefore = &t
		}
	}

	return in
}

func (h *Handler) handleListModelInvocationJobs(c *echo.Context) error {
	jobs, outToken := h.Backend.ListModelInvocationJobs(parseListModelInvocationJobsQuery(c))
	summaries := make([]map[string]any, 0, len(jobs))

	for _, j := range jobs {
		summaries = append(summaries, map[string]any{
			keyJobArn:           j.JobArn,
			keyJobName:          j.JobName,
			keyStatus:           j.Status,
			keyCreationTime:     j.CreationTime.Format(time.RFC3339),
			keyLastModifiedTime: j.LastModifiedTime.Format(time.RFC3339),
		})
	}

	resp := map[string]any{"invocationJobSummaries": summaries}
	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleStopModelInvocationJob(c *echo.Context, jobARN string) error {
	if err := h.Backend.StopModelInvocationJob(jobARN); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- ImportedModel handlers ---

func (h *Handler) handleGetImportedModel(c *echo.Context, modelARN string) error {
	job, err := h.Backend.GetImportedModel(modelARN)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyModelArn:  job.ImportedModelArn,
		keyJobName:   job.JobName,
		keyStatus:    job.Status,
		keyCreatedAt: job.CreationTime.Format(time.RFC3339),
	})
}

func (h *Handler) handleListImportedModels(c *echo.Context) error {
	models := h.Backend.ListImportedModels()
	summaries := make([]map[string]any, 0, len(models))

	for _, m := range models {
		summaries = append(summaries, map[string]any{
			keyModelArn:  m.ImportedModelArn,
			keyJobName:   m.JobName,
			keyStatus:    m.Status,
			keyCreatedAt: m.CreationTime.Format(time.RFC3339),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"modelSummaries": summaries})
}

func (h *Handler) handleDeleteImportedModel(c *echo.Context, modelARN string) error {
	if err := h.Backend.DeleteImportedModel(modelARN); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- PromptRouter handlers ---

type createPromptRouterInput struct {
	PromptRouterName string `json:"promptRouterName"`
	Tags             []Tag  `json:"tags,omitempty"`
}

func (h *Handler) handleCreatePromptRouter(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	in, parseErr := parseBody[createPromptRouterInput](body)
	if parseErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	router, opErr := h.Backend.CreatePromptRouter(in.PromptRouterName, in.Tags)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyPromptRouterArn: router.PromptRouterArn})
}

func (h *Handler) handleGetPromptRouter(c *echo.Context, routerARN string) error {
	router, err := h.Backend.GetPromptRouter(routerARN)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyPromptRouterArn: router.PromptRouterArn,
		"promptRouterName": router.PromptRouterName,
		keyStatus:          router.Status,
		keyCreatedAt:       router.CreatedAt.Format(time.RFC3339),
		keyUpdatedAt:       router.UpdatedAt.Format(time.RFC3339),
	})
}

func (h *Handler) handleListPromptRouters(c *echo.Context) error {
	routers := h.Backend.ListPromptRouters()
	summaries := make([]map[string]any, 0, len(routers))

	for _, r := range routers {
		summaries = append(summaries, map[string]any{
			keyPromptRouterArn: r.PromptRouterArn,
			"promptRouterName": r.PromptRouterName,
			keyStatus:          r.Status,
			keyCreatedAt:       r.CreatedAt.Format(time.RFC3339),
			keyUpdatedAt:       r.UpdatedAt.Format(time.RFC3339),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"promptRouters": summaries})
}

func (h *Handler) handleDeletePromptRouter(c *echo.Context, routerARN string) error {
	if err := h.Backend.DeletePromptRouter(routerARN); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- CustomModelDeployment read/update/delete handlers ---

func (h *Handler) handleGetCustomModelDeployment(c *echo.Context, deployARN string) error {
	d, err := h.Backend.GetCustomModelDeployment(deployARN)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCustomModelDeploymentArn: d.CustomModelDeploymentArn,
		"modelDeploymentName":       d.ModelDeploymentName,
		keyModelArn:                 d.ModelArn,
		keyStatus:                   d.Status,
		keyCreationTime:             d.CreationTime.Format(time.RFC3339),
		keyLastModifiedTime:         d.LastModifiedTime.Format(time.RFC3339),
	})
}

func (h *Handler) handleListCustomModelDeployments(c *echo.Context) error {
	deployments := h.Backend.ListCustomModelDeployments()
	summaries := make([]map[string]any, 0, len(deployments))

	for _, d := range deployments {
		summaries = append(summaries, map[string]any{
			keyCustomModelDeploymentArn: d.CustomModelDeploymentArn,
			"modelDeploymentName":       d.ModelDeploymentName,
			keyModelArn:                 d.ModelArn,
			keyStatus:                   d.Status,
			keyCreationTime:             d.CreationTime.Format(time.RFC3339),
			keyLastModifiedTime:         d.LastModifiedTime.Format(time.RFC3339),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"deploymentSummaries": summaries})
}

func (h *Handler) handleUpdateCustomModelDeployment(c *echo.Context, deployARN string) error {
	d, err := h.Backend.UpdateCustomModelDeployment(deployARN)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyCustomModelDeploymentArn: d.CustomModelDeploymentArn})
}

func (h *Handler) handleDeleteCustomModelDeployment(c *echo.Context, deployARN string) error {
	if err := h.Backend.DeleteCustomModelDeployment(deployARN); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- FoundationModelAgreement handlers ---

func (h *Handler) handleListFoundationModelAgreementOffers(c *echo.Context) error {
	agreements := h.Backend.ListFoundationModelAgreementOffers()
	offers := make([]map[string]any, 0, len(agreements))

	for _, a := range agreements {
		offers = append(offers, map[string]any{"modelId": a.ModelID})
	}

	return c.JSON(http.StatusOK, map[string]any{"modelAgreementOffers": offers})
}

func (h *Handler) handleDeleteFoundationModelAgreement(c *echo.Context, modelID string) error {
	if modelID == "" {
		return c.NoContent(http.StatusNoContent)
	}

	if err := h.Backend.DeleteFoundationModelAgreement(modelID); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- UseCaseForModelAccess handlers ---

type putUseCaseInput struct {
	UseCaseType        string `json:"useCaseType"`
	UseCaseDescription string `json:"useCaseDescription"`
}

func (h *Handler) handleGetUseCaseForModelAccess(c *echo.Context) error {
	result := h.Backend.GetUseCaseForModelAccess()

	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handlePutUseCaseForModelAccess(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	in, parseErr := parseBody[putUseCaseInput](body)
	if parseErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	h.Backend.PutUseCaseForModelAccess(in.UseCaseType, in.UseCaseDescription)

	return c.NoContent(http.StatusNoContent)
}

// --- EnforcedGuardrailConfiguration handlers ---

type putEnforcedGuardrailInput struct {
	GuardrailID      string `json:"guardrailId"`
	GuardrailVersion string `json:"guardrailVersion"`
}

func (h *Handler) handleListEnforcedGuardrailsConfiguration(c *echo.Context) error {
	configs := h.Backend.ListEnforcedGuardrailsConfiguration()
	items := make([]map[string]any, 0, len(configs))

	for _, cfg := range configs {
		items = append(items, map[string]any{
			"guardrailId":      cfg.GuardrailID,
			"guardrailVersion": cfg.GuardrailVersion,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"enforcedGuardrailConfigurations": items})
}

func (h *Handler) handlePutEnforcedGuardrailConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	in, parseErr := parseBody[putEnforcedGuardrailInput](body)
	if parseErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	h.Backend.PutEnforcedGuardrailConfiguration(in.GuardrailID, in.GuardrailVersion)

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleDeleteEnforcedGuardrailConfiguration(c *echo.Context) error {
	guardrailID := c.Request().URL.Query().Get("guardrailId")
	if guardrailID == "" {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "guardrailId query parameter is required"),
		)
	}

	if err := h.Backend.DeleteEnforcedGuardrailConfiguration(guardrailID); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
