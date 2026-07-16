package bedrock

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func extractARPOperation(path, method string) (string, bool) {
	if method != http.MethodPost {
		return "", false
	}

	switch {
	case path == automatedReasoningPrefix:
		return "CreateAutomatedReasoningPolicy", true
	case isARPBuildWorkflowCancelPath(path):
		return "CancelAutomatedReasoningPolicyBuildWorkflow", true
	case isARPTestCasesPath(path):
		return "CreateAutomatedReasoningPolicyTestCase", true
	case isARPVersionsPath(path):
		return "CreateAutomatedReasoningPolicyVersion", true
	default:
		return "", false
	}
}

// isARPBuildWorkflowCancelPath matches /automated-reasoning-policies/{arn}/build-workflows/{id}/cancel.
func isARPBuildWorkflowCancelPath(path string) bool {
	rest, ok := strings.CutPrefix(path, automatedReasoningPrefix+"/")
	if !ok {
		return false
	}

	return strings.Contains(rest, "/build-workflows/") && strings.HasSuffix(rest, "/cancel")
}

// isARPTestCasesPath matches /automated-reasoning-policies/{arn}/test-cases.
func isARPTestCasesPath(path string) bool {
	rest, ok := strings.CutPrefix(path, automatedReasoningPrefix+"/")
	if !ok {
		return false
	}

	return strings.HasSuffix(rest, "/test-cases")
}

// isARPVersionsPath matches /automated-reasoning-policies/{arn}/versions.
func isARPVersionsPath(path string) bool {
	rest, ok := strings.CutPrefix(path, automatedReasoningPrefix+"/")
	if !ok {
		return false
	}

	return strings.HasSuffix(rest, "/versions")
}

func (h *Handler) routeARP(c *echo.Context, path, method string, body []byte) (bool, error) {
	if !strings.HasPrefix(path, automatedReasoningPrefix) {
		return false, nil
	}

	if path == automatedReasoningPrefix {
		return h.routeARPRoot(c, method, body)
	}

	if ok, err := h.routeARPBuildWorkflow(c, path, method); ok {
		return true, err
	}

	if ok, err := h.routeARPTestCase(c, path, method); ok {
		return true, err
	}

	if ok, err := h.routeARPVersionAnnotation(c, path, method, body); ok {
		return true, err
	}

	return h.routeARPSingleItem(c, path, method, body)
}

func (h *Handler) routeARPRoot(c *echo.Context, method string, body []byte) (bool, error) {
	switch method {
	case http.MethodPost:
		return true, h.handleCreateAutomatedReasoningPolicy(c, body)
	case http.MethodGet:
		return true, h.handleListAutomatedReasoningPolicies(c)
	}

	return false, nil
}

func (h *Handler) routeARPBuildWorkflow(c *echo.Context, path, method string) (bool, error) {
	switch {
	case isARPBuildWorkflowCancelPath(path) && method == http.MethodPost:
		return true, h.handleCancelAutomatedReasoningPolicyBuildWorkflow(c, path)
	case isARPBuildWorkflowResultAssetsPath(path) && method == http.MethodGet:
		return true, h.handleGetARPBuildWorkflowResultAssets(c, path)
	case isARPBuildWorkflowSubPath(path) && method == http.MethodGet:
		return true, h.handleGetARPBuildWorkflow(c, path)
	case isARPBuildWorkflowSubPath(path) && method == http.MethodDelete:
		return true, h.handleDeleteARPBuildWorkflow(c, path)
	case isARPBuildWorkflowsPath(path) && method == http.MethodPost:
		return true, h.handleStartARPBuildWorkflow(c, path)
	case isARPBuildWorkflowsPath(path) && method == http.MethodGet:
		return true, h.handleListARPBuildWorkflows(c, path)
	}

	return false, nil
}

func (h *Handler) routeARPTestCase(c *echo.Context, path, method string) (bool, error) {
	if ok, err := h.routeARPTestCaseCreate(c, path, method); ok {
		return true, err
	}

	return h.routeARPTestCaseItem(c, path, method)
}

func (h *Handler) routeARPTestCaseCreate(c *echo.Context, path, method string) (bool, error) {
	switch {
	case isARPTestCasesPath(path) && method == http.MethodPost:
		return true, h.handleCreateAutomatedReasoningPolicyTestCase(c, path)
	case isARPTestCasesPath(path) && method == http.MethodGet:
		return true, h.handleListARPTestCases(c, path)
	}

	return false, nil
}

func (h *Handler) routeARPTestCaseItem(c *echo.Context, path, method string) (bool, error) {
	switch {
	case isARPTestCasesResultsPath(path) && method == http.MethodGet:
		return true, h.handleListARPTestResults(c, path)
	case isARPTestCaseRunPath(path) && method == http.MethodPost:
		return true, h.handleStartARPTestWorkflow(c, path)
	case isARPTestCaseResultPath(path) && method == http.MethodGet:
		return true, h.handleGetARPTestResult(c, path)
	case isARPTestCaseSubPath(path) && method == http.MethodGet:
		return true, h.handleGetARPTestCase(c, path)
	case isARPTestCaseSubPath(path) && method == http.MethodPut:
		return true, h.handleUpdateARPTestCase(c, path)
	case isARPTestCaseSubPath(path) && method == http.MethodDelete:
		return true, h.handleDeleteARPTestCase(c, path)
	}

	return false, nil
}

func (h *Handler) routeARPVersionAnnotation(
	c *echo.Context,
	path, method string,
	body []byte,
) (bool, error) {
	switch {
	case isARPVersionsPath(path) && method == http.MethodPost:
		return true, h.handleCreateAutomatedReasoningPolicyVersion(c, path, body)
	case isARPVersionExportPath(path) && method == http.MethodPost:
		return true, h.handleExportARPVersion(c, path)
	case isARPAnnotationsPath(path) && method == http.MethodGet:
		return true, h.handleGetARPAnnotations(c, path)
	case isARPAnnotationsPath(path) && method == http.MethodPut:
		return true, h.handleUpdateARPAnnotations(c, path)
	case isARPNextScenarioPath(path) && method == http.MethodGet:
		return true, h.handleGetARPNextScenario(c, path)
	}

	return false, nil
}

func (h *Handler) routeARPSingleItem(
	c *echo.Context,
	path, method string,
	body []byte,
) (bool, error) {
	if !strings.HasPrefix(path, automatedReasoningPrefix+"/") {
		return false, nil
	}

	policyARN := decodePath(strings.TrimPrefix(path, automatedReasoningPrefix+"/"))

	switch method {
	case http.MethodGet:
		return true, h.handleGetAutomatedReasoningPolicy(c, policyARN)
	case http.MethodPut:
		return true, h.handleUpdateAutomatedReasoningPolicy(c, policyARN, body)
	case http.MethodDelete:
		return true, h.handleDeleteAutomatedReasoningPolicy(c, policyARN)
	}

	return false, nil
}

type createAutomatedReasoningPolicyInput struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Tags        []Tag  `json:"tags,omitempty"`
}

type createAutomatedReasoningPolicyOutput struct {
	CreatedAt isoTime `json:"createdAt"`
	UpdatedAt isoTime `json:"updatedAt"`
	PolicyArn string  `json:"policyArn"`
	Name      string  `json:"name"`
	Status    string  `json:"status"`
}

func (h *Handler) handleCreateAutomatedReasoningPolicy(c *echo.Context, body []byte) error {
	in, err := parseBody[createAutomatedReasoningPolicyInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	policy, opErr := h.Backend.CreateAutomatedReasoningPolicy(in.Name, in.Description, in.Tags)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, createAutomatedReasoningPolicyOutput{
		PolicyArn: policy.PolicyArn,
		Name:      policy.Name,
		Status:    policy.Status,
		CreatedAt: isoTime{policy.CreatedAt},
		UpdatedAt: isoTime{policy.UpdatedAt},
	})
}

// handleCancelAutomatedReasoningPolicyBuildWorkflow cancels a build workflow.
// Path: /automated-reasoning-policies/{policyArn}/build-workflows/{buildWorkflowId}/cancel.
func (h *Handler) handleCancelAutomatedReasoningPolicyBuildWorkflow(
	c *echo.Context,
	path string,
) error {
	policyARN, workflowID := extractARPBuildWorkflowIDs(path)

	if opErr := h.Backend.CancelAutomatedReasoningPolicyBuildWorkflow(policyARN, workflowID); opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.NoContent(http.StatusOK)
}

// extractARPBuildWorkflowIDs parses policyArn and workflowId from the cancel path.
func extractARPBuildWorkflowIDs(path string) (string, string) {
	rest, _ := strings.CutPrefix(path, automatedReasoningPrefix+"/")
	// rest = {policyArn}/build-workflows/{buildWorkflowId}/cancel
	beforeCancel := strings.TrimSuffix(rest, "/cancel")
	idx := strings.LastIndex(beforeCancel, "/build-workflows/")

	if idx < 0 {
		return "", ""
	}

	policyARN := decodePath(beforeCancel[:idx])
	workflowID := beforeCancel[idx+len("/build-workflows/"):]

	return policyARN, workflowID
}

// handleCreateAutomatedReasoningPolicyTestCase creates a test case.
// Path: /automated-reasoning-policies/{policyArn}/test-cases.
func (h *Handler) handleCreateAutomatedReasoningPolicyTestCase(c *echo.Context, path string) error {
	rest, _ := strings.CutPrefix(path, automatedReasoningPrefix+"/")
	policyARN := decodePath(strings.TrimSuffix(rest, "/test-cases"))

	tc, opErr := h.Backend.CreateAutomatedReasoningPolicyTestCase(policyARN)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, map[string]string{
		"policyArn":   tc.PolicyArn,
		keyTestCaseID: tc.TestCaseID,
	})
}

type createAutomatedReasoningPolicyVersionInput struct {
	LastUpdatedDefinitionHash string `json:"lastUpdatedDefinitionHash"`
}

// handleCreateAutomatedReasoningPolicyVersion creates a policy version.
// Path: /automated-reasoning-policies/{policyArn}/versions.
func (h *Handler) handleCreateAutomatedReasoningPolicyVersion(
	c *echo.Context,
	path string,
	body []byte,
) error {
	in, err := parseBody[createAutomatedReasoningPolicyVersionInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	rest, _ := strings.CutPrefix(path, automatedReasoningPrefix+"/")
	policyARN := decodePath(strings.TrimSuffix(rest, "/versions"))

	version, opErr := h.Backend.CreateAutomatedReasoningPolicyVersion(
		policyARN,
		in.LastUpdatedDefinitionHash,
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{
		"policyArn":      version.PolicyArn,
		keyName:          version.Name,
		"definitionHash": version.DefinitionHash,
		"version":        version.Version,
		"createdAt":      isoTime{version.CreatedAt},
	})
}

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
