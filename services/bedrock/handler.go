package bedrock

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	guardrailsPrefix             = "/guardrails"
	foundationModelsPrefix       = "/foundation-models"
	provisionedModelThroughput   = "/provisioned-model-throughput"
	provisionedModelThroughputs  = "/provisioned-model-throughputs"
	listTagsForResourcePath      = "/listTagsForResource"
	tagResourcePath              = "/tagResource"
	untagResourcePath            = "/untagResource"
	evaluationJobsPrefix         = "/evaluation-jobs"
	evaluationJobsBatchDelete    = "/evaluation-jobs/batch-delete"
	automatedReasoningPrefix     = "/automated-reasoning-policies"
	customModelsCreate           = "/custom-models/create-custom-model"
	customModelDeploymentsPath   = "/model-customization/custom-model-deployments"
	foundationModelAgreement     = "/create-foundation-model-agreement"
	customModelsPrefix           = "/custom-models"
	modelCustomizationJobsPrefix = "/model-customization-jobs"
	inferenceProfilesPrefix      = "/inference-profiles"
	marketplaceEndpointsPrefix   = "/marketplace-model/endpoints"
	loggingConfigPath            = "/logging/modelinvocations"

	// Response key constants.
	keyJobArn                   = "jobArn"
	keyStatus                   = "status"
	keyDeploymentArn            = "deploymentArn"
	keyJobName                  = "jobName"
	keyCreationTime             = "creationTime"
	keyLastModifiedTime         = "lastModifiedTime"
	keyPolicyArn                = "policyArn"
	keyBuildWorkflowID          = "buildWorkflowId"
	keyCreatedAt                = "createdAt"
	jobStatusCompleted          = "Completed"
	keyTestCaseID               = "testCaseId"
	keyName                     = "name"
	keyUpdatedAt                = "updatedAt"
	keyModelArn                 = "modelArn"
	keyPromptRouterArn          = "promptRouterArn"
	keyCustomModelDeploymentArn = "customModelDeploymentArn"

	// Stub operation paths.
	modelCopyJobsPrefix           = "/model-copy-jobs"
	modelImportJobsPrefix         = "/model-import-jobs"
	modelInvocationJobsPrefix     = "/model-invocation-jobs"
	promptRoutersPrefix           = "/prompt-routers"
	importedModelsPrefix          = "/imported-models"
	foundationModelAvailPath      = "/foundation-model-availability"
	foundationModelAgreementsPath = "/foundation-model-agreement-offers"
	customModelDeployments2Path   = "/custom-model-deployments"
	useCaseForModelAccessPath     = "/usecase-for-model-access"
	enforcedGuardrailsPath        = "/enforced-guardrail-configuration"
)

// isoTime is a [time.Time] that marshals as RFC3339.
type isoTime struct {
	time.Time
}

func (t isoTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Time.Format(time.RFC3339))
}

// Handler is the Echo HTTP handler for Amazon Bedrock operations.
type Handler struct {
	Backend       *InMemoryBackend
	janitorCancel context.CancelFunc
	janitorDone   chan struct{}
}

// NewHandler creates a new Bedrock handler backed by backend.
// backend must not be nil.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// StartWorker starts the background janitor for status advancement.
func (h *Handler) StartWorker(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	h.janitorCancel = cancel
	h.janitorDone = done

	go func() {
		defer close(done)
		h.Backend.RunJanitor(runCtx, defaultJanitorInterval)
	}()

	return nil
}

// Shutdown stops the background janitor.
func (h *Handler) Shutdown(ctx context.Context) {
	if h.janitorCancel != nil {
		h.janitorCancel()
	}

	if h.janitorDone != nil {
		select {
		case <-h.janitorDone:
		case <-ctx.Done():
		}
	}
}

var _ service.BackgroundWorker = (*Handler)(nil)
var _ service.Shutdowner = (*Handler)(nil)

// Name returns the service name.
func (h *Handler) Name() string { return "Bedrock" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"BatchDeleteEvaluationJob",
		"CancelAutomatedReasoningPolicyBuildWorkflow",
		"CreateAutomatedReasoningPolicy",
		"CreateAutomatedReasoningPolicyTestCase",
		"CreateAutomatedReasoningPolicyVersion",
		"CreateCustomModel",
		"CreateCustomModelDeployment",
		"CreateEvaluationJob",
		"CreateFoundationModelAgreement",
		"CreateGuardrail",
		"CreateGuardrailVersion",
		"CreateInferenceProfile",
		"CreateMarketplaceModelEndpoint",
		"CreateModelCustomizationJob",
		"CreateProvisionedModelThroughput",
		"DeleteCustomModel",
		"DeleteGuardrail",
		"DeleteInferenceProfile",
		"DeleteMarketplaceModelEndpoint",
		"DeleteModelInvocationLoggingConfiguration",
		"DeleteProvisionedModelThroughput",
		"DeregisterMarketplaceModelEndpoint",
		"GetCustomModel",
		"GetFoundationModel",
		"GetGuardrail",
		"GetInferenceProfile",
		"GetMarketplaceModelEndpoint",
		"GetModelCustomizationJob",
		"GetModelInvocationLoggingConfiguration",
		"GetProvisionedModelThroughput",
		"ListCustomModels",
		"ListFoundationModels",
		"ListGuardrails",
		"ListInferenceProfiles",
		"ListMarketplaceModelEndpoints",
		"ListModelCustomizationJobs",
		"ListProvisionedModelThroughputs",
		"ListTagsForResource",
		"PutModelInvocationLoggingConfiguration",
		"RegisterMarketplaceModelEndpoint",
		"StopModelCustomizationJob",
		"TagResource",
		"UntagResource",
		"UpdateGuardrail",
		"UpdateMarketplaceModelEndpoint",
		"UpdateProvisionedModelThroughput",
		// Batch 2: real stateful ops implemented in this release.
		"CreateModelCopyJob",
		"CreateModelImportJob",
		"CreateModelInvocationJob",
		"CreatePromptRouter",
		"DeleteAutomatedReasoningPolicy",
		"DeleteAutomatedReasoningPolicyBuildWorkflow",
		"DeleteAutomatedReasoningPolicyTestCase",
		"DeleteCustomModelDeployment",
		"DeleteEnforcedGuardrailConfiguration",
		"DeleteFoundationModelAgreement",
		"DeleteImportedModel",
		"DeletePromptRouter",
		"ExportAutomatedReasoningPolicyVersion",
		"GetAutomatedReasoningPolicy",
		"GetAutomatedReasoningPolicyAnnotations",
		"GetAutomatedReasoningPolicyBuildWorkflow",
		"GetAutomatedReasoningPolicyBuildWorkflowResultAssets",
		"GetAutomatedReasoningPolicyNextScenario",
		"GetAutomatedReasoningPolicyTestCase",
		"GetAutomatedReasoningPolicyTestResult",
		"GetCustomModelDeployment",
		"GetEvaluationJob",
		"GetFoundationModelAvailability",
		"GetImportedModel",
		"GetModelCopyJob",
		"GetModelImportJob",
		"GetModelInvocationJob",
		"GetPromptRouter",
		"GetUseCaseForModelAccess",
		"ListAutomatedReasoningPolicies",
		"ListAutomatedReasoningPolicyBuildWorkflows",
		"ListAutomatedReasoningPolicyTestCases",
		"ListAutomatedReasoningPolicyTestResults",
		"ListCustomModelDeployments",
		"ListEnforcedGuardrailsConfiguration",
		"ListEvaluationJobs",
		"ListFoundationModelAgreementOffers",
		"ListImportedModels",
		"ListModelCopyJobs",
		"ListModelImportJobs",
		"ListModelInvocationJobs",
		"ListPromptRouters",
		"PutEnforcedGuardrailConfiguration",
		"PutUseCaseForModelAccess",
		"StartAutomatedReasoningPolicyBuildWorkflow",
		"StartAutomatedReasoningPolicyTestWorkflow",
		"StopEvaluationJob",
		"StopModelInvocationJob",
		"UpdateAutomatedReasoningPolicy",
		"UpdateAutomatedReasoningPolicyAnnotations",
		"UpdateAutomatedReasoningPolicyTestCase",
		"UpdateCustomModelDeployment",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "bedrock" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Bedrock requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return matchBedrockPath(c.Request().URL.Path)
	}
}

// matchBedrockPath returns true if the path matches a known Bedrock API path.
func matchBedrockPath(path string) bool {
	return matchBedrockPrefixPaths(path) || matchBedrockExactPaths(path)
}

// matchBedrockPrefixPaths returns true if path has a known Bedrock prefix.
func matchBedrockPrefixPaths(path string) bool {
	return matchBedrockCorePrefixes(path) || matchBedrockExtPrefixes(path)
}

// matchBedrockCorePrefixes checks the core Bedrock resource prefixes.
func matchBedrockCorePrefixes(path string) bool {
	return strings.HasPrefix(path, guardrailsPrefix) ||
		strings.HasPrefix(path, foundationModelsPrefix) ||
		strings.HasPrefix(path, provisionedModelThroughput) ||
		strings.HasPrefix(path, evaluationJobsPrefix) ||
		strings.HasPrefix(path, automatedReasoningPrefix) ||
		strings.HasPrefix(path, modelCustomizationJobsPrefix) ||
		strings.HasPrefix(path, inferenceProfilesPrefix) ||
		strings.HasPrefix(path, marketplaceEndpointsPrefix) ||
		strings.HasPrefix(path, customModelsPrefix)
}

// matchBedrockExtPrefixes checks the extended Bedrock resource prefixes.
func matchBedrockExtPrefixes(path string) bool {
	return strings.HasPrefix(path, modelCopyJobsPrefix) ||
		strings.HasPrefix(path, modelImportJobsPrefix) ||
		strings.HasPrefix(path, modelInvocationJobsPrefix) ||
		strings.HasPrefix(path, promptRoutersPrefix) ||
		strings.HasPrefix(path, importedModelsPrefix) ||
		strings.HasPrefix(path, foundationModelAvailPath) ||
		strings.HasPrefix(path, foundationModelAgreementsPath) ||
		strings.HasPrefix(path, customModelDeployments2Path)
}

// matchBedrockExactPaths returns true if path exactly matches a known Bedrock path.
func matchBedrockExactPaths(path string) bool {
	return path == useCaseForModelAccessPath ||
		path == enforcedGuardrailsPath ||
		path == loggingConfigPath ||
		path == customModelDeploymentsPath ||
		path == foundationModelAgreement ||
		path == listTagsForResourcePath ||
		path == tagResourcePath ||
		path == untagResourcePath
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation returns the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

	for _, fn := range []func(string, string) (string, bool){
		extractGuardrailOperation,
		extractFoundationModelOperation,
		extractPMTOperation,
		extractTagOperation,
		extractEvaluationJobOperation,
		extractARPOperation,
		extractCustomModelOperation,
		extractCustomModelListOperation,
		extractCustomizationJobOperation,
		extractInferenceProfileOperation,
		extractMarketplaceEndpointOperation,
		extractLoggingConfigOperation,
	} {
		if op, ok := fn(path, method); ok {
			return op
		}
	}

	return "Unknown"
}

func extractGuardrailOperation(path, method string) (string, bool) {
	switch {
	case path == guardrailsPrefix && method == http.MethodPost:
		return "CreateGuardrail", true
	case path == guardrailsPrefix && method == http.MethodGet:
		return "ListGuardrails", true
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodGet:
		return "GetGuardrail", true
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodPut:
		return "UpdateGuardrail", true
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodDelete:
		return "DeleteGuardrail", true
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodPost:
		return "CreateGuardrailVersion", true
	default:
		return "", false
	}
}

func extractFoundationModelOperation(path, method string) (string, bool) {
	switch {
	case path == foundationModelsPrefix && method == http.MethodGet:
		return "ListFoundationModels", true
	case strings.HasPrefix(path, foundationModelsPrefix+"/") && method == http.MethodGet:
		return "GetFoundationModel", true
	default:
		return "", false
	}
}

func extractPMTOperation(path, method string) (string, bool) {
	switch {
	case path == provisionedModelThroughput && method == http.MethodPost:
		return "CreateProvisionedModelThroughput", true
	case path == provisionedModelThroughputs && method == http.MethodGet:
		return "ListProvisionedModelThroughputs", true
	case strings.HasPrefix(path, provisionedModelThroughput+"/") && method == http.MethodGet:
		return "GetProvisionedModelThroughput", true
	case strings.HasPrefix(path, provisionedModelThroughput+"/") && method == http.MethodPut:
		return "UpdateProvisionedModelThroughput", true
	case strings.HasPrefix(path, provisionedModelThroughput+"/") && method == http.MethodDelete:
		return "DeleteProvisionedModelThroughput", true
	default:
		return "", false
	}
}

func extractTagOperation(path, method string) (string, bool) {
	if method != http.MethodPost {
		return "", false
	}

	switch path {
	case listTagsForResourcePath:
		return "ListTagsForResource", true
	case tagResourcePath:
		return "TagResource", true
	case untagResourcePath:
		return "UntagResource", true
	default:
		return "", false
	}
}

func extractEvaluationJobOperation(path, method string) (string, bool) {
	switch {
	case path == evaluationJobsBatchDelete && method == http.MethodPost:
		return "BatchDeleteEvaluationJob", true
	case path == evaluationJobsPrefix && method == http.MethodPost:
		return "CreateEvaluationJob", true
	default:
		return "", false
	}
}

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

func extractCustomModelOperation(path, method string) (string, bool) {
	if method != http.MethodPost {
		return "", false
	}

	switch path {
	case customModelsCreate:
		return "CreateCustomModel", true
	case customModelDeploymentsPath:
		return "CreateCustomModelDeployment", true
	case foundationModelAgreement:
		return "CreateFoundationModelAgreement", true
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

// ExtractResource extracts a resource identifier from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path

	if id, ok := strings.CutPrefix(path, guardrailsPrefix+"/"); ok {
		return id
	}

	if id, ok := strings.CutPrefix(path, foundationModelsPrefix+"/"); ok {
		return id
	}

	if id, ok := strings.CutPrefix(path, provisionedModelThroughput+"/"); ok {
		return id
	}

	if id, ok := strings.CutPrefix(path, automatedReasoningPrefix+"/"); ok {
		return id
	}

	if id, ok := strings.CutPrefix(path, customModelsPrefix+"/"); ok {
		return id
	}

	if id, ok := strings.CutPrefix(path, modelCustomizationJobsPrefix+"/"); ok {
		return id
	}

	if id, ok := strings.CutPrefix(path, inferenceProfilesPrefix+"/"); ok {
		return id
	}

	if id, ok := strings.CutPrefix(path, marketplaceEndpointsPrefix+"/"); ok {
		return id
	}

	return ""
}

// Handler returns the Echo handler function for Bedrock requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		path := r.URL.Path
		method := r.Method
		log := logger.Load(r.Context())

		var body []byte
		if method == http.MethodPost || method == http.MethodPut {
			var err error
			body, err = httputils.ReadBody(r)
			if err != nil {
				log.ErrorContext(r.Context(), "bedrock: failed to read request body", "error", err)

				return c.JSON(
					http.StatusInternalServerError,
					errorResponse("InternalFailure", "internal server error"),
				)
			}
		}

		return h.dispatch(c, path, method, body)
	}
}

// dispatch routes a Bedrock request to the appropriate handler.
func (h *Handler) dispatch(c *echo.Context, path, method string, body []byte) error {
	if ok, err := h.routeGuardrail(c, path, method, body); ok {
		return err
	}
	if ok, err := h.routeFoundationModel(c, path, method); ok {
		return err
	}
	if ok, err := h.routePMT(c, path, method, body); ok {
		return err
	}
	if ok, err := h.routeTag(c, path, method, body); ok {
		return err
	}
	if ok, err := h.routeEvaluationJob(c, path, method, body); ok {
		return err
	}
	if ok, err := h.routeARP(c, path, method, body); ok {
		return err
	}
	if ok, err := h.routeCustomModel(c, path, method, body); ok {
		return err
	}
	if ok, err := h.routeCustomModelList(c, path, method); ok {
		return err
	}

	return h.dispatchExtended(c, path, method, body)
}

// dispatchExtended handles additional Bedrock route groups.
func (h *Handler) dispatchExtended(c *echo.Context, path, method string, body []byte) error {
	if ok, err := h.routeCustomizationJob(c, path, method, body); ok {
		return err
	}
	if ok, err := h.routeInferenceProfile(c, path, method, body); ok {
		return err
	}
	if ok, err := h.routeMarketplaceEndpoint(c, path, method, body); ok {
		return err
	}
	if ok, err := h.routeLoggingConfig(c, path, method, body); ok {
		return err
	}

	if ok, err := h.routeStubOps(c, path, method); ok {
		return err
	}

	return c.JSON(
		http.StatusNotFound,
		errorResponse("UnknownOperationException", "unknown operation: "+path),
	)
}

// routeStubOps handles stub operations that return minimal valid responses.
func (h *Handler) routeStubOps(c *echo.Context, path, method string) (bool, error) {
	if ok, err := h.routeStubJobOps(c, path, method); ok {
		return true, err
	}

	if ok, err := h.routeStubModelOps(c, path, method); ok {
		return true, err
	}

	return h.routeStubMiscOps(c, path, method)
}

// routeStubJobOps handles model copy, import, and invocation job stubs.
func (h *Handler) routeStubJobOps(c *echo.Context, path, method string) (bool, error) {
	if ok, err := h.routeStubCopyImportOps(c, path, method); ok {
		return true, err
	}

	return h.routeStubInvocationOps(c, path, method)
}

// routeStubCopyImportOps handles model copy and import job operations backed by real state.
func (h *Handler) routeStubCopyImportOps(c *echo.Context, path, method string) (bool, error) {
	switch {
	case path == modelCopyJobsPrefix && method == http.MethodPost:
		return true, h.handleCreateModelCopyJob(c)
	case path == modelCopyJobsPrefix && method == http.MethodGet:
		return true, h.handleListModelCopyJobs(c)
	case strings.HasPrefix(path, modelCopyJobsPrefix+"/") && method == http.MethodGet:
		jobARN, _ := url.PathUnescape(strings.TrimPrefix(path, modelCopyJobsPrefix+"/"))

		return true, h.handleGetModelCopyJob(c, jobARN)
	case path == modelImportJobsPrefix && method == http.MethodPost:
		return true, h.handleCreateModelImportJob(c)
	case path == modelImportJobsPrefix && method == http.MethodGet:
		return true, h.handleListModelImportJobs(c)
	case strings.HasPrefix(path, modelImportJobsPrefix+"/") && method == http.MethodGet:
		jobARN, _ := url.PathUnescape(strings.TrimPrefix(path, modelImportJobsPrefix+"/"))

		return true, h.handleGetModelImportJob(c, jobARN)
	}

	return false, nil
}

// createModelCopyJobInput is the parsed request body for CreateModelCopyJob.
type createModelCopyJobInput struct {
	SourceModelArn string `json:"sourceModelArn"`
	Tags           []Tag  `json:"tags,omitempty"`
}

func (h *Handler) handleCreateModelCopyJob(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	in, parseErr := parseBody[createModelCopyJobInput](body)
	if parseErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	job, opErr := h.Backend.CreateModelCopyJob(in.SourceModelArn, in.Tags)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, modelCopyJobToOutput(job))
}

func (h *Handler) handleListModelCopyJobs(c *echo.Context) error {
	jobs := h.Backend.ListModelCopyJobs()
	summaries := make([]map[string]any, 0, len(jobs))

	for _, j := range jobs {
		summaries = append(summaries, modelCopyJobToOutput(j))
	}

	return c.JSON(http.StatusOK, map[string]any{"modelCopyJobSummaries": summaries})
}

func (h *Handler) handleGetModelCopyJob(c *echo.Context, jobARN string) error {
	job, err := h.Backend.GetModelCopyJob(jobARN)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, modelCopyJobToOutput(job))
}

func modelCopyJobToOutput(j *ModelCopyJob) map[string]any {
	out := map[string]any{
		keyJobArn:           j.JobArn,
		"sourceModelArn":    j.SourceModelArn,
		"targetModelArn":    j.TargetModelArn,
		keyStatus:           j.Status,
		keyCreationTime:     j.CreationTime.Format(time.RFC3339),
		keyLastModifiedTime: j.LastModifiedTime.Format(time.RFC3339),
	}

	if j.FailureMessage != "" {
		out["failureMessage"] = j.FailureMessage
	}

	if len(j.Tags) > 0 {
		out["tags"] = j.Tags
	}

	return out
}

// createModelImportJobInput is the parsed request body for CreateModelImportJob.
type createModelImportJobInput struct {
	JobName string `json:"jobName"`
	Tags    []Tag  `json:"tags,omitempty"`
}

func (h *Handler) handleCreateModelImportJob(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	in, parseErr := parseBody[createModelImportJobInput](body)
	if parseErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	job, opErr := h.Backend.CreateModelImportJob(in.JobName, in.Tags)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, modelImportJobToOutput(job))
}

func (h *Handler) handleListModelImportJobs(c *echo.Context) error {
	jobs := h.Backend.ListModelImportJobs()
	summaries := make([]map[string]any, 0, len(jobs))

	for _, j := range jobs {
		summaries = append(summaries, modelImportJobToOutput(j))
	}

	return c.JSON(http.StatusOK, map[string]any{"modelImportJobSummaries": summaries})
}

func (h *Handler) handleGetModelImportJob(c *echo.Context, jobARN string) error {
	job, err := h.Backend.GetModelImportJob(jobARN)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, modelImportJobToOutput(job))
}

func modelImportJobToOutput(j *ModelImportJob) map[string]any {
	out := map[string]any{
		keyJobArn:           j.JobArn,
		keyJobName:          j.JobName,
		"importedModelArn":  j.ImportedModelArn,
		keyStatus:           j.Status,
		keyCreationTime:     j.CreationTime.Format(time.RFC3339),
		keyLastModifiedTime: j.LastModifiedTime.Format(time.RFC3339),
	}

	if j.EndTime != nil {
		out["endTime"] = j.EndTime.Format(time.RFC3339)
	}

	if len(j.Tags) > 0 {
		out["tags"] = j.Tags
	}

	return out
}

// routeStubInvocationOps handles model invocation job operations.
func (h *Handler) routeStubInvocationOps(c *echo.Context, path, method string) (bool, error) {
	switch {
	case path == modelInvocationJobsPrefix && method == http.MethodPost:
		return true, h.handleCreateModelInvocationJob(c)
	case path == modelInvocationJobsPrefix && method == http.MethodGet:
		return true, h.handleListModelInvocationJobs(c)
	case strings.HasPrefix(path, modelInvocationJobsPrefix+"/") && method == http.MethodGet:
		jobARN, _ := url.PathUnescape(strings.TrimPrefix(path, modelInvocationJobsPrefix+"/"))

		return true, h.handleGetModelInvocationJob(c, jobARN)
	case strings.HasPrefix(path, modelInvocationJobsPrefix+"/") && method == http.MethodDelete:
		jobARN, _ := url.PathUnescape(strings.TrimPrefix(path, modelInvocationJobsPrefix+"/"))

		return true, h.handleStopModelInvocationJob(c, jobARN)
	}

	return false, nil
}

// routeStubModelOps handles prompt router, imported model, and foundation model stubs.
func (h *Handler) routeStubModelOps(c *echo.Context, path, method string) (bool, error) {
	if ok, err := h.routeStubPromptRouterOps(c, path, method); ok {
		return true, err
	}

	return h.routeStubFoundationModelOps(c, path, method)
}

// routeStubPromptRouterOps handles prompt router and imported model operations.
func (h *Handler) routeStubPromptRouterOps(c *echo.Context, path, method string) (bool, error) {
	switch {
	case path == promptRoutersPrefix && method == http.MethodPost:
		return true, h.handleCreatePromptRouter(c)
	case path == promptRoutersPrefix && method == http.MethodGet:
		return true, h.handleListPromptRouters(c)
	case strings.HasPrefix(path, promptRoutersPrefix+"/") && method == http.MethodGet:
		routerARN, _ := url.PathUnescape(strings.TrimPrefix(path, promptRoutersPrefix+"/"))

		return true, h.handleGetPromptRouter(c, routerARN)
	case strings.HasPrefix(path, promptRoutersPrefix+"/") && method == http.MethodDelete:
		routerARN, _ := url.PathUnescape(strings.TrimPrefix(path, promptRoutersPrefix+"/"))

		return true, h.handleDeletePromptRouter(c, routerARN)
	case path == importedModelsPrefix && method == http.MethodGet:
		return true, h.handleListImportedModels(c)
	case strings.HasPrefix(path, importedModelsPrefix+"/") && method == http.MethodGet:
		modelARN, _ := url.PathUnescape(strings.TrimPrefix(path, importedModelsPrefix+"/"))

		return true, h.handleGetImportedModel(c, modelARN)
	case strings.HasPrefix(path, importedModelsPrefix+"/") && method == http.MethodDelete:
		modelARN, _ := url.PathUnescape(strings.TrimPrefix(path, importedModelsPrefix+"/"))

		return true, h.handleDeleteImportedModel(c, modelARN)
	}

	return false, nil
}

// routeStubFoundationModelOps handles foundation model availability and agreement operations.
func (h *Handler) routeStubFoundationModelOps(c *echo.Context, path, method string) (bool, error) {
	switch {
	case strings.HasPrefix(path, foundationModelAvailPath+"/") && method == http.MethodGet:
		return true, c.JSON(http.StatusOK,
			map[string]any{"agreementAvailability": map[string]string{keyStatus: "AVAILABLE"}})
	case path == foundationModelAgreementsPath && method == http.MethodGet:
		return true, h.handleListFoundationModelAgreementOffers(c)
	case strings.HasPrefix(path, "/delete-foundation-model-agreement") && method == http.MethodDelete:
		modelID := strings.TrimPrefix(path, "/delete-foundation-model-agreement/")

		return true, h.handleDeleteFoundationModelAgreement(c, modelID)
	}

	return false, nil
}

// routeStubMiscOps handles custom model deployment, use case, and enforced guardrail stubs.
func (h *Handler) routeStubMiscOps(c *echo.Context, path, method string) (bool, error) {
	if ok, err := h.routeStubDeploymentOps(c, path, method); ok {
		return true, err
	}

	return h.routeStubAccessOps(c, path, method)
}

// routeStubDeploymentOps handles custom model deployment operations.
func (h *Handler) routeStubDeploymentOps(c *echo.Context, path, method string) (bool, error) {
	switch {
	case path == customModelDeployments2Path && method == http.MethodGet:
		return true, h.handleListCustomModelDeployments(c)
	case strings.HasPrefix(path, customModelDeployments2Path+"/") && method == http.MethodGet:
		deployARN, _ := url.PathUnescape(strings.TrimPrefix(path, customModelDeployments2Path+"/"))

		return true, h.handleGetCustomModelDeployment(c, deployARN)
	case strings.HasPrefix(path, customModelDeployments2Path+"/") && method == http.MethodPatch:
		deployARN, _ := url.PathUnescape(strings.TrimPrefix(path, customModelDeployments2Path+"/"))

		return true, h.handleUpdateCustomModelDeployment(c, deployARN)
	case strings.HasPrefix(path, customModelDeployments2Path+"/") && method == http.MethodDelete:
		deployARN, _ := url.PathUnescape(strings.TrimPrefix(path, customModelDeployments2Path+"/"))

		return true, h.handleDeleteCustomModelDeployment(c, deployARN)
	}

	return false, nil
}

// routeStubAccessOps handles use case for model access and enforced guardrail operations.
func (h *Handler) routeStubAccessOps(c *echo.Context, path, method string) (bool, error) {
	switch {
	case path == useCaseForModelAccessPath && method == http.MethodGet:
		return true, h.handleGetUseCaseForModelAccess(c)
	case path == useCaseForModelAccessPath && method == http.MethodPut:
		return true, h.handlePutUseCaseForModelAccess(c)
	case path == enforcedGuardrailsPath && method == http.MethodGet:
		return true, h.handleListEnforcedGuardrailsConfiguration(c)
	case path == enforcedGuardrailsPath && method == http.MethodPut:
		return true, h.handlePutEnforcedGuardrailConfiguration(c)
	case path == enforcedGuardrailsPath && method == http.MethodDelete:
		return true, h.handleDeleteEnforcedGuardrailConfiguration(c)
	}

	return false, nil
}

func (h *Handler) routeGuardrail(c *echo.Context, path, method string, body []byte) (bool, error) {
	id := decodePath(strings.TrimPrefix(path, guardrailsPrefix+"/"))

	switch {
	case path == guardrailsPrefix && method == http.MethodPost:
		return true, h.handleCreateGuardrail(c, body)
	case path == guardrailsPrefix && method == http.MethodGet:
		return true, h.handleListGuardrails(c)
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodGet:
		return true, h.handleGetGuardrail(c, id)
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodPut:
		return true, h.handleUpdateGuardrail(c, id, body)
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodDelete:
		return true, h.handleDeleteGuardrail(c, id)
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodPost:
		return true, h.handleCreateGuardrailVersion(c, id, body)
	default:
		return false, nil
	}
}

func (h *Handler) routeFoundationModel(c *echo.Context, path, method string) (bool, error) {
	switch {
	case path == foundationModelsPrefix && method == http.MethodGet:
		return true, h.handleListFoundationModels(c)
	case strings.HasPrefix(path, foundationModelsPrefix+"/") && method == http.MethodGet:
		id := decodePath(strings.TrimPrefix(path, foundationModelsPrefix+"/"))

		return true, h.handleGetFoundationModel(c, id)
	default:
		return false, nil
	}
}

func (h *Handler) routePMT(c *echo.Context, path, method string, body []byte) (bool, error) {
	id := decodePath(strings.TrimPrefix(path, provisionedModelThroughput+"/"))

	switch {
	case path == provisionedModelThroughput && method == http.MethodPost:
		return true, h.handleCreateProvisionedModelThroughput(c, body)
	case path == provisionedModelThroughputs && method == http.MethodGet:
		return true, h.handleListProvisionedModelThroughputs(c)
	case strings.HasPrefix(path, provisionedModelThroughput+"/") && method == http.MethodGet:
		return true, h.handleGetProvisionedModelThroughput(c, id)
	case strings.HasPrefix(path, provisionedModelThroughput+"/") && method == http.MethodPut:
		return true, h.handleUpdateProvisionedModelThroughput(c, id, body)
	case strings.HasPrefix(path, provisionedModelThroughput+"/") && method == http.MethodDelete:
		return true, h.handleDeleteProvisionedModelThroughput(c, id)
	default:
		return false, nil
	}
}

func (h *Handler) routeTag(c *echo.Context, path, method string, body []byte) (bool, error) {
	if method != http.MethodPost {
		return false, nil
	}

	switch path {
	case listTagsForResourcePath:
		return true, h.handleListTagsForResource(c, body)
	case tagResourcePath:
		return true, h.handleTagResource(c, body)
	case untagResourcePath:
		return true, h.handleUntagResource(c, body)
	default:
		return false, nil
	}
}

func (h *Handler) routeEvaluationJob(
	c *echo.Context,
	path, method string,
	body []byte,
) (bool, error) {
	switch {
	case path == evaluationJobsBatchDelete && method == http.MethodPost:
		return true, h.handleBatchDeleteEvaluationJob(c, body)
	case path == evaluationJobsPrefix && method == http.MethodPost:
		return true, h.handleCreateEvaluationJob(c, body)
	case path == evaluationJobsPrefix && method == http.MethodGet:
		return true, h.handleListEvaluationJobs(c)
	case strings.HasPrefix(path, evaluationJobsPrefix+"/") && method == http.MethodGet:
		jobARN, _ := url.PathUnescape(strings.TrimPrefix(path, evaluationJobsPrefix+"/"))

		return true, h.handleGetEvaluationJob(c, jobARN)
	case strings.HasPrefix(path, evaluationJobsPrefix+"/") && method == http.MethodDelete:
		jobARN, _ := url.PathUnescape(strings.TrimPrefix(path, evaluationJobsPrefix+"/"))

		return true, h.handleStopEvaluationJob(c, jobARN)
	default:
		return false, nil
	}
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

func (h *Handler) routeARPVersionAnnotation(c *echo.Context, path, method string, body []byte) (bool, error) {
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

func (h *Handler) routeARPSingleItem(c *echo.Context, path, method string, body []byte) (bool, error) {
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

func (h *Handler) routeCustomModel(
	c *echo.Context,
	path, method string,
	body []byte,
) (bool, error) {
	if method != http.MethodPost {
		return false, nil
	}

	switch path {
	case customModelsCreate:
		return true, h.handleCreateCustomModel(c, body)
	case customModelDeploymentsPath:
		return true, h.handleCreateCustomModelDeployment(c, body)
	case foundationModelAgreement:
		return true, h.handleCreateFoundationModelAgreement(c, body)
	default:
		return false, nil
	}
}

func (h *Handler) writeError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errorResponse("ResourceNotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errorResponse("ConflictException", err.Error()))
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"message": msg, "type": code}
}

// parseBody parses JSON bytes into a value of type T.
func parseBody[T any](body []byte) (*T, error) {
	var v T
	if len(body) == 0 {
		return &v, nil
	}

	if err := json.Unmarshal(body, &v); err != nil {
		return nil, err
	}

	return &v, nil
}

// decodePath URL-decodes a path segment (e.g., ARNs encoded with %3A).
func decodePath(s string) string {
	decoded, err := url.PathUnescape(s)
	if err != nil {
		return s
	}

	return decoded
}

// --- Guardrail handlers ---

type createGuardrailInput struct {
	Name                    string `json:"name"`
	Description             string `json:"description"`
	BlockedInputMessaging   string `json:"blockedInputMessaging"`
	BlockedOutputsMessaging string `json:"blockedOutputsMessaging"`
	Tags                    []Tag  `json:"tags"`
}

type createGuardrailOutput struct {
	CreatedAt    isoTime `json:"createdAt"`
	GuardrailArn string  `json:"guardrailArn"`
	GuardrailID  string  `json:"guardrailId"`
	Version      string  `json:"version"`
}

func (h *Handler) handleCreateGuardrail(c *echo.Context, body []byte) error {
	in, err := parseBody[createGuardrailInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	g, opErr := h.Backend.CreateGuardrail(
		in.Name, in.Description, in.BlockedInputMessaging, in.BlockedOutputsMessaging, in.Tags,
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusOK, createGuardrailOutput{
		GuardrailArn: g.GuardrailArn,
		GuardrailID:  g.GuardrailID,
		Version:      g.Version,
		CreatedAt:    isoTime{g.CreatedAt},
	})
}

type guardrailDetailOutput struct {
	CreatedAt               isoTime `json:"createdAt"`
	UpdatedAt               isoTime `json:"updatedAt"`
	GuardrailID             string  `json:"guardrailId"`
	GuardrailArn            string  `json:"guardrailArn"`
	Name                    string  `json:"name"`
	Description             string  `json:"description"`
	Status                  string  `json:"status"`
	Version                 string  `json:"version"`
	BlockedInputMessaging   string  `json:"blockedInputMessaging"`
	BlockedOutputsMessaging string  `json:"blockedOutputsMessaging"`
	Tags                    []Tag   `json:"tags,omitempty"`
}

func (h *Handler) handleGetGuardrail(c *echo.Context, id string) error {
	g, err := h.Backend.GetGuardrail(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, guardrailDetailOutput{
		GuardrailID:             g.GuardrailID,
		GuardrailArn:            g.GuardrailArn,
		Name:                    g.Name,
		Description:             g.Description,
		Status:                  g.Status,
		Version:                 g.Version,
		BlockedInputMessaging:   g.BlockedInputMessaging,
		BlockedOutputsMessaging: g.BlockedOutputsMessaging,
		Tags:                    g.Tags,
		CreatedAt:               isoTime{g.CreatedAt},
		UpdatedAt:               isoTime{g.UpdatedAt},
	})
}

type guardrailSummaryOutput struct {
	CreatedAt   isoTime `json:"createdAt"`
	UpdatedAt   isoTime `json:"updatedAt"`
	ID          string  `json:"id"`
	Arn         string  `json:"arn"`
	Name        string  `json:"name"`
	Description string  `json:"description,omitempty"`
	Status      string  `json:"status"`
	Version     string  `json:"version"`
}

type listGuardrailsOutput struct {
	NextToken  string                   `json:"nextToken,omitempty"`
	Guardrails []guardrailSummaryOutput `json:"guardrails"`
}

func (h *Handler) handleListGuardrails(c *echo.Context) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	guardrailIdentifier := q.Get("guardrailIdentifier")
	guardrails, outToken := h.Backend.ListGuardrails(nextToken, guardrailIdentifier)
	summaries := make([]guardrailSummaryOutput, 0, len(guardrails))

	for _, g := range guardrails {
		summaries = append(summaries, guardrailSummaryOutput{
			ID:          g.GuardrailID,
			Arn:         g.Arn,
			Name:        g.Name,
			Description: g.Description,
			Status:      g.Status,
			Version:     g.Version,
			CreatedAt:   isoTime{g.CreatedAt},
			UpdatedAt:   isoTime{g.UpdatedAt},
		})
	}

	resp := listGuardrailsOutput{Guardrails: summaries}
	if outToken != "" {
		resp.NextToken = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

type updateGuardrailInput struct {
	Name                    string `json:"name"`
	Description             string `json:"description"`
	BlockedInputMessaging   string `json:"blockedInputMessaging"`
	BlockedOutputsMessaging string `json:"blockedOutputsMessaging"`
}

type updateGuardrailOutput struct {
	UpdatedAt    isoTime `json:"updatedAt"`
	GuardrailArn string  `json:"guardrailArn"`
	GuardrailID  string  `json:"guardrailId"`
	Version      string  `json:"version"`
}

func (h *Handler) handleUpdateGuardrail(c *echo.Context, id string, body []byte) error {
	in, err := parseBody[updateGuardrailInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	g, opErr := h.Backend.UpdateGuardrail(
		id,
		in.Name,
		in.Description,
		in.BlockedInputMessaging,
		in.BlockedOutputsMessaging,
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusOK, updateGuardrailOutput{
		GuardrailArn: g.GuardrailArn,
		GuardrailID:  g.GuardrailID,
		Version:      g.Version,
		UpdatedAt:    isoTime{g.UpdatedAt},
	})
}

func (h *Handler) handleDeleteGuardrail(c *echo.Context, id string) error {
	if err := h.Backend.DeleteGuardrail(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Foundation model handlers ---

type foundationModelSummaryOutput struct {
	ModelArn         string   `json:"modelArn"`
	ModelID          string   `json:"modelId"`
	ModelName        string   `json:"modelName"`
	ProviderName     string   `json:"providerName"`
	InputModalities  []string `json:"inputModalities,omitempty"`
	OutputModalities []string `json:"outputModalities,omitempty"`
}

type listFoundationModelsOutput struct {
	NextToken      string                         `json:"nextToken,omitempty"`
	ModelSummaries []foundationModelSummaryOutput `json:"modelSummaries"`
}

func (h *Handler) handleListFoundationModels(c *echo.Context) error {
	nextToken := c.Request().URL.Query().Get("nextToken")
	models, outToken := h.Backend.ListFoundationModels(nextToken)
	summaries := make([]foundationModelSummaryOutput, 0, len(models))

	for _, m := range models {
		summaries = append(summaries, foundationModelSummaryOutput{
			ModelArn:         m.ModelArn,
			ModelID:          m.ModelID,
			ModelName:        m.ModelName,
			ProviderName:     m.ProviderName,
			InputModalities:  m.InputModalities,
			OutputModalities: m.OutputModalities,
		})
	}

	return c.JSON(
		http.StatusOK,
		listFoundationModelsOutput{ModelSummaries: summaries, NextToken: outToken},
	)
}

type getFoundationModelOutput struct {
	ModelDetails foundationModelSummaryOutput `json:"modelDetails"`
}

func (h *Handler) handleGetFoundationModel(c *echo.Context, modelID string) error {
	m, err := h.Backend.GetFoundationModel(modelID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, getFoundationModelOutput{
		ModelDetails: foundationModelSummaryOutput{
			ModelArn:         m.ModelArn,
			ModelID:          m.ModelID,
			ModelName:        m.ModelName,
			ProviderName:     m.ProviderName,
			InputModalities:  m.InputModalities,
			OutputModalities: m.OutputModalities,
		},
	})
}

// --- Provisioned model throughput handlers ---

type createProvisionedModelThroughputInput struct {
	ProvisionedModelName string `json:"provisionedModelName"`
	ModelID              string `json:"modelId"`
	CommitmentDuration   string `json:"commitmentDuration,omitempty"`
	Tags                 []Tag  `json:"tags"`
	ModelUnits           int32  `json:"modelUnits"`
}

type createProvisionedModelThroughputOutput struct {
	ProvisionedModelArn string `json:"provisionedModelArn"`
}

func (h *Handler) handleCreateProvisionedModelThroughput(c *echo.Context, body []byte) error {
	in, err := parseBody[createProvisionedModelThroughputInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	pmt, opErr := h.Backend.CreateProvisionedModelThroughput(
		in.ProvisionedModelName,
		in.ModelID,
		in.ModelUnits,
		in.CommitmentDuration,
		in.Tags,
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusOK, createProvisionedModelThroughputOutput{
		ProvisionedModelArn: pmt.ProvisionedModelArn,
	})
}

type provisionedModelSummaryOutput struct {
	CreationTime         isoTime `json:"creationTime"`
	LastModifiedTime     isoTime `json:"lastModifiedTime"`
	ProvisionedModelArn  string  `json:"provisionedModelArn"`
	ProvisionedModelName string  `json:"provisionedModelName"`
	ModelArn             string  `json:"modelArn"`
	DesiredModelArn      string  `json:"desiredModelArn"`
	FoundationModelArn   string  `json:"foundationModelArn"`
	Status               string  `json:"status"`
	CommitmentDuration   string  `json:"commitmentDuration,omitempty"`
	ModelUnits           int32   `json:"modelUnits"`
	DesiredModelUnits    int32   `json:"desiredModelUnits"`
}

func pmtToOutput(pmt *ProvisionedModelThroughput) provisionedModelSummaryOutput {
	return provisionedModelSummaryOutput{
		ProvisionedModelArn:  pmt.ProvisionedModelArn,
		ProvisionedModelName: pmt.ProvisionedModelName,
		ModelArn:             pmt.ModelArn,
		DesiredModelArn:      pmt.DesiredModelArn,
		FoundationModelArn:   pmt.FoundationModelArn,
		Status:               pmt.Status,
		ModelUnits:           pmt.ModelUnits,
		DesiredModelUnits:    pmt.DesiredModelUnits,
		CommitmentDuration:   pmt.CommitmentDuration,
		CreationTime:         isoTime{pmt.CreationTime},
		LastModifiedTime:     isoTime{pmt.LastModifiedTime},
	}
}

func (h *Handler) handleGetProvisionedModelThroughput(c *echo.Context, id string) error {
	pmt, err := h.Backend.GetProvisionedModelThroughput(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, pmtToOutput(pmt))
}

type listProvisionedModelThroughputsOutput struct {
	NextToken                 string                          `json:"nextToken,omitempty"`
	ProvisionedModelSummaries []provisionedModelSummaryOutput `json:"provisionedModelSummaries"`
}

func (h *Handler) handleListProvisionedModelThroughputs(c *echo.Context) error {
	nextToken := c.Request().URL.Query().Get("nextToken")
	pmts, outToken := h.Backend.ListProvisionedModelThroughputs(nextToken)
	summaries := make([]provisionedModelSummaryOutput, 0, len(pmts))

	for _, pmt := range pmts {
		summaries = append(summaries, pmtToOutput(pmt))
	}

	return c.JSON(
		http.StatusOK,
		listProvisionedModelThroughputsOutput{
			ProvisionedModelSummaries: summaries,
			NextToken:                 outToken,
		},
	)
}

type updateProvisionedModelThroughputInput struct {
	ModelUnits *int32 `json:"modelUnits,omitempty"`
	ModelID    string `json:"modelId"`
}

func (h *Handler) handleUpdateProvisionedModelThroughput(
	c *echo.Context,
	id string,
	body []byte,
) error {
	in, err := parseBody[updateProvisionedModelThroughputInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	_, opErr := h.Backend.UpdateProvisionedModelThroughput(id, in.ModelID, in.ModelUnits)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteProvisionedModelThroughput(c *echo.Context, id string) error {
	if err := h.Backend.DeleteProvisionedModelThroughput(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Tag handlers ---

type listTagsForResourceInput struct {
	ResourceARN string `json:"resourceARN"`
}

type listTagsForResourceOutput struct {
	Tags []Tag `json:"tags"`
}

func (h *Handler) handleListTagsForResource(c *echo.Context, body []byte) error {
	in, err := parseBody[listTagsForResourceInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	tags, opErr := h.Backend.ListTagsForResource(in.ResourceARN)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	if tags == nil {
		tags = []Tag{}
	}

	return c.JSON(http.StatusOK, listTagsForResourceOutput{Tags: tags})
}

type tagResourceInput struct {
	ResourceARN string `json:"resourceARN"`
	Tags        []Tag  `json:"tags"`
}

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	in, err := parseBody[tagResourceInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	if opErr := h.Backend.TagResource(in.ResourceARN, in.Tags); opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.NoContent(http.StatusOK)
}

type untagResourceInput struct {
	ResourceARN string   `json:"resourceARN"`
	TagKeys     []string `json:"tagKeys"`
}

func (h *Handler) handleUntagResource(c *echo.Context, body []byte) error {
	in, err := parseBody[untagResourceInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	if opErr := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.NoContent(http.StatusOK)
}

// --- EvaluationJob handlers ---

type createEvaluationJobInput struct {
	JobName string `json:"jobName"`
	Tags    []Tag  `json:"tags,omitempty"`
}

type createEvaluationJobOutput struct {
	JobArn string `json:"jobArn"`
}

func (h *Handler) handleCreateEvaluationJob(c *echo.Context, body []byte) error {
	in, err := parseBody[createEvaluationJobInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	job, opErr := h.Backend.CreateEvaluationJob(in.JobName, in.Tags)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, createEvaluationJobOutput{JobArn: job.JobArn})
}

type batchDeleteEvaluationJobInput struct {
	JobIdentifiers []string `json:"jobIdentifiers"`
}

type batchDeleteEvaluationJobOutput struct {
	Errors         []BatchDeleteEvaluationJobError `json:"errors"`
	EvaluationJobs []BatchDeleteEvaluationJobItem  `json:"evaluationJobs"`
}

func (h *Handler) handleBatchDeleteEvaluationJob(c *echo.Context, body []byte) error {
	in, err := parseBody[batchDeleteEvaluationJobInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	errs, deleted, opErr := h.Backend.BatchDeleteEvaluationJob(in.JobIdentifiers)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(
		http.StatusOK,
		batchDeleteEvaluationJobOutput{Errors: errs, EvaluationJobs: deleted},
	)
}

// --- AutomatedReasoningPolicy handlers ---

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

// --- CustomModel handlers ---

type createCustomModelInput struct {
	ModelName string `json:"modelName"`
	Tags      []Tag  `json:"tags,omitempty"`
}

type createCustomModelOutput struct {
	ModelArn string `json:"modelArn"`
}

func (h *Handler) handleCreateCustomModel(c *echo.Context, body []byte) error {
	in, err := parseBody[createCustomModelInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	model, opErr := h.Backend.CreateCustomModel(in.ModelName, in.Tags)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, createCustomModelOutput{ModelArn: model.ModelArn})
}

// --- CustomModelDeployment handlers ---

type createCustomModelDeploymentInput struct {
	ModelArn            string `json:"modelArn"`
	ModelDeploymentName string `json:"modelDeploymentName"`
	Tags                []Tag  `json:"tags,omitempty"`
}

type createCustomModelDeploymentOutput struct {
	CustomModelDeploymentArn string `json:"customModelDeploymentArn"`
}

func (h *Handler) handleCreateCustomModelDeployment(c *echo.Context, body []byte) error {
	in, err := parseBody[createCustomModelDeploymentInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	deployment, opErr := h.Backend.CreateCustomModelDeployment(
		in.ModelArn,
		in.ModelDeploymentName,
		in.Tags,
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, createCustomModelDeploymentOutput{
		CustomModelDeploymentArn: deployment.CustomModelDeploymentArn,
	})
}

// --- FoundationModelAgreement handlers ---

type createFoundationModelAgreementInput struct {
	ModelID    string `json:"modelId"`
	OfferToken string `json:"offerToken"`
}

type createFoundationModelAgreementOutput struct {
	ModelID string `json:"modelId"`
}

func (h *Handler) handleCreateFoundationModelAgreement(c *echo.Context, body []byte) error {
	in, err := parseBody[createFoundationModelAgreementInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	agreement, opErr := h.Backend.CreateFoundationModelAgreement(in.ModelID)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusOK, createFoundationModelAgreementOutput{ModelID: agreement.ModelID})
}

// --- GuardrailVersion handlers ---

type createGuardrailVersionInput struct {
	Description        string `json:"description,omitempty"`
	ClientRequestToken string `json:"clientRequestToken,omitempty"`
}

type createGuardrailVersionOutput struct {
	GuardrailID string `json:"guardrailId"`
	Version     string `json:"version"`
}

func extractCustomModelListOperation(path, method string) (string, bool) {
	isSubPath := strings.HasPrefix(path, customModelsPrefix+"/")

	switch {
	case path == customModelsPrefix && method == http.MethodGet:
		return "ListCustomModels", true
	case isSubPath && method == http.MethodGet:
		return "GetCustomModel", true
	case isSubPath && method == http.MethodDelete:
		return "DeleteCustomModel", true
	default:
		return "", false
	}
}

func extractCustomizationJobOperation(path, method string) (string, bool) {
	isSubPath := strings.HasPrefix(path, modelCustomizationJobsPrefix+"/")
	isStop := isSubPath && strings.HasSuffix(path, "/stop")

	switch {
	case path == modelCustomizationJobsPrefix && method == http.MethodPost:
		return "CreateModelCustomizationJob", true
	case path == modelCustomizationJobsPrefix && method == http.MethodGet:
		return "ListModelCustomizationJobs", true
	case isSubPath && method == http.MethodGet && !isStop:
		return "GetModelCustomizationJob", true
	case isStop && method == http.MethodPost:
		return "StopModelCustomizationJob", true
	default:
		return "", false
	}
}

func extractInferenceProfileOperation(path, method string) (string, bool) {
	switch {
	case path == inferenceProfilesPrefix && method == http.MethodPost:
		return "CreateInferenceProfile", true
	case path == inferenceProfilesPrefix && method == http.MethodGet:
		return "ListInferenceProfiles", true
	case strings.HasPrefix(path, inferenceProfilesPrefix+"/") && method == http.MethodGet:
		return "GetInferenceProfile", true
	case strings.HasPrefix(path, inferenceProfilesPrefix+"/") && method == http.MethodDelete:
		return "DeleteInferenceProfile", true
	default:
		return "", false
	}
}

func extractMarketplaceEndpointOperation(path, method string) (string, bool) {
	if path == marketplaceEndpointsPrefix {
		return extractMarketplaceEndpointRootOp(method)
	}

	if !strings.HasPrefix(path, marketplaceEndpointsPrefix+"/") {
		return "", false
	}

	return extractMarketplaceEndpointSubOp(path, method)
}

func extractMarketplaceEndpointRootOp(method string) (string, bool) {
	switch method {
	case http.MethodPost:
		return "CreateMarketplaceModelEndpoint", true
	case http.MethodGet:
		return "ListMarketplaceModelEndpoints", true
	default:
		return "", false
	}
}

func extractMarketplaceEndpointSubOp(path, method string) (string, bool) {
	isReg := strings.HasSuffix(path, "/registration")
	isDereg := strings.HasSuffix(path, "/deregistration")

	switch {
	case method == http.MethodPost && isReg:
		return "RegisterMarketplaceModelEndpoint", true
	case method == http.MethodPost && isDereg:
		return "DeregisterMarketplaceModelEndpoint", true
	case method == http.MethodGet && !isReg && !isDereg:
		return "GetMarketplaceModelEndpoint", true
	case method == http.MethodPut:
		return "UpdateMarketplaceModelEndpoint", true
	case method == http.MethodDelete:
		return "DeleteMarketplaceModelEndpoint", true
	default:
		return "", false
	}
}

func extractLoggingConfigOperation(path, method string) (string, bool) {
	switch {
	case path == loggingConfigPath && method == http.MethodGet:
		return "GetModelInvocationLoggingConfiguration", true
	case path == loggingConfigPath && method == http.MethodPut:
		return "PutModelInvocationLoggingConfiguration", true
	case path == loggingConfigPath && method == http.MethodDelete:
		return "DeleteModelInvocationLoggingConfiguration", true
	default:
		return "", false
	}
}

func (h *Handler) routeCustomModelList(c *echo.Context, path, method string) (bool, error) {
	isSubPath := strings.HasPrefix(path, customModelsPrefix+"/")

	switch {
	case path == customModelsPrefix && method == http.MethodGet:
		return true, h.handleListCustomModels(c)
	case isSubPath && method == http.MethodGet:
		id := decodePath(strings.TrimPrefix(path, customModelsPrefix+"/"))

		return true, h.handleGetCustomModel(c, id)
	case isSubPath && method == http.MethodDelete:
		id := decodePath(strings.TrimPrefix(path, customModelsPrefix+"/"))

		return true, h.handleDeleteCustomModel(c, id)
	default:
		return false, nil
	}
}

func (h *Handler) routeCustomizationJob(
	c *echo.Context,
	path, method string,
	body []byte,
) (bool, error) {
	isSubPath := strings.HasPrefix(path, modelCustomizationJobsPrefix+"/")
	isStop := isSubPath && strings.HasSuffix(path, "/stop")

	switch {
	case path == modelCustomizationJobsPrefix && method == http.MethodPost:
		return true, h.handleCreateModelCustomizationJob(c, body)
	case path == modelCustomizationJobsPrefix && method == http.MethodGet:
		return true, h.handleListModelCustomizationJobs(c)
	case isSubPath && method == http.MethodGet && !isStop:
		id := decodePath(strings.TrimPrefix(path, modelCustomizationJobsPrefix+"/"))

		return true, h.handleGetModelCustomizationJob(c, id)
	case isStop && method == http.MethodPost:
		rest := strings.TrimPrefix(path, modelCustomizationJobsPrefix+"/")
		id := decodePath(strings.TrimSuffix(rest, "/stop"))

		return true, h.handleStopModelCustomizationJob(c, id)
	default:
		return false, nil
	}
}

func (h *Handler) routeInferenceProfile(
	c *echo.Context,
	path, method string,
	body []byte,
) (bool, error) {
	switch {
	case path == inferenceProfilesPrefix && method == http.MethodPost:
		return true, h.handleCreateInferenceProfile(c, body)
	case path == inferenceProfilesPrefix && method == http.MethodGet:
		return true, h.handleListInferenceProfiles(c)
	case strings.HasPrefix(path, inferenceProfilesPrefix+"/") && method == http.MethodGet:
		id := decodePath(strings.TrimPrefix(path, inferenceProfilesPrefix+"/"))

		return true, h.handleGetInferenceProfile(c, id)
	case strings.HasPrefix(path, inferenceProfilesPrefix+"/") && method == http.MethodDelete:
		id := decodePath(strings.TrimPrefix(path, inferenceProfilesPrefix+"/"))

		return true, h.handleDeleteInferenceProfile(c, id)
	default:
		return false, nil
	}
}

func (h *Handler) routeMarketplaceEndpoint(
	c *echo.Context,
	path, method string,
	body []byte,
) (bool, error) {
	if path == marketplaceEndpointsPrefix {
		return h.routeMarketplaceEndpointRoot(c, method, body)
	}

	if !strings.HasPrefix(path, marketplaceEndpointsPrefix+"/") {
		return false, nil
	}

	return h.routeMarketplaceEndpointSub(c, path, method)
}

func (h *Handler) routeMarketplaceEndpointRoot(
	c *echo.Context, method string, body []byte,
) (bool, error) {
	switch method {
	case http.MethodPost:
		return true, h.handleCreateMarketplaceModelEndpoint(c, body)
	case http.MethodGet:
		return true, h.handleListMarketplaceModelEndpoints(c)
	default:
		return false, nil
	}
}

func (h *Handler) routeMarketplaceEndpointSub(c *echo.Context, path, method string) (bool, error) {
	rest := strings.TrimPrefix(path, marketplaceEndpointsPrefix+"/")

	switch {
	case method == http.MethodPost && strings.HasSuffix(path, "/registration"):
		id := decodePath(strings.TrimSuffix(rest, "/registration"))

		return true, h.handleRegisterMarketplaceModelEndpoint(c, id)
	case method == http.MethodPost && strings.HasSuffix(path, "/deregistration"):
		id := decodePath(strings.TrimSuffix(rest, "/deregistration"))

		return true, h.handleDeregisterMarketplaceModelEndpoint(c, id)
	case method == http.MethodGet:
		return true, h.handleGetMarketplaceModelEndpoint(c, decodePath(rest))
	case method == http.MethodPut:
		return true, h.handleUpdateMarketplaceModelEndpoint(c, decodePath(rest))
	case method == http.MethodDelete:
		return true, h.handleDeleteMarketplaceModelEndpoint(c, decodePath(rest))
	default:
		return false, nil
	}
}

func (h *Handler) routeLoggingConfig(
	c *echo.Context,
	path, method string,
	body []byte,
) (bool, error) {
	if path != loggingConfigPath {
		return false, nil
	}

	switch method {
	case http.MethodGet:
		return true, h.handleGetModelInvocationLoggingConfiguration(c)
	case http.MethodPut:
		return true, h.handlePutModelInvocationLoggingConfiguration(c, body)
	case http.MethodDelete:
		return true, h.handleDeleteModelInvocationLoggingConfiguration(c)
	default:
		return false, nil
	}
}

// --- GetCustomModel / ListCustomModels / DeleteCustomModel handlers ---

type customModelOutput struct {
	CreationTime string `json:"creationTime"`
	ModelArn     string `json:"modelArn"`
	ModelName    string `json:"modelName"`
	Tags         []Tag  `json:"tags,omitempty"`
}

func customModelToOutput(m *CustomModel) customModelOutput {
	return customModelOutput{
		ModelArn:     m.ModelArn,
		ModelName:    m.ModelName,
		CreationTime: m.CreationTime.Format(time.RFC3339),
		Tags:         m.Tags,
	}
}

func (h *Handler) handleGetCustomModel(c *echo.Context, id string) error {
	m, err := h.Backend.GetCustomModel(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, customModelToOutput(m))
}

type listCustomModelsOutput struct {
	NextToken      string              `json:"nextToken,omitempty"`
	ModelSummaries []customModelOutput `json:"modelSummaries"`
}

func (h *Handler) handleListCustomModels(c *echo.Context) error {
	nextToken := c.Request().URL.Query().Get("nextToken")
	models, outToken := h.Backend.ListCustomModels(nextToken)
	summaries := make([]customModelOutput, 0, len(models))

	for _, m := range models {
		summaries = append(summaries, customModelToOutput(m))
	}

	return c.JSON(
		http.StatusOK,
		listCustomModelsOutput{ModelSummaries: summaries, NextToken: outToken},
	)
}

func (h *Handler) handleDeleteCustomModel(c *echo.Context, id string) error {
	if err := h.Backend.DeleteCustomModel(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- ModelCustomizationJob handlers ---

type createModelCustomizationJobInput struct {
	JobName             string `json:"jobName"`
	BaseModelIdentifier string `json:"baseModelIdentifier"`
	CustomizationType   string `json:"customizationType,omitempty"`
	Tags                []Tag  `json:"tags,omitempty"`
}

type createModelCustomizationJobOutput struct {
	JobArn string `json:"jobArn"`
}

func (h *Handler) handleCreateModelCustomizationJob(c *echo.Context, body []byte) error {
	in, err := parseBody[createModelCustomizationJobInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	job, opErr := h.Backend.CreateModelCustomizationJob(
		in.JobName, in.BaseModelIdentifier, in.CustomizationType, in.Tags,
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, createModelCustomizationJobOutput{JobArn: job.JobArn})
}

type modelCustomizationJobOutput struct {
	CreationTime      string `json:"creationTime"`
	LastModifiedTime  string `json:"lastModifiedTime"`
	JobArn            string `json:"jobArn"`
	JobName           string `json:"jobName"`
	BaseModelArn      string `json:"baseModelArn"`
	OutputModelArn    string `json:"outputModelArn"`
	Status            string `json:"status"`
	CustomizationType string `json:"customizationType,omitempty"`
	Tags              []Tag  `json:"tags,omitempty"`
}

func customizationJobToOutput(j *ModelCustomizationJob) modelCustomizationJobOutput {
	return modelCustomizationJobOutput{
		JobArn:            j.JobArn,
		JobName:           j.JobName,
		BaseModelArn:      j.BaseModelArn,
		OutputModelArn:    j.OutputModelArn,
		Status:            j.Status,
		CustomizationType: j.CustomizationType,
		CreationTime:      j.CreationTime.Format(time.RFC3339),
		LastModifiedTime:  j.LastModifiedTime.Format(time.RFC3339),
		Tags:              j.Tags,
	}
}

func (h *Handler) handleGetModelCustomizationJob(c *echo.Context, id string) error {
	job, err := h.Backend.GetModelCustomizationJob(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, customizationJobToOutput(job))
}

type listModelCustomizationJobsOutput struct {
	NextToken                      string                        `json:"nextToken,omitempty"`
	ModelCustomizationJobSummaries []modelCustomizationJobOutput `json:"modelCustomizationJobSummaries"`
}

func (h *Handler) handleListModelCustomizationJobs(c *echo.Context) error {
	nextToken := c.Request().URL.Query().Get("nextToken")
	jobs, outToken := h.Backend.ListModelCustomizationJobs(nextToken)
	summaries := make([]modelCustomizationJobOutput, 0, len(jobs))

	for _, j := range jobs {
		summaries = append(summaries, customizationJobToOutput(j))
	}

	return c.JSON(http.StatusOK, listModelCustomizationJobsOutput{
		ModelCustomizationJobSummaries: summaries,
		NextToken:                      outToken,
	})
}

func (h *Handler) handleStopModelCustomizationJob(c *echo.Context, id string) error {
	if err := h.Backend.StopModelCustomizationJob(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- InferenceProfile handlers ---

type createInferenceProfileInput struct {
	InferenceProfileName string `json:"inferenceProfileName"`
	Description          string `json:"description,omitempty"`
	Tags                 []Tag  `json:"tags,omitempty"`
}

type createInferenceProfileOutput struct {
	InferenceProfileArn string `json:"inferenceProfileArn"`
	Status              string `json:"status"`
}

func (h *Handler) handleCreateInferenceProfile(c *echo.Context, body []byte) error {
	in, err := parseBody[createInferenceProfileInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	profile, opErr := h.Backend.CreateInferenceProfile(
		in.InferenceProfileName,
		in.Description,
		in.Tags,
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, createInferenceProfileOutput{
		InferenceProfileArn: profile.InferenceProfileArn,
		Status:              profile.Status,
	})
}

type inferenceProfileOutput struct {
	CreatedAt            string `json:"createdAt"`
	UpdatedAt            string `json:"updatedAt"`
	InferenceProfileArn  string `json:"inferenceProfileArn"`
	InferenceProfileID   string `json:"inferenceProfileId"`
	InferenceProfileName string `json:"inferenceProfileName"`
	Status               string `json:"status"`
	Type                 string `json:"type"`
	Description          string `json:"description,omitempty"`
}

func inferenceProfileToOutput(p *InferenceProfile) inferenceProfileOutput {
	return inferenceProfileOutput{
		InferenceProfileArn:  p.InferenceProfileArn,
		InferenceProfileID:   p.InferenceProfileID,
		InferenceProfileName: p.InferenceProfileName,
		Status:               p.Status,
		Type:                 p.Type,
		Description:          p.Description,
		CreatedAt:            p.CreatedAt.Format(time.RFC3339),
		UpdatedAt:            p.UpdatedAt.Format(time.RFC3339),
	}
}

func (h *Handler) handleGetInferenceProfile(c *echo.Context, id string) error {
	profile, err := h.Backend.GetInferenceProfile(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, inferenceProfileToOutput(profile))
}

type listInferenceProfilesOutput struct {
	NextToken                 string                   `json:"nextToken,omitempty"`
	InferenceProfileSummaries []inferenceProfileOutput `json:"inferenceProfileSummaries"`
}

func (h *Handler) handleListInferenceProfiles(c *echo.Context) error {
	nextToken := c.Request().URL.Query().Get("nextToken")
	profiles, outToken := h.Backend.ListInferenceProfiles(nextToken)
	summaries := make([]inferenceProfileOutput, 0, len(profiles))

	for _, p := range profiles {
		summaries = append(summaries, inferenceProfileToOutput(p))
	}

	return c.JSON(http.StatusOK, listInferenceProfilesOutput{
		InferenceProfileSummaries: summaries,
		NextToken:                 outToken,
	})
}

func (h *Handler) handleDeleteInferenceProfile(c *echo.Context, id string) error {
	if err := h.Backend.DeleteInferenceProfile(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- MarketplaceModelEndpoint handlers ---

type createMarketplaceModelEndpointInput struct {
	EndpointName          string `json:"endpointName"`
	ModelSourceIdentifier string `json:"modelSourceIdentifier"`
	Tags                  []Tag  `json:"tags,omitempty"`
}

type createMarketplaceModelEndpointOutput struct {
	MarketplaceModelEndpoint marketplaceEndpointOutput `json:"marketplaceModelEndpoint"`
}

type marketplaceEndpointOutput struct {
	CreatedAt             string `json:"createdAt"`
	UpdatedAt             string `json:"updatedAt"`
	EndpointArn           string `json:"endpointArn"`
	EndpointName          string `json:"endpointName"`
	ModelSourceIdentifier string `json:"modelSourceIdentifier"`
	Status                string `json:"status"`
}

func marketplaceEndpointToOutput(ep *MarketplaceModelEndpoint) marketplaceEndpointOutput {
	return marketplaceEndpointOutput{
		EndpointArn:           ep.EndpointArn,
		EndpointName:          ep.EndpointName,
		ModelSourceIdentifier: ep.ModelSourceID,
		Status:                ep.Status,
		CreatedAt:             ep.CreatedAt.Format(time.RFC3339),
		UpdatedAt:             ep.UpdatedAt.Format(time.RFC3339),
	}
}

func (h *Handler) handleCreateMarketplaceModelEndpoint(c *echo.Context, body []byte) error {
	in, err := parseBody[createMarketplaceModelEndpointInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	ep, opErr := h.Backend.CreateMarketplaceModelEndpoint(
		in.EndpointName,
		in.ModelSourceIdentifier,
		in.Tags,
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, createMarketplaceModelEndpointOutput{
		MarketplaceModelEndpoint: marketplaceEndpointToOutput(ep),
	})
}

func (h *Handler) handleGetMarketplaceModelEndpoint(c *echo.Context, id string) error {
	ep, err := h.Backend.GetMarketplaceModelEndpoint(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, marketplaceEndpointToOutput(ep))
}

type listMarketplaceModelEndpointsOutput struct {
	NextToken                 string                      `json:"nextToken,omitempty"`
	MarketplaceModelEndpoints []marketplaceEndpointOutput `json:"marketplaceModelEndpoints"`
}

func (h *Handler) handleListMarketplaceModelEndpoints(c *echo.Context) error {
	nextToken := c.Request().URL.Query().Get("nextToken")
	endpoints, outToken := h.Backend.ListMarketplaceModelEndpoints(nextToken)
	summaries := make([]marketplaceEndpointOutput, 0, len(endpoints))

	for _, ep := range endpoints {
		summaries = append(summaries, marketplaceEndpointToOutput(ep))
	}

	return c.JSON(http.StatusOK, listMarketplaceModelEndpointsOutput{
		MarketplaceModelEndpoints: summaries,
		NextToken:                 outToken,
	})
}

func (h *Handler) handleDeleteMarketplaceModelEndpoint(c *echo.Context, id string) error {
	if err := h.Backend.DeleteMarketplaceModelEndpoint(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUpdateMarketplaceModelEndpoint(c *echo.Context, id string) error {
	ep, err := h.Backend.UpdateMarketplaceModelEndpoint(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, marketplaceEndpointToOutput(ep))
}

func (h *Handler) handleRegisterMarketplaceModelEndpoint(c *echo.Context, id string) error {
	if err := h.Backend.RegisterMarketplaceModelEndpoint(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeregisterMarketplaceModelEndpoint(c *echo.Context, id string) error {
	if err := h.Backend.DeregisterMarketplaceModelEndpoint(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- ModelInvocationLoggingConfiguration handlers ---

type modelInvocationLoggingConfigOutput struct {
	LoggingConfig *ModelInvocationLoggingConfiguration `json:"loggingConfig,omitempty"`
}

func (h *Handler) handleGetModelInvocationLoggingConfiguration(c *echo.Context) error {
	cfg := h.Backend.GetModelInvocationLoggingConfiguration()

	return c.JSON(http.StatusOK, modelInvocationLoggingConfigOutput{LoggingConfig: cfg})
}

func (h *Handler) handlePutModelInvocationLoggingConfiguration(c *echo.Context, body []byte) error {
	in, err := parseBody[ModelInvocationLoggingConfiguration](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	h.Backend.PutModelInvocationLoggingConfiguration(in)

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteModelInvocationLoggingConfiguration(c *echo.Context) error {
	h.Backend.DeleteModelInvocationLoggingConfiguration()

	return c.NoContent(http.StatusOK)
}

// --- GuardrailVersion handlers ---

func (h *Handler) handleCreateGuardrailVersion(c *echo.Context, id string, body []byte) error {
	in, err := parseBody[createGuardrailVersionInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	gv, opErr := h.Backend.CreateGuardrailVersion(id, in.Description)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusOK, createGuardrailVersionOutput{
		GuardrailID: gv.GuardrailID,
		Version:     gv.Version,
	})
}
