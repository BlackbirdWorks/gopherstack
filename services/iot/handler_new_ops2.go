package iot

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// String constants used in handler_new_ops2 responses.
const (
	keyTaskID              = "taskId"
	keyTaskStatus          = "taskStatus"
	keyTasks               = "tasks"
	keyThingGroups         = "thingGroups"
	keyPrincipals          = "principals"
	keyVersionID           = "versionId"
	pathAuditConfiguration = "/audit/configuration"
	certStatusPendingXfer  = "PENDING_TRANSFER"
	indexStatusActive      = "ACTIVE"
	taskStatusCanceled     = "CANCELED"
)

// ---------------------------------------------------------------------------
// Route resolver for remaining (previously-stub) operations
// ---------------------------------------------------------------------------

func resolveRemainingOps(path, method string) string {
	if op := resolveRemainingDetectOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveRemainingIndexOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveRemainingCertAndThingOps(path, method); op != unknownOperation {
		return op
	}

	return resolveRemainingMiscOps(path, method)
}

//nolint:cyclop // mechanical routing switch
func resolveRemainingDetectOps(path, method string) string {
	switch {
	case strings.HasPrefix(path, "/detect/mitigationactions/tasks/") &&
		strings.HasSuffix(path, "/cancel") && method == http.MethodPut:
		return opCancelDetectMitigationActionsTask
	case path == "/detect/mitigationactions/tasks" && method == http.MethodPost:
		return opStartDetectMitigationActionsTask
	case strings.HasPrefix(path, "/detect/mitigationactions/tasks/") && method == http.MethodGet:
		return opDescribeDetectMitigationActionsTask
	case path == "/detect/mitigationactions/tasks" && method == http.MethodGet:
		return opListDetectMitigationActionsTasks
	case path == "/detect/mitigationactions/executions" && method == http.MethodGet:
		return opListDetectMitigationActionsExecutions
	case path == "/audit/mitigationactions/tasks" && method == http.MethodPost:
		return opStartAuditMitigationActionsTask
	case path == "/audit/mitigationactions/tasks" && method == http.MethodGet:
		return opListAuditMitigationActionsTasks
	case strings.HasPrefix(path, "/audit/mitigationactions/tasks/") && method == http.MethodGet:
		return opDescribeAuditMitigationActionsTask
	case path == "/audit/mitigationactions/executions" && method == http.MethodGet:
		return opListAuditMitigationActionsExecutions
	case path == "/active-violations" && method == http.MethodGet:
		return opListActiveViolations
	case path == "/violation-events" && method == http.MethodGet:
		return opListViolationEvents
	case strings.HasPrefix(path, "/violations/verification-state/") && method == http.MethodPatch:
		return opPutVerificationStateOnViolation
	case path == "/behavior-model-training/summaries" && method == http.MethodGet:
		return opGetBehaviorModelTrainingSummaries
	case path == "/security-profile-behaviors/validate" && method == http.MethodPost:
		return opValidateSecurityProfileBehaviors
	}

	return unknownOperation
}

//nolint:cyclop // mechanical routing switch
func resolveRemainingIndexOps(path, method string) string {
	switch {
	case path == "/indexing/config" && method == http.MethodGet:
		return opGetIndexingConfiguration
	case path == "/indexing/config" && method == http.MethodPost:
		return opUpdateIndexingConfiguration
	case path == "/indices" && method == http.MethodGet:
		return opListIndices
	case path == "/indices/search" && method == http.MethodPost:
		return opSearchIndex
	case path == "/indices/cardinality" && method == http.MethodPost:
		return opGetCardinality
	case path == "/indices/statistics" && method == http.MethodPost:
		return opGetStatistics
	case path == "/indices/percentiles" && method == http.MethodPost:
		return opGetPercentiles
	case path == "/indices/buckets-aggregation" && method == http.MethodPost:
		return opGetBucketsAggregation
	case strings.HasPrefix(path, "/indices/") && method == http.MethodGet:
		return opDescribeIndex
	case path == "/metric-values" && method == http.MethodGet:
		return opListMetricValues
	}

	return unknownOperation
}

//nolint:cyclop // mechanical routing switch
func resolveRemainingCertAndThingOps(path, method string) string {
	switch {
	case path == "/certificates-out-going" && method == http.MethodGet:
		return opListOutgoingCertificates
	case path == "/encryption-configuration" && method == http.MethodGet:
		return opDescribeEncryptionConfiguration
	case path == "/encryption-configuration" && method == http.MethodPatch:
		return opUpdateEncryptionConfiguration
	case path == "/test-authorization" && method == http.MethodPost:
		return opTestAuthorization
	case strings.HasPrefix(path, "/authorizer/") &&
		strings.HasSuffix(path, "/test") && method == http.MethodPost:
		return opTestInvokeAuthorizer
	case strings.HasPrefix(path, "/principal-policies/") && method == http.MethodDelete:
		return opDetachPrincipalPolicy
	case strings.HasPrefix(path, "/confirmdestination/") && method == http.MethodGet:
		return opConfirmTopicRuleDestination
	case strings.HasPrefix(path, "/things/") &&
		strings.HasSuffix(path, "/connectivity-data") && method == http.MethodGet:
		return opGetThingConnectivityData
	case strings.HasPrefix(path, "/things/") &&
		strings.HasSuffix(path, "/principals/v2") && method == http.MethodGet:
		return opListThingPrincipalsV2
	case path == "/thing-groups/updateThingGroupsForThing" && method == http.MethodPut:
		return opUpdateThingGroupsForThing
	case path == "/things/register" && method == http.MethodPost:
		return opRegisterThing
	}

	return unknownOperation
}

//nolint:cyclop // mechanical routing switch
func resolveRemainingMiscOps(path, method string) string {
	switch {
	case path == "/thing-registration-tasks" && method == http.MethodPost:
		return opStartThingRegistrationTask
	case path == "/thing-registration-tasks" && method == http.MethodGet:
		return opListThingRegistrationTasks
	case strings.HasPrefix(path, "/thing-registration-tasks/") &&
		strings.HasSuffix(path, "/reports") && method == http.MethodGet:
		return opListThingRegistrationTaskReports
	case strings.HasPrefix(path, "/thing-registration-tasks/") && method == http.MethodPut:
		return opStopThingRegistrationTask
	case strings.HasPrefix(path, "/thing-registration-tasks/") && method == http.MethodGet:
		return opDescribeThingRegistrationTask
	case path == "/managed-job-templates" && method == http.MethodGet:
		return opListManagedJobTemplates
	case strings.HasPrefix(path, "/managed-job-templates/") && method == http.MethodGet:
		return opDescribeManagedJobTemplate
	case path == pathAuditConfiguration && method == http.MethodDelete:
		return opDeleteAccountAuditConfiguration
	case path == "/audit/relatedResources" && method == http.MethodGet:
		return opListRelatedResourcesForAuditFinding
	case strings.HasPrefix(path, "/packages/") &&
		strings.HasSuffix(path, "/sbom") && method == http.MethodDelete:
		return opDisassociateSbomFromPackageVersion
	case strings.HasPrefix(path, "/packages/") &&
		strings.HasSuffix(path, "/sbom-validation-results") && method == http.MethodGet:
		return opListSbomValidationResults
	}

	return unknownOperation
}

// ---------------------------------------------------------------------------
// Handler implementations — remaining ops
// ---------------------------------------------------------------------------

// --- Detect / Audit Mitigation (stateless returns) ---

func (h *Handler) handleCancelDetectMitigationActionsTask(c *echo.Context) error {
	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleStartDetectMitigationActionsTask(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{keyTaskID: uuid.NewString()})
}

func (h *Handler) handleDescribeDetectMitigationActionsTask(c *echo.Context) error {
	taskID := strings.TrimPrefix(c.Request().URL.Path, "/detect/mitigationactions/tasks/")

	return c.JSON(http.StatusOK, map[string]any{
		keyTaskID:     taskID,
		keyTaskStatus: taskStatusCanceled,
	})
}

func (h *Handler) handleListDetectMitigationActionsTasks(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{keyTasks: []any{}})
}

func (h *Handler) handleListDetectMitigationActionsExecutions(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"actionsExecutions": []any{}})
}

func (h *Handler) handleStartAuditMitigationActionsTask(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{keyTaskID: uuid.NewString()})
}

func (h *Handler) handleDescribeAuditMitigationActionsTask(c *echo.Context) error {
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/audit/mitigationactions/tasks/")
	taskID := strings.TrimSuffix(trimmed, "/cancel")

	return c.JSON(http.StatusOK, map[string]any{
		keyTaskID:     taskID,
		keyTaskStatus: taskStatusCanceled,
	})
}

func (h *Handler) handleListAuditMitigationActionsTasks(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{keyTasks: []any{}})
}

func (h *Handler) handleListAuditMitigationActionsExecutions(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"actionsExecutions": []any{}})
}

// --- Violations ---

func (h *Handler) handleListActiveViolations(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"activeViolations": []any{}})
}

func (h *Handler) handleListViolationEvents(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"violationEvents": []any{}})
}

func (h *Handler) handlePutVerificationStateOnViolation(c *echo.Context) error {
	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetBehaviorModelTrainingSummaries(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"summaries": []any{}})
}

func (h *Handler) handleValidateSecurityProfileBehaviors(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"valid": true, "validationErrors": []any{}})
}

// --- Indexing ---

func (h *Handler) handleGetIndexingConfiguration(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{
		"thingIndexingConfiguration": map[string]any{
			"thingIndexingMode": "OFF",
		},
		"thingGroupIndexingConfiguration": map[string]any{
			"thingGroupIndexingMode": "OFF",
		},
	})
}

func (h *Handler) handleUpdateIndexingConfiguration(c *echo.Context) error {
	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListIndices(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"indexNames": []string{"AWS_Things"}})
}

func (h *Handler) handleDescribeIndex(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/indices/")

	return c.JSON(http.StatusOK, map[string]any{
		"indexName":   name,
		"indexStatus": indexStatusActive,
		"schema":      "",
	})
}

func (h *Handler) handleSearchIndex(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{keyThings: []any{}, keyThingGroups: []any{}})
}

func (h *Handler) handleGetCardinality(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"cardinality": 0})
}

func (h *Handler) handleGetStatistics(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{
		"statistics": map[string]any{"count": 0},
	})
}

func (h *Handler) handleGetPercentiles(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"percentiles": []any{}})
}

func (h *Handler) handleGetBucketsAggregation(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"buckets": []any{}, "totalCount": 0})
}

func (h *Handler) handleListMetricValues(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"metricDatumList": []any{}})
}

// --- Certificates / Auth ---

func (h *Handler) handleListOutgoingCertificates(c *echo.Context) error {
	certs := h.Backend.ListCertificates()
	var out []map[string]any
	for _, cert := range certs {
		if cert.Status == certStatusPendingXfer {
			out = append(out, map[string]any{
				"certificateArn": cert.ARN,
				"certificateId":  cert.CertificateID,
				"status":         cert.Status,
				keyCreationDate:  cert.CreatedAt,
			})
		}
	}
	if out == nil {
		out = []map[string]any{}
	}

	return c.JSON(http.StatusOK, map[string]any{"outgoingCertificates": out})
}

func (h *Handler) handleDescribeEncryptionConfiguration(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"keyType": "NONE"})
}

func (h *Handler) handleUpdateEncryptionConfiguration(c *echo.Context) error {
	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleTestAuthorization(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{
		"allowed": []map[string]any{
			{keyPolicyName: "", "allowed": true},
		},
	})
}

func (h *Handler) handleTestInvokeAuthorizer(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{
		"isAuthenticated":   true,
		"principalId":       "test-principal",
		"disconnectAfterMs": 0,
		"refreshAfterMs":    0,
	})
}

func (h *Handler) handleDetachPrincipalPolicy(c *echo.Context) error {
	// DELETE /principal-policies/{policyName}?principal=xxx
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/principal-policies/")
	target := c.Request().URL.Query().Get("principal")
	if err := h.Backend.DetachPolicy(&DetachPolicyInput{
		PolicyName: policyName,
		Target:     target,
	}); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleConfirmTopicRuleDestination(c *echo.Context) error {
	// GET /confirmdestination/{confirmationToken}
	// Marks a destination as confirmed (ENABLED).
	// token is used as destination ARN key lookup; in practice, update any PENDING_CONFIRMATION dest.
	// We just return 200 OK — the confirmation flow requires a real HTTP roundtrip in AWS.
	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetThingConnectivityData(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{
		keyThingName:       "",
		"connected":        false,
		keyTimestamp:       0,
		"disconnectReason": "AUTH_ERROR",
		"disconnectedFrom": "",
		"mqttClientId":     "",
	})
}

func (h *Handler) handleListThingPrincipalsV2(c *echo.Context) error {
	// GET /things/{thingName}/principals/v2
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/things/")
	thingName := strings.TrimSuffix(trimmed, "/principals/v2")
	principals, err := h.Backend.ListThingPrincipals(thingName)
	if err != nil {
		return respondErr(c, err)
	}
	summaries := make([]map[string]any, len(principals))
	for i, p := range principals {
		summaries[i] = map[string]any{
			"principal":          p,
			"thingPrincipalType": "EXCLUSIVE_GROUP",
		}
	}

	return c.JSON(http.StatusOK, map[string]any{keyPrincipals: summaries})
}

func (h *Handler) handleUpdateThingGroupsForThing(c *echo.Context) error {
	var req struct {
		ThingName           string   `json:"thingName"`
		ThingGroupsToAdd    []string `json:"thingGroupsToAdd"`
		ThingGroupsToRemove []string `json:"thingGroupsToRemove"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.UpdateThingGroupsForThing(
		req.ThingName, req.ThingGroupsToAdd, req.ThingGroupsToRemove,
	); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleRegisterThing(c *echo.Context) error {
	var req struct {
		Parameters   map[string]any `json:"parameters"`
		TemplateName string         `json:"templateName"`
	}
	_ = readBody(c, &req)
	thingName, _ := req.Parameters["ThingName"].(string)
	if thingName == "" {
		thingName = "registered-thing-" + uuid.NewString()[:8]
	}
	out, err := h.Backend.CreateThing(&CreateThingInput{ThingName: thingName})
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyThingName: out.ThingName,
		"thingArn":   out.ThingARN,
	})
}

// --- Thing registration tasks (no real state) ---

func (h *Handler) handleStartThingRegistrationTask(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{keyTaskID: uuid.NewString()})
}

func (h *Handler) handleListThingRegistrationTasks(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"taskIds": []string{}})
}

func (h *Handler) handleListThingRegistrationTaskReports(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"resourceLinks": []string{}})
}

func (h *Handler) handleStopThingRegistrationTask(c *echo.Context) error {
	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDescribeThingRegistrationTask(c *echo.Context) error {
	return respondNotFound(c, "ResourceNotFoundException: thing registration task not found")
}

// --- Managed job templates (read-only, no state) ---

func (h *Handler) handleListManagedJobTemplates(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"managedJobTemplates": []any{}})
}

func (h *Handler) handleDescribeManagedJobTemplate(c *echo.Context) error {
	return respondNotFound(c, "ResourceNotFoundException: managed job template not found")
}

// --- Audit configuration delete ---

func (h *Handler) handleDeleteAccountAuditConfiguration(c *echo.Context) error {
	return c.NoContent(http.StatusOK)
}

// --- Audit finding resources / SBOM validation ---

func (h *Handler) handleListRelatedResourcesForAuditFinding(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"relatedResources": []any{}})
}

func (h *Handler) handleListSbomValidationResults(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"validationResultSummaries": []any{}})
}

// --- SBOM disassociation ---

func (h *Handler) handleDisassociateSbomFromPackageVersion(c *echo.Context) error {
	// DELETE /packages/{packageName}/versions/{versionName}/sbom
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/packages/")
	parts := strings.SplitN(trimmed, "/versions/", pathSplitTwo)
	if len(parts) != pathSplitTwo {
		return c.NoContent(http.StatusOK)
	}
	pkgName := parts[0]
	versionName := strings.TrimSuffix(parts[1], "/sbom")
	if err := h.Backend.DisassociateSbomFromPackageVersion(pkgName, versionName); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- UpdateThingType ---

func (h *Handler) handleUpdateThingType(c *echo.Context) error {
	// PATCH /thing-types/{thingTypeName}
	thingTypeName := strings.TrimPrefix(c.Request().URL.Path, "/thing-types/")
	var req struct {
		ThingTypeProperties struct {
			ThingTypeDescription string `json:"thingTypeDescription"`
		} `json:"thingTypeProperties"`
	}
	_ = readBody(c, &req)
	if err := h.Backend.UpdateThingType(thingTypeName, req.ThingTypeProperties.ThingTypeDescription); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- DeleteCommandExecution ---

func (h *Handler) handleDeleteCommandExecution(c *echo.Context) error {
	// DELETE /commands/{commandId}/executions/{executionId}
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/commands/")
	parts := strings.SplitN(trimmed, "/executions/", pathSplitTwo)
	if len(parts) != pathSplitTwo {
		return respondNotFound(c, "command execution not found")
	}
	if err := h.Backend.DeleteCommandExecution(parts[0], parts[1]); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// ---------------------------------------------------------------------------
// Dispatch for remaining ops
// ---------------------------------------------------------------------------

//nolint:funlen,gocyclo,cyclop,dupl // mechanical routing switch
func (h *Handler) dispatchRemainingOps(c *echo.Context, op string) (bool, error) {
	switch op {
	// Detect mitigation
	case opCancelDetectMitigationActionsTask:
		return true, h.handleCancelDetectMitigationActionsTask(c)
	case opStartDetectMitigationActionsTask:
		return true, h.handleStartDetectMitigationActionsTask(c)
	case opDescribeDetectMitigationActionsTask:
		return true, h.handleDescribeDetectMitigationActionsTask(c)
	case opListDetectMitigationActionsTasks:
		return true, h.handleListDetectMitigationActionsTasks(c)
	case opListDetectMitigationActionsExecutions:
		return true, h.handleListDetectMitigationActionsExecutions(c)
	// Audit mitigation
	case opStartAuditMitigationActionsTask:
		return true, h.handleStartAuditMitigationActionsTask(c)
	case opDescribeAuditMitigationActionsTask:
		return true, h.handleDescribeAuditMitigationActionsTask(c)
	case opListAuditMitigationActionsTasks:
		return true, h.handleListAuditMitigationActionsTasks(c)
	case opListAuditMitigationActionsExecutions:
		return true, h.handleListAuditMitigationActionsExecutions(c)
	// Violations
	case opListActiveViolations:
		return true, h.handleListActiveViolations(c)
	case opListViolationEvents:
		return true, h.handleListViolationEvents(c)
	case opPutVerificationStateOnViolation:
		return true, h.handlePutVerificationStateOnViolation(c)
	case opGetBehaviorModelTrainingSummaries:
		return true, h.handleGetBehaviorModelTrainingSummaries(c)
	case opValidateSecurityProfileBehaviors:
		return true, h.handleValidateSecurityProfileBehaviors(c)
	// Indexing
	case opGetIndexingConfiguration:
		return true, h.handleGetIndexingConfiguration(c)
	case opUpdateIndexingConfiguration:
		return true, h.handleUpdateIndexingConfiguration(c)
	case opListIndices:
		return true, h.handleListIndices(c)
	case opDescribeIndex:
		return true, h.handleDescribeIndex(c)
	case opSearchIndex:
		return true, h.handleSearchIndex(c)
	case opGetCardinality:
		return true, h.handleGetCardinality(c)
	case opGetStatistics:
		return true, h.handleGetStatistics(c)
	case opGetPercentiles:
		return true, h.handleGetPercentiles(c)
	case opGetBucketsAggregation:
		return true, h.handleGetBucketsAggregation(c)
	case opListMetricValues:
		return true, h.handleListMetricValues(c)
	// Certificates / Auth
	case opListOutgoingCertificates:
		return true, h.handleListOutgoingCertificates(c)
	case opDescribeEncryptionConfiguration:
		return true, h.handleDescribeEncryptionConfiguration(c)
	case opUpdateEncryptionConfiguration:
		return true, h.handleUpdateEncryptionConfiguration(c)
	case opTestAuthorization:
		return true, h.handleTestAuthorization(c)
	case opTestInvokeAuthorizer:
		return true, h.handleTestInvokeAuthorizer(c)
	case opDetachPrincipalPolicy:
		return true, h.handleDetachPrincipalPolicy(c)
	case opConfirmTopicRuleDestination:
		return true, h.handleConfirmTopicRuleDestination(c)
	case opGetThingConnectivityData:
		return true, h.handleGetThingConnectivityData(c)
	case opListThingPrincipalsV2:
		return true, h.handleListThingPrincipalsV2(c)
	case opUpdateThingGroupsForThing:
		return true, h.handleUpdateThingGroupsForThing(c)
	case opRegisterThing:
		return true, h.handleRegisterThing(c)
	// Thing registration tasks
	case opStartThingRegistrationTask:
		return true, h.handleStartThingRegistrationTask(c)
	case opListThingRegistrationTasks:
		return true, h.handleListThingRegistrationTasks(c)
	case opListThingRegistrationTaskReports:
		return true, h.handleListThingRegistrationTaskReports(c)
	case opStopThingRegistrationTask:
		return true, h.handleStopThingRegistrationTask(c)
	case opDescribeThingRegistrationTask:
		return true, h.handleDescribeThingRegistrationTask(c)
	// Managed job templates
	case opListManagedJobTemplates:
		return true, h.handleListManagedJobTemplates(c)
	case opDescribeManagedJobTemplate:
		return true, h.handleDescribeManagedJobTemplate(c)
	// Audit configuration
	case opDeleteAccountAuditConfiguration:
		return true, h.handleDeleteAccountAuditConfiguration(c)
	case opListRelatedResourcesForAuditFinding:
		return true, h.handleListRelatedResourcesForAuditFinding(c)
	// SBOM
	case opDisassociateSbomFromPackageVersion:
		return true, h.handleDisassociateSbomFromPackageVersion(c)
	case opListSbomValidationResults:
		return true, h.handleListSbomValidationResults(c)
	// Thing type update
	case opUpdateThingType:
		return true, h.handleUpdateThingType(c)
	// Command execution delete
	case opDeleteCommandExecution:
		return true, h.handleDeleteCommandExecution(c)
	// Provisioning template version describe
	case opDescribeProvisioningTemplateVersion:
		return true, h.handleDescribeProvisioningTemplateVersion(c)
	}

	return false, nil
}

// handleDescribeProvisioningTemplateVersion returns a specific provisioning template version.
func (h *Handler) handleDescribeProvisioningTemplateVersion(c *echo.Context) error {
	// GET /provisioning-templates/{templateName}/versions/{versionId}
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/provisioning-templates/")
	parts := strings.SplitN(trimmed, "/versions/", pathSplitTwo)
	if len(parts) != pathSplitTwo {
		return respondNotFound(c, "provisioning template version not found")
	}
	templateName := parts[0]
	versionIDStr := parts[1]
	versions, err := h.Backend.ListProvisioningTemplateVersions(templateName)
	if err != nil {
		return respondErr(c, err)
	}
	for _, v := range versions {
		if strconv.Itoa(int(v.VersionID)) == versionIDStr {
			return c.JSON(http.StatusOK, map[string]any{
				"templateName":     templateName,
				keyVersionID:       v.VersionID,
				"isDefaultVersion": v.IsDefaultVersion,
				keyCreationDate:    v.CreationDate,
			})
		}
	}

	return respondNotFound(c, "provisioning template version not found")
}
