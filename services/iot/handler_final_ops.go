package iot

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

// ---------------------------------------------------------------------------
// Route matching / resolution for the final stub batch.
// ---------------------------------------------------------------------------

// matchFinalOpsPath reports whether path belongs to one of the final-batch
// routes, for the top-level RouteMatcher.
func matchFinalOpsPath(path string) bool {
	return path == pathEncryptionConfiguration ||
		path == pathTestAuthorization ||
		strings.HasPrefix(path, pathPrincipalPolicies+"/") ||
		strings.HasPrefix(path, pathConfirmDestination+"/") ||
		strings.HasPrefix(path, pathCommandExecutions+"/") ||
		path == pathBehaviorModelTrainingSummaries ||
		path == pathCertificatesOutgoing ||
		path == pathMetricValues ||
		path == pathValidateSecurityProfileBehaviors
}

const (
	pathEncryptionConfiguration          = "/encryption-configuration"
	pathTestAuthorization                = "/test-authorization"
	pathPrincipalPolicies                = "/principal-policies"
	pathConfirmDestination               = "/confirmdestination"
	pathCommandExecutions                = "/command-executions"
	pathBehaviorModelTrainingSummaries   = "/behavior-model-training/summaries"
	pathCertificatesOutgoing             = "/certificates-out-going"
	pathMetricValues                     = "/metric-values"
	pathValidateSecurityProfileBehaviors = "/security-profile-behaviors/validate"
)

// resolveFinalOps resolves the operation name for the final stub batch's
// routes. Called from resolveOperation's fallback chain.
func resolveFinalOps(path, method string) string {
	if op := resolveFinalOpsGroupA(path, method); op != unknownOperation {
		return op
	}

	return resolveFinalOpsGroupB(path, method)
}

func resolveFinalOpsGroupA(path, method string) string {
	switch {
	case path == pathEncryptionConfiguration && method == http.MethodGet:
		return opDescribeEncryptionConfiguration
	case path == pathEncryptionConfiguration && method == http.MethodPatch:
		return opUpdateEncryptionConfiguration
	case path == pathTestAuthorization && method == http.MethodPost:
		return opTestAuthorization
	case strings.HasPrefix(path, pathPrincipalPolicies+"/") && method == http.MethodDelete:
		return opDetachPrincipalPolicy
	case strings.HasPrefix(path, pathConfirmDestination+"/") && method == http.MethodGet:
		return opConfirmTopicRuleDestination
	}

	return unknownOperation
}

func resolveFinalOpsGroupB(path, method string) string {
	switch {
	case strings.HasPrefix(path, pathCommandExecutions+"/") && method == http.MethodDelete:
		return opDeleteCommandExecution
	case path == pathBehaviorModelTrainingSummaries && method == http.MethodGet:
		return opGetBehaviorModelTrainingSummaries
	case path == pathCertificatesOutgoing && method == http.MethodGet:
		return opListOutgoingCertificates
	case path == pathMetricValues && method == http.MethodGet:
		return opListMetricValues
	case path == pathValidateSecurityProfileBehaviors && method == http.MethodPost:
		return opValidateSecurityProfileBehaviors
	}

	return unknownOperation
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

func (h *Handler) dispatchFinalOps(c *echo.Context, op string) (bool, error) {
	if handled, err := h.dispatchFinalOpsGroupA(c, op); handled {
		return true, err
	}

	return h.dispatchFinalOpsGroupB(c, op)
}

func (h *Handler) dispatchFinalOpsGroupA(c *echo.Context, op string) (bool, error) {
	switch op {
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
	case opDeleteCommandExecution:
		return true, h.handleDeleteCommandExecution(c)
	}

	return false, nil
}

func (h *Handler) dispatchFinalOpsGroupB(c *echo.Context, op string) (bool, error) {
	switch op {
	case opGetBehaviorModelTrainingSummaries:
		return true, h.handleGetBehaviorModelTrainingSummaries(c)
	case opListOutgoingCertificates:
		return true, h.handleListOutgoingCertificates(c)
	case opDisassociateSbomFromPackageVersion:
		return true, h.handleDisassociateSbomFromPackageVersion(c)
	case opListSbomValidationResults:
		return true, h.handleListSbomValidationResults(c)
	case opListMetricValues:
		return true, h.handleListMetricValues(c)
	case opGetThingConnectivityData:
		return true, h.handleGetThingConnectivityData(c)
	case opDescribeProvisioningTemplateVersion:
		return true, h.handleDescribeProvisioningTemplateVersion(c)
	case opValidateSecurityProfileBehaviors:
		return true, h.handleValidateSecurityProfileBehaviors(c)
	}

	return false, nil
}

// ---------------------------------------------------------------------------
// Encryption configuration
// ---------------------------------------------------------------------------

func (h *Handler) handleDescribeEncryptionConfiguration(c *echo.Context) error {
	return c.JSON(http.StatusOK, h.Backend.DescribeEncryptionConfiguration())
}

func (h *Handler) handleUpdateEncryptionConfiguration(c *echo.Context) error {
	var req UpdateEncryptionConfigurationInput
	if err := readBody(c, &req); err != nil {
		return err
	}

	if err := h.Backend.UpdateEncryptionConfiguration(&req); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

// ---------------------------------------------------------------------------
// TestAuthorization / TestInvokeAuthorizer
// ---------------------------------------------------------------------------

func (h *Handler) handleTestAuthorization(c *echo.Context) error {
	var req struct {
		Principal             string     `json:"principal"`
		CognitoIdentityPoolID string     `json:"cognitoIdentityPoolId"`
		ClientID              string     `json:"clientId"`
		PolicyNamesToAdd      []string   `json:"policyNamesToAdd"`
		PolicyNamesToSkip     []string   `json:"policyNamesToSkip"`
		AuthInfos             []AuthInfo `json:"authInfos"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}

	results, err := h.Backend.TestAuthorization(&TestAuthorizationInput{
		Principal:             req.Principal,
		CognitoIdentityPoolID: req.CognitoIdentityPoolID,
		ClientID:              req.ClientID,
		PolicyNamesToAdd:      req.PolicyNamesToAdd,
		PolicyNamesToSkip:     req.PolicyNamesToSkip,
		AuthInfos:             req.AuthInfos,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"authResults": results})
}

func (h *Handler) handleTestInvokeAuthorizer(c *echo.Context) error {
	name := strings.TrimSuffix(strings.TrimPrefix(c.Request().URL.Path, "/authorizer/"), "/test")

	var req struct {
		Token          string `json:"token"`
		TokenSignature string `json:"tokenSignature"`
		MQTTContext    struct {
			ClientID string `json:"clientId"`
		} `json:"mqttContext"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}

	out, err := h.Backend.TestInvokeAuthorizer(name, &TestInvokeAuthorizerInput{
		Token:          req.Token,
		TokenSignature: req.TokenSignature,
		MQTTClientID:   req.MQTTContext.ClientID,
	})
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"isAuthenticated":          out.IsAuthenticated,
		"principalId":              out.PrincipalID,
		"policyDocuments":          out.PolicyDocuments,
		"disconnectAfterInSeconds": out.DisconnectAfterInSeconds,
		"refreshAfterInSeconds":    out.RefreshAfterInSeconds,
	})
}

// ---------------------------------------------------------------------------
// DetachPrincipalPolicy
// ---------------------------------------------------------------------------

func (h *Handler) handleDetachPrincipalPolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, pathPrincipalPolicies+"/")
	principal := c.Request().Header.Get("X-Amzn-Iot-Principal")

	if err := h.Backend.DetachPrincipalPolicy(policyName, principal); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// ---------------------------------------------------------------------------
// ConfirmTopicRuleDestination
// ---------------------------------------------------------------------------

func (h *Handler) handleConfirmTopicRuleDestination(c *echo.Context) error {
	token := strings.TrimPrefix(c.Request().URL.Path, pathConfirmDestination+"/")

	if err := h.Backend.ConfirmTopicRuleDestination(token); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

// ---------------------------------------------------------------------------
// DeleteCommandExecution
// ---------------------------------------------------------------------------

func (h *Handler) handleDeleteCommandExecution(c *echo.Context) error {
	executionID := strings.TrimPrefix(c.Request().URL.Path, pathCommandExecutions+"/")
	targetARN := c.QueryParam("targetArn")

	if err := h.Backend.DeleteCommandExecution(executionID, targetARN); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// ---------------------------------------------------------------------------
// GetBehaviorModelTrainingSummaries
// ---------------------------------------------------------------------------

func (h *Handler) handleGetBehaviorModelTrainingSummaries(c *echo.Context) error {
	securityProfileName := c.QueryParam(keySecurityProfileName)
	maxResults := parseInt32QueryParam(c, "maxResults")
	nextToken := c.QueryParam("nextToken")

	summaries, next, err := h.Backend.GetBehaviorModelTrainingSummaries(securityProfileName, maxResults, nextToken)
	if err != nil {
		return respondErr(c, err)
	}

	resp := map[string]any{"summaries": summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// ListOutgoingCertificates
// ---------------------------------------------------------------------------

func (h *Handler) handleListOutgoingCertificates(c *echo.Context) error {
	certs := h.Backend.ListOutgoingCertificates()

	return c.JSON(http.StatusOK, map[string]any{"outgoingCertificates": certs})
}

// ---------------------------------------------------------------------------
// SBOM disassociation / validation results
// ---------------------------------------------------------------------------

func (h *Handler) handleDisassociateSbomFromPackageVersion(c *echo.Context) error {
	// DELETE /packages/{packageName}/versions/{versionName}/sbom
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/packages/")
	trimmed = strings.TrimSuffix(trimmed, "/sbom")
	parts := strings.SplitN(trimmed, "/versions/", pathSplitTwo)

	if len(parts) != pathSplitTwo {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: keyInvalidPath})
	}

	if err := h.Backend.DisassociateSbomFromPackageVersion(parts[0], parts[1]); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListSbomValidationResults(c *echo.Context) error {
	// GET /packages/{packageName}/versions/{versionName}/sbom-validation-results
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/packages/")
	trimmed = strings.TrimSuffix(trimmed, "/sbom-validation-results")
	parts := strings.SplitN(trimmed, "/versions/", pathSplitTwo)

	if len(parts) != pathSplitTwo {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: keyInvalidPath})
	}

	maxResults := parseInt32QueryParam(c, "maxResults")
	nextToken := c.QueryParam("nextToken")

	results, next, err := h.Backend.ListSbomValidationResults(parts[0], parts[1], maxResults, nextToken)
	if err != nil {
		return respondErr(c, err)
	}

	resp := map[string]any{"validationResultSummaries": results}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// ListMetricValues
// ---------------------------------------------------------------------------

func (h *Handler) handleListMetricValues(c *echo.Context) error {
	thingName := c.QueryParam(keyThingName)
	metricName := c.QueryParam("metricName")
	startTime := parseIoTEpochQueryParam(c, "startTime")
	endTime := parseIoTEpochQueryParam(c, "endTime")
	maxResults := parseInt32QueryParam(c, "maxResults")
	nextToken := c.QueryParam("nextToken")

	values, next, err := h.Backend.ListMetricValues(thingName, metricName, startTime, endTime, maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{"metricDatumList": values}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// GetThingConnectivityData
// ---------------------------------------------------------------------------

func (h *Handler) handleGetThingConnectivityData(c *echo.Context) error {
	thingName := strings.TrimSuffix(strings.TrimPrefix(c.Request().URL.Path, "/things/"), "/connectivity-data")

	data, err := h.Backend.GetThingConnectivityData(thingName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyThingName:       thingName,
		"connected":        data.Connected,
		"timestamp":        data.Timestamp,
		"disconnectReason": data.DisconnectReason,
	})
}

// ---------------------------------------------------------------------------
// DescribeProvisioningTemplateVersion
// ---------------------------------------------------------------------------

func (h *Handler) handleDescribeProvisioningTemplateVersion(c *echo.Context) error {
	// GET /provisioning-templates/{templateName}/versions/{versionId}
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/provisioning-templates/")
	parts := strings.SplitN(trimmed, "/versions/", pathSplitTwo)

	if len(parts) != pathSplitTwo {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: keyInvalidPath})
	}

	var versionID int32
	if err := parseInt32(parts[1], &versionID); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: "invalid versionId"})
	}

	v, err := h.Backend.DescribeProvisioningTemplateVersion(parts[0], versionID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, v)
}

// ---------------------------------------------------------------------------
// ValidateSecurityProfileBehaviors
// ---------------------------------------------------------------------------

func (h *Handler) handleValidateSecurityProfileBehaviors(c *echo.Context) error {
	var req struct {
		Behaviors []SecurityProfileBehavior `json:"behaviors"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}

	valid, validationErrors := h.Backend.ValidateSecurityProfileBehaviors(req.Behaviors)

	errs := make([]map[string]string, len(validationErrors))
	for i, msg := range validationErrors {
		errs[i] = map[string]string{"errorMessage": msg}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"valid":            valid,
		"validationErrors": errs,
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// parseInt32QueryParam reads an int32 query parameter, defaulting to 0 when
// absent or invalid.
func parseInt32QueryParam(c *echo.Context, name string) int32 {
	v := c.QueryParam(name)
	if v == "" {
		return 0
	}

	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return 0
	}

	return int32(n)
}
