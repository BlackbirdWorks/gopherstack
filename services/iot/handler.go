package iot

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	iotMatchPriority = 90
	unknownOperation = "Unknown"
	// headerIoTPrincipal is the HTTP header name for the IoT principal (certificate ARN or Cognito identity).
	headerIoTPrincipal = "X-Amzn-Principal"
	// headerIoTThingName is the HTTP header name for the thing name used in AttachPrincipalPolicy.
	headerIoTThingName = "X-Amzn-Iot-Thingname"
)

// Handler is the Echo HTTP handler for IoT control-plane operations.
type Handler struct {
	Backend StorageBackend
	broker  *Broker
}

// NewHandler creates a new IoT Handler.
func NewHandler(backend StorageBackend, broker *Broker) *Handler {
	return &Handler{Backend: backend, broker: broker}
}

// Reset clears all backend state and resets the handler. Used for test isolation.
func (h *Handler) Reset() {
	if r, ok := h.Backend.(Resettable); ok {
		r.Reset()
	}
}

// Broker returns the embedded MQTT broker (used for cross-service wiring).
func (h *Handler) Broker() *Broker { return h.broker }

// Name returns the service name.
func (h *Handler) Name() string { return "IoT" }

// GetSupportedOperations returns the list of supported IoT control-plane operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateThing",
		"DescribeThing",
		"DeleteThing",
		"CreateTopicRule",
		"GetTopicRule",
		"DeleteTopicRule",
		"AttachPrincipalPolicy",
		"CreatePolicy",
		"DescribeEndpoint",
		"AcceptCertificateTransfer",
		"AddThingToBillingGroup",
		"AddThingToThingGroup",
		"AssociateSbomWithPackageVersion",
		"AssociateTargetsWithJob",
		"AttachPolicy",
		"AttachSecurityProfile",
		"AttachThingPrincipal",
		"CancelAuditMitigationActionsTask",
		"CancelAuditTask",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "iot" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this IoT instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function matching IoT control-plane requests by path prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, "/things/") ||
			strings.HasPrefix(path, "/rules/") ||
			strings.HasPrefix(path, "/target-policies/") ||
			strings.HasPrefix(path, "/policies/") ||
			path == "/endpoint" ||
			strings.HasPrefix(path, "/accept-certificate-transfer/") ||
			path == "/billing-groups/addThingToBillingGroup" ||
			path == "/thing-groups/addThingToThingGroup" ||
			strings.HasPrefix(path, "/packages/") ||
			strings.HasPrefix(path, "/jobs/") ||
			strings.HasPrefix(path, "/security-profiles/") ||
			strings.HasPrefix(path, "/audit/")
	}
}

// MatchPriority returns the routing priority for the IoT handler.
func (h *Handler) MatchPriority() int { return iotMatchPriority }

// ExtractOperation extracts the IoT operation name from the request method + path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return resolveOperation(c.Request().URL.Path, c.Request().Method)
}

// maxPathSegments is used to split the path into at most 2 segments.
const maxPathSegments = 2

// ExtractResource extracts the resource name (thing/rule/policy) from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path

	for _, prefix := range []string{"/things/", "/rules/", "/policies/", "/target-policies/"} {
		if after, ok := strings.CutPrefix(path, prefix); ok {
			return strings.SplitN(after, "/", maxPathSegments)[0]
		}
	}

	for _, prefix := range []string{
		"/accept-certificate-transfer/",
		"/security-profiles/",
		"/jobs/",
		"/packages/",
		"/audit/mitigationactions/tasks/",
		"/audit/tasks/",
	} {
		if after, ok := strings.CutPrefix(path, prefix); ok {
			return strings.SplitN(after, "/", maxPathSegments)[0]
		}
	}

	return ""
}

// StartWorker starts the embedded MQTT broker as a background worker.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.broker == nil {
		return nil
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "starting IoT MQTT broker", "port", h.broker.port)

	go func() {
		if err := h.broker.Start(ctx); err != nil {
			log.ErrorContext(ctx, "IoT MQTT broker stopped", "error", err)
		}
	}()

	return nil
}

func resolveOperation(path, method string) string {
	switch {
	case strings.HasPrefix(path, "/things/"):
		return thingOperation(path, method)
	case strings.HasPrefix(path, "/rules/"):
		return ruleOperation(method)
	case path == "/endpoint" && method == http.MethodGet:
		return "DescribeEndpoint"
	}

	if op := resolvePolicyAndCertOps(path, method); op != unknownOperation {
		return op
	}

	if op := resolveGroupAndPackageOps(path, method); op != unknownOperation {
		return op
	}

	return resolveJobAndAuditOps(path, method)
}

func resolvePolicyAndCertOps(path, method string) string {
	switch {
	case strings.HasPrefix(path, "/target-policies/") && method == http.MethodPost:
		return "AttachPrincipalPolicy"
	case strings.HasPrefix(path, "/target-policies/") && method == http.MethodPut:
		return "AttachPolicy"
	case strings.HasPrefix(path, "/policies/") && method == http.MethodPost:
		return "CreatePolicy"
	case strings.HasPrefix(path, "/accept-certificate-transfer/") && method == http.MethodPatch:
		return "AcceptCertificateTransfer"
	}

	return unknownOperation
}

func resolveGroupAndPackageOps(path, method string) string {
	switch {
	case path == "/billing-groups/addThingToBillingGroup" && method == http.MethodPut:
		return "AddThingToBillingGroup"
	case path == "/thing-groups/addThingToThingGroup" && method == http.MethodPut:
		return "AddThingToThingGroup"
	case strings.HasPrefix(path, "/packages/") &&
		strings.HasSuffix(path, "/sbom") &&
		method == http.MethodPut:
		return "AssociateSbomWithPackageVersion"
	}

	return unknownOperation
}

func resolveJobAndAuditOps(path, method string) string {
	switch {
	case strings.HasPrefix(path, "/jobs/") &&
		strings.HasSuffix(path, "/targets") &&
		method == http.MethodPost:
		return "AssociateTargetsWithJob"
	case strings.HasPrefix(path, "/security-profiles/") &&
		strings.HasSuffix(path, "/targets") &&
		method == http.MethodPut:
		return "AttachSecurityProfile"
	case strings.HasPrefix(path, "/audit/mitigationactions/tasks/") &&
		strings.HasSuffix(path, "/cancel") &&
		method == http.MethodPut:
		return "CancelAuditMitigationActionsTask"
	case strings.HasPrefix(path, "/audit/tasks/") &&
		strings.HasSuffix(path, "/cancel") &&
		method == http.MethodPut:
		return "CancelAuditTask"
	}

	return unknownOperation
}

func thingOperation(path, method string) string {
	// PUT /things/{thingName}/principals → AttachThingPrincipal
	if method == http.MethodPut && strings.HasSuffix(path, "/principals") {
		return "AttachThingPrincipal"
	}

	switch method {
	case http.MethodPost:
		return "CreateThing"
	case http.MethodGet:
		return "DescribeThing"
	case http.MethodDelete:
		return "DeleteThing"
	}

	return unknownOperation
}

func ruleOperation(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateTopicRule"
	case http.MethodGet:
		return "GetTopicRule"
	case http.MethodDelete:
		return "DeleteTopicRule"
	}

	return unknownOperation
}

// Handler returns the Echo handler function for IoT operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		log := logger.Load(c.Request().Context())
		op := resolveOperation(c.Request().URL.Path, c.Request().Method)

		log.Debug("iot request", "operation", op, "path", c.Request().URL.Path)

		if handled, err := h.dispatchCoreOp(c, op); handled {
			return err
		}

		if handled, err := h.dispatchNewOp(c, op); handled {
			return err
		}

		return c.JSON(http.StatusBadRequest, map[string]string{"error": "unknown operation: " + op})
	}
}

func (h *Handler) dispatchCoreOp(c *echo.Context, op string) (bool, error) {
	switch op {
	case "CreateThing":
		return true, h.handleCreateThing(c)
	case "DescribeThing":
		return true, h.handleDescribeThing(c)
	case "DeleteThing":
		return true, h.handleDeleteThing(c)
	case "CreateTopicRule":
		return true, h.handleCreateTopicRule(c)
	case "GetTopicRule":
		return true, h.handleGetTopicRule(c)
	case "DeleteTopicRule":
		return true, h.handleDeleteTopicRule(c)
	case "AttachPrincipalPolicy":
		return true, h.handleAttachPrincipalPolicy(c)
	case "CreatePolicy":
		return true, h.handleCreatePolicy(c)
	case "DescribeEndpoint":
		return true, h.handleDescribeEndpoint(c)
	}

	return false, nil
}

func (h *Handler) dispatchNewOp(c *echo.Context, op string) (bool, error) {
	switch op {
	case "AcceptCertificateTransfer":
		return true, h.handleAcceptCertificateTransfer(c)
	case "AddThingToBillingGroup":
		return true, h.handleAddThingToBillingGroup(c)
	case "AddThingToThingGroup":
		return true, h.handleAddThingToThingGroup(c)
	case "AssociateSbomWithPackageVersion":
		return true, h.handleAssociateSbomWithPackageVersion(c)
	case "AssociateTargetsWithJob":
		return true, h.handleAssociateTargetsWithJob(c)
	case "AttachPolicy":
		return true, h.handleAttachPolicy(c)
	case "AttachSecurityProfile":
		return true, h.handleAttachSecurityProfile(c)
	case "AttachThingPrincipal":
		return true, h.handleAttachThingPrincipal(c)
	case "CancelAuditMitigationActionsTask":
		return true, h.handleCancelAuditMitigationActionsTask(c)
	case "CancelAuditTask":
		return true, h.handleCancelAuditTask(c)
	}

	return false, nil
}

// handleError maps backend errors to appropriate HTTP responses.
func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrThingNotFound),
		errors.Is(err, ErrRuleNotFound),
		errors.Is(err, ErrPolicyNotFound):
		return c.JSON(http.StatusNotFound, map[string]string{"error": err.Error()})
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, map[string]string{"error": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
}

func (h *Handler) handleCreateThing(c *echo.Context) error {
	thingName := strings.TrimPrefix(c.Request().URL.Path, "/things/")

	var body struct {
		AttributePayload *AttributePayload `json:"attributePayload"`
		ThingTypeName    string            `json:"thingTypeName"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	out, err := h.Backend.CreateThing(&CreateThingInput{
		ThingName:        thingName,
		ThingTypeName:    body.ThingTypeName,
		AttributePayload: body.AttributePayload,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"thingName": out.ThingName,
		"thingArn":  out.ThingARN,
		"thingId":   out.ThingID,
	})
}

func (h *Handler) handleDescribeThing(c *echo.Context) error {
	thingName := strings.TrimPrefix(c.Request().URL.Path, "/things/")

	t, err := h.Backend.DescribeThing(thingName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"thingName":  t.ThingName,
		"thingArn":   t.ARN,
		"thingId":    t.ThingID,
		"thingType":  t.ThingType,
		"attributes": t.Attributes,
		"version":    t.Version,
	})
}

func (h *Handler) handleDeleteThing(c *echo.Context) error {
	thingName := strings.TrimPrefix(c.Request().URL.Path, "/things/")

	if err := h.Backend.DeleteThing(thingName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateTopicRule(c *echo.Context) error {
	ruleName := strings.TrimPrefix(c.Request().URL.Path, "/rules/")

	var payload TopicRulePayload

	if err := json.NewDecoder(c.Request().Body).Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	if err := h.Backend.CreateTopicRule(&CreateTopicRuleInput{
		RuleName:         ruleName,
		TopicRulePayload: &payload,
	}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetTopicRule(c *echo.Context) error {
	ruleName := strings.TrimPrefix(c.Request().URL.Path, "/rules/")

	r, err := h.Backend.GetTopicRule(ruleName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ruleArn": r.ARN,
		"rule": map[string]any{
			"ruleName":     r.RuleName,
			"sql":          r.SQL,
			"description":  r.Description,
			"actions":      r.Actions,
			"ruleDisabled": !r.Enabled,
			"createdAt":    r.CreatedAt,
		},
	})
}

func (h *Handler) handleDeleteTopicRule(c *echo.Context) error {
	ruleName := strings.TrimPrefix(c.Request().URL.Path, "/rules/")

	if err := h.Backend.DeleteTopicRule(ruleName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleAttachPrincipalPolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/target-policies/")
	principal := c.Request().Header.Get(headerIoTThingName)

	if err := h.Backend.AttachPrincipalPolicy(&AttachPrincipalPolicyInput{
		PolicyName: policyName,
		Principal:  principal,
	}); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleCreatePolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/policies/")

	var body struct {
		PolicyDocument string `json:"policyDocument"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	out, err := h.Backend.CreatePolicy(&CreatePolicyInput{
		PolicyName:     policyName,
		PolicyDocument: body.PolicyDocument,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"policyName":     out.PolicyName,
		"policyArn":      out.PolicyARN,
		"policyDocument": out.PolicyDocument,
	})
}

func (h *Handler) handleDescribeEndpoint(c *echo.Context) error {
	endpointType := c.QueryParam("endpointType")

	out, err := h.Backend.DescribeEndpoint(endpointType)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{
		"endpointAddress": out.EndpointAddress,
	})
}

func (h *Handler) handleAcceptCertificateTransfer(c *echo.Context) error {
	certID := strings.TrimPrefix(c.Request().URL.Path, "/accept-certificate-transfer/")

	var body struct {
		SetAsActive bool `json:"setAsActive"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	if err := h.Backend.AcceptCertificateTransfer(&AcceptCertificateTransferInput{
		CertificateID: certID,
		SetAsActive:   body.SetAsActive,
	}); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleAddThingToBillingGroup(c *echo.Context) error {
	var body AddThingToBillingGroupInput

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	if err := h.Backend.AddThingToBillingGroup(&body); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleAddThingToThingGroup(c *echo.Context) error {
	var body AddThingToThingGroupInput

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	if err := h.Backend.AddThingToThingGroup(&body); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.NoContent(http.StatusOK)
}

// maxPackagePathSegments is the number of segments in /packages/{pkg}/versions/{ver}/sbom.
const maxPackagePathSegments = 6

// packageVersionPartsMin is the minimum number of split parts to extract package/version from the path.
const packageVersionPartsMin = 3

func (h *Handler) handleAssociateSbomWithPackageVersion(c *echo.Context) error {
	// Path: /packages/{packageName}/versions/{versionName}/sbom
	parts := strings.SplitN(strings.TrimPrefix(c.Request().URL.Path, "/packages/"), "/", maxPackagePathSegments)

	var packageName, versionName string
	// len(parts) >= packageVersionPartsMin guarantees indices 0, 1, 2 are valid.
	if len(parts) >= packageVersionPartsMin {
		packageName = parts[0]
		versionName = parts[2]
	}

	var body struct {
		Sbom *SbomDocument `json:"sbom"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	out, err := h.Backend.AssociateSbomWithPackageVersion(&AssociateSbomWithPackageVersionInput{
		PackageName: packageName,
		VersionName: versionName,
		Sbom:        body.Sbom,
	})
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) handleAssociateTargetsWithJob(c *echo.Context) error {
	// Path: /jobs/{jobId}/targets
	after := strings.TrimPrefix(c.Request().URL.Path, "/jobs/")
	jobID := strings.SplitN(after, "/", maxPathSegments)[0]

	var body struct {
		Comment     string   `json:"comment"`
		NamespaceID string   `json:"namespaceId"`
		Targets     []string `json:"targets"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	out, err := h.Backend.AssociateTargetsWithJob(&AssociateTargetsWithJobInput{
		JobID:       jobID,
		Targets:     body.Targets,
		Comment:     body.Comment,
		NamespaceID: body.NamespaceID,
	})
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) handleAttachPolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/target-policies/")

	var body struct {
		Target string `json:"target"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	if err := h.Backend.AttachPolicy(&AttachPolicyInput{
		PolicyName: policyName,
		Target:     body.Target,
	}); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleAttachSecurityProfile(c *echo.Context) error {
	// Path: /security-profiles/{securityProfileName}/targets
	after := strings.TrimPrefix(c.Request().URL.Path, "/security-profiles/")
	profileName := strings.SplitN(after, "/", maxPathSegments)[0]
	targetArn := c.QueryParam("securityProfileTargetArn")

	if err := h.Backend.AttachSecurityProfile(&AttachSecurityProfileInput{
		SecurityProfileName:      profileName,
		SecurityProfileTargetArn: targetArn,
	}); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleAttachThingPrincipal(c *echo.Context) error {
	// Path: /things/{thingName}/principals
	after := strings.TrimPrefix(c.Request().URL.Path, "/things/")
	thingName := strings.SplitN(after, "/", maxPathSegments)[0]
	principal := c.Request().Header.Get(headerIoTPrincipal)

	if err := h.Backend.AttachThingPrincipal(&AttachThingPrincipalInput{
		ThingName: thingName,
		Principal: principal,
	}); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleCancelAuditMitigationActionsTask(c *echo.Context) error {
	// Path: /audit/mitigationactions/tasks/{taskId}/cancel
	after := strings.TrimPrefix(c.Request().URL.Path, "/audit/mitigationactions/tasks/")
	taskID := strings.SplitN(after, "/", maxPathSegments)[0]

	if err := h.Backend.CancelAuditMitigationActionsTask(&CancelAuditMitigationActionsTaskInput{
		TaskID: taskID,
	}); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleCancelAuditTask(c *echo.Context) error {
	// Path: /audit/tasks/{taskId}/cancel
	after := strings.TrimPrefix(c.Request().URL.Path, "/audit/tasks/")
	taskID := strings.SplitN(after, "/", maxPathSegments)[0]

	if err := h.Backend.CancelAuditTask(&CancelAuditTaskInput{
		AuditTaskID: taskID,
	}); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.NoContent(http.StatusOK)
}
