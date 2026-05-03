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
	keyError      = "error"
	keyThingName  = "thingName"
	keyThingArn   = "thingArn"
	keyPolicyName = "policyName"
	keyPolicyArn  = "policyArn"
)

const (
	opAcceptCertificateTransfer        = "AcceptCertificateTransfer"
	opAddThingToBillingGroup           = "AddThingToBillingGroup"
	opAddThingToThingGroup             = "AddThingToThingGroup"
	opAssociateSbomWithPackageVersion  = "AssociateSbomWithPackageVersion"
	opAssociateTargetsWithJob          = "AssociateTargetsWithJob"
	opAttachPolicy                     = "AttachPolicy"
	opAttachPrincipalPolicy            = "AttachPrincipalPolicy"
	opAttachSecurityProfile            = "AttachSecurityProfile"
	opAttachThingPrincipal             = "AttachThingPrincipal"
	opCancelAuditMitigationActionsTask = "CancelAuditMitigationActionsTask"
	opCancelAuditTask                  = "CancelAuditTask"
	opCreatePolicy                     = "CreatePolicy"
	opCreateThing                      = "CreateThing"
	opCreateTopicRule                  = "CreateTopicRule"
	opDeletePolicy                     = "DeletePolicy"
	opDeleteThing                      = "DeleteThing"
	opDeleteTopicRule                  = "DeleteTopicRule"
	opDescribeEndpoint                 = "DescribeEndpoint"
	opDescribeThing                    = "DescribeThing"
	opDisableTopicRule                 = "DisableTopicRule"
	opEnableTopicRule                  = "EnableTopicRule"
	opGetPolicy                        = "GetPolicy"
	opGetTopicRule                     = "GetTopicRule"
	opListPolicies                     = "ListPolicies"
	opListThingPrincipals              = "ListThingPrincipals"
	opListThings                       = "ListThings"
	opListTopicRules                   = "ListTopicRules"
	opReplaceTopicRule                 = "ReplaceTopicRule"
	opUpdateThing                      = "UpdateThing"
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
		opAcceptCertificateTransfer,
		opAddThingToBillingGroup,
		opAddThingToThingGroup,
		opAssociateSbomWithPackageVersion,
		opAssociateTargetsWithJob,
		opAttachPolicy,
		opAttachPrincipalPolicy,
		opAttachSecurityProfile,
		opAttachThingPrincipal,
		opCancelAuditMitigationActionsTask,
		opCancelAuditTask,
		opCreatePolicy,
		opCreateThing,
		opCreateTopicRule,
		opDeletePolicy,
		opDeleteThing,
		opDeleteTopicRule,
		opDescribeEndpoint,
		opDescribeThing,
		opDisableTopicRule,
		opEnableTopicRule,
		opGetPolicy,
		opGetTopicRule,
		opListPolicies,
		opListThingPrincipals,
		opListThings,
		opListTopicRules,
		opReplaceTopicRule,
		opUpdateThing,
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
			path == "/things" ||
			strings.HasPrefix(path, "/rules/") ||
			path == "/rules" ||
			strings.HasPrefix(path, "/target-policies/") ||
			strings.HasPrefix(path, "/policies/") ||
			path == "/policies" ||
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
			log.ErrorContext(ctx, "IoT MQTT broker stopped", keyError, err)
		}
	}()

	return nil
}

func resolveOperation(path, method string) string {
	switch {
	case path == "/things" && method == http.MethodGet:
		return opListThings
	case strings.HasPrefix(path, "/things/"):
		return thingOperation(path, method)
	case path == "/rules" && method == http.MethodGet:
		return opListTopicRules
	case strings.HasPrefix(path, "/rules/"):
		return ruleOperation(path, method)
	case path == "/endpoint" && method == http.MethodGet:
		return opDescribeEndpoint
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
		return opAttachPrincipalPolicy
	case strings.HasPrefix(path, "/target-policies/") && method == http.MethodPut:
		return opAttachPolicy
	case path == "/policies" && method == http.MethodGet:
		return opListPolicies
	case strings.HasPrefix(path, "/policies/") && method == http.MethodPost:
		return opCreatePolicy
	case strings.HasPrefix(path, "/policies/") && method == http.MethodGet:
		return opGetPolicy
	case strings.HasPrefix(path, "/policies/") && method == http.MethodDelete:
		return opDeletePolicy
	case strings.HasPrefix(path, "/accept-certificate-transfer/") && method == http.MethodPatch:
		return opAcceptCertificateTransfer
	}

	return unknownOperation
}

func resolveGroupAndPackageOps(path, method string) string {
	switch {
	case path == "/billing-groups/addThingToBillingGroup" && method == http.MethodPut:
		return opAddThingToBillingGroup
	case path == "/thing-groups/addThingToThingGroup" && method == http.MethodPut:
		return opAddThingToThingGroup
	case strings.HasPrefix(path, "/packages/") &&
		strings.HasSuffix(path, "/sbom") &&
		method == http.MethodPut:
		return opAssociateSbomWithPackageVersion
	}

	return unknownOperation
}

func resolveJobAndAuditOps(path, method string) string {
	switch {
	case strings.HasPrefix(path, "/jobs/") &&
		strings.HasSuffix(path, "/targets") &&
		method == http.MethodPost:
		return opAssociateTargetsWithJob
	case strings.HasPrefix(path, "/security-profiles/") &&
		strings.HasSuffix(path, "/targets") &&
		method == http.MethodPut:
		return opAttachSecurityProfile
	case strings.HasPrefix(path, "/audit/mitigationactions/tasks/") &&
		strings.HasSuffix(path, "/cancel") &&
		method == http.MethodPut:
		return opCancelAuditMitigationActionsTask
	case strings.HasPrefix(path, "/audit/tasks/") &&
		strings.HasSuffix(path, "/cancel") &&
		method == http.MethodPut:
		return opCancelAuditTask
	}

	return unknownOperation
}

func thingOperation(path, method string) string {
	// GET /things/{thingName}/principals → ListThingPrincipals
	if method == http.MethodGet && strings.HasSuffix(path, "/principals") {
		return opListThingPrincipals
	}

	// PUT /things/{thingName}/principals → AttachThingPrincipal
	if method == http.MethodPut && strings.HasSuffix(path, "/principals") {
		return opAttachThingPrincipal
	}

	switch method {
	case http.MethodPost:
		return opCreateThing
	case http.MethodGet:
		return opDescribeThing
	case http.MethodDelete:
		return opDeleteThing
	case http.MethodPatch:
		return opUpdateThing
	}

	return unknownOperation
}

func ruleOperation(path, method string) string {
	// PATCH /rules/{ruleName}/disable → DisableTopicRule
	if method == http.MethodPatch && strings.HasSuffix(path, "/disable") {
		return opDisableTopicRule
	}

	// PATCH /rules/{ruleName}/enable → EnableTopicRule
	if method == http.MethodPatch && strings.HasSuffix(path, "/enable") {
		return opEnableTopicRule
	}

	switch method {
	case http.MethodPost:
		return opCreateTopicRule
	case http.MethodGet:
		return opGetTopicRule
	case http.MethodDelete:
		return opDeleteTopicRule
	case http.MethodPatch:
		return opReplaceTopicRule
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

		return c.JSON(http.StatusBadRequest, map[string]string{keyError: "unknown operation: " + op})
	}
}

func (h *Handler) dispatchCoreOp(c *echo.Context, op string) (bool, error) {
	if handled, err := h.dispatchThingOps(c, op); handled {
		return true, err
	}

	if handled, err := h.dispatchRuleOps(c, op); handled {
		return true, err
	}

	return h.dispatchPolicyOps(c, op)
}

func (h *Handler) dispatchThingOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateThing:
		return true, h.handleCreateThing(c)
	case opDescribeThing:
		return true, h.handleDescribeThing(c)
	case opDeleteThing:
		return true, h.handleDeleteThing(c)
	case opUpdateThing:
		return true, h.handleUpdateThing(c)
	case opListThings:
		return true, h.handleListThings(c)
	case opListThingPrincipals:
		return true, h.handleListThingPrincipals(c)
	}

	return false, nil
}

func (h *Handler) dispatchRuleOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateTopicRule:
		return true, h.handleCreateTopicRule(c)
	case opGetTopicRule:
		return true, h.handleGetTopicRule(c)
	case opDeleteTopicRule:
		return true, h.handleDeleteTopicRule(c)
	case opDisableTopicRule:
		return true, h.handleDisableTopicRule(c)
	case opEnableTopicRule:
		return true, h.handleEnableTopicRule(c)
	case opReplaceTopicRule:
		return true, h.handleReplaceTopicRule(c)
	case opListTopicRules:
		return true, h.handleListTopicRules(c)
	}

	return false, nil
}

func (h *Handler) dispatchPolicyOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opAttachPrincipalPolicy:
		return true, h.handleAttachPrincipalPolicy(c)
	case opCreatePolicy:
		return true, h.handleCreatePolicy(c)
	case opGetPolicy:
		return true, h.handleGetPolicy(c)
	case opDeletePolicy:
		return true, h.handleDeletePolicy(c)
	case opListPolicies:
		return true, h.handleListPolicies(c)
	case opDescribeEndpoint:
		return true, h.handleDescribeEndpoint(c)
	}

	return false, nil
}

func (h *Handler) dispatchNewOp(c *echo.Context, op string) (bool, error) {
	switch op {
	case opAcceptCertificateTransfer:
		return true, h.handleAcceptCertificateTransfer(c)
	case opAddThingToBillingGroup:
		return true, h.handleAddThingToBillingGroup(c)
	case opAddThingToThingGroup:
		return true, h.handleAddThingToThingGroup(c)
	case opAssociateSbomWithPackageVersion:
		return true, h.handleAssociateSbomWithPackageVersion(c)
	case opAssociateTargetsWithJob:
		return true, h.handleAssociateTargetsWithJob(c)
	case opAttachPolicy:
		return true, h.handleAttachPolicy(c)
	case opAttachSecurityProfile:
		return true, h.handleAttachSecurityProfile(c)
	case opAttachThingPrincipal:
		return true, h.handleAttachThingPrincipal(c)
	case opCancelAuditMitigationActionsTask:
		return true, h.handleCancelAuditMitigationActionsTask(c)
	case opCancelAuditTask:
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
		return c.JSON(http.StatusNotFound, map[string]string{keyError: err.Error()})
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, map[string]string{keyError: err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
	}
}

func (h *Handler) handleCreateThing(c *echo.Context) error {
	thingName := strings.TrimPrefix(c.Request().URL.Path, "/things/")

	var body struct {
		AttributePayload *AttributePayload `json:"attributePayload"`
		ThingTypeName    string            `json:"thingTypeName"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
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
		keyThingName: out.ThingName,
		keyThingArn:  out.ThingARN,
		"thingId":    out.ThingID,
	})
}

func (h *Handler) handleDescribeThing(c *echo.Context) error {
	thingName := strings.TrimPrefix(c.Request().URL.Path, "/things/")

	t, err := h.Backend.DescribeThing(thingName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyThingName:    t.ThingName,
		keyThingArn:     t.ARN,
		"thingId":       t.ThingID,
		"thingTypeName": t.ThingTypeName,
		"attributes":    t.Attributes,
		"version":       t.Version,
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
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
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
			"ruleName":         r.RuleName,
			"sql":              r.SQL,
			"awsIotSqlVersion": r.AWSIoTSQLVersion,
			"description":      r.Description,
			"actions":          r.Actions,
			"ruleDisabled":     !r.Enabled,
			"createdAt":        r.CreatedAt,
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
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleCreatePolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/policies/")

	var body struct {
		PolicyDocument string `json:"policyDocument"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	out, err := h.Backend.CreatePolicy(&CreatePolicyInput{
		PolicyName:     policyName,
		PolicyDocument: body.PolicyDocument,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		keyPolicyName:    out.PolicyName,
		keyPolicyArn:     out.PolicyARN,
		"policyDocument": out.PolicyDocument,
	})
}

func (h *Handler) handleDescribeEndpoint(c *echo.Context) error {
	endpointType := c.QueryParam("endpointType")

	out, err := h.Backend.DescribeEndpoint(endpointType)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
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
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	if err := h.Backend.AcceptCertificateTransfer(&AcceptCertificateTransferInput{
		CertificateID: certID,
		SetAsActive:   body.SetAsActive,
	}); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleAddThingToBillingGroup(c *echo.Context) error {
	var body AddThingToBillingGroupInput

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	if err := h.Backend.AddThingToBillingGroup(&body); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleAddThingToThingGroup(c *echo.Context) error {
	var body AddThingToThingGroupInput

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	if err := h.Backend.AddThingToThingGroup(&body); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
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
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	out, err := h.Backend.AssociateSbomWithPackageVersion(&AssociateSbomWithPackageVersionInput{
		PackageName: packageName,
		VersionName: versionName,
		Sbom:        body.Sbom,
	})
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
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
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	out, err := h.Backend.AssociateTargetsWithJob(&AssociateTargetsWithJobInput{
		JobID:       jobID,
		Targets:     body.Targets,
		Comment:     body.Comment,
		NamespaceID: body.NamespaceID,
	})
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) handleAttachPolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/target-policies/")

	var body struct {
		Target string `json:"target"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	if err := h.Backend.AttachPolicy(&AttachPolicyInput{
		PolicyName: policyName,
		Target:     body.Target,
	}); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
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
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
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
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
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
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
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
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetPolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/policies/")

	out, err := h.Backend.GetPolicy(policyName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		keyPolicyName:    out.PolicyName,
		keyPolicyArn:     out.PolicyARN,
		"policyDocument": out.PolicyDocument,
	})
}

func (h *Handler) handleDeletePolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/policies/")

	if err := h.Backend.DeletePolicy(policyName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListPolicies(c *echo.Context) error {
	policies := h.Backend.ListPolicies()

	out := make([]map[string]string, 0, len(policies))
	for _, p := range policies {
		out = append(out, map[string]string{
			keyPolicyName: p.PolicyName,
			keyPolicyArn:  p.ARN,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"policies": out})
}

func (h *Handler) handleListThings(c *echo.Context) error {
	things := h.Backend.ListThings()

	out := make([]map[string]any, 0, len(things))
	for _, t := range things {
		out = append(out, map[string]any{
			keyThingName:    t.ThingName,
			keyThingArn:     t.ARN,
			"thingTypeName": t.ThingTypeName,
			"attributes":    t.Attributes,
			"version":       t.Version,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"things": out})
}

func (h *Handler) handleListTopicRules(c *echo.Context) error {
	rules := h.Backend.ListTopicRules()

	out := make([]map[string]any, 0, len(rules))
	for _, r := range rules {
		out = append(out, map[string]any{
			"ruleName":     r.RuleName,
			"ruleArn":      r.ARN,
			"sql":          r.SQL,
			"ruleDisabled": !r.Enabled,
			"createdAt":    r.CreatedAt,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"rules": out})
}

func (h *Handler) handleUpdateThing(c *echo.Context) error {
	thingName := strings.TrimPrefix(c.Request().URL.Path, "/things/")

	var body UpdateThingInput

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	body.ThingName = thingName

	if err := h.Backend.UpdateThing(&body); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDisableTopicRule(c *echo.Context) error {
	// Path: /rules/{ruleName}/disable
	after := strings.TrimPrefix(c.Request().URL.Path, "/rules/")
	ruleName := strings.TrimSuffix(after, "/disable")

	if err := h.Backend.DisableTopicRule(ruleName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleEnableTopicRule(c *echo.Context) error {
	// Path: /rules/{ruleName}/enable
	after := strings.TrimPrefix(c.Request().URL.Path, "/rules/")
	ruleName := strings.TrimSuffix(after, "/enable")

	if err := h.Backend.EnableTopicRule(ruleName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleReplaceTopicRule(c *echo.Context) error {
	ruleName := strings.TrimPrefix(c.Request().URL.Path, "/rules/")

	var payload TopicRulePayload

	if err := json.NewDecoder(c.Request().Body).Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	if err := h.Backend.ReplaceTopicRule(&ReplaceTopicRuleInput{
		RuleName:         ruleName,
		TopicRulePayload: &payload,
	}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListThingPrincipals(c *echo.Context) error {
	// Path: /things/{thingName}/principals
	after := strings.TrimPrefix(c.Request().URL.Path, "/things/")
	thingName := strings.TrimSuffix(after, "/principals")

	principals, err := h.Backend.ListThingPrincipals(thingName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"principals": principals})
}
