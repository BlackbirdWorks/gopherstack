package iot

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// matchIoTPath reports whether path belongs to the IoT control-plane.
func matchIoTPath(path string) bool {
	return matchCoreIoTPath(path) || matchNewIoTPath(path) || matchBatch4Path(path) ||
		matchFinalOpsPath(path) || matchTaggableResourcePath(path)
}

// matchTaggableResourcePath reports whether path belongs to one of the
// resource families gopherstack-2mwl found entirely missing from this
// route matcher -- the same previously-undiscovered unreachable-op bug
// class documented on matchJobAndTemplatePath below, but far larger:
// RouteMatcher is evaluated before op dispatch, so a real client's request
// to any of these paths 404'd regardless of resolveOperation/the handler
// being correct. Critically this included "/tags" itself, making
// TagResource/UntagResource/ListTagsForResource unreachable for every
// taggable resource kind, plus the Create path for every tag-accepting
// resource family not already covered elsewhere in this file (authorizers,
// billing groups, commands, custom metrics, dimensions, domain
// configurations, dynamic thing groups, fleet metrics, OTA updates,
// packages, provisioning templates, role aliases, streams).
func matchTaggableResourcePath(path string) bool {
	return matchTaggableResourcePathA(path) || matchTaggableResourcePathB(path)
}

func matchTaggableResourcePathA(path string) bool {
	return path == pathTags ||
		path == "/untag" ||
		path == "/authorizers" ||
		strings.HasPrefix(path, "/authorizer/") ||
		path == "/billing-groups" ||
		strings.HasPrefix(path, "/billing-groups/") ||
		path == "/commands" ||
		strings.HasPrefix(path, "/commands/") ||
		path == "/custom-metrics" ||
		strings.HasPrefix(path, "/custom-metric/") ||
		path == "/dimensions" ||
		strings.HasPrefix(path, "/dimensions/")
}

func matchTaggableResourcePathB(path string) bool {
	return path == "/domainConfigurations" ||
		strings.HasPrefix(path, "/domainConfigurations/") ||
		strings.HasPrefix(path, "/dynamic-thing-groups/") ||
		path == "/fleet-metrics" ||
		strings.HasPrefix(path, "/fleet-metric/") ||
		path == "/otaUpdates" ||
		strings.HasPrefix(path, "/otaUpdates/") ||
		path == "/packages" ||
		path == pathProvisioningTemplates ||
		strings.HasPrefix(path, "/provisioning-templates/") ||
		path == "/role-aliases" ||
		strings.HasPrefix(path, "/role-aliases/") ||
		path == "/streams" ||
		strings.HasPrefix(path, "/streams/")
}

// matchBatch4Path reports whether path belongs to the batch-4 routes (bulk
// thing registration tasks and managed job templates).
func matchBatch4Path(path string) bool {
	return path == pathThingRegistrationTasks ||
		strings.HasPrefix(path, pathThingRegistrationTasks+"/") ||
		path == pathManagedJobTemplates ||
		strings.HasPrefix(path, pathManagedJobTemplates+"/")
}

func matchCoreIoTPath(path string) bool {
	return matchCoreIoTPathPrimary(path) || matchCoreIoTPathSecondary(path)
}

// isThingShadowPath reports whether path is iotdataplane's real Get/Update/
// DeleteThingShadow shape: "/things/{thingName}/shadow", optionally followed
// by "/name/{shadowName}" (the named-shadow path variant; the query-string
// variant leaves the path exactly at "/shadow" since URL.Path excludes the
// query). Mirrors services/iotdataplane's isShadowPath.
func isThingShadowPath(path string) bool {
	after, ok := strings.CutPrefix(path, "/things/")
	if !ok {
		return false
	}

	idx := strings.Index(after, "/shadow")
	if idx <= 0 {
		return false
	}

	rest := after[idx+len("/shadow"):]

	return rest == "" || strings.HasPrefix(rest, "/name/")
}

func matchCoreIoTPathPrimary(path string) bool {
	return strings.HasPrefix(path, "/things/") ||
		path == pathThings ||
		strings.HasPrefix(path, "/rules/") ||
		path == "/rules" ||
		strings.HasPrefix(path, "/target-policies/") ||
		strings.HasPrefix(path, pathPolicies+"/") ||
		path == pathPolicies ||
		path == "/endpoint" ||
		strings.HasPrefix(path, "/accept-certificate-transfer/") ||
		strings.HasPrefix(path, "/packages/") ||
		strings.HasPrefix(path, "/jobs/")
}

// matchCoreIoTPathSecondary covers the job-template, security-profile,
// audit, and mitigation-action route families. Split out of
// matchCoreIoTPath to keep that function's cyclomatic complexity down.
// RouteMatcher is a separate, earlier gate than op dispatch — a path
// missing here never reaches the handler regardless of whether
// resolveSecurityProfileOps et al. would handle it correctly, and only a
// real SDK client round-tripped through service.Router catches the gap
// (direct h.Handler() calls bypass RouteMatcher entirely).
func matchCoreIoTPathSecondary(path string) bool {
	return matchJobAndTemplatePath(path) ||
		strings.HasPrefix(path, "/security-profiles/") ||
		path == "/security-profiles" ||
		path == "/security-profiles-for-target" ||
		strings.HasPrefix(path, "/audit/") ||
		strings.HasPrefix(path, "/mitigationactions/") ||
		matchDeviceDefenderPath(path)
}

// matchJobAndTemplatePath reports whether path is one of ListJobs (GET
// /jobs, no trailing slash) or the /job-templates family
// (CreateJobTemplate/DescribeJobTemplate/ListJobTemplates/
// DeleteJobTemplate). Both were entirely absent from this route matcher --
// the same previously-undiscovered unreachable-op bug class as
// "/mitigationactions/" above. Fixed this pass.
func matchJobAndTemplatePath(path string) bool {
	return path == "/jobs" ||
		path == "/job-templates" ||
		strings.HasPrefix(path, "/job-templates/")
}

// matchDeviceDefenderPath reports whether path belongs to the Device Defender
// detect mitigation-action or violation routes.
func matchDeviceDefenderPath(path string) bool {
	return strings.HasPrefix(path, "/detect/") ||
		path == "/active-violations" ||
		path == "/violation-events" ||
		strings.HasPrefix(path, "/violations/")
}

func matchNewIoTPath(path string) bool {
	return path == "/billing-groups/addThingToBillingGroup" ||
		strings.HasPrefix(path, "/thing-groups/") ||
		path == "/thing-groups" ||
		strings.HasPrefix(path, "/thing-types/") ||
		path == "/thing-types" ||
		matchNewIoTCertAndIndexPath(path)
}

func matchNewIoTCertAndIndexPath(path string) bool {
	return strings.HasPrefix(path, "/certificates/") ||
		path == "/certificates" ||
		path == "/certificate/register" ||
		path == "/certificate/register-no-ca" ||
		strings.HasPrefix(path, pathRuleDestinations+"/") ||
		path == pathRuleDestinations ||
		strings.HasPrefix(path, "/certificate-providers/") ||
		path == "/certificate-providers" ||
		path == pathIndices ||
		strings.HasPrefix(path, pathIndices+"/") ||
		path == pathIndexingConfig
}

// resolveOperation resolves the AWS IoT control-plane operation name for the given
// request path and HTTP method.
func resolveOperation(path, method string) string {
	if op := resolveCoreOperation(path, method); op != unknownOperation {
		return op
	}

	if op := resolvePolicyVersionOps(path, method); op != unknownOperation {
		return op
	}

	if op := resolvePolicyAndCertOps(path, method); op != unknownOperation {
		return op
	}

	if op := resolveGroupAndPackageOps(path, method); op != unknownOperation {
		return op
	}

	if op := resolveNewStatefulOps(path, method); op != unknownOperation {
		return op
	}

	if op := resolveJobAndAuditOps(path, method); op != unknownOperation {
		return op
	}

	if op := resolveBatch4Ops(path, method); op != unknownOperation {
		return op
	}

	if op := resolveDeviceDefenderOps(path, method); op != unknownOperation {
		return op
	}

	if op := resolveFinalOps(path, method); op != unknownOperation {
		return op
	}

	return resolveBatch1Ops(path, method)
}

// resolveCoreOperation resolves the handful of special-cased top-level paths (thing
// listing/registration, named shadows, thing-group/job shortcuts under /things/, rule
// listing, the account endpoint and fleet indexing) before falling through to the
// per-family resolvers.
func resolveCoreOperation(path, method string) string {
	switch {
	case path == pathThings && method == http.MethodGet:

		return opListThings
	// /things/register must be checked before generic /things/{name} routing.
	case path == "/things/register" && method == http.MethodPost:

		return opRegisterThing
	// ListNamedShadowsForThing uses a special /api/things/shadow/ prefix.
	case strings.HasPrefix(path, "/api/things/shadow/ListNamedShadowsForThing/"):

		return opListNamedShadowsForThing
	case strings.HasPrefix(path, "/things/"):

		return resolveThingsPathOperation(path, method)
	case path == "/rules" && method == http.MethodGet:

		return opListTopicRules
	case strings.HasPrefix(path, "/rules/"):

		return ruleOperation(path, method)
	case path == "/endpoint" && method == http.MethodGet:

		return opDescribeEndpoint
	case path == pathIndices || strings.HasPrefix(path, pathIndices+"/") || path == pathIndexingConfig:

		return resolveIndexOps(path, method)
	}

	return unknownOperation
}

// resolveThingsPathOperation resolves the sub-routes under /things/{name}/... that must be
// checked before the generic per-thing CRUD routing in thingOperation.
func resolveThingsPathOperation(path, method string) string {
	switch {
	// Batch 2: /things/{name}/thing-groups, /things/{name}/jobs before generic thing routing
	case strings.HasPrefix(path, "/things/") &&
		strings.HasSuffix(path, "/thing-groups") &&
		method == http.MethodGet:

		return opListThingGroupsForThing
	case strings.HasPrefix(path, "/things/") &&
		strings.HasSuffix(path, "/jobs") &&
		method == http.MethodGet:

		return opListJobExecutionsForThing
	// DescribeJobExecution/CancelJobExecution/DeleteJobExecution use
	// /things/{thingName}/jobs/{jobId}[...] -- confirmed against
	// aws-sdk-go-v2/service/iot@v1.77.4's serializers.go http bindings
	// (SplitURI("/things/{thingName}/jobs/{jobId}[/cancel|/executionNumber/{executionNumber}]")).
	// Must come before the generic thing routing default below, which would
	// otherwise silently swallow these as unmatched thing sub-resource paths.
	case strings.HasPrefix(path, "/things/") && strings.Contains(path, "/jobs/"):

		return resolveThingJobExecutionOps(path, method)
	// Shadow ops use /things/{name}/shadow — must come before generic thing routing.
	case strings.HasPrefix(path, "/things/") && strings.HasSuffix(path, "/shadow"):

		return shadowOperation(method)
	default:

		return thingOperation(path, method)
	}
}

// resolveThingJobExecutionOps resolves the /things/{thingName}/jobs/{jobId}[...]
// sub-routes for DescribeJobExecution/CancelJobExecution/DeleteJobExecution.
// Callers must have already verified path contains "/jobs/".
func resolveThingJobExecutionOps(path, method string) string {
	switch {
	case strings.HasSuffix(path, "/cancel") && method == http.MethodPut:
		return opCancelJobExecution
	case strings.Contains(path, "/executionNumber/") && method == http.MethodDelete:
		return opDeleteJobExecution
	case method == http.MethodGet:
		return opDescribeJobExecution
	}

	return unknownOperation
}

func resolveBatch1Ops(path, method string) string {
	if op := resolveJobOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveJobTemplateOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveRoleAliasOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveDomainConfigOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveProvisioningTemplateOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveAuthorizerOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveBillingGroupOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveScheduledAuditOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveMitigationActionOps(path, method); op != unknownOperation {
		return op
	}

	if op := resolveSecurityProfileOps(path, method); op != unknownOperation {
		return op
	}

	return resolveBatch2Ops(path, method)
}

func resolveBatch2Ops(path, method string) string {
	if op := resolveCACertOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveStreamOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveFleetMetricOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveCustomMetricOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveDimensionOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveTagOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveAuditConfigOps(path, method); op != unknownOperation {
		return op
	}

	if op := resolveDeviceFleetMiscOps(path, method); op != unknownOperation {
		return op
	}

	return resolveBatch3Op(path, method)
}

// resolveThingPrincipalDetachOps resolves DetachThingPrincipal.
func resolveThingPrincipalDetachOps(path, method string) string {
	if strings.HasPrefix(path, "/things/") &&
		strings.HasSuffix(path, "/principals") &&
		method == http.MethodDelete {
		return opDetachThingPrincipal
	}

	return unknownOperation
}

// resolveDeviceFleetMiscOps resolves the misc path-based ops that don't have their own
// dedicated per-family resolver: thing-principal detach, certificate transfer, thing
// registration code, billing-group/thing-group membership, policy-principal listings,
// default authorizer and job-execution listings.
func resolveDeviceFleetMiscOps(path, method string) string {
	if op := resolveThingPrincipalDetachOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveCertTransferPathOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveThingRegistrationCodeOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveBillingGroupMiscPathOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveThingGroupForThingOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolvePolicyPrincipalPathOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveDefaultAuthorizerPathOps(path, method); op != unknownOperation {
		return op
	}

	return resolveJobExecutionPathOps(path, method)
}

func resolveNewStatefulOps(path, method string) string {
	if op := resolveThingTypeOps(path, method); op != unknownOperation {
		return op
	}

	if op := resolveThingGroupOps(path, method); op != unknownOperation {
		return op
	}

	if op := resolveCertificateOps(path, method); op != unknownOperation {
		return op
	}

	if op := resolvePolicyVersionOps(path, method); op != unknownOperation {
		return op
	}

	if op := resolveTopicRuleDestinationOps(path, method); op != unknownOperation {
		return op
	}

	return resolveCertificateProviderOps(path, method)
}

func resolvePolicyAndCertOps(path, method string) string {
	switch {
	case strings.HasPrefix(path, "/target-policies/") && method == http.MethodPost:

		return opAttachPrincipalPolicy
	case strings.HasPrefix(path, "/target-policies/") && method == http.MethodPut:

		return opAttachPolicy
	case path == pathPolicies && method == http.MethodGet:

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
	case strings.HasPrefix(path, "/packages/") &&
		strings.HasSuffix(path, "/sbom") &&
		method == http.MethodDelete:

		return opDisassociateSbomFromPackageVersion
	case strings.HasPrefix(path, "/packages/") &&
		strings.HasSuffix(path, "/sbom-validation-results") &&
		method == http.MethodGet:

		return opListSbomValidationResults
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

func shadowOperation(method string) string {
	switch method {
	case http.MethodGet:
		return opGetThingShadow
	case http.MethodPost:
		return opUpdateThingShadow
	case http.MethodDelete:
		return opDeleteThingShadow
	}

	return unknownOperation
}

func thingOperation(path, method string) string {
	// GET /things/{thingName}/principals-v2 → ListThingPrincipalsV2
	// (must be checked before the "/principals" suffix below.)
	if method == http.MethodGet && strings.HasSuffix(path, "/principals-v2") {
		return opListThingPrincipalsV2
	}

	// GET /things/{thingName}/connectivity-data → GetThingConnectivityData
	if method == http.MethodGet && strings.HasSuffix(path, "/connectivity-data") {
		return opGetThingConnectivityData
	}

	// GET /things/{thingName}/principals → ListThingPrincipals
	if method == http.MethodGet && strings.HasSuffix(path, "/principals") {
		return opListThingPrincipals
	}

	// PUT /things/{thingName}/principals → AttachThingPrincipal
	if method == http.MethodPut && strings.HasSuffix(path, "/principals") {
		return opAttachThingPrincipal
	}

	// POST /things/{thingName}/connectivity-data → GetThingConnectivityData
	if method == http.MethodPost && strings.HasSuffix(path, "/connectivity-data") {
		return opGetThingConnectivityData
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
func (h *Handler) dispatchNewOp(c *echo.Context, op string) (bool, error) {
	if handled, err := h.dispatchIndexOps(c, op); handled {
		return true, err
	}

	if handled, err := h.dispatchMiscNewOps(c, op); handled {
		return true, err
	}

	if handled, err := h.dispatchThingTypeOps(c, op); handled {
		return true, err
	}

	if handled, err := h.dispatchThingGroupOps(c, op); handled {
		return true, err
	}

	if handled, err := h.dispatchCertificateOps(c, op); handled {
		return true, err
	}

	if handled, err := h.dispatchPolicyVersionOps(c, op); handled {
		return true, err
	}

	if handled, err := h.dispatchTopicRuleDestinationOps(c, op); handled {
		return true, err
	}

	if handled, err := h.dispatchCertificateProviderOps(c, op); handled {
		return true, err
	}

	if handled, err := h.dispatchExtendedResourceOps(c, op); handled {
		return true, err
	}

	if handled, err := h.dispatchDeviceFleetOps(c, op); handled {
		return true, err
	}

	if handled, err := h.dispatchDeviceManagementOps(c, op); handled {
		return true, err
	}

	if handled, err := h.dispatchBatch4Ops(c, op); handled {
		return true, err
	}

	if handled, err := h.dispatchDeviceDefenderOps(c, op); handled {
		return true, err
	}

	return h.dispatchFinalOps(c, op)
}

// dispatchExtendedResourceOps dispatches jobs, provisioning, authorizer, billing-group,
// scheduled-audit, mitigation-action and security-profile CRUD operations.
// opDispatchFunc dispatches a single op family; it reports whether op was handled.
type opDispatchFunc func(c *echo.Context, op string) (bool, error)

// dispatchFirstMatch tries each dispatcher in order and returns the result of the first
// one that reports it handled the operation.
func dispatchFirstMatch(c *echo.Context, op string, dispatchers []opDispatchFunc) (bool, error) {
	for _, dispatch := range dispatchers {
		if handled, err := dispatch(c, op); handled {
			return true, err
		}
	}

	return false, nil
}

func (h *Handler) dispatchExtendedResourceOps(c *echo.Context, op string) (bool, error) {
	return dispatchFirstMatch(c, op, []opDispatchFunc{
		h.dispatchJobOps,
		h.dispatchJobTemplateOps,
		h.dispatchRoleAliasOps,
		h.dispatchDomainConfigOps,
		h.dispatchProvisioningTemplateOps,
		h.dispatchAuthorizerOps,
		h.dispatchBillingGroupOps,
		h.dispatchScheduledAuditOps,
		h.dispatchMitigationActionOps,
		h.dispatchSecurityProfileOps,
	})
}

// dispatchDeviceFleetOps dispatches CA certificate, stream, fleet metric, custom metric,
// dimension, tag, audit-config, thing-registration-code and policy-principal-listing operations.
func (h *Handler) dispatchDeviceFleetOps(c *echo.Context, op string) (bool, error) {
	return dispatchFirstMatch(c, op, []opDispatchFunc{
		h.dispatchCACertOps,
		h.dispatchStreamOps,
		h.dispatchFleetMetricOps,
		h.dispatchCustomMetricOps,
		h.dispatchDimensionOps,
		h.dispatchTagOps,
		h.dispatchAuditConfigOps,
		h.dispatchThingRegistrationMiscOps,
		h.dispatchPolicyPrincipalOps,
	})
}

// dispatchDeviceManagementOps dispatches OTA update, package, audit suppression/finding,
// logging, certificate-claim, event-config, dynamic-thing-group and command operations.
func (h *Handler) dispatchDeviceManagementOps(c *echo.Context, op string) (bool, error) {
	return dispatchFirstMatch(c, op, []opDispatchFunc{
		h.dispatchOTAUpdateOps,
		h.dispatchPackageOps,
		h.dispatchPackageVersionOps,
		h.dispatchAuditSuppressionOps,
		h.dispatchV2LoggingOps,
		h.dispatchCertClaimOps,
		h.dispatchEventConfigOps,
		h.dispatchDynamicThingGroupOps,
		h.dispatchCommandOps,
	})
}

func (h *Handler) dispatchMiscNewOps(c *echo.Context, op string) (bool, error) {
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
func resolveBatch3MiscOps(path, method string) string {
	if op := resolveBatch3CertOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveBatch3DynamicGroupOps(path, method); op != unknownOperation {
		return op
	}
	switch {
	case path == "/event-configurations" && method == http.MethodGet:

		return opDescribeEventConfigurations
	case path == "/event-configurations" && method == http.MethodPatch:

		return opUpdateEventConfigurations
	}

	return unknownOperation
}

// resolveBatch3Op tries all batch-3 route resolvers and returns the op name.
func resolveBatch3Op(path, method string) string {
	if op := resolveOTAUpdateOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolvePackageOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveAuditSuppressionOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveV2LoggingOps(path, method); op != unknownOperation {
		return op
	}
	if op := resolveCommandOps(path, method); op != unknownOperation {
		return op
	}

	return resolveBatch3MiscOps(path, method)
}

// resolveBatch4Ops resolves all batch 4 routes.
//
// Note: ListThingPrincipalsV2 (GET /things/{name}/principals-v2) is resolved
// directly inside thingOperation in handler.go, since the top-level
// resolveOperation switch dispatches any "/things/"-prefixed path there
// before this function ever runs.
func resolveBatch4Ops(path, method string) string {
	if op := resolveThingRegistrationOps(path, method); op != unknownOperation {
		return op
	}

	if op := resolveManagedJobTemplateOps(path, method); op != unknownOperation {
		return op
	}

	if op := resolveThingTypeUpdateOp(path, method); op != unknownOperation {
		return op
	}

	return resolveThingGroupUpdateOp(path, method)
}

func (h *Handler) dispatchBatch4Ops(c *echo.Context, op string) (bool, error) {
	switch op {
	case opRegisterThing:
		return true, h.handleRegisterThing(c)
	case opStartThingRegistrationTask:
		return true, h.handleStartThingRegistrationTask(c)
	case opStopThingRegistrationTask:
		return true, h.handleStopThingRegistrationTask(c)
	case opListThingRegistrationTasks:
		return true, h.handleListThingRegistrationTasks(c)
	case opDescribeThingRegistrationTask:
		return true, h.handleDescribeThingRegistrationTask(c)
	case opListThingRegistrationTaskReports:
		return true, h.handleListThingRegistrationTaskReports(c)
	case opUpdateThingType:
		return true, h.handleUpdateThingType(c)
	case opUpdateThingGroupsForThing:
		return true, h.handleUpdateThingGroupsForThing(c)
	case opListThingPrincipalsV2:
		return true, h.handleListThingPrincipalsV2(c)
	case opDescribeManagedJobTemplate:
		return true, h.handleDescribeManagedJobTemplate(c)
	case opListManagedJobTemplates:
		return true, h.handleListManagedJobTemplates(c)
	}

	return false, nil
}

// matchFinalOpsPath reports whether path belongs to one of the final-batch
// routes, for the top-level RouteMatcher.
func matchFinalOpsPath(path string) bool {
	return path == pathEncryptionConfiguration ||
		path == pathTestAuthorization ||
		strings.HasPrefix(path, pathPrincipalPolicies+"/") ||
		strings.HasPrefix(path, pathConfirmDestination+"/") ||
		path == pathCommandExecutions ||
		strings.HasPrefix(path, pathCommandExecutions+"/") ||
		path == pathBehaviorModelTrainingSummaries ||
		path == pathCertificatesOutgoing ||
		path == pathMetricValues ||
		path == pathValidateSecurityProfileBehaviors
}

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
	case strings.HasPrefix(path, pathCommandExecutions+"/") && method == http.MethodGet:
		return opGetCommandExecution
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
