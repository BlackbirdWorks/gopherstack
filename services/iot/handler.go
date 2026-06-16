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
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyError      = "error"
	keyThingName  = "thingName"
	keyThingArn   = "thingArn"
	keyPolicyName = "policyName"
	keyPolicyArn  = "policyArn"

	// JSON field name constants used across handlers.
	keyThingTypeName           = "thingTypeName"
	keyThingTypeArn            = "thingTypeArn"
	keyThingGroupName          = "thingGroupName"
	keyThingGroupArn           = "thingGroupArn"
	keyCertificateID           = "certificateId"
	keyCertificateArn          = "certificateArn"
	keyCertificateProviderName = "certificateProviderName"
	keyCertificateProviderArn  = "certificateProviderArn"
	keyIsDefaultVersion        = "isDefaultVersion"
	keyPolicyDocument          = "policyDocument"
	keyAttributes              = "attributes"
	keyVersion                 = "version"
	keyStatus                  = "status"
	keyArn                     = "arn"
	keyCreationDate            = "creationDate"
	keyLastModifiedDate        = "lastModifiedDate"
	keyPolicyVersionID         = "policyVersionId"
	keyInvalidPath             = "invalid path"

	// URL path prefix constants.
	pathPolicies         = "/policies"
	pathRuleDestinations = "/rule-destinations"
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

// New operation name constants for stateful implementations.
const (
	opCreateThingType              = "CreateThingType"
	opDescribeThingType            = "DescribeThingType"
	opListThingTypes               = "ListThingTypes"
	opDeprecateThingType           = "DeprecateThingType"
	opDeleteThingType              = "DeleteThingType"
	opCreateThingGroup             = "CreateThingGroup"
	opDescribeThingGroup           = "DescribeThingGroup"
	opListThingGroups              = "ListThingGroups"
	opUpdateThingGroup             = "UpdateThingGroup"
	opDeleteThingGroup             = "DeleteThingGroup"
	opRemoveThingFromThingGroup    = "RemoveThingFromThingGroup"
	opListThingsInThingGroup       = "ListThingsInThingGroup"
	opCreateCertificateFromCsr     = "CreateCertificateFromCsr"
	opRegisterCertificate          = "RegisterCertificate"
	opRegisterCertificateWithoutCA = "RegisterCertificateWithoutCA"
	opDescribeCertificate          = "DescribeCertificate"
	opListCertificates             = "ListCertificates"
	opUpdateCertificate            = "UpdateCertificate"
	opDeleteCertificate            = "DeleteCertificate"
	opDetachPolicy                 = "DetachPolicy"
	opListAttachedPolicies         = "ListAttachedPolicies"
	opCreatePolicyVersion          = "CreatePolicyVersion"
	opGetPolicyVersion             = "GetPolicyVersion"
	opListPolicyVersions           = "ListPolicyVersions"
	opDeletePolicyVersion          = "DeletePolicyVersion"
	opSetDefaultPolicyVersion      = "SetDefaultPolicyVersion"
	opCreateTopicRuleDestination   = "CreateTopicRuleDestination"
	opGetTopicRuleDestination      = "GetTopicRuleDestination"
	opListTopicRuleDestinations    = "ListTopicRuleDestinations"
	opUpdateTopicRuleDestination   = "UpdateTopicRuleDestination"
	opDeleteTopicRuleDestination   = "DeleteTopicRuleDestination"
	opCreateCertificateProvider    = "CreateCertificateProvider"
	opDescribeCertificateProvider  = "DescribeCertificateProvider"
	opListCertificateProviders     = "ListCertificateProviders"
	opUpdateCertificateProvider    = "UpdateCertificateProvider"
	opDeleteCertificateProvider    = "DeleteCertificateProvider"
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
//
//nolint:funlen // mechanical list of all supported op names
func (h *Handler) GetSupportedOperations() []string {
	const coreOpCount = 65
	core := make([]string, 0, coreOpCount)
	core = append(
		core,
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
		// ThingType
		opCreateThingType,
		opDescribeThingType,
		opListThingTypes,
		opDeprecateThingType,
		opDeleteThingType,
		// ThingGroup
		opCreateThingGroup,
		opDescribeThingGroup,
		opListThingGroups,
		opUpdateThingGroup,
		opDeleteThingGroup,
		opRemoveThingFromThingGroup,
		opListThingsInThingGroup,
		// Certificate
		opCreateCertificateFromCsr,
		opRegisterCertificate,
		opRegisterCertificateWithoutCA,
		opDescribeCertificate,
		opListCertificates,
		opUpdateCertificate,
		opDeleteCertificate,
		// Policy attachment
		opDetachPolicy,
		opListAttachedPolicies,
		// PolicyVersion
		opCreatePolicyVersion,
		opGetPolicyVersion,
		opListPolicyVersions,
		opDeletePolicyVersion,
		opSetDefaultPolicyVersion,
		// TopicRuleDestination
		opCreateTopicRuleDestination,
		opGetTopicRuleDestination,
		opListTopicRuleDestinations,
		opUpdateTopicRuleDestination,
		opDeleteTopicRuleDestination,
		// CertificateProvider
		opCreateCertificateProvider,
		opDescribeCertificateProvider,
		opListCertificateProviders,
		opUpdateCertificateProvider,
		opDeleteCertificateProvider,
		// Batch 1: Jobs
		opCreateJob,
		opDescribeJob,
		opListJobs,
		opUpdateJob,
		opCancelJob,
		opDeleteJob,
		opGetJobDocument,
		opDescribeJobExecution,
		opCancelJobExecution,
		opDeleteJobExecution,
		// Batch 1: JobTemplates
		opCreateJobTemplate,
		opDescribeJobTemplate,
		opListJobTemplates,
		opDeleteJobTemplate,
		// Batch 1: RoleAliases
		opCreateRoleAlias,
		opDescribeRoleAlias,
		opListRoleAliases,
		opUpdateRoleAlias,
		opDeleteRoleAlias,
		// Batch 1: DomainConfigurations
		opCreateDomainConfiguration,
		opDescribeDomainConfiguration,
		opListDomainConfigurations,
		opUpdateDomainConfiguration,
		opDeleteDomainConfiguration,
		// Batch 1: ProvisioningTemplates
		opCreateProvisioningTemplate,
		opDescribeProvisioningTemplate,
		opListProvisioningTemplates,
		opUpdateProvisioningTemplate,
		opDeleteProvisioningTemplate,
		opCreateProvisioningTemplateVersion,
		opListProvisioningTemplateVersions,
		opDeleteProvisioningTemplateVersion,
		// Batch 1: Authorizers
		opCreateAuthorizer,
		opDescribeAuthorizer,
		opListAuthorizers,
		opUpdateAuthorizer,
		opDeleteAuthorizer,
		// Batch 1: BillingGroups
		opCreateBillingGroup,
		opDescribeBillingGroup,
		opListBillingGroups,
		opUpdateBillingGroup,
		opDeleteBillingGroup,
		// Batch 1: ScheduledAudits
		opCreateScheduledAudit,
		opDescribeScheduledAudit,
		opListScheduledAudits,
		opUpdateScheduledAudit,
		opDeleteScheduledAudit,
		// Batch 1: MitigationActions
		opCreateMitigationAction,
		opDescribeMitigationAction,
		opListMitigationActions,
		opUpdateMitigationAction,
		opDeleteMitigationAction,
		// Batch 1: SecurityProfiles
		opCreateSecurityProfile,
		opDescribeSecurityProfile,
		opListSecurityProfiles,
		opUpdateSecurityProfile,
		opDeleteSecurityProfile,
		// Batch 2: CACertificate
		opRegisterCACertificate,
		opDescribeCACertificate,
		opListCACertificates,
		opUpdateCACertificate,
		opDeleteCACertificate,
		opListCertificatesByCA,
		// Batch 2: Stream
		opCreateStream,
		opDescribeStream,
		opListStreams,
		opUpdateStream,
		opDeleteStream,
		// Batch 2: FleetMetric
		opCreateFleetMetric,
		opDescribeFleetMetric,
		opListFleetMetrics,
		opUpdateFleetMetric,
		opDeleteFleetMetric,
		// Batch 2: CustomMetric
		opCreateCustomMetric,
		opDescribeCustomMetric,
		opListCustomMetrics,
		opUpdateCustomMetric,
		opDeleteCustomMetric,
		// Batch 2: Dimension
		opCreateDimension,
		opDescribeDimension,
		opListDimensions,
		opUpdateDimension,
		opDeleteDimension,
		// Batch 2: Tags
		opTagResource,
		opUntagResource,
		opListTagsForResource,
		// Batch 2: Audit
		opDescribeAccountAuditConfiguration,
		opUpdateAccountAuditConfiguration,
		opStartOnDemandAuditTask,
		opDescribeAuditTask,
		opListAuditTasks,
		// Batch 2: Misc
		opDetachThingPrincipal,
		opCancelCertificateTransfer,
		opGetRegistrationCode,
		opDeleteRegistrationCode,
		opListThingGroupsForThing,
		opListThingsInBillingGroup,
		opRemoveThingFromBillingGroup,
		opListPrincipalPolicies,
		opListPolicyPrincipals,
		opListTargetsForPolicy,
		opListPrincipalThings,
		opListPrincipalThingsV2,
		opGetEffectivePolicies,
		opSetDefaultAuthorizer,
		opClearDefaultAuthorizer,
		opDescribeDefaultAuthorizer,
		opListJobExecutionsForJob,
		opListJobExecutionsForThing,
	)

	return append(core, allStubOps()...)
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "iot" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this IoT instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path
		if path == pathPolicies || strings.HasPrefix(path, pathPolicies+"/") {
			svc := httputils.ExtractServiceFromRequest(c.Request())

			return svc == "" || svc == "iot"
		}

		return matchIoTPath(path)
	}
}

// matchIoTPath reports whether path belongs to the IoT control-plane.
func matchIoTPath(path string) bool {
	return matchCoreIoTPath(path) || matchNewIoTPath(path)
}

func matchCoreIoTPath(path string) bool {
	return strings.HasPrefix(path, "/things/") ||
		path == "/things" ||
		strings.HasPrefix(path, "/rules/") ||
		path == "/rules" ||
		strings.HasPrefix(path, "/target-policies/") ||
		strings.HasPrefix(path, pathPolicies+"/") ||
		path == pathPolicies ||
		path == "/endpoint" ||
		strings.HasPrefix(path, "/accept-certificate-transfer/") ||
		strings.HasPrefix(path, "/packages/") ||
		strings.HasPrefix(path, "/jobs/") ||
		strings.HasPrefix(path, "/security-profiles/") ||
		strings.HasPrefix(path, "/audit/")
}

func matchNewIoTPath(path string) bool {
	return path == "/billing-groups/addThingToBillingGroup" ||
		strings.HasPrefix(path, "/thing-groups/") ||
		path == "/thing-groups" ||
		strings.HasPrefix(path, "/thing-types/") ||
		path == "/thing-types" ||
		strings.HasPrefix(path, "/certificates/") ||
		path == "/certificates" ||
		path == "/certificate/register" ||
		path == "/certificate/register-no-ca" ||
		strings.HasPrefix(path, pathRuleDestinations+"/") ||
		path == pathRuleDestinations ||
		strings.HasPrefix(path, "/certificate-providers/") ||
		path == "/certificate-providers"
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

//nolint:cyclop // mechanical path-based routing switch
func resolveOperation(path, method string) string {
	switch {
	case path == "/things" && method == http.MethodGet:

		return opListThings
	// Batch 2: /things/{name}/thing-groups, /things/{name}/jobs before generic thing routing
	case strings.HasPrefix(path, "/things/") &&
		strings.HasSuffix(path, "/thing-groups") &&
		method == http.MethodGet:

		return opListThingGroupsForThing
	case strings.HasPrefix(path, "/things/") &&
		strings.HasSuffix(path, "/jobs") &&
		method == http.MethodGet:

		return opListJobExecutionsForThing
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

	if op := resolveNewStatefulOps(path, method); op != unknownOperation {
		return op
	}

	if op := resolveJobAndAuditOps(path, method); op != unknownOperation {
		return op
	}

	return resolveBatch1Ops(path, method)
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

	if op := resolveMiscBatch2Ops(path, method); op != unknownOperation {
		return op
	}

	return resolveBatch3Op(path, method)
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

func resolveThingTypeOps(path, method string) string {
	switch {
	case path == "/thing-types" && method == http.MethodGet:

		return opListThingTypes
	case strings.HasPrefix(path, "/thing-types/") && strings.HasSuffix(path, "/deprecate") && method == http.MethodPost:

		return opDeprecateThingType
	case strings.HasPrefix(path, "/thing-types/") && method == http.MethodPost:

		return opCreateThingType
	case strings.HasPrefix(path, "/thing-types/") && method == http.MethodGet:

		return opDescribeThingType
	case strings.HasPrefix(path, "/thing-types/") && method == http.MethodDelete:

		return opDeleteThingType
	}

	return unknownOperation
}

func resolveThingGroupOps(path, method string) string {
	// Handle exact and special-suffix paths first.
	if op := resolveThingGroupSpecialPaths(path, method); op != unknownOperation {
		return op
	}

	// Handle generic /thing-groups/{name} CRUD paths.
	if !strings.HasPrefix(path, "/thing-groups/") {
		return unknownOperation
	}

	switch method {
	case http.MethodPost:

		return opCreateThingGroup
	case http.MethodGet:

		return opDescribeThingGroup
	case http.MethodPatch:

		return opUpdateThingGroup
	case http.MethodDelete:

		return opDeleteThingGroup
	}

	return unknownOperation
}

func resolveThingGroupSpecialPaths(path, method string) string {
	switch {
	case path == "/thing-groups" && method == http.MethodGet:

		return opListThingGroups
	case path == "/thing-groups/removeThingFromThingGroup" && method == http.MethodPut:

		return opRemoveThingFromThingGroup
	case strings.HasSuffix(path, "/things") && method == http.MethodGet &&
		strings.HasPrefix(path, "/thing-groups/"):

		return opListThingsInThingGroup
	}

	return unknownOperation
}

func resolveCertificateOps(path, method string) string {
	switch {
	case path == "/certificates" && method == http.MethodGet:

		return opListCertificates
	case path == "/certificate/register" && method == http.MethodPost:

		return opRegisterCertificate
	case path == "/certificate/register-no-ca" && method == http.MethodPost:

		return opRegisterCertificateWithoutCA
	case strings.HasPrefix(path, "/certificates/") && method == http.MethodPost:

		return opCreateCertificateFromCsr
	case strings.HasPrefix(path, "/certificates/") && method == http.MethodGet:

		return opDescribeCertificate
	case strings.HasPrefix(path, "/certificates/") && method == http.MethodPut:

		return opUpdateCertificate
	case strings.HasPrefix(path, "/certificates/") && method == http.MethodDelete:

		return opDeleteCertificate
	}

	return unknownOperation
}

func resolvePolicyVersionOps(path, method string) string {
	switch {
	case strings.HasPrefix(path, "/target-policies/") && method == http.MethodDelete:

		return opDetachPolicy
	case path == "/attached-policies" && method == http.MethodPost:

		return opListAttachedPolicies
	}

	return resolvePolicyVersionSubOps(path, method)
}

func resolvePolicyVersionSubOps(path, method string) string {
	if !strings.HasPrefix(path, "/policies/") {
		return unknownOperation
	}

	hasVersionSlash := strings.Contains(path, "/version/")
	endsVersion := strings.HasSuffix(path, "/version")
	endsDefault := strings.HasSuffix(path, "/default")

	return resolvePolicyVersionByMethod(path, method, hasVersionSlash, endsVersion, endsDefault)
}

func resolvePolicyVersionByMethod(
	path, method string,
	hasVersionSlash, endsVersion, endsDefault bool,
) string {
	switch method {
	case http.MethodGet:
		if hasVersionSlash {
			return opGetPolicyVersion
		}

		if endsVersion {
			return opListPolicyVersions
		}
	case http.MethodDelete:
		if hasVersionSlash {
			return opDeletePolicyVersion
		}
	case http.MethodPatch:
		if endsDefault {
			return opSetDefaultPolicyVersion
		}
	case http.MethodPost:
		if strings.Contains(path, "/version") && !endsDefault {
			return opCreatePolicyVersion
		}
	}

	return unknownOperation
}

func resolveTopicRuleDestinationOps(path, method string) string {
	switch {
	case path == pathRuleDestinations && method == http.MethodPost:

		return opCreateTopicRuleDestination
	case path == pathRuleDestinations && method == http.MethodGet:

		return opListTopicRuleDestinations
	case strings.HasPrefix(path, "/rule-destinations") && method == http.MethodGet:

		return opGetTopicRuleDestination
	case strings.HasPrefix(path, "/rule-destinations") && method == http.MethodPatch:

		return opUpdateTopicRuleDestination
	case strings.HasPrefix(path, "/rule-destinations") && method == http.MethodDelete:

		return opDeleteTopicRuleDestination
	}

	return unknownOperation
}

func resolveCertificateProviderOps(path, method string) string {
	switch {
	case path == "/certificate-providers" && method == http.MethodGet:

		return opListCertificateProviders
	case strings.HasPrefix(path, "/certificate-providers/") && method == http.MethodPost:

		return opCreateCertificateProvider
	case strings.HasPrefix(path, "/certificate-providers/") && method == http.MethodGet:

		return opDescribeCertificateProvider
	case strings.HasPrefix(path, "/certificate-providers/") && method == http.MethodPut:

		return opUpdateCertificateProvider
	case strings.HasPrefix(path, "/certificate-providers/") && method == http.MethodDelete:

		return opDeleteCertificateProvider
	}

	return unknownOperation
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

		if handled, err := h.dispatchStubOp(c, op); handled {
			return err
		}

		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyError: "unknown operation: " + op},
		)
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

	if handled, err := h.dispatchBatch1Ops(c, op); handled {
		return true, err
	}

	if handled, err := h.dispatchBatch2Ops(c, op); handled {
		return true, err
	}

	return h.dispatchBatch3Ops(c, op)
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

func (h *Handler) dispatchThingTypeOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateThingType:

		return true, h.handleCreateThingType(c)
	case opDescribeThingType:

		return true, h.handleDescribeThingType(c)
	case opListThingTypes:

		return true, h.handleListThingTypes(c)
	case opDeprecateThingType:

		return true, h.handleDeprecateThingType(c)
	case opDeleteThingType:

		return true, h.handleDeleteThingType(c)
	}

	return false, nil
}

func (h *Handler) dispatchThingGroupOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateThingGroup:

		return true, h.handleCreateThingGroup(c)
	case opDescribeThingGroup:

		return true, h.handleDescribeThingGroup(c)
	case opListThingGroups:

		return true, h.handleListThingGroups(c)
	case opUpdateThingGroup:

		return true, h.handleUpdateThingGroup(c)
	case opDeleteThingGroup:

		return true, h.handleDeleteThingGroup(c)
	case opRemoveThingFromThingGroup:

		return true, h.handleRemoveThingFromThingGroup(c)
	case opListThingsInThingGroup:

		return true, h.handleListThingsInThingGroup(c)
	}

	return false, nil
}

func (h *Handler) dispatchCertificateOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateCertificateFromCsr:

		return true, h.handleCreateCertificateFromCsr(c)
	case opRegisterCertificate:

		return true, h.handleRegisterCertificate(c)
	case opRegisterCertificateWithoutCA:

		return true, h.handleRegisterCertificateWithoutCA(c)
	case opDescribeCertificate:

		return true, h.handleDescribeCertificate(c)
	case opListCertificates:

		return true, h.handleListCertificates(c)
	case opUpdateCertificate:

		return true, h.handleUpdateCertificate(c)
	case opDeleteCertificate:

		return true, h.handleDeleteCertificate(c)
	}

	return false, nil
}

func (h *Handler) dispatchPolicyVersionOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opDetachPolicy:

		return true, h.handleDetachPolicy(c)
	case opListAttachedPolicies:

		return true, h.handleListAttachedPolicies(c)
	case opCreatePolicyVersion:

		return true, h.handleCreatePolicyVersion(c)
	case opGetPolicyVersion:

		return true, h.handleGetPolicyVersion(c)
	case opListPolicyVersions:

		return true, h.handleListPolicyVersions(c)
	case opDeletePolicyVersion:

		return true, h.handleDeletePolicyVersion(c)
	case opSetDefaultPolicyVersion:

		return true, h.handleSetDefaultPolicyVersion(c)
	}

	return false, nil
}

func (h *Handler) dispatchTopicRuleDestinationOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateTopicRuleDestination:

		return true, h.handleCreateTopicRuleDestination(c)
	case opGetTopicRuleDestination:

		return true, h.handleGetTopicRuleDestination(c)
	case opListTopicRuleDestinations:

		return true, h.handleListTopicRuleDestinations(c)
	case opUpdateTopicRuleDestination:

		return true, h.handleUpdateTopicRuleDestination(c)
	case opDeleteTopicRuleDestination:

		return true, h.handleDeleteTopicRuleDestination(c)
	}

	return false, nil
}

func (h *Handler) dispatchCertificateProviderOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateCertificateProvider:

		return true, h.handleCreateCertificateProvider(c)
	case opDescribeCertificateProvider:

		return true, h.handleDescribeCertificateProvider(c)
	case opListCertificateProviders:

		return true, h.handleListCertificateProviders(c)
	case opUpdateCertificateProvider:

		return true, h.handleUpdateCertificateProvider(c)
	case opDeleteCertificateProvider:

		return true, h.handleDeleteCertificateProvider(c)
	}

	return false, nil
}

//nolint:gocyclo,cyclop,funlen // mechanical routing switch
func (h *Handler) dispatchBatch1Ops(c *echo.Context, op string) (bool, error) {
	switch op {
	// Jobs
	case opCreateJob:
		return true, h.handleCreateJob(c)
	case opDescribeJob:
		return true, h.handleDescribeJob(c)
	case opListJobs:
		return true, h.handleListJobs(c)
	case opUpdateJob:
		return true, h.handleUpdateJob(c)
	case opCancelJob:
		return true, h.handleCancelJob(c)
	case opDeleteJob:
		return true, h.handleDeleteJob(c)
	case opGetJobDocument:
		return true, h.handleGetJobDocument(c)
	case opDescribeJobExecution:
		return true, h.handleDescribeJobExecution(c)
	case opCancelJobExecution:
		return true, h.handleCancelJobExecution(c)
	case opDeleteJobExecution:
		return true, h.handleDeleteJobExecution(c)
	// JobTemplates
	case opCreateJobTemplate:
		return true, h.handleCreateJobTemplate(c)
	case opDescribeJobTemplate:
		return true, h.handleDescribeJobTemplate(c)
	case opListJobTemplates:
		return true, h.handleListJobTemplates(c)
	case opDeleteJobTemplate:
		return true, h.handleDeleteJobTemplate(c)
	// RoleAliases
	case opCreateRoleAlias:
		return true, h.handleCreateRoleAlias(c)
	case opDescribeRoleAlias:
		return true, h.handleDescribeRoleAlias(c)
	case opListRoleAliases:
		return true, h.handleListRoleAliases(c)
	case opUpdateRoleAlias:
		return true, h.handleUpdateRoleAlias(c)
	case opDeleteRoleAlias:
		return true, h.handleDeleteRoleAlias(c)
	// DomainConfigurations
	case opCreateDomainConfiguration:
		return true, h.handleCreateDomainConfiguration(c)
	case opDescribeDomainConfiguration:
		return true, h.handleDescribeDomainConfiguration(c)
	case opListDomainConfigurations:
		return true, h.handleListDomainConfigurations(c)
	case opUpdateDomainConfiguration:
		return true, h.handleUpdateDomainConfiguration(c)
	case opDeleteDomainConfiguration:
		return true, h.handleDeleteDomainConfiguration(c)
	// ProvisioningTemplates
	case opCreateProvisioningTemplate:
		return true, h.handleCreateProvisioningTemplate(c)
	case opDescribeProvisioningTemplate:
		return true, h.handleDescribeProvisioningTemplate(c)
	case opListProvisioningTemplates:
		return true, h.handleListProvisioningTemplates(c)
	case opUpdateProvisioningTemplate:
		return true, h.handleUpdateProvisioningTemplate(c)
	case opDeleteProvisioningTemplate:
		return true, h.handleDeleteProvisioningTemplate(c)
	case opCreateProvisioningTemplateVersion:
		return true, h.handleCreateProvisioningTemplateVersion(c)
	case opListProvisioningTemplateVersions:
		return true, h.handleListProvisioningTemplateVersions(c)
	case opDeleteProvisioningTemplateVersion:
		return true, h.handleDeleteProvisioningTemplateVersion(c)
	// Authorizers
	case opCreateAuthorizer:
		return true, h.handleCreateAuthorizer(c)
	case opDescribeAuthorizer:
		return true, h.handleDescribeAuthorizer(c)
	case opListAuthorizers:
		return true, h.handleListAuthorizers(c)
	case opUpdateAuthorizer:
		return true, h.handleUpdateAuthorizer(c)
	case opDeleteAuthorizer:
		return true, h.handleDeleteAuthorizer(c)
	// BillingGroups
	case opCreateBillingGroup:
		return true, h.handleCreateBillingGroup(c)
	case opDescribeBillingGroup:
		return true, h.handleDescribeBillingGroup(c)
	case opListBillingGroups:
		return true, h.handleListBillingGroups(c)
	case opUpdateBillingGroup:
		return true, h.handleUpdateBillingGroup(c)
	case opDeleteBillingGroup:
		return true, h.handleDeleteBillingGroup(c)
	// ScheduledAudits
	case opCreateScheduledAudit:
		return true, h.handleCreateScheduledAudit(c)
	case opDescribeScheduledAudit:
		return true, h.handleDescribeScheduledAudit(c)
	case opListScheduledAudits:
		return true, h.handleListScheduledAudits(c)
	case opUpdateScheduledAudit:
		return true, h.handleUpdateScheduledAudit(c)
	case opDeleteScheduledAudit:
		return true, h.handleDeleteScheduledAudit(c)
	// MitigationActions
	case opCreateMitigationAction:
		return true, h.handleCreateMitigationAction(c)
	case opDescribeMitigationAction:
		return true, h.handleDescribeMitigationAction(c)
	case opListMitigationActions:
		return true, h.handleListMitigationActions(c)
	case opUpdateMitigationAction:
		return true, h.handleUpdateMitigationAction(c)
	case opDeleteMitigationAction:
		return true, h.handleDeleteMitigationAction(c)
	// SecurityProfiles
	case opCreateSecurityProfile:
		return true, h.handleCreateSecurityProfile(c)
	case opDescribeSecurityProfile:
		return true, h.handleDescribeSecurityProfile(c)
	case opListSecurityProfiles:
		return true, h.handleListSecurityProfiles(c)
	case opUpdateSecurityProfile:
		return true, h.handleUpdateSecurityProfile(c)
	case opDeleteSecurityProfile:
		return true, h.handleDeleteSecurityProfile(c)
	}

	return false, nil
}

//nolint:gocyclo,cyclop,funlen // mechanical routing switch
func (h *Handler) dispatchBatch2Ops(c *echo.Context, op string) (bool, error) {
	switch op {
	// CACertificate
	case opRegisterCACertificate:
		return true, h.handleRegisterCACertificate(c)
	case opDescribeCACertificate:
		return true, h.handleDescribeCACertificate(c)
	case opListCACertificates:
		return true, h.handleListCACertificates(c)
	case opUpdateCACertificate:
		return true, h.handleUpdateCACertificate(c)
	case opDeleteCACertificate:
		return true, h.handleDeleteCACertificate(c)
	case opListCertificatesByCA:
		return true, h.handleListCertificatesByCA(c)
	// Stream
	case opCreateStream:
		return true, h.handleCreateStream(c)
	case opDescribeStream:
		return true, h.handleDescribeStream(c)
	case opListStreams:
		return true, h.handleListStreams(c)
	case opUpdateStream:
		return true, h.handleUpdateStream(c)
	case opDeleteStream:
		return true, h.handleDeleteStream(c)
	// FleetMetric
	case opCreateFleetMetric:
		return true, h.handleCreateFleetMetric(c)
	case opDescribeFleetMetric:
		return true, h.handleDescribeFleetMetric(c)
	case opListFleetMetrics:
		return true, h.handleListFleetMetrics(c)
	case opUpdateFleetMetric:
		return true, h.handleUpdateFleetMetric(c)
	case opDeleteFleetMetric:
		return true, h.handleDeleteFleetMetric(c)
	// CustomMetric
	case opCreateCustomMetric:
		return true, h.handleCreateCustomMetric(c)
	case opDescribeCustomMetric:
		return true, h.handleDescribeCustomMetric(c)
	case opListCustomMetrics:
		return true, h.handleListCustomMetrics(c)
	case opUpdateCustomMetric:
		return true, h.handleUpdateCustomMetric(c)
	case opDeleteCustomMetric:
		return true, h.handleDeleteCustomMetric(c)
	// Dimension
	case opCreateDimension:
		return true, h.handleCreateDimension(c)
	case opDescribeDimension:
		return true, h.handleDescribeDimension(c)
	case opListDimensions:
		return true, h.handleListDimensions(c)
	case opUpdateDimension:
		return true, h.handleUpdateDimension(c)
	case opDeleteDimension:
		return true, h.handleDeleteDimension(c)
	// Tags
	case opTagResource:
		return true, h.handleTagResource(c)
	case opUntagResource:
		return true, h.handleUntagResource(c)
	case opListTagsForResource:
		return true, h.handleListTagsForResource(c)
	// Audit config
	case opDescribeAccountAuditConfiguration:
		return true, h.handleDescribeAccountAuditConfiguration(c)
	case opUpdateAccountAuditConfiguration:
		return true, h.handleUpdateAccountAuditConfiguration(c)
	case opStartOnDemandAuditTask:
		return true, h.handleStartOnDemandAuditTask(c)
	case opDescribeAuditTask:
		return true, h.handleDescribeAuditTask(c)
	case opListAuditTasks:
		return true, h.handleListAuditTasks(c)
	// Misc
	case opDetachThingPrincipal:
		return true, h.handleDetachThingPrincipal(c)
	case opCancelCertificateTransfer:
		return true, h.handleCancelCertificateTransfer(c)
	case opGetRegistrationCode:
		return true, h.handleGetRegistrationCode(c)
	case opDeleteRegistrationCode:
		return true, h.handleDeleteRegistrationCode(c)
	case opListThingGroupsForThing:
		return true, h.handleListThingGroupsForThing(c)
	case opListThingsInBillingGroup:
		return true, h.handleListThingsInBillingGroup(c)
	case opRemoveThingFromBillingGroup:
		return true, h.handleRemoveThingFromBillingGroup(c)
	case opListPrincipalPolicies:
		return true, h.handleListPrincipalPolicies(c)
	case opListPolicyPrincipals:
		return true, h.handleListPolicyPrincipals(c)
	case opListTargetsForPolicy:
		return true, h.handleListTargetsForPolicy(c)
	case opListPrincipalThings:
		return true, h.handleListPrincipalThings(c)
	case opListPrincipalThingsV2:
		return true, h.handleListPrincipalThingsV2(c)
	case opGetEffectivePolicies:
		return true, h.handleGetEffectivePolicies(c)
	case opSetDefaultAuthorizer:
		return true, h.handleSetDefaultAuthorizer(c)
	case opClearDefaultAuthorizer:
		return true, h.handleClearDefaultAuthorizer(c)
	case opDescribeDefaultAuthorizer:
		return true, h.handleDescribeDefaultAuthorizer(c)
	case opListJobExecutionsForJob:
		return true, h.handleListJobExecutionsForJob(c)
	case opListJobExecutionsForThing:
		return true, h.handleListJobExecutionsForThing(c)
	}

	return false, nil
}

// handleError maps backend errors to appropriate HTTP responses.
func (h *Handler) handleError(c *echo.Context, err error) error {
	type awsErr struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	switch {
	case errors.Is(err, ErrThingNotFound),
		errors.Is(err, ErrRuleNotFound),
		errors.Is(err, ErrPolicyNotFound),
		errors.Is(err, ErrThingTypeNotFound),
		errors.Is(err, ErrThingGroupNotFound),
		errors.Is(err, ErrCertificateNotFound),
		errors.Is(err, ErrCertificateProviderNotFound),
		errors.Is(err, ErrTopicRuleDestinationNotFound),
		errors.Is(err, ErrPolicyVersionNotFound):

		return c.JSON(http.StatusNotFound, awsErr{"ResourceNotFoundException", err.Error()})
	case errors.Is(err, ErrValidation):

		return c.JSON(http.StatusBadRequest, awsErr{"InvalidRequestException", err.Error()})
	case errors.Is(err, ErrAlreadyExists):

		return c.JSON(http.StatusConflict, awsErr{"ResourceAlreadyExistsException", err.Error()})
	case errors.Is(err, ErrVersionConflict):

		return c.JSON(http.StatusConflict, awsErr{"VersionConflictException", err.Error()})
	case errors.Is(err, ErrDeleteConflict):

		return c.JSON(http.StatusConflict, awsErr{"DeleteConflictException", err.Error()})
	default:

		return c.JSON(
			http.StatusInternalServerError,
			awsErr{"InternalFailureException", err.Error()},
		)
	}
}

func (h *Handler) handleCreateThing(c *echo.Context) error {
	thingName := strings.TrimPrefix(c.Request().URL.Path, "/things/")

	var body struct {
		AttributePayload *AttributePayload `json:"attributePayload"`
		ThingTypeName    string            `json:"thingTypeName"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
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
		keyThingName:     t.ThingName,
		keyThingArn:      t.ARN,
		"thingId":        t.ThingID,
		keyThingTypeName: t.ThingTypeName,
		keyAttributes:    t.Attributes,
		keyVersion:       t.Version,
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

	if err := json.NewDecoder(c.Request().Body).Decode(&payload); err != nil &&
		!errors.Is(err, io.EOF) {
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
			keyDescription:     r.Description,
			"actions":          r.Actions,
			"ruleDisabled":     !r.Enabled,
			keyCreatedAt:       r.CreatedAt,
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

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
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
		keyPolicyName:      out.PolicyName,
		keyPolicyArn:       out.PolicyARN,
		keyPolicyDocument:  out.PolicyDocument,
		keyPolicyVersionID: out.PolicyVersionID,
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

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
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

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	if err := h.Backend.AddThingToBillingGroup(&body); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleAddThingToThingGroup(c *echo.Context) error {
	var body AddThingToThingGroupInput

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
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
	parts := strings.SplitN(
		strings.TrimPrefix(c.Request().URL.Path, "/packages/"),
		"/",
		maxPackagePathSegments,
	)

	var packageName, versionName string
	// len(parts) >= packageVersionPartsMin guarantees indices 0, 1, 2 are valid.
	if len(parts) >= packageVersionPartsMin {
		packageName = parts[0]
		versionName = parts[2]
	}

	var body struct {
		Sbom *SbomDocument `json:"sbom"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
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

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
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

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
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

	return c.JSON(http.StatusOK, map[string]any{
		keyPolicyName:       out.PolicyName,
		keyPolicyArn:        out.PolicyARN,
		keyPolicyDocument:   out.PolicyDocument,
		"defaultVersionId":  out.DefaultVersionID,
		"creationDate":      out.CreatedAt,
		keyLastModifiedDate: out.LastModifiedAt,
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
			keyThingName:     t.ThingName,
			keyThingArn:      t.ARN,
			keyThingTypeName: t.ThingTypeName,
			keyAttributes:    t.Attributes,
			keyVersion:       t.Version,
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
			keyCreatedAt:   r.CreatedAt,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"rules": out})
}

func (h *Handler) handleUpdateThing(c *echo.Context) error {
	thingName := strings.TrimPrefix(c.Request().URL.Path, "/things/")

	var body UpdateThingInput

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
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

	if err := json.NewDecoder(c.Request().Body).Decode(&payload); err != nil &&
		!errors.Is(err, io.EOF) {
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

// -----------------------------------------------------------
// ThingType handlers
// -----------------------------------------------------------

func (h *Handler) handleCreateThingType(c *echo.Context) error {
	thingTypeName := strings.TrimPrefix(c.Request().URL.Path, "/thing-types/")

	var body struct {
		ThingTypeProperties *struct {
			ThingTypeDescription string   `json:"thingTypeDescription"`
			SearchableAttributes []string `json:"searchableAttributes"`
		} `json:"thingTypeProperties"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	desc := ""

	var searchable []string

	if body.ThingTypeProperties != nil {
		desc = body.ThingTypeProperties.ThingTypeDescription
		searchable = body.ThingTypeProperties.SearchableAttributes
	}

	tt, err := h.Backend.CreateThingType(&CreateThingTypeInput{
		ThingTypeName:        thingTypeName,
		Description:          desc,
		SearchableAttributes: searchable,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		keyThingTypeName: tt.ThingTypeName,
		keyThingTypeArn:  tt.ThingTypeARN,
		"thingTypeId":    tt.ThingTypeID,
	})
}

func (h *Handler) handleDescribeThingType(c *echo.Context) error {
	thingTypeName := strings.TrimPrefix(c.Request().URL.Path, "/thing-types/")

	tt, err := h.Backend.DescribeThingType(thingTypeName)
	if err != nil {
		return h.handleError(c, err)
	}

	var deprecationDate any
	if tt.Deprecated && !tt.DeprecationDate.IsZero() {
		deprecationDate = tt.DeprecationDate
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyThingTypeName: tt.ThingTypeName,
		keyThingTypeArn:  tt.ThingTypeARN,
		"thingTypeId":    tt.ThingTypeID,
		"thingTypeMetadata": map[string]any{
			"deprecated":      tt.Deprecated,
			keyCreationDate:   tt.CreatedAt,
			"deprecationDate": deprecationDate,
		},
		"thingTypeProperties": map[string]any{
			"thingTypeDescription": tt.Description,
			"searchableAttributes": tt.SearchableAttributes,
		},
	})
}

func (h *Handler) handleListThingTypes(c *echo.Context) error {
	types := h.Backend.ListThingTypes()
	out := make([]map[string]any, 0, len(types))

	for _, tt := range types {
		var deprecationDate any
		if tt.Deprecated && !tt.DeprecationDate.IsZero() {
			deprecationDate = tt.DeprecationDate
		}

		out = append(out, map[string]any{
			keyThingTypeName: tt.ThingTypeName,
			keyThingTypeArn:  tt.ThingTypeARN,
			"thingTypeMetadata": map[string]any{
				"deprecated":      tt.Deprecated,
				keyCreationDate:   tt.CreatedAt,
				"deprecationDate": deprecationDate,
			},
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"thingTypes": out})
}

func (h *Handler) handleDeprecateThingType(c *echo.Context) error {
	after := strings.TrimPrefix(c.Request().URL.Path, "/thing-types/")
	thingTypeName := strings.TrimSuffix(after, "/deprecate")

	var body struct {
		UndoDeprecate bool `json:"undoDeprecate"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	if err := h.Backend.DeprecateThingType(&DeprecateThingTypeInput{
		ThingTypeName: thingTypeName,
		UndoDeprecate: body.UndoDeprecate,
	}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteThingType(c *echo.Context) error {
	thingTypeName := strings.TrimPrefix(c.Request().URL.Path, "/thing-types/")
	if err := h.Backend.DeleteThingType(thingTypeName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// -----------------------------------------------------------
// ThingGroup handlers
// -----------------------------------------------------------

func (h *Handler) handleCreateThingGroup(c *echo.Context) error {
	thingGroupName := strings.TrimPrefix(c.Request().URL.Path, "/thing-groups/")

	var body struct {
		ThingGroupProperties *struct {
			AttributePayload      *AttributePayload `json:"attributePayload"`
			ThingGroupDescription string            `json:"thingGroupDescription"`
		} `json:"thingGroupProperties"`
		ParentGroupName string `json:"parentGroupName"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	desc := ""
	var attrs map[string]string
	if body.ThingGroupProperties != nil {
		desc = body.ThingGroupProperties.ThingGroupDescription
		if body.ThingGroupProperties.AttributePayload != nil {
			attrs = body.ThingGroupProperties.AttributePayload.Attributes
		}
	}

	tg, err := h.Backend.CreateThingGroup(&CreateThingGroupInput{
		ThingGroupName:  thingGroupName,
		ParentGroupName: body.ParentGroupName,
		Description:     desc,
		Attributes:      attrs,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		keyThingGroupName: tg.ThingGroupName,
		keyThingGroupArn:  tg.ThingGroupARN,
		keyThingGroupID:   tg.ThingGroupID,
	})
}

func (h *Handler) handleDescribeThingGroup(c *echo.Context) error {
	thingGroupName := strings.TrimPrefix(c.Request().URL.Path, "/thing-groups/")
	tg, err := h.Backend.DescribeThingGroup(thingGroupName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyThingGroupName: tg.ThingGroupName,
		keyThingGroupArn:  tg.ThingGroupARN,
		keyThingGroupID:   tg.ThingGroupID,
		keyVersion:        tg.Version,
		"thingGroupProperties": map[string]any{
			"thingGroupDescription": tg.Description,
			"attributePayload":      map[string]any{keyAttributes: tg.Attributes},
		},
		"thingGroupMetadata": map[string]any{
			keyCreationDate:   tg.CreatedAt,
			"parentGroupName": tg.ParentGroupName,
		},
	})
}

func (h *Handler) handleListThingGroups(c *echo.Context) error {
	groups := h.Backend.ListThingGroups()
	out := make([]map[string]string, 0, len(groups))
	for _, tg := range groups {
		out = append(out, map[string]string{
			keyThingGroupName: tg.ThingGroupName,
			keyThingGroupArn:  tg.ThingGroupARN,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"thingGroups": out})
}

func (h *Handler) handleUpdateThingGroup(c *echo.Context) error {
	thingGroupName := strings.TrimPrefix(c.Request().URL.Path, "/thing-groups/")

	var body struct {
		ThingGroupProperties *struct {
			AttributePayload      *AttributePayload `json:"attributePayload"`
			ThingGroupDescription string            `json:"thingGroupDescription"`
		} `json:"thingGroupProperties"`
		ExpectedVersion int64 `json:"expectedVersion"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	desc := ""
	var attrs map[string]string
	if body.ThingGroupProperties != nil {
		desc = body.ThingGroupProperties.ThingGroupDescription
		if body.ThingGroupProperties.AttributePayload != nil {
			attrs = body.ThingGroupProperties.AttributePayload.Attributes
		}
	}

	newVersion, err := h.Backend.UpdateThingGroup(&UpdateThingGroupInput{
		ThingGroupName:  thingGroupName,
		Description:     desc,
		Attributes:      attrs,
		ExpectedVersion: body.ExpectedVersion,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVersion: newVersion})
}

func (h *Handler) handleDeleteThingGroup(c *echo.Context) error {
	thingGroupName := strings.TrimPrefix(c.Request().URL.Path, "/thing-groups/")
	if err := h.Backend.DeleteThingGroup(thingGroupName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleRemoveThingFromThingGroup(c *echo.Context) error {
	var body RemoveThingFromThingGroupInput
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}
	if err := h.Backend.RemoveThingFromThingGroup(&body); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListThingsInThingGroup(c *echo.Context) error {
	after := strings.TrimPrefix(c.Request().URL.Path, "/thing-groups/")
	thingGroupName := strings.TrimSuffix(after, "/things")
	things, err := h.Backend.ListThingsInThingGroup(
		&ListThingsInThingGroupInput{ThingGroupName: thingGroupName},
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"things": things})
}

// -----------------------------------------------------------
// Certificate handlers
// -----------------------------------------------------------

func (h *Handler) handleCreateCertificateFromCsr(c *echo.Context) error {
	var body struct {
		CertificateSigningRequest string `json:"certificateSigningRequest"`
		SetAsActive               bool   `json:"setAsActive"`
	}
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}
	cert, err := h.Backend.CreateCertificateFromCsr(&CreateCertificateFromCsrInput{
		CertificateSigningRequest: body.CertificateSigningRequest,
		SetAsActive:               body.SetAsActive,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		keyCertificateID:  cert.CertificateID,
		keyCertificateArn: cert.ARN,
		keyCertificatePem: cert.PEM,
		keyStatus:         cert.Status,
	})
}

func (h *Handler) handleRegisterCertificate(c *echo.Context) error {
	var body struct {
		CertificatePem string `json:"certificatePem"`
		Status         string `json:"status"`
	}
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}
	cert, err := h.Backend.RegisterCertificate(&RegisterCertificateInput{
		CertificatePem: body.CertificatePem,
		Status:         body.Status,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		keyCertificateID:  cert.CertificateID,
		keyCertificateArn: cert.ARN,
	})
}

func (h *Handler) handleRegisterCertificateWithoutCA(c *echo.Context) error {
	var body struct {
		CertificatePem string `json:"certificatePem"`
		Status         string `json:"status"`
	}
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}
	cert, err := h.Backend.RegisterCertificateWithoutCA(&RegisterCertificateInput{
		CertificatePem: body.CertificatePem,
		Status:         body.Status,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		keyCertificateID:  cert.CertificateID,
		keyCertificateArn: cert.ARN,
	})
}

func (h *Handler) handleDescribeCertificate(c *echo.Context) error {
	certID := strings.TrimPrefix(c.Request().URL.Path, "/certificates/")
	cert, err := h.Backend.DescribeCertificate(certID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"certificateDescription": map[string]any{
			keyCertificateID:    cert.CertificateID,
			keyCertificateArn:   cert.ARN,
			keyStatus:           cert.Status,
			keyCreationDate:     cert.CreatedAt,
			keyLastModifiedDate: cert.LastModifiedAt,
		},
	})
}

func (h *Handler) handleListCertificates(c *echo.Context) error {
	certs := h.Backend.ListCertificates()
	out := make([]map[string]any, 0, len(certs))

	for _, cert := range certs {
		out = append(out, map[string]any{
			keyCertificateID:    cert.CertificateID,
			keyCertificateArn:   cert.ARN,
			keyStatus:           cert.Status,
			keyCreationDate:     cert.CreatedAt,
			keyLastModifiedDate: cert.LastModifiedAt,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"certificates": out})
}

func (h *Handler) handleUpdateCertificate(c *echo.Context) error {
	certID := strings.TrimPrefix(c.Request().URL.Path, "/certificates/")
	newStatus := c.QueryParam("newStatus")
	if err := h.Backend.UpdateCertificate(&UpdateCertificateInput{
		CertificateID: certID,
		NewStatus:     newStatus,
	}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteCertificate(c *echo.Context) error {
	certID := strings.TrimPrefix(c.Request().URL.Path, "/certificates/")
	if err := h.Backend.DeleteCertificate(certID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// -----------------------------------------------------------
// Policy attachment/version handlers
// -----------------------------------------------------------

func (h *Handler) handleDetachPolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/target-policies/")

	var body struct {
		Target string `json:"target"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	if err := h.Backend.DetachPolicy(&DetachPolicyInput{PolicyName: policyName, Target: body.Target}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListAttachedPolicies(c *echo.Context) error {
	var body struct {
		Target    string `json:"target"`
		Recursive bool   `json:"recursive"`
	}
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}
	policies, err := h.Backend.ListAttachedPolicies(
		&ListAttachedPoliciesInput{Target: body.Target, Recursive: body.Recursive},
	)
	if err != nil {
		return h.handleError(c, err)
	}
	out := make([]map[string]string, 0, len(policies))
	for _, p := range policies {
		out = append(out, map[string]string{keyPolicyName: p.PolicyName, keyPolicyArn: p.ARN})
	}

	return c.JSON(http.StatusOK, map[string]any{"policies": out})
}

func (h *Handler) handleCreatePolicyVersion(c *echo.Context) error {
	after := strings.TrimPrefix(c.Request().URL.Path, "/policies/")
	policyName := strings.TrimSuffix(after, "/version")

	var body struct {
		PolicyDocument string `json:"policyDocument"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	setAsDefault := c.QueryParam("setAsDefault") == keyBoolTrue

	pv, err := h.Backend.CreatePolicyVersion(&CreatePolicyVersionInput{
		PolicyName:     policyName,
		PolicyDocument: body.PolicyDocument,
		SetAsDefault:   setAsDefault,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyPolicyVersionID:  pv.VersionID,
		keyPolicyDocument:   pv.PolicyDocument,
		keyIsDefaultVersion: pv.IsDefaultVersion,
	})
}

func (h *Handler) handleGetPolicyVersion(c *echo.Context) error {
	after := strings.TrimPrefix(c.Request().URL.Path, "/policies/")
	parts := strings.SplitN(after, "/version/", maxPathSegments)
	if len(parts) != maxPathSegments {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: keyInvalidPath})
	}
	pv, err := h.Backend.GetPolicyVersion(parts[0], parts[1])
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyPolicyVersionID:  pv.VersionID,
		keyPolicyDocument:   pv.PolicyDocument,
		keyIsDefaultVersion: pv.IsDefaultVersion,
		"createDate":        pv.CreatedAt,
	})
}

func (h *Handler) handleListPolicyVersions(c *echo.Context) error {
	after := strings.TrimPrefix(c.Request().URL.Path, "/policies/")
	policyName := strings.TrimSuffix(after, "/version")
	versions, err := h.Backend.ListPolicyVersions(policyName)
	if err != nil {
		return h.handleError(c, err)
	}
	out := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		out = append(out, map[string]any{
			"versionId":         v.VersionID,
			keyIsDefaultVersion: v.IsDefaultVersion,
			"createDate":        v.CreatedAt,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"policyVersions": out})
}

func (h *Handler) handleDeletePolicyVersion(c *echo.Context) error {
	after := strings.TrimPrefix(c.Request().URL.Path, "/policies/")
	parts := strings.SplitN(after, "/version/", maxPathSegments)
	if len(parts) != maxPathSegments {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: keyInvalidPath})
	}
	if err := h.Backend.DeletePolicyVersion(parts[0], parts[1]); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleSetDefaultPolicyVersion(c *echo.Context) error {
	after := strings.TrimPrefix(c.Request().URL.Path, "/policies/")
	after = strings.TrimSuffix(after, "/default")
	parts := strings.SplitN(after, "/version/", maxPathSegments)
	if len(parts) != maxPathSegments {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: keyInvalidPath})
	}
	if err := h.Backend.SetDefaultPolicyVersion(parts[0], parts[1]); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// -----------------------------------------------------------
// TopicRuleDestination handlers
// -----------------------------------------------------------

func (h *Handler) handleCreateTopicRuleDestination(c *echo.Context) error {
	var body struct {
		DestinationConfiguration *TopicRuleDestinationConfiguration `json:"destinationConfiguration"`
	}
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}
	dest, err := h.Backend.CreateTopicRuleDestination(&CreateTopicRuleDestinationInput{
		DestinationConfiguration: body.DestinationConfiguration,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"topicRuleDestination": map[string]any{keyArn: dest.ARN, keyStatus: dest.Status},
	})
}

func (h *Handler) handleGetTopicRuleDestination(c *echo.Context) error {
	arn := strings.TrimPrefix(c.Request().URL.Path, "/rule-destinations/")
	dest, err := h.Backend.GetTopicRuleDestination(arn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"topicRuleDestination": map[string]any{keyArn: dest.ARN, keyStatus: dest.Status},
	})
}

func (h *Handler) handleListTopicRuleDestinations(c *echo.Context) error {
	dests := h.Backend.ListTopicRuleDestinations()
	out := make([]map[string]any, 0, len(dests))
	for _, d := range dests {
		out = append(out, map[string]any{keyArn: d.ARN, keyStatus: d.Status})
	}

	return c.JSON(http.StatusOK, map[string]any{"destinationSummaries": out})
}

func (h *Handler) handleUpdateTopicRuleDestination(c *echo.Context) error {
	var body struct {
		ARN    string `json:"arn"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}
	if err := h.Backend.UpdateTopicRuleDestination(&UpdateTopicRuleDestinationInput{
		ARN:    body.ARN,
		Status: body.Status,
	}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteTopicRuleDestination(c *echo.Context) error {
	arn := strings.TrimPrefix(c.Request().URL.Path, "/rule-destinations/")
	if err := h.Backend.DeleteTopicRuleDestination(arn); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// -----------------------------------------------------------
// CertificateProvider handlers
// -----------------------------------------------------------

func (h *Handler) handleCreateCertificateProvider(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/certificate-providers/")

	var body struct {
		LambdaFunctionARN           string   `json:"lambdaFunctionArn"`
		AccountDefaultForOperations []string `json:"accountDefaultForOperations"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	cp, err := h.Backend.CreateCertificateProvider(&CreateCertificateProviderInput{
		CertificateProviderName:     name,
		LambdaFunctionARN:           body.LambdaFunctionARN,
		AccountDefaultForOperations: body.AccountDefaultForOperations,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		keyCertificateProviderName: cp.CertificateProviderName,
		keyCertificateProviderArn:  cp.ARN,
	})
}

func (h *Handler) handleDescribeCertificateProvider(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/certificate-providers/")
	cp, err := h.Backend.DescribeCertificateProvider(name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCertificateProviderName:    cp.CertificateProviderName,
		keyCertificateProviderArn:     cp.ARN,
		"lambdaFunctionArn":           cp.LambdaFunctionARN,
		"accountDefaultForOperations": cp.AccountDefaultForOperations,
		keyCreationDate:               cp.CreatedAt,
		keyLastModifiedDate:           cp.LastModifiedAt,
	})
}

func (h *Handler) handleListCertificateProviders(c *echo.Context) error {
	providers := h.Backend.ListCertificateProviders()
	out := make([]map[string]string, 0, len(providers))
	for _, cp := range providers {
		out = append(out, map[string]string{
			keyCertificateProviderName: cp.CertificateProviderName,
			keyCertificateProviderArn:  cp.ARN,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"certificateProviders": out})
}

func (h *Handler) handleUpdateCertificateProvider(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/certificate-providers/")

	var body struct {
		LambdaFunctionARN           string   `json:"lambdaFunctionArn"`
		AccountDefaultForOperations []string `json:"accountDefaultForOperations"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	if err := h.Backend.UpdateCertificateProvider(&UpdateCertificateProviderInput{
		CertificateProviderName:     name,
		LambdaFunctionARN:           body.LambdaFunctionARN,
		AccountDefaultForOperations: body.AccountDefaultForOperations,
	}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteCertificateProvider(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/certificate-providers/")
	if err := h.Backend.DeleteCertificateProvider(name); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
