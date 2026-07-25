package sesv2

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	opBatchGetMetricData                     = "BatchGetMetricData"
	opCancelExportJob                        = "CancelExportJob"
	opCreateConfigurationSet                 = "CreateConfigurationSet"
	opCreateConfigurationSetEventDestination = "CreateConfigurationSetEventDestination"
	opCreateContact                          = "CreateContact"
	opCreateContactList                      = "CreateContactList"
	opCreateCustomVerificationEmailTemplate  = "CreateCustomVerificationEmailTemplate"
	opCreateDedicatedIPPool                  = "CreateDedicatedIpPool"
	opCreateDeliverabilityTestReport         = "CreateDeliverabilityTestReport"
	opCreateEmailIdentity                    = "CreateEmailIdentity"
	opCreateEmailIdentityPolicy              = "CreateEmailIdentityPolicy"
	opCreateEmailTemplate                    = "CreateEmailTemplate"
	opDeleteConfigurationSet                 = "DeleteConfigurationSet"
	opDeleteEmailIdentity                    = "DeleteEmailIdentity"
	opGetConfigurationSet                    = "GetConfigurationSet"
	opGetEmailIdentity                       = "GetEmailIdentity"
	opListConfigurationSets                  = "ListConfigurationSets"
	opListEmailIdentities                    = "ListEmailIdentities"
	opListTagsForResource                    = "ListTagsForResource"
	opSendEmail                              = "SendEmail"
	opTagResource                            = "TagResource"
	opUntagResource                          = "UntagResource"
)

const (
	// new ops — account.
	opGetAccount                            = "GetAccount"
	opGetBlacklistReports                   = "GetBlacklistReports"
	opPutAccountDedicatedIPWarmupAttributes = "PutAccountDedicatedIpWarmupAttributes"
	opPutAccountDetails                     = "PutAccountDetails"
	opPutAccountPricingAttributes           = "PutAccountPricingAttributes"
	opPutAccountSendingAttributes           = "PutAccountSendingAttributes"
	opPutAccountSuppressionAttributes       = "PutAccountSuppressionAttributes"
	opPutAccountVdmAttributes               = "PutAccountVdmAttributes"

	// new ops — suppressed destinations.
	opPutSuppressedDestination    = "PutSuppressedDestination"
	opGetSuppressedDestination    = "GetSuppressedDestination"
	opDeleteSuppressedDestination = "DeleteSuppressedDestination"
	opListSuppressedDestinations  = "ListSuppressedDestinations"

	// new ops — contact lists / contacts.
	opGetContactList    = "GetContactList"
	opDeleteContactList = "DeleteContactList"
	opUpdateContactList = "UpdateContactList"
	opListContactLists  = "ListContactLists"
	opGetContact        = "GetContact"
	opDeleteContact     = "DeleteContact"
	opUpdateContact     = "UpdateContact"
	opListContacts      = "ListContacts"

	// new ops — custom verification templates.
	opGetCustomVerificationEmailTemplate    = "GetCustomVerificationEmailTemplate"
	opDeleteCustomVerificationEmailTemplate = "DeleteCustomVerificationEmailTemplate"
	opUpdateCustomVerificationEmailTemplate = "UpdateCustomVerificationEmailTemplate"
	opListCustomVerificationEmailTemplates  = "ListCustomVerificationEmailTemplates"
	opSendCustomVerificationEmail           = "SendCustomVerificationEmail"

	// new ops — dedicated IP pools.
	opGetDedicatedIPPool                  = "GetDedicatedIpPool"
	opDeleteDedicatedIPPool               = "DeleteDedicatedIpPool"
	opListDedicatedIPPools                = "ListDedicatedIpPools"
	opGetDedicatedIP                      = "GetDedicatedIp"
	opGetDedicatedIps                     = "GetDedicatedIps"
	opPutDedicatedIPInPool                = "PutDedicatedIpInPool"
	opPutDedicatedIPPoolScalingAttributes = "PutDedicatedIpPoolScalingAttributes"
	opPutDedicatedIPWarmupAttributes      = "PutDedicatedIpWarmupAttributes"

	// new ops — deliverability.
	opGetDeliverabilityDashboardOptions = "GetDeliverabilityDashboardOptions"
	opPutDeliverabilityDashboardOption  = "PutDeliverabilityDashboardOption"
	opGetDeliverabilityTestReport       = "GetDeliverabilityTestReport"
	opListDeliverabilityTestReports     = "ListDeliverabilityTestReports"
	opGetDomainDeliverabilityCampaign   = "GetDomainDeliverabilityCampaign"
	opGetDomainStatisticsReport         = "GetDomainStatisticsReport"
	opListDomainDeliverabilityCampaigns = "ListDomainDeliverabilityCampaigns"
	opGetEmailAddressInsights           = "GetEmailAddressInsights"
	opGetMessageInsights                = "GetMessageInsights"
	opListRecommendations               = "ListRecommendations"

	// new ops — email templates.
	opGetEmailTemplate        = "GetEmailTemplate"
	opDeleteEmailTemplate     = "DeleteEmailTemplate"
	opUpdateEmailTemplate     = "UpdateEmailTemplate"
	opListEmailTemplates      = "ListEmailTemplates"
	opTestRenderEmailTemplate = "TestRenderEmailTemplate"

	// new ops — export / import jobs.
	opCreateExportJob = "CreateExportJob"
	opGetExportJob    = "GetExportJob"
	opListExportJobs  = "ListExportJobs"
	opCreateImportJob = "CreateImportJob"
	opGetImportJob    = "GetImportJob"
	opListImportJobs  = "ListImportJobs"

	// new ops — email identity policies.
	opGetEmailIdentityPolicies  = "GetEmailIdentityPolicies"
	opDeleteEmailIdentityPolicy = "DeleteEmailIdentityPolicy"
	opUpdateEmailIdentityPolicy = "UpdateEmailIdentityPolicy"

	// new ops — email identity attributes.
	opPutEmailIdentityConfigurationSetAttributes = "PutEmailIdentityConfigurationSetAttributes"
	opPutEmailIdentityDkimAttributes             = "PutEmailIdentityDkimAttributes"
	opPutEmailIdentityDkimSigningAttributes      = "PutEmailIdentityDkimSigningAttributes"
	opPutEmailIdentityFeedbackAttributes         = "PutEmailIdentityFeedbackAttributes"
	opPutEmailIdentityMailFromAttributes         = "PutEmailIdentityMailFromAttributes"

	// new ops — configuration set event destinations.
	opGetConfigurationSetEventDestinations   = "GetConfigurationSetEventDestinations"
	opDeleteConfigurationSetEventDestination = "DeleteConfigurationSetEventDestination"
	opUpdateConfigurationSetEventDestination = "UpdateConfigurationSetEventDestination"

	// new ops — configuration set attributes.
	opPutConfigurationSetArchivingOptions   = "PutConfigurationSetArchivingOptions"
	opPutConfigurationSetDeliveryOptions    = "PutConfigurationSetDeliveryOptions"
	opPutConfigurationSetReputationOptions  = "PutConfigurationSetReputationOptions"
	opPutConfigurationSetSendingOptions     = "PutConfigurationSetSendingOptions"
	opPutConfigurationSetSuppressionOptions = "PutConfigurationSetSuppressionOptions"
	opPutConfigurationSetTrackingOptions    = "PutConfigurationSetTrackingOptions"
	opPutConfigurationSetVdmOptions         = "PutConfigurationSetVdmOptions"

	// new ops — bulk email.
	opSendBulkEmail = "SendBulkEmail"

	// new ops — multi-region endpoints.
	opCreateMultiRegionEndpoint = "CreateMultiRegionEndpoint"
	opGetMultiRegionEndpoint    = "GetMultiRegionEndpoint"
	opDeleteMultiRegionEndpoint = "DeleteMultiRegionEndpoint"
	opListMultiRegionEndpoints  = "ListMultiRegionEndpoints"

	// new ops — tenants.
	opCreateTenant                    = "CreateTenant"
	opGetTenant                       = "GetTenant"
	opDeleteTenant                    = "DeleteTenant"
	opListTenants                     = "ListTenants"
	opCreateTenantResourceAssociation = "CreateTenantResourceAssociation"
	opDeleteTenantResourceAssociation = "DeleteTenantResourceAssociation"
	opListResourceTenants             = "ListResourceTenants"
	opListTenantResources             = "ListTenantResources"
	opPutTenantSuppressionAttributes  = "PutTenantSuppressionAttributes"

	// new ops — reputation entities.
	opGetReputationEntity                         = "GetReputationEntity"
	opListReputationEntities                      = "ListReputationEntities"
	opUpdateReputationEntityCustomerManagedStatus = "UpdateReputationEntityCustomerManagedStatus"
	opUpdateReputationEntityPolicy                = "UpdateReputationEntityPolicy"
)

const (
	sesv2PathPrefix = "/v2/email/"
	unknownAction   = "Unknown"
)

// Handler is the Echo HTTP handler for SES v2 operations.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new SES v2 handler with the given backend.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset resets the backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "SESv2"
}

// supportedOperationsGroupA returns the first alphabetical chunk of supported
// SES v2 operation names. The full list is split across several small
// functions (rather than one long literal or a package-level global) purely
// to keep each function short; the combined, concatenated data is unchanged
// from the original single literal.
func supportedOperationsGroupA() []string {
	return []string{
		opBatchGetMetricData,
		opCancelExportJob,
		opCreateConfigurationSet,
		opCreateConfigurationSetEventDestination,
		opCreateContact,
		opCreateContactList,
		opCreateCustomVerificationEmailTemplate,
		opCreateDedicatedIPPool,
		opCreateDeliverabilityTestReport,
		opCreateEmailIdentity,
		opCreateEmailIdentityPolicy,
		opCreateEmailTemplate,
		opCreateExportJob,
		opCreateImportJob,
		opCreateMultiRegionEndpoint,
		opCreateTenant,
		opCreateTenantResourceAssociation,
		opDeleteConfigurationSet,
		opDeleteConfigurationSetEventDestination,
		opDeleteContact,
		opDeleteContactList,
		opDeleteCustomVerificationEmailTemplate,
		opDeleteDedicatedIPPool,
		opDeleteEmailIdentity,
		opDeleteEmailIdentityPolicy,
		opDeleteEmailTemplate,
		opDeleteMultiRegionEndpoint,
		opDeleteSuppressedDestination,
		opDeleteTenant,
		opDeleteTenantResourceAssociation,
	}
}

// supportedOperationsGroupB returns the second alphabetical chunk (see
// supportedOperationsGroupA).
func supportedOperationsGroupB() []string {
	return []string{
		opGetAccount,
		opGetBlacklistReports,
		opGetConfigurationSet,
		opGetConfigurationSetEventDestinations,
		opGetContact,
		opGetContactList,
		opGetCustomVerificationEmailTemplate,
		opGetDedicatedIP,
		opGetDedicatedIPPool,
		opGetDedicatedIps,
		opGetDeliverabilityDashboardOptions,
		opGetDeliverabilityTestReport,
		opGetDomainDeliverabilityCampaign,
		opGetDomainStatisticsReport,
		opGetEmailAddressInsights,
		opGetEmailIdentity,
		opGetEmailIdentityPolicies,
		opGetEmailTemplate,
		opGetExportJob,
		opGetImportJob,
		opGetMessageInsights,
		opGetMultiRegionEndpoint,
		opGetReputationEntity,
		opGetSuppressedDestination,
		opGetTenant,
		opListConfigurationSets,
		opListContactLists,
		opListContacts,
		opListCustomVerificationEmailTemplates,
		opListDedicatedIPPools,
		opListDeliverabilityTestReports,
		opListDomainDeliverabilityCampaigns,
		opListEmailIdentities,
		opListEmailTemplates,
		opListExportJobs,
		opListImportJobs,
		opListMultiRegionEndpoints,
		opListRecommendations,
		opListReputationEntities,
		opListResourceTenants,
		opListSuppressedDestinations,
		opListTagsForResource,
		opListTenantResources,
		opListTenants,
	}
}

// supportedOperationsGroupC returns the third alphabetical chunk (see
// supportedOperationsGroupA).
func supportedOperationsGroupC() []string {
	return []string{
		opPutAccountDedicatedIPWarmupAttributes,
		opPutAccountDetails,
		opPutAccountPricingAttributes,
		opPutAccountSendingAttributes,
		opPutAccountSuppressionAttributes,
		opPutAccountVdmAttributes,
		opPutConfigurationSetArchivingOptions,
		opPutConfigurationSetDeliveryOptions,
		opPutConfigurationSetReputationOptions,
		opPutConfigurationSetSendingOptions,
		opPutConfigurationSetSuppressionOptions,
		opPutConfigurationSetTrackingOptions,
		opPutConfigurationSetVdmOptions,
		opPutDedicatedIPInPool,
		opPutDedicatedIPPoolScalingAttributes,
		opPutDedicatedIPWarmupAttributes,
		opPutDeliverabilityDashboardOption,
		opPutEmailIdentityConfigurationSetAttributes,
		opPutEmailIdentityDkimAttributes,
		opPutEmailIdentityDkimSigningAttributes,
		opPutEmailIdentityFeedbackAttributes,
		opPutEmailIdentityMailFromAttributes,
		opPutSuppressedDestination,
		opPutTenantSuppressionAttributes,
		opSendBulkEmail,
		opSendCustomVerificationEmail,
		opSendEmail,
		opTagResource,
		opTestRenderEmailTemplate,
		opUntagResource,
	}
}

// supportedOperationsGroupD returns the fourth and final alphabetical chunk
// (see supportedOperationsGroupA).
func supportedOperationsGroupD() []string {
	return []string{
		opUpdateConfigurationSetEventDestination,
		opUpdateContact,
		opUpdateContactList,
		opUpdateCustomVerificationEmailTemplate,
		opUpdateEmailIdentityPolicy,
		opUpdateEmailTemplate,
		opUpdateReputationEntityCustomerManagedStatus,
		opUpdateReputationEntityPolicy,
	}
}

// GetSupportedOperations returns the list of supported SES v2 operations.
func (h *Handler) GetSupportedOperations() []string {
	a := supportedOperationsGroupA()
	b := supportedOperationsGroupB()
	c := supportedOperationsGroupC()
	d := supportedOperationsGroupD()

	ops := make([]string, 0, len(a)+len(b)+len(c)+len(d))
	ops = append(ops, a...)
	ops = append(ops, b...)
	ops = append(ops, c...)
	ops = append(ops, d...)

	return ops
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "sesv2" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this SES v2 instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches SES v2 REST requests.
// SES v2 requests use the /v2/email/ path prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if strings.HasPrefix(r.URL.Path, "/dashboard/") {
			return false
		}

		return strings.HasPrefix(r.URL.Path, sesv2PathPrefix)
	}
}

// MatchPriority returns the routing priority for the SES v2 handler.
// Uses PriorityPathVersioned (85) since it matches a versioned path prefix.
func (h *Handler) MatchPriority() int {
	return service.PriorityPathVersioned
}

// ExtractOperation extracts the SES v2 operation from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := parseSESv2Path(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the identity or config set name from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := parseSESv2Path(c.Request().Method, c.Request().URL.Path)

	if decoded, err := url.PathUnescape(resource); err == nil {
		return decoded
	}

	return resource
}

// parseSESv2Path maps a method + path to a SES v2 operation and resource name.
// Returns (unknownAction, "") when no pattern matches.
func parseSESv2Path(method, path string) (string, string) {
	// Strip /v2/email/ prefix and split remaining path into segments.
	tail := strings.TrimPrefix(path, sesv2PathPrefix)
	tail = strings.TrimSuffix(tail, "/")
	segments := strings.Split(tail, "/")

	if len(segments) == 0 || segments[0] == "" {
		return unknownAction, ""
	}

	switch segments[0] {
	case "identities":
		return parseIdentityPath(method, segments)
	case "outbound-emails":
		if method == http.MethodPost {
			return opSendEmail, ""
		}
	case "configuration-sets":
		return parseConfigSetPath(method, segments)
	case "tags":
		return parseTagsPath(method)
	default:
		return parseMiscPaths(method, segments)
	}

	return unknownAction, ""
}

// Handler returns the Echo handler function for SES v2 requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		op, rawResource := parseSESv2Path(c.Request().Method, c.Request().URL.Path)

		// URL-decode the resource so identities with '@' survive SDK/Terraform percent-encoding.
		// Fall back to the raw value if the path segment is malformed.
		resource := rawResource
		if decoded, err := url.PathUnescape(rawResource); err == nil {
			resource = decoded
		}

		log.DebugContext(ctx, "SESv2 request", "operation", op, "resource", resource)

		if op == unknownAction {
			return h.writeError(c, http.StatusNotFound, "NotFoundException",
				fmt.Sprintf("no route for %s %s", c.Request().Method, c.Request().URL.Path))
		}

		resp, opErr := h.dispatchOp(c, op, resource)

		if opErr != nil {
			return h.handleOpError(c, op, opErr)
		}

		if resp == nil {
			return c.NoContent(http.StatusOK)
		}

		return c.JSON(http.StatusOK, resp)
	}
}

// errOpNotHandled is returned by sub-dispatchers when they don't recognise an operation.
var errOpNotHandled = errors.New("sesv2: operation not handled by this dispatcher")

// dispatchOp routes an already-identified operation to its handler.
func (h *Handler) dispatchOp(c *echo.Context, op, resource string) (any, error) {
	resp, err := h.dispatchCoreOps(c, op, resource)
	if !errors.Is(err, errOpNotHandled) {
		return resp, err
	}

	resp, err = h.dispatchNewOps(c, op, resource)
	if !errors.Is(err, errOpNotHandled) {
		return resp, err
	}

	resp, err = h.dispatchExtendedOps(c, op, resource)
	if !errors.Is(err, errOpNotHandled) {
		return resp, err
	}

	return nil, fmt.Errorf("%w: %s is not a valid SES v2 operation", ErrInvalidInput, op)
}

// ---- shared cross-family types/helpers ----

type emptyDeleteOutput struct{}

type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// tagsFromEntries converts tag key/value list to a map.
func tagsFromEntries(entries []tagEntry) map[string]string {
	if len(entries) == 0 {
		return nil
	}

	tags := make(map[string]string, len(entries))
	for _, e := range entries {
		tags[e.Key] = e.Value
	}

	return tags
}

// tagsToEntries converts a tag map to a sorted key/value list.
func tagsToEntries(tags map[string]string) []tagEntry {
	if len(tags) == 0 {
		return nil
	}

	entries := make([]tagEntry, 0, len(tags))
	for k, v := range tags {
		entries = append(entries, tagEntry{Key: k, Value: v})
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })

	return entries
}

// ---- error handling ----

func (h *Handler) handleOpError(c *echo.Context, op string, opErr error) error {
	switch {
	case errors.Is(opErr, ErrNotFound):
		return h.writeError(c, http.StatusNotFound, "NotFoundException", opErr.Error())
	case errors.Is(opErr, ErrAlreadyExists):
		return h.writeError(c, http.StatusConflict, "AlreadyExistsException", opErr.Error())
	case errors.Is(opErr, ErrInvalidInput):
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", opErr.Error())
	default:
		logger.Load(c.Request().Context()).Error("SESv2 internal error", "error", opErr, "op", op)

		return h.writeError(
			c,
			http.StatusInternalServerError,
			"InternalFailure",
			"internal server error",
		)
	}
}

type sesv2ErrorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	return c.JSON(statusCode, sesv2ErrorResponse{Type: code, Message: message})
}
