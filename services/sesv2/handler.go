package sesv2

import (
	"encoding/json"
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

// GetSupportedOperations returns the list of supported SES v2 operations.
func (h *Handler) GetSupportedOperations() []string { //nolint:funlen
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
		opPutAccountDedicatedIPWarmupAttributes,
		opPutAccountDetails,
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
		opSendBulkEmail,
		opSendCustomVerificationEmail,
		opSendEmail,
		opTagResource,
		opTestRenderEmailTemplate,
		opUntagResource,
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
//

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

// parseMiscPaths handles the newer SES v2 path prefixes.
func parseMiscPaths(method string, segments []string) (string, string) {
	if op, id := parseMiscPathsSpecial(method, segments); op != unknownAction {
		return op, id
	}

	return parseExtendedPaths(method, segments)
}

// parseMiscPathsSpecial handles paths with special POST-create logic before extended dispatch.
func parseMiscPathsSpecial(method string, segments []string) (string, string) {
	switch segments[0] {
	case "metrics":
		return parseMetricsPath(method, segments)
	case "export-jobs":
		return parseExportJobsPath(method, segments)
	case "contact-lists":
		return parseContactListSpecialPath(method, segments)
	case "custom-verification-email-templates":
		if method == http.MethodPost && len(segments) == 1 {
			return opCreateCustomVerificationEmailTemplate, ""
		}
	case "dedicated-ip-pools":
		if method == http.MethodPost && len(segments) == 1 {
			return opCreateDedicatedIPPool, ""
		}
	case "deliverability-dashboard":
		return parseDeliverabilityDashboardPath(method, segments)
	case "templates":
		if method == http.MethodPost && len(segments) == 1 {
			return opCreateEmailTemplate, ""
		}
	}

	return unknownAction, ""
}

// parseContactListSpecialPath handles contact list POST-create paths.
func parseContactListSpecialPath(method string, segments []string) (string, string) {
	if method == http.MethodPost && len(segments) == 1 {
		return opCreateContactList, ""
	}

	if method == http.MethodPost && len(segments) == 3 && segments[2] == segContacts {
		return opCreateContact, segments[1]
	}

	return unknownAction, ""
}

const metricsPathSegments = 2
const exportJobPathSegments = 3

func parseMetricsPath(method string, segments []string) (string, string) {
	if len(segments) == metricsPathSegments && segments[1] == "batch" && method == http.MethodPost {
		return opBatchGetMetricData, ""
	}

	return unknownAction, ""
}

func parseExportJobsPath(method string, segments []string) (string, string) {
	if len(segments) == exportJobPathSegments && segments[2] == "cancel" &&
		method == http.MethodPut {
		return opCancelExportJob, segments[1]
	}

	return unknownAction, ""
}

func parseDeliverabilityDashboardPath(method string, segments []string) (string, string) {
	if len(segments) == metricsPathSegments && segments[1] == "test" && method == http.MethodPost {
		return opCreateDeliverabilityTestReport, ""
	}

	return unknownAction, ""
}

func parseIdentityPath(method string, segments []string) (string, string) {
	switch {
	case method == http.MethodGet && len(segments) == 1:
		return opListEmailIdentities, ""
	case method == http.MethodPost && len(segments) == 1:
		return opCreateEmailIdentity, ""
	case method == http.MethodGet && len(segments) == 2:
		return opGetEmailIdentity, segments[1]
	case method == http.MethodDelete && len(segments) == 2:
		return opDeleteEmailIdentity, segments[1]
	case method == http.MethodPost && len(segments) == 4 && segments[2] == segPolicies:
		// POST /v2/email/identities/{identity}/policies/{policyName}
		return opCreateEmailIdentityPolicy, segments[1]
	}

	return parseIdentityExtPath(method, segments)
}

func parseConfigSetPath(method string, segments []string) (string, string) {
	switch {
	case method == http.MethodGet && len(segments) == 1:
		return opListConfigurationSets, ""
	case method == http.MethodPost && len(segments) == 1:
		return opCreateConfigurationSet, ""
	case method == http.MethodGet && len(segments) == 2:
		return opGetConfigurationSet, segments[1]
	case method == http.MethodDelete && len(segments) == 2:
		return opDeleteConfigurationSet, segments[1]
	case method == http.MethodPost && len(segments) == 3 && segments[2] == segEventDestinations:
		return opCreateConfigurationSetEventDestination, segments[1]
	}

	return parseConfigSetExtPath(method, segments)
}

func parseTagsPath(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return opListTagsForResource, ""
	case http.MethodPost:
		return opTagResource, ""
	case http.MethodDelete:
		return opUntagResource, ""
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

// dispatchCoreOps handles the original 12 SES v2 operations.
func (h *Handler) dispatchCoreOps(c *echo.Context, op, resource string) (any, error) {
	switch op {
	case opCreateEmailIdentity:
		return h.handleCreateEmailIdentity(c)
	case opGetEmailIdentity:
		return h.handleGetEmailIdentity(resource)
	case opListEmailIdentities:
		return h.handleListEmailIdentities(c), nil
	case opDeleteEmailIdentity:
		return h.handleDeleteEmailIdentity(resource)
	case opSendEmail:
		return h.handleSendEmail(c)
	case opCreateConfigurationSet:
		return h.handleCreateConfigurationSet(c)
	case opGetConfigurationSet:
		return h.handleGetConfigurationSet(resource)
	case opListConfigurationSets:
		return h.handleListConfigurationSets(c), nil
	case opDeleteConfigurationSet:
		return h.handleDeleteConfigurationSet(resource)
	case opListTagsForResource:
		return h.handleListTagsForResource(), nil
	case opTagResource, opUntagResource:
		return &emptyDeleteOutput{}, nil
	default:
		return nil, errOpNotHandled
	}
}

// dispatchNewOps handles the 10 newly added SES v2 operations.
func (h *Handler) dispatchNewOps(c *echo.Context, op, resource string) (any, error) {
	switch op {
	case opBatchGetMetricData:
		return h.handleBatchGetMetricData(c)
	case opCancelExportJob:
		return h.handleCancelExportJob(resource)
	case opCreateConfigurationSetEventDestination:
		return h.handleCreateConfigurationSetEventDestination(c, resource)
	case opCreateContact:
		return h.handleCreateContact(c, resource)
	case opCreateContactList:
		return h.handleCreateContactList(c)
	case opCreateCustomVerificationEmailTemplate:
		return h.handleCreateCustomVerificationEmailTemplate(c)
	case opCreateDedicatedIPPool:
		return h.handleCreateDedicatedIPPool(c)
	case opCreateDeliverabilityTestReport:
		return h.handleCreateDeliverabilityTestReport(c)
	case opCreateEmailIdentityPolicy:
		return h.handleCreateEmailIdentityPolicy(c, resource)
	case opCreateEmailTemplate:
		return h.handleCreateEmailTemplate(c)
	default:
		return nil, errOpNotHandled
	}
}

// ---- request types ----

type createEmailIdentityInput struct {
	EmailIdentity        string     `json:"EmailIdentity"`
	ConfigurationSetName string     `json:"ConfigurationSetName"`
	Tags                 []tagEntry `json:"Tags"`
}

type sendEmailInput struct {
	Content          emailContent     `json:"Content"`
	FromEmailAddress string           `json:"FromEmailAddress"`
	Destination      emailDestination `json:"Destination"`
}

type emailDestination struct {
	ToAddresses  []string `json:"ToAddresses"`
	CcAddresses  []string `json:"CcAddresses"`
	BccAddresses []string `json:"BccAddresses"`
}

type emailContent struct {
	Simple *simpleEmailContent `json:"Simple"`
	Raw    *rawEmailContent    `json:"Raw"`
}

type simpleEmailContent struct {
	Body    emailBody `json:"Body"`
	Subject emailData `json:"Subject"`
}

type emailBody struct {
	Text *emailData `json:"Text"`
	HTML *emailData `json:"Html"`
}

type emailData struct {
	Data    string `json:"Data"`
	Charset string `json:"Charset"`
}

type rawEmailContent struct {
	Data []byte `json:"Data"`
}

type createConfigurationSetInput struct {
	ConfigurationSetName string `json:"ConfigurationSetName"`
}

// ---- response types ----

type dkimAttributesOutput struct {
	Status                  string   `json:"Status,omitempty"`
	SigningAttributesOrigin string   `json:"SigningAttributesOrigin,omitempty"`
	Tokens                  []string `json:"Tokens,omitempty"`
	SigningEnabled          bool     `json:"SigningEnabled"`
}

type mailFromAttributesOutput struct {
	MailFromDomain       string `json:"MailFromDomain,omitempty"`
	MailFromDomainStatus string `json:"MailFromDomainStatus,omitempty"`
	BehaviorOnMxFailure  string `json:"BehaviorOnMxFailure,omitempty"`
}

type createEmailIdentityOutput struct {
	DkimAttributes     *dkimAttributesOutput `json:"DkimAttributes,omitempty"`
	IdentityType       string                `json:"IdentityType"`
	VerifiedForSending bool                  `json:"VerifiedForSendingStatus"`
}

type getEmailIdentityOutput struct {
	DkimAttributes       *dkimAttributesOutput     `json:"DkimAttributes,omitempty"`
	MailFromAttributes   *mailFromAttributesOutput `json:"MailFromAttributes,omitempty"`
	Policies             map[string]string         `json:"Policies,omitempty"`
	EmailIdentity        string                    `json:"EmailIdentity"`
	IdentityType         string                    `json:"IdentityType"`
	ConfigurationSetName string                    `json:"ConfigurationSetName,omitempty"`
	VerificationStatus   string                    `json:"VerificationStatus,omitempty"`
	Tags                 []tagEntry                `json:"Tags,omitempty"`
	VerifiedForSending   bool                      `json:"VerifiedForSendingStatus"`
	FeedbackForwarding   bool                      `json:"FeedbackForwardingStatus"`
}

type emailIdentitySummary struct {
	IdentityName   string `json:"IdentityName"`
	IdentityType   string `json:"IdentityType"`
	SendingEnabled bool   `json:"SendingEnabled"`
}

type listEmailIdentitiesOutput struct {
	NextToken       string                 `json:"NextToken,omitempty"`
	EmailIdentities []emailIdentitySummary `json:"EmailIdentities"`
}

type sendEmailOutput struct {
	MessageID string `json:"MessageId"`
}

type trackingOptionsOutput struct {
	CustomRedirectDomain string `json:"CustomRedirectDomain,omitempty"`
	HTTPSPolicy          string `json:"HttpsPolicy,omitempty"`
}

type deliveryOptionsOutput struct {
	TLSPolicy       string `json:"TlsPolicy,omitempty"`
	SendingPoolName string `json:"SendingPoolName,omitempty"`
}

type reputationOptionsOutput struct {
	ReputationMetricsEnabled bool `json:"ReputationMetricsEnabled"`
}

type sendingOptionsOutput struct {
	SendingEnabled bool `json:"SendingEnabled"`
}

type suppressionOptionsOutput struct {
	SuppressedReasons []string `json:"SuppressedReasons,omitempty"`
}

type createConfigurationSetOutput struct{}

type getConfigurationSetOutput struct {
	TrackingOptions      *trackingOptionsOutput    `json:"TrackingOptions,omitempty"`
	DeliveryOptions      *deliveryOptionsOutput    `json:"DeliveryOptions,omitempty"`
	ReputationOptions    *reputationOptionsOutput  `json:"ReputationOptions,omitempty"`
	SendingOptions       *sendingOptionsOutput     `json:"SendingOptions,omitempty"`
	SuppressionOptions   *suppressionOptionsOutput `json:"SuppressionOptions,omitempty"`
	ConfigurationSetName string                    `json:"ConfigurationSetName"`
	Tags                 []tagEntry                `json:"Tags,omitempty"`
}

type configurationSetSummary struct {
	Name string `json:"Name"`
}

type listConfigurationSetsOutput struct {
	NextToken         string                    `json:"NextToken,omitempty"`
	ConfigurationSets []configurationSetSummary `json:"ConfigurationSets"`
}

type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type listTagsOutput struct {
	Tags []tagEntry `json:"Tags"`
}

// ---- action handlers ----

func (h *Handler) handleCreateEmailIdentity(c *echo.Context) (any, error) {
	var in createEmailIdentityInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidParameter, err.Error())
	}

	tags := tagsFromEntries(in.Tags)

	ei, err := h.Backend.CreateEmailIdentity(in.EmailIdentity, in.ConfigurationSetName, tags)
	if err != nil {
		return nil, err
	}

	out := &createEmailIdentityOutput{
		IdentityType:       ei.IdentityType,
		VerifiedForSending: ei.VerifiedForSending,
	}

	if len(ei.DkimTokens) > 0 {
		out.DkimAttributes = &dkimAttributesOutput{
			SigningEnabled:          ei.DkimSigningEnabled,
			Status:                  ei.DkimStatus,
			SigningAttributesOrigin: "AWS_SES",
			Tokens:                  ei.DkimTokens,
		}
	}

	return out, nil
}

func (h *Handler) handleGetEmailIdentity(identity string) (any, error) {
	ei, err := h.Backend.GetEmailIdentity(identity)
	if err != nil {
		return nil, err
	}

	policies, _ := h.Backend.GetEmailIdentityPolicies(identity)

	out := &getEmailIdentityOutput{
		EmailIdentity:        ei.Identity,
		IdentityType:         ei.IdentityType,
		VerifiedForSending:   ei.VerifiedForSending,
		FeedbackForwarding:   ei.FeedbackForwarding,
		ConfigurationSetName: ei.ConfigurationSetName,
		VerificationStatus:   ei.VerificationStatus,
		Policies:             policies,
		Tags:                 tagsToEntries(ei.Tags),
	}

	out.DkimAttributes = &dkimAttributesOutput{
		SigningEnabled:          ei.DkimSigningEnabled,
		Status:                  ei.DkimStatus,
		SigningAttributesOrigin: "AWS_SES",
		Tokens:                  ei.DkimTokens,
	}

	if ei.MailFromDomain != "" {
		out.MailFromAttributes = &mailFromAttributesOutput{
			MailFromDomain:       ei.MailFromDomain,
			MailFromDomainStatus: ei.MailFromDomainStatus,
			BehaviorOnMxFailure:  ei.BehaviorOnMxFailure,
		}
	}

	return out, nil
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

func (h *Handler) handleListEmailIdentities(c *echo.Context) any {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListEmailIdentities(nextToken, 0)

	items := make([]emailIdentitySummary, 0, len(pg.Data))

	for _, ei := range pg.Data {
		items = append(items, emailIdentitySummary{
			IdentityName:   ei.Identity,
			IdentityType:   ei.IdentityType,
			SendingEnabled: ei.VerifiedForSending,
		})
	}

	return &listEmailIdentitiesOutput{
		EmailIdentities: items,
		NextToken:       pg.Next,
	}
}

type emptyDeleteOutput struct{}

func (h *Handler) handleDeleteEmailIdentity(identity string) (any, error) {
	if err := h.Backend.DeleteEmailIdentity(identity); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleSendEmail(c *echo.Context) (any, error) {
	var in sendEmailInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidParameter, err.Error())
	}

	from := in.FromEmailAddress
	to := in.Destination.ToAddresses

	var subject, bodyHTML, bodyText string

	if in.Content.Simple != nil {
		subject = in.Content.Simple.Subject.Data
		if in.Content.Simple.Body.HTML != nil {
			bodyHTML = in.Content.Simple.Body.HTML.Data
		}

		if in.Content.Simple.Body.Text != nil {
			bodyText = in.Content.Simple.Body.Text.Data
		}
	}

	msgID, err := h.Backend.SendEmail(from, to, subject, bodyHTML, bodyText)
	if err != nil {
		return nil, err
	}

	return &sendEmailOutput{MessageID: msgID}, nil
}

func (h *Handler) handleCreateConfigurationSet(c *echo.Context) (any, error) {
	var in createConfigurationSetInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidParameter, err.Error())
	}

	if _, err := h.Backend.CreateConfigurationSet(in.ConfigurationSetName); err != nil {
		return nil, err
	}

	return &createConfigurationSetOutput{}, nil
}

func (h *Handler) handleGetConfigurationSet(name string) (any, error) {
	cs, err := h.Backend.GetConfigurationSet(name)
	if err != nil {
		return nil, err
	}

	out := &getConfigurationSetOutput{
		ConfigurationSetName: cs.Name,
		SendingOptions:       &sendingOptionsOutput{SendingEnabled: cs.SendingEnabled},
		ReputationOptions:    &reputationOptionsOutput{ReputationMetricsEnabled: cs.ReputationMetricsEnabled},
		Tags:                 tagsToEntries(cs.Tags),
	}

	if cs.TrackingCustomRedirectDomain != "" || cs.TrackingHTTPSPolicy != "" {
		out.TrackingOptions = &trackingOptionsOutput{
			CustomRedirectDomain: cs.TrackingCustomRedirectDomain,
			HTTPSPolicy:          cs.TrackingHTTPSPolicy,
		}
	}

	if cs.DeliveryTLSPolicy != "" || cs.DeliverySendingPoolName != "" {
		out.DeliveryOptions = &deliveryOptionsOutput{
			TLSPolicy:       cs.DeliveryTLSPolicy,
			SendingPoolName: cs.DeliverySendingPoolName,
		}
	}

	if len(cs.SuppressionReasons) > 0 {
		out.SuppressionOptions = &suppressionOptionsOutput{
			SuppressedReasons: cs.SuppressionReasons,
		}
	}

	return out, nil
}

func (h *Handler) handleListConfigurationSets(c *echo.Context) any {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListConfigurationSets(nextToken, 0)

	items := make([]configurationSetSummary, 0, len(pg.Data))

	for _, cs := range pg.Data {
		items = append(items, configurationSetSummary{Name: cs.Name})
	}

	return &listConfigurationSetsOutput{
		ConfigurationSets: items,
		NextToken:         pg.Next,
	}
}

func (h *Handler) handleDeleteConfigurationSet(name string) (any, error) {
	if err := h.Backend.DeleteConfigurationSet(name); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// handleListTagsForResource returns an empty tag list.
// The SES v2 Terraform provider calls this after every create to refresh tag state.
// Tags are not persisted in this implementation.
func (h *Handler) handleListTagsForResource() any {
	return &listTagsOutput{Tags: []tagEntry{}}
}

// ---- new operation request/response types ----

type batchGetMetricDataInput struct {
	Queries []struct {
		ID        string `json:"Id"`
		Namespace string `json:"Namespace"`
		Metric    string `json:"Metric"`
	} `json:"Queries"`
}

type batchGetMetricDataOutput struct {
	Results []struct {
		ID         string    `json:"Id"`
		Timestamps []any     `json:"Timestamps"`
		Values     []float64 `json:"Values"`
	} `json:"Results"`
}

type createConfigurationSetEventDestinationInput struct {
	EventDestinationName string `json:"EventDestinationName"`
	EventDestination     struct {
		MatchingEventTypes []string `json:"MatchingEventTypes"`
		Enabled            bool     `json:"Enabled"`
	} `json:"EventDestination"`
}

type createContactInput struct {
	EmailAddress     string            `json:"EmailAddress"`
	TopicPreferences []TopicPreference `json:"TopicPreferences"`
}

type createContactListInput struct {
	ContactListName string `json:"ContactListName"`
	Description     string `json:"Description"`
}

type createCustomVerificationEmailTemplateInput struct {
	TemplateName          string `json:"TemplateName"`
	FromEmailAddress      string `json:"FromEmailAddress"`
	TemplateSubject       string `json:"TemplateSubject"`
	TemplateContent       string `json:"TemplateContent"`
	SuccessRedirectionURL string `json:"SuccessRedirectionURL"`
	FailureRedirectionURL string `json:"FailureRedirectionURL"`
}

type createDedicatedIPPoolInput struct {
	PoolName    string `json:"PoolName"`
	ScalingMode string `json:"ScalingMode"`
}

type createDeliverabilityTestReportInput struct {
	ReportName       string `json:"ReportName"`
	FromEmailAddress string `json:"FromEmailAddress"`
}

type createDeliverabilityTestReportOutput struct {
	ReportID                 string `json:"ReportId"`
	DeliverabilityTestStatus string `json:"DeliverabilityTestStatus"`
}

type createEmailIdentityPolicyInput struct {
	Policy string `json:"Policy"`
}

type createEmailTemplateInput struct {
	TemplateName    string               `json:"TemplateName"`
	TemplateContent EmailTemplateContent `json:"TemplateContent"`
}

// ---- new operation handlers ----

func (h *Handler) handleBatchGetMetricData(c *echo.Context) (any, error) {
	var in batchGetMetricDataInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	queries := make([]MetricDataQuery, 0, len(in.Queries))
	for _, q := range in.Queries {
		queries = append(
			queries,
			MetricDataQuery{ID: q.ID, Namespace: q.Namespace, Metric: q.Metric},
		)
	}

	results, err := h.Backend.BatchGetMetricData(queries)
	if err != nil {
		return nil, err
	}

	out := batchGetMetricDataOutput{}
	for _, r := range results {
		out.Results = append(out.Results, struct {
			ID         string    `json:"Id"`
			Timestamps []any     `json:"Timestamps"`
			Values     []float64 `json:"Values"`
		}{
			ID:         r.ID,
			Timestamps: []any{},
			Values:     r.Values,
		})
	}

	if out.Results == nil {
		out.Results = []struct {
			ID         string    `json:"Id"`
			Timestamps []any     `json:"Timestamps"`
			Values     []float64 `json:"Values"`
		}{}
	}

	return &out, nil
}

func (h *Handler) handleCancelExportJob(jobID string) (any, error) {
	if err := h.Backend.CancelExportJob(jobID); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleCreateConfigurationSetEventDestination(
	c *echo.Context,
	configSetName string,
) (any, error) {
	var in createConfigurationSetEventDestinationInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if _, err := h.Backend.CreateConfigurationSetEventDestination(
		configSetName,
		in.EventDestinationName,
		in.EventDestination.Enabled,
		in.EventDestination.MatchingEventTypes,
	); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleCreateContact(c *echo.Context, contactListName string) (any, error) {
	var in createContactInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if _, err := h.Backend.CreateContact(contactListName, in.EmailAddress, in.TopicPreferences); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleCreateContactList(c *echo.Context) (any, error) {
	var in createContactListInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if _, err := h.Backend.CreateContactList(in.ContactListName, in.Description); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleCreateCustomVerificationEmailTemplate(c *echo.Context) (any, error) {
	var in createCustomVerificationEmailTemplateInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	tmpl := &CustomVerificationEmailTemplate{
		TemplateName:          in.TemplateName,
		FromEmailAddress:      in.FromEmailAddress,
		TemplateSubject:       in.TemplateSubject,
		TemplateContent:       in.TemplateContent,
		SuccessRedirectionURL: in.SuccessRedirectionURL,
		FailureRedirectionURL: in.FailureRedirectionURL,
	}

	if _, err := h.Backend.CreateCustomVerificationEmailTemplate(tmpl); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleCreateDedicatedIPPool(c *echo.Context) (any, error) {
	var in createDedicatedIPPoolInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if _, err := h.Backend.CreateDedicatedIPPool(in.PoolName, in.ScalingMode); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleCreateDeliverabilityTestReport(c *echo.Context) (any, error) {
	var in createDeliverabilityTestReportInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	report, err := h.Backend.CreateDeliverabilityTestReport(in.ReportName, in.FromEmailAddress)
	if err != nil {
		return nil, err
	}

	return &createDeliverabilityTestReportOutput{
		ReportID:                 report.ReportID,
		DeliverabilityTestStatus: report.DeliverabilityTestStatus,
	}, nil
}

const policyPathMinSegments = 4

func (h *Handler) handleCreateEmailIdentityPolicy(c *echo.Context, identity string) (any, error) {
	// Extract policyName from the URL: .../identities/{identity}/policies/{policyName}
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < policyPathMinSegments {
		return nil, fmt.Errorf("%w: invalid policy path", ErrInvalidInput)
	}

	policyName := segments[3]
	if decoded, decErr := url.PathUnescape(policyName); decErr == nil {
		policyName = decoded
	}

	var in createEmailIdentityPolicyInput

	if decErr := json.NewDecoder(c.Request().Body).Decode(&in); decErr != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, decErr.Error())
	}

	if backErr := h.Backend.CreateEmailIdentityPolicy(identity, policyName, in.Policy); backErr != nil {
		return nil, backErr
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleCreateEmailTemplate(c *echo.Context) (any, error) {
	var in createEmailTemplateInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if _, err := h.Backend.CreateEmailTemplate(in.TemplateName, &in.TemplateContent); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
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
