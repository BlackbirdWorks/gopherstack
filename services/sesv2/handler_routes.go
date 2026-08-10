package sesv2

import (
	"net/http"
	"sync"
)

// path segment constants used across multiple parse functions.
const (
	segContacts          = "contacts"
	segPolicies          = "policies"
	segEventDestinations = "event-destinations"
	segResources         = "resources"
	segSuppression       = "suppression"
	keyNextToken         = "NextToken"

	// path depth sentinels for segment-count comparisons.
	pathDepth2 = 2
	pathDepth3 = 3
	pathDepth4 = 4
	pathDepth5 = 5
)

const (
	metricsPathSegments   = 2
	exportJobPathSegments = 3
)

const policyPathMinSegments = 4

// RPC-style path verb segments, shared by the tenant/import-job families
// (goconst: "list" alone appears 5+ times across those routing helpers).
const (
	verbGet    = "get"
	verbDelete = "delete"
	verbList   = "list"
)

// parseMiscPaths handles the newer SES v2 path prefixes.
func parseMiscPaths(method string, segments []string) (string, string) {
	if op, id := parseMiscPathsSpecial(method, segments); op != unknownAction {
		return op, id
	}

	return parseExtendedPaths(method, segments)
}

// onceSingleSegmentPostOps lazily initialises the lookup table for top-level
// path segments where "POST /v2/email/{segment}" (exactly one path segment,
// no sub-resource) maps directly to a single op -- e.g.
// "POST /v2/email/templates" -> CreateEmailTemplate,
// "POST /v2/email/list-export-jobs" -> ListExportJobs (an RPC-style op that
// happens to share this same single-segment-POST shape). Factored out of
// parseMiscPathsSpecial as a table to keep that function's cyclomatic
// complexity down.
//
//nolint:gochecknoglobals // read-only package-level lookup table
var onceSingleSegmentPostOps = sync.OnceValue(func() map[string]string {
	return map[string]string{
		"list-export-jobs":                    opListExportJobs,
		"custom-verification-email-templates": opCreateCustomVerificationEmailTemplate,
		"dedicated-ip-pools":                  opCreateDedicatedIPPool,
		"templates":                           opCreateEmailTemplate,
	}
})

// parseMiscPathsSpecial handles paths with special POST-create logic before extended dispatch.
func parseMiscPathsSpecial(method string, segments []string) (string, string) {
	switch segments[0] {
	case "metrics":
		return parseMetricsPath(method, segments)
	case "export-jobs":
		return parseExportJobsPath(method, segments)
	case "contact-lists":
		return parseContactListSpecialPath(method, segments)
	case "deliverability-dashboard":
		return parseDeliverabilityDashboardPath(method, segments)
	}

	if method == http.MethodPost && len(segments) == 1 {
		if op, ok := onceSingleSegmentPostOps()[segments[0]]; ok {
			return op, ""
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

// ---- path parsing for new ops ----

// parseExtendedPaths parses paths for newly-added operations.
// It is called from parseMiscPaths when the first segment is not handled by the
// original set of patterns.
func parseExtendedPaths(method string, segments []string) (string, string) {
	if op, id := parseExtendedPathsCoreGroup(method, segments); op != unknownAction {
		return op, id
	}

	return parseExtendedPathsExtGroup(method, segments)
}

// parseExtendedPathsCoreGroup handles core extended paths (account through recommendations).
func parseExtendedPathsCoreGroup(method string, segments []string) (string, string) {
	switch segments[0] {
	case "account":
		return parseAccountPath(method, segments)
	case segSuppression:
		return parseSuppressionPath(method, segments)
	case "contact-lists":
		return parseContactListExtPath(method, segments)
	case "custom-verification-email-templates":
		return parseCustomVerificationPath(method, segments)
	case "dedicated-ip-pools":
		return parseDedicatedIPPoolExtPath(method, segments)
	case "dedicated-ips":
		return parseDedicatedIPsPath(method, segments)
	case "deliverability-dashboard":
		return parseDeliverabilityExtPath(method, segments)
	}

	return parseExtendedInsightsPath(method, segments)
}

// parseExtendedInsightsPath handles insights and recommendations paths.
// Real SDK paths (verified against aws-sdk-go-v2/service/sesv2 v1.60.1
// serializers.go):
//
//	POST /v2/email/email-address-insights   -> GetEmailAddressInsights (EmailAddress in body)
//	GET  /v2/email/insights/{MessageId}     -> GetMessageInsights
//	POST /v2/email/vdm/recommendations      -> ListRecommendations (Filter/NextToken/PageSize in body)
func parseExtendedInsightsPath(method string, segments []string) (string, string) {
	switch segments[0] {
	case "email-address-insights":
		if method == http.MethodPost && len(segments) == 1 {
			return opGetEmailAddressInsights, ""
		}
	case "insights":
		if method == http.MethodGet && len(segments) == pathDepth2 {
			return opGetMessageInsights, segments[1]
		}
	case "vdm":
		if method == http.MethodPost && len(segments) == pathDepth2 && segments[1] == "recommendations" {
			return opListRecommendations, ""
		}
	}

	return unknownAction, ""
}

// parseExtendedPathsExtGroup handles extended paths (templates through reputation entities).
func parseExtendedPathsExtGroup(method string, segments []string) (string, string) {
	switch segments[0] {
	case "templates":
		return parseTemplateExtPath(method, segments)
	case "export-jobs":
		return parseExportJobExtPath(method, segments)
	case "import-jobs":
		return parseImportJobPath(method, segments)
	case "outbound-bulk-emails":
		return parseOutboundBulkEmailsPath(method, segments)
	case "outbound-custom-verification-emails":
		return parseOutboundCustomVerificationEmailsPath(method, segments)
	case "multi-region-endpoints":
		return parseMultiRegionEndpointPath(method, segments)
	case "tenants":
		return parseTenantPath(method, segments)
	case "tenant":
		return parseTenantSuppressionPath(method, segments)
	case "resources":
		return parseResourceTenantsPath(method, segments)
	case "reputation":
		return parseReputationPath(method, segments)
	}

	return unknownAction, ""
}

// parseReputationPath routes the AWS SDK-shaped reputation entity paths of the
// form /v2/email/reputation/entities/{Type}/{Reference}[/customer-managed-status|/policy].
// The entity reference is returned as the resource identifier.
func parseReputationPath(method string, segments []string) (string, string) {
	if len(segments) < pathDepth2 || segments[1] != "entities" {
		return unknownAction, ""
	}

	switch {
	case len(segments) == pathDepth2 && method == http.MethodPost:
		return opListReputationEntities, ""
	case len(segments) == pathDepth4 && method == http.MethodGet:
		return opGetReputationEntity, segments[3]
	case len(segments) == pathDepth5 && segments[4] == "customer-managed-status" &&
		method == http.MethodPut:
		return opUpdateReputationEntityCustomerManagedStatus, segments[3]
	case len(segments) == pathDepth5 && segments[4] == "policy" && method == http.MethodPut:
		return opUpdateReputationEntityPolicy, segments[3]
	}

	return unknownAction, ""
}

// parseOutboundBulkEmailsPath routes outbound bulk email paths.
func parseOutboundBulkEmailsPath(method string, segments []string) (string, string) {
	if method == http.MethodPost && len(segments) == 1 {
		return opSendBulkEmail, ""
	}

	return unknownAction, ""
}

// parseOutboundCustomVerificationEmailsPath routes
// POST /v2/email/outbound-custom-verification-emails -> SendCustomVerificationEmail.
func parseOutboundCustomVerificationEmailsPath(method string, segments []string) (string, string) {
	if method == http.MethodPost && len(segments) == 1 {
		return opSendCustomVerificationEmail, ""
	}

	return unknownAction, ""
}

// parseResourceTenantsPath routes POST /v2/email/resources/tenants/list ->
// ListResourceTenants (ResourceArn in the JSON body).
func parseResourceTenantsPath(method string, segments []string) (string, string) {
	if method == http.MethodPost && len(segments) == pathDepth3 &&
		segments[1] == "tenants" && segments[2] == verbList {
		return opListResourceTenants, ""
	}

	return unknownAction, ""
}

// parseAccountPath routes account and account attribute paths. The real SDK
// paths are (see aws-sdk-go-v2/service/sesv2 serializers.go):
//
//	GET  /v2/email/account                       -> GetAccount
//	PUT  /v2/email/account/dedicated-ips/warmup  -> PutAccountDedicatedIpWarmupAttributes
//	POST /v2/email/account/details                -> PutAccountDetails
//	PUT  /v2/email/account/pricing-attributes      -> PutAccountPricingAttributes
//	PUT  /v2/email/account/sending                -> PutAccountSendingAttributes
//	PUT  /v2/email/account/suppression             -> PutAccountSuppressionAttributes
//	PUT  /v2/email/account/vdm                     -> PutAccountVdmAttributes
func parseAccountPath(method string, segments []string) (string, string) {
	if len(segments) == 1 && method == http.MethodGet {
		return opGetAccount, ""
	}

	if len(segments) == pathDepth3 && segments[1] == "dedicated-ips" && segments[2] == "warmup" &&
		method == http.MethodPut {
		return opPutAccountDedicatedIPWarmupAttributes, ""
	}

	if len(segments) != pathDepth2 {
		return unknownAction, ""
	}

	return parseAccountAttrPath(method, segments[1])
}

// parseAccountAttrPath routes account attribute sub-paths.
func parseAccountAttrPath(method, sub string) (string, string) {
	switch sub {
	case "details":
		if method == http.MethodPost {
			return opPutAccountDetails, ""
		}
	case "pricing-attributes":
		if method == http.MethodPut {
			return opPutAccountPricingAttributes, ""
		}
	case "sending":
		if method == http.MethodPut {
			return opPutAccountSendingAttributes, ""
		}
	case segSuppression:
		if method == http.MethodPut {
			return opPutAccountSuppressionAttributes, ""
		}
	case "vdm":
		if method == http.MethodPut {
			return opPutAccountVdmAttributes, ""
		}
	}

	return unknownAction, ""
}

// parseSuppressionPath routes the real SDK suppression-list paths:
//
//	GET    /v2/email/suppression/addresses               -> ListSuppressedDestinations
//	PUT    /v2/email/suppression/addresses               -> PutSuppressedDestination
//	GET    /v2/email/suppression/addresses/{EmailAddress} -> GetSuppressedDestination
//	DELETE /v2/email/suppression/addresses/{EmailAddress} -> DeleteSuppressedDestination
func parseSuppressionPath(method string, segments []string) (string, string) {
	if len(segments) < pathDepth2 || segments[1] != "addresses" {
		return unknownAction, ""
	}

	switch {
	case len(segments) == pathDepth2 && method == http.MethodGet:
		return opListSuppressedDestinations, ""
	case len(segments) == pathDepth2 && method == http.MethodPut:
		return opPutSuppressedDestination, ""
	case len(segments) == pathDepth3 && method == http.MethodGet:
		return opGetSuppressedDestination, segments[2]
	case len(segments) == pathDepth3 && method == http.MethodDelete:
		return opDeleteSuppressedDestination, segments[2]
	}

	return unknownAction, ""
}

// parseContactListExtPath routes contact list and contact paths. Note that
// ListContacts is POST .../contacts/list (filters/pagination travel in the
// JSON body, not the query string) -- there is no GET .../contacts route in
// the real API.
func parseContactListExtPath(method string, segments []string) (string, string) {
	switch len(segments) {
	case 1:
		if method == http.MethodGet {
			return opListContactLists, ""
		}
	case pathDepth2:
		switch method {
		case http.MethodGet:
			return opGetContactList, segments[1]
		case http.MethodDelete:
			return opDeleteContactList, segments[1]
		case http.MethodPut:
			return opUpdateContactList, segments[1]
		}
	case pathDepth4:
		if segments[2] == segContacts {
			return parseContactItemPath(method, segments)
		}
	}

	return unknownAction, ""
}

// parseContactItemPath disambiguates the two 4-segment contact paths:
// .../contacts/list (POST, ListContacts) vs .../contacts/{EmailAddress}
// (GET/DELETE/PUT, Get/Delete/UpdateContact).
func parseContactItemPath(method string, segments []string) (string, string) {
	if segments[3] == verbList && method == http.MethodPost {
		return opListContacts, segments[1]
	}

	switch method {
	case http.MethodGet:
		return opGetContact, segments[1]
	case http.MethodDelete:
		return opDeleteContact, segments[1]
	case http.MethodPut:
		return opUpdateContact, segments[1]
	}

	return unknownAction, ""
}

func parseCustomVerificationPath(method string, segments []string) (string, string) {
	switch {
	case len(segments) == 1 && method == http.MethodGet:
		return opListCustomVerificationEmailTemplates, ""
	case len(segments) == 2 && method == http.MethodGet:
		return opGetCustomVerificationEmailTemplate, segments[1]
	case len(segments) == 2 && method == http.MethodDelete:
		return opDeleteCustomVerificationEmailTemplate, segments[1]
	case len(segments) == 2 && method == http.MethodPut:
		return opUpdateCustomVerificationEmailTemplate, segments[1]
	}

	return unknownAction, ""
}

func parseDedicatedIPPoolExtPath(method string, segments []string) (string, string) {
	switch {
	case len(segments) == 1 && method == http.MethodGet:
		return opListDedicatedIPPools, ""
	case len(segments) == 2 && method == http.MethodGet:
		return opGetDedicatedIPPool, segments[1]
	case len(segments) == 2 && method == http.MethodDelete:
		return opDeleteDedicatedIPPool, segments[1]
	case len(segments) == pathDepth3 && segments[2] == "scaling" && method == http.MethodPut:
		return opPutDedicatedIPPoolScalingAttributes, segments[1]
	}

	return unknownAction, ""
}

func parseDedicatedIPsPath(method string, segments []string) (string, string) {
	switch {
	case len(segments) == 1 && method == http.MethodGet:
		return opGetDedicatedIps, ""
	case len(segments) == 2 && method == http.MethodGet:
		return opGetDedicatedIP, segments[1]
	case len(segments) == pathDepth3 && segments[2] == "pool" && method == http.MethodPut:
		return opPutDedicatedIPInPool, segments[1]
	case len(segments) == pathDepth3 && segments[2] == "warmup" && method == http.MethodPut:
		return opPutDedicatedIPWarmupAttributes, segments[1]
	}

	return unknownAction, ""
}

// parseDeliverabilityExtPath routes deliverability dashboard paths.
func parseDeliverabilityExtPath(method string, segments []string) (string, string) {
	if len(segments) == 1 {
		switch method {
		case http.MethodGet:
			return opGetDeliverabilityDashboardOptions, ""
		case http.MethodPut:
			return opPutDeliverabilityDashboardOption, ""
		}

		return unknownAction, ""
	}

	if segments[1] == "test" && method == http.MethodPost {
		return opCreateDeliverabilityTestReport, ""
	}

	return parseDeliverabilitySubPath(method, segments)
}

// parseDeliverabilitySubPath routes deliverability report and campaign
// sub-paths. Real SDK paths (verified against
// aws-sdk-go-v2/service/sesv2 v1.60.1 serializers.go):
//
//	GET /v2/email/deliverability-dashboard/test-reports[/{ReportId}]
//	GET /v2/email/deliverability-dashboard/statistics-report/{Domain}
//	GET /v2/email/deliverability-dashboard/campaigns/{CampaignId}
//	GET /v2/email/deliverability-dashboard/domains/{SubscribedDomain}/campaigns
func parseDeliverabilitySubPath(method string, segments []string) (string, string) {
	if len(segments) < pathDepth2 {
		return unknownAction, ""
	}

	switch segments[1] {
	case "test-reports":
		return parseDeliverabilityReportsPath(method, segments)
	case "blacklist-report":
		if method == http.MethodGet {
			return opGetBlacklistReports, ""
		}
	case "statistics-report":
		if method == http.MethodGet && len(segments) == pathDepth3 {
			return opGetDomainStatisticsReport, segments[2]
		}
	case "campaigns":
		if method == http.MethodGet && len(segments) == pathDepth3 {
			return opGetDomainDeliverabilityCampaign, segments[2]
		}
	case "domains":
		if method == http.MethodGet && len(segments) == pathDepth4 && segments[3] == "campaigns" {
			return opListDomainDeliverabilityCampaigns, segments[2]
		}
	}

	return unknownAction, ""
}

// parseDeliverabilityReportsPath routes deliverability test report paths.
func parseDeliverabilityReportsPath(method string, segments []string) (string, string) {
	if method != http.MethodGet {
		return unknownAction, ""
	}

	switch len(segments) {
	case pathDepth2:
		return opListDeliverabilityTestReports, ""
	case pathDepth3:
		return opGetDeliverabilityTestReport, segments[2]
	}

	return unknownAction, ""
}

func parseTemplateExtPath(method string, segments []string) (string, string) {
	switch {
	case len(segments) == 1 && method == http.MethodGet:
		return opListEmailTemplates, ""
	case len(segments) == 2 && method == http.MethodGet:
		return opGetEmailTemplate, segments[1]
	case len(segments) == 2 && method == http.MethodDelete:
		return opDeleteEmailTemplate, segments[1]
	case len(segments) == 2 && method == http.MethodPut:
		return opUpdateEmailTemplate, segments[1]
	case len(segments) == pathDepth3 && segments[2] == "render" && method == http.MethodPost:
		return opTestRenderEmailTemplate, segments[1]
	}

	return unknownAction, ""
}

// parseExportJobExtPath routes /v2/email/export-jobs paths. Note ListExportJobs
// is NOT under this top-level segment in the real SDK -- it's the distinct
// POST /v2/email/list-export-jobs (see parseMiscPathsSpecial's "list-export-jobs"
// case).
func parseExportJobExtPath(method string, segments []string) (string, string) {
	switch {
	case len(segments) == 1 && method == http.MethodPost:
		return opCreateExportJob, ""
	case len(segments) == 2 && method == http.MethodGet:
		return opGetExportJob, segments[1]
	case len(segments) == exportJobPathSegments && segments[2] == "cancel" && method == http.MethodPut:
		return opCancelExportJob, segments[1]
	}

	return unknownAction, ""
}

// parseImportJobPath routes /v2/email/import-jobs paths, including the
// POST .../import-jobs/list variant of ListImportJobs (filter/pagination in body).
func parseImportJobPath(method string, segments []string) (string, string) {
	switch {
	case len(segments) == 1 && method == http.MethodPost:
		return opCreateImportJob, ""
	case len(segments) == 2 && method == http.MethodGet:
		return opGetImportJob, segments[1]
	case len(segments) == pathDepth2 && method == http.MethodPost && segments[1] == verbList:
		return opListImportJobs, ""
	}

	return unknownAction, ""
}

func parseMultiRegionEndpointPath(method string, segments []string) (string, string) {
	switch {
	case len(segments) == 1 && method == http.MethodGet:
		return opListMultiRegionEndpoints, ""
	case len(segments) == 1 && method == http.MethodPost:
		return opCreateMultiRegionEndpoint, ""
	case len(segments) == 2 && method == http.MethodGet:
		return opGetMultiRegionEndpoint, segments[1]
	case len(segments) == 2 && method == http.MethodDelete:
		return opDeleteMultiRegionEndpoint, segments[1]
	}

	return unknownAction, ""
}

// parseTenantPath routes the real SDK's RPC-style tenant paths -- every
// operation is POST to a fixed verb path with TenantName/ResourceArn in the
// JSON body, not a REST path parameter (confirmed against
// aws-sdk-go-v2/service/sesv2 v1.60.1 serializers.go; SplitURI has no
// {Placeholder} segments anywhere in this family):
//
//	POST /v2/email/tenants                  -> CreateTenant
//	POST /v2/email/tenants/get              -> GetTenant
//	POST /v2/email/tenants/delete           -> DeleteTenant
//	POST /v2/email/tenants/list             -> ListTenants
//	POST /v2/email/tenants/resources        -> CreateTenantResourceAssociation
//	POST /v2/email/tenants/resources/delete -> DeleteTenantResourceAssociation
//	POST /v2/email/tenants/resources/list   -> ListTenantResources
//
// (ListResourceTenants is POST /v2/email/resources/tenants/list -- a
// different top-level segment entirely -- see parseResourceTenantsPath.)
func parseTenantPath(method string, segments []string) (string, string) {
	if method != http.MethodPost {
		return unknownAction, ""
	}

	switch len(segments) {
	case 1:
		return opCreateTenant, ""
	case pathDepth2:
		switch segments[1] {
		case verbGet:
			return opGetTenant, ""
		case verbDelete:
			return opDeleteTenant, ""
		case verbList:
			return opListTenants, ""
		case segResources:
			return opCreateTenantResourceAssociation, ""
		}
	case pathDepth3:
		if segments[1] == segResources {
			switch segments[2] {
			case verbDelete:
				return opDeleteTenantResourceAssociation, ""
			case verbList:
				return opListTenantResources, ""
			}
		}
	}

	return unknownAction, ""
}

// parseTenantSuppressionPath routes POST /v2/email/tenant/suppression ->
// PutTenantSuppressionAttributes. Note the top-level segment is the
// *singular* "tenant" -- a distinct path from the rest of the tenant family,
// which all live under the plural "tenants" (see parseTenantPath). Confirmed
// against aws-sdk-go-v2/service/sesv2 v1.66.4 serializers.go
// (awsRestjson1_serializeOpPutTenantSuppressionAttributes's
// httpbinding.SplitURI("/v2/email/tenant/suppression")) -- this service has a
// history of invented paths, so don't assume it's under "tenants" without
// checking the serializer again.
func parseTenantSuppressionPath(method string, segments []string) (string, string) {
	if method == http.MethodPost && len(segments) == pathDepth2 && segments[1] == segSuppression {
		return opPutTenantSuppressionAttributes, ""
	}

	return unknownAction, ""
}

// parseIdentityExtPath handles additional identity paths.
func parseIdentityExtPath(method string, segments []string) (string, string) {
	if len(segments) < pathDepth3 {
		return unknownAction, ""
	}

	identity := segments[1]
	sub := segments[2]

	if op, id := parseIdentityPoliciesPath(method, segments, identity, sub); op != unknownAction {
		return op, id
	}

	return parseIdentityAttributesPath(method, segments, identity, sub)
}

// parseIdentityPoliciesPath routes identity policy sub-paths.
func parseIdentityPoliciesPath(method string, segments []string, identity, sub string) (string, string) {
	if sub != segPolicies {
		return unknownAction, ""
	}

	switch len(segments) {
	case pathDepth3:
		if method == http.MethodGet {
			return opGetEmailIdentityPolicies, identity
		}
	case pathDepth4:
		switch method {
		case http.MethodDelete:
			return opDeleteEmailIdentityPolicy, identity
		case http.MethodPut:
			return opUpdateEmailIdentityPolicy, identity
		}
	}

	return unknownAction, ""
}

// parseIdentityAttributesPath routes identity attribute sub-paths.
func parseIdentityAttributesPath(method string, segments []string, identity, sub string) (string, string) {
	if method != http.MethodPut {
		return unknownAction, ""
	}

	switch sub {
	case "configuration-set":
		if len(segments) == pathDepth3 {
			return opPutEmailIdentityConfigurationSetAttributes, identity
		}
	case "dkim":
		switch len(segments) {
		case pathDepth3:
			return opPutEmailIdentityDkimAttributes, identity
		case pathDepth4:
			if segments[3] == "signing" {
				return opPutEmailIdentityDkimSigningAttributes, identity
			}
		}
	case "feedback":
		if len(segments) == pathDepth3 {
			return opPutEmailIdentityFeedbackAttributes, identity
		}
	case "mail-from":
		if len(segments) == pathDepth3 {
			return opPutEmailIdentityMailFromAttributes, identity
		}
	}

	return unknownAction, ""
}

// parseConfigSetExtPath handles additional configuration set paths.
func parseConfigSetExtPath(method string, segments []string) (string, string) {
	if len(segments) < pathDepth3 {
		return unknownAction, ""
	}

	name := segments[1]
	sub := segments[2]

	if op, id := parseConfigSetEventDestPath(method, segments, name, sub); op != unknownAction {
		return op, id
	}

	if len(segments) == pathDepth3 {
		return parseConfigSetAttrPath(method, name, sub)
	}

	return unknownAction, ""
}

// parseConfigSetEventDestPath routes configuration set event destination paths.
func parseConfigSetEventDestPath(method string, segments []string, name, sub string) (string, string) {
	if sub != segEventDestinations {
		return unknownAction, ""
	}

	switch len(segments) {
	case pathDepth3:
		if method == http.MethodGet {
			return opGetConfigurationSetEventDestinations, name
		}
	case pathDepth4:
		switch method {
		case http.MethodDelete:
			return opDeleteConfigurationSetEventDestination, name
		case http.MethodPut:
			return opUpdateConfigurationSetEventDestination, name
		}
	}

	return unknownAction, ""
}

// parseConfigSetAttrPath routes configuration set attribute put paths.
func parseConfigSetAttrPath(method, name, sub string) (string, string) {
	if method != http.MethodPut {
		return unknownAction, ""
	}

	switch sub {
	case "archiving-options":
		return opPutConfigurationSetArchivingOptions, name
	case "delivery-options":
		return opPutConfigurationSetDeliveryOptions, name
	case "reputation-options":
		return opPutConfigurationSetReputationOptions, name
	case "sending":
		return opPutConfigurationSetSendingOptions, name
	case "suppression-options":
		return opPutConfigurationSetSuppressionOptions, name
	case "tracking-options":
		return opPutConfigurationSetTrackingOptions, name
	case "vdm-options":
		return opPutConfigurationSetVdmOptions, name
	}

	return unknownAction, ""
}
