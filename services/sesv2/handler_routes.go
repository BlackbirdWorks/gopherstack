package sesv2

import "net/http"

// path segment constants used across multiple parse functions.
const (
	segContacts          = "contacts"
	segPolicies          = "policies"
	segEventDestinations = "event-destinations"
	segResources         = "resources"
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
	case "suppression":
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
func parseExtendedInsightsPath(method string, segments []string) (string, string) {
	switch segments[0] {
	case "email-insights":
		if method == http.MethodGet && len(segments) == 2 {
			return opGetEmailAddressInsights, segments[1]
		}
	case "messages":
		if method == http.MethodGet && len(segments) == 2 {
			return opGetMessageInsights, segments[1]
		}
	case "recommendations":
		if method == http.MethodGet && len(segments) == 1 {
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
	case "resource-tenants":
		return parseResourceTenantsPath(method, segments)
	case "reputation-entities":
		return parseReputationEntityPath(method, segments)
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
	case len(segments) == pathDepth2 && method == http.MethodGet:
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

// parseResourceTenantsPath routes resource-tenants paths.
func parseResourceTenantsPath(method string, segments []string) (string, string) {
	if method == http.MethodGet && len(segments) == 1 {
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
	case "sending":
		if method == http.MethodPut {
			return opPutAccountSendingAttributes, ""
		}
	case "suppression":
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
	if segments[3] == "list" && method == http.MethodPost {
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

// parseDeliverabilitySubPath routes deliverability report and campaign sub-paths.
func parseDeliverabilitySubPath(method string, segments []string) (string, string) {
	if len(segments) < pathDepth2 {
		return unknownAction, ""
	}

	switch segments[1] {
	case "reports":
		return parseDeliverabilityReportsPath(method, segments)
	case "blacklist-report":
		if method == http.MethodGet {
			return opGetBlacklistReports, ""
		}
	case "statistics":
		if method == http.MethodGet && len(segments) == pathDepth3 {
			return opGetDomainStatisticsReport, segments[2]
		}
	case "campaigns":
		return parseDeliverabilityDomainCampaignsPath(method, segments)
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

// parseDeliverabilityDomainCampaignsPath routes domain deliverability campaign paths.
func parseDeliverabilityDomainCampaignsPath(method string, segments []string) (string, string) {
	if method != http.MethodGet {
		return unknownAction, ""
	}

	switch len(segments) {
	case pathDepth3:
		return opListDomainDeliverabilityCampaigns, segments[2]
	case pathDepth4:
		return opGetDomainDeliverabilityCampaign, segments[2]
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

func parseExportJobExtPath(method string, segments []string) (string, string) {
	switch {
	case len(segments) == 1 && method == http.MethodGet:
		return opListExportJobs, ""
	case len(segments) == 1 && method == http.MethodPost:
		return opCreateExportJob, ""
	case len(segments) == 2 && method == http.MethodGet:
		return opGetExportJob, segments[1]
	case len(segments) == exportJobPathSegments && segments[2] == "cancel" && method == http.MethodPut:
		return opCancelExportJob, segments[1]
	}

	return unknownAction, ""
}

func parseImportJobPath(method string, segments []string) (string, string) {
	switch {
	case len(segments) == 1 && method == http.MethodGet:
		return opListImportJobs, ""
	case len(segments) == 1 && method == http.MethodPost:
		return opCreateImportJob, ""
	case len(segments) == 2 && method == http.MethodGet:
		return opGetImportJob, segments[1]
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

// parseTenantPath routes tenant paths.
func parseTenantPath(method string, segments []string) (string, string) {
	switch len(segments) {
	case 1:
		switch method {
		case http.MethodGet:
			return opListTenants, ""
		case http.MethodPost:
			return opCreateTenant, ""
		}
	case pathDepth2:
		switch method {
		case http.MethodGet:
			return opGetTenant, segments[1]
		case http.MethodDelete:
			return opDeleteTenant, segments[1]
		}
	case pathDepth3:
		if segments[2] == segResources {
			switch method {
			case http.MethodGet:
				return opListTenantResources, segments[1]
			case http.MethodPost:
				return opCreateTenantResourceAssociation, segments[1]
			}
		}
	case pathDepth4:
		if segments[2] == segResources && method == http.MethodDelete {
			return opDeleteTenantResourceAssociation, segments[1]
		}
	}

	return unknownAction, ""
}

func parseReputationEntityPath(method string, segments []string) (string, string) {
	switch {
	case len(segments) == 1 && method == http.MethodGet:
		return opListReputationEntities, ""
	case len(segments) == 2 && method == http.MethodGet:
		return opGetReputationEntity, segments[1]
	case len(segments) == pathDepth3 && segments[2] == "customer-managed-status" && method == http.MethodPut:
		return opUpdateReputationEntityCustomerManagedStatus, segments[1]
	case len(segments) == pathDepth3 && segments[2] == "policy" && method == http.MethodPut:
		return opUpdateReputationEntityPolicy, segments[1]
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
