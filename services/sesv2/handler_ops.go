package sesv2

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"
)

// path segment constants used across multiple parse functions.
const (
	segContacts           = "contacts"
	segPolicies           = "policies"
	segEventDestinations  = "event-destinations"
	segResources          = "resources"
	keyNextToken          = "NextToken"
	keyStatus             = "Status"
	keyStatusSuccess      = "SUCCESS"
	warmupDone            = "DONE"
	warmupPercentComplete = 100

	// path depth sentinels for segment-count comparisons.
	pathDepth2 = 2
	pathDepth3 = 3
)

// dispatchExtendedOps handles all 89 newly-added SES v2 operations.
//
//nolint:cyclop,gocyclo,funlen // large switch dispatch for all 89 new ops
func (h *Handler) dispatchExtendedOps(
	c *echo.Context,
	op, resource string,
) (any, error) {
	switch op {
	// account
	case opGetAccount:
		return h.handleGetAccount()
	case opGetBlacklistReports:
		return h.handleGetBlacklistReports()
	case opPutAccountDedicatedIPWarmupAttributes:
		return h.handlePutAccountDedicatedIPWarmupAttributes()
	case opPutAccountDetails:
		return h.handlePutAccountDetails(c)
	case opPutAccountSendingAttributes:
		return h.handlePutAccountSendingAttributes(c)
	case opPutAccountSuppressionAttributes:
		return h.handlePutAccountSuppressionAttributes()
	case opPutAccountVdmAttributes:
		return h.handlePutAccountVdmAttributes()
	// suppressed destinations
	case opPutSuppressedDestination:
		return h.handlePutSuppressedDestination(c)
	case opGetSuppressedDestination:
		return h.handleGetSuppressedDestination(resource)
	case opDeleteSuppressedDestination:
		return h.handleDeleteSuppressedDestination(resource)
	case opListSuppressedDestinations:
		return h.handleListSuppressedDestinations(c)
	// contact lists / contacts
	case opGetContactList:
		return h.handleGetContactList(resource)
	case opDeleteContactList:
		return h.handleDeleteContactList(resource)
	case opUpdateContactList:
		return h.handleUpdateContactList(c, resource)
	case opListContactLists:
		return h.handleListContactLists(c)
	case opGetContact:
		return h.handleGetContact(c, resource)
	case opDeleteContact:
		return h.handleDeleteContact(c, resource)
	case opUpdateContact:
		return h.handleUpdateContact(c, resource)
	case opListContacts:
		return h.handleListContacts(c, resource)
	// custom verification templates
	case opGetCustomVerificationEmailTemplate:
		return h.handleGetCustomVerificationEmailTemplate(resource)
	case opDeleteCustomVerificationEmailTemplate:
		return h.handleDeleteCustomVerificationEmailTemplate(resource)
	case opUpdateCustomVerificationEmailTemplate:
		return h.handleUpdateCustomVerificationEmailTemplate(c, resource)
	case opListCustomVerificationEmailTemplates:
		return h.handleListCustomVerificationEmailTemplates(c)
	case opSendCustomVerificationEmail:
		return h.handleSendCustomVerificationEmail(c)
	// dedicated IP pools
	case opGetDedicatedIPPool:
		return h.handleGetDedicatedIPPool(resource)
	case opDeleteDedicatedIPPool:
		return h.handleDeleteDedicatedIPPool(resource)
	case opListDedicatedIPPools:
		return h.handleListDedicatedIPPools(c)
	case opGetDedicatedIP:
		return h.handleGetDedicatedIP(resource)
	case opGetDedicatedIps:
		return h.handleGetDedicatedIps()
	case opPutDedicatedIPInPool:
		return h.handlePutDedicatedIPInPool(c, resource)
	case opPutDedicatedIPPoolScalingAttributes:
		return h.handlePutDedicatedIPPoolScalingAttributes(c, resource)
	case opPutDedicatedIPWarmupAttributes:
		return h.handlePutDedicatedIPWarmupAttributes(resource)
	// deliverability
	case opGetDeliverabilityDashboardOptions:
		return h.handleGetDeliverabilityDashboardOptions()
	case opPutDeliverabilityDashboardOption:
		return h.handlePutDeliverabilityDashboardOption()
	case opGetDeliverabilityTestReport:
		return h.handleGetDeliverabilityTestReport(resource)
	case opListDeliverabilityTestReports:
		return h.handleListDeliverabilityTestReports(c)
	case opGetDomainDeliverabilityCampaign:
		return h.handleGetDomainDeliverabilityCampaign(c, resource)
	case opGetDomainStatisticsReport:
		return h.handleGetDomainStatisticsReport(c, resource)
	case opListDomainDeliverabilityCampaigns:
		return h.handleListDomainDeliverabilityCampaigns(c, resource)
	case opGetEmailAddressInsights:
		return h.handleGetEmailAddressInsights(resource)
	case opGetMessageInsights:
		return h.handleGetMessageInsights(resource)
	case opListRecommendations:
		return h.handleListRecommendations(c)
	// email templates
	case opGetEmailTemplate:
		return h.handleGetEmailTemplate(resource)
	case opDeleteEmailTemplate:
		return h.handleDeleteEmailTemplate(resource)
	case opUpdateEmailTemplate:
		return h.handleUpdateEmailTemplate(c, resource)
	case opListEmailTemplates:
		return h.handleListEmailTemplates(c)
	case opTestRenderEmailTemplate:
		return h.handleTestRenderEmailTemplate(c, resource)
	// export / import jobs
	case opCreateExportJob:
		return h.handleCreateExportJob(c)
	case opGetExportJob:
		return h.handleGetExportJob(resource)
	case opListExportJobs:
		return h.handleListExportJobs(c)
	case opCreateImportJob:
		return h.handleCreateImportJob(c)
	case opGetImportJob:
		return h.handleGetImportJob(resource)
	case opListImportJobs:
		return h.handleListImportJobs(c)
	// email identity policies
	case opGetEmailIdentityPolicies:
		return h.handleGetEmailIdentityPolicies(resource)
	case opDeleteEmailIdentityPolicy:
		return h.handleDeleteEmailIdentityPolicy(c, resource)
	case opUpdateEmailIdentityPolicy:
		return h.handleUpdateEmailIdentityPolicy(c, resource)
	// email identity attributes
	case opPutEmailIdentityConfigurationSetAttributes:
		return h.handlePutEmailIdentityConfigurationSetAttributes()
	case opPutEmailIdentityDkimAttributes:
		return h.handlePutEmailIdentityDkimAttributes()
	case opPutEmailIdentityDkimSigningAttributes:
		return h.handlePutEmailIdentityDkimSigningAttributes()
	case opPutEmailIdentityFeedbackAttributes:
		return h.handlePutEmailIdentityFeedbackAttributes()
	case opPutEmailIdentityMailFromAttributes:
		return h.handlePutEmailIdentityMailFromAttributes()
	// configuration set event destinations
	case opGetConfigurationSetEventDestinations:
		return h.handleGetConfigurationSetEventDestinations(resource)
	case opDeleteConfigurationSetEventDestination:
		return h.handleDeleteConfigurationSetEventDestination(c, resource)
	case opUpdateConfigurationSetEventDestination:
		return h.handleUpdateConfigurationSetEventDestination(c, resource)
	// configuration set attributes
	case opPutConfigurationSetArchivingOptions:
		return h.handlePutConfigurationSetArchivingOptions()
	case opPutConfigurationSetDeliveryOptions:
		return h.handlePutConfigurationSetDeliveryOptions()
	case opPutConfigurationSetReputationOptions:
		return h.handlePutConfigurationSetReputationOptions()
	case opPutConfigurationSetSendingOptions:
		return h.handlePutConfigurationSetSendingOptions()
	case opPutConfigurationSetSuppressionOptions:
		return h.handlePutConfigurationSetSuppressionOptions()
	case opPutConfigurationSetTrackingOptions:
		return h.handlePutConfigurationSetTrackingOptions()
	case opPutConfigurationSetVdmOptions:
		return h.handlePutConfigurationSetVdmOptions()
	// bulk email
	case opSendBulkEmail:
		return h.handleSendBulkEmail(c)
	// multi-region endpoints
	case opCreateMultiRegionEndpoint:
		return h.handleCreateMultiRegionEndpoint(c)
	case opGetMultiRegionEndpoint:
		return h.handleGetMultiRegionEndpoint(resource)
	case opDeleteMultiRegionEndpoint:
		return h.handleDeleteMultiRegionEndpoint(resource)
	case opListMultiRegionEndpoints:
		return h.handleListMultiRegionEndpoints(c)
	// tenants
	case opCreateTenant:
		return h.handleCreateTenant(c)
	case opGetTenant:
		return h.handleGetTenant(resource)
	case opDeleteTenant:
		return h.handleDeleteTenant(resource)
	case opListTenants:
		return h.handleListTenants(c)
	case opCreateTenantResourceAssociation:
		return h.handleCreateTenantResourceAssociation(c, resource)
	case opDeleteTenantResourceAssociation:
		return h.handleDeleteTenantResourceAssociation(c, resource)
	case opListResourceTenants:
		return h.handleListResourceTenants(c)
	case opListTenantResources:
		return h.handleListTenantResources(c, resource)
	// reputation entities
	case opGetReputationEntity:
		return h.handleGetReputationEntity(resource)
	case opListReputationEntities:
		return h.handleListReputationEntities(c)
	case opUpdateReputationEntityCustomerManagedStatus:
		return h.handleUpdateReputationEntityCustomerManagedStatus(resource)
	case opUpdateReputationEntityPolicy:
		return h.handleUpdateReputationEntityPolicy(c, resource)
	}

	return nil, errOpNotHandled
}

// ---- path parsing for new ops ----

// parseExtendedPaths parses paths for newly-added operations.
// It is called from parseMiscPaths when the first segment is not handled by the
// original set of patterns.
//
//nolint:cyclop,gocyclo // top-level path router for all new paths
func parseExtendedPaths(
	method string,
	segments []string,
) (string, string) {
	switch segments[0] {
	case "account":
		return parseAccountPath(method, segments)
	case "suppressed-destination":
		return parseSuppressedDestinationPath(method, segments)
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
	case "templates":
		return parseTemplateExtPath(method, segments)
	case "export-jobs":
		return parseExportJobExtPath(method, segments)
	case "import-jobs":
		return parseImportJobPath(method, segments)
	case "vdm-attributes":
		if method == http.MethodGet {
			return opGetAccount, ""
		}
		if method == http.MethodPut {
			return opPutAccountVdmAttributes, ""
		}
	case "outbound-bulk-emails":
		if method == http.MethodPost && len(segments) == 1 {
			return opSendBulkEmail, ""
		}
	case "multi-region-endpoints":
		return parseMultiRegionEndpointPath(method, segments)
	case "tenants":
		return parseTenantPath(method, segments)
	case "resource-tenants":
		if method == http.MethodGet && len(segments) == 1 {
			return opListResourceTenants, ""
		}
	case "reputation-entities":
		return parseReputationEntityPath(method, segments)
	}

	return unknownAction, ""
}

//nolint:gocognit // path branching inherently complex
func parseAccountPath(
	method string,
	segments []string,
) (string, string) {
	if len(segments) == 1 {
		if method == http.MethodGet {
			return opGetAccount, ""
		}
	}

	if len(segments) == pathDepth2 { //nolint:nestif // URL path segment dispatching requires nested if
		switch segments[1] {
		case "dedicated-ip-warmup-attributes":
			if method == http.MethodPut {
				return opPutAccountDedicatedIPWarmupAttributes, ""
			}
		case "details":
			if method == http.MethodPost {
				return opPutAccountDetails, ""
			}
		case "sending-attributes":
			if method == http.MethodPut {
				return opPutAccountSendingAttributes, ""
			}
		case "suppression-attributes":
			if method == http.MethodPut {
				return opPutAccountSuppressionAttributes, ""
			}
		case "vdm-attributes":
			if method == http.MethodPut {
				return opPutAccountVdmAttributes, ""
			}
		}
	}

	return unknownAction, ""
}

func parseSuppressedDestinationPath(method string, segments []string) (string, string) {
	switch {
	case len(segments) == 1 && method == http.MethodGet:
		return opListSuppressedDestinations, ""
	case len(segments) == 1 && method == http.MethodPut:
		return opPutSuppressedDestination, ""
	case len(segments) == 2 && method == http.MethodGet:
		return opGetSuppressedDestination, segments[1]
	case len(segments) == 2 && method == http.MethodDelete:
		return opDeleteSuppressedDestination, segments[1]
	}

	return unknownAction, ""
}

//nolint:cyclop // path branching inherently complex
func parseContactListExtPath(
	method string,
	segments []string,
) (string, string) {
	switch {
	case len(segments) == 1 && method == http.MethodGet:
		return opListContactLists, ""
	case len(segments) == 2 && method == http.MethodGet:
		return opGetContactList, segments[1]
	case len(segments) == 2 && method == http.MethodDelete:
		return opDeleteContactList, segments[1]
	case len(segments) == 2 && method == http.MethodPut:
		return opUpdateContactList, segments[1]
	case len(segments) == 3 && segments[2] == segContacts && method == http.MethodGet:
		return opListContacts, segments[1]
	case len(segments) == 4 && segments[2] == segContacts && method == http.MethodGet:
		return opGetContact, segments[1]
	case len(segments) == 4 && segments[2] == segContacts && method == http.MethodDelete:
		return opDeleteContact, segments[1]
	case len(segments) == 4 && segments[2] == segContacts && method == http.MethodPut:
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
	case len(segments) == 3 && segments[2] == "scaling-attributes" && method == http.MethodPut:
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
	case len(segments) == 3 && segments[2] == "pool" && method == http.MethodPut:
		return opPutDedicatedIPInPool, segments[1]
	case len(segments) == 3 && segments[2] == "warmup" && method == http.MethodPut:
		return opPutDedicatedIPWarmupAttributes, segments[1]
	}

	return unknownAction, ""
}

//nolint:cyclop,gocognit // path branching inherently complex
func parseDeliverabilityExtPath(
	method string,
	segments []string,
) (string, string) {
	if len(segments) == 1 {
		switch method {
		case http.MethodGet:
			return opGetDeliverabilityDashboardOptions, ""
		case http.MethodPut:
			return opPutDeliverabilityDashboardOption, ""
		}
	}

	if len(segments) == metricsPathSegments && segments[1] == "test" && method == http.MethodPost {
		return opCreateDeliverabilityTestReport, ""
	}

	if len(segments) >= pathDepth2 { //nolint:nestif // URL path segment dispatching requires nested if
		switch segments[1] {
		case "reports":
			if len(segments) == 2 && method == http.MethodGet {
				return opListDeliverabilityTestReports, ""
			}

			if len(segments) == 3 && method == http.MethodGet {
				return opGetDeliverabilityTestReport, segments[2]
			}

		case "blacklist-report":
			if method == http.MethodGet {
				return opGetBlacklistReports, ""
			}

		case "statistics":
			if method == http.MethodGet && len(segments) == 3 {
				return opGetDomainStatisticsReport, segments[2]
			}

		case "campaigns":
			if method == http.MethodGet && len(segments) == 3 {
				return opListDomainDeliverabilityCampaigns, segments[2]
			}

			if method == http.MethodGet && len(segments) == 4 {
				return opGetDomainDeliverabilityCampaign, segments[2]
			}
		}
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
	case len(segments) == 3 && segments[2] == "render" && method == http.MethodPost:
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

//nolint:cyclop // path branching inherently complex
func parseTenantPath(
	method string,
	segments []string,
) (string, string) {
	switch {
	case len(segments) == 1 && method == http.MethodGet:
		return opListTenants, ""
	case len(segments) == 1 && method == http.MethodPost:
		return opCreateTenant, ""
	case len(segments) == 2 && method == http.MethodGet:
		return opGetTenant, segments[1]
	case len(segments) == 2 && method == http.MethodDelete:
		return opDeleteTenant, segments[1]
	case len(segments) == 3 && segments[2] == segResources && method == http.MethodGet:
		return opListTenantResources, segments[1]
	case len(segments) == 3 && segments[2] == segResources && method == http.MethodPost:
		return opCreateTenantResourceAssociation, segments[1]
	case len(segments) == 4 && segments[2] == segResources && method == http.MethodDelete:
		return opDeleteTenantResourceAssociation, segments[1]
	}

	return unknownAction, ""
}

func parseReputationEntityPath(method string, segments []string) (string, string) {
	switch {
	case len(segments) == 1 && method == http.MethodGet:
		return opListReputationEntities, ""
	case len(segments) == 2 && method == http.MethodGet:
		return opGetReputationEntity, segments[1]
	case len(segments) == 3 && segments[2] == "customer-managed-status" && method == http.MethodPut:
		return opUpdateReputationEntityCustomerManagedStatus, segments[1]
	case len(segments) == 3 && segments[2] == "policy" && method == http.MethodPut:
		return opUpdateReputationEntityPolicy, segments[1]
	}

	return unknownAction, ""
}

// parseIdentityExtPath handles additional identity paths.
//
//nolint:cyclop // path branching inherently complex
func parseIdentityExtPath(
	method string,
	segments []string,
) (string, string) {
	// /v2/email/identities/{identity}/policies — GET returns all policies
	if len(segments) == 3 && segments[2] == segPolicies {
		if method == http.MethodGet {
			return opGetEmailIdentityPolicies, segments[1]
		}
	}

	// /v2/email/identities/{identity}/policies/{policyName}
	if len(segments) == 4 && segments[2] == segPolicies {
		switch method {
		case http.MethodDelete:
			return opDeleteEmailIdentityPolicy, segments[1]
		case http.MethodPut:
			return opUpdateEmailIdentityPolicy, segments[1]
		}
	}

	// /v2/email/identities/{identity}/configuration-set
	if len(segments) == 3 && segments[2] == "configuration-set" && method == http.MethodPut {
		return opPutEmailIdentityConfigurationSetAttributes, segments[1]
	}

	// /v2/email/identities/{identity}/dkim
	if len(segments) == 3 && segments[2] == "dkim" {
		if method == http.MethodPut {
			return opPutEmailIdentityDkimAttributes, segments[1]
		}
	}

	// /v2/email/identities/{identity}/dkim/signing
	if len(segments) == 4 && segments[2] == "dkim" && segments[3] == "signing" &&
		method == http.MethodPut {
		return opPutEmailIdentityDkimSigningAttributes, segments[1]
	}

	// /v2/email/identities/{identity}/feedback
	if len(segments) == 3 && segments[2] == "feedback" && method == http.MethodPut {
		return opPutEmailIdentityFeedbackAttributes, segments[1]
	}

	// /v2/email/identities/{identity}/mail-from
	if len(segments) == 3 && segments[2] == "mail-from" && method == http.MethodPut {
		return opPutEmailIdentityMailFromAttributes, segments[1]
	}

	return unknownAction, ""
}

// parseConfigSetExtPath handles additional configuration set paths.
//
//nolint:cyclop,gocognit // path branching inherently complex
func parseConfigSetExtPath(
	method string,
	segments []string,
) (string, string) {
	// /v2/email/configuration-sets/{name}/event-destinations
	if len(segments) == 3 && segments[2] == segEventDestinations {
		if method == http.MethodGet {
			return opGetConfigurationSetEventDestinations, segments[1]
		}
	}

	// /v2/email/configuration-sets/{name}/event-destinations/{destName}
	if len(segments) == 4 && segments[2] == segEventDestinations {
		switch method {
		case http.MethodDelete:
			return opDeleteConfigurationSetEventDestination, segments[1]
		case http.MethodPut:
			return opUpdateConfigurationSetEventDestination, segments[1]
		}
	}

	// /v2/email/configuration-sets/{name}/{attribute}
	if len(segments) == pathDepth3 { //nolint:nestif // URL path segment dispatching requires nested if
		switch segments[2] {
		case "archiving-options":
			if method == http.MethodPut {
				return opPutConfigurationSetArchivingOptions, segments[1]
			}
		case "delivery-options":
			if method == http.MethodPut {
				return opPutConfigurationSetDeliveryOptions, segments[1]
			}
		case "reputation-options":
			if method == http.MethodPut {
				return opPutConfigurationSetReputationOptions, segments[1]
			}
		case "sending-options":
			if method == http.MethodPut {
				return opPutConfigurationSetSendingOptions, segments[1]
			}
		case "suppression-options":
			if method == http.MethodPut {
				return opPutConfigurationSetSuppressionOptions, segments[1]
			}
		case "tracking-options":
			if method == http.MethodPut {
				return opPutConfigurationSetTrackingOptions, segments[1]
			}
		case "vdm-options":
			if method == http.MethodPut {
				return opPutConfigurationSetVdmOptions, segments[1]
			}
		}
	}

	return unknownAction, ""
}

// ---- handler functions for new ops ----

// account handlers

func (h *Handler) handleGetAccount() (any, error) {
	acct, err := h.Backend.GetAccount()
	if err != nil {
		return nil, err
	}

	return acct, nil
}

func (h *Handler) handleGetBlacklistReports() (any, error) {
	reports, err := h.Backend.GetBlacklistReports()
	if err != nil {
		return nil, err
	}

	return map[string]any{"BlacklistReport": reports}, nil
}

func (h *Handler) handlePutAccountDedicatedIPWarmupAttributes() (any, error) {
	if err := h.Backend.PutAccountDedicatedIPWarmupAttributes(); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type putAccountDetailsInput struct {
	MailType        string `json:"MailType"`
	WebsiteURL      string `json:"WebsiteURL"`
	ContactLanguage string `json:"ContactLanguage"`
	UseCaseName     string `json:"UseCaseName"`
}

func (h *Handler) handlePutAccountDetails(c *echo.Context) (any, error) {
	var in putAccountDetailsInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.PutAccountDetails(&AccountDetails{
		MailType:        in.MailType,
		WebsiteURL:      in.WebsiteURL,
		ContactLanguage: in.ContactLanguage,
		UseCaseName:     in.UseCaseName,
	}); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type putAccountSendingInput struct {
	SendingEnabled bool `json:"SendingEnabled"`
}

func (h *Handler) handlePutAccountSendingAttributes(c *echo.Context) (any, error) {
	var in putAccountSendingInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.PutAccountSendingAttributes(in.SendingEnabled); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handlePutAccountSuppressionAttributes() (any, error) {
	if err := h.Backend.PutAccountSuppressionAttributes(); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handlePutAccountVdmAttributes() (any, error) {
	if err := h.Backend.PutAccountVdmAttributes(); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// suppressed destination handlers

type putSuppressedDestinationInput struct {
	EmailAddress string `json:"EmailAddress"`
	Reason       string `json:"Reason"`
}

func (h *Handler) handlePutSuppressedDestination(c *echo.Context) (any, error) {
	var in putSuppressedDestinationInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.PutSuppressedDestination(in.EmailAddress, in.Reason); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleGetSuppressedDestination(email string) (any, error) {
	dest, err := h.Backend.GetSuppressedDestination(email)
	if err != nil {
		return nil, err
	}

	return dest, nil
}

func (h *Handler) handleDeleteSuppressedDestination(email string) (any, error) {
	if err := h.Backend.DeleteSuppressedDestination(email); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleListSuppressedDestinations(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListSuppressedDestinations(nextToken, 0)

	return map[string]any{
		"SuppressedDestinationSummaries": pg.Data,
		keyNextToken:                     pg.Next,
	}, nil
}

// contact list handlers

func (h *Handler) handleGetContactList(name string) (any, error) {
	cl, err := h.Backend.GetContactList(name)
	if err != nil {
		return nil, err
	}

	return cl, nil
}

func (h *Handler) handleDeleteContactList(name string) (any, error) {
	if err := h.Backend.DeleteContactList(name); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type updateContactListInput struct {
	Description string `json:"Description"`
}

func (h *Handler) handleUpdateContactList(c *echo.Context, name string) (any, error) {
	var in updateContactListInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.UpdateContactList(name, in.Description); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleListContactLists(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListContactLists(nextToken, 0)

	return map[string]any{
		"ContactLists": pg.Data,
		keyNextToken:   pg.Next,
	}, nil
}

// contact handlers

func (h *Handler) handleGetContact(c *echo.Context, contactListName string) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < 4 { //nolint:mnd // URL segment index is self-documenting in context
		return nil, fmt.Errorf("%w: invalid contact path", ErrInvalidInput)
	}

	emailAddress := segments[3]

	if decoded, err := url.PathUnescape(emailAddress); err == nil {
		emailAddress = decoded
	}

	c2, err := h.Backend.GetContact(contactListName, emailAddress)
	if err != nil {
		return nil, err
	}

	return c2, nil
}

func (h *Handler) handleDeleteContact(c *echo.Context, contactListName string) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < 4 { //nolint:mnd // URL segment index is self-documenting in context
		return nil, fmt.Errorf("%w: invalid contact path", ErrInvalidInput)
	}

	emailAddress := segments[3]

	if decoded, err := url.PathUnescape(emailAddress); err == nil {
		emailAddress = decoded
	}

	if err := h.Backend.DeleteContact(contactListName, emailAddress); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type updateContactInput struct {
	TopicPreferences []TopicPreference `json:"TopicPreferences"`
}

func (h *Handler) handleUpdateContact(c *echo.Context, contactListName string) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < 4 { //nolint:mnd // URL segment index is self-documenting in context
		return nil, fmt.Errorf("%w: invalid contact path", ErrInvalidInput)
	}

	emailAddress := segments[3]

	if decoded, err := url.PathUnescape(emailAddress); err == nil {
		emailAddress = decoded
	}

	var in updateContactInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.UpdateContact(contactListName, emailAddress, in.TopicPreferences); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleListContacts(c *echo.Context, contactListName string) (any, error) {
	nextToken := c.QueryParam("NextToken")

	pg, err := h.Backend.ListContacts(contactListName, nextToken, 0)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"Contacts":   pg.Data,
		keyNextToken: pg.Next,
	}, nil
}

// custom verification template handlers

func (h *Handler) handleGetCustomVerificationEmailTemplate(name string) (any, error) {
	t, err := h.Backend.GetCustomVerificationEmailTemplate(name)
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (h *Handler) handleDeleteCustomVerificationEmailTemplate(name string) (any, error) {
	if err := h.Backend.DeleteCustomVerificationEmailTemplate(name); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type updateCustomVerificationEmailTemplateInput struct {
	FromEmailAddress      string `json:"FromEmailAddress"`
	TemplateSubject       string `json:"TemplateSubject"`
	TemplateContent       string `json:"TemplateContent"`
	SuccessRedirectionURL string `json:"SuccessRedirectionURL"`
	FailureRedirectionURL string `json:"FailureRedirectionURL"`
}

func (h *Handler) handleUpdateCustomVerificationEmailTemplate(
	c *echo.Context,
	name string,
) (any, error) {
	var in updateCustomVerificationEmailTemplateInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	tmpl := &CustomVerificationEmailTemplate{
		TemplateName:          name,
		FromEmailAddress:      in.FromEmailAddress,
		TemplateSubject:       in.TemplateSubject,
		TemplateContent:       in.TemplateContent,
		SuccessRedirectionURL: in.SuccessRedirectionURL,
		FailureRedirectionURL: in.FailureRedirectionURL,
	}

	if err := h.Backend.UpdateCustomVerificationEmailTemplate(tmpl); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleListCustomVerificationEmailTemplates(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListCustomVerificationEmailTemplates(nextToken, 0)

	return map[string]any{
		"CustomVerificationEmailTemplates": pg.Data,
		keyNextToken:                       pg.Next,
	}, nil
}

type sendCustomVerificationEmailInput struct {
	EmailAddress string `json:"EmailAddress"`
	TemplateName string `json:"TemplateName"`
}

func (h *Handler) handleSendCustomVerificationEmail(c *echo.Context) (any, error) {
	var in sendCustomVerificationEmailInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	msgID, err := h.Backend.SendCustomVerificationEmail(in.EmailAddress, in.TemplateName)
	if err != nil {
		return nil, err
	}

	return map[string]any{"MessageId": msgID}, nil
}

// dedicated IP pool handlers

func (h *Handler) handleGetDedicatedIPPool(poolName string) (any, error) {
	pool, err := h.Backend.GetDedicatedIPPool(poolName)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func (h *Handler) handleDeleteDedicatedIPPool(poolName string) (any, error) {
	if err := h.Backend.DeleteDedicatedIPPool(poolName); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleListDedicatedIPPools(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListDedicatedIPPools(nextToken, 0)

	return map[string]any{
		"DedicatedIpPools": pg.Data,
		keyNextToken:       pg.Next,
	}, nil
}

func (h *Handler) handleGetDedicatedIP(ip string) (any, error) {
	info, err := h.Backend.GetDedicatedIP(ip)
	if err != nil {
		return nil, err
	}

	return map[string]any{"DedicatedIp": info}, nil
}

func (h *Handler) handleGetDedicatedIps() (any, error) {
	ips := h.Backend.GetDedicatedIps()

	return map[string]any{"DedicatedIps": ips}, nil
}

type putDedicatedIPInPoolInput struct {
	DestinationPoolName string `json:"DestinationPoolName"`
}

func (h *Handler) handlePutDedicatedIPInPool(c *echo.Context, ip string) (any, error) {
	var in putDedicatedIPInPoolInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.PutDedicatedIPInPool(ip, in.DestinationPoolName); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type putDedicatedIPPoolScalingInput struct {
	ScalingMode string `json:"ScalingMode"`
}

func (h *Handler) handlePutDedicatedIPPoolScalingAttributes(
	c *echo.Context,
	poolName string,
) (any, error) {
	var in putDedicatedIPPoolScalingInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.PutDedicatedIPPoolScalingAttributes(poolName, in.ScalingMode); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handlePutDedicatedIPWarmupAttributes(ip string) (any, error) {
	if err := h.Backend.PutDedicatedIPWarmupAttributes(ip, warmupDone); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// deliverability handlers

func (h *Handler) handleGetDeliverabilityDashboardOptions() (any, error) {
	opts, err := h.Backend.GetDeliverabilityDashboardOptions()
	if err != nil {
		return nil, err
	}

	return opts, nil
}

func (h *Handler) handlePutDeliverabilityDashboardOption() (any, error) {
	if err := h.Backend.PutDeliverabilityDashboardOption(); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleGetDeliverabilityTestReport(reportID string) (any, error) {
	r, err := h.Backend.GetDeliverabilityTestReport(reportID)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (h *Handler) handleListDeliverabilityTestReports(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListDeliverabilityTestReports(nextToken, 0)

	return map[string]any{
		"DeliverabilityTestReports": pg.Data,
		keyNextToken:                pg.Next,
	}, nil
}

func (h *Handler) handleGetDomainDeliverabilityCampaign(
	c *echo.Context,
	domain string,
) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	campaignID := ""

	if len(segments) >= 5 { //nolint:mnd // URL segment index is self-documenting in context
		campaignID = segments[4]
	}

	result, err := h.Backend.GetDomainDeliverabilityCampaign(domain, campaignID)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (h *Handler) handleGetDomainStatisticsReport(c *echo.Context, domain string) (any, error) {
	startDate := c.QueryParam("StartDate")
	endDate := c.QueryParam("EndDate")

	result, err := h.Backend.GetDomainStatisticsReport(domain, startDate, endDate)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (h *Handler) handleListDomainDeliverabilityCampaigns(
	c *echo.Context,
	domain string,
) (any, error) {
	startDate := c.QueryParam("StartDate")
	endDate := c.QueryParam("EndDate")
	nextToken := c.QueryParam("NextToken")

	items, next, err := h.Backend.ListDomainDeliverabilityCampaigns(
		startDate,
		endDate,
		domain,
		nextToken,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"DomainDeliverabilityCampaigns": items,
		keyNextToken:                    next,
	}, nil
}

func (h *Handler) handleGetEmailAddressInsights(email string) (any, error) {
	result, err := h.Backend.GetEmailAddressInsights(email)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (h *Handler) handleGetMessageInsights(messageID string) (any, error) {
	result, err := h.Backend.GetMessageInsights(messageID)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (h *Handler) handleListRecommendations(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")

	items, next, err := h.Backend.ListRecommendations(nextToken, 0)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"Recommendations": items,
		keyNextToken:      next,
	}, nil
}

// email template handlers

func (h *Handler) handleGetEmailTemplate(name string) (any, error) {
	t, err := h.Backend.GetEmailTemplate(name)
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (h *Handler) handleDeleteEmailTemplate(name string) (any, error) {
	if err := h.Backend.DeleteEmailTemplate(name); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type updateEmailTemplateInput struct {
	TemplateContent EmailTemplateContent `json:"TemplateContent"`
}

func (h *Handler) handleUpdateEmailTemplate(c *echo.Context, name string) (any, error) {
	var in updateEmailTemplateInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.UpdateEmailTemplate(name, &in.TemplateContent); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleListEmailTemplates(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListEmailTemplates(nextToken, 0)

	return map[string]any{
		"TemplatesMetadata": pg.Data,
		keyNextToken:        pg.Next,
	}, nil
}

type testRenderEmailTemplateInput struct {
	TemplateData string `json:"TemplateData"`
}

func (h *Handler) handleTestRenderEmailTemplate(c *echo.Context, name string) (any, error) {
	var in testRenderEmailTemplateInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	rendered, err := h.Backend.TestRenderEmailTemplate(name, in.TemplateData)
	if err != nil {
		return nil, err
	}

	return map[string]any{"RenderedTemplate": rendered}, nil
}

// export / import job handlers

type createExportJobInput struct {
	DataSource string `json:"DataSource"`
}

func (h *Handler) handleCreateExportJob(c *echo.Context) (any, error) {
	var in createExportJobInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	job, err := h.Backend.CreateExportJob(in.DataSource)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (h *Handler) handleGetExportJob(jobID string) (any, error) {
	job, err := h.Backend.GetExportJob(jobID)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (h *Handler) handleListExportJobs(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListExportJobs(nextToken, 0)

	return map[string]any{
		"ExportJobs": pg.Data,
		keyNextToken: pg.Next,
	}, nil
}

type createImportJobInput struct {
	DataSource string `json:"DataSource"`
}

func (h *Handler) handleCreateImportJob(c *echo.Context) (any, error) {
	var in createImportJobInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	job, err := h.Backend.CreateImportJob(in.DataSource)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (h *Handler) handleGetImportJob(jobID string) (any, error) {
	job, err := h.Backend.GetImportJob(jobID)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (h *Handler) handleListImportJobs(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListImportJobs(nextToken, 0)

	return map[string]any{
		"ImportJobs": pg.Data,
		keyNextToken: pg.Next,
	}, nil
}

// email identity policy handlers

func (h *Handler) handleGetEmailIdentityPolicies(identity string) (any, error) {
	policies, err := h.Backend.GetEmailIdentityPolicies(identity)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Policies": policies}, nil
}

func (h *Handler) handleDeleteEmailIdentityPolicy(c *echo.Context, identity string) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < policyPathMinSegments {
		return nil, fmt.Errorf("%w: invalid policy path", ErrInvalidInput)
	}

	policyName := segments[3]

	if decoded, err := url.PathUnescape(policyName); err == nil {
		policyName = decoded
	}

	if err := h.Backend.DeleteEmailIdentityPolicy(identity, policyName); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleUpdateEmailIdentityPolicy(c *echo.Context, identity string) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < policyPathMinSegments {
		return nil, fmt.Errorf("%w: invalid policy path", ErrInvalidInput)
	}

	policyName := segments[3]

	if decoded, err := url.PathUnescape(policyName); err == nil {
		policyName = decoded
	}

	var in createEmailIdentityPolicyInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.UpdateEmailIdentityPolicy(identity, policyName, in.Policy); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// email identity attribute stub handlers

func (h *Handler) handlePutEmailIdentityConfigurationSetAttributes() (any, error) {
	return &emptyDeleteOutput{}, h.Backend.PutEmailIdentityConfigurationSetAttributes("")
}

func (h *Handler) handlePutEmailIdentityDkimAttributes() (any, error) {
	return &emptyDeleteOutput{}, h.Backend.PutEmailIdentityDkimAttributes("")
}

func (h *Handler) handlePutEmailIdentityDkimSigningAttributes() (any, error) {
	return &emptyDeleteOutput{}, h.Backend.PutEmailIdentityDkimSigningAttributes("")
}

func (h *Handler) handlePutEmailIdentityFeedbackAttributes() (any, error) {
	return &emptyDeleteOutput{}, h.Backend.PutEmailIdentityFeedbackAttributes("")
}

func (h *Handler) handlePutEmailIdentityMailFromAttributes() (any, error) {
	return &emptyDeleteOutput{}, h.Backend.PutEmailIdentityMailFromAttributes("")
}

// configuration set event destination handlers

func (h *Handler) handleGetConfigurationSetEventDestinations(configSetName string) (any, error) {
	dests, err := h.Backend.GetConfigurationSetEventDestinations(configSetName)
	if err != nil {
		return nil, err
	}

	return map[string]any{"EventDestinations": dests}, nil
}

func (h *Handler) handleDeleteConfigurationSetEventDestination(
	c *echo.Context,
	configSetName string,
) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < 4 { //nolint:mnd // URL segment index is self-documenting in context
		return nil, fmt.Errorf("%w: invalid event destination path", ErrInvalidInput)
	}

	destName := segments[3]

	if decoded, err := url.PathUnescape(destName); err == nil {
		destName = decoded
	}

	if err := h.Backend.DeleteConfigurationSetEventDestination(configSetName, destName); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type updateConfigurationSetEventDestinationInput struct {
	EventDestination struct {
		MatchingEventTypes []string `json:"MatchingEventTypes"`
		Enabled            bool     `json:"Enabled"`
	} `json:"EventDestination"`
}

func (h *Handler) handleUpdateConfigurationSetEventDestination(
	c *echo.Context,
	configSetName string,
) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < 4 { //nolint:mnd // URL segment index is self-documenting in context
		return nil, fmt.Errorf("%w: invalid event destination path", ErrInvalidInput)
	}

	destName := segments[3]

	if decoded, err := url.PathUnescape(destName); err == nil {
		destName = decoded
	}

	var in updateConfigurationSetEventDestinationInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.UpdateConfigurationSetEventDestination(
		configSetName, destName, in.EventDestination.Enabled, in.EventDestination.MatchingEventTypes,
	); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// configuration set attribute stub handlers

func (h *Handler) handlePutConfigurationSetArchivingOptions() (any, error) {
	return &emptyDeleteOutput{}, h.Backend.PutConfigurationSetArchivingOptions("")
}

func (h *Handler) handlePutConfigurationSetDeliveryOptions() (any, error) {
	return &emptyDeleteOutput{}, h.Backend.PutConfigurationSetDeliveryOptions("")
}

func (h *Handler) handlePutConfigurationSetReputationOptions() (any, error) {
	return &emptyDeleteOutput{}, h.Backend.PutConfigurationSetReputationOptions("")
}

func (h *Handler) handlePutConfigurationSetSendingOptions() (any, error) {
	return &emptyDeleteOutput{}, h.Backend.PutConfigurationSetSendingOptions("")
}

func (h *Handler) handlePutConfigurationSetSuppressionOptions() (any, error) {
	return &emptyDeleteOutput{}, h.Backend.PutConfigurationSetSuppressionOptions("")
}

func (h *Handler) handlePutConfigurationSetTrackingOptions() (any, error) {
	return &emptyDeleteOutput{}, h.Backend.PutConfigurationSetTrackingOptions("")
}

func (h *Handler) handlePutConfigurationSetVdmOptions() (any, error) {
	return &emptyDeleteOutput{}, h.Backend.PutConfigurationSetVdmOptions("")
}

// bulk email handler

type sendBulkEmailInput struct {
	FromEmailAddress string           `json:"FromEmailAddress"`
	BulkEmailEntries []map[string]any `json:"BulkEmailEntries"`
}

func (h *Handler) handleSendBulkEmail(c *echo.Context) (any, error) {
	var in sendBulkEmailInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	results, err := h.Backend.SendBulkEmail(in.FromEmailAddress, in.BulkEmailEntries)
	if err != nil {
		return nil, err
	}

	return map[string]any{"BulkEmailEntryResults": results}, nil
}

// multi-region endpoint handlers

type createMultiRegionEndpointInput struct {
	EndpointName string `json:"EndpointName"`
}

func (h *Handler) handleCreateMultiRegionEndpoint(c *echo.Context) (any, error) {
	var in createMultiRegionEndpointInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	status, err := h.Backend.CreateMultiRegionEndpoint(in.EndpointName)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyStatus: status}, nil
}

func (h *Handler) handleGetMultiRegionEndpoint(name string) (any, error) {
	result, err := h.Backend.GetMultiRegionEndpoint(name)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (h *Handler) handleDeleteMultiRegionEndpoint(name string) (any, error) {
	if err := h.Backend.DeleteMultiRegionEndpoint(name); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleListMultiRegionEndpoints(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")

	items, next, err := h.Backend.ListMultiRegionEndpoints(nextToken, 0)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"MultiRegionEndpoints": items,
		keyNextToken:           next,
	}, nil
}

// tenant handlers

type createTenantInput struct {
	TenantName string `json:"TenantName"`
}

func (h *Handler) handleCreateTenant(c *echo.Context) (any, error) {
	var in createTenantInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	result, err := h.Backend.CreateTenant(in.TenantName)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (h *Handler) handleGetTenant(tenantName string) (any, error) {
	result, err := h.Backend.GetTenant(tenantName)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (h *Handler) handleDeleteTenant(tenantName string) (any, error) {
	if err := h.Backend.DeleteTenant(tenantName); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleListTenants(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")

	items, next, err := h.Backend.ListTenants(nextToken, 0)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"Tenants":    items,
		keyNextToken: next,
	}, nil
}

type createTenantResourceAssociationInput struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleCreateTenantResourceAssociation(
	c *echo.Context,
	tenantName string,
) (any, error) {
	var in createTenantResourceAssociationInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.CreateTenantResourceAssociation(tenantName, in.ResourceArn); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleDeleteTenantResourceAssociation(
	c *echo.Context,
	tenantName string,
) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	resourceArn := ""

	if len(segments) >= 4 { //nolint:mnd // URL segment index is self-documenting in context
		resourceArn = segments[3]

		if decoded, err := url.PathUnescape(resourceArn); err == nil {
			resourceArn = decoded
		}
	}

	if err := h.Backend.DeleteTenantResourceAssociation(tenantName, resourceArn); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleListResourceTenants(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")

	items, next, err := h.Backend.ListResourceTenants(nextToken, 0)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"ResourceTenants": items,
		keyNextToken:      next,
	}, nil
}

func (h *Handler) handleListTenantResources(c *echo.Context, tenantName string) (any, error) {
	nextToken := c.QueryParam("NextToken")

	items, next, err := h.Backend.ListTenantResources(tenantName, 0)
	_ = nextToken

	if err != nil {
		return nil, err
	}

	return map[string]any{
		"TenantResources": items,
		keyNextToken:      next,
	}, nil
}

// reputation entity handlers

func (h *Handler) handleGetReputationEntity(entityID string) (any, error) {
	result, err := h.Backend.GetReputationEntity(entityID)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (h *Handler) handleListReputationEntities(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")

	items, next, err := h.Backend.ListReputationEntities(nextToken, 0)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"ReputationEntities": items,
		keyNextToken:         next,
	}, nil
}

func (h *Handler) handleUpdateReputationEntityCustomerManagedStatus(entityID string) (any, error) {
	if err := h.Backend.UpdateReputationEntityCustomerManagedStatus(entityID); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type updateReputationEntityPolicyInput struct {
	Policy string `json:"Policy"`
}

func (h *Handler) handleUpdateReputationEntityPolicy(
	c *echo.Context,
	entityID string,
) (any, error) {
	var in updateReputationEntityPolicyInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.UpdateReputationEntityPolicy(entityID, in.Policy); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}
