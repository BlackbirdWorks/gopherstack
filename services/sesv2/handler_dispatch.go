package sesv2

import (
	"errors"

	"github.com/labstack/echo/v5"
)

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
		return h.handleListTagsForResource(c)
	case opTagResource:
		return h.handleTagResource(c)
	case opUntagResource:
		return h.handleUntagResource(c)
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

// dispatchExtendedOps handles all 89 newly-added SES v2 operations.
func (h *Handler) dispatchExtendedOps(c *echo.Context, op, resource string) (any, error) {
	if r, err := h.dispatchAccountAndSuppressionOps(c, op, resource); !errors.Is(err, errOpNotHandled) {
		return r, err
	}

	if r, err := h.dispatchContactAndVerificationOps(c, op, resource); !errors.Is(err, errOpNotHandled) {
		return r, err
	}

	if r, err := h.dispatchDedicatedIPAndDeliverabilityOps(c, op, resource); !errors.Is(err, errOpNotHandled) {
		return r, err
	}

	if r, err := h.dispatchTemplateAndJobOps(c, op, resource); !errors.Is(err, errOpNotHandled) {
		return r, err
	}

	if r, err := h.dispatchIdentityAndConfigSetOps(c, op, resource); !errors.Is(err, errOpNotHandled) {
		return r, err
	}

	return h.dispatchEndpointAndTenantOps(c, op, resource)
}

// dispatchAccountAndSuppressionOps handles account and suppression operations.
func (h *Handler) dispatchAccountAndSuppressionOps(c *echo.Context, op, resource string) (any, error) {
	switch op {
	case opGetAccount:
		return h.handleGetAccount()
	case opGetBlacklistReports:
		return h.handleGetBlacklistReports()
	case opPutAccountDedicatedIPWarmupAttributes:
		return h.handlePutAccountDedicatedIPWarmupAttributes(c)
	case opPutAccountDetails:
		return h.handlePutAccountDetails(c)
	case opPutAccountPricingAttributes:
		return h.handlePutAccountPricingAttributes(c)
	case opPutAccountSendingAttributes:
		return h.handlePutAccountSendingAttributes(c)
	case opPutAccountSuppressionAttributes:
		return h.handlePutAccountSuppressionAttributes(c)
	case opPutAccountVdmAttributes:
		return h.handlePutAccountVdmAttributes(c)
	case opPutSuppressedDestination:
		return h.handlePutSuppressedDestination(c)
	case opGetSuppressedDestination:
		return h.handleGetSuppressedDestination(resource)
	case opDeleteSuppressedDestination:
		return h.handleDeleteSuppressedDestination(resource)
	case opListSuppressedDestinations:
		return h.handleListSuppressedDestinations(c)
	}

	return nil, errOpNotHandled
}

// dispatchContactAndVerificationOps handles contact list, contact, and verification template operations.
func (h *Handler) dispatchContactAndVerificationOps(c *echo.Context, op, resource string) (any, error) {
	switch op {
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
	}

	return nil, errOpNotHandled
}

// dispatchDedicatedIPAndDeliverabilityOps handles dedicated IP pool and deliverability operations.
func (h *Handler) dispatchDedicatedIPAndDeliverabilityOps(c *echo.Context, op, resource string) (any, error) {
	if r, err := h.dispatchDedicatedIPOps(c, op, resource); !errors.Is(err, errOpNotHandled) {
		return r, err
	}

	return h.dispatchDeliverabilityOps(c, op, resource)
}

// dispatchDedicatedIPOps handles dedicated IP pool operations.
func (h *Handler) dispatchDedicatedIPOps(c *echo.Context, op, resource string) (any, error) {
	switch op {
	case opGetDedicatedIPPool:
		return h.handleGetDedicatedIPPool(resource)
	case opDeleteDedicatedIPPool:
		return h.handleDeleteDedicatedIPPool(resource)
	case opListDedicatedIPPools:
		return h.handleListDedicatedIPPools(c)
	case opGetDedicatedIP:
		return h.handleGetDedicatedIP(resource)
	case opGetDedicatedIps:
		return h.handleGetDedicatedIps(c)
	case opPutDedicatedIPInPool:
		return h.handlePutDedicatedIPInPool(c, resource)
	case opPutDedicatedIPPoolScalingAttributes:
		return h.handlePutDedicatedIPPoolScalingAttributes(c, resource)
	case opPutDedicatedIPWarmupAttributes:
		return h.handlePutDedicatedIPWarmupAttributes(c, resource)
	}

	return nil, errOpNotHandled
}

// dispatchDeliverabilityOps handles deliverability and insights operations.
func (h *Handler) dispatchDeliverabilityOps(c *echo.Context, op, resource string) (any, error) {
	switch op {
	case opGetDeliverabilityDashboardOptions:
		return h.handleGetDeliverabilityDashboardOptions()
	case opPutDeliverabilityDashboardOption:
		return h.handlePutDeliverabilityDashboardOption(c)
	case opGetDeliverabilityTestReport:
		return h.handleGetDeliverabilityTestReport(resource)
	case opListDeliverabilityTestReports:
		return h.handleListDeliverabilityTestReports(c)
	case opGetDomainDeliverabilityCampaign:
		return h.handleGetDomainDeliverabilityCampaign(resource)
	case opGetDomainStatisticsReport:
		return h.handleGetDomainStatisticsReport(c, resource)
	case opListDomainDeliverabilityCampaigns:
		return h.handleListDomainDeliverabilityCampaigns(c, resource)
	case opGetEmailAddressInsights:
		return h.handleGetEmailAddressInsights(c)
	case opGetMessageInsights:
		return h.handleGetMessageInsights(resource)
	case opListRecommendations:
		return h.handleListRecommendations(c)
	}

	return nil, errOpNotHandled
}

// dispatchTemplateAndJobOps handles email template, export/import job operations.
func (h *Handler) dispatchTemplateAndJobOps(c *echo.Context, op, resource string) (any, error) {
	switch op {
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
	case opSendBulkEmail:
		return h.handleSendBulkEmail(c)
	}

	return nil, errOpNotHandled
}

// dispatchIdentityAndConfigSetOps handles email identity and config set operations.
func (h *Handler) dispatchIdentityAndConfigSetOps(c *echo.Context, op, resource string) (any, error) {
	if r, err := h.dispatchEmailIdentityOps(c, op, resource); !errors.Is(err, errOpNotHandled) {
		return r, err
	}

	return h.dispatchConfigSetAttrOps(c, op, resource)
}

// dispatchEmailIdentityOps handles email identity policy and attribute operations.
func (h *Handler) dispatchEmailIdentityOps(c *echo.Context, op, resource string) (any, error) {
	switch op {
	case opGetEmailIdentityPolicies:
		return h.handleGetEmailIdentityPolicies(resource)
	case opDeleteEmailIdentityPolicy:
		return h.handleDeleteEmailIdentityPolicy(c, resource)
	case opUpdateEmailIdentityPolicy:
		return h.handleUpdateEmailIdentityPolicy(c, resource)
	case opPutEmailIdentityConfigurationSetAttributes:
		return h.handlePutEmailIdentityConfigurationSetAttributes(c, resource)
	case opPutEmailIdentityDkimAttributes:
		return h.handlePutEmailIdentityDkimAttributes(c, resource)
	case opPutEmailIdentityDkimSigningAttributes:
		return h.handlePutEmailIdentityDkimSigningAttributes(resource)
	case opPutEmailIdentityFeedbackAttributes:
		return h.handlePutEmailIdentityFeedbackAttributes(c, resource)
	case opPutEmailIdentityMailFromAttributes:
		return h.handlePutEmailIdentityMailFromAttributes(c, resource)
	case opGetConfigurationSetEventDestinations:
		return h.handleGetConfigurationSetEventDestinations(resource)
	case opDeleteConfigurationSetEventDestination:
		return h.handleDeleteConfigurationSetEventDestination(c, resource)
	case opUpdateConfigurationSetEventDestination:
		return h.handleUpdateConfigurationSetEventDestination(c, resource)
	}

	return nil, errOpNotHandled
}

// dispatchConfigSetAttrOps handles configuration set attribute put operations.
func (h *Handler) dispatchConfigSetAttrOps(c *echo.Context, op, resource string) (any, error) {
	switch op {
	case opPutConfigurationSetArchivingOptions:
		return h.handlePutConfigurationSetArchivingOptions(c, resource)
	case opPutConfigurationSetDeliveryOptions:
		return h.handlePutConfigurationSetDeliveryOptions(c, resource)
	case opPutConfigurationSetReputationOptions:
		return h.handlePutConfigurationSetReputationOptions(c, resource)
	case opPutConfigurationSetSendingOptions:
		return h.handlePutConfigurationSetSendingOptions(c, resource)
	case opPutConfigurationSetSuppressionOptions:
		return h.handlePutConfigurationSetSuppressionOptions(c, resource)
	case opPutConfigurationSetTrackingOptions:
		return h.handlePutConfigurationSetTrackingOptions(c, resource)
	case opPutConfigurationSetVdmOptions:
		return h.handlePutConfigurationSetVdmOptions(c, resource)
	}

	return nil, errOpNotHandled
}

// dispatchEndpointAndTenantOps handles multi-region endpoint, tenant, and reputation entity operations.
func (h *Handler) dispatchEndpointAndTenantOps(c *echo.Context, op, resource string) (any, error) {
	if r, err := h.dispatchEndpointTenantCRUDOps(c, op, resource); !errors.Is(err, errOpNotHandled) {
		return r, err
	}

	return h.dispatchReputationEntityOps(c, op, resource)
}

// dispatchEndpointTenantCRUDOps handles multi-region endpoint and tenant operations.
func (h *Handler) dispatchEndpointTenantCRUDOps(c *echo.Context, op, resource string) (any, error) {
	switch op {
	case opCreateMultiRegionEndpoint:
		return h.handleCreateMultiRegionEndpoint(c)
	case opGetMultiRegionEndpoint:
		return h.handleGetMultiRegionEndpoint(resource)
	case opDeleteMultiRegionEndpoint:
		return h.handleDeleteMultiRegionEndpoint(resource)
	case opListMultiRegionEndpoints:
		return h.handleListMultiRegionEndpoints(c)
	case opCreateTenant:
		return h.handleCreateTenant(c)
	case opGetTenant:
		return h.handleGetTenant(c)
	case opDeleteTenant:
		return h.handleDeleteTenant(c)
	case opListTenants:
		return h.handleListTenants(c)
	case opCreateTenantResourceAssociation:
		return h.handleCreateTenantResourceAssociation(c)
	case opDeleteTenantResourceAssociation:
		return h.handleDeleteTenantResourceAssociation(c)
	case opListResourceTenants:
		return h.handleListResourceTenants(c)
	case opListTenantResources:
		return h.handleListTenantResources(c)
	case opPutTenantSuppressionAttributes:
		return h.handlePutTenantSuppressionAttributes(c)
	}

	return nil, errOpNotHandled
}

// dispatchReputationEntityOps handles reputation entity operations.
func (h *Handler) dispatchReputationEntityOps(c *echo.Context, op, resource string) (any, error) {
	switch op {
	case opGetReputationEntity:
		return h.handleGetReputationEntity(resource)
	case opListReputationEntities:
		return h.handleListReputationEntities(c)
	case opUpdateReputationEntityCustomerManagedStatus:
		return h.handleUpdateReputationEntityCustomerManagedStatus(c, resource)
	case opUpdateReputationEntityPolicy:
		return h.handleUpdateReputationEntityPolicy(c, resource)
	}

	return nil, errOpNotHandled
}
