package sesv2

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// StorageBackend is the interface for sesv2 storage operations.
type StorageBackend interface {
	// Core identity ops
	CreateEmailIdentity(identity, configurationSetName string, tags map[string]string) (*EmailIdentity, error)
	GetEmailIdentity(identity string) (*EmailIdentity, error)
	ListEmailIdentities(nextToken string, pageSize int) page.Page[*EmailIdentity]
	DeleteEmailIdentity(identity string) error

	// Email identity policy ops
	CreateEmailIdentityPolicy(identity, policyName, policy string) error
	GetEmailIdentityPolicies(identity string) (map[string]string, error)
	DeleteEmailIdentityPolicy(identity, policyName string) error
	UpdateEmailIdentityPolicy(identity, policyName, policy string) error

	// Email identity attribute ops
	PutEmailIdentityConfigurationSetAttributes(identity, configSetName string) error
	PutEmailIdentityDkimAttributes(identity string, signingEnabled bool) error
	PutEmailIdentityDkimSigningAttributes(identity string) error
	PutEmailIdentityFeedbackAttributes(identity string, emailForwardingEnabled bool) error
	PutEmailIdentityMailFromAttributes(identity, mailFromDomain, behaviorOnMxFailure string) error

	// Configuration set ops
	CreateConfigurationSet(name string) (*ConfigurationSet, error)
	GetConfigurationSet(name string) (*ConfigurationSet, error)
	ListConfigurationSets(nextToken string, pageSize int) page.Page[*ConfigurationSet]
	DeleteConfigurationSet(name string) error

	// Configuration set attribute ops
	PutConfigurationSetArchivingOptions(name, archiveARN string) error
	PutConfigurationSetDeliveryOptions(name, tlsPolicy, sendingPoolName string) error
	PutConfigurationSetReputationOptions(name string, metricsEnabled bool) error
	PutConfigurationSetSendingOptions(name string, sendingEnabled bool) error
	PutConfigurationSetSuppressionOptions(name string, suppressedReasons []string) error
	PutConfigurationSetTrackingOptions(name, customRedirectDomain, httpsPolicy string) error
	PutConfigurationSetVdmOptions(name string, dashboardOptions, guardianOptions map[string]any) error

	// Event destination ops
	CreateConfigurationSetEventDestination(
		configSetName, destName string,
		enabled bool,
		matchingEventTypes []string,
	) (*EventDestination, error)
	GetConfigurationSetEventDestinations(configSetName string) ([]*EventDestination, error)
	DeleteConfigurationSetEventDestination(configSetName, destName string) error
	UpdateConfigurationSetEventDestination(
		configSetName, destName string,
		enabled bool,
		matchingEventTypes []string,
	) error

	// Email sending ops
	SendEmail(from string, to []string, subject, bodyHTML, bodyText string) (string, error)
	SendBulkEmail(
		fromEmailAddress string,
		bulkEmailEntries []map[string]any,
	) ([]map[string]any, error)
	SendCustomVerificationEmail(emailAddress, templateName string) (string, error)
	ListEmails() []Email

	// Contact list ops
	CreateContactList(name, description string) (*ContactList, error)
	GetContactList(name string) (*ContactList, error)
	DeleteContactList(name string) error
	UpdateContactList(name, description string) error
	ListContactLists(nextToken string, pageSize int) page.Page[*ContactList]

	// Contact ops
	CreateContact(
		contactListName, emailAddress string,
		topicPreferences []TopicPreference,
	) (*Contact, error)
	GetContact(contactListName, emailAddress string) (*Contact, error)
	DeleteContact(contactListName, emailAddress string) error
	UpdateContact(contactListName, emailAddress string, topicPreferences []TopicPreference) error
	ListContacts(contactListName, nextToken string, pageSize int) (page.Page[*Contact], error)

	// Custom verification template ops
	CreateCustomVerificationEmailTemplate(
		in *CustomVerificationEmailTemplate,
	) (*CustomVerificationEmailTemplate, error)
	GetCustomVerificationEmailTemplate(name string) (*CustomVerificationEmailTemplate, error)
	DeleteCustomVerificationEmailTemplate(name string) error
	UpdateCustomVerificationEmailTemplate(in *CustomVerificationEmailTemplate) error
	ListCustomVerificationEmailTemplates(
		nextToken string,
		pageSize int,
	) page.Page[*CustomVerificationEmailTemplate]

	// Dedicated IP pool ops
	CreateDedicatedIPPool(poolName, scalingMode string) (*DedicatedIPPool, error)
	GetDedicatedIPPool(poolName string) (*DedicatedIPPool, error)
	DeleteDedicatedIPPool(poolName string) error
	ListDedicatedIPPools(nextToken string, pageSize int) page.Page[string]
	GetDedicatedIP(ip string) (map[string]any, error)
	GetDedicatedIps() []map[string]any
	PutDedicatedIPInPool(ip, poolName string) error
	PutDedicatedIPPoolScalingAttributes(poolName, scalingMode string) error
	PutDedicatedIPWarmupAttributes(ip string, warmupPercentage int) error

	// Deliverability test report ops
	CreateDeliverabilityTestReport(
		reportName, fromEmailAddress string,
	) (*DeliverabilityTestReport, error)
	GetDeliverabilityTestReport(reportID string) (*DeliverabilityTestReport, error)
	ListDeliverabilityTestReports(
		nextToken string,
		pageSize int,
	) page.Page[*DeliverabilityTestReport]

	// Deliverability dashboard ops
	GetDeliverabilityDashboardOptions() (map[string]any, error)
	PutDeliverabilityDashboardOption(enabled bool, subscribedDomains []string) error
	GetDomainDeliverabilityCampaign(campaignID string) (map[string]any, error)
	GetDomainStatisticsReport(domain, startDate, endDate string) (map[string]any, error)
	ListDomainDeliverabilityCampaigns(
		startDate, endDate, domain, nextToken string,
	) ([]map[string]any, string, error)

	// Email template ops
	CreateEmailTemplate(templateName string, content *EmailTemplateContent) (*EmailTemplate, error)
	GetEmailTemplate(name string) (*EmailTemplate, error)
	DeleteEmailTemplate(name string) error
	UpdateEmailTemplate(name string, content *EmailTemplateContent) error
	ListEmailTemplates(nextToken string, pageSize int) page.Page[*EmailTemplate]
	TestRenderEmailTemplate(name, templateData string) (string, error)

	// Export job ops
	CreateExportJob(dataSource string) (*ExportJob, error)
	GetExportJob(jobID string) (*ExportJob, error)
	ListExportJobs(nextToken string, pageSize int) page.Page[*ExportJob]
	CancelExportJob(jobID string) error

	// Import job ops
	CreateImportJob(dataSource string) (*ImportJob, error)
	GetImportJob(jobID string) (*ImportJob, error)
	ListImportJobs(nextToken string, pageSize int) page.Page[*ImportJob]

	// Suppressed destination ops
	PutSuppressedDestination(email, reason string) error
	GetSuppressedDestination(email string) (*SuppressedDestination, error)
	DeleteSuppressedDestination(email string) error
	ListSuppressedDestinations(nextToken string, pageSize int) page.Page[*SuppressedDestination]

	// Account ops
	GetAccount() (*AccountDetails, error)
	PutAccountDetails(details *AccountDetails) error
	PutAccountSendingAttributes(sendingEnabled bool) error
	PutAccountSuppressionAttributes(suppressedReasons []string) error
	PutAccountVdmAttributes(vdmAttributes map[string]any) error
	PutAccountDedicatedIPWarmupAttributes(autoWarmupEnabled bool) error
	GetBlacklistReports() (map[string][]string, error)

	TagResource(arn string, tags map[string]string) error
	UntagResource(arn string, tagKeys []string) error
	ListTagsForResource(arn string) (map[string]string, error)

	// Insights / analytics (stubs)
	GetEmailAddressInsights(emailAddress string) (map[string]any, error)
	GetMessageInsights(messageID string) (map[string]any, error)
	ListRecommendations(nextToken string, pageSize int) ([]map[string]any, string, error)

	// Multi-region endpoint ops
	CreateMultiRegionEndpoint(
		endpointName string,
		secondaryRegions []string,
		tags map[string]string,
	) (map[string]any, error)
	GetMultiRegionEndpoint(endpointName string) (map[string]any, error)
	DeleteMultiRegionEndpoint(endpointName string) (string, error)
	ListMultiRegionEndpoints(nextToken string, pageSize int) ([]map[string]any, string, error)

	// Tenant ops
	CreateTenant(tenantName string, tags map[string]string) (map[string]any, error)
	GetTenant(tenantName string) (map[string]any, error)
	DeleteTenant(tenantName string) error
	ListTenants(nextToken string, pageSize int) ([]map[string]any, string, error)
	CreateTenantResourceAssociation(tenantName, resourceArn string) error
	DeleteTenantResourceAssociation(tenantName, resourceArn string) error
	ListResourceTenants(resourceArn, nextToken string, pageSize int) ([]map[string]any, string, error)
	ListTenantResources(
		tenantName string,
		filter map[string]string,
		nextToken string,
		pageSize int,
	) ([]map[string]any, string, error)

	// Reputation entity ops (stubs)
	GetReputationEntity(entityID string) (map[string]any, error)
	ListReputationEntities(nextToken string, pageSize int) ([]map[string]any, string, error)
	UpdateReputationEntityCustomerManagedStatus(entityID, status string) error
	UpdateReputationEntityPolicy(entityID, policy string) error

	// Metrics
	BatchGetMetricData(queries []MetricDataQuery) ([]MetricDataResult, error)

	// Infrastructure
	Region() string
	AccountID() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
