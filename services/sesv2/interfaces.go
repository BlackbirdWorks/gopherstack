package sesv2

import "github.com/blackbirdworks/gopherstack/pkgs/page"

// StorageBackend is the interface for sesv2 storage operations.
type StorageBackend interface {
	CreateEmailIdentity(identity string) (*EmailIdentity, error)
	GetEmailIdentity(identity string) (*EmailIdentity, error)
	ListEmailIdentities(nextToken string, pageSize int) page.Page[*EmailIdentity]
	DeleteEmailIdentity(identity string) error
	CreateConfigurationSet(name string) (*ConfigurationSet, error)
	GetConfigurationSet(name string) (*ConfigurationSet, error)
	ListConfigurationSets(nextToken string, pageSize int) page.Page[*ConfigurationSet]
	DeleteConfigurationSet(name string) error
	SendEmail(from string, to []string, subject, bodyHTML, bodyText string) (string, error)
	ListEmails() []Email
	Region() string
	AccountID() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
	BatchGetMetricData(queries []MetricDataQuery) ([]MetricDataResult, error)
	CancelExportJob(jobID string) error
	CreateConfigurationSetEventDestination(
		configSetName, destName string,
		enabled bool,
		matchingEventTypes []string,
	) (*EventDestination, error)
	CreateContact(contactListName, emailAddress string, topicPreferences []TopicPreference) (*Contact, error)
	CreateContactList(name, description string) (*ContactList, error)
	CreateCustomVerificationEmailTemplate(in *CustomVerificationEmailTemplate) (*CustomVerificationEmailTemplate, error)
	CreateDedicatedIPPool(poolName, scalingMode string) (*DedicatedIPPool, error)
	CreateDeliverabilityTestReport(reportName, fromEmailAddress string) (*DeliverabilityTestReport, error)
	CreateEmailIdentityPolicy(identity, policyName, policy string) error
	CreateEmailTemplate(templateName string, content *EmailTemplateContent) (*EmailTemplate, error)
}

var _ StorageBackend = (*InMemoryBackend)(nil)
