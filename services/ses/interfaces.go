package ses

import "github.com/blackbirdworks/gopherstack/pkgs/page"

// StorageBackend defines the persistence contract for the SES service.
type StorageBackend interface {
	VerifyEmailIdentity(identity string) error
	DeleteIdentity(identity string)
	ListIdentities(nextToken string, maxItems int) page.Page[string]
	GetIdentityVerificationAttributes(identities []string) map[string]string
	SendEmail(from string, to []string, subject, bodyHTML, bodyText string) (string, error)
	SendTemplatedEmail(from string, to []string, templateName string) (string, error)
	ListEmails() []Email
	GetEmailByID(messageID string) (Email, error)
	CreateTemplate(tmpl EmailTemplate) error
	UpdateTemplate(tmpl EmailTemplate) error
	GetTemplate(name string) (EmailTemplate, error)
	DeleteTemplate(name string)
	ListTemplates(nextToken string, maxItems int) page.Page[string]
	CreateConfigurationSet(name string) error
	DeleteConfigurationSet(name string) error
	ListConfigurationSets(nextToken string, maxItems int) page.Page[string]
	GetSendQuota() SendQuota
	GetSendStatistics() []SendDataPoint
	CreateReceiptRuleSet(name string) error
	CloneReceiptRuleSet(originalName, newName string) error
	CreateReceiptRule(ruleSetName string, rule ReceiptRule, after string) error
	CreateReceiptFilter(filter ReceiptFilter) error
	CreateConfigurationSetEventDestination(configSetName string, dest EventDestination) error
	DeleteConfigurationSetEventDestination(configSetName, destName string) error
	CreateConfigurationSetTrackingOptions(configSetName, customRedirectDomain string) error
	DeleteConfigurationSetTrackingOptions(configSetName string) error
	CreateCustomVerificationEmailTemplate(tmpl CustomVerificationEmailTemplate) error
	DeleteCustomVerificationEmailTemplate(templateName string) error
	ListReceiptFilters() []ReceiptFilter
	ListReceiptRuleSets() []ReceiptRuleSet
	DeleteReceiptFilter(name string) error
	DeleteReceiptRule(ruleSetName, ruleName string) error
	DeleteReceiptRuleSet(name string) error
	GetCustomVerificationEmailTemplate(templateName string) (CustomVerificationEmailTemplate, error)
	ListCustomVerificationEmailTemplates() []CustomVerificationEmailTemplate
	DescribeReceiptRuleSet(name string) (ReceiptRuleSet, error)
	SetActiveReceiptRuleSet(name string) error
	DescribeActiveReceiptRuleSet() (ReceiptRuleSet, bool, error)
	Region() string
	AccountID() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

// Compile-time check that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
