package support

import "time"

// StorageBackend defines the interface for Support backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	Reset()
	CreateCase(subject, serviceCode, categoryCode, severityCode, body string) (*Case, error)
	CreateCaseWithOptions(in CreateCaseOptions) (*Case, error)
	DescribeCases(caseIDs []string, includeResolvedCases bool) []Case
	DescribeCasesWithOptions(in DescribeCasesOptions) ([]Case, string, error)
	RecentCommunications(caseID string) ([]Communication, string)
	ResolveCase(caseID string) (*Case, error)
	ResolveCaseWithStatus(caseID string) (string, *Case, error)
	AddCommunicationToCase(caseID, body, attachmentSetID string) error
	AddCommunicationWithOptions(in AddCommunicationOptions) error
	DescribeCommunications(caseID string) ([]Communication, error)
	DescribeCommunicationsWithOptions(in DescribeCommunicationsOptions) ([]Communication, string, error)
	DescribeTrustedAdvisorChecks() []TrustedAdvisorCheck
	AddAttachmentsToSet(attachmentSetID string) (string, time.Time, error)
	AddAttachmentsToSetWithAttachments(attachmentSetID string, attachments []Attachment) (string, time.Time, error)
	DescribeAttachment(attachmentID string) (*Attachment, error)
	DescribeCreateCaseOptions(issueType, serviceCode, categoryCode, language string) *DescribeCreateCaseOptionsResult
	DescribeServices(serviceCodeList []string, language string) []Service
	DescribeSeverityLevels(language string) []SeverityLevel
	DescribeSupportedLanguages(issueType, serviceCode, severityLevel string) []SupportedLanguage
	DescribeTrustedAdvisorCheckRefreshStatuses(checkIDs []string) []TrustedAdvisorCheckRefreshStatus
	DescribeTrustedAdvisorCheckResult(checkID, language string) *TrustedAdvisorCheckResult
	DescribeTrustedAdvisorCheckSummaries(checkIDs []string) []TrustedAdvisorCheckSummary
	RefreshTrustedAdvisorCheck(checkID string) (*TrustedAdvisorCheckRefreshStatus, error)
	Snapshot() []byte
	Restore(data []byte) error
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
