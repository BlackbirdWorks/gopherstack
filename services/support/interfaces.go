package support

import "time"

// StorageBackend defines the interface for Support backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	CreateCase(subject, serviceCode, categoryCode, severityCode, body string) (*Case, error)
	DescribeCases(caseIDs []string) []Case
	ResolveCase(caseID string) (*Case, error)
	AddCommunicationToCase(caseID, body string) error
	DescribeCommunications(caseID string) ([]Communication, error)
	DescribeTrustedAdvisorChecks() []TrustedAdvisorCheck
	AddAttachmentsToSet(attachmentSetID string) (string, time.Time, error)
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
