package qldb

import "time"

// StorageBackend defines the interface for QLDB backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Ledger operations
	CreateLedger(name, permissionsMode string, deletionProtected bool, tags map[string]string) (*Ledger, error)
	GetLedger(name string) (*Ledger, error)
	ListLedgers() []*Ledger
	UpdateLedger(name string, deletionProtected bool) (*Ledger, error)
	DeleteLedger(name string) error

	// Tag operations
	TagResource(resourceARN string, kv map[string]string) error
	UntagResource(resourceARN string, keys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	// Journal kinesis stream operations
	AddJournalKinesisStreamInternal(s *JournalKinesisStream)
	CancelJournalKinesisStream(ledgerName, streamID string) (*JournalKinesisStream, error)
	GetJournalKinesisStream(ledgerName, streamID string) (*JournalKinesisStream, error)
	ListJournalKinesisStreamsForLedger(ledgerName string) ([]*JournalKinesisStream, error)

	// Journal S3 export operations
	ExportJournalToS3(
		ledgerName, roleArn string,
		s3Config S3ExportConfiguration,
		inclusiveStartTime, exclusiveEndTime time.Time,
		outputFormat string,
	) (*JournalS3Export, error)
	GetJournalS3Export(ledgerName, exportID string) (*JournalS3Export, error)
	ListJournalS3Exports() []*JournalS3Export
	ListJournalS3ExportsForLedger(ledgerName string) ([]*JournalS3Export, error)

	// Verification operations
	GetBlock(name string) (string, string, error)
	GetDigest(name string) ([]byte, string, error)
	GetRevision(name string) (string, string, error)

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}
