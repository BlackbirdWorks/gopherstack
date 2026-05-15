package qldb

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

const (
	// StateActive is the active state for a ledger.
	StateActive = "ACTIVE"
	// StateCreating is the creating state for a ledger.
	StateCreating = "CREATING"
	// StateDeleting is the deleting state for a ledger.
	StateDeleting = "DELETING"
	// StateFailed is the failed state for a ledger.
	StateFailed = "FAILED"

	// permissionsModeAllowAll is the ALLOW_ALL permissions mode.
	permissionsModeAllowAll = "ALLOW_ALL"
	// permissionsModeSuperuser is the SUPERUSER permissions mode.
	permissionsModeSuperuser = "SUPERUSER"
	// permissionsModeStandard is the STANDARD permissions mode.
	permissionsModeStandard = "STANDARD"

	// StreamStatusActive is the active status for a journal kinesis stream.
	StreamStatusActive = "ACTIVE"
	// StreamStatusFailed is the failed status for a journal kinesis stream.
	StreamStatusFailed = "FAILED"
	// StreamStatusImpaired is the impaired status for a journal kinesis stream.
	StreamStatusImpaired = "IMPAIRED"
	// StreamStatusCompleted is the completed status for a journal kinesis stream.
	StreamStatusCompleted = "COMPLETED"
	// streamStatusCanceled is the canceled status for a journal kinesis stream.
	streamStatusCanceled = "CANCELED"

	// ExportStatusCompleted is the completed status for a journal S3 export.
	ExportStatusCompleted = "COMPLETED"
	// ExportStatusFailed is the failed status for a journal S3 export.
	ExportStatusFailed = "FAILED"
	// ExportStatusCancelled is the cancelled status for a journal S3 export.
	ExportStatusCancelled = "CANCELLED"
	// exportStatusInProgress is the in-progress status for a journal S3 export.
	exportStatusInProgress = "IN_PROGRESS"

	// syntheticDigestTipIonText is a synthetic Ion text for digest tip addresses
	// returned by GetDigest/GetBlock/GetRevision.
	syntheticDigestTipIonText = `{strandId:"synthetic",sequenceNo:0}`
	// syntheticBlockIonText is a synthetic Ion text returned by GetBlock.
	syntheticBlockIonText = `{strandId:"synthetic",sequenceNo:0,hash:{{AAAA}}}`
	// syntheticRevisionIonText is a synthetic Ion text returned by GetRevision.
	syntheticRevisionIonText = `{blockAddress:{strandId:"synthetic",sequenceNo:0},hash:{{AAAA}},data:{version:0}}`
	// syntheticDigestLen is the number of bytes in the synthetic digest returned by GetDigest.
	syntheticDigestLen = 32
)

var (
	// ErrNotFound is returned when a ledger does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a ledger already exists.
	ErrAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrConflict)
	// ErrDeletionProtection is returned when deletion protection is enabled.
	ErrDeletionProtection = awserr.New("ResourcePreconditionNotMetException", awserr.ErrConflict)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// Ledger represents a QLDB ledger.
type Ledger struct {
	CreationDateTime  time.Time         `json:"creationDateTime"`
	Tags              map[string]string `json:"tags,omitempty"`
	Name              string            `json:"name"`
	ARN               string            `json:"arn"`
	State             string            `json:"state"`
	PermissionsMode   string            `json:"permissionsMode"`
	AccountID         string            `json:"accountID"`
	Region            string            `json:"region"`
	DeletionProtected bool              `json:"deletionProtected"`
}

// KinesisConfiguration holds the Kinesis Data Streams destination config for a journal stream.
type KinesisConfiguration struct {
	StreamArn          string `json:"StreamArn"`
	AggregationEnabled bool   `json:"AggregationEnabled"`
}

// JournalKinesisStream represents a QLDB journal kinesis stream.
type JournalKinesisStream struct {
	CreationTime       time.Time            `json:"creationTime"`
	ExclusiveEndTime   *time.Time           `json:"exclusiveEndTime,omitempty"`
	InclusiveStartTime *time.Time           `json:"inclusiveStartTime,omitempty"`
	LedgerName         string               `json:"ledgerName"`
	RoleArn            string               `json:"roleArn"`
	StreamID           string               `json:"streamId"`
	StreamName         string               `json:"streamName"`
	ARN                string               `json:"arn"`
	Status             string               `json:"status"`
	KinesisConfig      KinesisConfiguration `json:"kinesisConfiguration"`
}

// S3ExportConfiguration holds the S3 destination config for a journal export.
type S3ExportConfiguration struct {
	Bucket                  string `json:"Bucket"`
	Prefix                  string `json:"Prefix"`
	EncryptionConfiguration struct {
		ObjectEncryptionType string `json:"ObjectEncryptionType"`
		KmsKeyArn            string `json:"KmsKeyArn,omitempty"`
	} `json:"EncryptionConfiguration"`
}

// JournalS3Export represents a QLDB journal S3 export job.
type JournalS3Export struct {
	ExclusiveEndTime   time.Time             `json:"exclusiveEndTime"`
	ExportCreationTime time.Time             `json:"exportCreationTime"`
	InclusiveStartTime time.Time             `json:"inclusiveStartTime"`
	S3Config           S3ExportConfiguration `json:"s3ExportConfiguration"`
	ExportID           string                `json:"exportId"`
	LedgerName         string                `json:"ledgerName"`
	RoleArn            string                `json:"roleArn"`
	Status             string                `json:"status"`
	OutputFormat       string                `json:"outputFormat,omitempty"`
}

// cloneLedger returns a deep copy of l with the Tags map cloned.
func cloneLedger(l *Ledger) *Ledger {
	cp := *l
	cp.Tags = maps.Clone(l.Tags)

	return &cp
}

// cloneStream returns a shallow copy of s.
func cloneStream(s *JournalKinesisStream) *JournalKinesisStream {
	cp := *s

	return &cp
}

// cloneExport returns a shallow copy of e.
func cloneExport(e *JournalS3Export) *JournalS3Export {
	cp := *e

	return &cp
}

// InMemoryBackend is an in-memory QLDB backend.
type InMemoryBackend struct {
	ledgers        map[string]*Ledger
	ledgerARNIndex map[string]string                           // ARN → ledger name
	journalStreams map[string]map[string]*JournalKinesisStream // ledger name → stream ID → stream
	s3Exports      map[string]*JournalS3Export                 // export ID → export
	mu             *lockmetrics.RWMutex
	accountID      string
	region         string
}

// NewInMemoryBackend creates a new in-memory QLDB backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		ledgers:        make(map[string]*Ledger),
		ledgerARNIndex: make(map[string]string),
		journalStreams: make(map[string]map[string]*JournalKinesisStream),
		s3Exports:      make(map[string]*JournalS3Export),
		accountID:      accountID,
		region:         region,
		mu:             lockmetrics.New("qldb"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all state, returning the backend to a clean empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.ledgers = make(map[string]*Ledger)
	b.ledgerARNIndex = make(map[string]string)
	b.journalStreams = make(map[string]map[string]*JournalKinesisStream)
	b.s3Exports = make(map[string]*JournalS3Export)
}

// CreateLedger creates a new ledger.
func (b *InMemoryBackend) CreateLedger(
	name, permissionsMode string,
	deletionProtected bool,
	tags map[string]string,
) (*Ledger, error) {
	b.mu.Lock("CreateLedger")
	defer b.mu.Unlock()

	if _, ok := b.ledgers[name]; ok {
		return nil, fmt.Errorf("%w: ledger %s already exists", ErrAlreadyExists, name)
	}

	if permissionsMode == "" {
		permissionsMode = permissionsModeAllowAll
	}

	switch permissionsMode {
	case permissionsModeAllowAll, permissionsModeSuperuser, permissionsModeStandard:
		// valid
	default:
		return nil, fmt.Errorf("%w: invalid permissions mode %q", ErrValidation, permissionsMode)
	}

	ledgerARN := arn.Build("qldb", b.region, b.accountID, "ledger/"+name)
	l := &Ledger{
		Name:              name,
		ARN:               ledgerARN,
		State:             StateActive,
		PermissionsMode:   permissionsMode,
		DeletionProtected: deletionProtected,
		AccountID:         b.accountID,
		Region:            b.region,
		CreationDateTime:  time.Now(),
		Tags:              mergeTags(nil, tags),
	}
	b.ledgers[name] = l
	b.ledgerARNIndex[ledgerARN] = name

	return cloneLedger(l), nil
}

// GetLedger returns a ledger by name.
func (b *InMemoryBackend) GetLedger(name string) (*Ledger, error) {
	b.mu.RLock("GetLedger")
	defer b.mu.RUnlock()

	l, ok := b.ledgers[name]
	if !ok {
		return nil, fmt.Errorf("%w: ledger %s not found", ErrNotFound, name)
	}

	return cloneLedger(l), nil
}

// ListLedgers returns all ledgers.
func (b *InMemoryBackend) ListLedgers() []*Ledger {
	b.mu.RLock("ListLedgers")
	defer b.mu.RUnlock()

	list := make([]*Ledger, 0, len(b.ledgers))
	for _, l := range b.ledgers {
		list = append(list, cloneLedger(l))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	return list
}

// UpdateLedger updates an existing ledger's deletion protection setting.
func (b *InMemoryBackend) UpdateLedger(name string, deletionProtected bool) (*Ledger, error) {
	b.mu.Lock("UpdateLedger")
	defer b.mu.Unlock()

	l, ok := b.ledgers[name]
	if !ok {
		return nil, fmt.Errorf("%w: ledger %s not found", ErrNotFound, name)
	}

	l.DeletionProtected = deletionProtected

	return cloneLedger(l), nil
}

// DeleteLedger deletes a ledger by name.
func (b *InMemoryBackend) DeleteLedger(name string) error {
	b.mu.Lock("DeleteLedger")
	defer b.mu.Unlock()

	l, ok := b.ledgers[name]
	if !ok {
		return fmt.Errorf("%w: ledger %s not found", ErrNotFound, name)
	}

	if l.DeletionProtected {
		return fmt.Errorf("%w: ledger %s has deletion protection enabled", ErrDeletionProtection, name)
	}

	delete(b.ledgers, name)
	delete(b.ledgerARNIndex, l.ARN)
	delete(b.journalStreams, name)

	for id, e := range b.s3Exports {
		if e.LedgerName == name {
			delete(b.s3Exports, id)
		}
	}

	return nil
}

// TagResource adds or updates tags on a ledger identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	name, ok := b.ledgerARNIndex[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	l := b.ledgers[name]
	l.Tags = mergeTags(l.Tags, kv)

	return nil
}

// UntagResource removes specified tag keys from a ledger.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	name, ok := b.ledgerARNIndex[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	l := b.ledgers[name]

	for _, k := range keys {
		delete(l.Tags, k)
	}

	return nil
}

// ListTagsForResource returns tags for a ledger identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	name, ok := b.ledgerARNIndex[resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	l := b.ledgers[name]
	result := make(map[string]string, len(l.Tags))
	maps.Copy(result, l.Tags)

	return result, nil
}

// mergeTags merges new tags into existing ones, returning a new map.
func mergeTags(existing, incoming map[string]string) map[string]string {
	result := make(map[string]string, len(existing)+len(incoming))
	maps.Copy(result, existing)
	maps.Copy(result, incoming)

	return result
}

// newResourceID returns a new random UUID string suitable for use as a resource ID.
func newResourceID() string {
	return uuid.NewString()
}

// AddJournalKinesisStreamInternal seeds a journal kinesis stream directly into the backend
// (bypasses route; used by tests and seed helpers).
func (b *InMemoryBackend) AddJournalKinesisStreamInternal(s *JournalKinesisStream) {
	b.mu.Lock("AddJournalKinesisStreamInternal")
	defer b.mu.Unlock()

	if b.journalStreams[s.LedgerName] == nil {
		b.journalStreams[s.LedgerName] = make(map[string]*JournalKinesisStream)
	}

	b.journalStreams[s.LedgerName][s.StreamID] = s
}

// CancelJournalKinesisStream cancels an active journal kinesis stream.
func (b *InMemoryBackend) CancelJournalKinesisStream(ledgerName, streamID string) (*JournalKinesisStream, error) {
	b.mu.Lock("CancelJournalKinesisStream")
	defer b.mu.Unlock()

	streams, ok := b.journalStreams[ledgerName]
	if !ok {
		return nil, fmt.Errorf("%w: ledger %s not found", ErrNotFound, ledgerName)
	}

	s, ok := streams[streamID]
	if !ok {
		return nil, fmt.Errorf("%w: stream %s not found", ErrNotFound, streamID)
	}

	s.Status = streamStatusCanceled
	cp := cloneStream(s)

	return cp, nil
}

// GetJournalKinesisStream returns a journal kinesis stream by ledger name and stream ID.
func (b *InMemoryBackend) GetJournalKinesisStream(ledgerName, streamID string) (*JournalKinesisStream, error) {
	b.mu.RLock("GetJournalKinesisStream")
	defer b.mu.RUnlock()

	streams, ok := b.journalStreams[ledgerName]
	if !ok {
		return nil, fmt.Errorf("%w: ledger %s not found", ErrNotFound, ledgerName)
	}

	s, ok := streams[streamID]
	if !ok {
		return nil, fmt.Errorf("%w: stream %s not found", ErrNotFound, streamID)
	}

	return cloneStream(s), nil
}

// ListJournalKinesisStreamsForLedger returns all journal kinesis streams for a ledger.
func (b *InMemoryBackend) ListJournalKinesisStreamsForLedger(ledgerName string) ([]*JournalKinesisStream, error) {
	b.mu.RLock("ListJournalKinesisStreamsForLedger")
	defer b.mu.RUnlock()

	if _, ok := b.ledgers[ledgerName]; !ok {
		return nil, fmt.Errorf("%w: ledger %s not found", ErrNotFound, ledgerName)
	}

	streams := b.journalStreams[ledgerName]
	list := make([]*JournalKinesisStream, 0, len(streams))

	for _, s := range streams {
		list = append(list, cloneStream(s))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].StreamName < list[j].StreamName
	})

	return list, nil
}

// ExportJournalToS3 creates a new journal S3 export job.
func (b *InMemoryBackend) ExportJournalToS3(
	ledgerName, roleArn string,
	s3Config S3ExportConfiguration,
	inclusiveStartTime, exclusiveEndTime time.Time,
	outputFormat string,
) (*JournalS3Export, error) {
	b.mu.Lock("ExportJournalToS3")
	defer b.mu.Unlock()

	if _, ok := b.ledgers[ledgerName]; !ok {
		return nil, fmt.Errorf("%w: ledger %s not found", ErrNotFound, ledgerName)
	}

	exportID := newResourceID()
	e := &JournalS3Export{
		ExportID:           exportID,
		LedgerName:         ledgerName,
		RoleArn:            roleArn,
		S3Config:           s3Config,
		InclusiveStartTime: inclusiveStartTime,
		ExclusiveEndTime:   exclusiveEndTime,
		ExportCreationTime: time.Now(),
		Status:             exportStatusInProgress,
		OutputFormat:       outputFormat,
	}

	b.s3Exports[exportID] = e

	return cloneExport(e), nil
}

// GetJournalS3Export returns a journal S3 export by ledger name and export ID.
func (b *InMemoryBackend) GetJournalS3Export(ledgerName, exportID string) (*JournalS3Export, error) {
	b.mu.RLock("GetJournalS3Export")
	defer b.mu.RUnlock()

	e, ok := b.s3Exports[exportID]
	if !ok {
		return nil, fmt.Errorf("%w: export %s not found", ErrNotFound, exportID)
	}

	if e.LedgerName != ledgerName {
		return nil, fmt.Errorf("%w: export %s does not belong to ledger %s", ErrNotFound, exportID, ledgerName)
	}

	return cloneExport(e), nil
}

// ListJournalS3Exports returns all journal S3 export jobs across all ledgers.
func (b *InMemoryBackend) ListJournalS3Exports() []*JournalS3Export {
	b.mu.RLock("ListJournalS3Exports")
	defer b.mu.RUnlock()

	list := make([]*JournalS3Export, 0, len(b.s3Exports))

	for _, e := range b.s3Exports {
		list = append(list, cloneExport(e))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].ExportID < list[j].ExportID
	})

	return list
}

// ListJournalS3ExportsForLedger returns all journal S3 export jobs for a ledger.
func (b *InMemoryBackend) ListJournalS3ExportsForLedger(ledgerName string) ([]*JournalS3Export, error) {
	b.mu.RLock("ListJournalS3ExportsForLedger")
	defer b.mu.RUnlock()

	if _, ok := b.ledgers[ledgerName]; !ok {
		return nil, fmt.Errorf("%w: ledger %s not found", ErrNotFound, ledgerName)
	}

	list := make([]*JournalS3Export, 0, len(b.s3Exports))

	for _, e := range b.s3Exports {
		if e.LedgerName == ledgerName {
			list = append(list, cloneExport(e))
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].ExportID < list[j].ExportID
	})

	return list, nil
}

// GetBlock returns a synthetic block for a ledger (in-memory mock).
// The real AWS operation verifies journal integrity; this returns a stub response.
func (b *InMemoryBackend) GetBlock(name string) (string, string, error) {
	b.mu.RLock("GetBlock")
	defer b.mu.RUnlock()

	if _, ok := b.ledgers[name]; !ok {
		return "", "", fmt.Errorf("%w: ledger %s not found", ErrNotFound, name)
	}

	return syntheticBlockIonText, syntheticDigestTipIonText, nil
}

// GetDigest returns a synthetic digest for a ledger (in-memory mock).
// The real AWS operation returns a cryptographic hash; this returns a stub response.
func (b *InMemoryBackend) GetDigest(name string) ([]byte, string, error) {
	b.mu.RLock("GetDigest")
	defer b.mu.RUnlock()

	if _, ok := b.ledgers[name]; !ok {
		return nil, "", fmt.Errorf("%w: ledger %s not found", ErrNotFound, name)
	}

	return make([]byte, syntheticDigestLen), syntheticDigestTipIonText, nil
}

// GetRevision returns a synthetic revision for a ledger (in-memory mock).
// The real AWS operation verifies document revision integrity; this returns a stub response.
func (b *InMemoryBackend) GetRevision(name string) (string, string, error) {
	b.mu.RLock("GetRevision")
	defer b.mu.RUnlock()

	if _, ok := b.ledgers[name]; !ok {
		return "", "", fmt.Errorf("%w: ledger %s not found", ErrNotFound, name)
	}

	return syntheticRevisionIonText, syntheticDigestTipIonText, nil
}
