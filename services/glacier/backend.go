package glacier

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"maps"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Sentinel errors for Glacier backend operations.
var (
	// ErrVaultNotFound is returned when a vault does not exist.
	ErrVaultNotFound = errors.New("ResourceNotFoundException: Vault not found")
	// ErrArchiveNotFound is returned when an archive does not exist.
	ErrArchiveNotFound = errors.New("ResourceNotFoundException: Archive not found")
	// ErrJobNotFound is returned when a job does not exist.
	ErrJobNotFound = errors.New("ResourceNotFoundException: Job not found")
	// ErrUploadNotFound is returned when a multipart upload does not exist.
	ErrUploadNotFound = errors.New("ResourceNotFoundException: Multipart upload not found")
	// ErrValidation is returned when an invalid parameter is supplied.
	ErrValidation = errors.New("InvalidParameterValueException: invalid parameter")
	// ErrVaultNotEmpty is returned when deleting a vault that still has archives.
	ErrVaultNotEmpty = errors.New("InvalidParameterValueException: Vault not empty")
	// ErrLockConflict is returned when a vault lock is already in progress.
	ErrLockConflict = errors.New("InvalidParameterValueException: Vault lock already in progress")
	// ErrLockAlreadyLocked is returned when attempting to initiate a lock on an already-locked vault.
	ErrLockAlreadyLocked = errors.New("InvalidParameterValueException: Vault is already locked")
	// ErrTooManyTags is returned when adding tags would exceed the per-vault limit.
	ErrTooManyTags = errors.New("InvalidParameterValueException: too many tags on vault")
	// ErrProvisionedCapacityLimit is returned when trying to purchase more than 2 capacity units.
	ErrProvisionedCapacityLimit = errors.New("LimitExceededException: maximum 2 provisioned capacity units per account")
	// ErrInvalidTag is returned when a tag key or value fails validation.
	ErrInvalidTag = errors.New("InvalidParameterValueException: invalid tag key or value")
)

const (
	lockStateInProgress = "InProgress"
	lockStateLocked     = "Locked"
	lockStateUnlocked   = "Unlocked"

	// jobStatusInProgress and jobStatusSucceeded are the retrieval-job status codes.
	jobStatusInProgress = "InProgress"
	jobStatusSucceeded  = "Succeeded"

	// archiveIDLength is the length of the random archive ID suffix.
	archiveIDLength = 60
	// jobIDLength is the length of the random job ID.
	jobIDLength = 60
	// multipartUploadIDLength is the length of the random multipart upload ID.
	multipartUploadIDLength = 60
	// capacityIDLength is the length of the generated capacity unit ID.
	capacityIDLength = 32
	// idChars are the characters used for generating random IDs.
	idChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	// jobTypeArchiveRetrieval is the Glacier job action name for archive retrieval (response).
	jobTypeArchiveRetrieval = "ArchiveRetrieval"
	// jobTypeInventoryRetrieval is the Glacier job action name for inventory retrieval (response).
	jobTypeInventoryRetrieval = "InventoryRetrieval"
	// jobInputArchiveRetrieval is the type value sent by SDK/clients for archive retrieval (request).
	jobInputArchiveRetrieval = "archive-retrieval"
	// jobInputInventoryRetrieval is the type value sent by SDK/clients for inventory retrieval (request).
	jobInputInventoryRetrieval = "inventory-retrieval"
	// maxVaultTags is the maximum number of tags allowed on a single vault.
	maxVaultTags = 10
	// maxTagKeyLen is the maximum byte length of a tag key.
	maxTagKeyLen = 128
	// maxTagValueLen is the maximum byte length of a tag value.
	maxTagValueLen = 256
	// minMultipartPartSize is the minimum part size for multipart uploads (1 MiB).
	minMultipartPartSize = 1 << 20
	// maxMultipartPartSize is the maximum part size for multipart uploads (4 GiB).
	maxMultipartPartSize = 4 << 30
	// vaultLockExpirationHours is the number of hours before an InProgress vault lock expires.
	vaultLockExpirationHours = 24
	// maxProvisionedCapacityUnits is the per-account cap on provisioned capacity units.
	maxProvisionedCapacityUnits = 2
	// provisionedCapacityMonths is the number of months a purchased capacity unit is active.
	provisionedCapacityMonths = 1
)

// StorageBackend is the interface for the Glacier backend.
type StorageBackend interface {
	CreateVault(accountID, region, vaultName string) (*Vault, error)
	DescribeVault(accountID, region, vaultName string) (*Vault, error)
	DeleteVault(accountID, region, vaultName string) error
	ListVaults(accountID, region string) []*Vault

	UploadArchive(accountID, region, vaultName, description, checksum string, size int64) (*Archive, error)
	DeleteArchive(accountID, region, vaultName, archiveID string) error
	ListArchives(accountID, region, vaultName string) ([]*Archive, error)

	InitiateJob(accountID, region, vaultName string, req *initiateJobRequest) (*Job, error)
	DescribeJob(accountID, region, vaultName, jobID string) (*Job, error)
	ListJobs(accountID, region, vaultName string) ([]*Job, error)

	SetVaultNotifications(accountID, region, vaultName, snsTopic string, events []string) error
	GetVaultNotifications(accountID, region, vaultName string) (string, []string, error)
	DeleteVaultNotifications(accountID, region, vaultName string) error

	SetVaultAccessPolicy(accountID, region, vaultName, policy string) error
	GetVaultAccessPolicy(accountID, region, vaultName string) (string, error)
	DeleteVaultAccessPolicy(accountID, region, vaultName string) error

	AddTagsToVault(accountID, region, vaultName string, tags map[string]string) error
	ListTagsForVault(accountID, region, vaultName string) (map[string]string, error)
	RemoveTagsFromVault(accountID, region, vaultName string, tagKeys []string) error

	// Multipart upload operations.
	InitiateMultipartUpload(accountID, region, vaultName, description string, partSize int64) (*MultipartUpload, error)
	UploadMultipartPart(accountID, region, vaultName, uploadID, rangeHeader, checksum string) error
	CompleteMultipartUpload(
		accountID, region, vaultName, uploadID, checksum string,
		archiveSize int64,
	) (*Archive, error)
	AbortMultipartUpload(accountID, region, vaultName, uploadID string) error
	ListMultipartUploads(accountID, region, vaultName string) []*MultipartUpload
	ListParts(accountID, region, vaultName, uploadID string) (*ListPartsOutput, error)

	// Vault lock operations.
	GetVaultLock(accountID, region, vaultName string) (*VaultLock, error)
	SetVaultLock(accountID, region, vaultName, policy, lockID string) error
	AbortVaultLock(accountID, region, vaultName string) error
	CompleteVaultLock(accountID, region, vaultName, lockID string) error

	// Data retrieval policy operations.
	GetDataRetrievalPolicy(accountID string) string
	SetDataRetrievalPolicy(accountID string, policy []byte)

	// Provisioned capacity operations.
	ListProvisionedCapacity(accountID string) []*ProvisionedCapacity
	PurchaseProvisionedCapacity(accountID string) (*ProvisionedCapacity, error)

	Reset()
}

var _ StorageBackend = (*InMemoryBackend)(nil)

// vaultKey uniquely identifies a vault within an account and region.
type vaultKey struct {
	AccountID string `json:"accountID"`
	Region    string `json:"region"`
	VaultName string `json:"vaultName"`
}

// uploadKey uniquely identifies a multipart upload.
type uploadKey struct {
	AccountID string `json:"accountID"`
	Region    string `json:"region"`
	VaultName string `json:"vaultName"`
	UploadID  string `json:"uploadID"`
}

// InMemoryBackend is the in-memory backend for Glacier.
type InMemoryBackend struct {
	vaults                map[vaultKey]*Vault
	archives              map[vaultKey]map[string]*Archive
	jobs                  map[vaultKey]map[string]*Job
	multipartUploads      map[vaultKey]map[string]*MultipartUpload
	multipartParts        map[uploadKey][]MultipartPart
	vaultLocks            map[vaultKey]*VaultLock
	provisionedCapacity   map[string][]*ProvisionedCapacity
	dataRetrievalPolicies map[string]string
	// vaultsByAccountRegion indexes vault names by accountID+region for O(1) ListVaults
	// instead of a full scan of all vaults across every account and region.
	vaultsByAccountRegion map[string]map[string]map[string]struct{} // accountID -> region -> vaultName -> {}
	// retrievalDelay is the simulated asynchronous retrieval window applied to newly
	// initiated jobs. Jobs stay InProgress until CreationDate+retrievalDelay, matching
	// AWS, which does not make archive/inventory output available immediately.
	retrievalDelay time.Duration
	mu             sync.RWMutex
}

// NewInMemoryBackend creates a new in-memory Glacier backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		vaults:                make(map[vaultKey]*Vault),
		archives:              make(map[vaultKey]map[string]*Archive),
		jobs:                  make(map[vaultKey]map[string]*Job),
		multipartUploads:      make(map[vaultKey]map[string]*MultipartUpload),
		multipartParts:        make(map[uploadKey][]MultipartPart),
		vaultLocks:            make(map[vaultKey]*VaultLock),
		provisionedCapacity:   make(map[string][]*ProvisionedCapacity),
		dataRetrievalPolicies: make(map[string]string),
		vaultsByAccountRegion: make(map[string]map[string]map[string]struct{}),
		retrievalDelay:        defaultRetrievalDelay,
	}
}

// defaultRetrievalDelay is the simulated asynchronous retrieval window applied to
// newly initiated jobs. Kept short so callers and tests are not forced to wait a
// realistic multi-hour window, while still exercising the InProgress -> Succeeded
// lifecycle (real AWS Standard retrievals take 3-5 hours).
const defaultRetrievalDelay = 100 * time.Millisecond

// cloneVault returns a deep copy of the vault with Tags and NotificationEvents cloned.
func cloneVault(v *Vault) *Vault {
	cp := *v
	cp.Tags = maps.Clone(v.Tags)
	cp.NotificationEvents = append([]string(nil), v.NotificationEvents...)

	return &cp
}

// cloneJob returns a shallow copy of a Job.
func cloneJob(j *Job) *Job {
	cp := *j

	return &cp
}

// cloneArchive returns a shallow copy of an Archive.
func cloneArchive(a *Archive) *Archive {
	cp := *a

	return &cp
}

// generateID creates a random ID of the given length using a single batch read
// from crypto/rand rather than one syscall per character.
func generateID(length int) string {
	const nChars = len(idChars)
	const byteRange = 256 // number of distinct byte values
	// Bytes in [0, nChars*(byteRange/nChars)) have no modulo bias.
	const maxByte = byte(nChars * (byteRange / nChars))
	const bufHeadroom = 8 // extra headroom for rejected bytes

	result := make([]byte, 0, length)
	buf := make([]byte, length+length/2+bufHeadroom) // extra headroom for rejections

	for len(result) < length {
		if _, err := io.ReadFull(rand.Reader, buf); err != nil {
			// Unreachable in practice; degrade to index 0 for remaining chars.
			for len(result) < length {
				result = append(result, idChars[0])
			}

			break
		}

		for _, b := range buf {
			if b < maxByte {
				result = append(result, idChars[int(b)%nChars])
				if len(result) == length {
					break
				}
			}
		}
	}

	return string(result)
}

// vaultARN returns the ARN for a Glacier vault.
func vaultARN(accountID, region, vaultName string) string {
	return fmt.Sprintf("arn:aws:glacier:%s:%s:vaults/%s", region, accountID, vaultName)
}

// vaultLocation returns the location path for a vault creation response.
func vaultLocation(accountID, vaultName string) string {
	return fmt.Sprintf("/%s/vaults/%s", accountID, vaultName)
}

// normalizeJobType converts the SDK request type (kebab-case) to the canonical
// action name (PascalCase) used in job responses. Returns empty string if unknown.
func normalizeJobType(t string) string {
	switch t {
	case jobTypeArchiveRetrieval, jobInputArchiveRetrieval:
		return jobTypeArchiveRetrieval
	case jobTypeInventoryRetrieval, jobInputInventoryRetrieval:
		return jobTypeInventoryRetrieval
	default:
		return ""
	}
}

// CreateVault creates a new Glacier vault.
func (b *InMemoryBackend) CreateVault(accountID, region, vaultName string) (*Vault, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if vaultName == "" {
		return nil, ErrValidation
	}

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if _, ok := b.vaults[key]; ok {
		return b.vaults[key], nil
	}

	v := &Vault{
		VaultName:    vaultName,
		VaultARN:     vaultARN(accountID, region, vaultName),
		CreationDate: formatDate(time.Now()),
		Tags:         make(map[string]string),
	}
	b.vaults[key] = v
	b.archives[key] = make(map[string]*Archive)
	b.jobs[key] = make(map[string]*Job)
	b.multipartUploads[key] = make(map[string]*MultipartUpload)

	if b.vaultsByAccountRegion[accountID] == nil {
		b.vaultsByAccountRegion[accountID] = make(map[string]map[string]struct{})
	}
	if b.vaultsByAccountRegion[accountID][region] == nil {
		b.vaultsByAccountRegion[accountID][region] = make(map[string]struct{})
	}
	b.vaultsByAccountRegion[accountID][region][vaultName] = struct{}{}

	return v, nil
}

// DescribeVault returns vault metadata.
func (b *InMemoryBackend) DescribeVault(accountID, region, vaultName string) (*Vault, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}
	v, ok := b.vaults[key]

	if !ok {
		return nil, ErrVaultNotFound
	}

	return cloneVault(v), nil
}

// DeleteVault deletes a vault.
func (b *InMemoryBackend) DeleteVault(accountID, region, vaultName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if _, ok := b.vaults[key]; !ok {
		return ErrVaultNotFound
	}

	if len(b.archives[key]) > 0 {
		return ErrVaultNotEmpty
	}

	delete(b.vaults, key)
	delete(b.archives, key)
	delete(b.jobs, key)
	delete(b.multipartUploads, key)
	delete(b.vaultLocks, key)

	if regionMap, ok := b.vaultsByAccountRegion[accountID]; ok {
		delete(regionMap[region], vaultName)
	}

	return nil
}

// sortedVaultNames returns a sorted copy of the vaults slice ordered by VaultName.
func sortedVaultNames(vaults []*Vault) []*Vault {
	sorted := make([]*Vault, len(vaults))
	copy(sorted, vaults)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].VaultName < sorted[j].VaultName
	})

	return sorted
}

// ListVaults returns all vaults for the given account and region.
func (b *InMemoryBackend) ListVaults(accountID, region string) []*Vault {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := b.vaultsByAccountRegion[accountID][region]
	result := make([]*Vault, 0, len(names))

	for name := range names {
		key := vaultKey{AccountID: accountID, Region: region, VaultName: name}
		if v, ok := b.vaults[key]; ok {
			result = append(result, cloneVault(v))
		}
	}

	return sortedVaultNames(result)
}

// UploadArchive uploads an archive to a vault.
func (b *InMemoryBackend) UploadArchive(
	accountID, region, vaultName, description, checksum string,
	size int64,
) (*Archive, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if _, ok := b.vaults[key]; !ok {
		return nil, ErrVaultNotFound
	}

	archiveID := generateID(archiveIDLength)
	a := &Archive{
		ArchiveID:      archiveID,
		Description:    description,
		CreationDate:   formatDate(time.Now()),
		Size:           size,
		SHA256TreeHash: checksum,
	}

	b.archives[key][archiveID] = a
	b.vaults[key].NumberOfArchives++
	b.vaults[key].SizeInBytes += size

	return a, nil
}

// DeleteArchive deletes an archive from a vault.
func (b *InMemoryBackend) DeleteArchive(accountID, region, vaultName, archiveID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if _, ok := b.vaults[key]; !ok {
		return ErrVaultNotFound
	}

	a, ok := b.archives[key][archiveID]
	if !ok {
		return ErrArchiveNotFound
	}

	if b.vaults[key].NumberOfArchives > 0 {
		b.vaults[key].NumberOfArchives--
	}

	if b.vaults[key].SizeInBytes >= a.Size {
		b.vaults[key].SizeInBytes -= a.Size
	} else {
		b.vaults[key].SizeInBytes = 0
	}

	delete(b.archives[key], archiveID)

	return nil
}

// ListArchives returns all archives for the given vault.
func (b *InMemoryBackend) ListArchives(accountID, region, vaultName string) ([]*Archive, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if _, ok := b.vaults[key]; !ok {
		return nil, ErrVaultNotFound
	}

	archives := b.archives[key]
	result := make([]*Archive, 0, len(archives))

	for _, a := range archives {
		result = append(result, cloneArchive(a))
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ArchiveID < result[j].ArchiveID })

	return result, nil
}

// isValidTier reports whether tier is one of the allowed retrieval tier values.
func isValidTier(tier string) bool {
	return tier == "Bulk" || tier == "Standard" || tier == "Expedited"
}

// InitiateJob creates a new retrieval or inventory job.
func (b *InMemoryBackend) InitiateJob(accountID, region, vaultName string, req *initiateJobRequest) (*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Normalize the job type: the SDK sends kebab-case ("archive-retrieval",
	// "inventory-retrieval") but the job action is stored in PascalCase
	// ("ArchiveRetrieval", "InventoryRetrieval") per the AWS API spec.
	action := normalizeJobType(req.Type)
	if action == "" {
		return nil, ErrValidation
	}

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	v, vaultExists := b.vaults[key]
	if !vaultExists {
		return nil, ErrVaultNotFound
	}

	if action == jobTypeArchiveRetrieval {
		if req.ArchiveID == "" {
			return nil, ErrValidation
		}

		if _, archiveExists := b.archives[key][req.ArchiveID]; !archiveExists {
			return nil, ErrArchiveNotFound
		}
	}

	tier := req.Tier
	if tier == "" {
		tier = "Standard"
	}

	if !isValidTier(tier) {
		return nil, fmt.Errorf("%w: tier must be Bulk, Standard, or Expedited", ErrValidation)
	}

	inventoryFormat := req.InventoryFormat
	if inventoryFormat == "" {
		inventoryFormat = "JSON"
	}

	now := time.Now()

	// AWS retrievals are asynchronous: a freshly initiated job starts InProgress and
	// only completes after a retrieval window elapses. We simulate that window with
	// retrievalDelay; reads promote the job to Succeeded once it has passed. A zero
	// delay means jobs complete immediately.
	readyAt := now.Add(b.retrievalDelay)
	ready := !now.Before(readyAt)

	statusCode := jobStatusInProgress
	if ready {
		statusCode = jobStatusSucceeded
	}

	j := &Job{
		JobID:              generateID(jobIDLength),
		VaultARN:           v.VaultARN,
		VaultName:          vaultName,
		Action:             action,
		ArchiveID:          req.ArchiveID,
		InventoryFormat:    inventoryFormat,
		JobDescription:     req.Description,
		StatusCode:         statusCode,
		StatusMessage:      statusCode,
		CreationDate:       formatDate(now),
		Completed:          ready,
		Tier:               tier,
		SNSTopic:           req.SNSTopic,
		RetrievalByteRange: req.RetrievalByteRange,
		readyAt:            readyAt,
	}

	if ready {
		j.CompletionDate = formatDate(now)
	}

	if action == jobTypeArchiveRetrieval {
		if a, archiveFound := b.archives[key][req.ArchiveID]; archiveFound {
			j.ArchiveSizeInBytes = a.Size
			j.SHA256TreeHash = a.SHA256TreeHash
		}
	}

	if action == jobTypeInventoryRetrieval {
		b.vaults[key].LastInventoryDate = formatDate(now)
	}

	b.jobs[key][j.JobID] = j

	return j, nil
}

// DescribeJob returns metadata for a job.
func (b *InMemoryBackend) DescribeJob(accountID, region, vaultName, jobID string) (*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if _, ok := b.vaults[key]; !ok {
		return nil, ErrVaultNotFound
	}

	j, ok := b.jobs[key][jobID]
	if !ok {
		return nil, ErrJobNotFound
	}

	promoteJobIfReady(j)

	return cloneJob(j), nil
}

// promoteJobIfReady transitions an InProgress retrieval job to Succeeded once its
// simulated retrieval window (readyAt) has elapsed. Caller must hold b.mu for writing.
func promoteJobIfReady(j *Job) {
	if j.Completed {
		return
	}

	if j.readyAt.IsZero() || time.Now().Before(j.readyAt) {
		return
	}

	j.Completed = true
	j.StatusCode = jobStatusSucceeded
	j.StatusMessage = jobStatusSucceeded
	j.CompletionDate = formatDate(time.Now())
}

// ListJobs returns all jobs for the given vault.
// Returns ErrVaultNotFound if the vault does not exist.
func (b *InMemoryBackend) ListJobs(accountID, region, vaultName string) ([]*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if _, ok := b.vaults[key]; !ok {
		return nil, ErrVaultNotFound
	}

	jobs := b.jobs[key]
	if jobs == nil {
		return []*Job{}, nil
	}

	result := make([]*Job, 0, len(jobs))

	for _, j := range jobs {
		promoteJobIfReady(j)
		result = append(result, cloneJob(j))
	}

	sort.Slice(result, func(i, j int) bool { return result[i].JobID < result[j].JobID })

	return result, nil
}

// isValidNotificationEvent reports whether ev is an allowed vault notification event.
func isValidNotificationEvent(ev string) bool {
	return ev == "ArchiveRetrievalCompleted" || ev == "InventoryRetrievalCompleted"
}

// SetVaultNotifications sets the notification configuration for a vault.
func (b *InMemoryBackend) SetVaultNotifications(accountID, region, vaultName, snsTopic string, events []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	v, ok := b.vaults[key]
	if !ok {
		return ErrVaultNotFound
	}

	for _, ev := range events {
		if !isValidNotificationEvent(ev) {
			return fmt.Errorf(
				"%w: event %q must be ArchiveRetrievalCompleted or InventoryRetrievalCompleted",
				ErrValidation, ev,
			)
		}
	}

	v.NotificationSNSTopic = snsTopic
	v.NotificationEvents = append([]string(nil), events...)

	return nil
}

// GetVaultNotifications returns the notification configuration for a vault.
func (b *InMemoryBackend) GetVaultNotifications(accountID, region, vaultName string) (string, []string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	v, ok := b.vaults[key]
	if !ok {
		return "", nil, ErrVaultNotFound
	}

	return v.NotificationSNSTopic, v.NotificationEvents, nil
}

// DeleteVaultNotifications deletes the notification configuration for a vault.
func (b *InMemoryBackend) DeleteVaultNotifications(accountID, region, vaultName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	v, ok := b.vaults[key]
	if !ok {
		return ErrVaultNotFound
	}

	v.NotificationSNSTopic = ""
	v.NotificationEvents = nil

	return nil
}

// SetVaultAccessPolicy sets the access policy for a vault.
func (b *InMemoryBackend) SetVaultAccessPolicy(accountID, region, vaultName, policy string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	v, ok := b.vaults[key]
	if !ok {
		return ErrVaultNotFound
	}

	v.AccessPolicy = policy

	return nil
}

// GetVaultAccessPolicy returns the access policy for a vault.
func (b *InMemoryBackend) GetVaultAccessPolicy(accountID, region, vaultName string) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	v, ok := b.vaults[key]
	if !ok {
		return "", ErrVaultNotFound
	}

	return v.AccessPolicy, nil
}

// DeleteVaultAccessPolicy deletes the access policy for a vault.
func (b *InMemoryBackend) DeleteVaultAccessPolicy(accountID, region, vaultName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	v, ok := b.vaults[key]
	if !ok {
		return ErrVaultNotFound
	}

	v.AccessPolicy = ""

	return nil
}

// isValidTagChar reports whether b is an allowed tag character.
// AWS allows Unicode letters (L), spaces (Z), numbers (N), and _.:/=+\-@.
// For the ASCII subset used in practice this check is sufficient.
func isValidTagChar(b byte) bool {
	if b >= 'a' && b <= 'z' {
		return true
	}

	if b >= 'A' && b <= 'Z' {
		return true
	}

	if b >= '0' && b <= '9' {
		return true
	}

	switch b {
	case ' ', '_', '.', ':', '/', '=', '+', '-', '@':
		return true
	}

	return false
}

// validateTagKey returns an error if the tag key is invalid.
func validateTagKey(k string) error {
	if len(k) == 0 || len(k) > maxTagKeyLen {
		return fmt.Errorf("%w: tag key length must be 1-%d", ErrInvalidTag, maxTagKeyLen)
	}

	if strings.HasPrefix(k, "aws:") {
		return fmt.Errorf("%w: tag key prefix \"aws:\" is reserved", ErrInvalidTag)
	}

	for i := range len(k) {
		if !isValidTagChar(k[i]) {
			return fmt.Errorf("%w: tag key contains invalid character 0x%02x", ErrInvalidTag, k[i])
		}
	}

	return nil
}

// validateTagValue returns an error if the tag value is invalid.
func validateTagValue(v string) error {
	if len(v) > maxTagValueLen {
		return fmt.Errorf("%w: tag value exceeds %d characters", ErrInvalidTag, maxTagValueLen)
	}

	for i := range len(v) {
		if !isValidTagChar(v[i]) {
			return fmt.Errorf("%w: tag value contains invalid character 0x%02x", ErrInvalidTag, v[i])
		}
	}

	return nil
}

// AddTagsToVault adds or updates tags on a vault.
func (b *InMemoryBackend) AddTagsToVault(accountID, region, vaultName string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	v, ok := b.vaults[key]
	if !ok {
		return ErrVaultNotFound
	}

	// Validate individual key/value chars, length, and reserved prefix before applying.
	for k, val := range tags {
		if err := validateTagKey(k); err != nil {
			return err
		}

		if err := validateTagValue(val); err != nil {
			return err
		}
	}

	if v.Tags == nil {
		v.Tags = make(map[string]string)
	}

	// Check that adding these tags would not exceed the per-vault limit.
	merged := len(v.Tags)

	for k := range tags {
		if _, exists := v.Tags[k]; !exists {
			merged++
		}
	}

	if merged > maxVaultTags {
		return ErrTooManyTags
	}

	maps.Copy(v.Tags, tags)

	return nil
}

// ListTagsForVault returns all tags for a vault.
func (b *InMemoryBackend) ListTagsForVault(accountID, region, vaultName string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	v, ok := b.vaults[key]
	if !ok {
		return nil, ErrVaultNotFound
	}

	result := make(map[string]string, len(v.Tags))

	maps.Copy(result, v.Tags)

	return result, nil
}

// RemoveTagsFromVault removes tags from a vault.
func (b *InMemoryBackend) RemoveTagsFromVault(accountID, region, vaultName string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	v, ok := b.vaults[key]
	if !ok {
		return ErrVaultNotFound
	}

	for _, k := range tagKeys {
		delete(v.Tags, k)
	}

	return nil
}

// Reset clears all backend state, resetting to an empty store.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.vaults = make(map[vaultKey]*Vault)
	b.archives = make(map[vaultKey]map[string]*Archive)
	b.jobs = make(map[vaultKey]map[string]*Job)
	b.multipartUploads = make(map[vaultKey]map[string]*MultipartUpload)
	b.multipartParts = make(map[uploadKey][]MultipartPart)
	b.vaultLocks = make(map[vaultKey]*VaultLock)
	b.provisionedCapacity = make(map[string][]*ProvisionedCapacity)
	b.dataRetrievalPolicies = make(map[string]string)
	b.vaultsByAccountRegion = make(map[string]map[string]map[string]struct{})
}

// ----------------------------------------
// Multipart upload operations
// ----------------------------------------

// isPowerOfTwo reports whether n is a power of two (n > 0).
func isPowerOfTwo(n int64) bool {
	return n > 0 && (n&(n-1)) == 0
}

// InitiateMultipartUpload begins a multipart upload for a vault.
func (b *InMemoryBackend) InitiateMultipartUpload(
	accountID, region, vaultName, description string,
	partSize int64,
) (*MultipartUpload, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	v, ok := b.vaults[key]
	if !ok {
		return nil, ErrVaultNotFound
	}

	// Part size must be a power of 2 between 1 MiB and 4 GiB (inclusive).
	if partSize != 0 &&
		(!isPowerOfTwo(partSize) || partSize < minMultipartPartSize || partSize > maxMultipartPartSize) {
		return nil, ErrValidation
	}

	uploadID := generateID(multipartUploadIDLength)
	up := &MultipartUpload{
		MultipartUploadID:  uploadID,
		VaultARN:           v.VaultARN,
		ArchiveDescription: description,
		PartSizeInBytes:    partSize,
		CreationDate:       formatDate(time.Now()),
	}

	if b.multipartUploads[key] == nil {
		b.multipartUploads[key] = make(map[string]*MultipartUpload)
	}

	b.multipartUploads[key][uploadID] = up

	return up, nil
}

// UploadMultipartPart records a part for an in-progress multipart upload.
func (b *InMemoryBackend) UploadMultipartPart(
	accountID, region, vaultName, uploadID, rangeHeader, checksum string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if _, ok := b.vaults[key]; !ok {
		return ErrVaultNotFound
	}

	if b.multipartUploads[key] == nil || b.multipartUploads[key][uploadID] == nil {
		return ErrUploadNotFound
	}

	uKey := uploadKey{AccountID: accountID, Region: region, VaultName: vaultName, UploadID: uploadID}
	b.multipartParts[uKey] = append(b.multipartParts[uKey], MultipartPart{
		RangeInBytes:   rangeHeader,
		SHA256TreeHash: checksum,
	})

	return nil
}

// CompleteMultipartUpload finalises a multipart upload and creates an archive.
func (b *InMemoryBackend) CompleteMultipartUpload(
	accountID, region, vaultName, uploadID, checksum string,
	archiveSize int64,
) (*Archive, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if _, ok := b.vaults[key]; !ok {
		return nil, ErrVaultNotFound
	}

	if b.multipartUploads[key] == nil || b.multipartUploads[key][uploadID] == nil {
		return nil, ErrUploadNotFound
	}

	up := b.multipartUploads[key][uploadID]
	archiveID := generateID(archiveIDLength)
	a := &Archive{
		ArchiveID:      archiveID,
		Description:    up.ArchiveDescription,
		CreationDate:   formatDate(time.Now()),
		Size:           archiveSize,
		SHA256TreeHash: checksum,
	}

	b.archives[key][archiveID] = a
	b.vaults[key].NumberOfArchives++
	b.vaults[key].SizeInBytes += archiveSize

	uKey := uploadKey{AccountID: accountID, Region: region, VaultName: vaultName, UploadID: uploadID}
	delete(b.multipartUploads[key], uploadID)
	delete(b.multipartParts, uKey)

	return a, nil
}

// AbortMultipartUpload cancels an in-progress multipart upload.
func (b *InMemoryBackend) AbortMultipartUpload(accountID, region, vaultName, uploadID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if _, ok := b.vaults[key]; !ok {
		return ErrVaultNotFound
	}

	if b.multipartUploads[key] == nil || b.multipartUploads[key][uploadID] == nil {
		return ErrUploadNotFound
	}

	uKey := uploadKey{AccountID: accountID, Region: region, VaultName: vaultName, UploadID: uploadID}
	delete(b.multipartUploads[key], uploadID)
	delete(b.multipartParts, uKey)

	return nil
}

// ListMultipartUploads returns all in-progress multipart uploads for a vault.
func (b *InMemoryBackend) ListMultipartUploads(accountID, region, vaultName string) []*MultipartUpload {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}
	ups := b.multipartUploads[key]

	if ups == nil {
		return []*MultipartUpload{}
	}

	result := make([]*MultipartUpload, 0, len(ups))
	for _, up := range ups {
		cp := *up
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].MultipartUploadID < result[j].MultipartUploadID
	})

	return result
}

// ListParts returns the parts for an in-progress multipart upload.
func (b *InMemoryBackend) ListParts(
	accountID, region, vaultName, uploadID string,
) (*ListPartsOutput, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if _, ok := b.vaults[key]; !ok {
		return nil, ErrVaultNotFound
	}

	up, ok := b.multipartUploads[key][uploadID]
	if !ok {
		return nil, ErrUploadNotFound
	}

	uKey := uploadKey{AccountID: accountID, Region: region, VaultName: vaultName, UploadID: uploadID}
	stored := b.multipartParts[uKey]
	parts := make([]MultipartPart, len(stored))
	copy(parts, stored)

	// Sort parts by their byte-range start value for deterministic output.
	sort.Slice(parts, func(i, j int) bool {
		return rangeStart(parts[i].RangeInBytes) < rangeStart(parts[j].RangeInBytes)
	})

	return &ListPartsOutput{
		MultipartUploadID:  uploadID,
		VaultARN:           up.VaultARN,
		ArchiveDescription: up.ArchiveDescription,
		PartSizeInBytes:    up.PartSizeInBytes,
		CreationDate:       up.CreationDate,
		Parts:              parts,
	}, nil
}

// rangeStart parses the byte start from a Content-Range header value (e.g. "0-1048575/*").
// Returns 0 on parse failure to maintain stable sort behaviour.
func rangeStart(rangeHeader string) int64 {
	for i := range len(rangeHeader) {
		if rangeHeader[i] == '-' || rangeHeader[i] == '/' {
			n := int64(0)
			for j := range i {
				if rangeHeader[j] < '0' || rangeHeader[j] > '9' {
					return 0
				}

				n = n*10 + int64(rangeHeader[j]-'0') //nolint:mnd // decimal digit extraction
			}

			return n
		}
	}

	return 0
}

// ----------------------------------------
// Vault lock operations
// ----------------------------------------

// expireLockIfStale removes an InProgress vault lock that has passed its 24-hour window.
// Caller must hold b.mu.
func (b *InMemoryBackend) expireLockIfStale(key vaultKey) {
	lock, ok := b.vaultLocks[key]
	if !ok || lock.State != lockStateInProgress {
		return
	}

	exp, err := time.Parse("2006-01-02T15:04:05.000Z", lock.ExpirationDate)
	if err == nil && time.Now().UTC().After(exp) {
		delete(b.vaultLocks, key)
	}
}

// GetVaultLock returns the vault lock state.  If no lock has been initiated,
// the returned VaultLock has State "Unlocked".
func (b *InMemoryBackend) GetVaultLock(accountID, region, vaultName string) (*VaultLock, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if _, ok := b.vaults[key]; !ok {
		return nil, ErrVaultNotFound
	}

	b.expireLockIfStale(key)

	lock, ok := b.vaultLocks[key]
	if !ok {
		return &VaultLock{State: lockStateUnlocked}, nil
	}

	cp := *lock

	return &cp, nil
}

// SetVaultLock stores a vault lock policy (used by InitiateVaultLock).
func (b *InMemoryBackend) SetVaultLock(accountID, region, vaultName, policy, lockID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if _, ok := b.vaults[key]; !ok {
		return ErrVaultNotFound
	}

	// Expire stale InProgress lock before checking state.
	b.expireLockIfStale(key)

	if existing, ok := b.vaultLocks[key]; ok {
		if existing.State == lockStateInProgress {
			return ErrLockConflict
		}

		if existing.State == lockStateLocked {
			return ErrLockAlreadyLocked
		}
	}

	now := time.Now().UTC()
	b.vaultLocks[key] = &VaultLock{
		Policy:         policy,
		LockID:         lockID,
		State:          lockStateInProgress,
		CreationDate:   formatDate(now),
		ExpirationDate: formatDate(now.Add(vaultLockExpirationHours * time.Hour)),
	}

	return nil
}

// ----------------------------------------
// Provisioned capacity operations
// ----------------------------------------

// reapExpiredCapacity removes expired provisioned capacity units for an account.
// Caller must hold b.mu.
func (b *InMemoryBackend) reapExpiredCapacity(accountID string) {
	now := time.Now().UTC()
	caps := b.provisionedCapacity[accountID]
	active := caps[:0]

	for _, c := range caps {
		exp, err := time.Parse("2006-01-02T15:04:05.000Z", c.ExpirationDate)
		if err != nil || now.Before(exp) {
			active = append(active, c)
		}
	}

	b.provisionedCapacity[accountID] = active
}

// ListProvisionedCapacity returns all non-expired provisioned capacity units for an account.
func (b *InMemoryBackend) ListProvisionedCapacity(accountID string) []*ProvisionedCapacity {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.reapExpiredCapacity(accountID)

	caps := b.provisionedCapacity[accountID]
	result := make([]*ProvisionedCapacity, 0, len(caps))

	for _, c := range caps {
		cp := *c
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].CapacityID < result[j].CapacityID })

	return result
}

// PurchaseProvisionedCapacity adds a provisioned capacity unit for an account.
// Returns ErrProvisionedCapacityLimit if the account already has 2 active units.
func (b *InMemoryBackend) PurchaseProvisionedCapacity(accountID string) (*ProvisionedCapacity, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.reapExpiredCapacity(accountID)

	if len(b.provisionedCapacity[accountID]) >= maxProvisionedCapacityUnits {
		return nil, ErrProvisionedCapacityLimit
	}

	now := time.Now().UTC()
	unit := &ProvisionedCapacity{
		CapacityID:     generateID(capacityIDLength),
		StartDate:      formatDate(now),
		ExpirationDate: formatDate(now.AddDate(0, provisionedCapacityMonths, 0)),
	}

	b.provisionedCapacity[accountID] = append(b.provisionedCapacity[accountID], unit)

	return unit, nil
}

// AddVaultInternal adds a vault directly to the backend for testing.
func (b *InMemoryBackend) AddVaultInternal(accountID, region string, v *Vault) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: v.VaultName}
	cp := *v

	if cp.Tags == nil {
		cp.Tags = make(map[string]string)
	}

	b.vaults[key] = &cp

	if b.vaultsByAccountRegion[accountID] == nil {
		b.vaultsByAccountRegion[accountID] = make(map[string]map[string]struct{})
	}
	if b.vaultsByAccountRegion[accountID][region] == nil {
		b.vaultsByAccountRegion[accountID][region] = make(map[string]struct{})
	}
	b.vaultsByAccountRegion[accountID][region][v.VaultName] = struct{}{}

	if b.archives[key] == nil {
		b.archives[key] = make(map[string]*Archive)
	}

	if b.jobs[key] == nil {
		b.jobs[key] = make(map[string]*Job)
	}

	if b.multipartUploads[key] == nil {
		b.multipartUploads[key] = make(map[string]*MultipartUpload)
	}
}

// AddArchiveInternal adds an archive directly to the backend for testing.
// It does not update the vault's NumberOfArchives or SizeInBytes counters;
// callers that need accounting to be correct should update the vault via AddVaultInternal.
func (b *InMemoryBackend) AddArchiveInternal(accountID, region, vaultName string, a *Archive) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if b.archives[key] == nil {
		b.archives[key] = make(map[string]*Archive)
	}

	b.archives[key][a.ArchiveID] = cloneArchive(a)
}

// AddMultipartUploadInternal adds an in-progress multipart upload directly to the backend for testing.
func (b *InMemoryBackend) AddMultipartUploadInternal(accountID, region, vaultName string, up *MultipartUpload) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if b.multipartUploads[key] == nil {
		b.multipartUploads[key] = make(map[string]*MultipartUpload)
	}

	cp := *up
	b.multipartUploads[key][up.MultipartUploadID] = &cp
}

// AbortVaultLock removes an in-progress vault lock.
func (b *InMemoryBackend) AbortVaultLock(accountID, region, vaultName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if _, ok := b.vaults[key]; !ok {
		return ErrVaultNotFound
	}

	delete(b.vaultLocks, key)

	return nil
}

// CompleteVaultLock completes and seals a vault lock.
func (b *InMemoryBackend) CompleteVaultLock(accountID, region, vaultName, lockID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if _, ok := b.vaults[key]; !ok {
		return ErrVaultNotFound
	}

	b.expireLockIfStale(key)

	lock, ok := b.vaultLocks[key]
	if !ok || lock.State != lockStateInProgress {
		return ErrValidation
	}

	if lock.LockID != lockID {
		return ErrValidation
	}

	lock.State = lockStateLocked
	lock.ExpirationDate = ""

	return nil
}

// GetDataRetrievalPolicy returns the data retrieval policy for the account.
func (b *InMemoryBackend) GetDataRetrievalPolicy(accountID string) string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.dataRetrievalPolicies[accountID]
}

// SetDataRetrievalPolicy stores the data retrieval policy for the account.
func (b *InMemoryBackend) SetDataRetrievalPolicy(accountID string, policy []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.dataRetrievalPolicies[accountID] = string(policy)
}

// AddJobInternal adds a job directly to the backend for testing.
func (b *InMemoryBackend) AddJobInternal(accountID, region, vaultName string, j *Job) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if b.jobs[key] == nil {
		b.jobs[key] = make(map[string]*Job)
	}

	cp := *j
	b.jobs[key][j.JobID] = &cp
}
