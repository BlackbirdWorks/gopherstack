package glacier

import (
	"crypto/rand"
	"errors"
	"fmt"
	"maps"
	"math/big"
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
)

const (
	// archiveIDLength is the length of the random archive ID suffix.
	archiveIDLength = 60
	// jobIDLength is the length of the random job ID.
	jobIDLength = 60
	// idChars are the characters used for generating random IDs.
	idChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

// StorageBackend is the interface for the Glacier backend.
type StorageBackend interface {
	CreateVault(accountID, region, vaultName string) (*Vault, error)
	DescribeVault(accountID, region, vaultName string) (*Vault, error)
	DeleteVault(accountID, region, vaultName string) error
	ListVaults(accountID, region string) []*Vault

	UploadArchive(accountID, region, vaultName, description, checksum string, size int64) (*Archive, error)
	DeleteArchive(accountID, region, vaultName, archiveID string) error

	InitiateJob(accountID, region, vaultName string, req *initiateJobRequest) (*Job, error)
	DescribeJob(accountID, region, vaultName, jobID string) (*Job, error)
	ListJobs(accountID, region, vaultName string) []*Job

	SetVaultNotifications(accountID, region, vaultName, snsTopic string, events []string) error
	GetVaultNotifications(accountID, region, vaultName string) (string, []string, error)
	DeleteVaultNotifications(accountID, region, vaultName string) error

	SetVaultAccessPolicy(accountID, region, vaultName, policy string) error
	GetVaultAccessPolicy(accountID, region, vaultName string) (string, error)
	DeleteVaultAccessPolicy(accountID, region, vaultName string) error

	AddTagsToVault(accountID, region, vaultName string, tags map[string]string) error
	ListTagsForVault(accountID, region, vaultName string) (map[string]string, error)
	RemoveTagsFromVault(accountID, region, vaultName string, tagKeys []string) error
}

// vaultKey uniquely identifies a vault within an account and region.
type vaultKey struct {
	AccountID string
	Region    string
	VaultName string
}

// InMemoryBackend is the in-memory backend for Glacier.
type InMemoryBackend struct {
	vaults   map[vaultKey]*Vault
	archives map[vaultKey]map[string]*Archive
	jobs     map[vaultKey]map[string]*Job
	mu       sync.RWMutex
}

// NewInMemoryBackend creates a new in-memory Glacier backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		vaults:   make(map[vaultKey]*Vault),
		archives: make(map[vaultKey]map[string]*Archive),
		jobs:     make(map[vaultKey]map[string]*Job),
	}
}

// generateID creates a random ID of the given length.
func generateID(length int) string {
	b := make([]byte, length)

	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(idChars))))
		if err != nil {
			b[i] = idChars[0]

			continue
		}

		b[i] = idChars[n.Int64()]
	}

	return string(b)
}

// vaultARN returns the ARN for a Glacier vault.
func vaultARN(accountID, region, vaultName string) string {
	return fmt.Sprintf("arn:aws:glacier:%s:%s:vaults/%s", region, accountID, vaultName)
}

// vaultLocation returns the location path for a vault creation response.
func vaultLocation(accountID, vaultName string) string {
	return fmt.Sprintf("/%s/vaults/%s", accountID, vaultName)
}

// CreateVault creates a new Glacier vault.
func (b *InMemoryBackend) CreateVault(accountID, region, vaultName string) (*Vault, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

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

	return v, nil
}

// DeleteVault deletes a vault.
func (b *InMemoryBackend) DeleteVault(accountID, region, vaultName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if _, ok := b.vaults[key]; !ok {
		return ErrVaultNotFound
	}

	delete(b.vaults, key)
	delete(b.archives, key)
	delete(b.jobs, key)

	return nil
}

// ListVaults returns all vaults for the given account and region.
func (b *InMemoryBackend) ListVaults(accountID, region string) []*Vault {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []*Vault

	for k, v := range b.vaults {
		if k.AccountID == accountID && k.Region == region {
			result = append(result, v)
		}
	}

	return result
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

	b.vaults[key].NumberOfArchives--
	b.vaults[key].SizeInBytes -= a.Size
	delete(b.archives[key], archiveID)

	return nil
}

// InitiateJob creates a new retrieval or inventory job.
func (b *InMemoryBackend) InitiateJob(accountID, region, vaultName string, req *initiateJobRequest) (*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	v, ok := b.vaults[key]
	if !ok {
		return nil, ErrVaultNotFound
	}

	jobID := generateID(jobIDLength)
	tier := req.Tier
	if tier == "" {
		tier = "Standard"
	}

	j := &Job{
		JobID:          jobID,
		VaultARN:       v.VaultARN,
		VaultName:      vaultName,
		Action:         req.Type,
		ArchiveID:      req.ArchiveID,
		JobDescription: req.Description,
		StatusCode:     "Succeeded",
		StatusMessage:  "Succeeded",
		CreationDate:   formatDate(time.Now()),
		CompletionDate: formatDate(time.Now()),
		Completed:      true,
		Tier:           tier,
	}

	b.jobs[key][jobID] = j

	return j, nil
}

// DescribeJob returns metadata for a job.
func (b *InMemoryBackend) DescribeJob(accountID, region, vaultName, jobID string) (*Job, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if _, ok := b.vaults[key]; !ok {
		return nil, ErrVaultNotFound
	}

	j, ok := b.jobs[key][jobID]
	if !ok {
		return nil, ErrJobNotFound
	}

	return j, nil
}

// ListJobs returns all jobs for the given vault.
func (b *InMemoryBackend) ListJobs(accountID, region, vaultName string) []*Job {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	result := make([]*Job, 0, len(b.jobs[key]))

	for _, j := range b.jobs[key] {
		result = append(result, j)
	}

	return result
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

	v.NotificationSNSTopic = snsTopic
	v.NotificationEvents = events

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

// AddTagsToVault adds or updates tags on a vault.
func (b *InMemoryBackend) AddTagsToVault(accountID, region, vaultName string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	v, ok := b.vaults[key]
	if !ok {
		return ErrVaultNotFound
	}

	if v.Tags == nil {
		v.Tags = make(map[string]string)
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
