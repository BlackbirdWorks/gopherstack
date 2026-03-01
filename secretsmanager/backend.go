package secretsmanager

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	// ErrSecretNotFound is returned when the specified secret does not exist.
	ErrSecretNotFound = errors.New("ResourceNotFoundException")
	// ErrSecretAlreadyExists is returned when a secret with the given name already exists.
	ErrSecretAlreadyExists = errors.New("ResourceExistsException")
	// ErrSecretDeleted is returned when an operation is attempted on a deleted secret.
	ErrSecretDeleted = errors.New("InvalidRequestException")
	// ErrVersionNotFound is returned when the specified version does not exist.
	ErrVersionNotFound = errors.New("ResourceNotFoundException")
)

const (
	// defaultMaxResults is the default maximum number of secrets to list.
	defaultMaxResults = 100
	// randomSuffixBytes is the number of bytes to use for the ARN random suffix.
	randomSuffixBytes = 3
	// arnMinParts is the minimum number of colon-separated parts in a Secrets Manager ARN.
	arnMinParts = 7
	// arnNameIndex is the index of the name-with-suffix part in a Secrets Manager ARN.
	arnNameIndex = 6
	// arnSuffixLen is the length of the random ARN suffix: dash + 6 hex characters.
	arnSuffixLen = 7
)

// StorageBackend defines the interface for the Secrets Manager in-memory backend.
type StorageBackend interface {
	CreateSecret(input *CreateSecretInput) (*CreateSecretOutput, error)
	GetSecretValue(input *GetSecretValueInput) (*GetSecretValueOutput, error)
	PutSecretValue(input *PutSecretValueInput) (*PutSecretValueOutput, error)
	DeleteSecret(input *DeleteSecretInput) (*DeleteSecretOutput, error)
	ListSecrets(input *ListSecretsInput) (*ListSecretsOutput, error)
	DescribeSecret(input *DescribeSecretInput) (*DescribeSecretOutput, error)
	UpdateSecret(input *UpdateSecretInput) (*UpdateSecretOutput, error)
	RestoreSecret(input *RestoreSecretInput) (*RestoreSecretOutput, error)
	TagResource(input *TagResourceInput) error
	UntagResource(input *UntagResourceInput) error
	RotateSecret(input *RotateSecretInput) (*RotateSecretOutput, error)
	ListAll() []SecretListEntry
}

// InMemoryBackend is a concurrency-safe in-memory Secrets Manager backend.
type InMemoryBackend struct {
	secrets   map[string]*Secret // keyed by Name
	accountID string
	region    string
	mu        sync.RWMutex
}

// NewInMemoryBackend creates and returns a new empty Secrets Manager backend with default account/region.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(MockAccountID, MockRegion)
}

// NewInMemoryBackendWithConfig creates a new Secrets Manager backend with the given account ID and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		secrets:   make(map[string]*Secret),
		accountID: accountID,
		region:    region,
	}
}

// resolveSecretID resolves a name or ARN to the internal key (name).
func resolveSecretID(secretID string) string {
	if strings.HasPrefix(secretID, "arn:aws:secretsmanager:") {
		// Extract name from ARN: arn:aws:secretsmanager:region:account:secret:name-suffix
		parts := strings.Split(secretID, ":")
		if len(parts) >= arnMinParts {
			nameWithSuffix := parts[arnNameIndex]
			// Remove the trailing -XXXXXX suffix
			if len(nameWithSuffix) > arnSuffixLen {
				return nameWithSuffix[:len(nameWithSuffix)-arnSuffixLen]
			}

			return nameWithSuffix
		}
	}

	return secretID
}

// generateRandomSuffix generates a 6-character hex random suffix for ARNs.
func generateRandomSuffix() string {
	b := make([]byte, randomSuffixBytes)
	if _, err := rand.Read(b); err != nil {
		return "000000"
	}

	return hex.EncodeToString(b)
}

// buildARNWithRegion constructs a Secrets Manager ARN using the given region.
func (b *InMemoryBackend) buildARNWithRegion(region, name, suffix string) string {
	return arn.Build("secretsmanager", region, b.accountID, "secret:"+name+"-"+suffix)
}

// CreateSecret creates a new secret with an optional initial value.
func (b *InMemoryBackend) CreateSecret(input *CreateSecretInput) (*CreateSecretOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.secrets[input.Name]; exists {
		return nil, ErrSecretAlreadyExists
	}

	suffix := generateRandomSuffix()
	region := b.region
	if input.Region != "" {
		region = input.Region
	}
	arn := b.buildARNWithRegion(region, input.Name, suffix)

	secret := &Secret{
		ARN:         arn,
		Name:        input.Name,
		Description: input.Description,
		Versions:    make(map[string]*SecretVersion),
	}

	if len(input.Tags) > 0 {
		secret.Tags = tags.New(secret.Name + ".tags")

		for _, t := range input.Tags {
			secret.Tags.Set(t.Key, t.Value)
		}
	}

	var versionID string

	if input.SecretString != "" || len(input.SecretBinary) > 0 {
		versionID = uuid.New().String()
		version := &SecretVersion{
			VersionID:     versionID,
			SecretString:  input.SecretString,
			SecretBinary:  input.SecretBinary,
			StagingLabels: []string{StagingLabelCurrent},
			CreatedDate:   UnixTimeFloat(time.Now()),
		}
		secret.Versions[versionID] = version
		secret.CurrentVersionID = versionID
	}

	b.secrets[input.Name] = secret

	return &CreateSecretOutput{
		ARN:       arn,
		Name:      input.Name,
		VersionID: versionID,
	}, nil
}

// GetSecretValue retrieves the value of a secret version.
func (b *InMemoryBackend) GetSecretValue(input *GetSecretValueInput) (*GetSecretValueOutput, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	name := resolveSecretID(input.SecretID)

	secret, exists := b.secrets[name]
	if !exists {
		return nil, ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is deleted", ErrSecretDeleted, input.SecretID)
	}

	version := b.findVersion(secret, input.VersionID, input.VersionStage)
	if version == nil {
		return nil, ErrVersionNotFound
	}

	return &GetSecretValueOutput{
		ARN:           secret.ARN,
		Name:          secret.Name,
		VersionID:     version.VersionID,
		SecretString:  version.SecretString,
		SecretBinary:  version.SecretBinary,
		VersionStages: version.StagingLabels,
		CreatedDate:   version.CreatedDate,
	}, nil
}

// findVersion locates the appropriate version by ID or staging label.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) findVersion(secret *Secret, versionID, versionStage string) *SecretVersion {
	if versionID != "" {
		return secret.Versions[versionID]
	}

	label := versionStage
	if label == "" {
		label = StagingLabelCurrent
	}

	for _, v := range secret.Versions {
		if slices.Contains(v.StagingLabels, label) {
			return v
		}
	}

	return nil
}

// PutSecretValue adds a new version to an existing secret.
func (b *InMemoryBackend) PutSecretValue(input *PutSecretValueInput) (*PutSecretValueOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	name := resolveSecretID(input.SecretID)

	secret, exists := b.secrets[name]
	if !exists {
		return nil, ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is deleted", ErrSecretDeleted, input.SecretID)
	}

	versionID := input.ClientRequestToken
	if versionID == "" {
		versionID = uuid.New().String()
	}

	b.rotateStagingLabels(secret, versionID)

	version := &SecretVersion{
		VersionID:     versionID,
		SecretString:  input.SecretString,
		SecretBinary:  input.SecretBinary,
		StagingLabels: []string{StagingLabelCurrent},
		CreatedDate:   UnixTimeFloat(time.Now()),
	}

	secret.Versions[versionID] = version
	secret.CurrentVersionID = versionID

	return &PutSecretValueOutput{
		ARN:           secret.ARN,
		Name:          secret.Name,
		VersionID:     versionID,
		VersionStages: []string{StagingLabelCurrent},
	}, nil
}

// rotateStagingLabels moves AWSCURRENT to AWSPREVIOUS and removes old AWSPREVIOUS.
// Must be called with a write lock held.
func (b *InMemoryBackend) rotateStagingLabels(secret *Secret, newVersionID string) {
	for _, v := range secret.Versions {
		newLabels := make([]string, 0, len(v.StagingLabels))

		for _, sl := range v.StagingLabels {
			switch sl {
			case StagingLabelCurrent:
				// Promote current to previous only if there isn't already a previous
				newLabels = append(newLabels, StagingLabelPrevious)
			case StagingLabelPrevious:
				// Drop old previous label (will be replaced)
			default:
				newLabels = append(newLabels, sl)
			}
		}

		v.StagingLabels = newLabels
	}

	_ = newVersionID // newVersionID will get AWSCURRENT from the caller
}

// DeleteSecret marks a secret as deleted.
func (b *InMemoryBackend) DeleteSecret(input *DeleteSecretInput) (*DeleteSecretOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	name := resolveSecretID(input.SecretID)

	secret, exists := b.secrets[name]
	if !exists {
		return nil, ErrSecretNotFound
	}

	now := UnixTimeFloat(time.Now())
	secret.DeletedDate = &now

	return &DeleteSecretOutput{
		ARN:          secret.ARN,
		Name:         secret.Name,
		DeletionDate: now,
	}, nil
}

// ListSecrets returns a paginated list of secrets.
func (b *InMemoryBackend) ListSecrets(input *ListSecretsInput) (*ListSecretsOutput, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	entries := make([]SecretListEntry, 0, len(b.secrets))

	for _, s := range b.secrets {
		if s.DeletedDate != nil && !input.IncludeDeleted {
			continue
		}

		entries = append(entries, secretToListEntry(s))
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})

	startIdx := parseToken(input.NextToken)
	maxResults := int64(defaultMaxResults)

	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = *input.MaxResults
	}

	if startIdx >= len(entries) {
		return &ListSecretsOutput{SecretList: []SecretListEntry{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(entries) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(entries)
	}

	return &ListSecretsOutput{
		SecretList: entries[startIdx:end],
		NextToken:  nextToken,
	}, nil
}

// DescribeSecret returns metadata about a secret.
func (b *InMemoryBackend) DescribeSecret(input *DescribeSecretInput) (*DescribeSecretOutput, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	name := resolveSecretID(input.SecretID)

	secret, exists := b.secrets[name]
	if !exists {
		return nil, ErrSecretNotFound
	}

	versionIDsToStages := make(map[string][]string, len(secret.Versions))

	for vID, v := range secret.Versions {
		versionIDsToStages[vID] = v.StagingLabels
	}

	return &DescribeSecretOutput{
		ARN:                secret.ARN,
		Name:               secret.Name,
		Description:        secret.Description,
		Tags:               secret.Tags,
		DeletedDate:        secret.DeletedDate,
		VersionIDsToStages: versionIDsToStages,
	}, nil
}

// UpdateSecret updates the description of a secret and optionally creates a new version.
func (b *InMemoryBackend) UpdateSecret(input *UpdateSecretInput) (*UpdateSecretOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	name := resolveSecretID(input.SecretID)

	secret, exists := b.secrets[name]
	if !exists {
		return nil, ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is deleted", ErrSecretDeleted, input.SecretID)
	}

	if input.Description != "" {
		secret.Description = input.Description
	}

	var versionID string

	if input.SecretString != "" || len(input.SecretBinary) > 0 {
		versionID = uuid.New().String()

		b.rotateStagingLabels(secret, versionID)

		version := &SecretVersion{
			VersionID:     versionID,
			SecretString:  input.SecretString,
			SecretBinary:  input.SecretBinary,
			StagingLabels: []string{StagingLabelCurrent},
			CreatedDate:   UnixTimeFloat(time.Now()),
		}

		secret.Versions[versionID] = version
		secret.CurrentVersionID = versionID
	}

	return &UpdateSecretOutput{
		ARN:       secret.ARN,
		Name:      secret.Name,
		VersionID: versionID,
	}, nil
}

// RestoreSecret clears the deletion mark from a secret.
func (b *InMemoryBackend) RestoreSecret(input *RestoreSecretInput) (*RestoreSecretOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	name := resolveSecretID(input.SecretID)

	secret, exists := b.secrets[name]
	if !exists {
		return nil, ErrSecretNotFound
	}

	secret.DeletedDate = nil

	return &RestoreSecretOutput{
		ARN:  secret.ARN,
		Name: secret.Name,
	}, nil
}

// ListAll returns all secrets as list entries, sorted by name (for dashboard use).
func (b *InMemoryBackend) ListAll() []SecretListEntry {
	b.mu.RLock()
	defer b.mu.RUnlock()

	entries := make([]SecretListEntry, 0, len(b.secrets))

	for _, s := range b.secrets {
		entries = append(entries, secretToListEntry(s))
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})

	return entries
}

// secretToListEntry converts a Secret to a SecretListEntry.
func secretToListEntry(s *Secret) SecretListEntry {
	return SecretListEntry{
		ARN:         s.ARN,
		Name:        s.Name,
		Description: s.Description,
		DeletedDate: s.DeletedDate,
		Tags:        s.Tags,
	}
}

// parseToken converts a pagination token string to an integer start index.
func parseToken(token string) int {
	if token == "" {
		return 0
	}

	idx, err := strconv.Atoi(token)
	if err != nil || idx < 0 {
		return 0
	}

	return idx
}

// generateVersionID generates a random version ID for secret rotation.
func generateVersionID() string {
	return fmt.Sprintf("%s-%s", generateRandomSuffix(), generateRandomSuffix())
}

// TagResource adds or updates tags on a secret.
func (b *InMemoryBackend) TagResource(input *TagResourceInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := resolveSecretID(input.SecretID)
	secret, ok := b.secrets[id]
	if !ok {
		return ErrSecretNotFound
	}
	if secret.DeletedDate != nil {
		return ErrSecretDeleted
	}
	if secret.Tags == nil {
		secret.Tags = tags.New(id + ".tags")
	}
	for _, t := range input.Tags {
		secret.Tags.Set(t.Key, t.Value)
	}

	return nil
}

// UntagResource removes tags from a secret.
func (b *InMemoryBackend) UntagResource(input *UntagResourceInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := resolveSecretID(input.SecretID)
	secret, ok := b.secrets[id]
	if !ok {
		return ErrSecretNotFound
	}
	if secret.DeletedDate != nil {
		return ErrSecretDeleted
	}
	if secret.Tags != nil {
		secret.Tags.DeleteKeys(input.TagKeys)
	}

	return nil
}

// RotateSecret creates a new version of the secret (rotation stub).
func (b *InMemoryBackend) RotateSecret(input *RotateSecretInput) (*RotateSecretOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := resolveSecretID(input.SecretID)
	secret, ok := b.secrets[id]
	if !ok {
		return nil, ErrSecretNotFound
	}
	if secret.DeletedDate != nil {
		return nil, ErrSecretDeleted
	}

	currentVer := b.findVersion(secret, "", StagingLabelCurrent)
	if currentVer == nil {
		return nil, ErrVersionNotFound
	}

	versionID := generateVersionID()
	newVer := &SecretVersion{
		VersionID:     versionID,
		SecretString:  currentVer.SecretString,
		SecretBinary:  currentVer.SecretBinary,
		StagingLabels: []string{"AWSPENDING"},
		CreatedDate:   UnixTimeFloat(time.Now()),
	}
	secret.Versions[versionID] = newVer

	b.rotateStagingLabels(secret, versionID)
	newVer.StagingLabels = []string{StagingLabelCurrent}
	secret.CurrentVersionID = versionID

	return &RotateSecretOutput{
		ARN:       secret.ARN,
		Name:      secret.Name,
		VersionID: versionID,
	}, nil
}
