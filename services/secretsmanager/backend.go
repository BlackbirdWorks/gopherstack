package secretsmanager

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	errResourceNotFoundException = "ResourceNotFoundException"
)

var (
	// ErrSecretNotFound is returned when the specified secret does not exist.
	ErrSecretNotFound = errors.New(errResourceNotFoundException)
	// ErrSecretAlreadyExists is returned when a secret with the given name already exists.
	ErrSecretAlreadyExists = errors.New("ResourceExistsException")
	// ErrSecretDeleted is returned when an operation is attempted on a deleted secret.
	ErrSecretDeleted = errors.New("InvalidRequestException")
	// ErrVersionNotFound is returned when the specified version does not exist.
	ErrVersionNotFound = errors.New(errResourceNotFoundException)
	// ErrInvalidPasswordParameters is returned when password generation parameters are invalid
	// (e.g. PasswordLength out of range, or empty charset after exclusions, or too few positions
	// to satisfy RequireEachIncludedType).
	ErrInvalidPasswordParameters = errors.New("InvalidParameterException")
	// ErrCryptoRandInvalidRange is returned when cryptoRandInt is called with a non-positive bound.
	ErrCryptoRandInvalidRange = errors.New("random integer bound must be positive")
	// ErrSecretValueTooLarge is returned when a secret value exceeds the 64 KB AWS limit.
	ErrSecretValueTooLarge = errors.New("InvalidParameterException")
	// ErrInvalidParameter is returned when an invalid parameter value is provided.
	ErrInvalidParameter = errors.New("InvalidParameterException")
	// ErrInvalidSecretName is returned when a secret name does not match the allowed pattern.
	ErrInvalidSecretName = errors.New("InvalidParameterException")
)

// secretNamePattern is the set of characters allowed in secret names by AWS.
// AWS allows letters, digits, and the following special characters: /_+=.@-.
var secretNamePattern = regexp.MustCompile(`^[a-zA-Z0-9/_+=.@-]+$`)

// maxSecretNameLength is the maximum allowed length of a secret name.
const maxSecretNameLength = 512

// defaultRecoveryWindowDays is the default recovery window in days for soft-deleted secrets.
const defaultRecoveryWindowDays = 30

// minRecoveryWindowDays is the minimum recovery window in days.
const minRecoveryWindowDays = 7

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
	// maxVersionsPerSecret is the maximum number of versions retained per secret.
	// Matches the AWS Secrets Manager limit of 100 versions.
	maxVersionsPerSecret = 100
	// maxSecretValueBytes is the maximum allowed size of a secret value in bytes (64 KB).
	maxSecretValueBytes = 65536
	// maxTagsPerSecret is the maximum number of tags allowed per secret.
	maxTagsPerSecret = 50
	// maxResultsListSecrets is the maximum allowed MaxResults for ListSecrets/ListSecretVersionIds.
	maxResultsListSecrets = 100
	// maxResultsBatchGet is the maximum allowed MaxResults for BatchGetSecretValue.
	maxResultsBatchGet = 20
	// maxSecretIDListSize is the maximum number of IDs allowed in BatchGetSecretValue.SecretIdList.
	maxSecretIDListSize = 20
	// rotationSchedulerInterval controls the background schedule evaluation cadence.
	rotationSchedulerInterval = time.Second
	// hoursPerDay is the number of hours in a day, used for day-granularity truncation.
	hoursPerDay = 24
	// maxSmallPoolSize is the maximum pool size for which the rejection-sampling fast path is used.
	maxSmallPoolSize = 256
	// randomBufMultiplier is the over-allocation factor for the random byte buffer.
	randomBufMultiplier = 4
)

// InMemoryBackend is a concurrency-safe in-memory Secrets Manager backend.
type InMemoryBackend struct {
	secrets            map[string]*Secret
	resourcePolicies   map[string]string
	replicationConfigs map[string][]ReplicationStatusType
	mu                 *lockmetrics.RWMutex
	now                func() time.Time
	accountID          string
	region             string
	schedulerOnce      sync.Once
	lambdaInvoker      LambdaInvoker
}

// SetLambdaInvoker stores the Lambda invoker on the backend. The rotation
// scheduler uses it to invoke Lambda steps for scheduled rotations.
func (b *InMemoryBackend) SetLambdaInvoker(invoker LambdaInvoker) {
	b.lambdaInvoker = invoker
}

// NewInMemoryBackend creates and returns a new empty Secrets Manager backend with default account/region.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(MockAccountID, MockRegion)
}

// NewInMemoryBackendWithConfig creates a new Secrets Manager backend with the given account ID and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		secrets:            make(map[string]*Secret),
		resourcePolicies:   make(map[string]string),
		replicationConfigs: make(map[string][]ReplicationStatusType),
		accountID:          accountID,
		region:             region,
		mu:                 lockmetrics.New("secretsmanager"),
		now:                time.Now,
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
func generateRandomSuffix() (string, error) {
	b := make([]byte, randomSuffixBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate ARN suffix: %w", err)
	}

	return hex.EncodeToString(b), nil
}

// buildARNWithRegion constructs a Secrets Manager ARN using the given region.
func (b *InMemoryBackend) buildARNWithRegion(region, name, suffix string) string {
	return arn.Build("secretsmanager", region, b.accountID, "secret:"+name+"-"+suffix)
}

// validateSecretName returns an error when the name is empty, too long, contains invalid chars,
// or starts with the "aws/" prefix reserved for AWS managed secrets.
func validateSecretName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: secret name must not be empty", ErrInvalidSecretName)
	}

	if len(name) > maxSecretNameLength {
		return fmt.Errorf(
			"%w: secret name must be %d characters or fewer",
			ErrInvalidSecretName,
			maxSecretNameLength,
		)
	}

	if !secretNamePattern.MatchString(name) {
		return fmt.Errorf(
			"%w: secret name must match pattern [a-zA-Z0-9/_+=.@-]+",
			ErrInvalidSecretName,
		)
	}

	if strings.HasPrefix(name, "aws/") {
		return fmt.Errorf(
			"%w: secret name must not start with \"aws/\" (reserved for AWS managed secrets)",
			ErrInvalidSecretName,
		)
	}

	return nil
}

// validateMaxResults returns an error when MaxResults is outside [1, limit].
func validateMaxResults(n *int64, limit int64) error {
	if n == nil {
		return nil
	}

	if *n < 1 || *n > limit {
		return fmt.Errorf(
			"%w: MaxResults must be between 1 and %d",
			ErrInvalidParameter,
			limit,
		)
	}

	return nil
}

// validateTagCount returns an error when the total number of tags would exceed the AWS limit.
func validateTagCount(existing int, adding int) error {
	if existing+adding > maxTagsPerSecret {
		return fmt.Errorf(
			"%w: maximum of %d tags per secret exceeded",
			ErrInvalidParameter,
			maxTagsPerSecret,
		)
	}

	return nil
}

// CreateSecret creates a new secret with an optional initial value.
func (b *InMemoryBackend) CreateSecret(input *CreateSecretInput) (*CreateSecretOutput, error) {
	if err := validateSecretName(input.Name); err != nil {
		return nil, err
	}

	if err := validateSecretSize(input.SecretString, input.SecretBinary); err != nil {
		return nil, err
	}

	if err := validateTagCount(0, len(input.Tags)); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateSecret")
	defer b.mu.Unlock()

	if existing, exists := b.secrets[input.Name]; exists {
		if existing.DeletedDate != nil {
			return nil, fmt.Errorf(
				"%w: a secret with this name is already scheduled for deletion; restore or force-delete it first",
				ErrSecretDeleted,
			)
		}

		return nil, ErrSecretAlreadyExists
	}

	suffix, err := generateRandomSuffix()
	if err != nil {
		return nil, err
	}

	region := b.region
	if input.Region != "" {
		region = input.Region
	}
	arn := b.buildARNWithRegion(region, input.Name, suffix)

	secret := &Secret{
		ARN:         arn,
		Name:        input.Name,
		Description: input.Description,
		KmsKeyID:    input.KmsKeyID,
		Versions:    make(map[string]*SecretVersion),
	}

	createdNow := UnixTimeFloat(time.Now())
	secret.CreatedDate = &createdNow

	if len(input.Tags) > 0 {
		secret.Tags = tags.New(secret.Name + ".tags")

		for _, t := range input.Tags {
			secret.Tags.Set(t.Key, t.Value)
		}
	}

	var versionID string

	if input.SecretString != "" || len(input.SecretBinary) > 0 {
		// Use ClientRequestToken as initial version ID for idempotency.
		versionID = input.ClientRequestToken
		if versionID == "" {
			versionID = uuid.New().String()
		}

		now := UnixTimeFloat(time.Now())
		version := &SecretVersion{
			VersionID:     versionID,
			SecretString:  input.SecretString,
			SecretBinary:  input.SecretBinary,
			StagingLabels: []string{StagingLabelCurrent},
			CreatedDate:   now,
		}
		secret.Versions[versionID] = version
		secret.CurrentVersionID = versionID
		secret.LastChangedDate = &now
	}

	b.secrets[input.Name] = secret

	if len(input.AddReplicaRegions) > 0 {
		replicas := make([]ReplicationStatusType, 0, len(input.AddReplicaRegions))
		for _, r := range input.AddReplicaRegions {
			replicas = append(replicas, ReplicationStatusType{
				Region:        r.Region,
				KmsKeyID:      r.KmsKeyID,
				Status:        replicationStatusInProgress,
				StatusMessage: "replication queued",
			})
		}
		b.replicationConfigs[input.Name] = replicas
	}

	b.syncReplicationStatusLocked(secret)

	return &CreateSecretOutput{
		ARN:               arn,
		Name:              input.Name,
		VersionID:         versionID,
		ReplicationStatus: b.replicationConfigs[input.Name],
	}, nil
}

// GetSecretValue retrieves the value of a secret version.
func (b *InMemoryBackend) GetSecretValue(input *GetSecretValueInput) (*GetSecretValueOutput, error) {
	b.mu.Lock("GetSecretValue")
	defer b.mu.Unlock()

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

	// When both VersionId and VersionStage are supplied, they must agree.
	// AWS returns ResourceNotFoundException when the ID does not carry the requested stage.
	if input.VersionID != "" && input.VersionStage != "" {
		if !slices.Contains(version.StagingLabels, input.VersionStage) {
			return nil, fmt.Errorf(
				"%w: staging label %s not found on version %s",
				ErrVersionNotFound,
				input.VersionStage,
				input.VersionID,
			)
		}
	}

	// Track access date (truncated to day granularity as AWS does).
	accessDay := UnixTimeFloat(time.Now().UTC().Truncate(hoursPerDay * time.Hour))
	secret.LastAccessedDate = &accessDay
	version.LastAccessedDate = &accessDay

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
	if input.SecretString == "" && len(input.SecretBinary) == 0 {
		return nil, fmt.Errorf(
			"%w: you must provide either SecretString or SecretBinary",
			ErrInvalidParameter,
		)
	}

	if err := validateSecretSize(input.SecretString, input.SecretBinary); err != nil {
		return nil, err
	}

	b.mu.Lock("PutSecretValue")
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

	// Idempotency: if the version ID already exists with identical content, return it.
	if existing, ok := secret.Versions[versionID]; ok {
		if existing.SecretString == input.SecretString && string(existing.SecretBinary) == string(input.SecretBinary) {
			return &PutSecretValueOutput{
				ARN:           secret.ARN,
				Name:          secret.Name,
				VersionID:     existing.VersionID,
				VersionStages: existing.StagingLabels,
			}, nil
		}
	}

	b.rotateStagingLabels(secret)

	// Determine staging labels: AWSCURRENT is always applied; any additional
	// labels provided in VersionStages are merged in (e.g. AWSPENDING).
	stagingLabels := []string{StagingLabelCurrent}

	for _, label := range input.VersionStages {
		if label != StagingLabelCurrent {
			stagingLabels = append(stagingLabels, label)
		}
	}

	now := UnixTimeFloat(time.Now())
	version := &SecretVersion{
		VersionID:     versionID,
		SecretString:  input.SecretString,
		SecretBinary:  input.SecretBinary,
		StagingLabels: stagingLabels,
		CreatedDate:   now,
	}

	secret.Versions[versionID] = version
	secret.CurrentVersionID = versionID
	secret.LastChangedDate = &now
	b.syncReplicationStatusLocked(secret)

	pruneVersions(secret)

	return &PutSecretValueOutput{
		ARN:           secret.ARN,
		Name:          secret.Name,
		VersionID:     versionID,
		VersionStages: stagingLabels,
	}, nil
}

// rotateStagingLabels moves AWSCURRENT to AWSPREVIOUS and removes old AWSPREVIOUS.
// Must be called with a write lock held.
func (b *InMemoryBackend) rotateStagingLabels(secret *Secret) {
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
}

// pruneVersions removes the oldest unlabeled versions when the total version count
// exceeds maxVersionsPerSecret. Versions with any staging labels are never pruned.
// Must be called with a write lock held.
func pruneVersions(secret *Secret) {
	if len(secret.Versions) <= maxVersionsPerSecret {
		return
	}

	type versionEntry struct {
		id          string
		createdDate float64
	}

	unlabeled := make([]versionEntry, 0, len(secret.Versions))

	for id, v := range secret.Versions {
		if len(v.StagingLabels) == 0 {
			unlabeled = append(unlabeled, versionEntry{id: id, createdDate: v.CreatedDate})
		}
	}

	// Sort oldest first; break ties by ID for deterministic eviction order.
	sort.Slice(unlabeled, func(i, j int) bool {
		if unlabeled[i].createdDate != unlabeled[j].createdDate {
			return unlabeled[i].createdDate < unlabeled[j].createdDate
		}

		return unlabeled[i].id < unlabeled[j].id
	})

	toRemove := min(len(secret.Versions)-maxVersionsPerSecret, len(unlabeled))

	for i := range toRemove {
		delete(secret.Versions, unlabeled[i].id)
	}
}

// validateSecretSize returns ErrSecretValueTooLarge when the secret value exceeds 64 KB.
func validateSecretSize(secretString string, secretBinary []byte) error {
	if len(secretString) > maxSecretValueBytes {
		return fmt.Errorf(
			"%w: secret string value exceeds maximum size of %d bytes",
			ErrSecretValueTooLarge,
			maxSecretValueBytes,
		)
	}

	if len(secretBinary) > maxSecretValueBytes {
		return fmt.Errorf(
			"%w: secret binary value exceeds maximum size of %d bytes",
			ErrSecretValueTooLarge,
			maxSecretValueBytes,
		)
	}

	return nil
}

// DeleteSecret marks a secret as deleted, or permanently removes it when ForceDeleteWithoutRecovery is set.
func (b *InMemoryBackend) DeleteSecret(input *DeleteSecretInput) (*DeleteSecretOutput, error) {
	b.mu.Lock("DeleteSecret")
	defer b.mu.Unlock()

	name := resolveSecretID(input.SecretID)

	secret, exists := b.secrets[name]
	if !exists {
		return nil, ErrSecretNotFound
	}

	now := UnixTimeFloat(b.now())

	if input.ForceDeleteWithoutRecovery {
		if secret.Tags != nil {
			secret.Tags.Close()
		}

		delete(b.secrets, name)
		delete(b.resourcePolicies, name)
		delete(b.replicationConfigs, name)

		return &DeleteSecretOutput{
			ARN:          secret.ARN,
			Name:         secret.Name,
			DeletionDate: now,
		}, nil
	}

	// Reject deletion of an already soft-deleted secret.
	if secret.DeletedDate != nil {
		return nil, fmt.Errorf(
			"%w: you can't delete a secret that's already scheduled for deletion",
			ErrInvalidParameter,
		)
	}

	// Validate recovery window if provided.
	recoveryDays := int64(defaultRecoveryWindowDays)
	if input.RecoveryWindowInDays != nil {
		recoveryDays = *input.RecoveryWindowInDays
		if recoveryDays < minRecoveryWindowDays || recoveryDays > defaultRecoveryWindowDays {
			return nil, fmt.Errorf(
				"%w: RecoveryWindowInDays must be between %d and %d",
				ErrInvalidParameter,
				minRecoveryWindowDays,
				defaultRecoveryWindowDays,
			)
		}
	}

	secret.DeletedDate = &now
	deletionDate := UnixTimeFloat(b.now().Add(time.Duration(recoveryDays) * hoursPerDay * time.Hour))

	return &DeleteSecretOutput{
		ARN:          secret.ARN,
		Name:         secret.Name,
		DeletionDate: deletionDate,
	}, nil
}

// ListSecrets returns a paginated list of secrets.
func (b *InMemoryBackend) ListSecrets(input *ListSecretsInput) (*ListSecretsOutput, error) {
	if err := validateMaxResults(input.MaxResults, maxResultsListSecrets); err != nil {
		return nil, err
	}

	b.mu.RLock("ListSecrets")
	defer b.mu.RUnlock()

	entries := make([]SecretListEntry, 0, len(b.secrets))

	for _, s := range b.secrets {
		if s.DeletedDate != nil && !input.IncludeDeleted {
			continue
		}

		if !secretMatchesFilters(s, input.Filters) {
			continue
		}

		entries = append(entries, secretToListEntry(s))
	}

	sort.Slice(entries, func(i, j int) bool {
		if strings.EqualFold(input.SortOrder, "desc") {
			return entries[i].Name > entries[j].Name
		}

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

// secretMatchesFilters returns true if the secret matches all provided filters.
func secretMatchesFilters(s *Secret, filters []SecretFilter) bool {
	for _, f := range filters {
		if !secretMatchesFilter(s, f) {
			return false
		}
	}

	return true
}

// secretMatchesFilter returns true if the secret matches a single filter.
func secretMatchesFilter(s *Secret, f SecretFilter) bool {
	switch f.Key {
	case "name":
		return anyMatchPrefix(f.Values, s.Name)
	case "description":
		return anyMatchPrefix(f.Values, s.Description)
	case "tag-key":
		return secretHasTagKey(s, f.Values)
	case "tag-value":
		return secretHasTagValue(s, f.Values)
	case "all":
		// "all" matches any of the filterable string fields.
		return anyMatchPrefix(f.Values, s.Name) ||
			anyMatchPrefix(f.Values, s.Description) ||
			secretHasTagKey(s, f.Values) ||
			secretHasTagValue(s, f.Values)
	case "primary-region":
		// In a single-region mock every secret belongs to the single region;
		// the filter always passes (no cross-region replication routing needed).
		return true
	case "owned-by-me":
		// In a single-account mock all secrets are owned by the configured account;
		// the filter always passes.
		return true
	default:
		return true
	}
}

// anyMatchPrefix returns true if target has any of the given values as a prefix.
func anyMatchPrefix(values []string, target string) bool {
	for _, v := range values {
		if strings.HasPrefix(target, v) {
			return true
		}
	}

	return false
}

// secretHasTagKey returns true if the secret has at least one of the given tag keys.
func secretHasTagKey(s *Secret, keys []string) bool {
	if s.Tags == nil {
		return false
	}

	tagMap := s.Tags.Clone()
	for _, k := range keys {
		if _, ok := tagMap[k]; ok {
			return true
		}
	}

	return false
}

// secretHasTagValue returns true if the secret has at least one tag with any of the given values.
func secretHasTagValue(s *Secret, values []string) bool {
	if s.Tags == nil {
		return false
	}

	tagMap := s.Tags.Clone()
	for _, v := range tagMap {
		if slices.Contains(values, v) {
			return true
		}
	}

	return false
}

// ListSecretVersionIDs returns the list of versions for a secret with optional pagination.
func (b *InMemoryBackend) ListSecretVersionIDs(input *ListSecretVersionIDsInput) (*ListSecretVersionIDsOutput, error) {
	if err := validateMaxResults(input.MaxResults, maxResultsListSecrets); err != nil {
		return nil, err
	}

	b.mu.RLock("ListSecretVersionIDs")
	defer b.mu.RUnlock()

	name := resolveSecretID(input.SecretID)

	secret, exists := b.secrets[name]
	if !exists {
		return nil, ErrSecretNotFound
	}

	entries := make([]SecretVersionEntry, 0, len(secret.Versions))

	for _, v := range secret.Versions {
		if !input.IncludeDeprecated && len(v.StagingLabels) == 0 {
			continue
		}

		entries = append(entries, SecretVersionEntry{
			VersionID:        v.VersionID,
			StagingLabels:    v.StagingLabels,
			CreatedDate:      v.CreatedDate,
			LastAccessedDate: v.LastAccessedDate,
			KmsKeyIDs:        append([]string{}, v.KmsKeyIDs...),
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].CreatedDate != entries[j].CreatedDate {
			return entries[i].CreatedDate > entries[j].CreatedDate
		}

		return entries[i].VersionID > entries[j].VersionID
	})

	startIdx := parseToken(input.NextToken)
	maxResults := int64(defaultMaxResults)

	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = *input.MaxResults
	}

	if startIdx >= len(entries) {
		return &ListSecretVersionIDsOutput{
			ARN:      secret.ARN,
			Name:     secret.Name,
			Versions: []SecretVersionEntry{},
		}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(entries) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(entries)
	}

	return &ListSecretVersionIDsOutput{
		ARN:       secret.ARN,
		Name:      secret.Name,
		Versions:  entries[startIdx:end],
		NextToken: nextToken,
	}, nil
}

// DescribeSecret returns metadata about a secret.
func (b *InMemoryBackend) DescribeSecret(input *DescribeSecretInput) (*DescribeSecretOutput, error) {
	b.mu.RLock("DescribeSecret")
	defer b.mu.RUnlock()

	name := resolveSecretID(input.SecretID)

	secret, exists := b.secrets[name]
	if !exists {
		return nil, ErrSecretNotFound
	}

	versionIDsToStages := make(map[string][]string, len(secret.Versions))

	for vID, v := range secret.Versions {
		if len(v.StagingLabels) > 0 {
			versionIDsToStages[vID] = append([]string(nil), v.StagingLabels...)
		}
	}

	out := &DescribeSecretOutput{
		ARN:                secret.ARN,
		Name:               secret.Name,
		Description:        secret.Description,
		KmsKeyID:           secret.KmsKeyID,
		RotationLambdaARN:  secret.RotationLambdaARN,
		RotationRules:      cloneRotationRules(secret.RotationRules),
		Tags:               secret.Tags,
		CreatedDate:        secret.CreatedDate,
		DeletedDate:        secret.DeletedDate,
		LastChangedDate:    secret.LastChangedDate,
		LastRotatedDate:    secret.LastRotatedDate,
		LastAccessedDate:   secret.LastAccessedDate,
		VersionIDsToStages: versionIDsToStages,
		RotationEnabled:    secret.RotationEnabled,
		ReplicationStatus:  b.replicationConfigs[name],
		OwnerAccountID:     b.accountID,
		PrimaryRegion:      b.region,
	}

	// Compute NextRotationDate from the last rotation base + interval.
	out.NextRotationDate = computeNextRotationDate(secret)

	return out, nil
}

// computeNextRotationDate returns the predicted next rotation timestamp for a secret, or nil if
// rotation is not configured or cannot be computed.
func computeNextRotationDate(secret *Secret) *float64 {
	if !secret.RotationEnabled || secret.RotationRules == nil {
		return nil
	}

	base := secret.LastRotatedDate
	if base == nil {
		base = secret.LastChangedDate
	}

	if base == nil {
		return nil
	}

	baseTime := time.Unix(0, int64(*base*float64(time.Second)))

	if isCronExpression(secret.RotationRules.ScheduleExpression) {
		next, ok := nextCronTime(secret.RotationRules.ScheduleExpression, baseTime)
		if !ok {
			return nil
		}

		nextFloat := UnixTimeFloat(next)

		return &nextFloat
	}

	interval, ok := rotationInterval(secret.RotationRules)
	if !ok {
		return nil
	}

	nextFloat := UnixTimeFloat(baseTime.Add(interval))

	return &nextFloat
}

// UpdateSecret updates the description of a secret and optionally creates a new version.
func (b *InMemoryBackend) UpdateSecret(input *UpdateSecretInput) (*UpdateSecretOutput, error) {
	if err := validateSecretSize(input.SecretString, input.SecretBinary); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateSecret")
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

	if input.KmsKeyID != "" {
		secret.KmsKeyID = input.KmsKeyID
	}

	var versionID string

	if input.SecretString != "" || len(input.SecretBinary) > 0 {
		versionID = input.ClientRequestToken
		if versionID == "" {
			versionID = uuid.New().String()
		}

		// Idempotency: if a version with this token already exists and content matches, skip.
		if existing, ok := secret.Versions[versionID]; ok {
			if existing.SecretString == input.SecretString &&
				string(existing.SecretBinary) == string(input.SecretBinary) {
				return &UpdateSecretOutput{
					ARN:       secret.ARN,
					Name:      secret.Name,
					VersionID: versionID,
				}, nil
			}
		}

		b.rotateStagingLabels(secret)

		now := UnixTimeFloat(time.Now())
		version := &SecretVersion{
			VersionID:     versionID,
			SecretString:  input.SecretString,
			SecretBinary:  input.SecretBinary,
			StagingLabels: []string{StagingLabelCurrent},
			CreatedDate:   now,
		}

		secret.Versions[versionID] = version
		secret.CurrentVersionID = versionID
		secret.LastChangedDate = &now
		b.syncReplicationStatusLocked(secret)

		pruneVersions(secret)
	}

	return &UpdateSecretOutput{
		ARN:       secret.ARN,
		Name:      secret.Name,
		VersionID: versionID,
	}, nil
}

// RestoreSecret clears the deletion mark from a secret.
func (b *InMemoryBackend) RestoreSecret(input *RestoreSecretInput) (*RestoreSecretOutput, error) {
	b.mu.Lock("RestoreSecret")
	defer b.mu.Unlock()

	name := resolveSecretID(input.SecretID)

	secret, exists := b.secrets[name]
	if !exists {
		return nil, ErrSecretNotFound
	}

	// AWS returns InvalidRequestException when trying to restore a secret that
	// is not in a deleted (pending deletion) state.
	if secret.DeletedDate == nil {
		return nil, fmt.Errorf(
			"%w: secret %s is not in a deleted state",
			ErrInvalidParameter,
			input.SecretID,
		)
	}

	secret.DeletedDate = nil

	return &RestoreSecretOutput{
		ARN:  secret.ARN,
		Name: secret.Name,
	}, nil
}

// ListAll returns all secrets as list entries, sorted by name (for dashboard use).
func (b *InMemoryBackend) ListAll() []SecretListEntry {
	b.mu.RLock("ListAll")
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
	var versionStages map[string][]string
	if len(s.Versions) > 0 {
		versionStages = make(map[string][]string, len(s.Versions))
		for id, v := range s.Versions {
			if len(v.StagingLabels) > 0 {
				versionStages[id] = append([]string(nil), v.StagingLabels...)
			}
		}
		if len(versionStages) == 0 {
			versionStages = nil
		}
	}

	return SecretListEntry{
		ARN:                    s.ARN,
		Name:                   s.Name,
		Description:            s.Description,
		KmsKeyID:               s.KmsKeyID,
		RotationLambdaARN:      s.RotationLambdaARN,
		RotationRules:          cloneRotationRules(s.RotationRules),
		RotationEnabled:        s.RotationEnabled,
		DeletedDate:            s.DeletedDate,
		LastChangedDate:        s.LastChangedDate,
		LastAccessedDate:       s.LastAccessedDate,
		LastRotatedDate:        s.LastRotatedDate,
		CreatedDate:            s.CreatedDate,
		Tags:                   s.Tags,
		SecretVersionsToStages: versionStages,
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
	return uuid.New().String()
}

// TagResource adds or updates tags on a secret.
func (b *InMemoryBackend) TagResource(input *TagResourceInput) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	id := resolveSecretID(input.SecretID)
	secret, ok := b.secrets[id]
	if !ok {
		return ErrSecretNotFound
	}
	if secret.DeletedDate != nil {
		return ErrSecretDeleted
	}

	existingCount := 0
	if secret.Tags != nil {
		existingCount = len(secret.Tags.Clone())
	}
	// Count net new keys (keys not already present don't increase the total).
	if err := validateTagCount(existingCount, len(input.Tags)); err != nil {
		return err
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
	b.mu.Lock("UntagResource")
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
	b.mu.Lock("RotateSecret")
	defer b.mu.Unlock()

	id := resolveSecretID(input.SecretID)
	secret, ok := b.secrets[id]
	if !ok {
		return nil, ErrSecretNotFound
	}
	if secret.DeletedDate != nil {
		return nil, ErrSecretDeleted
	}

	if input.RotationLambdaARN != "" {
		secret.RotationLambdaARN = input.RotationLambdaARN
	}

	if input.RotationRules != nil {
		if err := validateRotationRules(input.RotationRules); err != nil {
			return nil, err
		}

		secret.RotationRules = cloneRotationRules(input.RotationRules)
		secret.RotationEnabled = true
		b.ensureRotationScheduler()
	}

	rotateImmediately := true
	if input.RotateImmediately != nil {
		rotateImmediately = *input.RotateImmediately
	}

	if !rotateImmediately {
		return &RotateSecretOutput{
			ARN:  secret.ARN,
			Name: secret.Name,
		}, nil
	}

	versionID, err := b.rotateSecretLocked(secret)
	if err != nil {
		return nil, err
	}

	// Promote immediately when no Lambda invoker is wired (stub/direct-backend usage).
	// When a Lambda ARN is set AND a Lambda invoker is configured, the handler or
	// scheduler will call FinishRotation after invoking the four rotation steps.
	if input.RotationLambdaARN == "" || b.lambdaInvoker == nil {
		b.finishRotationLocked(secret, versionID)
	}

	return &RotateSecretOutput{
		ARN:       secret.ARN,
		Name:      secret.Name,
		VersionID: versionID,
	}, nil
}

// rotateSecretLocked creates a new secret version with the AWSPENDING staging label.
// Callers MUST follow up with finishRotationLocked (to promote to AWSCURRENT) or
// abortRotationLocked (to discard the pending version). Must be called with b.mu held.
func (b *InMemoryBackend) rotateSecretLocked(secret *Secret) (string, error) {
	currentVer := b.findVersion(secret, "", StagingLabelCurrent)
	if currentVer == nil {
		return "", ErrVersionNotFound
	}

	// When a rotation Lambda ARN is configured, generate a fresh secret value.
	// The Lambda lifecycle (createSecret/setSecret/testSecret/finishSecret) will
	// validate and promote the value. Without a Lambda ARN, preserve the existing value.
	newSecretString := currentVer.SecretString
	newSecretBinary := currentVer.SecretBinary

	if secret.RotationLambdaARN != "" {
		newSecretString = uuid.New().String()
		newSecretBinary = nil
	}

	versionID := generateVersionID()
	newVer := &SecretVersion{
		VersionID:     versionID,
		SecretString:  newSecretString,
		SecretBinary:  newSecretBinary,
		StagingLabels: []string{"AWSPENDING"},
		CreatedDate:   UnixTimeFloat(b.now()),
	}
	secret.Versions[versionID] = newVer

	return versionID, nil
}

// finishRotationLocked promotes the AWSPENDING version identified by versionID to
// AWSCURRENT, moving the old AWSCURRENT to AWSPREVIOUS. Must be called with b.mu held.
func (b *InMemoryBackend) finishRotationLocked(secret *Secret, versionID string) {
	newVer, ok := secret.Versions[versionID]
	if !ok {
		return
	}

	b.rotateStagingLabels(secret) // AWSCURRENT → AWSPREVIOUS, drops old AWSPREVIOUS
	newVer.StagingLabels = []string{StagingLabelCurrent}
	secret.CurrentVersionID = versionID
	secret.RotationEnabled = true
	now := UnixTimeFloat(b.now())
	secret.LastChangedDate = &now
	secret.LastRotatedDate = &now
	pruneVersions(secret)
	b.syncReplicationStatusLocked(secret)
}

// abortRotationLocked removes the AWSPENDING version, cancelling an in-progress rotation.
// Must be called with b.mu held.
func (b *InMemoryBackend) abortRotationLocked(secret *Secret, versionID string) {
	delete(secret.Versions, versionID)
}

// FinishRotation promotes the AWSPENDING version to AWSCURRENT. Called by the
// handler after all Lambda rotation steps succeed.
func (b *InMemoryBackend) FinishRotation(secretID, versionID string) error {
	b.mu.Lock("FinishRotation")
	defer b.mu.Unlock()

	id := resolveSecretID(secretID)
	secret, ok := b.secrets[id]

	if !ok || secret.DeletedDate != nil {
		return ErrSecretNotFound
	}

	b.finishRotationLocked(secret, versionID)

	return nil
}

// AbortRotation removes the AWSPENDING version, aborting an in-progress rotation.
// Called by the handler when a Lambda rotation step fails.
func (b *InMemoryBackend) AbortRotation(secretID, versionID string) error {
	b.mu.Lock("AbortRotation")
	defer b.mu.Unlock()

	id := resolveSecretID(secretID)
	secret, ok := b.secrets[id]

	if !ok || secret.DeletedDate != nil {
		return ErrSecretNotFound
	}

	b.abortRotationLocked(secret, versionID)

	return nil
}

// runLambdaRotationSteps invokes the four Lambda rotation steps (createSecret,
// setSecret, testSecret, finishSecret) for the given secret and version token.
func (b *InMemoryBackend) runLambdaRotationSteps(ctx context.Context, lambdaARN, secretID, token string) error {
	fnName := extractFunctionNameFromARN(lambdaARN)

	for _, step := range rotationSteps {
		event, _ := json.Marshal(map[string]string{
			"SecretId":           secretID,
			"ClientRequestToken": token,
			"Step":               step,
		})

		if _, _, err := b.lambdaInvoker.InvokeFunction(ctx, fnName, "RequestResponse", event); err != nil {
			return fmt.Errorf("rotation Lambda step %q failed: %w", step, err)
		}
	}

	return nil
}

func cloneRotationRules(rules *RotationRulesType) *RotationRulesType {
	if rules == nil {
		return nil
	}

	cloned := *rules
	if rules.AutomaticallyAfterDays != nil {
		days := *rules.AutomaticallyAfterDays
		cloned.AutomaticallyAfterDays = &days
	}

	return &cloned
}

// validateRotationRules checks that rotation rule values are within AWS-allowed bounds.
func validateRotationRules(rules *RotationRulesType) error {
	const (
		minRotationDays = 1
		maxRotationDays = 365
	)

	if rules.AutomaticallyAfterDays != nil {
		days := *rules.AutomaticallyAfterDays
		if days < minRotationDays || days > maxRotationDays {
			return fmt.Errorf(
				"%w: AutomaticallyAfterDays must be between %d and %d",
				ErrInvalidParameter,
				minRotationDays,
				maxRotationDays,
			)
		}
	}

	return nil
}

// GetRandomPassword generates a cryptographically random password according to the given constraints.
func (b *InMemoryBackend) GetRandomPassword(input *GetRandomPasswordInput) (*GetRandomPasswordOutput, error) {
	const (
		defaultPasswordLength = 32
		minPasswordLength     = 1
		maxPasswordLength     = 4096
	)

	length := int64(defaultPasswordLength)
	if input.PasswordLength != nil {
		length = *input.PasswordLength
	}

	if length < minPasswordLength || length > maxPasswordLength {
		return nil, fmt.Errorf(
			"%w: PasswordLength must be between %d and %d",
			ErrInvalidPasswordParameters,
			minPasswordLength,
			maxPasswordLength,
		)
	}

	pool, required, err := buildPasswordCharset(input)
	if err != nil {
		return nil, err
	}

	pw, err := randomRunes(pool, int(length))
	if err != nil {
		return nil, fmt.Errorf("random password generation: %w", err)
	}

	if input.RequireEachIncludedType {
		if int64(len(required)) > length {
			return nil, fmt.Errorf(
				"%w: PasswordLength %d is too short to include all required character types",
				ErrInvalidPasswordParameters,
				length,
			)
		}

		if injectErr := injectRequiredTypes(pw, required, input.ExcludeCharacters); injectErr != nil {
			return nil, fmt.Errorf("random password generation: %w", injectErr)
		}
	}

	return &GetRandomPasswordOutput{RandomPassword: string(pw)}, nil
}

// buildPasswordCharset constructs the character pool and the per-type groups from the input constraints.
func buildPasswordCharset(input *GetRandomPasswordInput) ([]rune, []string, error) {
	const (
		lowercase   = "abcdefghijklmnopqrstuvwxyz"
		uppercase   = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		digits      = "0123456789"
		punctuation = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"
		space       = " "
	)

	var charset strings.Builder

	var required []string

	if !input.ExcludeLowercase {
		charset.WriteString(lowercase)
		required = append(required, lowercase)
	}

	if !input.ExcludeUppercase {
		charset.WriteString(uppercase)
		required = append(required, uppercase)
	}

	if !input.ExcludeNumbers {
		charset.WriteString(digits)
		required = append(required, digits)
	}

	if !input.ExcludePunctuation {
		charset.WriteString(punctuation)
		required = append(required, punctuation)
	}

	if input.IncludeSpace {
		charset.WriteString(space)
		required = append(required, space)
	}

	filtered := filterRunes([]rune(charset.String()), input.ExcludeCharacters)
	if len(filtered) == 0 {
		return nil, nil, fmt.Errorf(
			"%w: no characters remain after applying exclusion constraints",
			ErrInvalidPasswordParameters,
		)
	}

	return filtered, required, nil
}

// randomRunes fills a slice of runes with random characters drawn from pool.
// It uses a single io.ReadFull call to fetch a random byte buffer, eliminating
// per-character big.Int allocations that are costly for long passwords.
func randomRunes(pool []rune, length int) ([]rune, error) {
	if len(pool) == 0 || length == 0 {
		return make([]rune, length), nil
	}

	poolSize := len(pool)

	// Use rejection sampling with a power-of-two mask when pool is small enough
	// to avoid modulo bias while minimising random bytes consumed.
	// For pool sizes > maxSmallPoolSize fall back to per-character cryptoRandInt.
	if poolSize <= maxSmallPoolSize {
		return randomRunesSmallPool(pool, length)
	}

	return randomRunesLargePool(pool, length)
}

// randomRunesSmallPool fills a rune slice using rejection-sampling with a byte buffer.
// It is used when pool size fits in one byte (≤ maxSmallPoolSize).
func randomRunesSmallPool(pool []rune, length int) ([]rune, error) {
	pw := make([]rune, length)
	poolSize := len(pool)

	// Find smallest mask that covers poolSize.
	mask := byte(1)
	for int(mask) < poolSize {
		mask = (mask << 1) | 1
	}

	buf := make([]byte, length*randomBufMultiplier)
	filled := 0
	offset := len(buf) // trigger initial fill

	for filled < length {
		if offset >= len(buf) {
			if _, err := io.ReadFull(rand.Reader, buf); err != nil {
				return nil, fmt.Errorf("random password generation: %w", err)
			}

			offset = 0
		}

		idx := int(buf[offset] & mask)
		offset++

		if idx < poolSize {
			pw[filled] = pool[idx]
			filled++
		}
	}

	return pw, nil
}

// randomRunesLargePool fills a rune slice using per-character cryptoRandInt.
// It is used when pool size exceeds maxSmallPoolSize.
func randomRunesLargePool(pool []rune, length int) ([]rune, error) {
	pw := make([]rune, length)

	for i := range pw {
		idx, err := cryptoRandInt(len(pool))
		if err != nil {
			return nil, err
		}

		pw[i] = pool[idx]
	}

	return pw, nil
}

// injectRequiredTypes places exactly one character from each required type group into pw,
// using distinct positions (sampling without replacement via a Fisher-Yates shuffle).
func injectRequiredTypes(pw []rune, required []string, excludeChars string) error {
	// Collect the rune groups that remain non-empty after applying excludeChars.
	nonEmptyGroups := make([][]rune, 0, len(required))

	for _, group := range required {
		groupRunes := filterRunes([]rune(group), excludeChars)
		if len(groupRunes) == 0 {
			continue
		}

		nonEmptyGroups = append(nonEmptyGroups, groupRunes)
	}

	if len(nonEmptyGroups) == 0 {
		return nil
	}

	// Shuffle positions so each group gets a unique slot.
	positions := make([]int, len(pw))
	for i := range positions {
		positions[i] = i
	}

	for i := len(positions) - 1; i > 0; i-- {
		j, err := cryptoRandInt(i + 1)
		if err != nil {
			return err
		}

		positions[i], positions[j] = positions[j], positions[i]
	}

	for i, groupRunes := range nonEmptyGroups {
		idx, err := cryptoRandInt(len(groupRunes))
		if err != nil {
			return err
		}

		pw[positions[i]] = groupRunes[idx]
	}

	return nil
}

// cryptoRandInt returns a cryptographically random non-negative integer in [0, n)
// with uniform distribution using [crypto/rand.Int].
func cryptoRandInt(n int) (int, error) {
	if n <= 0 {
		return 0, ErrCryptoRandInvalidRange
	}

	v, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		return 0, err
	}

	return int(v.Int64()), nil
}

// filterRunes returns the runes from s that are not in excludeChars.
func filterRunes(runes []rune, excludeChars string) []rune {
	if excludeChars == "" {
		return runes
	}

	result := make([]rune, 0, len(runes))

	for _, r := range runes {
		if !strings.ContainsRune(excludeChars, r) {
			result = append(result, r)
		}
	}

	return result
}

// TaggedSecretInfo contains a secret's ARN and tag snapshot.
// Used by the Resource Groups Tagging API cross-service listing.
type TaggedSecretInfo struct {
	Tags map[string]string
	ARN  string
}

// TaggedSecrets returns a snapshot of all secrets with their ARNs and tags.
// Intended for use by the Resource Groups Tagging API provider.
func (b *InMemoryBackend) TaggedSecrets() []TaggedSecretInfo {
	b.mu.RLock("TaggedSecrets")
	defer b.mu.RUnlock()

	result := make([]TaggedSecretInfo, 0, len(b.secrets))

	for _, secret := range b.secrets {
		if secret.DeletedDate != nil {
			continue
		}

		var tagMap map[string]string
		if secret.Tags != nil {
			tagMap = secret.Tags.Clone()
		}

		result = append(result, TaggedSecretInfo{ARN: secret.ARN, Tags: tagMap})
	}

	return result
}

// TagSecretByARN applies tags to the secret identified by its ARN.
func (b *InMemoryBackend) TagSecretByARN(secretARN string, newTags map[string]string) error {
	b.mu.Lock("TagSecretByARN")
	defer b.mu.Unlock()

	name := resolveSecretID(secretARN)

	secret, ok := b.secrets[name]
	if !ok {
		return fmt.Errorf("%w: %s", ErrSecretNotFound, secretARN)
	}

	if secret.Tags == nil {
		secret.Tags = tags.New(secret.Name + ".tags")
	}

	secret.Tags.Merge(newTags)

	return nil
}

// UntagSecretByARN removes the specified tag keys from the secret identified by its ARN.
func (b *InMemoryBackend) UntagSecretByARN(secretARN string, tagKeys []string) error {
	b.mu.Lock("UntagSecretByARN")
	defer b.mu.Unlock()

	name := resolveSecretID(secretARN)

	secret, ok := b.secrets[name]
	if !ok {
		return fmt.Errorf("%w: %s", ErrSecretNotFound, secretARN)
	}

	if secret.Tags != nil {
		secret.Tags.DeleteKeys(tagKeys)
	}

	return nil
}

// BatchGetSecretValue retrieves the values of multiple secrets in a single call.
func (b *InMemoryBackend) BatchGetSecretValue(input *BatchGetSecretValueInput) (*BatchGetSecretValueOutput, error) {
	if input.MaxResults != nil {
		mr := int64(*input.MaxResults)
		if err := validateMaxResults(&mr, maxResultsBatchGet); err != nil {
			return nil, err
		}
	}

	if len(input.SecretIDList) > maxSecretIDListSize {
		return nil, fmt.Errorf(
			"%w: SecretIdList must not contain more than %d entries",
			ErrInvalidParameter,
			maxSecretIDListSize,
		)
	}

	b.mu.Lock("BatchGetSecretValue")
	defer b.mu.Unlock()

	out := &BatchGetSecretValueOutput{
		SecretValues: []SecretValueEntry{},
		Errors:       []APIErrorType{},
	}

	if len(input.SecretIDList) > 0 {
		b.batchGetByIDList(input.SecretIDList, out)

		return out, nil
	}

	return b.batchGetByFilter(input, out), nil
}

// batchGetByIDList populates out with values and errors for each explicit secret ID.
// Must be called with write lock held.
func (b *InMemoryBackend) batchGetByIDList(ids []string, out *BatchGetSecretValueOutput) {
	accessDay := UnixTimeFloat(time.Now().UTC().Truncate(hoursPerDay * time.Hour))

	for _, id := range ids {
		name := resolveSecretID(id)

		secret, ok := b.secrets[name]
		if !ok {
			out.Errors = append(out.Errors, APIErrorType{
				ErrorCode: errResourceNotFoundException,
				Message:   "Secrets Manager can't find the specified secret.",
				SecretID:  id,
			})

			continue
		}

		if secret.DeletedDate != nil {
			out.Errors = append(out.Errors, APIErrorType{
				ErrorCode: "InvalidRequestException",
				Message:   "You can't perform this operation on the secret because it was deleted.",
				SecretID:  id,
			})

			continue
		}

		ver := b.findVersion(secret, "", StagingLabelCurrent)
		if ver == nil {
			out.Errors = append(out.Errors, APIErrorType{
				ErrorCode: errResourceNotFoundException,
				Message:   "Secrets Manager can't find the specified secret version.",
				SecretID:  id,
			})

			continue
		}

		secret.LastAccessedDate = &accessDay
		ver.LastAccessedDate = &accessDay
		out.SecretValues = append(out.SecretValues, secretVersionEntry(secret, ver))
	}
}

// batchGetByFilter collects and paginates secrets matching filters.
// Must be called with write lock held.
func (b *InMemoryBackend) batchGetByFilter(
	input *BatchGetSecretValueInput,
	out *BatchGetSecretValueOutput,
) *BatchGetSecretValueOutput {
	allValues := make([]SecretValueEntry, 0, len(b.secrets))
	accessDay := UnixTimeFloat(time.Now().UTC().Truncate(hoursPerDay * time.Hour))

	for _, secret := range b.secrets {
		if secret.DeletedDate != nil || !batchMatchesFilters(secret, input.Filters) {
			continue
		}

		ver := b.findVersion(secret, "", StagingLabelCurrent)
		if ver == nil {
			continue
		}

		secret.LastAccessedDate = &accessDay
		ver.LastAccessedDate = &accessDay
		allValues = append(allValues, secretVersionEntry(secret, ver))
	}

	// Sort by name for deterministic pagination.
	sort.Slice(allValues, func(i, j int) bool {
		return allValues[i].Name < allValues[j].Name
	})

	maxResults := int64(defaultMaxResults)
	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = int64(*input.MaxResults)
	}

	startIdx := parseToken(input.NextToken)
	if startIdx >= len(allValues) {
		return out
	}

	end := startIdx + int(maxResults)

	if end < len(allValues) {
		out.NextToken = strconv.Itoa(end)
	} else {
		end = len(allValues)
	}

	out.SecretValues = allValues[startIdx:end]

	return out
}

// secretVersionEntry builds a SecretValueEntry from a secret and version.
func secretVersionEntry(secret *Secret, ver *SecretVersion) SecretValueEntry {
	return SecretValueEntry{
		ARN:           secret.ARN,
		Name:          secret.Name,
		VersionID:     ver.VersionID,
		SecretString:  ver.SecretString,
		SecretBinary:  ver.SecretBinary,
		VersionStages: ver.StagingLabels,
		CreatedDate:   ver.CreatedDate,
	}
}

// batchMatchesFilters returns true if the secret matches all provided filters.
func batchMatchesFilters(secret *Secret, filters []BatchGetSecretValueFilter) bool {
	for _, f := range filters {
		switch f.Key {
		case "name":
			if !anyMatch(f.Values, secret.Name) {
				return false
			}
		case "description":
			if !anyMatch(f.Values, secret.Description) {
				return false
			}
		case "tag-key":
			if !secretHasTagKey(secret, f.Values) {
				return false
			}
		case "tag-value":
			if !secretHasTagValue(secret, f.Values) {
				return false
			}
		}
	}

	return true
}

// anyMatch returns true if target equals any of the values.
func anyMatch(values []string, target string) bool {
	return slices.Contains(values, target)
}

// CancelRotateSecret cancels an in-progress rotation by removing the AWSPENDING staging label.
func (b *InMemoryBackend) CancelRotateSecret(input *CancelRotateSecretInput) (*CancelRotateSecretOutput, error) {
	b.mu.Lock("CancelRotateSecret")
	defer b.mu.Unlock()

	name := resolveSecretID(input.SecretID)

	secret, ok := b.secrets[name]
	if !ok {
		return nil, ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is deleted", ErrSecretDeleted, input.SecretID)
	}

	const pendingLabel = "AWSPENDING"

	var canceledVersionID string

	for _, ver := range secret.Versions {
		newLabels := make([]string, 0, len(ver.StagingLabels))

		for _, lbl := range ver.StagingLabels {
			if lbl == pendingLabel {
				canceledVersionID = ver.VersionID

				continue
			}

			newLabels = append(newLabels, lbl)
		}

		ver.StagingLabels = newLabels
	}

	secret.RotationEnabled = false

	return &CancelRotateSecretOutput{
		ARN:       secret.ARN,
		Name:      secret.Name,
		VersionID: canceledVersionID,
	}, nil
}

// GetResourcePolicy retrieves the resource-based policy for a secret.
func (b *InMemoryBackend) GetResourcePolicy(input *GetResourcePolicyInput) (*GetResourcePolicyOutput, error) {
	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	name := resolveSecretID(input.SecretID)

	secret, ok := b.secrets[name]
	if !ok {
		return nil, ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is deleted", ErrSecretDeleted, input.SecretID)
	}

	policy := b.resourcePolicies[name]

	return &GetResourcePolicyOutput{
		ARN:            secret.ARN,
		Name:           secret.Name,
		ResourcePolicy: policy,
	}, nil
}

// PutResourcePolicy stores a resource-based policy for a secret.
func (b *InMemoryBackend) PutResourcePolicy(input *PutResourcePolicyInput) (*PutResourcePolicyOutput, error) {
	if input.ResourcePolicy == "" {
		return nil, fmt.Errorf("%w: ResourcePolicy must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	name := resolveSecretID(input.SecretID)

	secret, ok := b.secrets[name]
	if !ok {
		return nil, ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is deleted", ErrSecretDeleted, input.SecretID)
	}

	b.resourcePolicies[name] = input.ResourcePolicy

	return &PutResourcePolicyOutput{
		ARN:  secret.ARN,
		Name: secret.Name,
	}, nil
}

// DeleteResourcePolicy removes the resource-based policy from a secret.
func (b *InMemoryBackend) DeleteResourcePolicy(input *DeleteResourcePolicyInput) (*DeleteResourcePolicyOutput, error) {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	name := resolveSecretID(input.SecretID)

	secret, ok := b.secrets[name]
	if !ok {
		return nil, ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is deleted", ErrSecretDeleted, input.SecretID)
	}

	delete(b.resourcePolicies, name)

	return &DeleteResourcePolicyOutput{
		ARN:  secret.ARN,
		Name: secret.Name,
	}, nil
}

// replicationStatusInSync is the status used for in-sync replicas.
const (
	replicationStatusFailed     = "Failed"
	replicationStatusInProgress = "InProgress"
	replicationStatusInSync     = "InSync"
)

// ReplicateSecretToRegions adds replication configuration for the specified regions.
func (b *InMemoryBackend) ReplicateSecretToRegions(
	input *ReplicateSecretToRegionsInput,
) (*ReplicateSecretToRegionsOutput, error) {
	b.mu.Lock("ReplicateSecretToRegions")
	defer b.mu.Unlock()

	name := resolveSecretID(input.SecretID)

	secret, ok := b.secrets[name]
	if !ok {
		return nil, ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is deleted", ErrSecretDeleted, input.SecretID)
	}

	existing := b.replicationConfigs[name]
	existingByRegion := make(map[string]int, len(existing))

	for i, r := range existing {
		existingByRegion[r.Region] = i
	}

	for _, replica := range input.AddReplicaRegions {
		status := ReplicationStatusType{
			Region:        replica.Region,
			KmsKeyID:      replica.KmsKeyID,
			Status:        replicationStatusInProgress,
			StatusMessage: "replication queued",
		}

		if idx, found := existingByRegion[replica.Region]; found {
			existing[idx] = status
		} else {
			existing = append(existing, status)
		}
	}

	b.replicationConfigs[name] = existing
	b.syncReplicationStatusLocked(secret)

	return &ReplicateSecretToRegionsOutput{
		ARN:               secret.ARN,
		ReplicationStatus: b.replicationConfigs[name],
	}, nil
}

// RemoveRegionsFromReplication removes replication configuration for the specified regions.
func (b *InMemoryBackend) RemoveRegionsFromReplication(
	input *RemoveRegionsFromReplicationInput,
) (*RemoveRegionsFromReplicationOutput, error) {
	b.mu.Lock("RemoveRegionsFromReplication")
	defer b.mu.Unlock()

	name := resolveSecretID(input.SecretID)

	secret, ok := b.secrets[name]
	if !ok {
		return nil, ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is deleted", ErrSecretDeleted, input.SecretID)
	}

	toRemove := make(map[string]struct{}, len(input.RemoveReplicaRegions))

	for _, r := range input.RemoveReplicaRegions {
		toRemove[r] = struct{}{}
	}

	existing := b.replicationConfigs[name]
	remaining := make([]ReplicationStatusType, 0, len(existing))

	for _, r := range existing {
		if _, remove := toRemove[r.Region]; !remove {
			remaining = append(remaining, r)
		}
	}

	b.replicationConfigs[name] = remaining

	return &RemoveRegionsFromReplicationOutput{
		ARN:               secret.ARN,
		ReplicationStatus: remaining,
	}, nil
}

// StopReplicationToReplica promotes a replica secret to a standalone secret.
func (b *InMemoryBackend) StopReplicationToReplica(
	input *StopReplicationToReplicaInput,
) (*StopReplicationToReplicaOutput, error) {
	b.mu.Lock("StopReplicationToReplica")
	defer b.mu.Unlock()

	name := resolveSecretID(input.SecretID)

	secret, ok := b.secrets[name]
	if !ok {
		return nil, ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is deleted", ErrSecretDeleted, input.SecretID)
	}

	// In the in-memory backend, we simply remove any replication config for this secret.
	delete(b.replicationConfigs, name)

	return &StopReplicationToReplicaOutput{
		ARN: secret.ARN,
	}, nil
}

// UpdateSecretVersionStage moves or adds a staging label to a specific secret version.
func (b *InMemoryBackend) UpdateSecretVersionStage(
	input *UpdateSecretVersionStageInput,
) (*UpdateSecretVersionStageOutput, error) {
	b.mu.Lock("UpdateSecretVersionStage")
	defer b.mu.Unlock()

	name := resolveSecretID(input.SecretID)

	secret, ok := b.secrets[name]
	if !ok {
		return nil, ErrSecretNotFound
	}

	if secret.DeletedDate != nil {
		return nil, fmt.Errorf("%w: secret %s is deleted", ErrSecretDeleted, input.SecretID)
	}

	// AWS does not allow removing AWSCURRENT without moving it to another version.
	if input.VersionStage == StagingLabelCurrent && input.MoveToVersionID == "" {
		return nil, fmt.Errorf(
			"%w: AWSCURRENT staging label can only be moved to another version, not removed",
			ErrInvalidParameter,
		)
	}

	if input.MoveToVersionID != "" {
		if err := b.moveStagingLabel(secret, input); err != nil {
			return nil, err
		}
	} else if input.RemoveFromVersionID != "" {
		if err := removeLabelFromVersion(secret, input.RemoveFromVersionID, input.VersionStage); err != nil {
			return nil, err
		}
	}

	return &UpdateSecretVersionStageOutput{
		ARN:  secret.ARN,
		Name: secret.Name,
	}, nil
}

// moveStagingLabel strips a staging label from all versions and applies it to the target version.
// Must be called with write lock held.
func (b *InMemoryBackend) moveStagingLabel(secret *Secret, input *UpdateSecretVersionStageInput) error {
	if input.RemoveFromVersionID != "" {
		if _, exists := secret.Versions[input.RemoveFromVersionID]; !exists {
			return ErrVersionNotFound
		}
	}

	// Strip the label from ALL versions — a staging label belongs to exactly one version.
	for _, ver := range secret.Versions {
		ver.StagingLabels = removeLabel(ver.StagingLabels, input.VersionStage)
	}

	targetVer, exists := secret.Versions[input.MoveToVersionID]
	if !exists {
		return ErrVersionNotFound
	}

	targetVer.StagingLabels = append(targetVer.StagingLabels, input.VersionStage)

	if input.VersionStage == StagingLabelCurrent {
		secret.CurrentVersionID = input.MoveToVersionID
	}

	return nil
}

// removeLabelFromVersion removes a label from a specific version.
func removeLabelFromVersion(secret *Secret, versionID, label string) error {
	ver, exists := secret.Versions[versionID]
	if !exists {
		return ErrVersionNotFound
	}

	ver.StagingLabels = removeLabel(ver.StagingLabels, label)

	return nil
}

// removeLabel returns a copy of labels with the given label removed.
func removeLabel(labels []string, label string) []string {
	newLabels := make([]string, 0, len(labels))

	for _, lbl := range labels {
		if lbl != label {
			newLabels = append(newLabels, lbl)
		}
	}

	return newLabels
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, secret := range b.secrets {
		if secret.Tags != nil {
			secret.Tags.Close()
		}
	}

	b.secrets = make(map[string]*Secret)
	b.resourcePolicies = make(map[string]string)
	b.replicationConfigs = make(map[string][]ReplicationStatusType)
}

func (b *InMemoryBackend) ensureRotationScheduler() {
	b.schedulerOnce.Do(func() {
		go b.rotationSchedulerLoop()
	})
}

func (b *InMemoryBackend) rotationSchedulerLoop() {
	ticker := time.NewTicker(rotationSchedulerInterval)
	defer ticker.Stop()

	for now := range ticker.C {
		b.runScheduledRotations(now)
	}
}

func (b *InMemoryBackend) runScheduledRotations(now time.Time) {
	type pendingRotation struct {
		secretID  string
		versionID string
		lambdaARN string
	}

	// Phase 1: create AWSPENDING versions while holding the lock.
	b.mu.Lock("rotationScheduler")
	var pending []pendingRotation

	for id, secret := range b.secrets {
		if secret.DeletedDate != nil || !secret.RotationEnabled || secret.RotationRules == nil {
			continue
		}

		base := secret.LastRotatedDate
		if base == nil {
			base = secret.LastChangedDate
		}

		if !rotationDue(secret.RotationRules, now, base) {
			continue
		}

		versionID, err := b.rotateSecretLocked(secret)
		if err != nil {
			continue
		}

		lambdaARN := secret.RotationLambdaARN
		if b.lambdaInvoker == nil || lambdaARN == "" {
			// No Lambda configured — promote immediately while still locked.
			b.finishRotationLocked(secret, versionID)
			continue
		}

		pending = append(pending, pendingRotation{secretID: id, versionID: versionID, lambdaARN: lambdaARN})
	}

	b.mu.Unlock()

	// Phase 2: invoke Lambda WITHOUT holding the lock, then promote or abort.
	for _, p := range pending {
		lambdaErr := b.runLambdaRotationSteps(context.Background(), p.lambdaARN, p.secretID, p.versionID)
		if lambdaErr != nil {
			_ = b.AbortRotation(p.secretID, p.versionID)
		} else {
			_ = b.FinishRotation(p.secretID, p.versionID)
		}
	}
}

// rotationDue reports whether a rotation should fire at `now` given the rotation rules and
// the base time (last rotation or last change). Returns false when rules are nil or unparseable.
func rotationDue(rules *RotationRulesType, now time.Time, base *float64) bool {
	if rules == nil || base == nil {
		return false
	}

	baseTime := time.Unix(0, int64(*base*float64(time.Second)))

	if now.Before(baseTime) {
		return false
	}

	if isCronExpression(rules.ScheduleExpression) {
		next, ok := nextCronTime(rules.ScheduleExpression, baseTime)
		if !ok {
			return false
		}

		return !now.Before(next)
	}

	interval, ok := rotationInterval(rules)
	if !ok {
		return false
	}

	return now.Sub(baseTime) >= interval
}

func rotationInterval(rules *RotationRulesType) (time.Duration, bool) {
	const rateExpressionParts = 2

	if rules == nil {
		return 0, false
	}

	if rules.AutomaticallyAfterDays != nil && *rules.AutomaticallyAfterDays > 0 {
		return time.Duration(*rules.AutomaticallyAfterDays) * 24 * time.Hour, true
	}

	expr := strings.TrimSpace(rules.ScheduleExpression)
	if expr == "" {
		return 0, false
	}
	if !strings.HasPrefix(expr, "rate(") || !strings.HasSuffix(expr, ")") {
		return 0, false
	}

	payload := strings.TrimSuffix(strings.TrimPrefix(expr, "rate("), ")")
	parts := strings.Fields(payload)
	if len(parts) != rateExpressionParts {
		return 0, false
	}

	n, err := strconv.Atoi(parts[0])
	if err != nil || n <= 0 {
		return 0, false
	}

	switch parts[1] {
	case "second", "seconds":
		return time.Duration(n) * time.Second, true
	case "minute", "minutes":
		return time.Duration(n) * time.Minute, true
	case "hour", "hours":
		return time.Duration(n) * time.Hour, true
	case "day", "days":
		return time.Duration(n) * 24 * time.Hour, true
	default:
		return 0, false
	}
}

func (b *InMemoryBackend) syncReplicationStatusLocked(secret *Secret) {
	statuses, exists := b.replicationConfigs[secret.Name]
	if !exists || len(statuses) == 0 {
		return
	}

	currentVer := b.findVersion(secret, "", StagingLabelCurrent)
	if currentVer == nil {
		for i := range statuses {
			statuses[i].Status = replicationStatusFailed
			statuses[i].StatusMessage = "no current secret version to replicate"
		}
		b.replicationConfigs[secret.Name] = statuses

		return
	}

	for i := range statuses {
		statuses[i].Status = replicationStatusInSync
		statuses[i].StatusMessage = "replicated version " + currentVer.VersionID
	}

	b.replicationConfigs[secret.Name] = statuses
}

// AccountID returns the AWS account ID configured for this backend.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the AWS region configured for this backend.
func (b *InMemoryBackend) Region() string { return b.region }

// AddSecretInternal seeds the backend with a pre-built Secret for testing.
// Must not be called concurrently with other operations.
func (b *InMemoryBackend) AddSecretInternal(s *Secret) {
	b.secrets[s.Name] = s
}

// ValidateResourcePolicy validates a resource-based policy document for a secret.
// It performs basic structural validation and returns any detected issues.
func (b *InMemoryBackend) ValidateResourcePolicy(
	input *ValidateResourcePolicyInput,
) (*ValidateResourcePolicyOutput, error) {
	if input.ResourcePolicy == "" {
		return nil, fmt.Errorf("%w: ResourcePolicy must not be empty", ErrInvalidParameter)
	}

	// If a secret ID is provided, verify the secret exists.
	if input.SecretID != "" {
		b.mu.RLock("ValidateResourcePolicy")
		defer b.mu.RUnlock()

		name := resolveSecretID(input.SecretID)
		if _, ok := b.secrets[name]; !ok {
			return nil, ErrSecretNotFound
		}
	}

	// Parse the JSON policy document.
	var policy map[string]json.RawMessage
	if err := json.Unmarshal([]byte(input.ResourcePolicy), &policy); err != nil {
		return &ValidateResourcePolicyOutput{
			PolicyValidationPassed: false,
			ValidationErrors: []PolicyValidationException{
				{
					CheckName:    "SyntaxCheck",
					ErrorMessage: "Policy document is not valid JSON: " + err.Error(),
				},
			},
		}, nil
	}

	var errs []PolicyValidationException

	// Validate required top-level fields.
	if _, hasVersion := policy["Version"]; !hasVersion {
		errs = append(errs, PolicyValidationException{
			CheckName:    "VersionCheck",
			ErrorMessage: "Policy document must include a Version element.",
		})
	}

	if _, hasStatement := policy["Statement"]; !hasStatement {
		errs = append(errs, PolicyValidationException{
			CheckName:    "StatementCheck",
			ErrorMessage: "Policy document must include a Statement element.",
		})
	}

	return &ValidateResourcePolicyOutput{
		PolicyValidationPassed: len(errs) == 0,
		ValidationErrors:       errs,
	}, nil
}
