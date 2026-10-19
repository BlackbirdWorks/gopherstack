package s3files

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxClientTokenLen   = 64
	maxPolicyBytes      = 20 * 1024
	maxPrefixLen        = 1024
	maxExpirationDays   = 365
	defaultPageSize     = 100
	roleARNFields       = 6
	bucketARNFieldCount = 6
)

// CreateFileSystemInput is the backend form of CreateFileSystem.
type CreateFileSystemInput struct {
	Tags        map[string]string
	Bucket      string
	RoleARN     string
	ClientToken string
	KMSKeyID    string
	Prefix      string
}

// bucketName extracts the bucket name from an S3 bucket ARN.
func bucketName(bucketARN string) (string, error) {
	parts := strings.SplitN(bucketARN, ":", bucketARNFieldCount)
	if len(parts) != bucketARNFieldCount || parts[0] != "arn" || parts[2] != "s3" ||
		parts[3] != "" || parts[4] != "" || parts[5] == "" || strings.Contains(parts[5], "/") {
		return "", validation("Bucket must be an S3 bucket ARN (arn:aws:s3:::bucket-name)")
	}

	return parts[5], nil
}

func validateRoleARN(roleARN string) error {
	parts := strings.SplitN(roleARN, ":", roleARNFields)
	if len(parts) != roleARNFields || parts[0] != "arn" || parts[2] != "iam" ||
		!strings.HasPrefix(parts[5], "role/") || len(parts[5]) == len("role/") {
		return validation("RoleArn must be an IAM role ARN")
	}

	return nil
}

// CreateFileSystem creates a file system on an S3 bucket; created is false for an idempotent replay.
func (b *InMemoryBackend) CreateFileSystem(
	ctx context.Context,
	in CreateFileSystemInput,
) (*FileSystem, bool, error) {
	name, err := validateCreateFileSystem(in)
	if err != nil {
		return nil, false, err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateFileSystem")
	defer b.mu.Unlock()

	b.sweepLocked()

	if in.ClientToken != "" {
		for _, existing := range b.fileSystems.All() {
			if existing.Region != region || existing.ClientToken != in.ClientToken {
				continue
			}

			if existing.Bucket != in.Bucket || existing.Prefix != in.Prefix ||
				existing.RoleARN != in.RoleARN || existing.KMSKeyID != in.KMSKeyID {
				return nil, false, conflict("FileSystem", existing.ID,
					"ClientToken was already used with different parameters")
			}

			return b.fsView(existing), false, nil
		}
	}

	if b.buckets != nil && !b.buckets.BucketExists(ctx, name) {
		return nil, false, validation("Bucket " + in.Bucket + " does not exist")
	}

	id := awsid.New("fs-")
	rec := &FileSystem{
		ID:          id,
		ARN:         arn.Build("s3files", region, b.accountID, "file-system/"+id),
		Bucket:      in.Bucket,
		Prefix:      in.Prefix,
		RoleARN:     in.RoleARN,
		KMSKeyID:    in.KMSKeyID,
		ClientToken: in.ClientToken,
		OwnerID:     b.accountID,
		Region:      region,
		Tags:        maps.Clone(in.Tags),
		Lifecycle:   b.newLifecycle(),
	}
	b.fileSystems.Put(rec)

	return b.fsView(rec), true, nil
}

func validateCreateFileSystem(in CreateFileSystemInput) (string, error) {
	if in.Bucket == "" || in.RoleARN == "" {
		return "", validation("Bucket and RoleArn are required")
	}

	name, err := bucketName(in.Bucket)
	if err != nil {
		return "", err
	}

	if err = validateRoleARN(in.RoleARN); err != nil {
		return "", err
	}

	if len(in.ClientToken) > maxClientTokenLen {
		return "", validation("ClientToken must be at most 64 characters")
	}

	if len(in.Prefix) > maxPrefixLen {
		return "", validation("Prefix is too long")
	}

	return name, validateTags(in.Tags)
}

// GetFileSystem returns a file system by ID or ARN.
func (b *InMemoryBackend) GetFileSystem(ctx context.Context, ref string) (*FileSystem, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetFileSystem")
	defer b.mu.RUnlock()

	fs, err := b.liveFileSystem(region, ref)
	if err != nil {
		return nil, err
	}

	return b.fsView(fs), nil
}

// ListFileSystems lists file systems, optionally restricted to one bucket ARN.
func (b *InMemoryBackend) ListFileSystems(
	ctx context.Context,
	bucket string,
	maxResults int,
	token string,
) (page.Page[*FileSystem], error) {
	if err := validatePaging(maxResults, token); err != nil {
		return page.Page[*FileSystem]{}, err
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("ListFileSystems")
	defer b.mu.RUnlock()

	var all []*FileSystem

	for _, fs := range b.fileSystems.All() {
		if _, gone := b.state(&fs.Lifecycle); gone || fs.Region != region || (bucket != "" && fs.Bucket != bucket) {
			continue
		}

		all = append(all, b.fsView(fs))
	}

	slices.SortFunc(all, func(x, y *FileSystem) int {
		return strings.Compare(x.ID, y.ID)
	})

	return page.New(all, token, maxResults, defaultPageSize), nil
}

func validatePaging(maxResults int, token string) error {
	if maxResults < 0 {
		return validation("MaxResults must be at least 1")
	}

	return validatePageToken(token)
}

func validatePageToken(token string) error {
	if err := page.ValidateToken(token); err != nil {
		return validation("Invalid NextToken")
	}

	return nil
}

// DeleteFileSystem deletes a file system that has no mount targets or access points.
func (b *InMemoryBackend) DeleteFileSystem(ctx context.Context, ref string, _ bool) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteFileSystem")
	defer b.mu.Unlock()

	b.sweepLocked()

	fs, err := b.liveFileSystem(region, ref)
	if err != nil {
		return err
	}

	for _, mt := range b.mountTargets.All() {
		if mt.FileSystemID == fs.ID {
			return conflict("FileSystem", fs.ID, "File system "+fs.ID+" still has mount targets")
		}
	}

	for _, ap := range b.accessPoints.All() {
		if ap.FileSystemID == fs.ID {
			return conflict("FileSystem", fs.ID, "File system "+fs.ID+" still has access points")
		}
	}

	if b.beginDelete(&fs.Lifecycle) {
		b.fileSystems.Delete(fs.ID)
	}

	return nil
}

// PutFileSystemPolicy sets the resource policy of a file system.
func (b *InMemoryBackend) PutFileSystemPolicy(ctx context.Context, ref, policy string) error {
	if err := validatePolicy(policy); err != nil {
		return err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("PutFileSystemPolicy")
	defer b.mu.Unlock()

	fs, err := b.liveFileSystem(region, ref)
	if err != nil {
		return err
	}

	fs.Policy = policy

	return nil
}

func validatePolicy(policy string) error {
	if policy == "" || len(policy) > maxPolicyBytes {
		return validation("Policy must be between 1 and 20480 bytes")
	}

	var doc map[string]json.RawMessage
	if err := json.Unmarshal([]byte(policy), &doc); err != nil {
		return validation("Policy is not a valid JSON document")
	}

	if _, ok := doc["Statement"]; !ok {
		return validation("Policy must contain a Statement")
	}

	return nil
}

// GetFileSystemPolicy returns the resource policy and the file system's ID.
func (b *InMemoryBackend) GetFileSystemPolicy(ctx context.Context, ref string) (string, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetFileSystemPolicy")
	defer b.mu.RUnlock()

	fs, err := b.liveFileSystem(region, ref)
	if err != nil {
		return "", "", err
	}

	if fs.Policy == "" {
		return "", "", notFound("File system " + fs.ID + " has no policy")
	}

	return fs.ID, fs.Policy, nil
}

// DeleteFileSystemPolicy removes the resource policy of a file system.
func (b *InMemoryBackend) DeleteFileSystemPolicy(ctx context.Context, ref string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteFileSystemPolicy")
	defer b.mu.Unlock()

	fs, err := b.liveFileSystem(region, ref)
	if err != nil {
		return err
	}

	if fs.Policy == "" {
		return notFound("File system " + fs.ID + " has no policy")
	}

	fs.Policy = ""

	return nil
}

// PutSyncInput is the backend form of PutSynchronizationConfiguration.
type PutSyncInput struct {
	LatestVersion *int32
	ImportRules   []ImportDataRule
	ExpireRules   []ExpirationDataRule
}

func validateSyncRules(in PutSyncInput) error {
	for _, r := range in.ImportRules {
		if r.Prefix == nil || r.SizeLessThan == nil || *r.SizeLessThan < 0 ||
			len(*r.Prefix) > maxPrefixLen ||
			(r.Trigger != triggerFileAccess && r.Trigger != triggerDirectoryFirstAccess) {
			return validation("Invalid import data rule")
		}
	}

	for _, r := range in.ExpireRules {
		if r.DaysAfterLastAccess == nil || *r.DaysAfterLastAccess < 1 || *r.DaysAfterLastAccess > maxExpirationDays {
			return validation(fmt.Sprintf("DaysAfterLastAccess must be between 1 and %d", maxExpirationDays))
		}
	}

	return nil
}

// PutSynchronizationConfiguration creates or replaces the sync configuration, with optimistic versioning.
func (b *InMemoryBackend) PutSynchronizationConfiguration(ctx context.Context, ref string, in PutSyncInput) error {
	if in.ImportRules == nil || in.ExpireRules == nil {
		return validation("ImportDataRules and ExpirationDataRules are required")
	}

	if err := validateSyncRules(in); err != nil {
		return err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("PutSynchronizationConfiguration")
	defer b.mu.Unlock()

	fs, err := b.liveFileSystem(region, ref)
	if err != nil {
		return err
	}

	var current int32
	if fs.Sync != nil {
		current = fs.Sync.Version
	}

	if (in.LatestVersion == nil && current != 0) || (in.LatestVersion != nil && *in.LatestVersion != current) {
		return conflict("FileSystem", fs.ID, fmt.Sprintf(
			"LatestVersionNumber does not match the current configuration version %d", current))
	}

	fs.Sync = &SyncConfiguration{
		ImportDataRules:     slices.Clone(in.ImportRules),
		ExpirationDataRules: slices.Clone(in.ExpireRules),
		Version:             current + 1,
	}

	return nil
}

// GetSynchronizationConfiguration returns the sync configuration of a file system.
func (b *InMemoryBackend) GetSynchronizationConfiguration(ctx context.Context, ref string) (*SyncConfiguration, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetSynchronizationConfiguration")
	defer b.mu.RUnlock()

	fs, err := b.liveFileSystem(region, ref)
	if err != nil {
		return nil, err
	}

	if fs.Sync == nil {
		return nil, notFound("File system " + fs.ID + " has no synchronization configuration")
	}

	cp := *fs.Sync
	cp.ImportDataRules = slices.Clone(fs.Sync.ImportDataRules)
	cp.ExpirationDataRules = slices.Clone(fs.Sync.ExpirationDataRules)

	return &cp, nil
}
