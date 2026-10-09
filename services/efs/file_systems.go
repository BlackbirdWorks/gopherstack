package efs

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	maxProvisionedThroughputMib   = 3414
	defaultProvisionedLimitMib    = 1024
	provisionedThroughputRangeMsg = "ProvisionedThroughputInMibps must be between 1 and 3414"
)

// provisionedThroughputLimit is the per-region provisioned write-throughput quota in MiBps
// (efs limits page: 3.33 GiBps in us-east-1/us-east-2/us-west-2/eu-west-1, 1 GiBps elsewhere;
// the SDK documents 1-3414 with the upper limit depending on Region).
func provisionedThroughputLimit(region string) float64 {
	switch region {
	case "us-east-1", "us-east-2", "us-west-2", "eu-west-1":
		return maxProvisionedThroughputMib
	default:
		return defaultProvisionedLimitMib
	}
}

// checkProvisionedQuota rejects a value inside the SDK's 1-3414 range that exceeds the region quota.
func checkProvisionedQuota(region string, mib float64) error {
	if limit := provisionedThroughputLimit(region); mib > limit {
		return fmt.Errorf(
			"%w: provisioned throughput %g MiB/s exceeds the %g MiB/s limit in %s",
			ErrThroughputLimitExceeded, mib, limit, region,
		)
	}

	return nil
}

// validateProvisionedThroughput checks provisioned throughput constraints.
func validateProvisionedThroughput(region, mode string, mib float64) error {
	if mode == throughputModeProvisioned {
		if mib < 1 || mib > maxProvisionedThroughputMib {
			return fmt.Errorf("%w: %s when ThroughputMode is provisioned, got %g",
				ErrValidation, provisionedThroughputRangeMsg, mib)
		}

		return checkProvisionedQuota(region, mib)
	} else if mib != 0 {
		return fmt.Errorf(
			"%w: ProvisionedThroughputInMibps is only valid when ThroughputMode is provisioned",
			ErrValidation,
		)
	}

	return nil
}

// validateCreateFSRequest validates and normalizes a CreateFileSystemRequest,
// returning the resolved KMS key ID on success.
func validateCreateFSRequest(region string, req *CreateFileSystemRequest) (string, error) {
	if len(req.CreationToken) > maxCreationTokenLen {
		return "", fmt.Errorf(
			"%w: CreationToken length must be 1-%d, got %d",
			ErrValidation,
			maxCreationTokenLen,
			len(req.CreationToken),
		)
	}

	if err := validateTags(req.Tags); err != nil {
		return "", err
	}

	if req.PerformanceMode == "" {
		req.PerformanceMode = performanceModeGeneral
	}
	if req.ThroughputMode == "" {
		req.ThroughputMode = throughputModeBursting
	}

	if req.PerformanceMode != performanceModeGeneral &&
		req.PerformanceMode != performanceModeMaxIO {
		return "", fmt.Errorf(
			"%w: invalid PerformanceMode %q, must be generalPurpose or maxIO",
			ErrValidation,
			req.PerformanceMode,
		)
	}
	if req.ThroughputMode != throughputModeBursting &&
		req.ThroughputMode != throughputModeProvisioned &&
		req.ThroughputMode != throughputModeElastic {
		return "", fmt.Errorf(
			"%w: invalid ThroughputMode %q, must be bursting, provisioned, or elastic",
			ErrValidation,
			req.ThroughputMode,
		)
	}

	if err := validateProvisionedThroughput(region, req.ThroughputMode, req.ProvisionedThroughputMib); err != nil {
		return "", err
	}

	kmsKeyID := req.KmsKeyID
	if req.Encrypted && kmsKeyID == "" {
		kmsKeyID = managedKMSKeyARN
	}
	if !req.Encrypted && kmsKeyID != "" {
		return "", fmt.Errorf(
			"%w: KmsKeyID can only be specified when Encrypted is true",
			ErrValidation,
		)
	}

	return kmsKeyID, nil
}

// applyInitialBackupPolicy sets the backup policy a newly created file
// system starts with, per CreateFileSystemInput.Backup's documented default:
// false, or true when AvailabilityZoneName is set (One Zone). Must be called
// while holding b.mu.
func (b *InMemoryBackend) applyInitialBackupPolicy(region, id string, req CreateFileSystemRequest) {
	enableBackup := req.AvailabilityZoneName != ""
	if req.Backup != nil {
		enableBackup = *req.Backup
	}
	if enableBackup {
		b.backupStore(region)[id] = backupStatusEnabled
	}
}

// checkFileSystemIdempotency returns found=true when req.CreationToken already maps to
// an existing file system, along with its copy and either ErrCreationTokenExists (args
// match) or ErrAlreadyExists (args differ). CreateFileSystem's only caller must return
// (fs, err) verbatim when found is true, and otherwise proceed to create a new one.
// Callers must hold b.mu.
func (b *InMemoryBackend) checkFileSystemIdempotency(
	region string,
	tokenIdx map[string]string,
	req CreateFileSystemRequest,
) (*FileSystem, bool, error) {
	existingID, ok := tokenIdx[req.CreationToken]
	if !ok {
		return nil, false, nil
	}

	existing, _ := b.fileSystems.Get(regionKey(region, existingID))
	cp := *existing

	if existing.PerformanceMode == req.PerformanceMode &&
		existing.ThroughputMode == req.ThroughputMode &&
		existing.Encrypted == req.Encrypted &&
		existing.KmsKeyID == req.KmsKeyID &&
		existing.AvailabilityZoneName == req.AvailabilityZoneName {
		return &cp, true, fmt.Errorf(
			"%w: file system with token %s already exists (identical args)",
			ErrCreationTokenExists,
			req.CreationToken,
		)
	}

	return &cp, true, fmt.Errorf(
		"%w: file system with token %s already exists with different parameters (FileSystemId: %s)",
		ErrAlreadyExists,
		req.CreationToken,
		existing.FileSystemID,
	)
}

// CreateFileSystem creates a new EFS file system.
func (b *InMemoryBackend) CreateFileSystem(
	ctx context.Context,
	req CreateFileSystemRequest,
) (*FileSystem, error) {
	region := getRegion(ctx, b.region)

	kmsKeyID, err := validateCreateFSRequest(region, &req)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateFileSystem")
	defer b.mu.Unlock()

	tokenIdx := b.tokenIdxStore(region)

	// O(1) idempotency check via creation-token index.
	if cp, found, idemErr := b.checkFileSystemIdempotency(region, tokenIdx, req); found {
		return cp, idemErr
	}

	if len(b.fileSystemsByRegion.Get(region)) >= b.limits.fileSystemsPerAccount {
		return nil, fileSystemLimitExceededErr(b.limits.fileSystemsPerAccount)
	}

	id := "fs-" + uuid.NewString()[:8]
	fsARN := arn.Build("elasticfilesystem", region, b.accountID, "file-system/"+id)
	t := tags.New("efs.filesystem." + id + ".tags")

	tagCopy := make(map[string]string, len(req.Tags))
	maps.Copy(tagCopy, req.Tags)

	if len(tagCopy) > 0 {
		t.Merge(tagCopy)
	}

	name := req.Tags["Name"]

	initialState := statusAvailable
	if b.fsActivationDelay > 0 {
		initialState = statusCreating
	}

	fs := &FileSystem{
		FileSystemID:                   id,
		FileSystemArn:                  fsARN,
		CreationToken:                  req.CreationToken,
		Name:                           name,
		PerformanceMode:                req.PerformanceMode,
		ThroughputMode:                 req.ThroughputMode,
		LifeCycleState:                 initialState,
		Encrypted:                      req.Encrypted,
		KmsKeyID:                       kmsKeyID,
		AvailabilityZoneName:           req.AvailabilityZoneName,
		ProvisionedThroughputMib:       req.ProvisionedThroughputMib,
		ReplicationOverwriteProtection: protectionDisabled,
		AccountID:                      b.accountID,
		Region:                         region,
		CreationTime:                   time.Now().UTC(),
		Tags:                           t,
	}
	b.fileSystems.Put(fs)
	b.fileSystemsByARN.Put(fs)
	tokenIdx[req.CreationToken] = id

	b.applyInitialBackupPolicy(region, id, req)

	// When a non-zero activation delay is configured, the AWS "creating" ->
	// "available" lifecycle transition is resolved lazily by
	// effectiveFileSystemState instead of a background goroutine (see
	// PARITY.md's leaks note).
	cp := *fs
	cp.LifeCycleState = b.effectiveFileSystemState(fs)

	return &cp, nil
}

// DescribeFileSystems returns file systems, optionally filtered by ID or creation token, with pagination support.
func (b *InMemoryBackend) DescribeFileSystems(
	ctx context.Context,
	fileSystemID, creationToken, marker string,
	maxItems int,
) ([]*FileSystem, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeFileSystems")
	defer b.mu.RUnlock()

	if fileSystemID != "" {
		fs, ok := b.fileSystems.Get(regionKey(region, fileSystemID))
		if !ok {
			return nil, "", fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
		}
		cp := *fs
		cp.LifeCycleState = b.effectiveFileSystemState(fs)

		return []*FileSystem{&cp}, "", nil
	}

	regionFS := b.fileSystemsByRegion.Get(region)

	if creationToken != "" {
		for _, fs := range regionFS {
			if fs.CreationToken == creationToken {
				cp := *fs
				cp.LifeCycleState = b.effectiveFileSystemState(fs)

				return []*FileSystem{&cp}, "", nil
			}
		}

		return []*FileSystem{}, "", nil
	}

	all := make([]*FileSystem, 0, len(regionFS))
	for _, fs := range regionFS {
		cp := *fs
		cp.LifeCycleState = b.effectiveFileSystemState(fs)
		all = append(all, &cp)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].FileSystemID < all[j].FileSystemID })

	return paginate(all, marker, maxItems, func(fs *FileSystem) string { return fs.FileSystemID })
}

// DeleteFileSystem deletes a file system by ID.
// Returns ErrFileSystemInUse if mount targets, access points, or a
// replication configuration exist for it.
func (b *InMemoryBackend) DeleteFileSystem(ctx context.Context, fileSystemID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteFileSystem")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems.Get(regionKey(region, fileSystemID))
	if !ok {
		return fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	// O(1) conflict check via indexes: reject delete if mount targets or access points exist.
	if b.mtSubnetIdx[region] != nil && len(b.mtSubnetIdx[region][fileSystemID]) > 0 {
		return fmt.Errorf(
			"%w: file system %s has existing mount targets",
			ErrFileSystemInUse,
			fileSystemID,
		)
	}

	if b.apByFS[region] != nil && len(b.apByFS[region][fileSystemID]) > 0 {
		return fmt.Errorf(
			"%w: file system %s has existing access points",
			ErrFileSystemInUse,
			fileSystemID,
		)
	}

	if _, exists := b.replicationConfigs.Get(regionKey(region, fileSystemID)); exists {
		return fmt.Errorf(
			"%w: file system %s is part of an EFS replication configuration; delete the replication "+
				"configuration first",
			ErrFileSystemInUse,
			fileSystemID,
		)
	}

	b.fileSystemsByARN.Delete(regionKey(region, fs.FileSystemArn))
	// Remove from creation-token index so the token can be reused.
	if b.creationTokenIdx[region] != nil {
		delete(b.creationTokenIdx[region], fs.CreationToken)
	}

	fs.Tags.Close()
	b.fileSystems.Delete(regionKey(region, fileSystemID))
	delete(b.lifecycleStore(region), fileSystemID)
	delete(b.backupStore(region), fileSystemID)
	delete(b.fsPolicyStore(region), fileSystemID)

	return nil
}

// applyThroughputModeChange validates and applies a throughput mode change to
// a file system. Must be called under b.mu write lock. UpdateFileSystem (this
// helper's only caller) declares BadRequest, never ValidationException, for
// malformed input (efs@v1.44.4 deserializers.go).
func (b *InMemoryBackend) applyThroughputModeChange(
	region string,
	fs *FileSystem,
	req UpdateFileSystemRequest,
) error {
	if req.ThroughputMode != throughputModeBursting &&
		req.ThroughputMode != throughputModeProvisioned &&
		req.ThroughputMode != throughputModeElastic {
		return fmt.Errorf(
			"%w: invalid ThroughputMode %q, must be bursting, provisioned, or elastic",
			ErrBadRequest,
			req.ThroughputMode,
		)
	}

	if !fs.LastThroughputChange.IsZero() &&
		time.Since(fs.LastThroughputChange) < throughputCooldown {
		return fmt.Errorf(
			"%w: throughput mode was last changed at %s; must wait 24 hours between changes",
			ErrTooManyRequests,
			fs.LastThroughputChange.Format(time.RFC3339),
		)
	}

	if req.ThroughputMode == throughputModeProvisioned {
		if req.ProvisionedThroughputMib < 1 || req.ProvisionedThroughputMib > maxProvisionedThroughputMib {
			return fmt.Errorf("%w: %s when ThroughputMode is provisioned, got %g",
				ErrBadRequest, provisionedThroughputRangeMsg, req.ProvisionedThroughputMib)
		}

		if err := checkProvisionedQuota(region, req.ProvisionedThroughputMib); err != nil {
			return err
		}
	}

	fs.ThroughputMode = req.ThroughputMode
	fs.LastThroughputChange = time.Now().UTC()

	// types.FileSystemDescription: provisioned throughput is "Valid for ... ThroughputMode set to provisioned".
	if req.ThroughputMode != throughputModeProvisioned {
		fs.ProvisionedThroughputMib = 0
	}

	return nil
}

// effectiveFileSystemState resolves fs's currently-visible LifeCycleState,
// lazily promoting "creating" to "available" once fsActivationDelay has
// elapsed since CreationTime instead of a background goroutine (see
// PARITY.md's leaks note). Pure: never mutates fs. Callers must hold b.mu
// (read or write).
func (b *InMemoryBackend) effectiveFileSystemState(fs *FileSystem) string {
	if fs.LifeCycleState == statusCreating && b.fsActivationDelay > 0 &&
		!time.Now().Before(fs.CreationTime.Add(b.fsActivationDelay)) {
		return statusAvailable
	}

	return fs.LifeCycleState
}

// checkFileSystemAvailable returns ErrIncorrectFileSystemLifeCycleState unless fs is in
// the "available" state, per the CreateMountTarget precondition
// (api_op_CreateMountTarget.go:29-30) shared by every op that declares the same error.
// Callers must hold b.mu.
func (b *InMemoryBackend) checkFileSystemAvailable(fs *FileSystem) error {
	if state := b.effectiveFileSystemState(fs); state != statusAvailable {
		return fmt.Errorf(
			"%w: file system %s is in lifecycle state %q, not %q",
			ErrIncorrectFileSystemLifeCycleState,
			fs.FileSystemID,
			state,
			statusAvailable,
		)
	}

	return nil
}

// UpdateFileSystem updates throughput settings for a file system.
// Enforces a 24-hour cooldown between throughput mode changes.
func (b *InMemoryBackend) UpdateFileSystem(
	ctx context.Context,
	fileSystemID string,
	req UpdateFileSystemRequest,
) (*FileSystem, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateFileSystem")
	defer b.mu.Unlock()

	fs, ok := b.fileSystems.Get(regionKey(region, fileSystemID))
	if !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}
	if err := b.checkFileSystemAvailable(fs); err != nil {
		return nil, err
	}

	if req.ThroughputMode != "" {
		if err := b.applyThroughputModeChange(region, fs, req); err != nil {
			return nil, err
		}
	}

	if req.ProvisionedThroughputMib != 0 {
		if fs.ThroughputMode != throughputModeProvisioned {
			return nil, fmt.Errorf(
				"%w: ProvisionedThroughputInMibps is only valid when ThroughputMode is provisioned",
				ErrBadRequest,
			)
		}
		if req.ProvisionedThroughputMib < 1 || req.ProvisionedThroughputMib > maxProvisionedThroughputMib {
			return nil, fmt.Errorf("%w: %s, got %g",
				ErrBadRequest, provisionedThroughputRangeMsg, req.ProvisionedThroughputMib)
		}

		if err := checkProvisionedQuota(region, req.ProvisionedThroughputMib); err != nil {
			return nil, err
		}
		fs.ProvisionedThroughputMib = req.ProvisionedThroughputMib
	}

	cp := *fs

	return &cp, nil
}

// AddFileSystemInternal inserts a pre-built FileSystem directly into the backend (test seed helper).
func (b *InMemoryBackend) AddFileSystemInternal(fs *FileSystem) {
	b.mu.Lock("AddFileSystemInternal")
	defer b.mu.Unlock()

	region := fs.Region
	if region == "" {
		region = regionFromARN(fs.FileSystemArn, b.region)
		fs.Region = region
	}

	b.fileSystems.Put(fs)
	b.fileSystemsByARN.Put(fs)
}
