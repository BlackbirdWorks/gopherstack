package s3

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // MD5 required for S3 ETag compatibility
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // SHA1 required for S3 checksum compatibility
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"maps"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type md5ContextKey struct{}
type sseContextKey struct{}

var md5Key = md5ContextKey{} //nolint:gochecknoglobals // internal context key
var sseKey = sseContextKey{} //nolint:gochecknoglobals // internal context key

// objectVersionIDBytes is the number of random bytes used to generate a version ID.
// 16 bytes produces a 32-character lowercase hex string.
const objectVersionIDBytes = 16

// newObjectVersionID generates a random S3-compatible version ID using 16 random
// bytes encoded as a 32-character lowercase hex string. This matches the format
// expected by AWS clients better than the previous Unix-nanosecond approach.
func newObjectVersionID() string {
	b := make([]byte, objectVersionIDBytes)
	if _, err := rand.Read(b); err != nil {
		// Fallback to time-based ID if crypto/rand fails (should never happen).
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	return hex.EncodeToString(b)
}

const defaultRegionName = config.DefaultRegion

// objectChecksums holds the optional checksum values supplied with a PutObject request.
type objectChecksums struct {
	crc32, crc32c, sha1, sha256, crc64nvme *string
}

// populateComputed fills the appropriate checksum field when the client requested
// server-side computation (supplied algorithm but no value).
func (c *objectChecksums) populateComputed(computed, algo string) {
	if computed == "" {
		return
	}

	switch algo {
	case ChecksumCRC32:
		if c.crc32 == nil {
			c.crc32 = aws.String(computed)
		}
	case ChecksumCRC32C:
		if c.crc32c == nil {
			c.crc32c = aws.String(computed)
		}
	case ChecksumSHA1:
		if c.sha1 == nil {
			c.sha1 = aws.String(computed)
		}
	case ChecksumSHA256:
		if c.sha256 == nil {
			c.sha256 = aws.String(computed)
		}
	case ChecksumCRC64NVME:
		if c.crc64nvme == nil {
			c.crc64nvme = aws.String(computed)
		}
	}
}

var _ StorageBackend = (*InMemoryBackend)(nil)

// getRegionFromS3Context extracts the region from S3 request context.
// Returns the default region if region is not found in context.
func getRegionFromS3Context(ctx context.Context, defaultRegion string) string {
	if region, ok := ctx.Value(regionContextKey{}).(string); ok && region != "" {
		return region
	}

	return defaultRegion
}

type InMemoryBackend struct {
	buckets             map[string]map[string]*StoredBucket
	bucketIndex         map[string]string // name → region for O(1) cross-region lookup
	tags                map[string][]types.Tag
	uploads             map[string]map[string]*StoredMultipartUpload // bucket → uploadID → upload
	mu                  *lockmetrics.RWMutex
	compressor          Compressor
	defaultRegion       string
	compressionMinBytes int
	// skipMultipartSizeCheck disables the 5 MiB minimum part size check during
	// CompleteMultipartUpload. This is intended for use in unit tests only.
	skipMultipartSizeCheck bool
}

// WithSkipMultipartSizeCheck disables the 5 MiB minimum non-last-part size
// enforcement in CompleteMultipartUpload. Use only in tests.
func (b *InMemoryBackend) WithSkipMultipartSizeCheck() *InMemoryBackend {
	b.skipMultipartSizeCheck = true

	return b
}

func NewInMemoryBackend(compressor Compressor) *InMemoryBackend {
	return &InMemoryBackend{
		buckets:       make(map[string]map[string]*StoredBucket),
		bucketIndex:   make(map[string]string),
		compressor:    compressor,
		defaultRegion: defaultRegionName,
		mu:            lockmetrics.New("s3"),
	}
}

// WithCompressionMinBytes sets the minimum object size (in bytes) below which
// gzip compression is skipped. A value of 0 compresses all objects regardless
// of size (the original behaviour). Negative values are clamped to 0 to
// prevent misconfiguration (e.g., via env/flags) from silently changing semantics.
func (b *InMemoryBackend) WithCompressionMinBytes(n int) *InMemoryBackend {
	if n < 0 {
		n = 0
	}

	b.compressionMinBytes = n

	return b
}

// getBucket returns the bucket for a given name, returning ErrNoSuchBucket when the
// bucket does not exist or is pending async deletion. The caller must hold at least b.mu.RLock.
// bucketIndex provides O(1) name→region resolution, so no region scan is needed.
func (b *InMemoryBackend) getBucket(name string) (*StoredBucket, error) {
	region, ok := b.bucketIndex[name]
	if !ok {
		return nil, ErrNoSuchBucket
	}

	bucket := b.buckets[region][name]
	if bucket == nil || bucket.DeletePending {
		return nil, ErrNoSuchBucket
	}

	return bucket, nil
}

// BucketRegion returns the region a bucket is stored in, or "" if the bucket
// does not exist. Safe for concurrent use.
func (b *InMemoryBackend) BucketRegion(name string) string {
	b.mu.RLock("BucketRegion")
	defer b.mu.RUnlock()

	return b.bucketIndex[name]
}

// SetDefaultRegion sets the default region for this backend.
func (b *InMemoryBackend) SetDefaultRegion(region string) {
	if region == "" {
		region = defaultRegionName
	}
	b.defaultRegion = region
}

func (b *InMemoryBackend) CreateBucket(
	ctx context.Context,
	input *s3.CreateBucketInput,
) (*s3.CreateBucketOutput, error) {
	// Prefer the LocationConstraint from the input (set by the SDK for non-us-east-1
	// regions) over the region extracted from the context, so the bucket is stored
	// in the region the caller actually requested.
	region := getRegionFromS3Context(ctx, b.defaultRegion)
	if input.CreateBucketConfiguration != nil &&
		input.CreateBucketConfiguration.LocationConstraint != "" {
		region = string(input.CreateBucketConfiguration.LocationConstraint)
	}

	b.mu.Lock("CreateBucket")
	defer b.mu.Unlock()

	bucketName := *input.Bucket

	// Use bucketIndex for O(1) global-uniqueness check. Since this is a
	// single-tenant mock, a pre-existing bucket is always owned by the
	// caller → return BucketAlreadyOwnedByYou (not BucketAlreadyExists).
	// Pending-delete buckets remain in the index and still block re-creation,
	// matching real S3 behaviour (you must wait for deletion to complete).
	if _, exists := b.bucketIndex[bucketName]; exists {
		return nil, ErrBucketAlreadyOwnedByYou
	}

	// Initialize region map if it doesn't exist
	if _, exists := b.buckets[region]; !exists {
		b.buckets[region] = make(map[string]*StoredBucket)
	}

	b.buckets[region][bucketName] = &StoredBucket{
		Name:         bucketName,
		CreationDate: time.Now().UTC(),
		Objects:      make(map[string]*StoredObject),
		// Versioning is intentionally not set: new buckets have never had versioning
		// configured, which AWS represents as an empty VersioningConfiguration element.
		mu:                        lockmetrics.New("s3.bucket." + bucketName),
		AnalyticsConfigs:          make(map[string]string),
		IntelligentTieringConfigs: make(map[string]string),
		InventoryConfigs:          make(map[string]string),
		MetricsConfigs:            make(map[string]string),
	}
	b.bucketIndex[bucketName] = region

	return &s3.CreateBucketOutput{
		Location: aws.String("/" + bucketName),
	}, nil
}

func (b *InMemoryBackend) DeleteBucket(
	_ context.Context,
	input *s3.DeleteBucketInput,
) (*s3.DeleteBucketOutput, error) {
	bucketName := *input.Bucket

	b.mu.Lock("DeleteBucket")
	defer b.mu.Unlock()

	// Use bucketIndex for O(1) lookup. Pending buckets remain in the index
	// so that idempotent deletes can be detected without a region scan.
	region, ok := b.bucketIndex[bucketName]
	if !ok {
		return nil, ErrNoSuchBucket
	}

	bucket := b.buckets[region][bucketName]
	if bucket == nil {
		return nil, ErrNoSuchBucket
	}

	// If already pending deletion, return success (idempotent).
	if bucket.DeletePending {
		return &s3.DeleteBucketOutput{}, nil
	}

	// Mark bucket as pending — the Janitor will drain its objects and remove it.
	bucket.DeletePending = true

	return &s3.DeleteBucketOutput{}, nil
}

func (b *InMemoryBackend) HeadBucket(
	_ context.Context,
	input *s3.HeadBucketInput,
) (*s3.HeadBucketOutput, error) {
	b.mu.RLock("HeadBucket")
	defer b.mu.RUnlock()

	if _, err := b.getBucket(*input.Bucket); err != nil {
		return nil, err
	}

	return &s3.HeadBucketOutput{}, nil
}

func (b *InMemoryBackend) ListBuckets(
	_ context.Context,
	_ *s3.ListBucketsInput,
) (*s3.ListBucketsOutput, error) {
	// Snapshot bucket data under lock across all regions, release immediately
	b.mu.RLock("ListBuckets")
	buckets := make([]types.Bucket, 0, len(b.buckets))
	for _, regionBuckets := range b.buckets {
		for _, bucket := range regionBuckets {
			if bucket.DeletePending {
				continue
			}
			buckets = append(buckets, types.Bucket{
				Name:         aws.String(bucket.Name),
				CreationDate: aws.Time(bucket.CreationDate),
			})
		}
	}
	b.mu.RUnlock()

	// Sort outside the lock
	sort.Slice(buckets, func(i, j int) bool {
		return *buckets[i].Name < *buckets[j].Name
	})

	return &s3.ListBucketsOutput{
		Buckets: buckets,
		Owner: &types.Owner{
			DisplayName: aws.String("Gopherstack"),
			ID:          aws.String("placeholder-id"),
		},
	}, nil
}

// Regions returns all distinct regions that contain at least one active bucket.
func (b *InMemoryBackend) Regions() []string {
	b.mu.RLock("Regions")
	defer b.mu.RUnlock()

	var regions []string

	for region, regionBuckets := range b.buckets {
		for _, bucket := range regionBuckets {
			if !bucket.DeletePending {
				regions = append(regions, region)

				break
			}
		}
	}

	sort.Strings(regions)

	return regions
}

// BucketsByRegion returns a snapshot of all Bucket SDK objects in the given region.
// Returns all buckets (cross-region) if region is empty.
func (b *InMemoryBackend) BucketsByRegion(region string) []types.Bucket {
	b.mu.RLock("BucketsByRegion")
	defer b.mu.RUnlock()

	var buckets []types.Bucket

	for r, regionBuckets := range b.buckets {
		if region != "" && r != region {
			continue
		}

		for _, bucket := range regionBuckets {
			if bucket.DeletePending {
				continue
			}

			buckets = append(buckets, types.Bucket{
				Name:         aws.String(bucket.Name),
				CreationDate: aws.Time(bucket.CreationDate),
			})
		}
	}

	sort.Slice(buckets, func(i, j int) bool {
		return *buckets[i].Name < *buckets[j].Name
	})

	return buckets
}

func (b *InMemoryBackend) PutObject(
	ctx context.Context,
	input *s3.PutObjectInput,
) (*s3.PutObjectOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key

	bucket, err := b.checkPutObjectAuthAndLock(bucketName, key)
	if err != nil {
		return nil, err
	}

	// 2. Prepare data and metadata outside the lock.
	originalSize, storedData, isCompressed, etag, computedChecksumB64, err := b.prepareObjectData(
		ctx,
		input,
	)
	if err != nil {
		return nil, err
	}

	checksums := objectChecksums{
		crc32:     input.ChecksumCRC32,
		crc32c:    input.ChecksumCRC32C,
		sha1:      input.ChecksumSHA1,
		sha256:    input.ChecksumSHA256,
		crc64nvme: input.ChecksumCRC64NVME,
	}

	// When the client specifies a checksum algorithm but omits the checksum value
	// (requesting server-side computation), populate the computed value.
	checksums.populateComputed(
		computedChecksumB64,
		strings.ToUpper(string(input.ChecksumAlgorithm)),
	)

	// Extract SSE info from context (set by putObject handler).
	sseFromCtx, _ := ctx.Value(sseKey).(sseInfo)

	// Real envelope encryption: when SSE is configured, encrypt the stored
	// (post-compression) bytes with AES-256-GCM and stash the DEK + nonce on
	// the version so GET can decrypt. ETag stays as MD5(plaintext) so
	// existing checksum-based tests + SDK clients keep matching.
	encryptedData, dek, nonce, encErr := encryptWithSSE(
		storedData,
		sseFromCtx,
		sseFromCtx.SSECKeyB64,
	)
	if encErr != nil {
		return nil, encErr
	}

	finalQuotedETag := "\"" + etag + "\""
	newVersion := &StoredObjectVersion{
		VersionID:          NullVersion, // default, saveObjectVersion will assign if enabled
		Key:                key,
		Data:               encryptedData,
		IsCompressed:       isCompressed,
		Size:               originalSize,
		ETag:               finalQuotedETag,
		LastModified:       time.Now().UTC(),
		ContentType:        aws.ToString(input.ContentType),
		ContentEncoding:    aws.ToString(input.ContentEncoding),
		ContentDisposition: aws.ToString(input.ContentDisposition),
		Metadata:           maps.Clone(input.Metadata),
		ChecksumCRC32:      checksums.crc32,
		ChecksumCRC32C:     checksums.crc32c,
		ChecksumSHA1:       checksums.sha1,
		ChecksumSHA256:     checksums.sha256,
		ChecksumCRC64NVME:  checksums.crc64nvme,
		ChecksumAlgorithm:  input.ChecksumAlgorithm,
		SSEAlgorithm:       sseFromCtx.Algorithm,
		SSEKMSKeyID:        sseFromCtx.KMSKeyID,
		SSECAlgorithm:      sseFromCtx.SSECAlgorithm,
		SSECKeyMD5:         sseFromCtx.SSECKeyMD5,
		EncryptionDEK:      dek,
		EncryptionNonce:    nonce,
		IsLatest:           true,
	}

	newVersionID := b.saveObjectVersion(bucket, key, newVersion)

	// Store tags outside bucket.mu to respect the lock ordering
	// (b.mu must not be acquired while bucket.mu is held).
	b.storeObjectTags(input.Tagging, bucketName, key, newVersionID)

	logger.Load(ctx).DebugContext(ctx, "S3 Backend PutObject",
		"bucket", bucketName, "key", key,
		"contentType", aws.ToString(input.ContentType),
		"versionId", newVersionID)

	// Async replication to configured destination buckets.
	go b.triggerReplication(ctx, bucketName, key, finalQuotedETag)

	return &s3.PutObjectOutput{
		ETag:              aws.String(finalQuotedETag),
		VersionId:         aws.String(newVersionID),
		ChecksumCRC32:     checksums.crc32,
		ChecksumCRC32C:    checksums.crc32c,
		ChecksumSHA1:      checksums.sha1,
		ChecksumSHA256:    checksums.sha256,
		ChecksumCRC64NVME: checksums.crc64nvme,
	}, nil
}

func (b *InMemoryBackend) prepareObjectData(
	ctx context.Context,
	input *s3.PutObjectInput,
) (int64, []byte, bool, string, string, error) {
	n, data, etag, s3Hasher, err := b.computeObjectHashes(ctx, input.Body, input.ChecksumAlgorithm)
	if err != nil {
		return 0, nil, false, "", "", err
	}

	logger.Load(ctx).DebugContext(ctx, "prepareObjectData trace",
		"n", n, "dataLen", len(data), "etag", etag)

	// 2. Validate Content-MD5 from context if present.
	if vErr := b.validateContentMD5(ctx, data, etag); vErr != nil {
		return 0, nil, false, "", "", vErr
	}

	// 3. Validate S3 checksum if provided; compute it if the algorithm is set.
	computedChecksumB64, fErr := b.finalizeChecksum(s3Hasher, input)
	if fErr != nil {
		return 0, nil, false, "", "", fErr
	}

	// 4. Decide whether to compress based on size.
	if b.compressor != nil && (b.compressionMinBytes == 0 || n >= int64(b.compressionMinBytes)) {
		cData, cErr := b.compressor.Compress(data)
		if cErr == nil {
			return n, cData, true, etag, computedChecksumB64, nil
		}

		return 0, nil, false, "", "", cErr
	}

	return n, data, false, etag, computedChecksumB64, nil
}

func (b *InMemoryBackend) finalizeChecksum(
	s3Hasher hash.Hash,
	input *s3.PutObjectInput,
) (string, error) {
	if s3Hasher == nil {
		return "", nil
	}

	computedChecksumB64 := checksumBytesToB64(s3Hasher)
	algo := strings.ToUpper(string(input.ChecksumAlgorithm))

	var supplied *string

	switch algo {
	case ChecksumCRC32:
		supplied = input.ChecksumCRC32
	case ChecksumCRC32C:
		supplied = input.ChecksumCRC32C
	case ChecksumSHA1:
		supplied = input.ChecksumSHA1
	case ChecksumSHA256:
		supplied = input.ChecksumSHA256
	case ChecksumCRC64NVME:
		supplied = input.ChecksumCRC64NVME
	}

	if supplied != nil && *supplied != "" && computedChecksumB64 != *supplied {
		return "", ErrBadChecksum
	}

	return computedChecksumB64, nil
}

func (b *InMemoryBackend) storeObjectTags(tagging *string, bucket, key, versionID string) {
	if tagging == nil {
		return
	}

	pTags, pErr := url.ParseQuery(*tagging)
	if pErr != nil {
		return
	}

	var tagList []types.Tag
	for k, v := range pTags {
		tagList = append(tagList, types.Tag{
			Key:   aws.String(k),
			Value: aws.String(v[0]),
		})
	}

	if len(tagList) == 0 {
		return
	}

	b.mu.Lock("PutObject.tags")
	defer b.mu.Unlock()

	if b.tags == nil {
		b.tags = make(map[string][]types.Tag)
	}

	tagKey := fmt.Sprintf("%s/%s/%s", bucket, key, versionID)
	b.tags[tagKey] = tagList
}

func (b *InMemoryBackend) GetObject(
	ctx context.Context,
	input *s3.GetObjectInput,
) (*s3.GetObjectOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key
	versionID := input.VersionId

	b.mu.RLock("GetObject")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("GetObject")
	obj, exists := bucket.Objects[key]
	if !exists {
		bucket.mu.RUnlock()

		return nil, ErrNoSuchKey
	}
	bucket.mu.RUnlock()

	// Use per-object lock for version operations instead of holding bucket lock
	obj.mu.RLock("GetObject")
	defer obj.mu.RUnlock()

	var ver *StoredObjectVersion
	if versionID != nil && *versionID != "" {
		v, ok := obj.Versions[*versionID]
		if !ok {
			return nil, ErrNoSuchKey
		}
		ver = v
	} else {
		ver = findLatestVersion(obj.Versions)
	}

	if ver == nil || ver.Deleted {
		return nil, ErrNoSuchKey
	}

	// Copy data + metadata under the lock; decryption + decompression
	// happen outside.
	dataToDecompress := ver.Data
	isCompressed := ver.IsCompressed
	size := ver.Size
	metadata := maps.Clone(ver.Metadata)
	versionIDStr := ver.VersionID
	sseAlg := ver.SSEAlgorithm
	sseCAlg := ver.SSECAlgorithm
	dek := ver.EncryptionDEK
	nonce := ver.EncryptionNonce

	// Reverse envelope encryption when the version was stored under SSE. For
	// SSE-C the customer must re-supply the key on GET via the request — read
	// it from context (set by getObject handler) before decrypting. If no key
	// is supplied for an SSE-C version, skip decrypt and let the handler's
	// validateSSECOnRead surface the proper 400 ErrSSECRequired.
	if sseAlg != "" || sseCAlg != "" {
		sseFromCtx, _ := ctx.Value(sseKey).(sseInfo)
		if sseCAlg != "" && sseFromCtx.SSECKeyB64 == "" {
			// Fall through with the (still-encrypted) blob; the handler will
			// reject the request before reading the body.
			return buildGetObjectOutput(dataToDecompress, size, ver, metadata, versionIDStr), nil
		}
		decrypted, decErr := decryptWithSSE(
			dataToDecompress,
			sseAlg,
			sseCAlg,
			dek,
			nonce,
			sseFromCtx.SSECKeyB64,
		)
		if decErr != nil {
			return nil, decErr
		}
		dataToDecompress = decrypted
	}

	data, err := b.decompressObjectData(dataToDecompress, isCompressed)
	if err != nil {
		return nil, err
	}

	return buildGetObjectOutput(data, size, ver, metadata, versionIDStr), nil
}

// decompressObjectData decompresses storedData when isCompressed is true.
func (b *InMemoryBackend) decompressObjectData(
	storedData []byte,
	isCompressed bool,
) ([]byte, error) {
	if !isCompressed {
		return storedData, nil
	}

	if b.compressor == nil {
		return nil, ErrNoCompressor
	}

	data, err := b.compressor.Decompress(storedData)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// buildGetObjectOutput assembles a GetObjectOutput from decompressed data and version fields.
func buildGetObjectOutput(
	data []byte,
	size int64,
	ver *StoredObjectVersion,
	metadata map[string]string,
	versionIDStr string,
) *s3.GetObjectOutput {
	return &s3.GetObjectOutput{
		Body:                 io.NopCloser(bytes.NewReader(data)),
		ContentLength:        aws.Int64(size),
		ContentType:          aws.String(ver.ContentType),
		ContentEncoding:      nilStringIfEmpty(ver.ContentEncoding),
		ContentDisposition:   nilStringIfEmpty(ver.ContentDisposition),
		ETag:                 aws.String(ver.ETag),
		LastModified:         aws.Time(ver.LastModified),
		Metadata:             metadata,
		VersionId:            aws.String(versionIDStr),
		ChecksumCRC32:        ver.ChecksumCRC32,
		ChecksumCRC32C:       ver.ChecksumCRC32C,
		ChecksumSHA1:         ver.ChecksumSHA1,
		ChecksumSHA256:       ver.ChecksumSHA256,
		ChecksumCRC64NVME:    ver.ChecksumCRC64NVME,
		ServerSideEncryption: types.ServerSideEncryption(ver.SSEAlgorithm),
		SSEKMSKeyId:          nilStringIfEmpty(ver.SSEKMSKeyID),
		SSECustomerAlgorithm: nilStringIfEmpty(ver.SSECAlgorithm),
		SSECustomerKeyMD5:    nilStringIfEmpty(ver.SSECKeyMD5),
	}
}

func (b *InMemoryBackend) HeadObject(
	ctx context.Context,
	input *s3.HeadObjectInput,
) (*s3.HeadObjectOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key
	versionID := input.VersionId

	b.mu.RLock("HeadObject")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("HeadObject")
	obj, exists := bucket.Objects[key]
	bucket.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchKey
	}

	obj.mu.RLock("HeadObject")
	defer obj.mu.RUnlock()

	var ver *StoredObjectVersion
	if versionID != nil && *versionID != "" {
		// Use provided version ID
		v, ok := obj.Versions[*versionID]
		if !ok {
			return nil, ErrNoSuchKey
		}
		ver = v
	} else if latestID := obj.LatestVersionID; latestID != "" {
		// Use cached latest version ID to avoid scanning all versions
		ver = obj.Versions[latestID]
	} else {
		// Fallback: scan for latest (shouldn't happen in normal operation)
		ver = findLatestVersion(obj.Versions)
	}

	if ver == nil {
		return nil, ErrNoSuchKey
	}

	// If a specific version was requested and it's a delete marker, return 405 (MethodNotAllowed).
	if versionID != nil && *versionID != "" && ver.Deleted {
		return nil, ErrDeleteMarker
	}

	// If no version specified and latest is a delete marker, return 404.
	if ver.Deleted {
		return nil, ErrNoSuchKey
	}

	logger.Load(ctx).DebugContext(ctx, "S3 Backend HeadObject",
		"bucket", bucketName, "key", key,
		"versionId", aws.ToString(versionID),
		"foundContentType", ver.ContentType)

	sc := ver.StorageClass
	if sc == "" {
		sc = storageStandard
	}

	return &s3.HeadObjectOutput{
		ContentLength:        aws.Int64(ver.Size),
		ContentType:          aws.String(ver.ContentType),
		ContentEncoding:      nilStringIfEmpty(ver.ContentEncoding),
		ContentDisposition:   nilStringIfEmpty(ver.ContentDisposition),
		ETag:                 aws.String(ver.ETag),
		LastModified:         aws.Time(ver.LastModified),
		Metadata:             maps.Clone(ver.Metadata),
		VersionId:            aws.String(ver.VersionID),
		ChecksumCRC32:        ver.ChecksumCRC32,
		ChecksumCRC32C:       ver.ChecksumCRC32C,
		ChecksumSHA1:         ver.ChecksumSHA1,
		ChecksumSHA256:       ver.ChecksumSHA256,
		ChecksumCRC64NVME:    ver.ChecksumCRC64NVME,
		StorageClass:         types.StorageClass(sc),
		ServerSideEncryption: types.ServerSideEncryption(ver.SSEAlgorithm),
		SSEKMSKeyId:          nilStringIfEmpty(ver.SSEKMSKeyID),
		SSECustomerAlgorithm: nilStringIfEmpty(ver.SSECAlgorithm),
		SSECustomerKeyMD5:    nilStringIfEmpty(ver.SSECKeyMD5),
	}, nil
}

func (b *InMemoryBackend) DeleteObject(
	ctx context.Context,
	input *s3.DeleteObjectInput,
) (*s3.DeleteObjectOutput, error) {
	bucketName := *input.Bucket

	b.mu.RLock("DeleteObject")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.Lock("DeleteObject")
	out, err := b.deleteObjectLocked(bucket, *input.Key, input.VersionId)
	bucket.mu.Unlock()

	if err != nil {
		return nil, err
	}

	// Clean up tags for the deleted version (not when a delete marker is added).
	if out.DeleteMarker == nil || !aws.ToBool(out.DeleteMarker) {
		vid := NullVersion
		if input.VersionId != nil && *input.VersionId != "" {
			vid = *input.VersionId
		}

		b.mu.Lock("DeleteObject.tags")
		if b.tags != nil {
			delete(b.tags, fmt.Sprintf("%s/%s/%s", bucketName, *input.Key, vid))
		}
		b.mu.Unlock()
	}

	// Async delete-marker replication when versioning created a delete marker.
	if out.DeleteMarker != nil && aws.ToBool(out.DeleteMarker) {
		go b.triggerDeleteMarkerReplication(ctx, bucketName, *input.Key)
	}

	return out, nil
}

// deleteObjectLocked performs a single-object deletion assuming bucket.mu is
// already held by the caller. It is used by both DeleteObject and DeleteObjects
// (which holds the lock for the entire batch to avoid per-object lock churn).
func (b *InMemoryBackend) deleteObjectLocked(
	bucket *StoredBucket,
	key string,
	versionID *string,
) (*s3.DeleteObjectOutput, error) {
	obj, exists := bucket.Objects[key]
	if !exists {
		// S3 spec: Delete on non-existent object is 204
		return &s3.DeleteObjectOutput{}, nil
	}

	if err := checkObjectLockForDelete(obj, versionID); err != nil {
		return nil, err
	}

	if versionID != nil && *versionID != "" {
		return deleteSpecificVersion(bucket, obj, key, versionID), nil
	}

	return deleteLatestVersion(bucket, obj, key), nil
}

// findLatestVersion returns the version with IsLatest set, or nil if none exists.
func findLatestVersion(versions map[string]*StoredObjectVersion) *StoredObjectVersion {
	for _, v := range versions {
		if v.IsLatest {
			return v
		}
	}

	return nil
}

// checkObjectLockForDelete returns ErrObjectLocked if the target version is under
// a legal hold or an active retention policy. Must be called with bucket.mu held.
// obj.mu is acquired internally to guard against concurrent PutObject calls that
// update LatestVersionID / Versions under obj.mu after releasing bucket.mu.
func checkObjectLockForDelete(obj *StoredObject, versionID *string) error {
	obj.mu.RLock("checkObjectLockForDelete")
	defer obj.mu.RUnlock()

	var ver *StoredObjectVersion

	switch {
	case versionID != nil && *versionID != "":
		ver = obj.Versions[*versionID]
	case obj.LatestVersionID != "":
		ver = obj.Versions[obj.LatestVersionID]
	default:
		ver = findLatestVersion(obj.Versions)
	}

	if ver == nil || ver.Deleted {
		return nil
	}

	if ver.LegalHold {
		return ErrInvalidObjectState
	}

	if ver.RetentionMode != "" && !ver.RetainUntil.IsZero() && time.Now().Before(ver.RetainUntil) {
		return ErrInvalidObjectState
	}

	return nil
}

// checkObjectLockForOverwrite returns ErrObjectLocked if the null version (current
// when versioning is not enabled) is under a legal hold or an active retention
// policy. This prevents PutObject from silently overwriting a protected object.
// Must be called with bucket.mu held.
// obj.mu is acquired internally to guard against concurrent PutObject calls.
func checkObjectLockForOverwrite(obj *StoredObject) error {
	obj.mu.RLock("checkObjectLockForOverwrite")
	defer obj.mu.RUnlock()

	var ver *StoredObjectVersion

	if obj.LatestVersionID != "" {
		ver = obj.Versions[obj.LatestVersionID]
	} else {
		ver = findLatestVersion(obj.Versions)
	}

	if ver == nil || ver.Deleted {
		return nil
	}

	if ver.LegalHold {
		return ErrObjectLocked
	}

	if ver.RetentionMode != "" && !ver.RetainUntil.IsZero() && time.Now().Before(ver.RetainUntil) {
		return ErrObjectLocked
	}

	return nil
}

// deleteSpecificVersion removes the specified version from the object.
// Must be called with bucket.mu held by the caller.
func deleteSpecificVersion(
	bucket *StoredBucket,
	obj *StoredObject,
	key string,
	versionID *string,
) *s3.DeleteObjectOutput {
	if _, ok := obj.Versions[*versionID]; !ok {
		return &s3.DeleteObjectOutput{}
	}

	// Acquire obj.mu to serialize the map deletion with concurrent readers
	// (GetObject/HeadObject use obj.mu.RLock after releasing bucket.mu).
	obj.mu.Lock("deleteSpecificVersion")
	delete(obj.Versions, *versionID)
	empty := len(obj.Versions) == 0
	obj.mu.Unlock()

	if empty {
		// Remove the now-empty object from the bucket map (still under bucket.mu).
		delete(bucket.Objects, key)
		obj.mu.Close()
	}

	return &s3.DeleteObjectOutput{VersionId: versionID}
}

// deleteLatestVersion deletes the latest version of an object (or marks it deleted if versioning is enabled).
// Must be called with bucket.mu held by the caller.
func deleteLatestVersion(
	bucket *StoredBucket,
	obj *StoredObject,
	key string,
) *s3.DeleteObjectOutput {
	// Delete latest (Versioning enabled -> add delete marker, Suspended -> delete null version)
	if bucket.Versioning == types.BucketVersioningStatusEnabled {
		newVersionID := newObjectVersionID()

		// Acquire obj.mu to serialize the version-map mutation with concurrent
		// readers that use obj.mu.RLock after releasing bucket.mu.
		obj.mu.Lock("deleteLatestVersion")

		for _, v := range obj.Versions {
			v.IsLatest = false
		}
		deleteMarker := &StoredObjectVersion{
			VersionID:    newVersionID,
			Key:          key,
			Deleted:      true,
			IsLatest:     true,
			LastModified: time.Now().UTC(),
		}
		obj.Versions[newVersionID] = deleteMarker
		obj.LatestVersionID = newVersionID // Update cache

		obj.mu.Unlock()

		return &s3.DeleteObjectOutput{
			DeleteMarker: aws.Bool(true),
			VersionId:    aws.String(newVersionID),
		}
	}

	// Suspended versioning: an unversioned DELETE removes only the existing
	// "null" version and inserts a "null" delete marker. Any non-null versions
	// created while versioning was enabled must remain retrievable by versionId.
	obj.mu.Lock("deleteLatestVersion")

	delete(obj.Versions, NullVersion)

	// If no prior (non-null) versions remain, the object disappears entirely —
	// matching the behaviour of a bucket that was never versioned.
	if len(obj.Versions) == 0 {
		obj.mu.Unlock()
		delete(bucket.Objects, key)
		obj.mu.Close()

		return &s3.DeleteObjectOutput{}
	}

	for _, v := range obj.Versions {
		v.IsLatest = false
	}
	deleteMarker := &StoredObjectVersion{
		VersionID:    NullVersion,
		Key:          key,
		Deleted:      true,
		IsLatest:     true,
		LastModified: time.Now().UTC(),
	}
	obj.Versions[NullVersion] = deleteMarker
	obj.LatestVersionID = NullVersion

	obj.mu.Unlock()

	return &s3.DeleteObjectOutput{
		DeleteMarker: aws.Bool(true),
		VersionId:    aws.String(NullVersion),
	}
}

func (b *InMemoryBackend) DeleteObjects(
	_ context.Context,
	input *s3.DeleteObjectsInput,
) (*s3.DeleteObjectsOutput, error) {
	out := &s3.DeleteObjectsOutput{
		Deleted: make([]types.DeletedObject, 0, len(input.Delete.Objects)),
		Errors:  make([]types.Error, 0, len(input.Delete.Objects)),
	}

	bucketName := *input.Bucket

	b.mu.RLock("DeleteObjects")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		for _, obj := range input.Delete.Objects {
			out.Errors = append(out.Errors, types.Error{
				Key:     obj.Key,
				Code:    aws.String("NoSuchBucket"),
				Message: aws.String(ErrNoSuchBucket.Error()),
			})
		}

		return out, nil
	}

	// Hold the bucket lock for the entire batch to avoid per-object lock churn
	// when deleting thousands of objects.
	var tagKeysToDelete []string

	bucket.mu.Lock("DeleteObjects")
	for _, obj := range input.Delete.Objects {
		deleted, tagKey, delErr := b.deleteSingleObject(bucket, bucketName, obj)
		if delErr != nil {
			out.Errors = append(out.Errors, types.Error{
				Key:     obj.Key,
				Code:    aws.String("AccessDenied"),
				Message: aws.String(delErr.Error()),
			})

			continue
		}

		if tagKey != "" {
			tagKeysToDelete = append(tagKeysToDelete, tagKey)
		}

		out.Deleted = append(out.Deleted, deleted)
	}
	bucket.mu.Unlock()

	// Clean up tags after the bucket lock is released.
	if len(tagKeysToDelete) > 0 {
		b.mu.Lock("DeleteObjects.tags")
		if b.tags != nil {
			for _, k := range tagKeysToDelete {
				delete(b.tags, k)
			}
		}
		b.mu.Unlock()
	}

	return out, nil
}

// deleteSingleObject deletes one object from the bucket and returns the deleted record,
// the tag key to clean up (if any), and any error. Must be called with bucket.mu held.
func (b *InMemoryBackend) deleteSingleObject(
	bucket *StoredBucket,
	bucketName string,
	obj types.ObjectIdentifier,
) (types.DeletedObject, string, error) {
	delOut, delErr := b.deleteObjectLocked(bucket, aws.ToString(obj.Key), obj.VersionId)
	if delErr != nil {
		return types.DeletedObject{}, "", delErr
	}

	tagKey := ""
	if delOut.DeleteMarker == nil || !aws.ToBool(delOut.DeleteMarker) {
		vid := NullVersion
		if obj.VersionId != nil && *obj.VersionId != "" {
			vid = *obj.VersionId
		}
		tagKey = fmt.Sprintf("%s/%s/%s", bucketName, aws.ToString(obj.Key), vid)
	}

	deleted := types.DeletedObject{
		Key:       obj.Key,
		VersionId: obj.VersionId,
	}
	if delOut.DeleteMarker != nil {
		deleted.DeleteMarker = delOut.DeleteMarker
	}
	if delOut.VersionId != nil {
		deleted.DeleteMarkerVersionId = delOut.VersionId
	}

	return deleted, tagKey, nil
}

func applyDelimiter(
	prefix, delimiter string,
	contents []types.Object,
) ([]types.Object, []types.CommonPrefix) {
	var filtered []types.Object
	var cpList []types.CommonPrefix
	seenPrefixes := make(map[string]struct{})

	for _, obj := range contents {
		key := aws.ToString(obj.Key)
		rest := key[len(prefix):]
		idx := strings.Index(rest, delimiter)

		if idx != -1 {
			cp := prefix + rest[:idx+len(delimiter)]
			if _, seen := seenPrefixes[cp]; !seen {
				seenPrefixes[cp] = struct{}{}
				cpList = append(cpList, types.CommonPrefix{Prefix: aws.String(cp)})
			}
		} else {
			filtered = append(filtered, obj)
		}
	}

	sort.Slice(cpList, func(i, j int) bool {
		return aws.ToString(cpList[i].Prefix) < aws.ToString(cpList[j].Prefix)
	})

	return filtered, cpList
}

func (b *InMemoryBackend) processListObjects(
	bucket *StoredBucket,
	input *s3.ListObjectsInput,
) ([]types.Object, []types.CommonPrefix, bool, string, int32) {
	// Snapshot object pointers under lock
	bucket.mu.RLock("ListObjects")
	prefix := aws.ToString(input.Prefix)
	objectSnapshots := make([]*StoredObject, 0, len(bucket.Objects))
	for _, obj := range bucket.Objects {
		if strings.HasPrefix(obj.Key, prefix) {
			objectSnapshots = append(objectSnapshots, obj)
		}
	}
	bucket.mu.RUnlock()

	// Process objects outside the bucket lock
	contents := b.processObjectSnapshots(objectSnapshots)

	sort.Slice(contents, func(i, j int) bool {
		return *contents[i].Key < *contents[j].Key
	})

	// Apply Marker using binary search for O(log n) seek instead of O(n) linear scan.
	marker := aws.ToString(input.Marker)
	if marker != "" {
		startIndex := sort.Search(len(contents), func(i int) bool {
			return *contents[i].Key > marker
		})
		if startIndex >= len(contents) {
			contents = nil
		} else {
			contents = contents[startIndex:]
		}
	}

	delimiter := aws.ToString(input.Delimiter)
	var cpList []types.CommonPrefix

	if delimiter != "" {
		contents, cpList = applyDelimiter(prefix, delimiter, contents)
	}

	maxKeys := int32(defaultMaxKeys)
	if input.MaxKeys != nil {
		maxKeys = *input.MaxKeys
	}

	contents, cpList, isTruncated, nextMarker := b.truncateListResults(contents, cpList, maxKeys)

	return contents, cpList, isTruncated, nextMarker, maxKeys
}

func (b *InMemoryBackend) processObjectSnapshots(objectSnapshots []*StoredObject) []types.Object {
	contents := make([]types.Object, 0, len(objectSnapshots)) // #61: capacity hint
	for _, obj := range objectSnapshots {
		obj.mu.RLock("ListObjects")
		latestID := obj.LatestVersionID
		var latest *StoredObjectVersion
		if latestID != "" {
			latest = obj.Versions[latestID]
		} else {
			// Fallback: scan for latest if not cached
			latest = findLatestVersion(obj.Versions)
		}
		obj.mu.RUnlock()

		if latest == nil || latest.Deleted {
			continue
		}

		var checksumAlgos []types.ChecksumAlgorithm
		if latest.ChecksumAlgorithm != "" {
			checksumAlgos = []types.ChecksumAlgorithm{latest.ChecksumAlgorithm}
		}

		contents = append(contents, types.Object{
			Key:               aws.String(latest.Key),
			LastModified:      aws.Time(latest.LastModified),
			ETag:              aws.String(latest.ETag),
			Size:              aws.Int64(latest.Size),
			StorageClass:      types.ObjectStorageClassStandard,
			ChecksumAlgorithm: checksumAlgos,
			Owner: &types.Owner{
				ID:          aws.String(gopherstackName),
				DisplayName: aws.String(gopherstackName),
			},
		})
	}

	return contents
}

func (b *InMemoryBackend) ListObjects(
	_ context.Context,
	input *s3.ListObjectsInput,
) (*s3.ListObjectsOutput, error) {
	bucketName := *input.Bucket

	b.mu.RLock("ListObjects")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	contents, cpList, isTruncated, nextMarker, maxKeys := b.processListObjects(bucket, input)

	return &s3.ListObjectsOutput{
		Name:           input.Bucket,
		Prefix:         input.Prefix,
		Delimiter:      input.Delimiter,
		MaxKeys:        aws.Int32(maxKeys),
		Marker:         input.Marker,
		Contents:       contents,
		CommonPrefixes: cpList,
		IsTruncated:    aws.Bool(isTruncated),
		NextMarker:     aws.String(nextMarker),
	}, nil
}

func (b *InMemoryBackend) ListObjectsV2(
	ctx context.Context,
	input *s3.ListObjectsV2Input,
) (*s3.ListObjectsV2Output, error) {
	// Re-use ListObjects logic but handle V2 specific params
	marker := ""
	if input.ContinuationToken != nil && *input.ContinuationToken != "" {
		marker = *input.ContinuationToken
	} else if input.StartAfter != nil && *input.StartAfter != "" {
		marker = *input.StartAfter
	}

	listOut, err := b.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket:    input.Bucket,
		Prefix:    input.Prefix,
		MaxKeys:   input.MaxKeys,
		Delimiter: input.Delimiter,
		Marker:    aws.String(marker),
	})
	if err != nil {
		return nil, err
	}

	count64 := int64(len(listOut.Contents)) + int64(len(listOut.CommonPrefixes))
	count := int32(uint32(count64)) //nolint:gosec // intentional conversion for key count

	nextCont := ""
	if aws.ToBool(listOut.IsTruncated) {
		nextCont = aws.ToString(listOut.NextMarker)
	}

	return &s3.ListObjectsV2Output{
		Name:                  input.Bucket,
		Prefix:                input.Prefix,
		MaxKeys:               input.MaxKeys,
		Contents:              listOut.Contents,
		CommonPrefixes:        listOut.CommonPrefixes,
		KeyCount:              aws.Int32(count),
		IsTruncated:           listOut.IsTruncated,
		NextContinuationToken: aws.String(nextCont),
		ContinuationToken:     input.ContinuationToken,
		StartAfter:            input.StartAfter,
		Delimiter:             input.Delimiter,
	}, nil
}

// versionSnapshot holds the subset of StoredObjectVersion fields needed for
// listing. It is captured under the bucket lock and processed outside it.
type versionSnapshot struct {
	lastModified time.Time
	key          string
	versionID    string
	etag         string
	storageClass string
	size         int64
	isLatest     bool
	deleted      bool
}

func (b *InMemoryBackend) ListObjectVersions(
	_ context.Context,
	input *s3.ListObjectVersionsInput,
) (*s3.ListObjectVersionsOutput, error) {
	bucketName := *input.Bucket

	b.mu.RLock("ListObjectVersions")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	prefix := aws.ToString(input.Prefix)
	keyMarker := aws.ToString(input.KeyMarker)
	versionIDMarker := aws.ToString(input.VersionIdMarker)
	delimiter := aws.ToString(input.Delimiter)

	maxKeys := int32(defaultMaxKeys)
	if input.MaxKeys != nil && *input.MaxKeys > 0 {
		maxKeys = *input.MaxKeys
	}

	snapshots := b.snapshotVersions(bucket, prefix)

	// Sort: primary by key (ascending), secondary by LastModified (newest first).
	sort.Slice(snapshots, func(i, j int) bool {
		if snapshots[i].key != snapshots[j].key {
			return snapshots[i].key < snapshots[j].key
		}

		return snapshots[i].lastModified.After(snapshots[j].lastModified)
	})

	snapshots = seekVersionMarker(snapshots, keyMarker, versionIDMarker)
	filteredSnapshots, commonPrefixes := applyVersionDelimiter(snapshots, prefix, delimiter)

	versions, deleteMarkers, isTruncated, nextKeyMarker, nextVersionIDMarker := buildVersionPage(
		filteredSnapshots,
		maxKeys,
	)

	var cpList []types.CommonPrefix
	for _, cp := range commonPrefixes {
		cpList = append(cpList, types.CommonPrefix{Prefix: aws.String(cp)})
	}

	return &s3.ListObjectVersionsOutput{
		Name:                aws.String(bucketName),
		Prefix:              input.Prefix,
		KeyMarker:           input.KeyMarker,
		VersionIdMarker:     input.VersionIdMarker,
		MaxKeys:             aws.Int32(maxKeys),
		Delimiter:           input.Delimiter,
		IsTruncated:         aws.Bool(isTruncated),
		NextKeyMarker:       aws.String(nextKeyMarker),
		NextVersionIdMarker: aws.String(nextVersionIDMarker),
		Versions:            versions,
		DeleteMarkers:       deleteMarkers,
		CommonPrefixes:      cpList,
	}, nil
}

// snapshotVersions captures all versions from bucket.Objects that match prefix,
// under the bucket read lock.
func (b *InMemoryBackend) snapshotVersions(bucket *StoredBucket, prefix string) []versionSnapshot {
	var snapshots []versionSnapshot

	bucket.mu.RLock("ListObjectVersions")
	for _, obj := range bucket.Objects {
		if !strings.HasPrefix(obj.Key, prefix) {
			continue
		}

		for _, v := range obj.Versions {
			sc := v.StorageClass
			if sc == "" {
				sc = "STANDARD"
			}

			snapshots = append(snapshots, versionSnapshot{
				key:          v.Key,
				versionID:    v.VersionID,
				etag:         v.ETag,
				lastModified: v.LastModified,
				size:         v.Size,
				isLatest:     v.IsLatest,
				deleted:      v.Deleted,
				storageClass: sc,
			})
		}
	}
	bucket.mu.RUnlock()

	return snapshots
}

// seekVersionMarker advances the snapshot slice past the (keyMarker, versionIDMarker) cursor.
func seekVersionMarker(
	snapshots []versionSnapshot,
	keyMarker, versionIDMarker string,
) []versionSnapshot {
	if keyMarker == "" {
		return snapshots
	}

	for i, s := range snapshots {
		if s.key > keyMarker {
			return snapshots[i:]
		}

		if s.key == keyMarker && versionIDMarker != "" && s.versionID == versionIDMarker {
			return snapshots[i+1:]
		}

		// Skip all versions of keyMarker when no versionIDMarker specified.
		if s.key == keyMarker && versionIDMarker == "" {
			continue
		}
	}

	return nil
}

// applyVersionDelimiter groups snapshot keys that share a common prefix
// (when delimiter is set) and returns the remaining non-grouped snapshots
// together with the sorted list of discovered common-prefix strings.
func applyVersionDelimiter(
	snapshots []versionSnapshot,
	prefix, delimiter string,
) ([]versionSnapshot, []string) {
	if delimiter == "" {
		return snapshots, nil
	}

	seenCommonPrefixes := make(map[string]struct{})
	var filtered []versionSnapshot
	var commonPrefixes []string

	for _, snap := range snapshots {
		rest := strings.TrimPrefix(snap.key, prefix)
		if idx := strings.Index(rest, delimiter); idx != -1 {
			cp := prefix + rest[:idx+len(delimiter)]
			if _, seen := seenCommonPrefixes[cp]; !seen {
				seenCommonPrefixes[cp] = struct{}{}
				commonPrefixes = append(commonPrefixes, cp)
			}

			continue
		}

		filtered = append(filtered, snap)
	}

	return filtered, commonPrefixes
}

// buildVersionPage builds the Versions and DeleteMarkers page from snapshots,
// enforcing maxKeys. It returns the pagination flags for the next request.
func buildVersionPage(snapshots []versionSnapshot, maxKeys int32) (
	[]types.ObjectVersion,
	[]types.DeleteMarkerEntry,
	bool,
	string,
	string,
) {
	var versions []types.ObjectVersion
	var deleteMarkers []types.DeleteMarkerEntry
	count := int32(0)
	var lastKey, lastVersionID string

	for _, snap := range snapshots {
		if count >= maxKeys {
			// NextKeyMarker is the last key returned (follow-up uses key-marker=last).
			return versions, deleteMarkers, true, lastKey, lastVersionID
		}

		lastKey = snap.key
		lastVersionID = snap.versionID

		if snap.deleted {
			deleteMarkers = append(deleteMarkers, types.DeleteMarkerEntry{
				Key:          aws.String(snap.key),
				VersionId:    aws.String(snap.versionID),
				IsLatest:     aws.Bool(snap.isLatest),
				LastModified: aws.Time(snap.lastModified),
				Owner: &types.Owner{
					ID:          aws.String(gopherstackName),
					DisplayName: aws.String(gopherstackName),
				},
			})
		} else {
			versions = append(versions, types.ObjectVersion{
				Key:          aws.String(snap.key),
				VersionId:    aws.String(snap.versionID),
				IsLatest:     aws.Bool(snap.isLatest),
				LastModified: aws.Time(snap.lastModified),
				ETag:         aws.String(snap.etag),
				Size:         aws.Int64(snap.size),
				StorageClass: types.ObjectVersionStorageClass(snap.storageClass),
				Owner:        &types.Owner{ID: aws.String(gopherstackName), DisplayName: aws.String(gopherstackName)},
			})
		}

		count++
	}

	return versions, deleteMarkers, false, "", ""
}

func (b *InMemoryBackend) PutBucketVersioning(
	_ context.Context,
	input *s3.PutBucketVersioningInput,
) (*s3.PutBucketVersioningOutput, error) {
	bucketName := *input.Bucket

	b.mu.RLock("PutBucketVersioning")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.Lock("PutBucketVersioning")
	defer bucket.mu.Unlock()

	status := input.VersioningConfiguration.Status
	bucket.Versioning = status

	return &s3.PutBucketVersioningOutput{}, nil
}

func (b *InMemoryBackend) GetBucketVersioning(
	_ context.Context,
	input *s3.GetBucketVersioningInput,
) (*s3.GetBucketVersioningOutput, error) {
	bucketName := *input.Bucket

	b.mu.RLock("GetBucketVersioning")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("GetBucketVersioning")
	defer bucket.mu.RUnlock()

	return &s3.GetBucketVersioningOutput{
		Status: bucket.Versioning,
	}, nil
}

func (b *InMemoryBackend) PutObjectTagging(
	_ context.Context,
	input *s3.PutObjectTaggingInput,
) (*s3.PutObjectTaggingOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key
	versionID := input.VersionId

	b.mu.RLock("PutObjectTagging")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	resolvedVersionID, err := func() (string, error) {
		bucket.mu.Lock("PutObjectTagging")
		defer bucket.mu.Unlock()

		obj, objExists := bucket.Objects[key]
		if !objExists {
			return "", ErrNoSuchKey
		}

		var ver *StoredObjectVersion
		if versionID != nil && *versionID != "" {
			v, ok := obj.Versions[*versionID]
			if !ok {
				return "", ErrNoSuchKey
			}
			ver = v
		} else {
			ver = findLatestVersion(obj.Versions)
		}

		if ver == nil || ver.Deleted {
			return "", ErrNoSuchKey
		}

		return ver.VersionID, nil
	}()
	if err != nil {
		return nil, err
	}

	b.mu.Lock("PutObjectTagging")
	defer b.mu.Unlock()

	tagKey := fmt.Sprintf("%s/%s/%s", bucketName, key, resolvedVersionID)
	if b.tags == nil {
		b.tags = make(map[string][]types.Tag)
	}

	b.tags[tagKey] = slices.Clone(input.Tagging.TagSet)

	return &s3.PutObjectTaggingOutput{}, nil
}

func (b *InMemoryBackend) GetObjectTagging(
	_ context.Context,
	input *s3.GetObjectTaggingInput,
) (*s3.GetObjectTaggingOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key
	versionID := input.VersionId

	b.mu.RLock("GetObjectTagging")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	vid, err := func() (string, error) {
		bucket.mu.RLock("GetObjectTagging")
		defer bucket.mu.RUnlock()

		obj, objExists := bucket.Objects[key]
		if !objExists {
			return "", ErrNoSuchKey
		}

		if versionID != nil && *versionID != "" {
			v, ok := obj.Versions[*versionID]
			if !ok {
				return "", ErrNoSuchKey
			}

			return v.VersionID, nil
		}

		for _, v := range obj.Versions {
			if v.IsLatest {
				return v.VersionID, nil
			}
		}

		return "", ErrNoSuchKey
	}()
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetObjectTagging")
	defer b.mu.RUnlock()

	tagKey := fmt.Sprintf("%s/%s/%s", bucketName, key, vid)
	tags := slices.Clone(b.tags[tagKey])

	return &s3.GetObjectTaggingOutput{
		TagSet: tags,
	}, nil
}

func (b *InMemoryBackend) DeleteObjectTagging(
	_ context.Context,
	input *s3.DeleteObjectTaggingInput,
) (*s3.DeleteObjectTaggingOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key
	versionID := input.VersionId

	b.mu.RLock("DeleteObjectTagging")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	vid, err := func() (string, error) {
		bucket.mu.RLock("DeleteObjectTagging")
		defer bucket.mu.RUnlock()

		obj, objExists := bucket.Objects[key]
		if !objExists {
			return "", nil // S3 Delete is idempotent
		}

		if versionID != nil && *versionID != "" {
			return *versionID, nil
		}

		for _, v := range obj.Versions {
			if v.IsLatest {
				return v.VersionID, nil
			}
		}

		return "", nil
	}()
	if err != nil {
		return nil, err
	}

	if vid == "" {
		return &s3.DeleteObjectTaggingOutput{}, nil
	}

	b.mu.Lock("DeleteObjectTagging")
	defer b.mu.Unlock()

	tagKey := bucketName + "/" + key + "/" + vid
	if b.tags != nil {
		delete(b.tags, tagKey)
	}

	return &s3.DeleteObjectTaggingOutput{}, nil
}

// Multipart

func (b *InMemoryBackend) CreateMultipartUpload(
	ctx context.Context,
	input *s3.CreateMultipartUploadInput,
) (*s3.CreateMultipartUploadOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key

	b.mu.RLock("CreateMultipartUpload")
	_, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	uploadID := newObjectVersionID()
	tagging := aws.ToString(input.Tagging)

	// Capture SSE config so envelope encryption can be applied to the
	// assembled body at Complete time. Prefer the ctx-supplied sseInfo
	// (set by the handler) because it carries the SSE-C raw key bytes that
	// the SDK input struct doesn't expose.
	sse, _ := ctx.Value(sseKey).(sseInfo)

	b.mu.Lock("CreateMultipartUpload")
	if b.uploads == nil {
		b.uploads = make(map[string]map[string]*StoredMultipartUpload)
	}

	if b.uploads[bucketName] == nil {
		b.uploads[bucketName] = make(map[string]*StoredMultipartUpload)
	}

	b.uploads[bucketName][uploadID] = &StoredMultipartUpload{
		UploadID:  uploadID,
		Bucket:    bucketName,
		Key:       key,
		Parts:     make(map[int32]*StoredPart),
		Initiated: time.Now().UTC(),
		Tagging:   tagging,
		SSE:       sse,
		mu:        lockmetrics.New("s3.upload"),
	}
	b.mu.Unlock()

	return &s3.CreateMultipartUploadOutput{
		Bucket:   input.Bucket,
		Key:      input.Key,
		UploadId: aws.String(uploadID),
	}, nil
}

func (b *InMemoryBackend) UploadPart(
	ctx context.Context,
	input *s3.UploadPartInput,
) (*s3.UploadPartOutput, error) {
	uploadID := *input.UploadId
	partNumber := *input.PartNumber
	bucketName := aws.ToString(input.Bucket)

	// 1. Snapshot the body while computing MD5 etag and S3 checksums.
	//nolint:gosec // MD5 required
	md5Hasher := md5.New()
	var buf bytes.Buffer
	writers := []io.Writer{md5Hasher, &buf}

	var s3Hasher hash.Hash
	algo := string(input.ChecksumAlgorithm)
	if algo != "" {
		switch strings.ToUpper(algo) {
		case ChecksumCRC32:
			s3Hasher = crc32.NewIEEE()
		case ChecksumCRC32C:
			s3Hasher = crc32.New(crc32.MakeTable(crc32.Castagnoli))
		case ChecksumSHA1:
			//nolint:gosec // SHA1 supported
			s3Hasher = sha1.New()
		case ChecksumSHA256:
			s3Hasher = sha256.New()
		}
		if s3Hasher != nil {
			writers = append(writers, s3Hasher)
		}
	}

	tr := io.TeeReader(input.Body, io.MultiWriter(writers...))
	originalSize, err := io.Copy(io.Discard, tr)
	if err != nil {
		return nil, err
	}

	storedData := buf.Bytes()
	etag := hex.EncodeToString(md5Hasher.Sum(nil))

	// 2. Validate Content-MD5 from context if present.
	if md5Header, ok := ctx.Value(md5Key).(string); ok && md5Header != "" {
		decoded, dErr := base64.StdEncoding.DecodeString(md5Header)
		if dErr != nil || len(decoded) != md5.Size {
			return nil, ErrBadChecksum
		}

		computed := md5Hasher.Sum(nil)
		if !bytes.Equal(computed, decoded) {
			return nil, ErrBadChecksum
		}
	}

	if vErr := b.verifyChecksum(input, s3Hasher, algo); vErr != nil {
		return nil, vErr
	}

	quotedETag := "\"" + etag + "\""

	// 3. Store the part.
	if sErr := b.storePart(bucketName, uploadID, partNumber, &StoredPart{
		PartNumber: partNumber,
		Data:       storedData,
		ETag:       quotedETag,
		Size:       originalSize,
	}); sErr != nil {
		return nil, sErr
	}

	return &s3.UploadPartOutput{
		ETag:           aws.String(quotedETag),
		ChecksumCRC32:  input.ChecksumCRC32,
		ChecksumCRC32C: input.ChecksumCRC32C,
		ChecksumSHA1:   input.ChecksumSHA1,
		ChecksumSHA256: input.ChecksumSHA256,
	}, nil
}

func (b *InMemoryBackend) CompleteMultipartUpload(
	_ context.Context,
	input *s3.CompleteMultipartUploadInput,
) (*s3.CompleteMultipartUploadOutput, error) {
	uploadID := *input.UploadId
	bucketName := *input.Bucket
	key := *input.Key

	// 1. Read the upload pointer — we need it for assembly but don't consume
	// the upload yet, since assembly may fail (e.g. ErrInvalidPart) and the
	// caller should still be able to retry or abort.
	b.mu.RLock(opCompleteMultipartUpload)
	upload := b.uploads[bucketName][uploadID] // nil map read returns nil safely
	b.mu.RUnlock()

	if upload == nil {
		return nil, ErrNoSuchUpload
	}

	// Snapshot the upload's tagging + SSE before claiming (the upload is
	// removed from the index during claim, so we must capture them first).
	upload.mu.RLock("CompleteMultipartUpload.tagging")
	tagging := upload.Tagging
	sse := upload.SSE
	upload.mu.RUnlock()

	// 2. Assemble and compress data. If this fails, the upload is untouched and
	// can be retried or aborted by the caller.
	assembled, err := b.assembleMultipartData(upload, input)
	if err != nil {
		return nil, err
	}

	// 3. Atomically claim the upload: verify it is still present (wasn't aborted
	// concurrently after step 1), mark it closed, and remove it from the index.
	if claimErr := b.claimMultipartUpload(bucketName, uploadID); claimErr != nil {
		return nil, claimErr
	}

	// 4. Update bucket/object state.
	b.mu.RLock(opCompleteMultipartUpload)
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	versionID, err := b.commitMultipartObject(bucket, bucketName, key, assembled, tagging, sse)
	if err != nil {
		return nil, err
	}

	return &s3.CompleteMultipartUploadOutput{
		Bucket:    input.Bucket,
		Key:       input.Key,
		ETag:      aws.String(assembled.etag),
		VersionId: aws.String(versionID),
	}, nil
}

// claimMultipartUpload atomically marks the upload as closed and removes it from
// b.uploads under b.mu.Lock. Called by CompleteMultipartUpload after successful
// assembly so that a concurrent AbortMultipartUpload cannot also succeed.
func (b *InMemoryBackend) claimMultipartUpload(bucketName, uploadID string) error {
	b.mu.Lock("CompleteMultipartUpload.claim")

	upload := b.uploads[bucketName][uploadID] // nil map read returns nil safely
	if upload == nil {
		b.mu.Unlock()

		return ErrNoSuchUpload
	}

	// Mark closed while holding b.mu to block concurrent UploadPart calls that
	// already hold a pointer to this upload struct.
	upload.mu.Lock("CompleteMultipartUpload.claim")
	upload.closed = true
	upload.mu.Unlock()

	delete(b.uploads[bucketName], uploadID)
	b.mu.Unlock()

	return nil
}

// multipartAssemblyResult holds the results of assembleMultipartData.
type multipartAssemblyResult struct {
	etag           string
	data           []byte
	compressedData []byte
	isCompressed   bool
}

// collectPartsData gathers raw data and part MD5 bytes under upload.mu.RLock.
// Returns the combined buffer and MD5-concatenation used for multipart ETag.
// Must be called without upload.mu held; acquires and releases it internally.
func (b *InMemoryBackend) collectPartsData(
	upload *StoredMultipartUpload,
	parts []types.CompletedPart,
) ([]byte, []byte, error) {
	upload.mu.RLock(opCompleteMultipartUpload)

	// Validate ascending order.
	for i := 1; i < len(parts); i++ {
		if *parts[i].PartNumber <= *parts[i-1].PartNumber {
			upload.mu.RUnlock()

			return nil, nil, ErrInvalidPartOrder
		}
	}

	// Pre-calculate total size.
	totalSize := 0
	for _, part := range parts {
		if sp, ok := upload.Parts[*part.PartNumber]; ok {
			totalSize += len(sp.Data)
		}
	}

	buf := bytes.NewBuffer(make([]byte, 0, totalSize))
	partMD5s := make([]byte, 0, len(parts)*md5.Size)

	for i, part := range parts {
		pNum := *part.PartNumber
		storedPart, ok := upload.Parts[pNum]
		if !ok {
			upload.mu.RUnlock()

			return nil, nil, ErrInvalidPart
		}

		if *part.ETag != storedPart.ETag {
			upload.mu.RUnlock()

			return nil, nil, ErrInvalidPart
		}

		isLastPart := i == len(parts)-1
		if !isLastPart && storedPart.Size < multipartMinPartSize && !b.skipMultipartSizeCheck {
			upload.mu.RUnlock()

			return nil, nil, ErrEntityTooSmall
		}

		buf.Write(storedPart.Data)

		rawETag := strings.Trim(storedPart.ETag, "\"")
		rawBytes, decErr := hex.DecodeString(rawETag)
		if decErr != nil {
			upload.mu.RUnlock()

			return nil, nil, ErrInvalidPart
		}
		partMD5s = append(partMD5s, rawBytes...)
	}
	upload.mu.RUnlock()

	return buf.Bytes(), partMD5s, nil
}

// assembleMultipartData reads all parts under the per-upload read lock, assembles
// the combined payload, compresses it, and returns the assembled result.
//
// The ETag follows the AWS multipart format: MD5 of the concatenated raw MD5
// bytes of each part, formatted as "<hex>-<partCount>".
func (b *InMemoryBackend) assembleMultipartData(
	upload *StoredMultipartUpload,
	input *s3.CompleteMultipartUploadInput,
) (multipartAssemblyResult, error) {
	parts := input.MultipartUpload.Parts

	data, partMD5s, err := b.collectPartsData(upload, parts)
	if err != nil {
		return multipartAssemblyResult{}, err
	}

	var compressedData []byte
	var isCompressed bool

	if b.compressor != nil && (b.compressionMinBytes == 0 || len(data) >= b.compressionMinBytes) {
		var compErr error
		compressedData, compErr = b.compressor.Compress(data)
		if compErr != nil {
			return multipartAssemblyResult{}, compErr
		}
		isCompressed = true
	} else {
		compressedData = data
	}

	// Compute the AWS multipart ETag: MD5 of the concatenated raw part MD5 bytes,
	// followed by "-N" where N is the part count.
	combinedHash := md5.Sum(partMD5s) //nolint:gosec // MD5 required for S3 ETag
	etag := fmt.Sprintf("\"%s-%d\"", hex.EncodeToString(combinedHash[:]), len(parts))

	return multipartAssemblyResult{
		data:           data,
		compressedData: compressedData,
		etag:           etag,
		isCompressed:   isCompressed,
	}, nil
}

// commitMultipartObject stores the assembled multipart data as an object version,
// returning the new versionID. Acquires and releases bucket.mu internally.
// tagging is an optional URL-encoded tag string to associate with the new version.
// sse, when non-zero, drives envelope encryption of the assembled body so the
// completed object is sealed under the same algorithm/key the caller chose at
// CreateMultipartUpload time.
func (b *InMemoryBackend) commitMultipartObject(
	bucket *StoredBucket,
	bucketName, key string,
	assembled multipartAssemblyResult,
	tagging string,
	sse sseInfo,
) (string, error) {
	bucket.mu.Lock(opCompleteMultipartUpload)

	obj, exists := bucket.Objects[key]
	if !exists {
		obj = &StoredObject{
			Key:      key,
			Versions: make(map[string]*StoredObjectVersion),
			mu:       lockmetrics.New("s3.object"),
		}
		bucket.Objects[key] = obj
	}

	versionID := NullVersion
	if bucket.Versioning == types.BucketVersioningStatusEnabled {
		versionID = newObjectVersionID()
	}

	// Seal the assembled body under SSE if the session was created with it.
	// On encryption failure we release the bucket lock and surface the error
	// to the caller so it can be returned as a structured S3 error response
	// rather than panicking the request goroutine.
	storedBody := assembled.compressedData
	var dek, nonce []byte
	if sse.Algorithm != "" || sse.SSECAlgorithm != "" {
		var encErr error
		storedBody, dek, nonce, encErr = encryptWithSSE(
			assembled.compressedData,
			sse,
			sse.SSECKeyB64,
		)
		if encErr != nil {
			bucket.mu.Unlock()

			return "", fmt.Errorf("commitMultipartObject: SSE encryption failed: %w", encErr)
		}
	}

	newVersion := &StoredObjectVersion{
		VersionID:       versionID,
		Key:             key,
		Data:            storedBody,
		IsCompressed:    assembled.isCompressed,
		Size:            int64(len(assembled.data)),
		ETag:            assembled.etag,
		LastModified:    time.Now(),
		IsLatest:        true,
		SSEAlgorithm:    sse.Algorithm,
		SSEKMSKeyID:     sse.KMSKeyID,
		SSECAlgorithm:   sse.SSECAlgorithm,
		SSECKeyMD5:      sse.SSECKeyMD5,
		EncryptionDEK:   dek,
		EncryptionNonce: nonce,
	}

	// Acquire obj.mu while bucket.mu is still held to prevent TOCTOU and to
	// serialize version-map mutations with concurrent readers (obj.mu.RLock).
	obj.mu.Lock(opCompleteMultipartUpload)
	bucket.mu.Unlock()

	for _, v := range obj.Versions {
		v.IsLatest = false
	}
	obj.Versions[versionID] = newVersion
	obj.LatestVersionID = versionID

	obj.mu.Unlock()

	// Store tags outside bucket.mu to respect lock ordering.
	if tagging != "" {
		b.storeObjectTags(&tagging, bucketName, key, versionID)
	}

	return versionID, nil
}

func (b *InMemoryBackend) AbortMultipartUpload(
	_ context.Context,
	input *s3.AbortMultipartUploadInput,
) (*s3.AbortMultipartUploadOutput, error) {
	uploadID := *input.UploadId
	bucketName := aws.ToString(input.Bucket)

	b.mu.Lock("AbortMultipartUpload")

	upload := b.uploads[bucketName][uploadID]
	if upload == nil {
		b.mu.Unlock()

		return nil, ErrNoSuchUpload
	}

	// Mark closed while holding b.mu so concurrent UploadPart calls that already
	// hold a pointer to this upload will observe the invalidation flag.
	upload.mu.Lock("AbortMultipartUpload")
	upload.closed = true
	upload.mu.Unlock()

	delete(b.uploads[bucketName], uploadID)
	b.mu.Unlock()

	return &s3.AbortMultipartUploadOutput{}, nil
}

// ListMultipartUploads returns in-progress multipart uploads for a bucket.
func (b *InMemoryBackend) ListMultipartUploads(
	_ context.Context,
	input *s3.ListMultipartUploadsInput,
) (*s3.ListMultipartUploadsOutput, error) {
	bucketName := aws.ToString(input.Bucket)

	b.mu.RLock("ListMultipartUploads")
	defer b.mu.RUnlock()

	if _, err := b.getBucket(bucketName); err != nil {
		return nil, err
	}

	const defaultMaxUploads = int32(1000)
	maxUploads := defaultMaxUploads
	if input.MaxUploads != nil && *input.MaxUploads > 0 && *input.MaxUploads < defaultMaxUploads {
		maxUploads = *input.MaxUploads
	}

	uploads := b.collectAndSortUploads(bucketName, aws.ToString(input.Prefix))
	uploads = seekMultipartMarker(
		uploads,
		aws.ToString(input.KeyMarker),
		aws.ToString(input.UploadIdMarker),
	)

	isTruncated, nextKeyMarker, nextUploadIDMarker := truncateUploads(&uploads, maxUploads)

	return &s3.ListMultipartUploadsOutput{
		Bucket:             aws.String(bucketName),
		Uploads:            uploads,
		MaxUploads:         aws.Int32(maxUploads),
		IsTruncated:        aws.Bool(isTruncated),
		NextKeyMarker:      aws.String(nextKeyMarker),
		NextUploadIdMarker: aws.String(nextUploadIDMarker),
	}, nil
}

// collectAndSortUploads snapshots and sorts the in-progress uploads for a bucket.
func (b *InMemoryBackend) collectAndSortUploads(bucketName, prefix string) []types.MultipartUpload {
	var uploads []types.MultipartUpload

	for _, u := range b.uploads[bucketName] {
		if prefix != "" && !strings.HasPrefix(u.Key, prefix) {
			continue
		}

		uploads = append(uploads, types.MultipartUpload{
			Key:       aws.String(u.Key),
			UploadId:  aws.String(u.UploadID),
			Initiated: aws.Time(u.Initiated),
		})
	}

	sort.Slice(uploads, func(i, j int) bool {
		ki, kj := aws.ToString(uploads[i].Key), aws.ToString(uploads[j].Key)
		if ki != kj {
			return ki < kj
		}

		return aws.ToString(uploads[i].UploadId) < aws.ToString(uploads[j].UploadId)
	})

	return uploads
}

// seekMultipartMarker skips all upload entries that come at or before the
// (keyMarker, uploadIDMarker) pagination cursor.
func seekMultipartMarker(
	uploads []types.MultipartUpload,
	keyMarker, uploadIDMarker string,
) []types.MultipartUpload {
	if keyMarker == "" {
		return uploads
	}

	for i, u := range uploads {
		k := aws.ToString(u.Key)
		if k > keyMarker {
			return uploads[i:]
		}

		if k == keyMarker && uploadIDMarker != "" && aws.ToString(u.UploadId) == uploadIDMarker {
			return uploads[i+1:]
		}
	}

	return nil
}

// truncateUploads enforces the MaxUploads page size, returning the IsTruncated flag and
// the next-page markers. The uploads slice is truncated in-place.
func truncateUploads(uploads *[]types.MultipartUpload, maxUploads int32) (bool, string, string) {
	uploadCount := int32(len(*uploads)) //nolint:gosec // G115: len is bounded by maxUploads limit
	if uploadCount <= maxUploads {
		return false, "", ""
	}

	nextKey := aws.ToString((*uploads)[maxUploads].Key)
	nextID := aws.ToString((*uploads)[maxUploads].UploadId)
	*uploads = (*uploads)[:maxUploads]

	return true, nextKey, nextID
}

// ListParts returns the parts that have been uploaded for a specific multipart upload.
const listPartsDefaultMax = int32(1000)

func (b *InMemoryBackend) ListParts(
	_ context.Context,
	input *s3.ListPartsInput,
) (*s3.ListPartsOutput, error) {
	uploadID := aws.ToString(input.UploadId)
	bucketName := aws.ToString(input.Bucket)

	b.mu.RLock("ListParts")
	upload := b.uploads[bucketName][uploadID] // reading nil map returns nil safely
	b.mu.RUnlock()

	if upload == nil {
		return nil, ErrNoSuchUpload
	}

	maxParts := listPartsDefaultMax
	if input.MaxParts != nil && *input.MaxParts > 0 && *input.MaxParts < listPartsDefaultMax {
		maxParts = *input.MaxParts
	}

	partNumberMarker := int32(0)
	if input.PartNumberMarker != nil && *input.PartNumberMarker != "" {
		if n, parseErr := strconv.ParseInt(*input.PartNumberMarker, 10, 32); parseErr == nil {
			partNumberMarker = int32(n)
		}
	}

	upload.mu.RLock("ListParts")
	partNumbers := make([]int32, 0, len(upload.Parts))
	for pn := range upload.Parts {
		partNumbers = append(partNumbers, pn)
	}

	slices.Sort(partNumbers)

	// Apply part-number-marker: skip parts whose number is <= marker.
	startIdx := sort.Search(len(partNumbers), func(i int) bool {
		return partNumbers[i] > partNumberMarker
	})
	partNumbers = partNumbers[startIdx:]

	var parts []types.Part
	for _, pn := range partNumbers {
		if int32(len(parts)) >= maxParts { //nolint:gosec // safe: len bounded by slice
			break
		}
		p := upload.Parts[pn]
		parts = append(parts, types.Part{
			PartNumber: aws.Int32(pn),
			ETag:       aws.String(p.ETag),
			Size:       aws.Int64(p.Size),
		})
	}
	upload.mu.RUnlock()

	partsCount := int32(len(parts)) //nolint:gosec // G115: bounded by maxParts
	isTruncated := partsCount == maxParts && int(partsCount) < len(partNumbers)
	var nextPartNumberMarker *string
	if isTruncated && len(parts) > 0 {
		last := parts[len(parts)-1]
		nextPartNumberMarker = aws.String(strconv.Itoa(int(aws.ToInt32(last.PartNumber))))
	}

	return &s3.ListPartsOutput{
		Bucket:               input.Bucket,
		Key:                  input.Key,
		UploadId:             input.UploadId,
		Parts:                parts,
		IsTruncated:          aws.Bool(isTruncated),
		MaxParts:             aws.Int32(maxParts),
		PartNumberMarker:     input.PartNumberMarker,
		NextPartNumberMarker: nextPartNumberMarker,
	}, nil
}

// PutBucketACL stores the canned ACL for a bucket.
func (b *InMemoryBackend) PutBucketACL(_ context.Context, bucketName, acl string) error {
	b.mu.RLock("PutBucketACL")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketACL")
	defer bucket.mu.Unlock()

	bucket.ACL = acl

	return nil
}

// GetBucketACL returns the canned ACL for a bucket.
func (b *InMemoryBackend) GetBucketACL(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetBucketACL")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketACL")
	defer bucket.mu.RUnlock()

	acl := bucket.ACL
	if acl == "" {
		acl = aclPrivate
	}

	return acl, nil
}

// nilStringIfEmpty returns nil if s is empty, otherwise returns aws.String(s).
func nilStringIfEmpty(s string) *string {
	if s == "" {
		return nil
	}

	return aws.String(s)
}

// PutBucketPolicy stores the bucket policy document.
func (b *InMemoryBackend) PutBucketPolicy(_ context.Context, bucketName, policy string) error {
	b.mu.RLock("PutBucketPolicy")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketPolicy")
	defer bucket.mu.Unlock()

	bucket.Policy = policy

	return nil
}

// GetBucketPolicy returns the bucket policy document.
func (b *InMemoryBackend) GetBucketPolicy(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetBucketPolicy")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketPolicy")
	defer bucket.mu.RUnlock()

	if bucket.Policy == "" {
		return "", ErrNoBucketPolicy
	}

	return bucket.Policy, nil
}

// DeleteBucketPolicy clears the bucket policy document.
func (b *InMemoryBackend) DeleteBucketPolicy(_ context.Context, bucketName string) error {
	b.mu.RLock("DeleteBucketPolicy")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketPolicy")
	defer bucket.mu.Unlock()

	bucket.Policy = ""

	return nil
}

// PutBucketCORS stores the bucket CORS configuration.
func (b *InMemoryBackend) PutBucketCORS(_ context.Context, bucketName, corsXML string) error {
	b.mu.RLock("PutBucketCORS")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketCORS")
	defer bucket.mu.Unlock()

	bucket.CORSConfig = corsXML

	return nil
}

// GetBucketCORS returns the bucket CORS configuration.
func (b *InMemoryBackend) GetBucketCORS(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetBucketCORS")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketCORS")
	defer bucket.mu.RUnlock()

	if bucket.CORSConfig == "" {
		return "", ErrNoCORSConfig
	}

	return bucket.CORSConfig, nil
}

// DeleteBucketCORS clears the bucket CORS configuration.
func (b *InMemoryBackend) DeleteBucketCORS(_ context.Context, bucketName string) error {
	b.mu.RLock("DeleteBucketCORS")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketCORS")
	defer bucket.mu.Unlock()

	bucket.CORSConfig = ""

	return nil
}

// PutBucketLifecycleConfiguration stores the lifecycle configuration for a bucket.
func (b *InMemoryBackend) PutBucketLifecycleConfiguration(
	_ context.Context,
	bucketName, lifecycleXML string,
) error {
	b.mu.RLock("PutBucketLifecycleConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketLifecycleConfiguration")
	defer bucket.mu.Unlock()

	bucket.LifecycleConfig = lifecycleXML

	return nil
}

// GetBucketLifecycleConfiguration returns the lifecycle configuration for a bucket.
func (b *InMemoryBackend) GetBucketLifecycleConfiguration(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketLifecycleConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketLifecycleConfiguration")
	defer bucket.mu.RUnlock()

	if bucket.LifecycleConfig == "" {
		return "", ErrNoLifecycleConfig
	}

	return bucket.LifecycleConfig, nil
}

// DeleteBucketLifecycleConfiguration clears the lifecycle configuration for a bucket.
func (b *InMemoryBackend) DeleteBucketLifecycleConfiguration(
	_ context.Context,
	bucketName string,
) error {
	b.mu.RLock("DeleteBucketLifecycleConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketLifecycleConfiguration")
	defer bucket.mu.Unlock()

	bucket.LifecycleConfig = ""

	return nil
}

// PutBucketWebsite stores the static website configuration for a bucket.
func (b *InMemoryBackend) PutBucketWebsite(_ context.Context, bucketName, websiteXML string) error {
	b.mu.RLock("PutBucketWebsite")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketWebsite")
	defer bucket.mu.Unlock()

	bucket.WebsiteConfig = websiteXML

	return nil
}

// GetBucketWebsite returns the static website configuration for a bucket.
func (b *InMemoryBackend) GetBucketWebsite(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetBucketWebsite")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketWebsite")
	defer bucket.mu.RUnlock()

	if bucket.WebsiteConfig == "" {
		return "", ErrNoWebsiteConfig
	}

	return bucket.WebsiteConfig, nil
}

// DeleteBucketWebsite clears the static website configuration for a bucket.
func (b *InMemoryBackend) DeleteBucketWebsite(_ context.Context, bucketName string) error {
	b.mu.RLock("DeleteBucketWebsite")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketWebsite")
	defer bucket.mu.Unlock()

	bucket.WebsiteConfig = ""

	return nil
}

// PutBucketEncryption stores the server-side encryption configuration for a bucket.
func (b *InMemoryBackend) PutBucketEncryption(
	_ context.Context,
	bucketName, encryptionXML string,
) error {
	b.mu.RLock("PutBucketEncryption")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketEncryption")
	defer bucket.mu.Unlock()

	bucket.EncryptionConfig = encryptionXML

	return nil
}

// GetBucketEncryption returns the server-side encryption configuration for a bucket.
func (b *InMemoryBackend) GetBucketEncryption(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketEncryption")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketEncryption")
	defer bucket.mu.RUnlock()

	if bucket.EncryptionConfig == "" {
		return "", ErrNoEncryptionConfig
	}

	return bucket.EncryptionConfig, nil
}

// DeleteBucketEncryption clears the server-side encryption configuration for a bucket.
func (b *InMemoryBackend) DeleteBucketEncryption(_ context.Context, bucketName string) error {
	b.mu.RLock("DeleteBucketEncryption")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketEncryption")
	defer bucket.mu.Unlock()

	bucket.EncryptionConfig = ""

	return nil
}

// PutPublicAccessBlock stores the public access block configuration for a bucket.
func (b *InMemoryBackend) PutPublicAccessBlock(
	_ context.Context,
	bucketName, configXML string,
) error {
	b.mu.RLock("PutPublicAccessBlock")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutPublicAccessBlock")
	defer bucket.mu.Unlock()

	bucket.PublicAccessBlockConfig = configXML

	return nil
}

// GetPublicAccessBlock returns the public access block configuration for a bucket.
func (b *InMemoryBackend) GetPublicAccessBlock(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetPublicAccessBlock")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetPublicAccessBlock")
	defer bucket.mu.RUnlock()

	if bucket.PublicAccessBlockConfig == "" {
		return "", ErrNoPublicAccessBlock
	}

	return bucket.PublicAccessBlockConfig, nil
}

// DeletePublicAccessBlock removes the public access block configuration for a bucket.
func (b *InMemoryBackend) DeletePublicAccessBlock(_ context.Context, bucketName string) error {
	b.mu.RLock("DeletePublicAccessBlock")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeletePublicAccessBlock")
	defer bucket.mu.Unlock()

	bucket.PublicAccessBlockConfig = ""

	return nil
}

// PutBucketOwnershipControls stores the ownership controls configuration for a bucket.
func (b *InMemoryBackend) PutBucketOwnershipControls(
	_ context.Context,
	bucketName, configXML string,
) error {
	b.mu.RLock("PutBucketOwnershipControls")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketOwnershipControls")
	defer bucket.mu.Unlock()

	bucket.OwnershipControlsConfig = configXML

	return nil
}

// GetBucketOwnershipControls returns the ownership controls configuration for a bucket.
func (b *InMemoryBackend) GetBucketOwnershipControls(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketOwnershipControls")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketOwnershipControls")
	defer bucket.mu.RUnlock()

	if bucket.OwnershipControlsConfig == "" {
		return "", ErrNoOwnershipControls
	}

	return bucket.OwnershipControlsConfig, nil
}

// DeleteBucketOwnershipControls removes the ownership controls configuration for a bucket.
func (b *InMemoryBackend) DeleteBucketOwnershipControls(
	_ context.Context,
	bucketName string,
) error {
	b.mu.RLock("DeleteBucketOwnershipControls")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketOwnershipControls")
	defer bucket.mu.Unlock()

	bucket.OwnershipControlsConfig = ""

	return nil
}

// PutBucketLogging stores the logging configuration for a bucket.
func (b *InMemoryBackend) PutBucketLogging(_ context.Context, bucketName, loggingXML string) error {
	b.mu.RLock("PutBucketLogging")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketLogging")
	defer bucket.mu.Unlock()

	bucket.LoggingConfig = loggingXML

	return nil
}

// GetBucketLogging returns the logging configuration for a bucket.
// Returns "" (empty string) when no logging is configured; the handler
// synthesizes the AWS-compatible empty XML response.
func (b *InMemoryBackend) GetBucketLogging(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetBucketLogging")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketLogging")
	defer bucket.mu.RUnlock()

	return bucket.LoggingConfig, nil
}

// PutBucketReplication stores the replication configuration for a bucket.
func (b *InMemoryBackend) PutBucketReplication(
	_ context.Context,
	bucketName, replicationXML string,
) error {
	b.mu.RLock("PutBucketReplication")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketReplication")
	defer bucket.mu.Unlock()

	bucket.ReplicationConfig = replicationXML

	return nil
}

// GetBucketReplication returns the replication configuration for a bucket.
func (b *InMemoryBackend) GetBucketReplication(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketReplication")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketReplication")
	defer bucket.mu.RUnlock()

	if bucket.ReplicationConfig == "" {
		return "", ErrNoReplicationConfig
	}

	return bucket.ReplicationConfig, nil
}

// DeleteBucketReplication removes the replication configuration for a bucket.
func (b *InMemoryBackend) DeleteBucketReplication(_ context.Context, bucketName string) error {
	b.mu.RLock("DeleteBucketReplication")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketReplication")
	defer bucket.mu.Unlock()

	bucket.ReplicationConfig = ""

	return nil
}

func (b *InMemoryBackend) PutBucketNotificationConfiguration(
	_ context.Context,
	bucketName, notifXML string,
) error {
	b.mu.RLock("PutBucketNotificationConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketNotificationConfiguration")
	defer bucket.mu.Unlock()

	bucket.NotificationConfig = notifXML

	return nil
}

// GetBucketNotificationConfiguration returns the notification configuration for a bucket.
func (b *InMemoryBackend) GetBucketNotificationConfiguration(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketNotificationConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketNotificationConfiguration")
	defer bucket.mu.RUnlock()

	// Notification config is always returned, even if empty (AWS returns empty XML)
	return bucket.NotificationConfig, nil
}

// PutObjectLockConfiguration stores the object lock configuration for a bucket.
func (b *InMemoryBackend) PutObjectLockConfiguration(
	_ context.Context,
	bucketName, configXML string,
) error {
	b.mu.RLock("PutObjectLockConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutObjectLockConfiguration")
	defer bucket.mu.Unlock()

	bucket.ObjectLockConfig = configXML

	return nil
}

// GetObjectLockConfiguration retrieves the object lock configuration for a bucket.
func (b *InMemoryBackend) GetObjectLockConfiguration(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetObjectLockConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetObjectLockConfiguration")
	defer bucket.mu.RUnlock()

	if bucket.ObjectLockConfig == "" {
		return "", ErrNoObjectLockConfig
	}

	return bucket.ObjectLockConfig, nil
}

// PutObjectRetention sets the retention mode and retain-until-date for a specific object version.
func (b *InMemoryBackend) PutObjectRetention(
	_ context.Context,
	bucketName, key string,
	versionID *string,
	mode string,
	retainUntil time.Time,
) error {
	b.mu.RLock("PutObjectRetention")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutObjectRetention")
	defer bucket.mu.Unlock()

	ver, findErr := findObjectVersion(bucket, key, versionID)
	if findErr != nil {
		return findErr
	}

	ver.RetentionMode = mode
	ver.RetainUntil = retainUntil

	return nil
}

// GetObjectRetention returns the retention mode and retain-until-date for a specific object version.
func (b *InMemoryBackend) GetObjectRetention(
	_ context.Context,
	bucketName, key string,
	versionID *string,
) (string, time.Time, error) {
	b.mu.RLock("GetObjectRetention")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", time.Time{}, err
	}

	bucket.mu.RLock("GetObjectRetention")
	defer bucket.mu.RUnlock()

	ver, findErr := findObjectVersion(bucket, key, versionID)
	if findErr != nil {
		return "", time.Time{}, findErr
	}

	if ver.RetentionMode == "" {
		return "", time.Time{}, ErrNoSuchObjectLockConfig
	}

	return ver.RetentionMode, ver.RetainUntil, nil
}

// PutObjectLegalHold sets or clears the legal hold status for a specific object version.
func (b *InMemoryBackend) PutObjectLegalHold(
	_ context.Context,
	bucketName, key string,
	versionID *string,
	status string,
) error {
	b.mu.RLock("PutObjectLegalHold")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutObjectLegalHold")
	defer bucket.mu.Unlock()

	ver, findErr := findObjectVersion(bucket, key, versionID)
	if findErr != nil {
		return findErr
	}

	ver.LegalHold = status == "ON"

	return nil
}

// GetObjectLegalHold returns the legal hold status for a specific object version.
func (b *InMemoryBackend) GetObjectLegalHold(
	_ context.Context,
	bucketName, key string,
	versionID *string,
) (string, error) {
	b.mu.RLock("GetObjectLegalHold")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetObjectLegalHold")
	defer bucket.mu.RUnlock()

	ver, findErr := findObjectVersion(bucket, key, versionID)
	if findErr != nil {
		return "", findErr
	}

	if ver.LegalHold {
		return "ON", nil
	}

	return "OFF", nil
}

// findObjectVersion returns the specified object version (or the latest version if versionID is nil).
// Must be called with at least a read lock on bucket.mu.
func findObjectVersion(
	bucket *StoredBucket,
	key string,
	versionID *string,
) (*StoredObjectVersion, error) {
	obj, exists := bucket.Objects[key]
	if !exists {
		return nil, ErrNoSuchKey
	}

	if versionID != nil && *versionID != "" {
		ver, ok := obj.Versions[*versionID]
		if !ok || ver.Deleted {
			return nil, ErrNoSuchKey
		}

		return ver, nil
	}

	// Find latest
	if obj.LatestVersionID != "" {
		ver := obj.Versions[obj.LatestVersionID]
		if ver != nil && !ver.Deleted {
			return ver, nil
		}
	}

	for _, ver := range obj.Versions {
		if ver.IsLatest && !ver.Deleted {
			return ver, nil
		}
	}

	return nil, ErrNoSuchKey
}

// PutBucketTagging sets the tag set for a bucket.
func (b *InMemoryBackend) PutBucketTagging(
	_ context.Context,
	bucketName string,
	tags []types.Tag,
) error {
	b.mu.RLock("PutBucketTagging")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketTagging")
	defer bucket.mu.Unlock()

	bucket.Tags = tags

	return nil
}

// GetBucketTagging returns the tag set for a bucket.
func (b *InMemoryBackend) GetBucketTagging(
	_ context.Context,
	bucketName string,
) ([]types.Tag, error) {
	b.mu.RLock("GetBucketTagging")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("GetBucketTagging")
	defer bucket.mu.RUnlock()

	if len(bucket.Tags) == 0 {
		return nil, ErrNoSuchTagSet
	}

	result := make([]types.Tag, len(bucket.Tags))
	copy(result, bucket.Tags)

	return result, nil
}

// DeleteBucketTagging removes the tag set from a bucket.
func (b *InMemoryBackend) DeleteBucketTagging(_ context.Context, bucketName string) error {
	b.mu.RLock("DeleteBucketTagging")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketTagging")
	defer bucket.mu.Unlock()

	bucket.Tags = nil

	return nil
}

// PutBucketAnalyticsConfiguration stores an analytics configuration for a bucket by ID.
func (b *InMemoryBackend) PutBucketAnalyticsConfiguration(
	_ context.Context,
	bucketName, id, configXML string,
) error {
	b.mu.RLock("PutBucketAnalyticsConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketAnalyticsConfiguration")
	defer bucket.mu.Unlock()

	if bucket.AnalyticsConfigs == nil {
		bucket.AnalyticsConfigs = make(map[string]string)
	}

	bucket.AnalyticsConfigs[id] = configXML

	return nil
}

// GetBucketAnalyticsConfiguration returns an analytics configuration for a bucket by ID.
func (b *InMemoryBackend) GetBucketAnalyticsConfiguration(
	_ context.Context,
	bucketName, id string,
) (string, error) {
	b.mu.RLock("GetBucketAnalyticsConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketAnalyticsConfiguration")
	defer bucket.mu.RUnlock()

	config, ok := bucket.AnalyticsConfigs[id]
	if !ok {
		return "", ErrNoAnalyticsConfig
	}

	return config, nil
}

// DeleteBucketAnalyticsConfiguration removes an analytics configuration from a bucket by ID.
func (b *InMemoryBackend) DeleteBucketAnalyticsConfiguration(
	_ context.Context,
	bucketName, id string,
) error {
	b.mu.RLock("DeleteBucketAnalyticsConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketAnalyticsConfiguration")
	defer bucket.mu.Unlock()

	delete(bucket.AnalyticsConfigs, id)

	return nil
}

// ListBucketAnalyticsConfigurations returns all analytics configurations for a bucket.
func (b *InMemoryBackend) ListBucketAnalyticsConfigurations(
	_ context.Context,
	bucketName string,
) ([]string, error) {
	b.mu.RLock("ListBucketAnalyticsConfigurations")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("ListBucketAnalyticsConfigurations")
	defer bucket.mu.RUnlock()

	configs := make([]string, 0, len(bucket.AnalyticsConfigs))
	for _, v := range bucket.AnalyticsConfigs {
		configs = append(configs, v)
	}

	return configs, nil
}

// PutBucketIntelligentTieringConfiguration stores an Intelligent-Tiering configuration for a bucket by ID.
func (b *InMemoryBackend) PutBucketIntelligentTieringConfiguration(
	_ context.Context,
	bucketName, id, configXML string,
) error {
	b.mu.RLock("PutBucketIntelligentTieringConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketIntelligentTieringConfiguration")
	defer bucket.mu.Unlock()

	if bucket.IntelligentTieringConfigs == nil {
		bucket.IntelligentTieringConfigs = make(map[string]string)
	}

	bucket.IntelligentTieringConfigs[id] = configXML

	return nil
}

// GetBucketIntelligentTieringConfiguration returns an Intelligent-Tiering configuration for a bucket by ID.
func (b *InMemoryBackend) GetBucketIntelligentTieringConfiguration(
	_ context.Context,
	bucketName, id string,
) (string, error) {
	b.mu.RLock("GetBucketIntelligentTieringConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketIntelligentTieringConfiguration")
	defer bucket.mu.RUnlock()

	config, ok := bucket.IntelligentTieringConfigs[id]
	if !ok {
		return "", ErrNoIntelligentTieringConfig
	}

	return config, nil
}

// DeleteBucketIntelligentTieringConfiguration removes an Intelligent-Tiering configuration from a bucket by ID.
func (b *InMemoryBackend) DeleteBucketIntelligentTieringConfiguration(
	_ context.Context,
	bucketName, id string,
) error {
	b.mu.RLock("DeleteBucketIntelligentTieringConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketIntelligentTieringConfiguration")
	defer bucket.mu.Unlock()

	delete(bucket.IntelligentTieringConfigs, id)

	return nil
}

// ListBucketIntelligentTieringConfigurations returns all Intelligent-Tiering configurations for a bucket.
func (b *InMemoryBackend) ListBucketIntelligentTieringConfigurations(
	_ context.Context,
	bucketName string,
) ([]string, error) {
	b.mu.RLock("ListBucketIntelligentTieringConfigurations")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("ListBucketIntelligentTieringConfigurations")
	defer bucket.mu.RUnlock()

	configs := make([]string, 0, len(bucket.IntelligentTieringConfigs))
	for _, v := range bucket.IntelligentTieringConfigs {
		configs = append(configs, v)
	}

	return configs, nil
}

// PutBucketInventoryConfiguration stores an inventory configuration for a bucket by ID.
func (b *InMemoryBackend) PutBucketInventoryConfiguration(
	_ context.Context,
	bucketName, id, configXML string,
) error {
	b.mu.RLock("PutBucketInventoryConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketInventoryConfiguration")
	defer bucket.mu.Unlock()

	if bucket.InventoryConfigs == nil {
		bucket.InventoryConfigs = make(map[string]string)
	}

	bucket.InventoryConfigs[id] = configXML

	return nil
}

// GetBucketInventoryConfiguration returns an inventory configuration for a bucket by ID.
func (b *InMemoryBackend) GetBucketInventoryConfiguration(
	_ context.Context,
	bucketName, id string,
) (string, error) {
	b.mu.RLock("GetBucketInventoryConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketInventoryConfiguration")
	defer bucket.mu.RUnlock()

	config, ok := bucket.InventoryConfigs[id]
	if !ok {
		return "", ErrNoInventoryConfig
	}

	return config, nil
}

// DeleteBucketInventoryConfiguration removes an inventory configuration from a bucket by ID.
func (b *InMemoryBackend) DeleteBucketInventoryConfiguration(
	_ context.Context,
	bucketName, id string,
) error {
	b.mu.RLock("DeleteBucketInventoryConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketInventoryConfiguration")
	defer bucket.mu.Unlock()

	delete(bucket.InventoryConfigs, id)

	return nil
}

// ListBucketInventoryConfigurations returns all inventory configurations for a bucket.
func (b *InMemoryBackend) ListBucketInventoryConfigurations(
	_ context.Context,
	bucketName string,
) ([]string, error) {
	b.mu.RLock("ListBucketInventoryConfigurations")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("ListBucketInventoryConfigurations")
	defer bucket.mu.RUnlock()

	configs := make([]string, 0, len(bucket.InventoryConfigs))
	for _, v := range bucket.InventoryConfigs {
		configs = append(configs, v)
	}

	return configs, nil
}

// PutBucketMetricsConfiguration stores a metrics configuration for a bucket by ID.
func (b *InMemoryBackend) PutBucketMetricsConfiguration(
	_ context.Context,
	bucketName, id, configXML string,
) error {
	b.mu.RLock("PutBucketMetricsConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketMetricsConfiguration")
	defer bucket.mu.Unlock()

	if bucket.MetricsConfigs == nil {
		bucket.MetricsConfigs = make(map[string]string)
	}

	bucket.MetricsConfigs[id] = configXML

	return nil
}

// GetBucketMetricsConfiguration returns a metrics configuration for a bucket by ID.
func (b *InMemoryBackend) GetBucketMetricsConfiguration(
	_ context.Context,
	bucketName, id string,
) (string, error) {
	b.mu.RLock("GetBucketMetricsConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketMetricsConfiguration")
	defer bucket.mu.RUnlock()

	config, ok := bucket.MetricsConfigs[id]
	if !ok {
		return "", ErrNoMetricsConfig
	}

	return config, nil
}

// DeleteBucketMetricsConfiguration removes a metrics configuration from a bucket by ID.
func (b *InMemoryBackend) DeleteBucketMetricsConfiguration(
	_ context.Context,
	bucketName, id string,
) error {
	b.mu.RLock("DeleteBucketMetricsConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketMetricsConfiguration")
	defer bucket.mu.Unlock()

	delete(bucket.MetricsConfigs, id)

	return nil
}

// ListBucketMetricsConfigurations returns all metrics configurations for a bucket.
func (b *InMemoryBackend) ListBucketMetricsConfigurations(
	_ context.Context,
	bucketName string,
) ([]string, error) {
	b.mu.RLock("ListBucketMetricsConfigurations")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("ListBucketMetricsConfigurations")
	defer bucket.mu.RUnlock()

	configs := make([]string, 0, len(bucket.MetricsConfigs))
	for _, v := range bucket.MetricsConfigs {
		configs = append(configs, v)
	}

	return configs, nil
}

// DeleteBucketLifecycle clears the lifecycle configuration for a bucket.
// This is the legacy alias for DeleteBucketLifecycleConfiguration.
func (b *InMemoryBackend) DeleteBucketLifecycle(ctx context.Context, bucketName string) error {
	return b.DeleteBucketLifecycleConfiguration(ctx, bucketName)
}

// CreateBucketMetadataConfiguration stores the metadata configuration for a bucket.
func (b *InMemoryBackend) CreateBucketMetadataConfiguration(
	_ context.Context,
	bucketName, configXML string,
) error {
	b.mu.RLock("CreateBucketMetadataConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("CreateBucketMetadataConfiguration")
	defer bucket.mu.Unlock()

	bucket.MetadataConfig = configXML

	return nil
}

// GetBucketMetadataConfiguration returns the metadata configuration for a bucket.
func (b *InMemoryBackend) GetBucketMetadataConfiguration(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketMetadataConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketMetadataConfiguration")
	defer bucket.mu.RUnlock()

	if bucket.MetadataConfig == "" {
		return "", ErrNoMetadataConfig
	}

	return bucket.MetadataConfig, nil
}

// DeleteBucketMetadataConfiguration clears the metadata configuration for a bucket.
func (b *InMemoryBackend) DeleteBucketMetadataConfiguration(
	_ context.Context,
	bucketName string,
) error {
	b.mu.RLock("DeleteBucketMetadataConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketMetadataConfiguration")
	defer bucket.mu.Unlock()

	bucket.MetadataConfig = ""

	return nil
}

// CreateBucketMetadataTableConfiguration stores the metadata table configuration for a bucket.
func (b *InMemoryBackend) CreateBucketMetadataTableConfiguration(
	_ context.Context,
	bucketName, configXML string,
) error {
	b.mu.RLock("CreateBucketMetadataTableConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("CreateBucketMetadataTableConfiguration")
	defer bucket.mu.Unlock()

	bucket.MetadataTableConfig = configXML

	return nil
}

// GetBucketMetadataTableConfiguration returns the metadata table configuration for a bucket.
func (b *InMemoryBackend) GetBucketMetadataTableConfiguration(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketMetadataTableConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketMetadataTableConfiguration")
	defer bucket.mu.RUnlock()

	if bucket.MetadataTableConfig == "" {
		return "", ErrNoMetadataTableConfig
	}

	return bucket.MetadataTableConfig, nil
}

// DeleteBucketMetadataTableConfiguration clears the metadata table configuration for a bucket.
func (b *InMemoryBackend) DeleteBucketMetadataTableConfiguration(
	_ context.Context,
	bucketName string,
) error {
	b.mu.RLock("DeleteBucketMetadataTableConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketMetadataTableConfiguration")
	defer bucket.mu.Unlock()

	bucket.MetadataTableConfig = ""

	return nil
}

// CreateSession returns a stub session response for a bucket (S3 Express One Zone).
func (b *InMemoryBackend) CreateSession(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("CreateSession")
	_, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	const sessionXML = `<CreateSessionResponse xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<Credentials>` +
		`<SessionToken>gopherstack-mock-session-token</SessionToken>` +
		`<SecretAccessKey>gopherstack-mock-secret</SecretAccessKey>` +
		`<AccessKeyId>gopherstack-mock-access-key</AccessKeyId>` +
		`<Expiration>2099-01-01T00:00:00Z</Expiration>` +
		`</Credentials></CreateSessionResponse>`

	return sessionXML, nil
}

// purgeBucketLocked removes a single bucket and its associated data from the
// backend. Caller must hold b.mu.
func (b *InMemoryBackend) purgeBucketLocked(
	regionBuckets map[string]*StoredBucket,
	bucketName string,
	bucket *StoredBucket,
) {
	// Close per-object mutexes to avoid Prometheus metric leaks.
	for _, obj := range bucket.Objects {
		obj.mu.Close()
	}

	bucket.mu.Close()
	delete(regionBuckets, bucketName)
	delete(b.bucketIndex, bucketName)

	for tagKey := range b.tags {
		if strings.HasPrefix(tagKey, bucketName+"/") {
			delete(b.tags, tagKey)
		}
	}

	delete(b.uploads, bucketName)
}

// Purge removes all buckets created before the given cutoff time.
func (b *InMemoryBackend) Purge(ctx context.Context, cutoff time.Time) {
	if ctx.Err() != nil {
		return
	}

	b.mu.Lock("Purge")
	defer b.mu.Unlock()

	for _, regionBuckets := range b.buckets {
		for bucketName, bucket := range regionBuckets {
			if ctx.Err() != nil {
				return
			}

			if bucket.CreationDate.Before(cutoff) {
				b.purgeBucketLocked(regionBuckets, bucketName, bucket)
			}
		}
	}
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// Close all bucket and object mutexes to prevent Prometheus metric leaks.
	for _, regionBuckets := range b.buckets {
		for _, bucket := range regionBuckets {
			for _, obj := range bucket.Objects {
				obj.mu.Close()
			}
			bucket.mu.Close()
		}
	}

	b.buckets = make(map[string]map[string]*StoredBucket)
	b.bucketIndex = make(map[string]string)
	b.tags = make(map[string][]types.Tag)
	b.uploads = make(map[string]map[string]*StoredMultipartUpload)
}

func (b *InMemoryBackend) GetBucketMetadata(
	_ context.Context,
	bucketName string,
) (string, string, []types.Tag, error) {
	b.mu.RLock("GetBucketMetadata")
	region, ok := b.bucketIndex[bucketName]
	if !ok {
		b.mu.RUnlock()

		return "", "", nil, ErrNoSuchBucket
	}
	bucket := b.buckets[region][bucketName]
	b.mu.RUnlock()

	bucket.mu.RLock("GetBucketMetadata")
	lcXML := bucket.LifecycleConfig
	tags := slices.Clone(bucket.Tags)
	bucket.mu.RUnlock()

	return region, lcXML, tags, nil
}

// verifyChecksum validates the S3 checksum if a hasher is provided.
func (b *InMemoryBackend) verifyChecksum(
	input *s3.UploadPartInput,
	s3Hasher hash.Hash,
	algo string,
) error {
	if s3Hasher == nil {
		return nil
	}

	computedSum := s3Hasher.Sum(nil)

	// Go's crc32 Sum(nil) may not be big-endian. S3 expects big-endian for CRC32/CRC32C.
	if h32, ok := s3Hasher.(hash.Hash32); ok {
		const checksumSize = 4
		tmp := make([]byte, checksumSize)
		binary.BigEndian.PutUint32(tmp, h32.Sum32())
		computedSum = tmp
	}

	computedChecksumB64 := base64.StdEncoding.EncodeToString(computedSum)

	var supplied *string

	switch strings.ToUpper(algo) {
	case ChecksumCRC32:
		supplied = input.ChecksumCRC32
	case ChecksumCRC32C:
		supplied = input.ChecksumCRC32C
	case ChecksumSHA1:
		supplied = input.ChecksumSHA1
	case ChecksumSHA256:
		supplied = input.ChecksumSHA256
	}

	if supplied != nil && *supplied != "" {
		if computedChecksumB64 != *supplied {
			return ErrBadChecksum
		}

		return nil
	}

	// Client requested server-side checksum computation; propagate result.
	switch strings.ToUpper(algo) {
	case ChecksumCRC32:
		input.ChecksumCRC32 = aws.String(computedChecksumB64)
	case ChecksumCRC32C:
		input.ChecksumCRC32C = aws.String(computedChecksumB64)
	case ChecksumSHA1:
		input.ChecksumSHA1 = aws.String(computedChecksumB64)
	case ChecksumSHA256:
		input.ChecksumSHA256 = aws.String(computedChecksumB64)
	}

	return nil
}

// storePart saves a multipart upload part under the per-upload lock.
func (b *InMemoryBackend) storePart(
	bucketName, uploadID string,
	partNumber int32,
	part *StoredPart,
) error {
	b.mu.RLock("storePart")
	bucketUploads, ok := b.uploads[bucketName]
	var upload *StoredMultipartUpload
	if ok {
		upload, ok = bucketUploads[uploadID]
	}
	b.mu.RUnlock()

	if !ok {
		return ErrNoSuchUpload
	}

	upload.mu.Lock("storePart")
	defer upload.mu.Unlock()

	if upload.closed {
		return ErrNoSuchUpload
	}

	upload.Parts[partNumber] = part

	return nil
}

// checkPutObjectAuthAndLock performs initial checks for bucket existence and object lock.
func (b *InMemoryBackend) checkPutObjectAuthAndLock(bucketName, key string) (*StoredBucket, error) {
	b.mu.RLock("PutObjectCheck")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("PutObjectCheck")
	defer bucket.mu.RUnlock()

	isVersioningEnabled := bucket.Versioning == types.BucketVersioningStatusEnabled
	if !isVersioningEnabled {
		if obj, ok := bucket.Objects[key]; ok {
			err = checkObjectLockForOverwrite(obj)
			if err != nil {
				return nil, err
			}
		}
	}

	return bucket, nil
}

// saveObjectVersion saves an object version under the bucket lock.
func (b *InMemoryBackend) saveObjectVersion(
	bucket *StoredBucket,
	key string,
	ver *StoredObjectVersion,
) string {
	bucket.mu.Lock("saveObjectVersion")
	defer bucket.mu.Unlock()

	// Handle versioning
	if ver.VersionID == NullVersion && bucket.Versioning == types.BucketVersioningStatusEnabled {
		ver.VersionID = newObjectVersionID()
	}

	obj, ok := bucket.Objects[key]
	if !ok {
		obj = &StoredObject{
			Key:      key,
			Versions: make(map[string]*StoredObjectVersion),
			mu:       lockmetrics.New("s3.object"),
		}
		bucket.Objects[key] = obj
	}

	// Capture obj.mu while bucket lock is held
	obj.mu.Lock("saveObjectVersionObj")
	defer obj.mu.Unlock()

	for _, v := range obj.Versions {
		v.IsLatest = false
	}

	obj.Versions[ver.VersionID] = ver
	obj.LatestVersionID = ver.VersionID

	return ver.VersionID
}

// computeObjectHashes snapshots the body while computing MD5 and S3 checksums.
func (b *InMemoryBackend) computeObjectHashes(
	_ context.Context,
	body io.Reader,
	algorithm types.ChecksumAlgorithm,
) (int64, []byte, string, hash.Hash, error) {
	//nolint:gosec // MD5 required for S3 ETag
	md5Hasher := md5.New()
	var buf bytes.Buffer
	writers := []io.Writer{md5Hasher, &buf}

	var s3Hasher hash.Hash
	algo := string(algorithm)
	if algo != "" {
		switch strings.ToUpper(algo) {
		case ChecksumCRC32:
			s3Hasher = crc32.NewIEEE()
		case ChecksumCRC32C:
			s3Hasher = crc32.New(crc32.MakeTable(crc32.Castagnoli))
		case ChecksumSHA1:
			//nolint:gosec // SHA1 supported
			s3Hasher = sha1.New()
		case ChecksumSHA256:
			s3Hasher = sha256.New()
		case ChecksumCRC64NVME:
			s3Hasher = NewCRC64NVME()
		}
		if s3Hasher != nil {
			writers = append(writers, s3Hasher)
		}
	}

	tr := io.TeeReader(body, io.MultiWriter(writers...))
	n, err := io.Copy(io.Discard, tr)
	if err != nil {
		return 0, nil, "", nil, err
	}

	return n, buf.Bytes(), hex.EncodeToString(md5Hasher.Sum(nil)), s3Hasher, nil
}

// validateContentMD5 validates the Content-MD5 header from context against the computed etag.
func (b *InMemoryBackend) validateContentMD5(ctx context.Context, _ []byte, etag string) error {
	if md5Header, ok := ctx.Value(md5Key).(string); ok && md5Header != "" {
		decoded, dErr := base64.StdEncoding.DecodeString(md5Header)
		if dErr != nil || len(decoded) != md5.Size {
			return ErrBadChecksum
		}

		if hex.EncodeToString(decoded) != etag {
			return ErrBadChecksum
		}
	}

	return nil
}

func (b *InMemoryBackend) truncateListResults(
	contents []types.Object,
	cpList []types.CommonPrefix,
	maxKeys int32,
) ([]types.Object, []types.CommonPrefix, bool, string) {
	// AWS clamps MaxKeys to [0, 1000]; a zero value means return no objects.
	if maxKeys <= 0 {
		return nil, nil, len(contents)+len(cpList) > 0, ""
	}

	totalCount64 := int64(len(contents)) + int64(len(cpList))
	if totalCount64 <= int64(maxKeys) {
		return contents, cpList, false, ""
	}

	isTruncated := true
	var nextMarker string

	if int64(len(contents)) > int64(maxKeys) {
		nextMarker = aws.ToString(contents[maxKeys-1].Key)
		contents = contents[:maxKeys]
		cpList = nil
	} else {
		remaining := int64(maxKeys) - int64(len(contents))
		if remaining > 0 {
			// Some CommonPrefixes fit on this page; the marker is the last prefix
			// we return so the next page resumes after it.
			nextMarker = aws.ToString(cpList[remaining-1].Prefix)
			cpList = cpList[:remaining]
		} else {
			// The page is filled exactly by object keys with CommonPrefixes still
			// pending. Resume from the last returned key and defer the prefixes to
			// the next page, otherwise IsTruncated=true would carry an empty marker
			// and the client could never fetch the remaining prefixes.
			nextMarker = aws.ToString(contents[len(contents)-1].Key)
			cpList = nil
		}
	}

	return contents, cpList, isTruncated, nextMarker
}
