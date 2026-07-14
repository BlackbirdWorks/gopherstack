package s3

import (
	"bytes"
	"context"
	"crypto/md5"  //nolint:gosec // MD5 required for S3 ETag compatibility
	"crypto/sha1" //nolint:gosec // SHA1 required for S3 checksum compatibility
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"maps"
	"net/url"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

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

	// Apply default bucket encryption if no SSE is specified in the request
	if sseFromCtx.Algorithm == "" && sseFromCtx.SSECAlgorithm == "" && bucket.EncryptionConfig != "" {
		var config ServerSideEncryptionConfiguration
		if xmlErr := xml.Unmarshal([]byte(bucket.EncryptionConfig), &config); xmlErr == nil && len(config.Rules) > 0 {
			def := config.Rules[0].ApplyServerSideEncryptionByDefault
			if def.SSEAlgorithm != "" {
				sseFromCtx.Algorithm = def.SSEAlgorithm
				sseFromCtx.KMSKeyID = def.KMSMasterKeyID
			}
		}
	}

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
	newVersion := buildStoredObjectVersion(key, finalQuotedETag, encryptedData, isCompressed,
		originalSize, input, checksums, sseFromCtx, dek, nonce)

	newVersionID := b.saveObjectVersion(bucket, key, newVersion)

	// Store tags outside bucket.mu to respect the lock ordering
	// (b.mu must not be acquired while bucket.mu is held).
	// When overwriting a non-versioned object (NullVersion) without new tags,
	// evict any stale tags from the previous version to prevent b.tags from growing unbounded.
	if newVersionID == NullVersion && input.Tagging == nil {
		b.mu.Lock("PutObject.evictTags")
		if b.tags != nil {
			delete(b.tags, fmt.Sprintf("%s/%s/%s", bucketName, key, NullVersion))
		}
		b.mu.Unlock()
	}
	b.storeObjectTags(input.Tagging, bucketName, key, newVersionID)

	logger.Load(ctx).DebugContext(ctx, "S3 Backend PutObject",
		"bucket", bucketName, "key", key,
		"contentType", aws.ToString(input.ContentType),
		"versionId", newVersionID)

	// Async replication to configured destination buckets, parented to the
	// service context (cancellable on shutdown) rather than the request context.
	repCtx := b.replicationContext(ctx)
	b.replicationWg.Go(func() {
		b.triggerReplication(repCtx, bucketName, key, finalQuotedETag)
	})

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

func buildStoredObjectVersion(
	key, etag string,
	data []byte,
	isCompressed bool,
	size int64,
	input *s3.PutObjectInput,
	checksums objectChecksums,
	sse sseInfo,
	dek, nonce []byte,
) *StoredObjectVersion {
	sc := string(input.StorageClass)
	if sc == "" {
		sc = storageStandard
	}

	return &StoredObjectVersion{
		VersionID:          NullVersion,
		Key:                key,
		Data:               data,
		IsCompressed:       isCompressed,
		Size:               size,
		ETag:               etag,
		LastModified:       time.Now().UTC(),
		ContentType:        aws.ToString(input.ContentType),
		ContentEncoding:    aws.ToString(input.ContentEncoding),
		ContentDisposition: aws.ToString(input.ContentDisposition),
		StorageClass:       sc,
		Metadata:           maps.Clone(input.Metadata),
		ChecksumCRC32:      checksums.crc32,
		ChecksumCRC32C:     checksums.crc32c,
		ChecksumSHA1:       checksums.sha1,
		ChecksumSHA256:     checksums.sha256,
		ChecksumCRC64NVME:  checksums.crc64nvme,
		ChecksumAlgorithm:  input.ChecksumAlgorithm,
		SSEAlgorithm:       sse.Algorithm,
		SSEKMSKeyID:        sse.KMSKeyID,
		SSECAlgorithm:      sse.SSECAlgorithm,
		SSECKeyMD5:         sse.SSECKeyMD5,
		EncryptionDEK:      dek,
		EncryptionNonce:    nonce,
		IsLatest:           true,
	}
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

	ver, err := resolveObjectVersion(obj, versionID)
	if err != nil {
		return nil, err
	}

	// Copy data + metadata under the lock; decryption + decompression
	// happen outside.
	dataToDecompress := ver.Data
	isCompressed := ver.IsCompressed
	size := ver.Size
	metadata := maps.Clone(ver.Metadata)
	versionIDStr := ver.VersionID

	decrypted, skipDecompress, decErr := decryptVersionForGet(ctx, ver, dataToDecompress)
	if decErr != nil {
		return nil, decErr
	}

	if skipDecompress {
		return buildGetObjectOutput(decrypted, size, ver, metadata, versionIDStr), nil
	}
	dataToDecompress = decrypted

	data, err := b.decompressObjectData(dataToDecompress, isCompressed)
	if err != nil {
		return nil, err
	}

	return buildGetObjectOutput(data, size, ver, metadata, versionIDStr), nil
}

// resolveObjectVersion selects the requested (or latest) live version of an
// object, translating delete markers and missing versions into the proper
// S3 errors. The caller must hold obj's read lock.
func resolveObjectVersion(
	obj *StoredObject,
	versionID *string,
) (*StoredObjectVersion, error) {
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

	if ver == nil {
		return nil, ErrNoSuchKey
	}

	if ver.Deleted {
		// GET of a delete marker: AWS returns 405 for a versioned request (with
		// x-amz-delete-marker + Allow: DELETE) and 404 for the latest version
		// (with x-amz-delete-marker). The handler sets the headers.
		if versionID != nil && *versionID != "" {
			return nil, ErrDeleteMarker
		}

		return nil, ErrLatestDeleteMarker
	}

	return ver, nil
}

// decryptVersionForGet reverses SSE envelope encryption for a GET. It returns
// the (possibly decrypted) data and a skipDecompress flag indicating the blob
// must be returned as-is (SSE-C version with no key supplied — the handler will
// reject the request before the body is read).
func decryptVersionForGet(
	ctx context.Context,
	ver *StoredObjectVersion,
	data []byte,
) ([]byte, bool, error) {
	sseAlg := ver.SSEAlgorithm
	sseCAlg := ver.SSECAlgorithm
	if sseAlg == "" && sseCAlg == "" {
		return data, false, nil
	}

	// For SSE-C the customer must re-supply the key on GET via the request —
	// read it from context (set by getObject handler) before decrypting. If no
	// key is supplied for an SSE-C version, skip decrypt and let the handler's
	// validateSSECOnRead surface the proper 400 ErrSSECRequired.
	sseFromCtx, _ := ctx.Value(sseKey).(sseInfo)
	if sseCAlg != "" && sseFromCtx.SSECKeyB64 == "" {
		return data, true, nil
	}

	decrypted, decErr := decryptWithSSE(
		data,
		sseAlg,
		sseCAlg,
		ver.EncryptionDEK,
		ver.EncryptionNonce,
		sseFromCtx.SSECKeyB64,
	)
	if decErr != nil {
		return nil, false, decErr
	}

	return decrypted, false, nil
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
	sc := ver.StorageClass
	if sc == "" {
		sc = storageStandard
	}

	return &s3.GetObjectOutput{
		Body:                 io.NopCloser(bytes.NewReader(data)),
		ContentLength:        aws.Int64(size),
		ContentType:          aws.String(ver.ContentType),
		ContentEncoding:      ptrconv.NilIfEmpty(ver.ContentEncoding),
		ContentDisposition:   ptrconv.NilIfEmpty(ver.ContentDisposition),
		ETag:                 aws.String(ver.ETag),
		LastModified:         aws.Time(ver.LastModified),
		Metadata:             metadata,
		VersionId:            aws.String(versionIDStr),
		StorageClass:         types.StorageClass(sc),
		ChecksumCRC32:        ver.ChecksumCRC32,
		ChecksumCRC32C:       ver.ChecksumCRC32C,
		ChecksumSHA1:         ver.ChecksumSHA1,
		ChecksumSHA256:       ver.ChecksumSHA256,
		ChecksumCRC64NVME:    ver.ChecksumCRC64NVME,
		ServerSideEncryption: types.ServerSideEncryption(ver.SSEAlgorithm),
		SSEKMSKeyId:          ptrconv.NilIfEmpty(ver.SSEKMSKeyID),
		SSECustomerAlgorithm: ptrconv.NilIfEmpty(ver.SSECAlgorithm),
		SSECustomerKeyMD5:    ptrconv.NilIfEmpty(ver.SSECKeyMD5),
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

	// If no version specified and latest is a delete marker, return 404 with the
	// x-amz-delete-marker header (set by the handler).
	if ver.Deleted {
		return nil, ErrLatestDeleteMarker
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
		ContentEncoding:      ptrconv.NilIfEmpty(ver.ContentEncoding),
		ContentDisposition:   ptrconv.NilIfEmpty(ver.ContentDisposition),
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
		SSEKMSKeyId:          ptrconv.NilIfEmpty(ver.SSEKMSKeyID),
		SSECustomerAlgorithm: ptrconv.NilIfEmpty(ver.SSECAlgorithm),
		SSECustomerKeyMD5:    ptrconv.NilIfEmpty(ver.SSECKeyMD5),
	}, nil
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
