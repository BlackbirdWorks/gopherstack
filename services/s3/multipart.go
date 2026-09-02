package s3

import (
	"bytes"
	"context"
	"crypto/md5"  //nolint:gosec // MD5 required for S3 ETag compatibility
	"crypto/sha1" //nolint:gosec // SHA1 required for S3 checksum compatibility
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// multipartMinPartSize is the AWS minimum non-last part size for multipart uploads.
const multipartMinPartSize = 5 * 1024 * 1024

// getUpload returns the multipart upload for uploadID, but only if it belongs
// to bucketName — mirroring the old b.uploads[bucketName][uploadID] nested-map
// lookup, which returned nil both when the uploadID was absent and when it
// existed but under a different bucket. The caller must hold at least
// b.mu.RLock.
func (b *InMemoryBackend) getUpload(bucketName, uploadID string) *StoredMultipartUpload {
	upload, ok := b.uploads.Get(uploadID)
	if !ok || upload.Bucket != bucketName {
		return nil
	}

	return upload
}

func (b *InMemoryBackend) CreateMultipartUpload(
	ctx context.Context,
	input *s3.CreateMultipartUploadInput,
) (*s3.CreateMultipartUploadOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key

	var bucket *StoredBucket
	var err error
	func() {
		b.mu.RLock("CreateMultipartUpload")
		defer b.mu.RUnlock()

		bucket, err = b.getBucket(bucketName)
	}()

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

	// Apply default bucket encryption if no SSE is specified in the request
	if sse.Algorithm == "" && sse.SSECAlgorithm == "" && bucket.EncryptionConfig != "" {
		var config ServerSideEncryptionConfiguration
		if xmlErr := xml.Unmarshal([]byte(bucket.EncryptionConfig), &config); xmlErr == nil && len(config.Rules) > 0 {
			def := config.Rules[0].ApplyServerSideEncryptionByDefault
			if def.SSEAlgorithm != "" {
				sse.Algorithm = def.SSEAlgorithm
				sse.KMSKeyID = def.KMSMasterKeyID
			}
		}
	}

	b.mu.Lock("CreateMultipartUpload")
	defer b.mu.Unlock()

	b.uploads.Put(&StoredMultipartUpload{
		UploadID:     uploadID,
		Bucket:       bucketName,
		Key:          key,
		Parts:        make(map[int32]*StoredPart),
		Initiated:    time.Now().UTC(),
		Tagging:      tagging,
		SSE:          sse,
		StorageClass: string(input.StorageClass),
		mu:           lockmetrics.New("s3.upload"),
	})

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
	md5Hasher := httputils.GetMD5()
	defer httputils.PutMD5(md5Hasher)

	buf := httputils.GetBuffer()
	defer httputils.PutBuffer(buf)

	writers := []io.Writer{md5Hasher, buf}

	algo := inferChecksumAlgo(input)
	s3Hasher := newS3Hasher(algo)
	if s3Hasher != nil {
		writers = append(writers, s3Hasher)
	}

	tr := io.TeeReader(input.Body, io.MultiWriter(writers...))
	originalSize, err := io.Copy(io.Discard, tr)
	if err != nil {
		return nil, err
	}

	storedData := bytes.Clone(buf.Bytes())
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

	// 3. Store the part, including the checksum values just verified above --
	// ListParts (real Part.ChecksumCRC32/-CRC32C/-SHA1/-SHA256, api_op_ListParts.go)
	// otherwise has no way to report them, since they exist only on this
	// call's request/response and were never persisted onto the part before.
	if sErr := b.storePart(bucketName, uploadID, partNumber, &StoredPart{
		PartNumber:        partNumber,
		Data:              storedData,
		ETag:              quotedETag,
		Size:              originalSize,
		ChecksumCRC32:     input.ChecksumCRC32,
		ChecksumCRC32C:    input.ChecksumCRC32C,
		ChecksumCRC64NVME: input.ChecksumCRC64NVME,
		ChecksumSHA1:      input.ChecksumSHA1,
		ChecksumSHA256:    input.ChecksumSHA256,
	}); sErr != nil {
		return nil, sErr
	}

	return &s3.UploadPartOutput{
		ETag:              aws.String(quotedETag),
		ChecksumCRC32:     input.ChecksumCRC32,
		ChecksumCRC32C:    input.ChecksumCRC32C,
		ChecksumCRC64NVME: input.ChecksumCRC64NVME,
		ChecksumSHA1:      input.ChecksumSHA1,
		ChecksumSHA256:    input.ChecksumSHA256,
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
	var upload *StoredMultipartUpload
	func() {
		b.mu.RLock(opCompleteMultipartUpload)
		defer b.mu.RUnlock()

		upload = b.getUpload(bucketName, uploadID)
	}()

	if upload == nil {
		return nil, ErrNoSuchUpload
	}

	// Snapshot the upload's tagging + SSE + storage class before claiming (the
	// upload is removed from the index during claim, so we must capture them
	// first).
	var tagging string
	var sse sseInfo
	var storageClass string
	func() {
		upload.mu.RLock("CompleteMultipartUpload.tagging")
		defer upload.mu.RUnlock()

		tagging = upload.Tagging
		sse = upload.SSE
		storageClass = upload.StorageClass
	}()

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
	var bucket *StoredBucket
	func() {
		b.mu.RLock(opCompleteMultipartUpload)
		defer b.mu.RUnlock()

		bucket, err = b.getBucket(bucketName)
	}()

	if err != nil {
		return nil, err
	}

	versionID, err := b.commitMultipartObject(bucket, bucketName, key, assembled, tagging, sse, storageClass)
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
	var err error

	func() {
		b.mu.Lock("CompleteMultipartUpload.claim")
		defer b.mu.Unlock()

		upload := b.getUpload(bucketName, uploadID)
		if upload == nil {
			err = ErrNoSuchUpload

			return
		}

		// Mark closed while holding b.mu to block concurrent UploadPart calls that
		// already hold a pointer to this upload struct.
		func() {
			upload.mu.Lock("CompleteMultipartUpload.claim")
			defer upload.mu.Unlock()

			upload.closed = true
		}()

		b.uploads.Delete(uploadID)
	}()

	return err
}

// multipartAssemblyResult holds the results of assembleMultipartData.
type multipartAssemblyResult struct {
	etag           string
	data           []byte
	compressedData []byte
	parts          []StoredObjectPart
	isCompressed   bool
}

// collectPartsData gathers raw data and part MD5 bytes under upload.mu.RLock.
// Returns the combined buffer and MD5-concatenation used for multipart ETag.
// Must be called without upload.mu held; acquires and releases it internally.
func (b *InMemoryBackend) collectPartsData(
	upload *StoredMultipartUpload,
	parts []types.CompletedPart,
) ([]byte, []byte, []StoredObjectPart, error) {
	upload.mu.RLock(opCompleteMultipartUpload)
	defer upload.mu.RUnlock()

	return b.collectPartsDataLocked(upload, parts)
}

// collectPartsDataLocked does the actual work of collectPartsData under
// upload.mu.RLock. Extracted so the locked region is a plain method body
// rather than a function literal, and so per-part validation can be delegated
// to validateAndAppendPart to keep cognitive complexity down.
func (b *InMemoryBackend) collectPartsDataLocked(
	upload *StoredMultipartUpload,
	parts []types.CompletedPart,
) ([]byte, []byte, []StoredObjectPart, error) {
	// Validate ascending order.
	for i := 1; i < len(parts); i++ {
		if *parts[i].PartNumber <= *parts[i-1].PartNumber {
			return nil, nil, nil, ErrInvalidPartOrder
		}
	}

	// Pre-calculate total size.
	totalSize := 0
	for _, part := range parts {
		if sp, ok := upload.Parts[*part.PartNumber]; ok {
			totalSize += len(sp.Data)
		}
	}

	data := make([]byte, totalSize)
	offset := 0
	md5s := make([]byte, 0, len(parts)*md5.Size)
	partsMeta := make([]StoredObjectPart, 0, len(parts))

	for i, part := range parts {
		rawBytes, spMeta, partBytes, err := b.validateAndExtractPart(upload, part, i == len(parts)-1)
		if err != nil {
			return nil, nil, nil, err
		}

		copy(data[offset:], partBytes)
		offset += len(partBytes)
		md5s = append(md5s, rawBytes...)
		partsMeta = append(partsMeta, spMeta)
	}

	return data, md5s, partsMeta, nil
}

// validateAndExtractPart validates a single completed part against its stored
// counterpart, and returns the raw MD5 bytes decoded from its ETag, metadata, and raw data bytes.
func (b *InMemoryBackend) validateAndExtractPart(
	upload *StoredMultipartUpload,
	part types.CompletedPart,
	isLastPart bool,
) ([]byte, StoredObjectPart, []byte, error) {
	pNum := *part.PartNumber
	storedPart, ok := upload.Parts[pNum]
	if !ok {
		return nil, StoredObjectPart{}, nil, ErrInvalidPart
	}

	if *part.ETag != storedPart.ETag {
		return nil, StoredObjectPart{}, nil, ErrInvalidPart
	}

	if !isLastPart && storedPart.Size < multipartMinPartSize && !b.skipMultipartSizeCheck {
		return nil, StoredObjectPart{}, nil, ErrEntityTooSmall
	}

	rawETag := strings.Trim(storedPart.ETag, "\"")

	rawBytes, err := hex.DecodeString(rawETag)
	if err != nil {
		return nil, StoredObjectPart{}, nil, ErrInvalidPart
	}

	meta := StoredObjectPart{
		PartNumber:        pNum,
		Size:              storedPart.Size,
		ChecksumCRC32:     storedPart.ChecksumCRC32,
		ChecksumCRC32C:    storedPart.ChecksumCRC32C,
		ChecksumCRC64NVME: storedPart.ChecksumCRC64NVME,
		ChecksumSHA1:      storedPart.ChecksumSHA1,
		ChecksumSHA256:    storedPart.ChecksumSHA256,
	}

	return rawBytes, meta, storedPart.Data, nil
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
	// AWS rejects CompleteMultipartUpload with an empty (or absent) parts list:
	// the request must enumerate at least one previously-uploaded part.
	if input.MultipartUpload == nil || len(input.MultipartUpload.Parts) == 0 {
		return multipartAssemblyResult{}, ErrEmptyParts
	}

	parts := input.MultipartUpload.Parts

	data, partMD5s, partsMeta, err := b.collectPartsData(upload, parts)
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
		parts:          partsMeta,
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
	storageClass string,
) (string, error) {
	var obj *StoredObject
	var newVersion *StoredObjectVersion
	var versionID string
	var err error

	func() {
		bucket.mu.Lock(opCompleteMultipartUpload)
		defer bucket.mu.Unlock()

		var exists bool
		obj, exists = bucket.Objects[key]
		if !exists {
			obj = &StoredObject{
				Key:      key,
				Versions: make(map[string]*StoredObjectVersion),
				mu:       lockmetrics.New("s3.object"),
			}
			bucket.Objects[key] = obj
		}

		versionID = NullVersion
		if bucket.Versioning == types.BucketVersioningStatusEnabled {
			versionID = newObjectVersionID()
		}

		// Seal the assembled body under SSE if the session was created with it.
		// On encryption failure we surface the error to the caller so it can be
		// returned as a structured S3 error response rather than panicking the
		// request goroutine.
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
				err = fmt.Errorf("commitMultipartObject: SSE encryption failed: %w", encErr)

				return
			}
		}

		newVersion = &StoredObjectVersion{
			VersionID:       versionID,
			Key:             key,
			Data:            storedBody,
			IsCompressed:    assembled.isCompressed,
			Size:            int64(len(assembled.data)),
			ETag:            assembled.etag,
			Parts:           assembled.parts,
			LastModified:    time.Now(),
			IsLatest:        true,
			SSEAlgorithm:    sse.Algorithm,
			SSEKMSKeyID:     sse.KMSKeyID,
			SSECAlgorithm:   sse.SSECAlgorithm,
			SSECKeyMD5:      sse.SSECKeyMD5,
			EncryptionDEK:   dek,
			EncryptionNonce: nonce,
			StorageClass:    storageClass,
		}

		// Acquire obj.mu while bucket.mu is still held (the defer above releases
		// bucket.mu as soon as this closure returns, i.e. right after obj.mu is
		// acquired) to prevent TOCTOU and to serialize version-map mutations
		// with concurrent readers (obj.mu.RLock). obj.mu itself is released by
		// the follow-up closure below once the version-map mutation is done.
		obj.mu.Lock(opCompleteMultipartUpload)
	}()

	if err != nil {
		return "", err
	}

	func() {
		defer obj.mu.Unlock()

		for _, v := range obj.Versions {
			v.IsLatest = false
		}
		obj.Versions[versionID] = newVersion
		obj.LatestVersionID = versionID
	}()

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

	var err error
	func() {
		b.mu.Lock("AbortMultipartUpload")
		defer b.mu.Unlock()

		upload := b.getUpload(bucketName, uploadID)
		if upload == nil {
			err = ErrNoSuchUpload

			return
		}

		// Mark closed while holding b.mu so concurrent UploadPart calls that already
		// hold a pointer to this upload will observe the invalidation flag.
		func() {
			upload.mu.Lock("AbortMultipartUpload")
			defer upload.mu.Unlock()

			upload.closed = true
		}()

		b.uploads.Delete(uploadID)
	}()

	if err != nil {
		return nil, err
	}

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

	prefix := aws.ToString(input.Prefix)
	delimiter := aws.ToString(input.Delimiter)

	uploads := b.collectAndSortUploads(bucketName, prefix)
	uploads = seekMultipartMarker(
		uploads,
		aws.ToString(input.KeyMarker),
		aws.ToString(input.UploadIdMarker),
		delimiter,
	)

	entries := groupUploadsByDelimiter(uploads, prefix, delimiter)

	uploads, commonPrefixes, isTruncated, nextKeyMarker, nextUploadIDMarker := truncateUploads(entries, maxUploads)

	return &s3.ListMultipartUploadsOutput{
		Bucket:             aws.String(bucketName),
		Uploads:            uploads,
		CommonPrefixes:     commonPrefixes,
		MaxUploads:         aws.Int32(maxUploads),
		IsTruncated:        aws.Bool(isTruncated),
		NextKeyMarker:      aws.String(nextKeyMarker),
		NextUploadIdMarker: aws.String(nextUploadIDMarker),
	}, nil
}

// collectAndSortUploads snapshots and sorts the in-progress uploads for a bucket.
func (b *InMemoryBackend) collectAndSortUploads(bucketName, prefix string) []types.MultipartUpload {
	var uploads []types.MultipartUpload

	for _, u := range b.uploadsByBucket.Get(bucketName) {
		if prefix != "" && !strings.HasPrefix(u.Key, prefix) {
			continue
		}

		sc := u.StorageClass
		if sc == "" {
			sc = storageStandard
		}

		uploads = append(uploads, types.MultipartUpload{
			Key:          aws.String(u.Key),
			UploadId:     aws.String(u.UploadID),
			Initiated:    aws.Time(u.Initiated),
			StorageClass: types.StorageClass(sc),
			Owner: &types.Owner{
				ID:          aws.String(gopherstackName),
				DisplayName: aws.String(gopherstackName),
			},
			Initiator: &types.Initiator{
				ID:          aws.String(gopherstackName),
				DisplayName: aws.String(gopherstackName),
			},
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
// (keyMarker, uploadIDMarker) pagination cursor. When keyMarker is itself a
// CommonPrefix boundary (ends with delimiter -- every CommonPrefix
// groupUploadsByDelimiter emits does, by construction), every upload whose
// key falls under that prefix must also be skipped: it was already
// summarized and returned as that one CommonPrefix entry. A plain
// `k > keyMarker` alone would resume inside that same prefix's key range
// and re-emit the CommonPrefix on the next page.
func seekMultipartMarker(
	uploads []types.MultipartUpload,
	keyMarker, uploadIDMarker, delimiter string,
) []types.MultipartUpload {
	if keyMarker == "" {
		return uploads
	}

	skipWholePrefix := delimiter != "" && strings.HasSuffix(keyMarker, delimiter)

	for i, u := range uploads {
		k := aws.ToString(u.Key)

		if skipWholePrefix {
			if k > keyMarker && !strings.HasPrefix(k, keyMarker) {
				return uploads[i:]
			}

			continue
		}

		if k > keyMarker {
			return uploads[i:]
		}

		if k == keyMarker && uploadIDMarker != "" && aws.ToString(u.UploadId) == uploadIDMarker {
			return uploads[i+1:]
		}
	}

	return nil
}

// uploadListEntry is one lexicographically-ordered slot in a delimited
// ListMultipartUploads listing: either one upload or one common-prefix
// group, never both. A single ordered sequence (rather than two
// separately-truncated lists) is what lets truncateUploads cut the page and
// compute the next-page markers in true key order -- see listObjectEntry
// (services/s3/listing.go) for the general shape of the bug this avoids.
type uploadListEntry struct {
	upload *types.MultipartUpload
	prefix string
}

// groupUploadsByDelimiter groups uploads that share a common prefix (when
// delimiter is set) into ordered uploadListEntry values, preserving the
// input's sorted order.
func groupUploadsByDelimiter(uploads []types.MultipartUpload, prefix, delimiter string) []uploadListEntry {
	entries := make([]uploadListEntry, 0, len(uploads))

	if delimiter == "" {
		for i := range uploads {
			entries = append(entries, uploadListEntry{upload: &uploads[i]})
		}

		return entries
	}

	var lastCP string
	haveCP := false

	for i := range uploads {
		u := &uploads[i]
		key := aws.ToString(u.Key)
		keyAfterPrefix := strings.TrimPrefix(key, prefix)

		if idx := strings.Index(keyAfterPrefix, delimiter); idx >= 0 {
			cp := prefix + keyAfterPrefix[:idx+len(delimiter)]
			if !haveCP || cp != lastCP {
				lastCP = cp
				haveCP = true
				entries = append(entries, uploadListEntry{prefix: cp})
			}

			continue
		}

		entries = append(entries, uploadListEntry{upload: u})
	}

	return entries
}

// truncateUploads cuts entries (already in true lexicographic key order) at
// maxUploads, splitting the retained prefix back into the Uploads/
// CommonPrefixes wire lists and deriving NextKeyMarker/NextUploadIdMarker
// from the last entry actually included, whichever kind it is.
//
// The predecessor of this function truncated only the flat-upload list and
// derived NextKeyMarker/NextUploadIdMarker from `uploads[maxUploads]` -- the
// first upload NOT returned, i.e. a token naming the first item of the next
// page -- while seekMultipartMarker's decoder resumes after the item that
// matches the marker. Naming the next page's first item and then skipping
// past whatever matches it drops that exact upload on every truncation
// boundary (Class D) -- no delimiter, deletion, or tampering needed, a
// plain walk over more uploads than one page holds triggers it every time.
// It also never truncated or counted CommonPrefixes toward maxUploads,
// which is the delimiter-listing sibling of the same "cut two lists
// independently" bug fixed in services/s3/listing.go.
func truncateUploads(entries []uploadListEntry, maxUploads int32) (
	[]types.MultipartUpload, []types.CommonPrefix, bool, string, string,
) {
	isTruncated := int64(len(entries)) > int64(maxUploads)
	page := entries

	var nextKeyMarker, nextUploadIDMarker string

	if isTruncated {
		page = entries[:maxUploads]

		last := page[len(page)-1]
		if last.upload != nil {
			nextKeyMarker = aws.ToString(last.upload.Key)
			nextUploadIDMarker = aws.ToString(last.upload.UploadId)
		} else {
			nextKeyMarker = last.prefix
		}
	}

	uploads := make([]types.MultipartUpload, 0, len(page))
	var commonPrefixes []types.CommonPrefix

	for _, e := range page {
		if e.upload != nil {
			uploads = append(uploads, *e.upload)
		} else {
			commonPrefixes = append(commonPrefixes, types.CommonPrefix{Prefix: aws.String(e.prefix)})
		}
	}

	return uploads, commonPrefixes, isTruncated, nextKeyMarker, nextUploadIDMarker
}

// ListParts returns the parts that have been uploaded for a specific multipart upload.
const listPartsDefaultMax = int32(1000)

func (b *InMemoryBackend) ListParts(
	_ context.Context,
	input *s3.ListPartsInput,
) (*s3.ListPartsOutput, error) {
	uploadID := aws.ToString(input.UploadId)
	bucketName := aws.ToString(input.Bucket)

	var upload *StoredMultipartUpload
	func() {
		b.mu.RLock("ListParts")
		defer b.mu.RUnlock()

		upload = b.getUpload(bucketName, uploadID)
	}()

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

	var parts []types.Part
	var totalPartNumbers int
	func() {
		upload.mu.RLock("ListParts")
		defer upload.mu.RUnlock()

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
		totalPartNumbers = len(partNumbers)

		for _, pn := range partNumbers {
			if int32(len(parts)) >= maxParts { //nolint:gosec // safe: len bounded by slice
				break
			}
			p := upload.Parts[pn]
			parts = append(parts, types.Part{
				PartNumber:        aws.Int32(pn),
				ETag:              aws.String(p.ETag),
				Size:              aws.Int64(p.Size),
				ChecksumCRC32:     p.ChecksumCRC32,
				ChecksumCRC32C:    p.ChecksumCRC32C,
				ChecksumCRC64NVME: p.ChecksumCRC64NVME,
				ChecksumSHA1:      p.ChecksumSHA1,
				ChecksumSHA256:    p.ChecksumSHA256,
			})
		}
	}()

	partsCount := int32(len(parts)) //nolint:gosec // G115: bounded by maxParts
	isTruncated := partsCount == maxParts && int(partsCount) < totalPartNumbers
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

// storePart saves a multipart upload part under the per-upload lock.
func (b *InMemoryBackend) storePart(
	bucketName, uploadID string,
	partNumber int32,
	part *StoredPart,
) error {
	b.mu.RLock("storePart")
	upload := b.getUpload(bucketName, uploadID)
	b.mu.RUnlock()

	if upload == nil {
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

func inferChecksumAlgo(input *s3.UploadPartInput) string {
	algo := string(input.ChecksumAlgorithm)
	if algo != "" {
		return algo
	}

	switch {
	case input.ChecksumCRC32 != nil:
		return ChecksumCRC32
	case input.ChecksumCRC32C != nil:
		return ChecksumCRC32C
	case input.ChecksumCRC64NVME != nil:
		return ChecksumCRC64NVME
	case input.ChecksumSHA1 != nil:
		return ChecksumSHA1
	case input.ChecksumSHA256 != nil:
		return ChecksumSHA256
	default:
		return ""
	}
}

func newS3Hasher(algo string) hash.Hash {
	switch strings.ToUpper(algo) {
	case ChecksumCRC32:
		return crc32.NewIEEE()
	case ChecksumCRC32C:
		return NewCRC32C()
	case ChecksumCRC64NVME:
		return NewCRC64NVME()
	case ChecksumSHA1:
		//nolint:gosec // SHA1 supported
		return sha1.New()
	case ChecksumSHA256:
		return sha256.New()
	default:
		return nil
	}
}
