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
		UploadID:  uploadID,
		Bucket:    bucketName,
		Key:       key,
		Parts:     make(map[int32]*StoredPart),
		Initiated: time.Now().UTC(),
		Tagging:   tagging,
		SSE:       sse,
		mu:        lockmetrics.New("s3.upload"),
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
	var upload *StoredMultipartUpload
	func() {
		b.mu.RLock(opCompleteMultipartUpload)
		defer b.mu.RUnlock()

		upload = b.getUpload(bucketName, uploadID)
	}()

	if upload == nil {
		return nil, ErrNoSuchUpload
	}

	// Snapshot the upload's tagging + SSE before claiming (the upload is
	// removed from the index during claim, so we must capture them first).
	var tagging string
	var sse sseInfo
	func() {
		upload.mu.RLock("CompleteMultipartUpload.tagging")
		defer upload.mu.RUnlock()

		tagging = upload.Tagging
		sse = upload.SSE
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
	isCompressed   bool
}

// collectPartsData gathers raw data and part MD5 bytes under upload.mu.RLock.
// Returns the combined buffer and MD5-concatenation used for multipart ETag.
// Must be called without upload.mu held; acquires and releases it internally.
func (b *InMemoryBackend) collectPartsData(
	upload *StoredMultipartUpload,
	parts []types.CompletedPart,
) ([]byte, []byte, error) {
	var data, partMD5s []byte
	var err error

	func() {
		upload.mu.RLock(opCompleteMultipartUpload)
		defer upload.mu.RUnlock()

		// Validate ascending order.
		for i := 1; i < len(parts); i++ {
			if *parts[i].PartNumber <= *parts[i-1].PartNumber {
				err = ErrInvalidPartOrder

				return
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
		md5s := make([]byte, 0, len(parts)*md5.Size)

		for i, part := range parts {
			pNum := *part.PartNumber
			storedPart, ok := upload.Parts[pNum]
			if !ok {
				err = ErrInvalidPart

				return
			}

			if *part.ETag != storedPart.ETag {
				err = ErrInvalidPart

				return
			}

			isLastPart := i == len(parts)-1
			if !isLastPart && storedPart.Size < multipartMinPartSize && !b.skipMultipartSizeCheck {
				err = ErrEntityTooSmall

				return
			}

			buf.Write(storedPart.Data)

			rawETag := strings.Trim(storedPart.ETag, "\"")
			rawBytes, decErr := hex.DecodeString(rawETag)
			if decErr != nil {
				err = ErrInvalidPart

				return
			}
			md5s = append(md5s, rawBytes...)
		}

		data = buf.Bytes()
		partMD5s = md5s
	}()

	if err != nil {
		return nil, nil, err
	}

	return data, partMD5s, nil
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
			LastModified:    time.Now(),
			IsLatest:        true,
			SSEAlgorithm:    sse.Algorithm,
			SSEKMSKeyID:     sse.KMSKeyID,
			SSECAlgorithm:   sse.SSECAlgorithm,
			SSECKeyMD5:      sse.SSECKeyMD5,
			EncryptionDEK:   dek,
			EncryptionNonce: nonce,
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
	)

	uploads, commonPrefixes := groupUploadsByDelimiter(uploads, prefix, delimiter)

	isTruncated, nextKeyMarker, nextUploadIDMarker := truncateUploads(&uploads, maxUploads)

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
func groupUploadsByDelimiter(
	uploads []types.MultipartUpload,
	prefix, delimiter string,
) ([]types.MultipartUpload, []types.CommonPrefix) {
	if delimiter == "" {
		return uploads, nil
	}
	var filtered []types.MultipartUpload
	var commonPrefixes []types.CommonPrefix
	seen := make(map[string]struct{})
	for _, u := range uploads {
		key := aws.ToString(u.Key)
		keyAfterPrefix := strings.TrimPrefix(key, prefix)
		if idx := strings.Index(keyAfterPrefix, delimiter); idx >= 0 {
			cp := prefix + keyAfterPrefix[:idx+len(delimiter)]
			if _, ok := seen[cp]; !ok {
				seen[cp] = struct{}{}
				commonPrefixes = append(commonPrefixes, types.CommonPrefix{Prefix: aws.String(cp)})
			}
		} else {
			filtered = append(filtered, u)
		}
	}

	return filtered, commonPrefixes
}

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
				PartNumber: aws.Int32(pn),
				ETag:       aws.String(p.ETag),
				Size:       aws.Int64(p.Size),
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
