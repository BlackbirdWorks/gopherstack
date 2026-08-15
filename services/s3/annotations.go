package s3

import (
	"bytes"
	"context"
	"hash"
	"io"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// validateAnnotationName enforces PutObjectAnnotationInput.AnnotationName's
// documented constraints (s3@v1.106.5 api_op_PutObjectAnnotation.go:
// "Minimum length of 1. Maximum length of 512 bytes.") plus the reserved-prefix
// rule from api_op_DeleteObjectAnnotation.go's doc comment ("Annotation names
// ... cannot start with aws or s3 (case-insensitive)").
func validateAnnotationName(name string) error {
	if name == "" {
		return ErrInvalidAnnotationName
	}
	if len(name) > maxAnnotationNameBytes {
		return ErrAnnotationNameTooLong
	}

	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "aws") || strings.HasPrefix(lower, "s3") {
		return ErrInvalidAnnotationName
	}

	return nil
}

// getObjectForAnnotation resolves the bucket and object shared by all four
// object-level annotation operations.
func (b *InMemoryBackend) getObjectForAnnotation(bucketName, key string) (*StoredObject, error) {
	b.mu.RLock("getObjectForAnnotation")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("getObjectForAnnotation")
	obj, exists := bucket.Objects[key]
	bucket.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchKey
	}

	return obj, nil
}

// PutObjectAnnotation attaches a named annotation to an object version.
func (b *InMemoryBackend) PutObjectAnnotation(
	ctx context.Context,
	input *s3.PutObjectAnnotationInput,
) (*s3.PutObjectAnnotationOutput, error) {
	name := aws.ToString(input.AnnotationName)
	if err := validateAnnotationName(name); err != nil {
		return nil, err
	}

	obj, err := b.getObjectForAnnotation(aws.ToString(input.Bucket), aws.ToString(input.Key))
	if err != nil {
		return nil, err
	}

	obj.mu.Lock("PutObjectAnnotation")
	defer obj.mu.Unlock()

	ver, err := resolveObjectVersion(obj, input.VersionId)
	if err != nil {
		return nil, err
	}

	// "Objects encrypted with SSE-C cannot have annotations" (s3@v1.106.5
	// api_op_PutObjectAnnotation.go doc comment).
	if ver.SSECAlgorithm != "" {
		return nil, ErrAnnotationSSECNotSupported
	}

	return b.putAnnotation(ctx, ver, name, input)
}

// putAnnotation computes the payload hashes, applies the per-object cap, and
// stores the annotation. Split out of PutObjectAnnotation to keep that
// function's cyclomatic complexity low.
func (b *InMemoryBackend) putAnnotation(
	ctx context.Context,
	ver *StoredObjectVersion,
	name string,
	input *s3.PutObjectAnnotationInput,
) (*s3.PutObjectAnnotationOutput, error) {
	_, data, etagHex, s3Hasher, err := b.computeObjectHashes(ctx, input.AnnotationPayload, input.ChecksumAlgorithm)
	if err != nil {
		return nil, err
	}

	if !utf8.Valid(data) {
		return nil, ErrAnnotationUnsupportedMediaType
	}

	computedChecksumB64, err := finalizeAnnotationChecksum(s3Hasher, input)
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
	checksums.populateComputed(computedChecksumB64, strings.ToUpper(string(input.ChecksumAlgorithm)))

	if ver.Annotations == nil {
		ver.Annotations = make(map[string]*StoredAnnotation)
	}
	if _, exists := ver.Annotations[name]; !exists && len(ver.Annotations) >= maxAnnotationsPerObject {
		return nil, ErrAnnotationLimitExceeded
	}

	ann := &StoredAnnotation{
		Name:              name,
		Payload:           data,
		ETag:              "\"" + etagHex + "\"",
		LastModified:      time.Now().UTC(),
		ChecksumAlgorithm: input.ChecksumAlgorithm,
		ChecksumCRC32:     checksums.crc32,
		ChecksumCRC32C:    checksums.crc32c,
		ChecksumSHA1:      checksums.sha1,
		ChecksumSHA256:    checksums.sha256,
		ChecksumCRC64NVME: checksums.crc64nvme,
	}
	ver.Annotations[name] = ann

	return &s3.PutObjectAnnotationOutput{
		AnnotationName:    aws.String(name),
		ETag:              aws.String(ann.ETag),
		Key:               aws.String(ver.Key),
		ObjectVersionId:   aws.String(ver.VersionID),
		ChecksumCRC32:     checksums.crc32,
		ChecksumCRC32C:    checksums.crc32c,
		ChecksumSHA1:      checksums.sha1,
		ChecksumSHA256:    checksums.sha256,
		ChecksumCRC64NVME: checksums.crc64nvme,
	}, nil
}

// finalizeAnnotationChecksum mirrors objects.go's finalizeChecksum for
// PutObjectAnnotationInput: validates a client-supplied checksum against the
// computed one, and returns the computed value for server-side population.
func finalizeAnnotationChecksum(s3Hasher hash.Hash, input *s3.PutObjectAnnotationInput) (string, error) {
	if s3Hasher == nil {
		return "", nil
	}

	computedChecksumB64 := checksumBytesToB64(s3Hasher)

	var supplied *string

	switch strings.ToUpper(string(input.ChecksumAlgorithm)) {
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

// GetObjectAnnotation retrieves a single named annotation from an object version.
func (b *InMemoryBackend) GetObjectAnnotation(
	_ context.Context,
	input *s3.GetObjectAnnotationInput,
) (*s3.GetObjectAnnotationOutput, error) {
	obj, err := b.getObjectForAnnotation(aws.ToString(input.Bucket), aws.ToString(input.Key))
	if err != nil {
		return nil, err
	}

	obj.mu.RLock("GetObjectAnnotation")
	defer obj.mu.RUnlock()

	ver, err := resolveObjectVersion(obj, input.VersionId)
	if err != nil {
		return nil, err
	}

	ann, ok := ver.Annotations[aws.ToString(input.AnnotationName)]
	if !ok {
		return nil, ErrNoSuchAnnotation
	}

	return &s3.GetObjectAnnotationOutput{
		AnnotationPayload: io.NopCloser(bytes.NewReader(ann.Payload)),
		ContentLength:     aws.Int64(int64(len(ann.Payload))),
		ETag:              aws.String(ann.ETag),
		LastModified:      aws.Time(ann.LastModified),
		ObjectVersionId:   aws.String(ver.VersionID),
		ChecksumCRC32:     ann.ChecksumCRC32,
		ChecksumCRC32C:    ann.ChecksumCRC32C,
		ChecksumSHA1:      ann.ChecksumSHA1,
		ChecksumSHA256:    ann.ChecksumSHA256,
		ChecksumCRC64NVME: ann.ChecksumCRC64NVME,
	}, nil
}

// DeleteObjectAnnotation removes a named annotation from an object version.
// Deleting a name that doesn't exist is not an error: DeleteObjectAnnotation's
// error switch (s3@v1.106.5 deserializers.go) declares only NoSuchBucket and
// NoSuchKey -- no NoSuchAnnotation -- matching real S3's idempotent-delete
// semantics for DeleteObject itself.
func (b *InMemoryBackend) DeleteObjectAnnotation(
	_ context.Context,
	input *s3.DeleteObjectAnnotationInput,
) (*s3.DeleteObjectAnnotationOutput, error) {
	obj, err := b.getObjectForAnnotation(aws.ToString(input.Bucket), aws.ToString(input.Key))
	if err != nil {
		return nil, err
	}

	obj.mu.Lock("DeleteObjectAnnotation")
	defer obj.mu.Unlock()

	ver, err := resolveObjectVersion(obj, input.VersionId)
	if err != nil {
		return nil, err
	}

	delete(ver.Annotations, aws.ToString(input.AnnotationName))

	return &s3.DeleteObjectAnnotationOutput{
		ObjectVersionId: aws.String(ver.VersionID),
	}, nil
}

// ListObjectAnnotations lists the annotations attached to an object version,
// optionally filtered by name prefix and paginated via ContinuationToken.
func (b *InMemoryBackend) ListObjectAnnotations(
	_ context.Context,
	input *s3.ListObjectAnnotationsInput,
) (*s3.ListObjectAnnotationsOutput, error) {
	obj, err := b.getObjectForAnnotation(aws.ToString(input.Bucket), aws.ToString(input.Key))
	if err != nil {
		return nil, err
	}

	obj.mu.RLock("ListObjectAnnotations")
	defer obj.mu.RUnlock()

	ver, err := resolveObjectVersion(obj, input.VersionId)
	if err != nil {
		return nil, err
	}

	prefix := aws.ToString(input.AnnotationPrefix)
	names := matchingAnnotationNames(ver.Annotations, prefix)

	maxResults := int(aws.ToInt32(input.MaxAnnotationResults))
	if maxResults <= 0 || maxResults > maxAnnotationsPerObject {
		maxResults = defaultMaxAnnotationResults
	}

	start := 0
	if token := aws.ToString(input.ContinuationToken); token != "" {
		start = annotationStartIndex(names, token)
	}

	page, next := paginateAnnotationNames(names, start, maxResults)

	return buildListObjectAnnotationsOutput(input, ver, prefix, page, next), nil
}

func buildListObjectAnnotationsOutput(
	input *s3.ListObjectAnnotationsInput,
	ver *StoredObjectVersion,
	prefix string,
	page []string,
	next string,
) *s3.ListObjectAnnotationsOutput {
	entries := make([]types.AnnotationEntry, 0, len(page))
	for _, n := range page {
		entries = append(entries, annotationEntry(ver.Annotations[n]))
	}

	maxResults := int(aws.ToInt32(input.MaxAnnotationResults))
	if maxResults <= 0 || maxResults > maxAnnotationsPerObject {
		maxResults = defaultMaxAnnotationResults
	}

	//nolint:gosec // G115: bounded by maxAnnotationsPerObject (1000)
	out := &s3.ListObjectAnnotationsOutput{
		AnnotationCount:      aws.Int32(int32(len(entries))),
		Annotations:          entries,
		Bucket:               aws.String(aws.ToString(input.Bucket)),
		Key:                  aws.String(aws.ToString(input.Key)),
		MaxAnnotationResults: aws.Int32(int32(maxResults)),
		ObjectVersionId:      aws.String(ver.VersionID),
	}
	if prefix != "" {
		out.AnnotationPrefix = aws.String(prefix)
	}
	if token := aws.ToString(input.ContinuationToken); token != "" {
		out.ContinuationToken = aws.String(token)
	}
	if next != "" {
		out.NextContinuationToken = aws.String(next)
	}

	return out
}

func matchingAnnotationNames(anns map[string]*StoredAnnotation, prefix string) []string {
	names := make([]string, 0, len(anns))
	for n := range anns {
		if prefix == "" || strings.HasPrefix(n, prefix) {
			names = append(names, n)
		}
	}
	sort.Strings(names)

	return names
}

// annotationStartIndex returns the index of the first name strictly greater
// than token, implementing "resume after the last name of the previous page"
// pagination (the ContinuationToken value gopherstack hands back is the last
// name of the page it terminates).
func annotationStartIndex(names []string, token string) int {
	for i, n := range names {
		if n > token {
			return i
		}
	}

	return len(names)
}

func paginateAnnotationNames(names []string, start, maxResults int) ([]string, string) {
	if start >= len(names) {
		return nil, ""
	}

	end := start + maxResults
	if end >= len(names) {
		return names[start:], ""
	}

	return names[start:end], names[end-1]
}

func annotationEntry(ann *StoredAnnotation) types.AnnotationEntry {
	entry := types.AnnotationEntry{
		AnnotationName: aws.String(ann.Name),
		ETag:           aws.String(ann.ETag),
		LastModified:   aws.Time(ann.LastModified),
		Size:           aws.Int64(int64(len(ann.Payload))),
	}
	if ann.ChecksumAlgorithm != "" {
		entry.ChecksumAlgorithm = []types.ChecksumAlgorithm{ann.ChecksumAlgorithm}
	}

	return entry
}
