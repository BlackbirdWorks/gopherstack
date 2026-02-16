package s3

import (
	"Gopherstack/pkgs/httputils"
	"bytes"
	"crypto/sha1" //nolint:gosec // SHA1 required for S3 checksum compatibility
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	pathSplitParts   = 2
	tagKeyValueParts = 2
	defaultMaxKeys   = 1000

	locationConstraintXML = `<?xml version="1.0" encoding="UTF-8"?>` + "\n" +
		`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`

	checksumCRC32   = "CRC32"
	checksumCRC32C  = "CRC32C"
	checksumSHA1    = "SHA1"
	checksumSHA256  = "SHA256"
	storageStandard = "STANDARD"
)

// Handler implements [http.Handler] for S3-compatible API requests.
type Handler struct {
	Backend StorageBackend
	Logger  *slog.Logger
	// Endpoint is the base host (e.g. "localhost:9000") of this server.
	// When set, virtual-hosted-style URLs (bucket.host/key) are supported
	// in addition to path-style URLs (/bucket/key).
	Endpoint string
}

// NewHandler creates a new S3 Handler with the given backend.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend: backend,
		Logger:  slog.New(slog.NewJSONHandler(os.Stderr, nil)),
	}
}

// GetSupportedOperations returns a list of supported S3 operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateBucket",
		"DeleteBucket",
		"ListBuckets",
		"HeadBucket",
		"GetBucketVersioning",
		"PutBucketVersioning",
		"PutObject",
		"GetObject",
		"HeadObject",
		"DeleteObject",
		"ListObjects",
		"ListObjectsV2",
		"PutObjectTagging",
		"GetObjectTagging",
		"DeleteObjectTagging",
		"CreateMultipartUpload",
		"UploadPart",
		"CompleteMultipartUpload",
		"AbortMultipartUpload",
	}
}

// ServeHTTP dispatches incoming requests to the appropriate bucket or object handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bucketName, key, ok := h.resolveBucketAndKey(w, r)
	if !ok {
		return
	}

	if bucketName == "" {
		if r.Method != http.MethodGet {
			httputils.WriteError(h.Logger, w, r, ErrMethodNotAllowed, http.StatusMethodNotAllowed)

			return
		}

		h.listBuckets(w, r)

		return
	}

	if key == "" {
		h.handleBucketOperation(w, r, bucketName)

		return
	}

	h.handleObjectOperation(w, r, bucketName, key)
}

// resolveBucketAndKey extracts the bucket name and object key from the request.
// It supports both path-style (/bucket/key) and virtual-hosted-style (bucket.host/key).
// Returns (bucket, key, true) on success, or ("", "", false) when an error response
// has already been written.
func (h *Handler) resolveBucketAndKey(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", pathSplitParts)

	// Try virtual-hosted-style first: bucket name as subdomain in Host header.
	if vhBucket := h.extractVirtualHostedBucketName(r); vhBucket != "" {
		bucket := vhBucket
		key := path
		if key != "" && !IsValidObjectKey(key) {
			httputils.WriteError(h.Logger, w, r, ErrInvalidArgument, http.StatusBadRequest)

			return "", "", false
		}

		return bucket, key, true
	}

	// Fall back to path-style (/bucket/key).
	bucket, key := "", ""
	if path != "" && path != "/" {
		bucket = parts[0]
		if !IsValidBucketName(bucket) {
			httputils.WriteError(h.Logger, w, r, ErrInvalidBucketName, http.StatusBadRequest)

			return "", "", false
		}

		if len(parts) > 1 {
			key = parts[1]
			if key != "" && !IsValidObjectKey(key) {
				httputils.WriteError(h.Logger, w, r, ErrInvalidArgument, http.StatusBadRequest)

				return "", "", false
			}
		}
	}

	return bucket, key, true
}

// extractVirtualHostedBucketName returns the bucket name from the Host header
// when using virtual-hosted-style URLs (e.g. my-bucket.localhost:8080).
// Returns "" when Endpoint is not configured or the Host does not match.
func (h *Handler) extractVirtualHostedBucketName(r *http.Request) string {
	if h.Endpoint == "" {
		return ""
	}

	reqHost := r.Host
	if reqHost == "" {
		return ""
	}

	// Normalise both sides: strip ports before comparing.
	baseHost := h.Endpoint
	if stripped, _, err := net.SplitHostPort(baseHost); err == nil {
		baseHost = stripped
	}

	reqHostNoPort := reqHost
	if stripped, _, err := net.SplitHostPort(reqHost); err == nil {
		reqHostNoPort = stripped
	}

	// Request Host must end with ".<baseHost>".
	suffix := "." + baseHost
	if !strings.HasSuffix(reqHostNoPort, suffix) {
		return ""
	}

	candidate := reqHostNoPort[:len(reqHostNoPort)-len(suffix)]
	if IsValidBucketName(candidate) {
		return candidate
	}

	return ""
}

func (h *Handler) handleBucketOperation(w http.ResponseWriter, r *http.Request, bucket string) {
	switch r.Method {
	case http.MethodPut:
		h.routeBucketPut(w, r, bucket)
	case http.MethodDelete:
		h.deleteBucket(w, r, bucket)
	case http.MethodGet:
		h.routeBucketGet(w, r, bucket)
	case http.MethodHead:
		h.headBucket(w, r, bucket)
	default:
		httputils.WriteError(h.Logger, w, r, ErrMethodNotAllowed, http.StatusMethodNotAllowed)
	}
}

func (h *Handler) routeBucketPut(w http.ResponseWriter, r *http.Request, bucket string) {
	switch {
	case r.URL.Query().Has("versioning"):
		h.putBucketVersioning(w, r, bucket)
	case r.URL.Query().Has("tagging"):
		httputils.WriteError(h.Logger, w, r, ErrNotImplemented, http.StatusNotImplemented)
	default:
		h.createBucket(w, r, bucket)
	}
}

func (h *Handler) routeBucketGet(w http.ResponseWriter, r *http.Request, bucket string) {
	switch {
	case r.URL.Query().Has("versioning"):
		h.getBucketVersioning(w, r, bucket)
	case r.URL.Query().Has("versions"):
		h.listObjectVersions(w, r, bucket)
	case r.URL.Query().Has("location"):
		h.getBucketLocation(w, r, bucket)
	case r.URL.Query().Has("tagging"):
		httputils.WriteError(h.Logger, w, r, ErrNotImplemented, http.StatusNotImplemented)
	case r.URL.Query().Get("list-type") == "2":
		h.listObjectsV2(w, r, bucket)
	default:
		h.listObjects(w, r, bucket)
	}
}

func (h *Handler) handleObjectOperation(w http.ResponseWriter, r *http.Request, bucket, key string) {
	switch r.Method {
	case http.MethodPut:
		h.routeObjectPut(w, r, bucket, key)
	case http.MethodGet:
		h.routeObjectGet(w, r, bucket, key)
	case http.MethodDelete:
		h.routeObjectDelete(w, r, bucket, key)
	case http.MethodPost:
		h.routeObjectPost(w, r, bucket, key)
	case http.MethodHead:
		h.headObject(w, r, bucket, key)
	default:
		httputils.WriteError(h.Logger, w, r, ErrMethodNotAllowed, http.StatusMethodNotAllowed)
	}
}

func (h *Handler) routeObjectPut(w http.ResponseWriter, r *http.Request, bucket, key string) {
	switch {
	case r.URL.Query().Has("tagging"):
		h.putObjectTagging(w, r, bucket, key)
	case r.URL.Query().Has("acl"):
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK) // ACLs ignored
	case r.URL.Query().Has("partNumber") && r.URL.Query().Has("uploadId"):
		h.uploadPart(w, r, bucket, key)
	case r.Header.Get("X-Amz-Copy-Source") != "":
		h.copyObject(w, r, bucket, key)
	default:
		h.putObject(w, r, bucket, key)
	}
}

func (h *Handler) routeObjectGet(w http.ResponseWriter, r *http.Request, bucket, key string) {
	switch {
	case r.URL.Query().Has("tagging"):
		h.getObjectTagging(w, r, bucket, key)
	case r.URL.Query().Has("acl"):
		w.WriteHeader(http.StatusNotImplemented) // ACLs ignored
	default:
		h.getObject(w, r, bucket, key)
	}
}

func (h *Handler) routeObjectDelete(w http.ResponseWriter, r *http.Request, bucket, key string) {
	switch {
	case r.URL.Query().Has("tagging"):
		h.deleteObjectTagging(w, r, bucket, key)
	case r.URL.Query().Has("uploadId"):
		h.abortMultipartUpload(w, r, bucket, key)
	default:
		h.deleteObject(w, r, bucket, key)
	}
}

func (h *Handler) routeObjectPost(w http.ResponseWriter, r *http.Request, bucket, key string) {
	switch {
	case r.URL.Query().Has("uploads"):
		h.createMultipartUpload(w, r, bucket, key)
	case r.URL.Query().Has("uploadId"):
		h.completeMultipartUpload(w, r, bucket, key)
	default:
		httputils.WriteError(h.Logger, w, r, ErrMethodNotAllowed, http.StatusMethodNotAllowed)
	}
}

func (h *Handler) listBuckets(w http.ResponseWriter, r *http.Request) {
	out, err := h.Backend.ListBuckets(r.Context(), &s3.ListBucketsInput{})
	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	resp := ListAllMyBucketsResult{
		Owner: &Owner{
			ID:          "gopherstack",
			DisplayName: "gopherstack",
		},
	}

	for _, b := range out.Buckets {
		if b.Name != nil && b.CreationDate != nil {
			resp.Buckets = append(resp.Buckets, BucketXML{
				Name:         *b.Name,
				CreationDate: b.CreationDate.Format(time.RFC3339),
			})
		}
	}

	httputils.WriteXML(h.Logger, w, http.StatusOK, resp)
}

func (h *Handler) createBucket(w http.ResponseWriter, r *http.Request, bucketName string) {
	var region string
	// Read the body to check for LocationConstraint
	body, err := httputils.ReadBody(r)
	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	if len(body) > 0 {
		var config struct {
			LocationConstraint string `xml:"LocationConstraint"`
		}
		if xmlErr := xml.Unmarshal(body, &config); xmlErr == nil {
			region = config.LocationConstraint
		} else {
			h.Logger.Warn("failed to parse CreateBucketConfiguration", "error", xmlErr)
		}
	}

	// Default to us-east-1 if region is empty
	if region == "" {
		region = "us-east-1"
	}

	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}
	if region != "us-east-1" {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		}
	}

	_, err = h.Backend.CreateBucket(r.Context(), input)
	if errors.Is(err, ErrBucketAlreadyExists) {
		httputils.WriteError(h.Logger, w, r, err, http.StatusConflict)

		return
	}

	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	w.Header().Set("Location", "/"+bucketName)
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) deleteBucket(w http.ResponseWriter, r *http.Request, bucketName string) {
	_, err := h.Backend.DeleteBucket(r.Context(), &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})
	if errors.Is(err, ErrNoSuchBucket) {
		httputils.WriteError(h.Logger, w, r, err, http.StatusNotFound)

		return
	}

	if errors.Is(err, ErrBucketNotEmpty) {
		httputils.WriteError(h.Logger, w, r, err, http.StatusConflict)

		return
	}

	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) listObjects(w http.ResponseWriter, r *http.Request, bucketName string) {
	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	marker := r.URL.Query().Get("marker")

	maxKeys := int32(defaultMaxKeys)
	if mk := r.URL.Query().Get("max-keys"); mk != "" {
		if n, err := strconv.Atoi(mk); err == nil && n >= 0 {
			maxKeys = int32(n) //nolint:gosec // Validated non-negative, safe conversion
		}
	}

	out, err := h.Backend.ListObjects(r.Context(), &s3.ListObjectsInput{
		Bucket:  aws.String(bucketName),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(maxKeys),
	})
	if errors.Is(err, ErrNoSuchBucket) {
		httputils.WriteError(h.Logger, w, r, err, http.StatusNotFound)

		return
	}

	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	objects := out.Contents

	// Apply marker: skip all keys <= marker
	if marker != "" {
		start := len(objects)
		for i, obj := range objects {
			if *obj.Key > marker {
				start = i

				break
			}
		}
		objects = objects[start:]
	}

	isTruncated := false
	var nextMarker string
	if maxKeys > 0 && len(objects) > int(maxKeys) {
		isTruncated = true
		objects = objects[:maxKeys]
		nextMarker = *objects[maxKeys-1].Key
	}

	resp := ListBucketResult{
		Name:        bucketName,
		Prefix:      prefix,
		Delimiter:   delimiter,
		Marker:      marker,
		NextMarker:  nextMarker,
		MaxKeys:     int(maxKeys),
		IsTruncated: isTruncated,
	}

	seenPrefixes := make(map[string]struct{})
	resp.Contents, resp.CommonPrefixes = h.mapObjectsToXML(r, bucketName, objects, prefix, delimiter, seenPrefixes)
	resp.KeyCount = len(resp.Contents)

	httputils.WriteXML(h.Logger, w, http.StatusOK, resp)
}

func (h *Handler) listObjectsV2(w http.ResponseWriter, r *http.Request, bucketName string) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	continuationToken := q.Get("continuation-token")
	startAfter := q.Get("start-after")
	encodingType := q.Get("encoding-type")

	maxKeys := int32(defaultMaxKeys)
	if mk := q.Get("max-keys"); mk != "" {
		if n, err := strconv.Atoi(mk); err == nil && n >= 0 {
			maxKeys = int32(n) //nolint:gosec // Validated non-negative, safe conversion
		}
	}

	outV2, err := h.Backend.ListObjectsV2(r.Context(), &s3.ListObjectsV2Input{
		Bucket:            aws.String(bucketName),
		Prefix:            aws.String(prefix),
		ContinuationToken: aws.String(continuationToken),
		StartAfter:        aws.String(startAfter),
		MaxKeys:           aws.Int32(maxKeys),
	})

	if errors.Is(err, ErrNoSuchBucket) {
		httputils.WriteError(h.Logger, w, r, err, http.StatusNotFound)

		return
	}

	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	objects := outV2.Contents

	startCursor := startAfter
	if continuationToken != "" {
		startCursor = continuationToken
	}

	if startCursor != "" {
		var filtered []types.Object
		for i, obj := range objects {
			if *obj.Key > startCursor {
				filtered = objects[i:]

				break
			}
		}
		objects = filtered
	}

	isTruncated := false
	var nextToken string
	if maxKeys > 0 && len(objects) > int(maxKeys) {
		isTruncated = true
		objects = objects[:maxKeys]
		nextToken = *objects[maxKeys-1].Key
	}

	resp := ListBucketV2Result{
		Name:                  bucketName,
		Prefix:                prefix,
		Delimiter:             delimiter,
		ContinuationToken:     continuationToken,
		StartAfter:            startAfter,
		MaxKeys:               int(maxKeys),
		EncodingType:          encodingType,
		IsTruncated:           isTruncated,
		NextContinuationToken: nextToken,
	}

	seenPrefixes := make(map[string]struct{})
	resp.Contents, resp.CommonPrefixes = h.mapObjectsToXML(r, bucketName, objects, prefix, delimiter, seenPrefixes)
	resp.KeyCount = len(resp.Contents) + len(resp.CommonPrefixes)

	httputils.WriteXML(h.Logger, w, http.StatusOK, resp)
}

func (h *Handler) getBucketLocation(w http.ResponseWriter, _ *http.Request, _ string) {
	// For now, always return us-east-1 or the configured region if we had it.
	fmt.Fprint(w, locationConstraintXML)
}

func (h *Handler) mapObjectsToXML(
	r *http.Request,
	bucketName string,
	objects []types.Object,
	prefix, delimiter string,
	seenPrefixes map[string]struct{},
) ([]ObjectXML, []CommonPrefixXML) {
	var contents []ObjectXML
	var commonPrefixes []CommonPrefixXML

	for _, obj := range objects {
		key := *obj.Key
		if cp, isCommon := commonPrefixFor(key, prefix, delimiter); isCommon {
			if _, seen := seenPrefixes[cp]; !seen {
				seenPrefixes[cp] = struct{}{}
				commonPrefixes = append(commonPrefixes, CommonPrefixXML{Prefix: cp})
			}

			continue
		}

		ver, getErr := h.Backend.GetObject(r.Context(), &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    obj.Key,
		})
		if getErr != nil {
			continue
		}
		_ = ver.Body.Close()

		contents = append(contents, ObjectXML{
			Key:          key,
			LastModified: obj.LastModified.Format(time.RFC3339),
			Size:         *obj.Size,
			ETag:         *ver.ETag,
			StorageClass: storageStandard,
			ChecksumAlgorithm: getChecksumAlgo(
				ver.ChecksumCRC32,
				ver.ChecksumCRC32C,
				ver.ChecksumSHA1,
				ver.ChecksumSHA256,
			),
		})
	}

	return contents, commonPrefixes
}

func getChecksumAlgo(crc32, crc32c, sha1, sha256 *string) string {
	switch {
	case crc32 != nil:
		return checksumCRC32
	case crc32c != nil:
		return checksumCRC32C
	case sha1 != nil:
		return checksumSHA1
	case sha256 != nil:
		return checksumSHA256
	default:
		return ""
	}
}

func (h *Handler) headBucket(w http.ResponseWriter, r *http.Request, bucketName string) {
	_, err := h.Backend.HeadBucket(r.Context(), &s3.HeadBucketInput{Bucket: aws.String(bucketName)})
	if errors.Is(err, ErrNoSuchBucket) {
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusNotFound)

		return
	}

	if err != nil {
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}

func commonPrefixFor(key, prefix, delimiter string) (string, bool) {
	if delimiter == "" {
		return "", false
	}

	rest := strings.TrimPrefix(key, prefix)
	idx := strings.Index(rest, delimiter)

	if idx < 0 {
		return "", false
	}

	return prefix + rest[:idx+len(delimiter)], true
}

type objectCommonDetails struct {
	Metadata       map[string]string
	ETag           *string
	ContentType    *string
	ContentLength  *int64
	LastModified   *time.Time
	VersionID      *string
	ChecksumCRC32  *string
	ChecksumCRC32C *string
	ChecksumSHA1   *string
	ChecksumSHA256 *string
}

func (h *Handler) setCommonHeaders(w http.ResponseWriter, out objectCommonDetails) {
	if out.ETag != nil {
		w.Header().Set("ETag", *out.ETag)
	}

	if out.ContentLength != nil {
		w.Header().Set("Content-Length", strconv.FormatInt(*out.ContentLength, 10))
	}

	if out.LastModified != nil {
		w.Header().Set("Last-Modified", out.LastModified.Format(http.TimeFormat))
	}

	if out.ContentType != nil {
		w.Header().Set("Content-Type", *out.ContentType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}

	for k, v := range out.Metadata {
		w.Header().Set("X-Amz-Meta-"+k, v)
	}

	if out.VersionID != nil && *out.VersionID != NullVersion {
		w.Header().Set("X-Amz-Version-Id", *out.VersionID)
	}

	h.setChecksumHeaders(w, out)
}

func parseCopySource(src string) (string, string, string, bool) {
	src = strings.TrimPrefix(src, "/")
	parts := strings.SplitN(src, "/", pathSplitParts)

	if len(parts) != pathSplitParts {
		return "", "", "", false
	}

	bucket := parts[0]
	key := parts[1]
	versionID := ""

	if idx := strings.Index(key, "?versionId="); idx != -1 {
		versionID = key[idx+11:]
		key = key[:idx]
	}

	return bucket, key, versionID, true
}

func (h *Handler) setChecksumHeaders(w http.ResponseWriter, out objectCommonDetails) {
	var algo, val string

	switch {
	case out.ChecksumCRC32 != nil:
		algo, val = checksumCRC32, *out.ChecksumCRC32
	case out.ChecksumCRC32C != nil:
		algo, val = checksumCRC32C, *out.ChecksumCRC32C
	case out.ChecksumSHA1 != nil:
		algo, val = checksumSHA1, *out.ChecksumSHA1
	case out.ChecksumSHA256 != nil:
		algo, val = checksumSHA256, *out.ChecksumSHA256
	}

	if algo != "" {
		w.Header().Set("X-Amz-Checksum-"+algo, val)
		w.Header().Set("X-Amz-Checksum-Algorithm", algo)
	}
}

func extractChecksumPointers(h http.Header, algo string) (*string, *string, *string, *string) {
	if algo == "" {
		return nil, nil, nil, nil
	}

	headerName := "X-Amz-Checksum-" + strings.ToLower(algo)
	checksum := h.Get(headerName)

	if checksum == "" {
		return nil, nil, nil, nil
	}

	switch algo {
	case checksumCRC32:
		return aws.String(checksum), nil, nil, nil
	case checksumCRC32C:
		return nil, aws.String(checksum), nil, nil
	case checksumSHA1:
		return nil, nil, aws.String(checksum), nil
	case checksumSHA256:
		return nil, nil, nil, aws.String(checksum)
	default:
		return nil, nil, nil, nil
	}
}

func (h *Handler) putObject(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	data, err := httputils.ReadBody(r)
	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	userMeta := parseUserMetadata(r.Header)
	algo := strings.ToUpper(r.Header.Get("X-Amz-Checksum-Algorithm"))

	if algo == "" {
		algo = strings.ToUpper(r.Header.Get("X-Amz-Sdk-Checksum-Algorithm"))
	}

	checksumCRC32, checksumCRC32C, checksumSHA1, checksumSHA256 := extractChecksumPointers(r.Header, algo)

	contentType := r.Header.Get("Content-Type")

	ver, err := h.Backend.PutObject(
		r.Context(),
		&s3.PutObjectInput{
			Bucket:            aws.String(bucketName),
			Key:               aws.String(key),
			Body:              bytes.NewReader(data), // Using NewReader from data slice
			Metadata:          userMeta,
			ContentType:       aws.String(contentType),
			ChecksumAlgorithm: types.ChecksumAlgorithm(algo),
			ChecksumCRC32:     checksumCRC32,
			ChecksumCRC32C:    checksumCRC32C,
			ChecksumSHA1:      checksumSHA1,
			ChecksumSHA256:    checksumSHA256,
			Tagging:           aws.String(r.Header.Get("X-Amz-Tagging")),
		},
	)
	if errors.Is(err, ErrNoSuchBucket) {
		httputils.WriteError(h.Logger, w, r, err, http.StatusNotFound)

		return
	}

	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	w.Header().Set("ETag", *ver.ETag)

	details := objectCommonDetails{
		ETag:           ver.ETag,
		VersionID:      ver.VersionId,
		ChecksumCRC32:  ver.ChecksumCRC32,
		ChecksumCRC32C: ver.ChecksumCRC32C,
		ChecksumSHA1:   ver.ChecksumSHA1,
		ChecksumSHA256: ver.ChecksumSHA256,
	}

	h.setChecksumHeaders(w, details)

	if ver.VersionId != nil && *ver.VersionId != NullVersion {
		w.Header().Set("X-Amz-Version-Id", *ver.VersionId)
	}

	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) copyObject(w http.ResponseWriter, r *http.Request, destBucket, destKey string) {
	srcBucket, srcKey, srcVersionID, ok := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
	if !ok {
		httputils.WriteError(h.Logger, w, r, ErrInvalidArgument, http.StatusBadRequest)

		return
	}

	// VersionId can also be in the header override
	if hID := r.Header.Get("X-Amz-Copy-Source-Version-Id"); hID != "" {
		srcVersionID = hID
	}

	var vid *string
	if srcVersionID != "" {
		vid = aws.String(srcVersionID)
	}

	srcVer, err := h.Backend.GetObject(r.Context(), &s3.GetObjectInput{
		Bucket:    aws.String(srcBucket),
		Key:       aws.String(srcKey),
		VersionId: vid,
	})
	if errors.Is(err, ErrNoSuchBucket) || errors.Is(err, ErrNoSuchKey) {
		httputils.WriteError(h.Logger, w, r, err, http.StatusNotFound)

		return
	}
	defer srcVer.Body.Close()

	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	data, err := io.ReadAll(srcVer.Body)
	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	userMeta := srcVer.Metadata
	contentType := srcVer.ContentType

	// Checksum: if src had one, preserve?
	// Simplified: Copy does not automatically copy checksums in simple impl unless set explicitly
	// but we can try to reuse logic if available.
	// For now, ignore ChecksumAlgorithm in Copy unless REPLACE directives.

	// REPLACE directive
	if r.Header.Get("X-Amz-Metadata-Directive") == "REPLACE" {
		if ct := r.Header.Get("Content-Type"); ct != "" {
			contentType = aws.String(ct)
		}
		userMeta = parseUserMetadata(r.Header)
	}

	putInput := &s3.PutObjectInput{
		Bucket:      aws.String(destBucket),
		Key:         aws.String(destKey),
		Body:        bytes.NewReader(data),
		Metadata:    userMeta,
		ContentType: contentType,
		// No checksum copying
	}

	destVer, err := h.Backend.PutObject(r.Context(), putInput)
	if errors.Is(err, ErrNoSuchBucket) {
		httputils.WriteError(h.Logger, w, r, err, http.StatusNotFound)

		return
	}

	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	if destVer.VersionId != nil && *destVer.VersionId != NullVersion {
		w.Header().Set("X-Amz-Version-Id", *destVer.VersionId)
	}

	etag := ""
	if destVer.ETag != nil {
		etag = *destVer.ETag
	}

	httputils.WriteXML(h.Logger, w, http.StatusOK, CopyObjectResult{
		ETag:         etag,
		LastModified: time.Now().Format(time.RFC3339),
	})
}

func (h *Handler) getObject(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	versionID := r.URL.Query().Get("versionId")
	var vid *string

	if versionID != "" {
		vid = aws.String(versionID)
	}

	ver, err := h.Backend.GetObject(r.Context(), &s3.GetObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: vid,
	})
	if errors.Is(err, ErrNoSuchBucket) || errors.Is(err, ErrNoSuchKey) {
		httputils.WriteError(h.Logger, w, r, err, http.StatusNotFound)

		return
	}

	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}
	defer ver.Body.Close()

	details := objectCommonDetails{
		Metadata:       ver.Metadata,
		ETag:           ver.ETag,
		ContentType:    ver.ContentType,
		ContentLength:  ver.ContentLength,
		LastModified:   ver.LastModified,
		VersionID:      ver.VersionId,
		ChecksumCRC32:  ver.ChecksumCRC32,
		ChecksumCRC32C: ver.ChecksumCRC32C,
		ChecksumSHA1:   ver.ChecksumSHA1,
		ChecksumSHA256: ver.ChecksumSHA256,
	}

	h.setCommonHeaders(w, details)
	w.Header().Set("Accept-Ranges", "bytes")

	// Support flexible checksums if requested
	if r.Header.Get("X-Amz-Checksum-Mode") == "ENABLED" {
		h.handleChecksumMode(w, ver, details)
	}

	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		data, _ := io.ReadAll(ver.Body)
		if h.serveRange(w, data, rangeHeader) {
			return
		}

		// Fallback to full response if range header ignored/invalidly formatted
		ver.Body = io.NopCloser(bytes.NewReader(data))
	}

	w.WriteHeader(http.StatusOK)

	if _, copyErr := io.Copy(w, ver.Body); copyErr != nil {
		h.Logger.Error("failed to write object data", "error", copyErr)
	}
}

func (h *Handler) handleChecksumMode(w http.ResponseWriter, ver *s3.GetObjectOutput, details objectCommonDetails) {
	algo, val := h.getStoredChecksum(details)
	if algo == "" {
		data, _ := io.ReadAll(ver.Body)
		// Reset body for sending
		ver.Body = io.NopCloser(bytes.NewReader(data))

		algo = checksumCRC32
		val = CalculateChecksum(data, algo)
	}

	w.Header().Set("X-Amz-Checksum-Algorithm", algo)
	w.Header().Set("X-Amz-Checksum-"+algo, val)
}

func (h *Handler) getStoredChecksum(out objectCommonDetails) (string, string) {
	switch {
	case out.ChecksumCRC32 != nil:
		return checksumCRC32, *out.ChecksumCRC32
	case out.ChecksumCRC32C != nil:
		return checksumCRC32C, *out.ChecksumCRC32C
	case out.ChecksumSHA1 != nil:
		return checksumSHA1, *out.ChecksumSHA1
	case out.ChecksumSHA256 != nil:
		return checksumSHA256, *out.ChecksumSHA256
	default:
		return "", ""
	}
}

func (h *Handler) serveRange(w http.ResponseWriter, data []byte, rangeHeader string) bool {
	total := int64(len(data))
	start, end, ok := parseRange(rangeHeader, total)

	if !ok {
		// S3 ignores invalid range formats/units and returns 200 OK with full body
		if !strings.HasPrefix(rangeHeader, "bytes=") {
			return false
		}

		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)

		return true
	}

	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.WriteHeader(http.StatusPartialContent)

	if _, err := w.Write(data[start : end+1]); err != nil {
		h.Logger.Error("failed to write range data", "error", err)
	}

	return true
}

const rangeSpecMaxParts = 2

// parseRange parses a "bytes=X-Y" Range header and returns clamped [start, end] indices.
func parseRange(header string, size int64) (int64, int64, bool) {
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false
	}

	spec := strings.TrimSpace(strings.SplitN(header[len("bytes="):], ",", rangeSpecMaxParts)[0])
	startStr, endStr, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false
	}

	var start, end int64
	switch {
	case startStr == "":
		// bytes=-N (last N bytes)
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		start = max(size-n, 0)
		end = size - 1
	case endStr == "":
		// bytes=N-
		var err error
		start, err = strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			return 0, 0, false
		}
		end = size - 1
	default:
		var err error
		start, err = strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			return 0, 0, false
		}
		end, err = strconv.ParseInt(endStr, 10, 64)
		if err != nil {
			return 0, 0, false
		}
	}

	if start > end || start >= size {
		return 0, 0, false
	}
	if end >= size {
		end = size - 1
	}

	return start, end, true
}

func (h *Handler) deleteObject(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	versionID := r.URL.Query().Get("versionId")
	var vid *string
	if versionID != "" {
		vid = aws.String(versionID)
	}

	out, err := h.Backend.DeleteObject(r.Context(), &s3.DeleteObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: vid,
	})
	if errors.Is(err, ErrNoSuchBucket) {
		httputils.WriteError(h.Logger, w, r, err, http.StatusNotFound)

		return
	}

	if errors.Is(err, ErrNoSuchKey) {
		httputils.WriteError(h.Logger, w, r, err, http.StatusNotFound)

		return
	}

	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	if out.VersionId != nil && *out.VersionId != "" && *out.VersionId != NullVersion {
		w.Header().Set("X-Amz-Version-Id", *out.VersionId)
	}
	if out.DeleteMarker != nil && *out.DeleteMarker {
		w.Header().Set("X-Amz-Delete-Marker", "true")
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) headObject(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	versionID := r.URL.Query().Get("versionId")
	var vid *string

	if versionID != "" {
		vid = aws.String(versionID)
	}

	out, err := h.Backend.HeadObject(r.Context(), &s3.HeadObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: vid,
	})
	if errors.Is(err, ErrNoSuchBucket) || errors.Is(err, ErrNoSuchKey) {
		w.WriteHeader(http.StatusNotFound)

		return
	}

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	details := objectCommonDetails{
		Metadata:       out.Metadata,
		ETag:           out.ETag,
		ContentType:    out.ContentType,
		ContentLength:  out.ContentLength,
		LastModified:   out.LastModified,
		VersionID:      out.VersionId,
		ChecksumCRC32:  out.ChecksumCRC32,
		ChecksumCRC32C: out.ChecksumCRC32C,
		ChecksumSHA1:   out.ChecksumSHA1,
		ChecksumSHA256: out.ChecksumSHA256,
	}

	h.setCommonHeaders(w, details)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) putBucketVersioning(w http.ResponseWriter, r *http.Request, bucketName string) {
	var conf VersioningConfiguration
	if err := xml.NewDecoder(r.Body).Decode(&conf); err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusBadRequest)

		return
	}

	_, err := h.Backend.PutBucketVersioning(r.Context(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatus(conf.Status),
		},
	})
	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusNotFound)

		return
	}

	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) getBucketVersioning(w http.ResponseWriter, r *http.Request, bucketName string) {
	out, err := h.Backend.GetBucketVersioning(r.Context(), &s3.GetBucketVersioningInput{Bucket: aws.String(bucketName)})
	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusNotFound)

		return
	}

	status := ""
	if out.Status != "" {
		status = string(out.Status)
	}

	httputils.WriteXML(h.Logger, w, http.StatusOK, VersioningConfiguration{
		Status: status,
	})
}

func (h *Handler) listObjectVersions(w http.ResponseWriter, r *http.Request, bucketName string) {
	prefix := r.URL.Query().Get("prefix")

	out, err := h.Backend.ListObjectVersions(r.Context(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	})
	if errors.Is(err, ErrNoSuchBucket) {
		httputils.WriteError(h.Logger, w, r, err, http.StatusNotFound)

		return
	}

	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	resp := ListVersionsResult{
		Name:    bucketName,
		Prefix:  prefix,
		MaxKeys: defaultMaxKeys,
	}

	// Map SDK types to XML
	for _, v := range out.Versions {
		size := int64(0)
		if v.Size != nil {
			size = *v.Size
		}
		etag := ""
		if v.ETag != nil {
			etag = *v.ETag
		}
		resp.Versions = append(resp.Versions, ObjectVersionXML{
			Key:          *v.Key,
			VersionID:    *v.VersionId,
			IsLatest:     *v.IsLatest,
			LastModified: v.LastModified.Format(time.RFC3339),
			ETag:         etag,
			Size:         size,
			Owner: &Owner{
				ID:          "gopherstack",
				DisplayName: "gopherstack",
			},
			StorageClass: "STANDARD",
		})
	}

	for _, d := range out.DeleteMarkers {
		resp.DeleteMarkers = append(resp.DeleteMarkers, DeleteMarkerXML{
			Key:          *d.Key,
			VersionID:    *d.VersionId,
			IsLatest:     *d.IsLatest,
			LastModified: d.LastModified.Format(time.RFC3339),
			Owner: &Owner{
				ID:          "gopherstack",
				DisplayName: "gopherstack",
			},
		})
	}

	httputils.WriteXML(h.Logger, w, http.StatusOK, resp)
}

func (h *Handler) putObjectTagging(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	var tagging Tagging
	if err := xml.NewDecoder(r.Body).Decode(&tagging); err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusBadRequest)

		return
	}

	var tags []types.Tag
	for _, t := range tagging.TagSet.Tags {
		tags = append(tags, types.Tag{
			Key:   aws.String(t.Key),
			Value: aws.String(t.Value),
		})
	}

	versionID := r.URL.Query().Get("versionId")
	var vid *string
	if versionID != "" {
		vid = aws.String(versionID)
	}

	if _, err := h.Backend.PutObjectTagging(r.Context(), &s3.PutObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: vid,
		Tagging:   &types.Tagging{TagSet: tags},
	}); err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) getObjectTagging(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	versionID := r.URL.Query().Get("versionId")
	var vid *string
	if versionID != "" {
		vid = aws.String(versionID)
	}

	out, err := h.Backend.GetObjectTagging(r.Context(), &s3.GetObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: vid,
	})
	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusNotFound)

		return
	}

	resp := Tagging{
		TagSet: TagSet{},
	}

	for _, t := range out.TagSet {
		if t.Key != nil && t.Value != nil {
			resp.TagSet.Tags = append(resp.TagSet.Tags, Tag{Key: *t.Key, Value: *t.Value})
		}
	}

	httputils.WriteXML(h.Logger, w, http.StatusOK, resp)
}

func (h *Handler) deleteObjectTagging(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	versionID := r.URL.Query().Get("versionId")
	var vid *string
	if versionID != "" {
		vid = aws.String(versionID)
	}

	if _, err := h.Backend.DeleteObjectTagging(r.Context(), &s3.DeleteObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: vid,
	}); err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// Multipart Upload Handlers

func (h *Handler) createMultipartUpload(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	out, err := h.Backend.CreateMultipartUpload(r.Context(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	resp := InitiateMultipartUploadResult{
		Bucket:   bucketName,
		Key:      key,
		UploadID: *out.UploadId,
	}

	httputils.WriteXML(h.Logger, w, http.StatusOK, resp)
}

func (h *Handler) uploadPart(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	uploadID := r.URL.Query().Get("uploadId")
	partNumberStr := r.URL.Query().Get("partNumber")
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil {
		httputils.WriteError(h.Logger, w, r, ErrInvalidArgument, http.StatusBadRequest)

		return
	}

	// Read body to bytes to create reader
	data, err := httputils.ReadBody(r)
	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	out, err := h.Backend.UploadPart(r.Context(), &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(int32(partNumber)), //nolint:gosec // Part number validated in request parsing
		Body:       bytes.NewReader(data),
	})
	if errors.Is(err, ErrNoSuchBucket) || errors.Is(err, ErrNoSuchKey) || errors.Is(err, ErrNoSuchUpload) {
		httputils.WriteError(h.Logger, w, r, err, http.StatusNotFound)

		return
	}

	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	w.Header().Set("ETag", *out.ETag)
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) completeMultipartUpload(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	uploadID := r.URL.Query().Get("uploadId")

	// Parse XML body for parts list
	var partsReq CompleteMultipartUpload
	if err := xml.NewDecoder(r.Body).Decode(&partsReq); err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusBadRequest)

		return
	}

	// Convert XML parts to SDK parts
	var sdkParts []types.CompletedPart
	for _, p := range partsReq.Parts {
		sdkParts = append(sdkParts, types.CompletedPart{
			ETag:       aws.String(p.ETag),
			PartNumber: aws.Int32(int32(p.PartNumber)), // #nosec G115
		})
	}

	out, err := h.Backend.CompleteMultipartUpload(
		r.Context(),
		&s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(bucketName),
			Key:             aws.String(key),
			UploadId:        aws.String(uploadID),
			MultipartUpload: &types.CompletedMultipartUpload{Parts: sdkParts},
		},
	)
	if errors.Is(err, ErrNoSuchBucket) || errors.Is(err, ErrNoSuchKey) || errors.Is(err, ErrNoSuchUpload) {
		httputils.WriteError(h.Logger, w, r, err, http.StatusNotFound)

		return
	}

	if errors.Is(err, ErrInvalidPart) {
		httputils.WriteError(h.Logger, w, r, err, http.StatusBadRequest)

		return
	}

	if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	resp := CompleteMultipartUploadResult{
		Location: "/" + bucketName + "/" + key,
		Bucket:   bucketName,
		Key:      key,
		ETag:     *out.ETag,
	}

	httputils.WriteXML(h.Logger, w, http.StatusOK, resp)
}

func (h *Handler) abortMultipartUpload(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	uploadID := r.URL.Query().Get("uploadId")

	if _, err := h.Backend.AbortMultipartUpload(r.Context(), &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	}); errors.Is(err, ErrNoSuchUpload) {
		httputils.WriteError(h.Logger, w, r, err, http.StatusNotFound)

		return
	} else if err != nil {
		httputils.WriteError(h.Logger, w, r, err, http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func parseUserMetadata(h http.Header) map[string]string {
	meta := make(map[string]string)
	for k, v := range h {
		lowerK := strings.ToLower(k)
		if key, ok := strings.CutPrefix(lowerK, "x-amz-meta-"); ok {
			if len(v) > 0 {
				meta[key] = v[0]
			}
		}
	}

	return meta
}

const (
	crc32Len   = 4
	bitsInByte = 8
)

func CalculateChecksum(data []byte, algorithm string) string {
	var sum []byte

	switch strings.ToUpper(algorithm) {
	case "CRC32":
		c := crc32.ChecksumIEEE(data)
		sum = make([]byte, crc32Len)
		for i := range crc32Len {
			sum[crc32Len-1-i] = byte(c >> (bitsInByte * i))
		}
	case "CRC32C":
		c := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
		sum = make([]byte, crc32Len)
		for i := range crc32Len {
			sum[crc32Len-1-i] = byte(c >> (bitsInByte * i))
		}
	case "SHA1":
		//nolint:gosec // SHA1 supported as per S3 spec
		h := sha1.Sum(data)
		sum = h[:]
	case "SHA256":
		h := sha256.Sum256(data)
		sum = h[:]
	default:
		return ""
	}

	return base64.StdEncoding.EncodeToString(sum)
}
