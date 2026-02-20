package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func (h *S3Handler) handleBucketOperation(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	log := logger.Load(ctx)
	switch r.Method {
	case http.MethodPut:
		h.routeBucketPut(ctx, w, r, bucket)
	case http.MethodDelete:
		h.deleteBucket(ctx, w, r, bucket)
	case http.MethodGet:
		h.routeBucketGet(ctx, w, r, bucket)
	case http.MethodPost:
		h.routeBucketPost(ctx, w, r, bucket)
	case http.MethodHead:
		h.headBucket(ctx, w, r, bucket)
	default:
		httputil.WriteError(log, w, r, ErrMethodNotAllowed, http.StatusMethodNotAllowed)
	}
}

func (h *S3Handler) routeBucketPut(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	log := logger.Load(ctx)
	switch {
	case r.URL.Query().Has("versioning"):
		h.putBucketVersioning(ctx, w, r, bucket)
	case r.URL.Query().Has("tagging"):
		httputil.WriteError(log, w, r, ErrNotImplemented, http.StatusNotImplemented)
	default:
		h.createBucket(ctx, w, r, bucket)
	}
}

func (h *S3Handler) routeBucketPost(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	log := logger.Load(ctx)
	if r.URL.Query().Has("delete") {
		h.deleteObjects(ctx, w, r, bucket)

		return
	}

	httputil.WriteError(log, w, r, ErrMethodNotAllowed, http.StatusMethodNotAllowed)
}

func (h *S3Handler) routeBucketGet(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	log := logger.Load(ctx)
	switch {
	case r.URL.Query().Has("versioning"):
		h.getBucketVersioning(ctx, w, r, bucket)
	case r.URL.Query().Has("versions"):
		h.listObjectVersions(ctx, w, r, bucket)
	case r.URL.Query().Has("location"):
		h.getBucketLocation(ctx, w, r, bucket)
	case r.URL.Query().Has("tagging"):
		httputil.WriteError(log, w, r, ErrNotImplemented, http.StatusNotImplemented)
	case r.URL.Query().Get("list-type") == "2":
		h.listObjectsV2(ctx, w, r, bucket)
	default:
		h.listObjects(ctx, w, r, bucket)
	}
}

func (h *S3Handler) listBuckets(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	h.setOperation(ctx, "ListBuckets")
	log := logger.Load(ctx)
	out, err := h.Backend.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		httputil.WriteError(log, w, r, err, http.StatusInternalServerError)

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

	httputil.WriteXML(log, w, http.StatusOK, resp)
}

func (h *S3Handler) createBucket(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "CreateBucket")
	log := logger.Load(ctx)
	log.DebugContext(ctx, "S3 createBucket input", "bucket", bucketName)

	var region string
	// Read the body to check for LocationConstraint
	body, err := httputil.ReadBody(r)
	if err != nil {
		httputil.WriteError(log, w, r, err, http.StatusInternalServerError)

		return
	}

	if len(body) > 0 {
		var config struct {
			LocationConstraint string `xml:"LocationConstraint"`
		}
		if xmlErr := xml.Unmarshal(body, &config); xmlErr == nil {
			region = config.LocationConstraint
		} else {
			log.WarnContext(ctx, "failed to parse CreateBucketConfiguration", "error", xmlErr)
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

	output, err := h.Backend.CreateBucket(ctx, input)
	if errors.Is(err, ErrBucketAlreadyExists) {
		httputil.WriteError(log, w, r, err, http.StatusConflict)

		return
	}

	if err != nil {
		httputil.WriteError(log, w, r, err, http.StatusInternalServerError)

		return
	}

	log.DebugContext(ctx, "S3 createBucket output", "bucket", bucketName, "region", region)

	// Set Location header from output
	if output.Location != nil {
		w.Header().Set("Location", *output.Location)
	}
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) deleteBucket(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "DeleteBucket")
	log := logger.Load(ctx)
	log.DebugContext(ctx, "S3 deleteBucket input", "bucket", bucketName)

	_, err := h.Backend.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})
	if errors.Is(err, ErrNoSuchBucket) {
		httputil.WriteError(log, w, r, err, http.StatusNotFound)

		return
	}

	if errors.Is(err, ErrBucketNotEmpty) {
		httputil.WriteError(log, w, r, err, http.StatusConflict)

		return
	}

	if err != nil {
		httputil.WriteError(log, w, r, err, http.StatusInternalServerError)

		return
	}

	log.DebugContext(ctx, "S3 deleteBucket output", "bucket", bucketName)

	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) listObjects(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "ListObjects")
	log := logger.Load(ctx)
	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	marker := r.URL.Query().Get("marker")

	log.DebugContext(ctx,
		"S3 listObjects input",
		"bucket", bucketName, "prefix", prefix, "delimiter", delimiter, "marker", marker,
	)

	maxKeys := int32(defaultMaxKeys)
	if mk := r.URL.Query().Get("max-keys"); mk != "" {
		if n, err := strconv.Atoi(mk); err == nil && n >= 0 && n <= 1000 {
			maxKeys = int32(n) //nolint:gosec // Validated range
		}
	}

	out, err := h.Backend.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket:  aws.String(bucketName),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(maxKeys),
	})
	if errors.Is(err, ErrNoSuchBucket) {
		httputil.WriteError(log, w, r, err, http.StatusNotFound)

		return
	}

	if err != nil {
		httputil.WriteError(log, w, r, err, http.StatusInternalServerError)

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

	log.DebugContext(ctx,
		"S3 listObjects output",
		"bucket", bucketName, "objectCount", len(objects), "isTruncated", isTruncated,
	)

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
	resp.Contents, resp.CommonPrefixes = h.mapObjectsToXML(
		r,
		bucketName,
		objects,
		prefix,
		delimiter,
		seenPrefixes,
	)
	resp.KeyCount = len(resp.Contents)

	httputil.WriteXML(log, w, http.StatusOK, resp)
}

func (h *S3Handler) getBucketLocation(
	ctx context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	_ string,
) {
	h.setOperation(ctx, "GetBucketLocation")

	// For now, always return us-east-1 or the configured region if we had it.
	fmt.Fprint(w, locationConstraintXML)
}

func (h *S3Handler) mapObjectsToXML(
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

func (h *S3Handler) headBucket(
	ctx context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "HeadBucket")
	_, err := h.Backend.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucketName)})
	if errors.Is(err, ErrNoSuchBucket) {
		w.WriteHeader(http.StatusNotFound)

		return
	}

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

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

func (h *S3Handler) putBucketVersioning(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "PutBucketVersioning")
	log := logger.Load(ctx)
	var conf VersioningConfiguration
	if err := xml.NewDecoder(r.Body).Decode(&conf); err != nil {
		httputil.WriteError(log, w, r, err, http.StatusBadRequest)

		return
	}

	_, err := h.Backend.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatus(conf.Status),
		},
	})
	if err != nil {
		httputil.WriteError(log, w, r, err, http.StatusNotFound)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketVersioning(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "GetBucketVersioning")
	log := logger.Load(ctx)
	out, err := h.Backend.GetBucketVersioning(
		ctx,
		&s3.GetBucketVersioningInput{Bucket: aws.String(bucketName)},
	)
	if err != nil {
		httputil.WriteError(log, w, r, err, http.StatusNotFound)

		return
	}

	status := ""
	if out.Status != "" {
		status = string(out.Status)
	}

	httputil.WriteXML(log, w, http.StatusOK, VersioningConfiguration{
		Status: status,
	})
}

func (h *S3Handler) listObjectVersions(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "ListObjectVersions")
	log := logger.Load(ctx)
	prefix := r.URL.Query().Get("prefix")

	out, err := h.Backend.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	})
	if errors.Is(err, ErrNoSuchBucket) {
		httputil.WriteError(log, w, r, err, http.StatusNotFound)

		return
	}

	if err != nil {
		httputil.WriteError(log, w, r, err, http.StatusInternalServerError)

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

	httputil.WriteXML(log, w, http.StatusOK, resp)
}
