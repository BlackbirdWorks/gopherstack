package s3

import (
	"context"
	"encoding/xml"
	"errors"
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
		switch {
		case r.URL.Query().Has("policy"):
			h.deleteBucketPolicy(ctx, w, r, bucket)
		case r.URL.Query().Has("cors"):
			h.deleteBucketCORS(ctx, w, r, bucket)
		default:
			h.deleteBucket(ctx, w, r, bucket)
		}
	case http.MethodGet:
		h.routeBucketGet(ctx, w, r, bucket)
	case http.MethodPost:
		h.routeBucketPost(ctx, w, r, bucket)
	case http.MethodHead:
		h.headBucket(ctx, w, r, bucket)
	case http.MethodOptions:
		h.handleCORSPreflight(ctx, w, r, bucket)
	default:
		WriteError(log, w, r, ErrMethodNotAllowed)
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
	case r.URL.Query().Has("acl"):
		h.putBucketACL(ctx, w, r, bucket)
	case r.URL.Query().Has("versioning"):
		h.putBucketVersioning(ctx, w, r, bucket)
	case r.URL.Query().Has("notification"):
		// Stub: accept notification configuration but do not deliver events.
		h.setOperation(ctx, "PutBucketNotificationConfiguration")
		w.WriteHeader(http.StatusOK)
	case r.URL.Query().Has("policy"):
		h.putBucketPolicy(ctx, w, r, bucket)
	case r.URL.Query().Has("cors"):
		h.putBucketCORS(ctx, w, r, bucket)
	case r.URL.Query().Has("website"):
		// Stub: accept static website configuration.
		h.setOperation(ctx, "PutBucketWebsite")
		w.WriteHeader(http.StatusOK)
	case r.URL.Query().Has("lifecycle"):
		// Stub: accept lifecycle configuration.
		h.setOperation(ctx, "PutBucketLifecycleConfiguration")
		w.WriteHeader(http.StatusNoContent)
	case r.URL.Query().Has("replication"):
		// Stub: accept replication configuration.
		h.setOperation(ctx, "PutBucketReplication")
		w.WriteHeader(http.StatusOK)
	case r.URL.Query().Has("encryption"):
		// Stub: accept encryption configuration.
		h.setOperation(ctx, "PutBucketEncryption")
		w.WriteHeader(http.StatusOK)
	case r.URL.Query().Has("tagging"):
		WriteError(log, w, r, ErrNotImplemented)
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

	WriteError(log, w, r, ErrMethodNotAllowed)
}

func (h *S3Handler) routeBucketGet(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	q := r.URL.Query()
	switch {
	case q.Has("policy"):
		h.getBucketPolicy(ctx, w, r, bucket)
		return
	case q.Has("cors"):
		h.getBucketCORS(ctx, w, r, bucket)
		return
	}

	if h.routeBucketGetStubs(ctx, w, r) {
		return
	}

	log := logger.Load(ctx)

	switch {
	case r.URL.Query().Has("acl"):
		h.getBucketACL(ctx, w, r, bucket)
	case r.URL.Query().Has("versioning"):
		h.getBucketVersioning(ctx, w, r, bucket)
	case r.URL.Query().Has("versions"):
		h.listObjectVersions(ctx, w, r, bucket)
	case r.URL.Query().Has("uploads"):
		h.listMultipartUploads(ctx, w, r, bucket)
	case r.URL.Query().Has("location"):
		h.getBucketLocation(ctx, w, r, bucket)
	case r.URL.Query().Has("tagging"):
		WriteError(log, w, r, ErrNotImplemented)
	case r.URL.Query().Get("list-type") == "2":
		h.listObjectsV2(ctx, w, r, bucket)
	default:
		h.listObjects(ctx, w, r, bucket)
	}
}

// routeBucketGetStubs handles Terraform-compatible bucket sub-resource stub
// responses (always returns empty config or NoSuchX error). Returns true if the
// request was handled so the caller can skip further processing.
func (h *S3Handler) routeBucketGetStubs(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) bool {
	log := logger.Load(ctx)
	q := r.URL.Query()

	switch {
	case q.Has("notification"):
		h.setOperation(ctx, "GetBucketNotificationConfiguration")
		httputil.WriteXML(log, w, http.StatusOK, struct {
			XMLName xml.Name `xml:"NotificationConfiguration"`
		}{})
		h.setOperation(ctx, "GetBucketAccelerateConfiguration")
		httputil.WriteXML(log, w, http.StatusOK, struct {
			XMLName xml.Name `xml:"AccelerateConfiguration"`
			Xmlns   string   `xml:"xmlns,attr"`
		}{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"})
	case q.Has("cors"):
		h.setOperation(ctx, "GetBucketCors")
		httputil.WriteS3ErrorResponse(log, w, r, ErrorResponse{
			Code:    "NoSuchCORSConfiguration",
			Message: "The CORS configuration does not exist",
		}, http.StatusNotFound)
	case q.Has("website"):
		h.setOperation(ctx, "GetBucketWebsite")
		httputil.WriteS3ErrorResponse(log, w, r, ErrorResponse{
			Code:    "NoSuchWebsiteConfiguration",
			Message: "The specified bucket does not have a website configuration",
		}, http.StatusNotFound)
	case q.Has("logging"):
		h.setOperation(ctx, "GetBucketLogging")
		httputil.WriteXML(log, w, http.StatusOK, struct {
			XMLName xml.Name `xml:"BucketLoggingStatus"`
			Xmlns   string   `xml:"xmlns,attr"`
		}{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"})
	case q.Has("replication"):
		h.setOperation(ctx, "GetBucketReplication")
		httputil.WriteS3ErrorResponse(log, w, r, ErrorResponse{
			Code:    "ReplicationConfigurationNotFoundError",
			Message: "The replication configuration was not found",
		}, http.StatusNotFound)
	case q.Has("lifecycle"):
		h.setOperation(ctx, "GetBucketLifecycleConfiguration")
		httputil.WriteS3ErrorResponse(log, w, r, ErrorResponse{
			Code:    "NoSuchLifecycleConfiguration",
			Message: "The lifecycle configuration does not exist",
		}, http.StatusNotFound)
	case q.Has("object-lock"):
		h.setOperation(ctx, "GetObjectLockConfiguration")
		httputil.WriteS3ErrorResponse(log, w, r, ErrorResponse{
			Code:    "ObjectLockConfigurationNotFoundError",
			Message: "Object Lock configuration does not exist for this bucket",
		}, http.StatusNotFound)
	case q.Has("request-payment"):
		h.setOperation(ctx, "GetBucketRequestPayment")
		httputil.WriteXML(log, w, http.StatusOK, struct {
			XMLName xml.Name `xml:"RequestPaymentConfiguration"`
			Xmlns   string   `xml:"xmlns,attr"`
			Payer   string   `xml:"Payer"`
		}{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Payer: "BucketOwner"})
	case q.Has("encryption"):
		h.setOperation(ctx, "GetBucketEncryption")
		httputil.WriteS3ErrorResponse(log, w, r, ErrorResponse{
			Code:    "ServerSideEncryptionConfigurationNotFoundError",
			Message: "The server side encryption configuration was not found",
		}, http.StatusNotFound)
	case q.Has("intelligent-tiering"):
		h.setOperation(ctx, "ListBucketIntelligentTieringConfigurations")
		httputil.WriteXML(log, w, http.StatusOK, struct {
			XMLName                         xml.Name `xml:"ListBucketIntelligentTieringConfigurationsOutput"`
			Xmlns                           string   `xml:"xmlns,attr"`
			IntelligentTieringConfiguration []any    `xml:"IntelligentTieringConfiguration"`
			IsTruncated                     bool     `xml:"IsTruncated"`
		}{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"})
	default:
		return false
	}

	return true
}

func (h *S3Handler) listBuckets(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	h.setOperation(ctx, "ListBuckets")
	log := logger.Load(ctx)
	out, err := h.Backend.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		WriteError(log, w, r, err)

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
		WriteError(log, w, r, err)

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

	// If region not in body, try to get from context (extracted from Authorization header)
	if region == "" {
		if contextRegion, ok := ctx.Value(regionContextKey{}).(string); ok && contextRegion != "" {
			region = contextRegion
		}
	}

	// Default to us-east-1 if still empty
	if region == "" {
		region = defaultRegionName
	}

	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}
	if region != defaultRegionName {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		}
	}

	output, err := h.Backend.CreateBucket(ctx, input)
	if errors.Is(err, ErrBucketAlreadyOwnedByYou) {
		log.ErrorContext(ctx, "request failed", "error", err, "code", http.StatusConflict, "path", r.URL.Path)
		httputil.WriteS3ErrorResponse(log, w, r, ErrorResponse{
			Code:     "BucketAlreadyOwnedByYou",
			Message:  "Your previous request to create the named bucket succeeded and you already own it.",
			Resource: r.URL.Path,
		}, http.StatusConflict)

		return
	}

	if errors.Is(err, ErrBucketAlreadyExists) {
		log.ErrorContext(ctx, "request failed", "error", err, "code", http.StatusConflict, "path", r.URL.Path)
		httputil.WriteS3ErrorResponse(log, w, r, ErrorResponse{
			Code: "BucketAlreadyExists",
			Message: "The requested bucket name is not available. " +
				"The bucket namespace is shared by all users of the system. " +
				"Select a different name and try again.",
			Resource: r.URL.Path,
		}, http.StatusConflict)

		return
	}

	if err != nil {
		WriteError(log, w, r, err)

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
		WriteError(log, w, r, err)

		return
	}

	if errors.Is(err, ErrBucketNotEmpty) {
		WriteError(log, w, r, err)

		return
	}

	if err != nil {
		WriteError(log, w, r, err)

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
		WriteError(log, w, r, err)

		return
	}

	if err != nil {
		WriteError(log, w, r, err)

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

	// Get the region from context
	region := h.DefaultRegion
	if contextRegion, ok := ctx.Value(regionContextKey{}).(string); ok && contextRegion != "" {
		region = contextRegion
	}

	log := logger.Load(ctx)
	httputil.WriteXML(log, w, http.StatusOK, &LocationConstraintResponse{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		Region: region,
	})
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
		WriteError(log, w, r, ErrInvalidArgument)

		return
	}

	_, err := h.Backend.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatus(conf.Status),
		},
	})
	if err != nil {
		WriteError(log, w, r, err)

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
		WriteError(log, w, r, err)

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
		WriteError(log, w, r, err)

		return
	}

	if err != nil {
		WriteError(log, w, r, err)

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

func (h *S3Handler) putBucketACL(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "PutBucketAcl")
	log := logger.Load(ctx)

	acl := r.Header.Get("X-Amz-Acl")
	if acl == "" {
		acl = "private"
	}

	if err := h.Backend.PutBucketACL(ctx, bucketName, acl); err != nil {
		WriteError(log, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketACL(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "GetBucketAcl")
	log := logger.Load(ctx)

	_, err := h.Backend.GetBucketACL(ctx, bucketName)
	if err != nil {
		WriteError(log, w, r, err)

		return
	}

	resp := AccessControlPolicy{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: Owner{
			ID:          "gopherstack",
			DisplayName: "gopherstack",
		},
		ACL: AccessControlList{
			Grants: []Grant{
				{
					Grantee: Grantee{
						XmlnsXsi: "http://www.w3.org/2001/XMLSchema-instance",
						XsiType:  "CanonicalUser",
						ID:       "gopherstack",
					},
					Permission: "FULL_CONTROL",
				},
			},
		},
	}

	httputil.WriteXML(log, w, http.StatusOK, resp)
}

func (h *S3Handler) putBucketPolicy(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
h.setOperation(ctx, "PutBucketPolicy")
log := logger.Load(ctx)
body, err := httputil.ReadBody(r)
if err != nil {
WriteError(log, w, r, err)
return
}
if err := h.Backend.PutBucketPolicy(ctx, bucket, string(body)); err != nil {
WriteError(log, w, r, err)
return
}
w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) getBucketPolicy(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
h.setOperation(ctx, "GetBucketPolicy")
log := logger.Load(ctx)
policy, err := h.Backend.GetBucketPolicy(ctx, bucket)
if err != nil {
WriteError(log, w, r, err)
return
}
w.Header().Set("Content-Type", "application/json")
w.WriteHeader(http.StatusOK)
_, _ = w.Write([]byte(policy))
}

func (h *S3Handler) deleteBucketPolicy(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
h.setOperation(ctx, "DeleteBucketPolicy")
log := logger.Load(ctx)
if err := h.Backend.DeleteBucketPolicy(ctx, bucket); err != nil {
WriteError(log, w, r, err)
return
}
w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) putBucketCORS(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
h.setOperation(ctx, "PutBucketCors")
log := logger.Load(ctx)
body, err := httputil.ReadBody(r)
if err != nil {
WriteError(log, w, r, err)
return
}
if err := h.Backend.PutBucketCORS(ctx, bucket, string(body)); err != nil {
WriteError(log, w, r, err)
return
}
w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketCORS(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
h.setOperation(ctx, "GetBucketCors")
log := logger.Load(ctx)
corsXML, err := h.Backend.GetBucketCORS(ctx, bucket)
if err != nil {
WriteError(log, w, r, err)
return
}
w.Header().Set("Content-Type", "application/xml")
w.WriteHeader(http.StatusOK)
_, _ = w.Write([]byte(corsXML))
}

func (h *S3Handler) deleteBucketCORS(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
h.setOperation(ctx, "DeleteBucketCors")
log := logger.Load(ctx)
if err := h.Backend.DeleteBucketCORS(ctx, bucket); err != nil {
WriteError(log, w, r, err)
return
}
w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) handleCORSPreflight(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
h.setOperation(ctx, "CORSPreflight")
_, err := h.Backend.GetBucketCORS(ctx, bucket)
if err != nil {
w.WriteHeader(http.StatusForbidden)
return
}
origin := r.Header.Get("Origin")
method := r.Header.Get("Access-Control-Request-Method")
w.Header().Set("Access-Control-Allow-Origin", origin)
w.Header().Set("Access-Control-Allow-Methods", method)
w.Header().Set("Access-Control-Allow-Headers", r.Header.Get("Access-Control-Request-Headers"))
w.Header().Set("Access-Control-Max-Age", "3000")
w.WriteHeader(http.StatusOK)
}
