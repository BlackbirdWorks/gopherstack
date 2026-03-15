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

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// createBucketConfiguration is the XML body of a CreateBucket request.
type createBucketConfiguration struct {
	LocationConstraint string `xml:"LocationConstraint"`
}

// s3BucketLoggingStatus is the XML response for GetBucketLogging (empty by default).
type s3BucketLoggingStatus struct {
	XMLName xml.Name `xml:"BucketLoggingStatus"`
	Xmlns   string   `xml:"xmlns,attr"`
}

// s3RequestPaymentConfiguration is the XML response for GetBucketRequestPayment.
type s3RequestPaymentConfiguration struct {
	XMLName xml.Name `xml:"RequestPaymentConfiguration"`
	Xmlns   string   `xml:"xmlns,attr"`
	Payer   string   `xml:"Payer"`
}

// s3ListIntelligentTieringOutput is the XML response for ListBucketIntelligentTieringConfigurations.
type s3ListIntelligentTieringOutput struct {
	XMLName                         xml.Name `xml:"ListBucketIntelligentTieringConfigurationsOutput"`
	Xmlns                           string   `xml:"xmlns,attr"`
	IntelligentTieringConfiguration []any    `xml:"IntelligentTieringConfiguration"`
	IsTruncated                     bool     `xml:"IsTruncated"`
}

// s3NotificationConfiguration is the XML response for GetBucketNotificationConfiguration (empty).
type s3NotificationConfiguration struct {
	XMLName xml.Name `xml:"NotificationConfiguration"`
}

func (h *S3Handler) handleBucketOperation(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	switch r.Method {
	case http.MethodPut:
		h.routeBucketPut(ctx, w, r, bucket)
	case http.MethodDelete:
		h.routeBucketDelete(ctx, w, r, bucket)
	case http.MethodGet:
		h.routeBucketGet(ctx, w, r, bucket)
	case http.MethodPost:
		h.routeBucketPost(ctx, w, r, bucket)
	case http.MethodHead:
		h.headBucket(ctx, w, r, bucket)
	case http.MethodOptions:
		h.handleCORSPreflight(ctx, w, r, bucket)
	default:
		WriteError(ctx, w, r, ErrMethodNotAllowed)
	}
}

func (h *S3Handler) routeBucketDelete(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	switch {
	case r.URL.Query().Has("policy"):
		h.deleteBucketPolicy(ctx, w, r, bucket)
	case r.URL.Query().Has("cors"):
		h.deleteBucketCORS(ctx, w, r, bucket)
	case r.URL.Query().Has("lifecycle"):
		h.deleteBucketLifecycleConfiguration(ctx, w, r, bucket)
	case r.URL.Query().Has("website"):
		h.deleteBucketWebsite(ctx, w, r, bucket)
	case r.URL.Query().Has("encryption"):
		h.deleteBucketEncryption(ctx, w, r, bucket)
	case r.URL.Query().Has("publicAccessBlock"):
		h.deletePublicAccessBlock(ctx, w, r, bucket)
	case r.URL.Query().Has("ownershipControls"):
		h.deleteBucketOwnershipControls(ctx, w, r, bucket)
	case r.URL.Query().Has("replication"):
		h.deleteBucketReplication(ctx, w, r, bucket)
	case r.URL.Query().Has("tagging"):
		h.deleteBucketTagging(ctx, w, r, bucket)
	default:
		h.deleteBucket(ctx, w, r, bucket)
	}
}

func (h *S3Handler) routeBucketPut(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	q := r.URL.Query()

	switch {
	case q.Has("acl"):
		h.putBucketACL(ctx, w, r, bucket)
	case q.Has("versioning"):
		h.putBucketVersioning(ctx, w, r, bucket)
	case q.Has("notification"):
		h.putBucketNotificationConfiguration(ctx, w, r, bucket)
	case q.Has("policy"):
		h.putBucketPolicy(ctx, w, r, bucket)
	case q.Has("cors"):
		h.putBucketCORS(ctx, w, r, bucket)
	case q.Has("website"):
		h.putBucketWebsite(ctx, w, r, bucket)
	case q.Has("lifecycle"):
		h.putBucketLifecycleConfiguration(ctx, w, r, bucket)
	case q.Has("tagging"):
		h.putBucketTagging(ctx, w, r, bucket)
	default:
		h.routeBucketPutExtra(ctx, w, r, bucket)
	}
}

func (h *S3Handler) routeBucketPutExtra(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	q := r.URL.Query()

	switch {
	case q.Has("replication"):
		h.putBucketReplication(ctx, w, r, bucket)
	case q.Has("encryption"):
		h.putBucketEncryption(ctx, w, r, bucket)
	case q.Has("object-lock"):
		h.putObjectLockConfiguration(ctx, w, r, bucket)
	case q.Has("publicAccessBlock"):
		h.putPublicAccessBlock(ctx, w, r, bucket)
	case q.Has("ownershipControls"):
		h.putBucketOwnershipControls(ctx, w, r, bucket)
	case q.Has("logging"):
		h.putBucketLogging(ctx, w, r, bucket)
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
	if r.URL.Query().Has("delete") {
		h.deleteObjects(ctx, w, r, bucket)

		return
	}

	WriteError(ctx, w, r, ErrMethodNotAllowed)
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
	case q.Has("notification"):
		h.getBucketNotificationConfiguration(ctx, w, r, bucket)

		return
	case q.Has("lifecycle"):
		h.getBucketLifecycleConfiguration(ctx, w, r, bucket)

		return
	case q.Has("website"):
		h.getBucketWebsite(ctx, w, r, bucket)

		return
	case q.Has("encryption"):
		h.getBucketEncryption(ctx, w, r, bucket)

		return
	case q.Has("object-lock"):
		h.getObjectLockConfiguration(ctx, w, r, bucket)

		return
	case q.Has("publicAccessBlock"):
		h.getPublicAccessBlock(ctx, w, r, bucket)

		return
	case q.Has("ownershipControls"):
		h.getBucketOwnershipControls(ctx, w, r, bucket)

		return
	case q.Has("replication"):
		h.getBucketReplication(ctx, w, r, bucket)

		return
	case q.Has("logging"):
		h.getBucketLogging(ctx, w, r, bucket)

		return
	}

	if h.routeBucketGetStubs(ctx, w, r) {
		return
	}

	h.routeBucketGetOrList(ctx, w, r, bucket)
}

// routeBucketGetOrList handles ACL, versioning, listing, and other bucket GET requests.
func (h *S3Handler) routeBucketGetOrList(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
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
		h.getBucketTagging(ctx, w, r, bucket)
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
	q := r.URL.Query()

	switch {
	case q.Has("request-payment"):
		h.setOperation(ctx, "GetBucketRequestPayment")
		httputils.WriteXML(
			ctx,
			w,
			http.StatusOK,
			s3RequestPaymentConfiguration{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Payer: "BucketOwner"},
		)
	case q.Has("intelligent-tiering"):
		h.setOperation(ctx, "ListBucketIntelligentTieringConfigurations")
		httputils.WriteXML(
			ctx,
			w,
			http.StatusOK,
			s3ListIntelligentTieringOutput{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"},
		)
	default:
		return false
	}

	return true
}

func (h *S3Handler) listBuckets(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	h.setOperation(ctx, "ListBuckets")
	out, err := h.Backend.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		WriteError(ctx, w, r, err)

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

	httputils.WriteXML(ctx, w, http.StatusOK, resp)
}

func (h *S3Handler) createBucket(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "CreateBucket")
	logger.Load(ctx).DebugContext(ctx, "S3 createBucket input", "bucket", bucketName)

	var region string
	// Read the body to check for LocationConstraint
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	if len(body) > 0 {
		var bucketConfig createBucketConfiguration
		if xmlErr := xml.Unmarshal(body, &bucketConfig); xmlErr == nil {
			region = bucketConfig.LocationConstraint
		} else {
			logger.Load(ctx).WarnContext(ctx, "failed to parse CreateBucketConfiguration", "error", xmlErr)
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
		logger.Load(ctx).
			ErrorContext(ctx, "request failed", "error", err, "code", http.StatusConflict, "path", r.URL.Path)
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:     "BucketAlreadyOwnedByYou",
			Message:  "Your previous request to create the named bucket succeeded and you already own it.",
			Resource: r.URL.Path,
		}, http.StatusConflict)

		return
	}

	if errors.Is(err, ErrBucketAlreadyExists) {
		logger.Load(ctx).
			ErrorContext(ctx, "request failed", "error", err, "code", http.StatusConflict, "path", r.URL.Path)
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code: "BucketAlreadyExists",
			Message: "The requested bucket name is not available. " +
				"The bucket namespace is shared by all users of the system. " +
				"Select a different name and try again.",
			Resource: r.URL.Path,
		}, http.StatusConflict)

		return
	}

	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	logger.Load(ctx).DebugContext(ctx, "S3 createBucket output", "bucket", bucketName, "region", region)

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
	logger.Load(ctx).DebugContext(ctx, "S3 deleteBucket input", "bucket", bucketName)

	_, err := h.Backend.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})
	if errors.Is(err, ErrNoSuchBucket) {
		WriteError(ctx, w, r, err)

		return
	}

	if errors.Is(err, ErrBucketNotEmpty) {
		WriteError(ctx, w, r, err)

		return
	}

	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	logger.Load(ctx).DebugContext(ctx, "S3 deleteBucket output", "bucket", bucketName)

	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) listObjects(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "ListObjects")
	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	marker := r.URL.Query().Get("marker")

	logger.Load(ctx).DebugContext(ctx,
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
		WriteError(ctx, w, r, err)

		return
	}

	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	objects := out.Contents

	// Apply marker: skip all keys <= marker
	if marker != "" {
		objects = objects[findFirstIndexAfterMarker(objects, marker):]
	}

	isTruncated := false
	var nextMarker string
	if maxKeys > 0 && len(objects) > int(maxKeys) {
		isTruncated = true
		objects = objects[:maxKeys]
		nextMarker = *objects[maxKeys-1].Key
	}

	logger.Load(ctx).DebugContext(ctx,
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
		objects,
		prefix,
		delimiter,
		seenPrefixes,
	)
	resp.KeyCount = len(resp.Contents)

	httputils.WriteXML(ctx, w, http.StatusOK, resp)
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

	httputils.WriteXML(ctx, w, http.StatusOK, &LocationConstraintResponse{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		Region: region,
	})
}

func (h *S3Handler) mapObjectsToXML(
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

		checksumAlgo := ""
		if len(obj.ChecksumAlgorithm) > 0 {
			checksumAlgo = string(obj.ChecksumAlgorithm[0])
		}

		contents = append(contents, ObjectXML{
			Key:               key,
			LastModified:      obj.LastModified.Format(time.RFC3339),
			Size:              *obj.Size,
			ETag:              aws.ToString(obj.ETag),
			StorageClass:      storageStandard,
			ChecksumAlgorithm: checksumAlgo,
		})
	}

	return contents, commonPrefixes
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
	var conf VersioningConfiguration
	if err := xml.NewDecoder(r.Body).Decode(&conf); err != nil {
		WriteError(ctx, w, r, ErrInvalidArgument)

		return
	}

	_, err := h.Backend.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatus(conf.Status),
		},
	})
	if err != nil {
		WriteError(ctx, w, r, err)

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
	out, err := h.Backend.GetBucketVersioning(
		ctx,
		&s3.GetBucketVersioningInput{Bucket: aws.String(bucketName)},
	)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	status := ""
	if out.Status != "" {
		status = string(out.Status)
	}

	httputils.WriteXML(ctx, w, http.StatusOK, VersioningConfiguration{
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
	prefix := r.URL.Query().Get("prefix")

	out, err := h.Backend.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	})
	if errors.Is(err, ErrNoSuchBucket) {
		WriteError(ctx, w, r, err)

		return
	}

	if err != nil {
		WriteError(ctx, w, r, err)

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

	httputils.WriteXML(ctx, w, http.StatusOK, resp)
}

func (h *S3Handler) putBucketACL(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "PutBucketAcl")

	acl := r.Header.Get("X-Amz-Acl")
	if acl == "" {
		acl = "private"
	}

	if err := h.Backend.PutBucketACL(ctx, bucketName, acl); err != nil {
		WriteError(ctx, w, r, err)

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

	_, err := h.Backend.GetBucketACL(ctx, bucketName)
	if err != nil {
		WriteError(ctx, w, r, err)

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

	httputils.WriteXML(ctx, w, http.StatusOK, resp)
}

func (h *S3Handler) putBucketPolicy(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "PutBucketPolicy")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	err = h.Backend.PutBucketPolicy(ctx, bucket, string(body))
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) getBucketPolicy(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "GetBucketPolicy")
	policy, err := h.Backend.GetBucketPolicy(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(policy))
}

func (h *S3Handler) deleteBucketPolicy(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "DeleteBucketPolicy")
	if err := h.Backend.DeleteBucketPolicy(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) putBucketCORS(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "PutBucketCors")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	// Validate the CORS XML is well-formed before storing it.
	var cfg CORSConfiguration
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "MalformedXML",
			Message: "The XML you provided was not well-formed or did not validate against our published schema.",
		}, http.StatusBadRequest)

		return
	}

	err = h.Backend.PutBucketCORS(ctx, bucket, string(body))
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketCORS(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "GetBucketCors")
	corsXML, err := h.Backend.GetBucketCORS(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(corsXML))
}

func (h *S3Handler) deleteBucketCORS(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "DeleteBucketCors")
	if err := h.Backend.DeleteBucketCORS(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) putBucketWebsite(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "PutBucketWebsite")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	// Validate the website XML is well-formed before storing it.
	var cfg WebsiteConfiguration
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "MalformedXML",
			Message: "The XML you provided was not well-formed or did not validate against our published schema.",
		}, http.StatusBadRequest)

		return
	}

	if err = h.Backend.PutBucketWebsite(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketWebsite(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "GetBucketWebsite")
	websiteXML, err := h.Backend.GetBucketWebsite(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(websiteXML))
}

func (h *S3Handler) deleteBucketWebsite(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "DeleteBucketWebsite")
	if err := h.Backend.DeleteBucketWebsite(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) putBucketEncryption(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "PutBucketEncryption")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	var cfg ServerSideEncryptionConfiguration
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "MalformedXML",
			Message: "The XML you provided was not well-formed or did not validate against our published schema.",
		}, http.StatusBadRequest)

		return
	}

	if err = h.Backend.PutBucketEncryption(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketEncryption(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "GetBucketEncryption")
	encryptionXML, err := h.Backend.GetBucketEncryption(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(encryptionXML))
}

func (h *S3Handler) deleteBucketEncryption(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "DeleteBucketEncryption")
	if err := h.Backend.DeleteBucketEncryption(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) putPublicAccessBlock(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "PutPublicAccessBlock")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	var cfg PublicAccessBlockConfiguration
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "MalformedXML",
			Message: "The XML you provided was not well-formed or did not validate against our published schema.",
		}, http.StatusBadRequest)

		return
	}

	if err = h.Backend.PutPublicAccessBlock(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getPublicAccessBlock(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "GetPublicAccessBlock")
	configXML, err := h.Backend.GetPublicAccessBlock(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(configXML))
}

func (h *S3Handler) deletePublicAccessBlock(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeletePublicAccessBlock")
	if err := h.Backend.DeletePublicAccessBlock(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) putBucketOwnershipControls(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutBucketOwnershipControls")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	var cfg OwnershipControls
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "MalformedXML",
			Message: "The XML you provided was not well-formed or did not validate against our published schema.",
		}, http.StatusBadRequest)

		return
	}

	if err = h.Backend.PutBucketOwnershipControls(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketOwnershipControls(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketOwnershipControls")
	configXML, err := h.Backend.GetBucketOwnershipControls(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(configXML))
}

func (h *S3Handler) deleteBucketOwnershipControls(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeleteBucketOwnershipControls")
	if err := h.Backend.DeleteBucketOwnershipControls(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) putBucketLogging(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "PutBucketLogging")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	var cfg BucketLoggingStatus
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "MalformedXML",
			Message: "The XML you provided was not well-formed or did not validate against our published schema.",
		}, http.StatusBadRequest)

		return
	}

	if err = h.Backend.PutBucketLogging(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketLogging(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "GetBucketLogging")
	loggingXML, err := h.Backend.GetBucketLogging(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	if loggingXML == "" {
		httputils.WriteXML(
			ctx,
			w,
			http.StatusOK,
			s3BucketLoggingStatus{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"},
		)

		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(loggingXML))
}

func (h *S3Handler) putBucketReplication(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "PutBucketReplication")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	var cfg ReplicationConfiguration
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "MalformedXML",
			Message: "The XML you provided was not well-formed or did not validate against our published schema.",
		}, http.StatusBadRequest)

		return
	}

	if err = h.Backend.PutBucketReplication(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketReplication(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "GetBucketReplication")
	replicationXML, err := h.Backend.GetBucketReplication(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(replicationXML))
}

func (h *S3Handler) deleteBucketReplication(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeleteBucketReplication")
	if err := h.Backend.DeleteBucketReplication(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) putBucketTagging(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "PutBucketTagging")

	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	var tagging Tagging
	if xmlErr := xml.Unmarshal(body, &tagging); xmlErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "MalformedXML",
			Message: "The XML you provided was not well-formed or did not validate against our published schema.",
		}, http.StatusBadRequest)

		return
	}

	tags := make([]types.Tag, 0, len(tagging.TagSet.Tags))
	for _, t := range tagging.TagSet.Tags {
		tags = append(tags, types.Tag{
			Key:   aws.String(t.Key),
			Value: aws.String(t.Value),
		})
	}

	if putErr := h.Backend.PutBucketTagging(ctx, bucket, tags); putErr != nil {
		WriteError(ctx, w, r, putErr)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) getBucketTagging(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "GetBucketTagging")

	tags, err := h.Backend.GetBucketTagging(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	tagging := Tagging{}
	for _, t := range tags {
		tagging.TagSet.Tags = append(tagging.TagSet.Tags, Tag{
			Key:   aws.ToString(t.Key),
			Value: aws.ToString(t.Value),
		})
	}

	httputils.WriteXML(ctx, w, http.StatusOK, tagging)
}

func (h *S3Handler) deleteBucketTagging(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "DeleteBucketTagging")

	if err := h.Backend.DeleteBucketTagging(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) handleCORSPreflight(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	h.setOperation(ctx, "CORSPreflight")
	corsXML, err := h.Backend.GetBucketCORS(ctx, bucket)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)

		return
	}

	var cfg CORSConfiguration
	if unmarshalErr := xml.Unmarshal([]byte(corsXML), &cfg); unmarshalErr != nil {
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	origin := r.Header.Get("Origin")
	method := r.Header.Get("Access-Control-Request-Method")

	// Reject structurally invalid preflights: Origin and Access-Control-Request-Method
	// are required for a well-formed CORS preflight request.
	if origin == "" || method == "" {
		w.WriteHeader(http.StatusForbidden)

		return
	}

	reqHeaders := r.Header.Get("Access-Control-Request-Headers")

	rule := matchCORSRule(cfg.Rules, origin, method, reqHeaders)
	if rule == nil {
		w.WriteHeader(http.StatusForbidden)

		return
	}

	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Methods", method)

	if reqHeaders != "" {
		w.Header().Set("Access-Control-Allow-Headers", reqHeaders)
	}

	if rule.MaxAgeSeconds > 0 {
		w.Header().Set("Access-Control-Max-Age", strconv.Itoa(rule.MaxAgeSeconds))
	} else {
		w.Header().Set("Access-Control-Max-Age", "3000")
	}

	w.WriteHeader(http.StatusOK)
}

// matchCORSRule returns the first CORSRule whose AllowedOrigins, AllowedMethods,
// and AllowedHeaders all match the supplied preflight parameters.
// Returns nil when no rule matches.
func matchCORSRule(rules []CORSRule, origin, method, reqHeaders string) *CORSRule {
	for i := range rules {
		rule := &rules[i]

		if !corsOriginMatches(rule.AllowedOrigins, origin) {
			continue
		}

		if !corsMethodMatches(rule.AllowedMethods, method) {
			continue
		}

		if !corsHeadersMatch(rule.AllowedHeaders, reqHeaders) {
			continue
		}

		return rule
	}

	return nil
}

// corsOriginMatches returns true when origin matches one of the allowedOrigins.
// A wildcard entry "*" matches any origin.
func corsOriginMatches(allowedOrigins []string, origin string) bool {
	for _, allowed := range allowedOrigins {
		if allowed == "*" || strings.EqualFold(allowed, origin) {
			return true
		}
	}

	return false
}

// corsMethodMatches returns true when method is found in allowedMethods.
func corsMethodMatches(allowedMethods []string, method string) bool {
	for _, allowed := range allowedMethods {
		if strings.EqualFold(allowed, method) {
			return true
		}
	}

	return false
}

// corsHeadersMatch returns true when every header listed in reqHeaders (comma-separated)
// is covered by allowedHeaders. A wildcard entry "*" covers any header.
// An empty reqHeaders string is always allowed.
func corsHeadersMatch(allowedHeaders []string, reqHeaders string) bool {
	if reqHeaders == "" {
		return true
	}

	for rh := range strings.SplitSeq(reqHeaders, ",") {
		rh = strings.TrimSpace(rh)
		if rh == "" {
			continue
		}

		matched := false

		for _, ah := range allowedHeaders {
			if ah == "*" || strings.EqualFold(ah, rh) {
				matched = true

				break
			}
		}

		if !matched {
			return false
		}
	}

	return true
}

func (h *S3Handler) putBucketLifecycleConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutBucketLifecycleConfiguration")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	err = h.Backend.PutBucketLifecycleConfiguration(ctx, bucket, string(body))
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) getBucketLifecycleConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketLifecycleConfiguration")
	lifecycleXML, err := h.Backend.GetBucketLifecycleConfiguration(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(lifecycleXML))
}

func (h *S3Handler) deleteBucketLifecycleConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeleteBucketLifecycleConfiguration")
	if err := h.Backend.DeleteBucketLifecycleConfiguration(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) putBucketNotificationConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutBucketNotificationConfiguration")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	err = h.Backend.PutBucketNotificationConfiguration(ctx, bucket, string(body))
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketNotificationConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketNotificationConfiguration")
	notifXML, err := h.Backend.GetBucketNotificationConfiguration(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	if notifXML == "" {
		// Return empty notification config
		httputils.WriteXML(ctx, w, http.StatusOK, s3NotificationConfiguration{})

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(notifXML))
}

func (h *S3Handler) putObjectLockConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutObjectLockConfiguration")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	if err = h.Backend.PutObjectLockConfiguration(ctx, bucket, string(body)); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getObjectLockConfiguration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetObjectLockConfiguration")
	configXML, err := h.Backend.GetObjectLockConfiguration(ctx, bucket)

	if errors.Is(err, ErrNoObjectLockConfig) {
		WriteError(ctx, w, r, err)

		return
	}

	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(configXML))
}

// findFirstIndexAfterMarker returns the index of the first object whose key
// is strictly greater than marker. Returns len(objects) if no such object exists.
func findFirstIndexAfterMarker(objects []types.Object, marker string) int {
	for i, obj := range objects {
		if *obj.Key > marker {
			return i
		}
	}

	return len(objects)
}
