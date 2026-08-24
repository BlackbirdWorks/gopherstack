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
	LocationConstraint string               `xml:"LocationConstraint"`
	Tags               []createBucketTagXML `xml:"Tags>Tag"`
}

type createBucketTagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// s3BucketLoggingStatus is the XML response for GetBucketLogging (empty by default).
type s3BucketLoggingStatus struct {
	XMLName xml.Name `xml:"BucketLoggingStatus"`
	Xmlns   string   `xml:"xmlns,attr"`
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
		h.deleteBucketLifecycle(ctx, w, r, bucket)
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
		h.routeBucketDeleteExtra(ctx, w, r, bucket)
	}
}

func (h *S3Handler) routeBucketDeleteExtra(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	switch {
	case r.URL.Query().Has("analytics"):
		h.deleteBucketAnalyticsConfiguration(ctx, w, r, bucket)
	case r.URL.Query().Has("intelligent-tiering"):
		h.deleteBucketIntelligentTieringConfiguration(ctx, w, r, bucket)
	case r.URL.Query().Has("inventory"):
		h.deleteBucketInventoryConfiguration(ctx, w, r, bucket)
	case r.URL.Query().Has("metadataConfiguration"):
		h.deleteBucketMetadataConfiguration(ctx, w, r, bucket)
	case r.URL.Query().Has("metadataTable"):
		h.deleteBucketMetadataTableConfiguration(ctx, w, r, bucket)
	case r.URL.Query().Has("metrics"):
		h.deleteBucketMetricsConfiguration(ctx, w, r, bucket)
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
		if !h.routeBucketPutExtra(ctx, w, r, bucket) {
			h.createBucket(ctx, w, r, bucket)
		}
	}
}

// routeBucketPutExtra handles PUT sub-resources that are not in the primary switch.
// Returns true if the request was handled.
func (h *S3Handler) routeBucketPutExtra(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) bool {
	if h.routeBucketPutStorage(ctx, w, r, bucket) {
		return true
	}

	return h.routeBucketPutConfig(ctx, w, r, bucket)
}

// routeBucketPutStorage handles replication, encryption, access-control, and analytics PUT sub-resources.
func (h *S3Handler) routeBucketPutStorage(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) bool {
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
	case q.Has("analytics"):
		h.putBucketAnalyticsConfiguration(ctx, w, r, bucket)
	case q.Has("intelligent-tiering"):
		h.putBucketIntelligentTieringConfiguration(ctx, w, r, bucket)
	default:
		return false
	}

	return true
}

// routeBucketPutConfig handles inventory, metrics, and miscellaneous PUT sub-resources.
func (h *S3Handler) routeBucketPutConfig(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) bool {
	q := r.URL.Query()

	switch {
	case q.Has("inventory"):
		h.putBucketInventoryConfiguration(ctx, w, r, bucket)
	case q.Has("metrics"):
		h.putBucketMetricsConfiguration(ctx, w, r, bucket)
	case q.Has("accelerate"):
		h.handlePutBucketAccelerate(ctx, w, r)
	case q.Has("abac"):
		h.handlePutBucketAbac(ctx, w, r)
	case q.Has("requestPayment"):
		h.handlePutBucketRequestPayment(ctx, w, r)
	case q.Has("metadataInventoryTable"):
		h.handleUpdateBucketMetadataInventoryTableConfig(ctx, w, r)
	case q.Has("metadataJournalTable"):
		h.handleUpdateBucketMetadataJournalTableConfig(ctx, w, r)
	case q.Has("metadataAnnotationTable"):
		h.handleUpdateBucketMetadataAnnotationTableConfig(ctx, w, r)
	default:
		return false
	}

	return true
}

func (h *S3Handler) routeBucketPost(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	q := r.URL.Query()

	if q.Has("delete") {
		h.deleteObjects(ctx, w, r, bucket)

		return
	}

	// CreateBucketMetadataConfiguration and CreateBucketMetadataTableConfiguration
	// are POST, not PUT, per the pinned SDK (s3@v1.106.5 serializers.go:
	// awsRestxml_serializeOpCreateBucketMetadataConfiguration /
	// ...CreateBucketMetadataTableConfiguration both set request.Method = "POST").
	if q.Has("metadataConfiguration") {
		h.createBucketMetadataConfiguration(ctx, w, r, bucket)

		return
	}
	if q.Has("metadataTable") {
		h.createBucketMetadataTableConfiguration(ctx, w, r, bucket)

		return
	}

	// Browser-style POST upload: POST /bucket with multipart/form-data and a
	// `file` field. Matches LocalStack / real S3 presigned-POST semantics.
	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
		h.handlePostObject(ctx, w, r, bucket)

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

	if h.routeBucketGetExtra(ctx, w, r, bucket) {
		return
	}

	if h.routeBucketGetStubs(ctx, w, r, bucket) {
		return
	}

	h.routeBucketGetOrList(ctx, w, r, bucket)
}

// routeBucketGetExtra handles newer bucket GET sub-resources.
// Returns true if the request was handled.
func (h *S3Handler) routeBucketGetExtra(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) bool {
	q := r.URL.Query()

	switch {
	case q.Has("metadataConfiguration"):
		h.getBucketMetadataConfiguration(ctx, w, r, bucket)
	case q.Has("metadataTable"):
		h.getBucketMetadataTableConfiguration(ctx, w, r, bucket)
	case q.Has("session"):
		h.createSession(ctx, w, r, bucket)
	case q.Has("analytics"):
		if q.Has("id") {
			h.getBucketAnalyticsConfiguration(ctx, w, r, bucket)
		} else {
			h.listBucketAnalyticsConfigurations(ctx, w, r, bucket)
		}
	case q.Has("intelligent-tiering"):
		if q.Has("id") {
			h.getBucketIntelligentTieringConfiguration(ctx, w, r, bucket)
		} else {
			h.listBucketIntelligentTieringConfigurations(ctx, w, r, bucket)
		}
	case q.Has("inventory"):
		if q.Has("id") {
			h.getBucketInventoryConfiguration(ctx, w, r, bucket)
		} else {
			h.listBucketInventoryConfigurations(ctx, w, r, bucket)
		}
	case q.Has("metrics"):
		if q.Has("id") {
			h.getBucketMetricsConfiguration(ctx, w, r, bucket)
		} else {
			h.listBucketMetricsConfigurations(ctx, w, r, bucket)
		}
	default:
		return false
	}

	return true
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
	bucket string,
) bool {
	q := r.URL.Query()

	if q.Has("request-payment") || q.Has("requestPayment") {
		h.handleGetBucketRequestPayment(ctx, w, r)

		return true
	}

	return h.routeBucketGetStubsExtra(ctx, w, r, bucket)
}

func (h *S3Handler) listBuckets(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	h.setOperation(ctx, "ListBuckets")

	q := r.URL.Query()
	input := &s3.ListBucketsInput{}

	if ct := q.Get("continuation-token"); ct != "" {
		input.ContinuationToken = aws.String(ct)
	}

	if prefix := q.Get("prefix"); prefix != "" {
		input.Prefix = aws.String(prefix)
	}

	if mb := q.Get("max-buckets"); mb != "" {
		if n, convErr := strconv.ParseInt(mb, 10, 32); convErr == nil && n > 0 {
			input.MaxBuckets = aws.Int32(int32(n))
		}
	}

	out, err := h.Backend.ListBuckets(ctx, input)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	resp := ListAllMyBucketsResult{
		Owner: &Owner{
			ID:          gopherstackName,
			DisplayName: gopherstackName,
		},
		ContinuationToken: aws.ToString(out.ContinuationToken),
	}

	for _, b := range out.Buckets {
		if b.Name != nil && b.CreationDate != nil {
			resp.Buckets = append(resp.Buckets, BucketXML{
				Name:         *b.Name,
				CreationDate: b.CreationDate.Format(time.RFC3339),
				BucketRegion: aws.ToString(b.BucketRegion),
			})
		}
	}

	httputils.WriteXML(ctx, w, http.StatusOK, resp)
}

// parseCreateBucketRequest reads a CreateBucket request body and extracts the
// LocationConstraint and Tags carried in its CreateBucketConfiguration XML, if any. A malformed
// or absent body is not an error here -- region/tags are simply left at their zero values and
// resolved by the caller from other sources (region) or omitted (tags).
func parseCreateBucketRequest(ctx context.Context, r *http.Request) (string, []types.Tag, error) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		return "", nil, err
	}

	if len(body) == 0 {
		return "", nil, nil
	}

	var bucketConfig createBucketConfiguration
	if xmlErr := xml.Unmarshal(body, &bucketConfig); xmlErr != nil {
		logger.Load(ctx).WarnContext(ctx, "failed to parse CreateBucketConfiguration", "error", xmlErr)

		return "", nil, nil
	}

	tags := make([]types.Tag, 0, len(bucketConfig.Tags))
	for _, t := range bucketConfig.Tags {
		tags = append(tags, types.Tag{Key: aws.String(t.Key), Value: aws.String(t.Value)})
	}

	return bucketConfig.LocationConstraint, tags, nil
}

// writeCreateBucketError writes the appropriate S3 error response for a CreateBucket failure.
// Returns true if err was handled (a response was written), false if err is nil.
func writeCreateBucketError(ctx context.Context, w http.ResponseWriter, r *http.Request, err error) bool {
	switch {
	case errors.Is(err, ErrBucketAlreadyOwnedByYou):
		logger.Load(ctx).
			ErrorContext(ctx, "request failed", "error", err, "code", http.StatusConflict, "path", r.URL.Path)
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:     "BucketAlreadyOwnedByYou",
			Message:  "Your previous request to create the named bucket succeeded and you already own it.",
			Resource: r.URL.Path,
		}, http.StatusConflict)

		return true
	case errors.Is(err, ErrBucketAlreadyExists):
		logger.Load(ctx).
			ErrorContext(ctx, "request failed", "error", err, "code", http.StatusConflict, "path", r.URL.Path)
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code: "BucketAlreadyExists",
			Message: "The requested bucket name is not available. " +
				"The bucket namespace is shared by all users of the system. " +
				"Select a different name and try again.",
			Resource: r.URL.Path,
		}, http.StatusConflict)

		return true
	case err != nil:
		WriteError(ctx, w, r, err)

		return true
	default:
		return false
	}
}

func (h *S3Handler) createBucket(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "CreateBucket")
	logger.Load(ctx).DebugContext(ctx, "S3 createBucket input", "bucket", bucketName)

	region, tags, err := parseCreateBucketRequest(ctx, r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
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
	if region != defaultRegionName || len(tags) > 0 {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			Tags: tags,
		}
		if region != defaultRegionName {
			input.CreateBucketConfiguration.LocationConstraint = types.BucketLocationConstraint(region)
		}
	}

	output, err := h.Backend.CreateBucket(ctx, input)
	if writeCreateBucketError(ctx, w, r, err) {
		return
	}

	logger.Load(ctx).
		DebugContext(ctx, "S3 createBucket output", "bucket", bucketName, "region", region)

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

	// Object Lambda access-point config lives on the handler (not the backend
	// table), keyed by bucket name. Clear it so a future bucket recreated
	// under the same name doesn't inherit a stale Lambda wiring left over
	// from the deleted bucket's identity.
	h.clearObjectLambdaConfig(bucketName)

	logger.Load(ctx).DebugContext(ctx, "S3 deleteBucket output", "bucket", bucketName)

	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) getBucketLocation(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketLocation")

	region, _, _, err := h.Backend.GetBucketMetadata(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	// AWS returns an empty LocationConstraint for us-east-1 buckets (the
	// "classic" region that predates LocationConstraint). All other regions
	// echo their constraint string.
	if region == defaultRegionName {
		region = ""
	}

	httputils.WriteXML(ctx, w, http.StatusOK, &LocationConstraintResponse{
		Xmlns:  xmlNamespaceS3,
		Region: region,
	})
}

func (h *S3Handler) headBucket(
	ctx context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "HeadBucket")
	region, _, _, err := h.Backend.GetBucketMetadata(ctx, bucketName)
	if errors.Is(err, ErrNoSuchBucket) {
		w.WriteHeader(http.StatusNotFound)

		return
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	if region != "" {
		w.Header().Set("X-Amz-Bucket-Region", region)
	}
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) createSession(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "CreateSession")
	sessionXML, err := h.Backend.CreateSession(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(sessionXML))
}
