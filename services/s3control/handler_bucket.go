package s3control

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	pathBucketPrefix    = "/v20180820/bucket/"
	pathRegionalBuckets = "/v20180820/bucket"
	// pathBucketLifecycleSuffix is the real SDK's lifecycle sub-resource
	// suffix; NOT "/lifecycle".
	pathBucketLifecycleSuffix = "/lifecycleconfiguration"
)

// extractBucketCRUDOps handles bucket CRUD, lifecycle, and policy operations.
func extractBucketCRUDOps(path, method string) string {
	if isSimplePath(pathBucketPrefix, path) {
		switch method {
		case http.MethodPut:
			return "CreateBucket"
		case http.MethodGet:
			return "GetBucket"
		case http.MethodDelete:
			return "DeleteBucket"
		}

		return ""
	}

	if isPrefixSuffix(pathBucketPrefix, path, pathBucketLifecycleSuffix) {
		switch method {
		case http.MethodGet:
			return "GetBucketLifecycleConfiguration"
		case http.MethodPut:
			return "PutBucketLifecycleConfiguration"
		case http.MethodDelete:
			return "DeleteBucketLifecycleConfiguration"
		}

		return ""
	}

	if isPrefixSuffix(pathBucketPrefix, path, "/policy") {
		switch method {
		case http.MethodGet:
			return "GetBucketPolicy"
		case http.MethodPut:
			return "PutBucketPolicy"
		case http.MethodDelete:
			return "DeleteBucketPolicy"
		}
	}

	return ""
}

// extractBucketSubResourceOps handles bucket replication, tagging, versioning, and listing.
func extractBucketSubResourceOps(path, method string) string {
	if op := extractBucketReplicationTaggingOp(path, method); op != "" {
		return op
	}

	if isPrefixSuffix(pathBucketPrefix, path, "/versioning") {
		switch method {
		case http.MethodGet:
			return "GetBucketVersioning"
		case http.MethodPut:
			return "PutBucketVersioning"
		}
	}

	if path == pathRegionalBuckets && method == http.MethodGet {
		return "ListRegionalBuckets"
	}

	return ""
}

// extractBucketReplicationTaggingOp handles bucket replication and tagging operations.
func extractBucketReplicationTaggingOp(path, method string) string {
	switch {
	case isPrefixSuffix(pathBucketPrefix, path, "/replication") && method == http.MethodGet:
		return "GetBucketReplication"
	case isPrefixSuffix(pathBucketPrefix, path, "/replication") && method == http.MethodPut:
		return "PutBucketReplication"
	case isPrefixSuffix(pathBucketPrefix, path, "/replication") && method == http.MethodDelete:
		return "DeleteBucketReplication"
	case isPrefixSuffix(pathBucketPrefix, path, "/tagging") && method == http.MethodGet:
		return "GetBucketTagging"
	case isPrefixSuffix(pathBucketPrefix, path, "/tagging") && method == http.MethodPut:
		return "PutBucketTagging"
	case isPrefixSuffix(pathBucketPrefix, path, "/tagging") && method == http.MethodDelete:
		return "DeleteBucketTagging"
	}

	return ""
}

// dispatchBucketCRUDStubs handles bucket CRUD, lifecycle, and policy stub operations.
func (h *Handler) dispatchBucketCRUDStubs(c *echo.Context, path, method string) (bool, error) {
	switch {
	case isSimplePath(pathBucketPrefix, path):
		return h.dispatchBucketBaseMethod(c, method)
	case isPrefixSuffix(pathBucketPrefix, path, pathBucketLifecycleSuffix):
		return h.dispatchBucketLifecycleMethod(c, method)
	case isPrefixSuffix(pathBucketPrefix, path, "/policy"):
		return h.dispatchBucketPolicyMethod(c, method)
	}

	return false, nil
}

func (h *Handler) dispatchBucketBaseMethod(c *echo.Context, method string) (bool, error) {
	switch method {
	case http.MethodPut:
		return true, h.handleCreateBucket(c)
	case http.MethodGet:
		return true, h.handleGetBucket(c)
	case http.MethodDelete:
		return true, h.handleDeleteBucket(c)
	}

	return false, nil
}

func (h *Handler) dispatchBucketLifecycleMethod(c *echo.Context, method string) (bool, error) {
	switch method {
	case http.MethodGet:
		return true, h.handleGetBucketLifecycleConfiguration(c)
	case http.MethodPut:
		return true, h.handlePutBucketLifecycleConfiguration(c)
	case http.MethodDelete:
		return true, h.handleDeleteBucketLifecycleConfiguration(c)
	}

	return false, nil
}

func (h *Handler) dispatchBucketPolicyMethod(c *echo.Context, method string) (bool, error) {
	switch method {
	case http.MethodGet:
		return true, h.handleGetBucketPolicy(c)
	case http.MethodPut:
		return true, h.handlePutBucketPolicy(c)
	case http.MethodDelete:
		return true, h.handleDeleteBucketPolicy(c)
	}

	return false, nil
}

// dispatchBucketSubResourceStubs handles bucket replication, tagging, versioning, and listing stubs.
func (h *Handler) dispatchBucketSubResourceStubs(c *echo.Context, path, method string) (bool, error) {
	if isPrefixSuffix(pathBucketPrefix, path, "/replication") {
		switch method {
		case http.MethodGet:
			return true, h.handleGetBucketReplication(c)
		case http.MethodPut:
			return true, h.handlePutBucketReplication(c)
		case http.MethodDelete:
			return true, h.handleDeleteBucketReplication(c)
		}

		return false, nil
	}

	return h.dispatchBucketTagVersionStubs(c, path, method)
}

// dispatchBucketTagVersionStubs handles bucket tagging, versioning, and listing stubs.
func (h *Handler) dispatchBucketTagVersionStubs(c *echo.Context, path, method string) (bool, error) {
	if isPrefixSuffix(pathBucketPrefix, path, "/tagging") {
		switch method {
		case http.MethodGet:
			return true, h.handleGetBucketTagging(c)
		case http.MethodPut:
			return true, h.handlePutBucketTagging(c)
		case http.MethodDelete:
			return true, h.handleDeleteBucketTagging(c)
		}

		return false, nil
	}

	if isPrefixSuffix(pathBucketPrefix, path, "/versioning") {
		switch method {
		case http.MethodGet:
			return true, h.handleGetBucketVersioning(c)
		case http.MethodPut:
			return true, h.handlePutBucketVersioning(c)
		}

		return false, nil
	}

	if path == pathRegionalBuckets && method == http.MethodGet {
		return true, h.handleListRegionalBuckets(c)
	}

	return false, nil
}

// --- CreateBucket handler ---

type createBucketResponseXML struct {
	XMLName   xml.Name `xml:"CreateBucketResult"`
	BucketArn string   `xml:"BucketArn"`
}

func (h *Handler) handleCreateBucket(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix)

	bkt := h.Backend.CreateBucket(accountID, bucketName)

	c.Response().Header().Set("Location", bkt.Location)

	return writeXML(c, createBucketResponseXML{
		BucketArn: bkt.BucketArn,
	})
}

// ---- Outposts Bucket ----

// handleGetBucket. GetBucketOutput's real fields are Bucket, CreationDate,
// and PublicAccessBlockEnabled -- it has NO BucketArn or OutpostId field at
// all (confirmed via aws-sdk-go-v2/service/s3control's GetBucketOutput and
// its deserializer, which only recognizes those three elements). A
// previous version of this handler fabricated a BucketArn element and
// mislabeled the internal "Location" value (an HTTP Location-header path
// fragment, not an Outpost ID -- see CreateBucket/bucket.go) as OutpostId.
// CreationDate/PublicAccessBlockEnabled are omitted (GAP, not fabricated):
// OutpostsBucket in this backend tracks neither (see models.go).
func (h *Handler) handleGetBucket(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix)

	bucket, err := h.Backend.GetBucket(accountID, bucketName)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"GetBucketResult"`
		Bucket  string   `xml:"Bucket"`
	}{
		Bucket: bucket.Name,
	})
}

func (h *Handler) handleDeleteBucket(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix)

	if err := h.Backend.DeleteBucket(accountID, bucketName); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}

func (h *Handler) handleGetBucketLifecycleConfiguration(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/lifecycleconfiguration",
	)

	config, err := h.Backend.GetBucketLifecycleConfiguration(accountID, bucketName)
	if err != nil {
		return handleBackendError(c, err)
	}

	if config == "" {
		return writeXML(c, struct {
			XMLName xml.Name `xml:"GetBucketLifecycleConfigurationResult"`
		}{})
	}

	return c.Blob(http.StatusOK, "application/xml", []byte(config))
}

func (h *Handler) handlePutBucketLifecycleConfiguration(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/lifecycleconfiguration",
	)

	body := readBody(c)
	if err := h.Backend.PutBucketLifecycleConfiguration(accountID, bucketName, string(body)); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusOK, "")
}

func (h *Handler) handleDeleteBucketLifecycleConfiguration(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/lifecycleconfiguration",
	)

	if err := h.Backend.DeleteBucketLifecycleConfiguration(accountID, bucketName); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}

func (h *Handler) handleGetBucketPolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/policy",
	)

	policy, err := h.Backend.GetBucketPolicy(accountID, bucketName)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"GetBucketPolicyResult"`
		Policy  string   `xml:"Policy"`
	}{Policy: policy})
}

// putBucketPolicyRequestXML mirrors the real PutBucketPolicyInput wire shape:
// root PutBucketPolicyRequest, with the policy JSON document as the text
// content of a nested Policy element (confirmed against
// awsRestxml_serializeOpDocumentPutBucketPolicyInput in the installed SDK's
// serializers.go -- unlike PutBucketLifecycleConfiguration/PutBucketTagging/
// PutBucketVersioning, Policy is NOT a payload-bound field, so the request
// body is NOT the bare policy document; it is wrapped in this envelope).
type putBucketPolicyRequestXML struct {
	XMLName xml.Name `xml:"PutBucketPolicyRequest"`
	Policy  string   `xml:"Policy"`
}

func (h *Handler) handlePutBucketPolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/policy",
	)

	var body putBucketPolicyRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	if err := h.Backend.PutBucketPolicy(accountID, bucketName, body.Policy); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusOK, "")
}

func (h *Handler) handleDeleteBucketPolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/policy",
	)

	if err := h.Backend.DeleteBucketPolicy(accountID, bucketName); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}

type bucketTagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// getBucketTaggingResponseXML mirrors GetBucketTaggingOutput's real wire
// shape. TagSet is aws-sdk-go-v2's shared S3TagSet type, whose entries
// serialize as "<member>", NOT "<Tag>" (confirmed via
// awsRestxml_serializeDocumentS3TagSet -- the same type job tagging uses,
// see jobTagSetXML in handler_jobs.go; it is a DIFFERENT type from the
// "Tag"-named TagList used by generic resource tagging in handler_tags.go).
// A previous version of this handler used "Tag" here, which would make
// every tag invisible to a real client's S3TagSet decoder.
type getBucketTaggingResponseXML struct {
	XMLName xml.Name       `xml:"GetBucketTaggingResult"`
	Tags    []bucketTagXML `xml:"TagSet>member"`
}

func (h *Handler) handleGetBucketTagging(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/tagging",
	)

	tags, err := h.Backend.GetBucketTagging(accountID, bucketName)
	if err != nil {
		return handleBackendError(c, err)
	}

	resp := getBucketTaggingResponseXML{}
	for k, v := range tags {
		resp.Tags = append(resp.Tags, bucketTagXML{Key: k, Value: v})
	}

	return writeXML(c, resp)
}

// putBucketTaggingRequestXML mirrors PutBucketTaggingInput's real wire
// shape. Tagging is a "payload"-bound field in the real SDK, meaning the
// ENTIRE request body root element is "<Tagging>" -- there is no
// "<PutBucketTaggingRequest>" wrapper at all (confirmed via
// awsRestxml_serializeOpPutBucketTaggingRequest, which sets the XML root
// element to "Tagging" directly). A previous version of this handler
// expected the payload nested one level deeper, under
// "<PutBucketTaggingRequest><Tagging>...", which a real aws-sdk-go-v2
// client's request would never match (root-element mismatch), rejecting
// every real PutBucketTagging call outright. TagSet's member name is
// "member", not "Tag" -- see getBucketTaggingResponseXML's doc comment.
type putBucketTaggingRequestXML struct {
	XMLName xml.Name       `xml:"Tagging"`
	Tags    []bucketTagXML `xml:"TagSet>member"`
}

func (h *Handler) handlePutBucketTagging(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/tagging",
	)

	var body putBucketTaggingRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	tags := make(TagSet, len(body.Tags))
	for _, t := range body.Tags {
		tags[t.Key] = t.Value
	}

	if err := h.Backend.PutBucketTagging(accountID, bucketName, tags); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusOK, "")
}

func (h *Handler) handleDeleteBucketTagging(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/tagging",
	)

	if err := h.Backend.DeleteBucketTagging(accountID, bucketName); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}

func (h *Handler) handleGetBucketVersioning(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/versioning",
	)

	status, err := h.Backend.GetBucketVersioning(accountID, bucketName)
	if err != nil {
		return handleBackendError(c, err)
	}

	// GAP (no backing data, not fabricated): the real GetBucketVersioningOutput
	// also carries MfaDelete -- this backend tracks only a bare Status
	// string per bucket (see bucket.go), not MFA delete state.
	return writeXML(c, struct {
		XMLName xml.Name `xml:"GetBucketVersioningResult"`
		Status  string   `xml:"Status"`
	}{Status: status})
}

// putBucketVersioningRequestXML mirrors PutBucketVersioningInput's real
// wire shape. VersioningConfiguration is a "payload"-bound field, meaning
// the ENTIRE request body root element is "<VersioningConfiguration>" --
// there is no "<PutBucketVersioningRequest>" wrapper (confirmed via
// awsRestxml_serializeOpPutBucketVersioningRequest, which sets the XML
// root element to "VersioningConfiguration" directly, with Status as its
// direct child). A previous version of this handler expected the payload
// nested one level deeper under
// "<PutBucketVersioningRequest><VersioningConfiguration><Status>...",
// which a real aws-sdk-go-v2 client's request would never match
// (root-element mismatch), rejecting every real PutBucketVersioning call
// outright.
type putBucketVersioningRequestXML struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Status  string   `xml:"Status"`
}

func (h *Handler) handlePutBucketVersioning(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/versioning",
	)

	var body putBucketVersioningRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	if err := h.Backend.PutBucketVersioning(accountID, bucketName, body.Status); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusOK, "")
}

// listRegionalBucketItemXML mirrors aws-sdk-go-v2's RegionalBucket type.
// CreationDate/OutpostId/PublicAccessBlockEnabled have no backing data in
// this backend (see OutpostsBucket in models.go) -- GAP, not fabricated.
type listRegionalBucketItemXML struct {
	Bucket    string `xml:"Bucket"`
	BucketArn string `xml:"BucketArn"`
}

func (h *Handler) handleListRegionalBuckets(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	buckets := h.Backend.ListRegionalBuckets(accountID)
	items := make([]listRegionalBucketItemXML, 0, len(buckets))
	for _, b := range buckets {
		items = append(items, listRegionalBucketItemXML{Bucket: b.Name, BucketArn: b.BucketArn})
	}

	page, tok := s3cPaginate(items, nextToken, maxResults)

	return writeXML(c, struct {
		XMLName   xml.Name                    `xml:"ListRegionalBucketsResult"`
		NextToken string                      `xml:"NextToken,omitempty"`
		Buckets   []listRegionalBucketItemXML `xml:"RegionalBucketList>RegionalBucket"`
	}{Buckets: page, NextToken: tok})
}

// ---- Bucket Replication ----

type replicationConfigurationXML struct {
	XMLName xml.Name `xml:"ReplicationConfiguration"`
	Inner   string   `xml:",innerxml"`
}

type getReplicationResultXML struct {
	XMLName                  xml.Name                    `xml:"GetBucketReplicationResult"`
	ReplicationConfiguration replicationConfigurationXML `xml:"ReplicationConfiguration"`
}

func (h *Handler) handleGetBucketReplication(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/replication",
	)

	cfg, err := h.Backend.GetBucketReplication(accountID, bucketName)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getReplicationResultXML{
		ReplicationConfiguration: replicationConfigurationXML{Inner: cfg},
	})
}

type putReplicationRequestXML struct {
	XMLName xml.Name `xml:"ReplicationConfiguration"`
	Inner   string   `xml:",innerxml"`
}

func (h *Handler) handlePutBucketReplication(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/replication",
	)

	var body putReplicationRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	if err := h.Backend.PutBucketReplication(accountID, bucketName, body.Inner); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteBucketReplication(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/replication",
	)

	if err := h.Backend.DeleteBucketReplication(accountID, bucketName); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// readBody reads and returns the request body as bytes.
func readBody(c *echo.Context) []byte {
	r := c.Request()
	if r.Body == nil {
		return nil
	}
	buf := make([]byte, 0, 4096) //nolint:mnd // 4KB initial buffer
	tmp := make([]byte, 512)     //nolint:mnd // 512B read chunk
	for {
		n, err := r.Body.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err != nil {
			break
		}
	}

	return buf
}
