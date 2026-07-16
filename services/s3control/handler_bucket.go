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

func (h *Handler) handleGetBucket(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix)

	bucket, err := h.Backend.GetBucket(accountID, bucketName)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName   xml.Name `xml:"GetBucketResult"`
		Bucket    string   `xml:"Bucket"`
		BucketArn string   `xml:"BucketArn"`
		Location  string   `xml:"OutpostId,omitempty"`
	}{
		Bucket:    bucket.Name,
		BucketArn: bucket.BucketArn,
		Location:  bucket.Location,
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

func (h *Handler) handlePutBucketPolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/policy",
	)

	body := readBody(c)
	if err := h.Backend.PutBucketPolicy(accountID, bucketName, string(body)); err != nil {
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

type getBucketTaggingResponseXML struct {
	XMLName xml.Name       `xml:"GetBucketTaggingResult"`
	Tags    []bucketTagXML `xml:"TagSet>Tag"`
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

type putBucketTaggingRequestXML struct {
	XMLName xml.Name       `xml:"PutBucketTaggingRequest"`
	Tags    []bucketTagXML `xml:"Tagging>TagSet>Tag"`
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

	return writeXML(c, struct {
		XMLName xml.Name `xml:"GetBucketVersioningResult"`
		Status  string   `xml:"Status"`
	}{Status: status})
}

type putBucketVersioningRequestXML struct {
	XMLName xml.Name `xml:"PutBucketVersioningRequest"`
	Status  string   `xml:"VersioningConfiguration>Status"`
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
