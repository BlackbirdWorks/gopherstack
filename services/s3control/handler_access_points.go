package s3control

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	pathAccessPointPrefix = "/v20180820/accesspoint/"
	// pathAccessPointsDirectoryBuckets matches the real SDK's singular
	// "accesspointfordirectory" URI, not a pluralized "accesspointfordirectories".
	pathAccessPointsDirectoryBuckets = "/v20180820/accesspointfordirectory"
)

// extractAccessPointCRUDOp handles standard access point CRUD and listing.
func extractAccessPointCRUDOp(path, method string) string {
	if op := extractAccessPointBasicOp(path, method); op != "" {
		return op
	}

	return extractAccessPointSubResourceOp(path, method)
}

// extractAccessPointBasicOp handles access point CRUD and list operations.
func extractAccessPointBasicOp(path, method string) string {
	if isSimplePath(pathAccessPointPrefix, path) {
		switch method {
		case http.MethodPut:
			return "CreateAccessPoint"
		case http.MethodGet:
			return "GetAccessPoint"
		case http.MethodDelete:
			return "DeleteAccessPoint"
		}

		return ""
	}

	switch path {
	case "/v20180820/accesspoint":
		if method == http.MethodGet {
			return "ListAccessPoints"
		}
	case pathAccessPointsDirectoryBuckets:
		if method == http.MethodGet {
			return "ListAccessPointsForDirectoryBuckets"
		}
	case pathAccessPointsForObjectLambdaList:
		if method == http.MethodGet {
			return "ListAccessPointsForObjectLambda"
		}
	}

	return ""
}

// extractAccessPointSubResourceOp handles access point policy, status, and scope operations.
func extractAccessPointSubResourceOp(path, method string) string {
	switch {
	case isPrefixSuffix(pathAccessPointPrefix, path, "/policy") && method == http.MethodGet:
		return "GetAccessPointPolicy"
	case isPrefixSuffix(pathAccessPointPrefix, path, "/policy") && method == http.MethodPut:
		return "PutAccessPointPolicy"
	case isPrefixSuffix(pathAccessPointPrefix, path, "/policy") && method == http.MethodDelete:
		return "DeleteAccessPointPolicy"
	case isPrefixSuffix(pathAccessPointPrefix, path, "/policyStatus") && method == http.MethodGet:
		return "GetAccessPointPolicyStatus"
	case isPrefixSuffix(pathAccessPointPrefix, path, "/scope") && method == http.MethodGet:
		return "GetAccessPointScope"
	case isPrefixSuffix(pathAccessPointPrefix, path, "/scope") && method == http.MethodPut:
		return "PutAccessPointScope"
	case isPrefixSuffix(pathAccessPointPrefix, path, "/scope") && method == http.MethodDelete:
		return "DeleteAccessPointScope"
	}

	return ""
}

// dispatchAccessPointCRUDOps handles standard access point CRUD and listing.
func (h *Handler) dispatchAccessPointCRUDOps(c *echo.Context, path, method string) (bool, error) {
	if handled, err := h.dispatchAccessPointBasicOps(c, path, method); handled {
		return true, err
	}

	return h.dispatchAccessPointSubResourceOps(c, path, method)
}

// dispatchAccessPointBasicOps handles access point CRUD and list dispatch.
func (h *Handler) dispatchAccessPointBasicOps(c *echo.Context, path, method string) (bool, error) {
	if isSimplePath(pathAccessPointPrefix, path) {
		switch method {
		case http.MethodPut:
			return true, h.handleCreateAccessPoint(c)
		case http.MethodGet:
			return true, h.handleGetAccessPoint(c)
		case http.MethodDelete:
			return true, h.handleDeleteAccessPoint(c)
		}

		return false, nil
	}

	switch path {
	case "/v20180820/accesspoint":
		if method == http.MethodGet {
			return true, h.handleListAccessPoints(c)
		}
	case pathAccessPointsDirectoryBuckets:
		if method == http.MethodGet {
			return true, h.handleListAccessPointsForDirectoryBuckets(c)
		}
	case pathAccessPointsForObjectLambdaList:
		if method == http.MethodGet {
			return true, h.handleListAccessPointsForObjectLambda(c)
		}
	}

	return false, nil
}

// dispatchAccessPointSubResourceOps handles access point policy, status, and scope dispatch.
//
// Deliberately no "/publicAccessBlock" sub-resource route here:
// aws-sdk-go-v2/service/s3control has no Get/Put/DeleteAccessPointPublicAccessBlock
// operations. PublicAccessBlockConfiguration is account-level only
// (Get/Put/DeletePublicAccessBlock) except for the inline field real AWS
// embeds directly in CreateAccessPointInput/GetAccessPointOutput — see
// access_points.go for the storage methods backing that inline field.
func (h *Handler) dispatchAccessPointSubResourceOps(c *echo.Context, path, method string) (bool, error) {
	if isPrefixSuffix(pathAccessPointPrefix, path, "/policy") {
		return h.dispatchAccessPointPolicyMethod(c, method)
	}

	if isPrefixSuffix(pathAccessPointPrefix, path, "/policyStatus") && method == http.MethodGet {
		return true, h.handleGetAccessPointPolicyStatus(c)
	}

	if isPrefixSuffix(pathAccessPointPrefix, path, "/scope") {
		return h.dispatchAccessPointScopeMethod(c, method)
	}

	return false, nil
}

func (h *Handler) dispatchAccessPointPolicyMethod(c *echo.Context, method string) (bool, error) {
	switch method {
	case http.MethodGet:
		return true, h.handleGetAccessPointPolicy(c)
	case http.MethodPut:
		return true, h.handlePutAccessPointPolicy(c)
	case http.MethodDelete:
		return true, h.handleDeleteAccessPointPolicy(c)
	}

	return false, nil
}

func (h *Handler) dispatchAccessPointScopeMethod(c *echo.Context, method string) (bool, error) {
	switch method {
	case http.MethodGet:
		return true, h.handleGetAccessPointScope(c)
	case http.MethodPut:
		return true, h.handlePutAccessPointScope(c)
	case http.MethodDelete:
		return true, h.handleDeleteAccessPointScope(c)
	}

	return false, nil
}

// --- CreateAccessPoint handler ---

type apVpcConfigurationXML struct {
	VpcID string `xml:"VpcId"`
}

type apPublicAccessBlockXML struct {
	BlockPublicAcls       bool `xml:"BlockPublicAcls"`
	IgnorePublicAcls      bool `xml:"IgnorePublicAcls"`
	BlockPublicPolicy     bool `xml:"BlockPublicPolicy"`
	RestrictPublicBuckets bool `xml:"RestrictPublicBuckets"`
}

type createAccessPointRequestXML struct {
	XMLName                        xml.Name               `xml:"CreateAccessPointRequest"`
	Bucket                         string                 `xml:"Bucket"`
	BucketAccountID                string                 `xml:"BucketAccountId"`
	VpcConfiguration               apVpcConfigurationXML  `xml:"VpcConfiguration"`
	PublicAccessBlockConfiguration apPublicAccessBlockXML `xml:"PublicAccessBlockConfiguration"`
}

type createAccessPointResponseXML struct {
	XMLName        xml.Name `xml:"CreateAccessPointResult"`
	AccessPointArn string   `xml:"AccessPointArn"`
	Alias          string   `xml:"Alias,omitempty"`
}

func (h *Handler) handleCreateAccessPoint(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathAccessPointPrefix)

	var body createAccessPointRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	ap := h.Backend.CreateAccessPoint(accountID, name, body.Bucket)

	// Persist VPC config and bucket account ID when provided.
	if body.VpcConfiguration.VpcID != "" || body.BucketAccountID != "" {
		if err := h.Backend.SetAccessPointVpcConfig(
			accountID, name,
			body.VpcConfiguration.VpcID,
			body.BucketAccountID,
		); err != nil {
			return handleBackendError(c, err)
		}
		// Refresh ap after update to get correct NetworkOrigin and Alias.
		var errGet error
		ap, errGet = h.Backend.GetAccessPoint(accountID, name)
		if errGet != nil {
			return handleBackendError(c, errGet)
		}
	}

	// Store per-AP PAB when provided.
	if body.PublicAccessBlockConfiguration != (apPublicAccessBlockXML{}) {
		pab := PublicAccessBlock{
			AccountID:             accountID,
			BlockPublicAcls:       body.PublicAccessBlockConfiguration.BlockPublicAcls,
			IgnorePublicAcls:      body.PublicAccessBlockConfiguration.IgnorePublicAcls,
			BlockPublicPolicy:     body.PublicAccessBlockConfiguration.BlockPublicPolicy,
			RestrictPublicBuckets: body.PublicAccessBlockConfiguration.RestrictPublicBuckets,
		}
		_ = h.Backend.PutAccessPointPublicAccessBlock(accountID, name, pab)
	}

	return writeXML(c, createAccessPointResponseXML{
		AccessPointArn: ap.AccessPointArn,
		Alias:          ap.Alias,
	})
}

// --- GetAccessPoint handler ---

type getAccessPointVpcConfigXML struct {
	VpcID string `xml:"VpcId"`
}

type getAccessPointResponseXML struct {
	XMLName                        xml.Name                    `xml:"GetAccessPointResult"`
	Name                           string                      `xml:"Name"`
	Bucket                         string                      `xml:"Bucket"`
	NetworkOrigin                  string                      `xml:"NetworkOrigin"`
	AccessPointArn                 string                      `xml:"AccessPointArn,omitempty"`
	Alias                          string                      `xml:"Alias,omitempty"`
	BucketAccountID                string                      `xml:"BucketAccountId,omitempty"`
	VpcConfiguration               *getAccessPointVpcConfigXML `xml:"VpcConfiguration,omitempty"`
	PublicAccessBlockConfiguration *apPublicAccessBlockXML     `xml:"PublicAccessBlockConfiguration,omitempty"`
	CreationDate                   string                      `xml:"CreationDate,omitempty"`
}

// handleGetAccessPoint serves GetAccessPoint. PublicAccessBlockConfiguration
// travels inline in this response, not as a separate operation. GAP, not
// fabricated: real GetAccessPointOutput also carries DataSourceId,
// DataSourceType, and Endpoints (Multi-Region Access Point-backed access
// points on Outposts/Snow) — AccessPoint (models.go) tracks none of the three.
func (h *Handler) handleGetAccessPoint(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathAccessPointPrefix)

	ap, err := h.Backend.GetAccessPoint(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	resp := getAccessPointResponseXML{
		Name:            ap.Name,
		Bucket:          ap.Bucket,
		NetworkOrigin:   ap.NetworkOrigin,
		AccessPointArn:  ap.AccessPointArn,
		Alias:           ap.Alias,
		BucketAccountID: ap.BucketAccountID,
		CreationDate:    ap.CreationDate,
	}

	if ap.VpcID != "" {
		resp.VpcConfiguration = &getAccessPointVpcConfigXML{VpcID: ap.VpcID}
	}

	if pab, pabErr := h.Backend.GetAccessPointPublicAccessBlock(accountID, name); pabErr == nil {
		resp.PublicAccessBlockConfiguration = &apPublicAccessBlockXML{
			BlockPublicAcls:       pab.BlockPublicAcls,
			IgnorePublicAcls:      pab.IgnorePublicAcls,
			BlockPublicPolicy:     pab.BlockPublicPolicy,
			RestrictPublicBuckets: pab.RestrictPublicBuckets,
		}
	}

	return writeXML(c, resp)
}

func (h *Handler) handleDeleteAccessPoint(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathAccessPointPrefix)

	if err := h.Backend.DeleteAccessPoint(accountID, name); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- ListAccessPoints handler ---

type listAccessPointVpcConfigXML struct {
	VpcID string `xml:"VpcId"`
}

type listAccessPointItemXML struct {
	VpcConfiguration *listAccessPointVpcConfigXML `xml:"VpcConfiguration,omitempty"`
	Name             string                       `xml:"Name"`
	Bucket           string                       `xml:"Bucket"`
	NetworkOrigin    string                       `xml:"NetworkOrigin"`
	AccessPointArn   string                       `xml:"AccessPointArn,omitempty"`
	Alias            string                       `xml:"Alias,omitempty"`
	BucketAccountID  string                       `xml:"BucketAccountId,omitempty"`
}

type listAccessPointsResponseXML struct {
	XMLName      xml.Name                 `xml:"ListAccessPointsResult"`
	NextToken    string                   `xml:"NextToken,omitempty"`
	AccessPoints []listAccessPointItemXML `xml:"AccessPointList>AccessPoint"`
}

func (h *Handler) handleListAccessPoints(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))
	bucketFilter := q.Get("bucket")

	aps := h.Backend.ListAccessPoints(accountID)

	items := make([]listAccessPointItemXML, 0, len(aps))
	for _, ap := range aps {
		if bucketFilter != "" && ap.Bucket != bucketFilter {
			continue
		}

		item := listAccessPointItemXML{
			Name:            ap.Name,
			Bucket:          ap.Bucket,
			NetworkOrigin:   ap.NetworkOrigin,
			AccessPointArn:  ap.AccessPointArn,
			Alias:           ap.Alias,
			BucketAccountID: ap.BucketAccountID,
		}
		if ap.VpcID != "" {
			item.VpcConfiguration = &listAccessPointVpcConfigXML{VpcID: ap.VpcID}
		}
		items = append(items, item)
	}

	page, tok := s3cPaginate(items, nextToken, maxResults)

	return writeXML(c, listAccessPointsResponseXML{AccessPoints: page, NextToken: tok})
}

// --- Access point policy handlers ---

type putAccessPointPolicyRequestXML struct {
	XMLName xml.Name `xml:"PutAccessPointPolicyRequest"`
	Policy  string   `xml:"Policy"`
}

func (h *Handler) handlePutAccessPointPolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	path := c.Request().URL.Path
	name := strings.TrimSuffix(strings.TrimPrefix(path, pathAccessPointPrefix), "/policy")

	var body putAccessPointPolicyRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	if err := h.Backend.PutAccessPointPolicy(accountID, name, body.Policy); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusCreated)
}

type getAccessPointPolicyResponseXML struct {
	XMLName xml.Name `xml:"GetAccessPointPolicyResult"`
	Policy  string   `xml:"Policy"`
}

func (h *Handler) handleGetAccessPointPolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	path := c.Request().URL.Path
	name := strings.TrimSuffix(strings.TrimPrefix(path, pathAccessPointPrefix), "/policy")

	policy, err := h.Backend.GetAccessPointPolicy(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getAccessPointPolicyResponseXML{Policy: policy})
}

func (h *Handler) handleDeleteAccessPointPolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	path := c.Request().URL.Path
	name := strings.TrimSuffix(strings.TrimPrefix(path, pathAccessPointPrefix), "/policy")

	if err := h.Backend.DeleteAccessPointPolicy(accountID, name); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

type getAccessPointPolicyStatusResponseXML struct {
	XMLName  xml.Name `xml:"GetAccessPointPolicyStatusResult"`
	IsPublic bool     `xml:"PolicyStatus>IsPublic"`
}

func (h *Handler) handleGetAccessPointPolicyStatus(c *echo.Context) error {
	return writeXML(c, getAccessPointPolicyStatusResponseXML{IsPublic: false})
}

// ---- Access Point Scope ----

// handleGetAccessPointScope. GetAccessPointScopeOutput's Scope field is a
// structured type (Permissions []ScopePermission, Prefixes []string;
// awsRestxml_deserializeDocumentScope), not a flat string. This backend
// stores each access point's scope as an opaque raw XML blob
// (accessPointScopes), captured/replayed as raw inner XML nested under
// "<Scope>" to preserve the real structure.
func (h *Handler) handleGetAccessPointScope(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathAccessPointPrefix),
		"/scope",
	)

	scope, err := h.Backend.GetAccessPointScope(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	resp := struct {
		Scope   *describeJobInnerXML `xml:"Scope,omitempty"`
		XMLName xml.Name             `xml:"GetAccessPointScopeResult"`
	}{}
	if scope != "" {
		resp.Scope = &describeJobInnerXML{Raw: scope}
	}

	return writeXML(c, resp)
}

type putAccessPointScopeRequestXML struct {
	XMLName xml.Name            `xml:"PutAccessPointScopeRequest"`
	Scope   createJobXMLCapture `xml:"Scope"`
}

func (h *Handler) handlePutAccessPointScope(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathAccessPointPrefix),
		"/scope",
	)

	var body putAccessPointScopeRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	if err := h.Backend.PutAccessPointScope(accountID, name, body.Scope.Raw); err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"PutAccessPointScopeResult"`
	}{})
}

func (h *Handler) handleDeleteAccessPointScope(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathAccessPointPrefix),
		"/scope",
	)

	if err := h.Backend.DeleteAccessPointScope(accountID, name); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// handleListAccessPointsForDirectoryBuckets. ListAccessPointsForDirectoryBucketsOutput
// shares the same "AccessPointList>AccessPoint" wrapper and types.AccessPoint
// entry type as ListAccessPoints
// (awsRestxml_deserializeOpDocumentListAccessPointsForDirectoryBucketsOutput
// delegates to the same awsRestxml_deserializeDocumentAccessPointList), so
// this reuses listAccessPointItemXML rather than a narrower ad hoc type.
func (h *Handler) handleListAccessPointsForDirectoryBuckets(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	aps := h.Backend.ListAccessPointsForDirectoryBuckets(accountID)
	items := make([]listAccessPointItemXML, 0, len(aps))
	for _, ap := range aps {
		item := listAccessPointItemXML{
			Name:            ap.Name,
			Bucket:          ap.Bucket,
			NetworkOrigin:   ap.NetworkOrigin,
			AccessPointArn:  ap.AccessPointArn,
			Alias:           ap.Alias,
			BucketAccountID: ap.BucketAccountID,
		}
		if ap.VpcID != "" {
			item.VpcConfiguration = &listAccessPointVpcConfigXML{VpcID: ap.VpcID}
		}
		items = append(items, item)
	}

	page, tok := s3cPaginate(items, nextToken, maxResults)

	return writeXML(c, struct {
		XMLName      xml.Name                 `xml:"ListAccessPointsForDirectoryBucketsResult"`
		NextToken    string                   `xml:"NextToken,omitempty"`
		AccessPoints []listAccessPointItemXML `xml:"AccessPointList>AccessPoint"`
	}{AccessPoints: page, NextToken: tok})
}
