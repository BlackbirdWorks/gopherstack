package s3control

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	pathMRAPCreate       = "/v20180820/async-requests/mrap/create"
	pathMRAPPrefix       = "/v20180820/async-requests/mrap/"
	pathMRAPDeletePrefix = "/v20180820/async-requests/mrap/delete"
	// pathMRAPPutPolicyPrefix matches the real SDK's hyphenated
	// "put-policy" URI segment, not an underscore.
	pathMRAPPutPolicyPrefix = "/v20180820/async-requests/mrap/put-policy"
	pathMRAPList            = "/v20180820/mrap/instances"
	pathMRAPInstancePrefix  = "/v20180820/mrap/instances/"
	// pathMRAPPolicyStatusSuffix is the real SDK's MRAP policy-status
	// sub-resource suffix. Unlike the AccessPoint/ObjectLambda equivalents
	// (which use camelCase "/policyStatus"), the MRAP API uses all-lowercase
	// "/policystatus" -- see aws-sdk-go-v2/service/s3control serializers.go.
	pathMRAPPolicyStatusSuffix = "/policystatus"

	// opDeleteMRAP is the operation name for DeleteMultiRegionAccessPoint.
	// It is used in multiple dispatch cases to avoid a goconst violation.
	opDeleteMRAP = "DeleteMultiRegionAccessPoint"
)

// extractMRAPOps handles Multi-Region Access Point operations.
func extractMRAPOps(path, method string) string {
	if op := extractMRAPCreateListOp(path, method); op != "" {
		return op
	}

	return extractMRAPInstanceOp(path, method)
}

// extractMRAPCreateListOp handles MRAP create, delete, policy, and list operations.
func extractMRAPCreateListOp(path, method string) string {
	switch {
	case path == pathMRAPCreate && method == http.MethodPost:
		return "CreateMultiRegionAccessPoint"
	case strings.HasPrefix(path, pathMRAPDeletePrefix) && method == http.MethodPost:
		return "DeleteMultiRegionAccessPoint"
	case strings.HasPrefix(path, pathMRAPPutPolicyPrefix) && method == http.MethodPost:
		return "PutMultiRegionAccessPointPolicy"
	case strings.HasPrefix(path, pathMRAPPrefix) && method == http.MethodGet:
		return "DescribeMultiRegionAccessPointOperation"
	case path == pathMRAPList && method == http.MethodGet:
		return "ListMultiRegionAccessPoints"
	}

	return ""
}

// extractMRAPInstanceOp handles MRAP instance CRUD and sub-resource operations.
func extractMRAPInstanceOp(path, method string) string {
	if isSimplePath(pathMRAPInstancePrefix, path) {
		switch method {
		case http.MethodGet:
			return "GetMultiRegionAccessPoint"
		case http.MethodDelete:
			return opDeleteMRAP
		}

		return ""
	}

	switch {
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/policy") && method == http.MethodGet:
		return "GetMultiRegionAccessPointPolicy"
	case isPrefixSuffix(pathMRAPInstancePrefix, path, pathMRAPPolicyStatusSuffix) && method == http.MethodGet:
		return "GetMultiRegionAccessPointPolicyStatus"
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/routes") && method == http.MethodGet:
		return "GetMultiRegionAccessPointRoutes"
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/routes") && method == http.MethodPatch:
		return "SubmitMultiRegionAccessPointRoutes"
	}

	return ""
}

// dispatchMRAPDispatchOps handles Multi-Region Access Point dispatch.
func (h *Handler) dispatchMRAPDispatchOps(c *echo.Context, path, method string) (bool, error) {
	if handled, err := h.dispatchMRAPCreateListOps(c, path, method); handled {
		return true, err
	}

	return h.dispatchMRAPInstanceDispatch(c, path, method)
}

// dispatchMRAPCreateListOps handles MRAP create, delete, policy, and list dispatch.
func (h *Handler) dispatchMRAPCreateListOps(c *echo.Context, path, method string) (bool, error) {
	switch {
	case path == pathMRAPCreate && method == http.MethodPost:
		return true, h.handleCreateMultiRegionAccessPoint(c)
	case strings.HasPrefix(path, pathMRAPDeletePrefix) && method == http.MethodPost:
		return true, h.handleDeleteMultiRegionAccessPointAsync(c)
	case strings.HasPrefix(path, pathMRAPPutPolicyPrefix) && method == http.MethodPost:
		return true, h.handlePutMultiRegionAccessPointPolicy(c)
	case strings.HasPrefix(path, pathMRAPPrefix) && method == http.MethodGet:
		return true, h.handleDescribeMultiRegionAccessPointOperation(c)
	case path == pathMRAPList && method == http.MethodGet:
		return true, h.handleListMultiRegionAccessPoints(c)
	}

	return false, nil
}

// dispatchMRAPInstanceDispatch handles MRAP instance CRUD and sub-resource dispatch.
func (h *Handler) dispatchMRAPInstanceDispatch(c *echo.Context, path, method string) (bool, error) {
	if isSimplePath(pathMRAPInstancePrefix, path) {
		switch method {
		case http.MethodGet:
			return true, h.handleGetMultiRegionAccessPoint(c)
		case http.MethodDelete:
			return true, h.handleDeleteMultiRegionAccessPoint(c)
		}

		return false, nil
	}

	switch {
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/policy") && method == http.MethodGet:
		return true, h.handleGetMultiRegionAccessPointPolicy(c)
	case isPrefixSuffix(pathMRAPInstancePrefix, path, pathMRAPPolicyStatusSuffix) && method == http.MethodGet:
		return true, h.handleGetMultiRegionAccessPointPolicyStatus(c)
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/routes") && method == http.MethodGet:
		return true, h.handleGetMultiRegionAccessPointRoutes(c)
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/routes") && method == http.MethodPatch:
		return true, h.handleSubmitMultiRegionAccessPointRoutes(c)
	}

	return false, nil
}

// --- CreateMultiRegionAccessPoint handler ---

type createMRAPRegionXML struct {
	Bucket string `xml:"Bucket"`
}

type createMRAPDetailsXML struct {
	Name    string                `xml:"Name"`
	Regions []createMRAPRegionXML `xml:"Regions>Region"`
}

type createMRAPRequestXML struct {
	XMLName     xml.Name             `xml:"CreateMultiRegionAccessPointRequest"`
	ClientToken string               `xml:"ClientToken"`
	Details     createMRAPDetailsXML `xml:"Details"`
}

type createMRAPResponseXML struct {
	XMLName         xml.Name `xml:"CreateMultiRegionAccessPointResult"`
	RequestTokenARN string   `xml:"RequestTokenARN"`
}

func (h *Handler) handleCreateMultiRegionAccessPoint(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body createMRAPRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	req := h.Backend.CreateMultiRegionAccessPoint(accountID, body.Details.Name, body.ClientToken)

	// Persist regions if provided.
	if len(body.Details.Regions) > 0 {
		buckets := make([]string, 0, len(body.Details.Regions))
		for _, r := range body.Details.Regions {
			buckets = append(buckets, r.Bucket)
		}
		_ = h.Backend.SetMRAPRegions(accountID, body.Details.Name, buckets)
	}

	return writeXML(c, createMRAPResponseXML{
		RequestTokenARN: req.RequestTokenARN,
	})
}

// --- MRAP handlers ---

// getMRAPRegionXML mirrors aws-sdk-go-v2's RegionReport. Region (the AWS
// region code, e.g. "us-east-1") and BucketAccountId have no backing data
// in this backend -- MultiRegionAccessPoint.Regions (models.go) tracks
// only bucket names -- GAP, not fabricated.
type getMRAPRegionXML struct {
	Bucket string `xml:"Bucket"`
}

// getMRAPAccessPointXML mirrors aws-sdk-go-v2's MultiRegionAccessPointReport,
// shared by GetMultiRegionAccessPoint and (per-item) ListMultiRegionAccessPoints.
// PublicAccessBlock has no backing data in this backend (GAP, not
// fabricated): MRAP-level PublicAccessBlockConfiguration is not tracked
// separately from the account-/access-point-level PAB maps.
type getMRAPAccessPointXML struct {
	Name      string             `xml:"Name"`
	Alias     string             `xml:"Alias"`
	Status    string             `xml:"Status"`
	CreatedAt string             `xml:"CreatedAt,omitempty"`
	Regions   []getMRAPRegionXML `xml:"Regions>Region,omitempty"`
}

type getMRAPResponseXML struct {
	XMLName     xml.Name              `xml:"GetMultiRegionAccessPointResult"`
	AccessPoint getMRAPAccessPointXML `xml:"AccessPoint"`
}

func (h *Handler) handleGetMultiRegionAccessPoint(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathMRAPInstancePrefix)

	mrap, err := h.Backend.GetMultiRegionAccessPoint(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	regionItems := make([]getMRAPRegionXML, 0, len(mrap.Regions))
	for _, bucket := range mrap.Regions {
		regionItems = append(regionItems, getMRAPRegionXML{Bucket: bucket})
	}

	return writeXML(c, getMRAPResponseXML{
		AccessPoint: getMRAPAccessPointXML{
			Name:      mrap.Name,
			Alias:     mrap.Alias,
			Status:    mrap.Status,
			CreatedAt: mrap.CreatedAt,
			Regions:   regionItems,
		},
	})
}

func (h *Handler) handleDeleteMultiRegionAccessPoint(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathMRAPInstancePrefix)

	if err := h.Backend.DeleteMultiRegionAccessPoint(accountID, name); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

type deleteMRAPAsyncRequestXML struct {
	XMLName xml.Name `xml:"DeleteMultiRegionAccessPointRequest"`
	Details struct {
		Name string `xml:"Name"`
	} `xml:"Details"`
}

type deleteMRAPAsyncResponseXML struct {
	XMLName         xml.Name `xml:"DeleteMultiRegionAccessPointResult"`
	RequestTokenARN string   `xml:"RequestTokenARN"`
}

func (h *Handler) handleDeleteMultiRegionAccessPointAsync(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body deleteMRAPAsyncRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	if err := h.Backend.DeleteMultiRegionAccessPoint(accountID, body.Details.Name); err != nil {
		return handleBackendError(c, err)
	}

	tokenARN := "arn:aws:s3::" + accountID + ":async-request/mrap/delete/1"

	return writeXML(c, deleteMRAPAsyncResponseXML{RequestTokenARN: tokenARN})
}

// listMRAPsResponseXML mirrors ListMultiRegionAccessPointsOutput's real
// wire shape: each entry is the SAME MultiRegionAccessPointReport type
// GetMultiRegionAccessPoint returns (see getMRAPAccessPointXML), and the
// list wraps under "AccessPoints" with member name "AccessPoint" -- NOT
// "item" (confirmed via
// awsRestxml_deserializeDocumentMultiRegionAccessPointReportList). A
// previous version of this handler used "item" as the member name, which a
// real client's field-matching loop would silently skip, yielding an empty
// list on every real ListMultiRegionAccessPoints call, and omitted
// CreatedAt/Regions despite this backend having real data for both.
type listMRAPsResponseXML struct {
	XMLName      xml.Name                `xml:"ListMultiRegionAccessPointsResult"`
	NextToken    string                  `xml:"NextToken,omitempty"`
	AccessPoints []getMRAPAccessPointXML `xml:"AccessPoints>AccessPoint"`
}

func (h *Handler) handleListMultiRegionAccessPoints(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	mraps := h.Backend.ListMultiRegionAccessPoints(accountID)

	items := make([]getMRAPAccessPointXML, 0, len(mraps))
	for _, m := range mraps {
		regionItems := make([]getMRAPRegionXML, 0, len(m.Regions))
		for _, bucket := range m.Regions {
			regionItems = append(regionItems, getMRAPRegionXML{Bucket: bucket})
		}

		items = append(items, getMRAPAccessPointXML{
			Name:      m.Name,
			Alias:     m.Alias,
			Status:    m.Status,
			CreatedAt: m.CreatedAt,
			Regions:   regionItems,
		})
	}

	page, tok := s3cPaginate(items, nextToken, maxResults)

	return writeXML(c, listMRAPsResponseXML{AccessPoints: page, NextToken: tok})
}

type putMRAPPolicyRequestXML struct {
	XMLName xml.Name `xml:"PutMultiRegionAccessPointPolicyRequest"`
	Details struct {
		Name   string `xml:"Name"`
		Policy string `xml:"Policy"`
	} `xml:"Details"`
}

type putMRAPPolicyResponseXML struct {
	XMLName         xml.Name `xml:"PutMultiRegionAccessPointPolicyResult"`
	RequestTokenARN string   `xml:"RequestTokenARN"`
}

func (h *Handler) handlePutMultiRegionAccessPointPolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body putMRAPPolicyRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	if err := h.Backend.PutMultiRegionAccessPointPolicy(accountID, body.Details.Name, body.Details.Policy); err != nil {
		return handleBackendError(c, err)
	}

	policyTokenARN := "arn:aws:s3::" + accountID + ":async-request/mrap/put_policy/1"

	return writeXML(c, putMRAPPolicyResponseXML{RequestTokenARN: policyTokenARN})
}

// --- MRAP handlers (describe / policy / routes) ---

// handleDescribeMultiRegionAccessPointOperation. The real AsyncOperation
// type also carries CreationTime, Operation, RequestParameters, and
// ResponseDetails (confirmed via
// awsRestxml_deserializeDocumentAsyncOperation) -- GAP, not fabricated:
// MultiRegionAccessPointRequest (models.go) tracks only the request token
// and name, not a full async-operation audit trail. RequestStatus is
// hardcoded to "SUCCEEDED" rather than fabricated per se: every MRAP
// mutation in this backend completes synchronously, so by the time this
// endpoint can be queried the operation has, in fact, already succeeded.
func (h *Handler) handleDescribeMultiRegionAccessPointOperation(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	requestToken := strings.TrimPrefix(c.Request().URL.Path, pathMRAPPrefix)

	req, err := h.Backend.DescribeMultiRegionAccessPointOperation(accountID, requestToken)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName        xml.Name `xml:"DescribeMultiRegionAccessPointOperationResult"`
		AsyncOperation struct {
			RequestTokenARN string `xml:"RequestTokenARN"`
			RequestStatus   string `xml:"RequestStatus"`
		} `xml:"AsyncOperation"`
	}{
		AsyncOperation: struct {
			RequestTokenARN string `xml:"RequestTokenARN"`
			RequestStatus   string `xml:"RequestStatus"`
		}{
			RequestTokenARN: req.RequestTokenARN,
			RequestStatus:   "SUCCEEDED",
		},
	})
}

func (h *Handler) handleGetMultiRegionAccessPointPolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathMRAPInstancePrefix),
		"/policy",
	)

	policy, err := h.Backend.GetMultiRegionAccessPointPolicy(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"GetMultiRegionAccessPointPolicyResult"`
		Policy  struct {
			Established struct {
				Policy string `xml:"Policy"`
			} `xml:"Established"`
		} `xml:"Policy"`
	}{
		Policy: struct {
			Established struct {
				Policy string `xml:"Policy"`
			} `xml:"Established"`
		}{
			Established: struct {
				Policy string `xml:"Policy"`
			}{Policy: policy},
		},
	})
}

func (h *Handler) handleGetMultiRegionAccessPointPolicyStatus(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathMRAPInstancePrefix),
		pathMRAPPolicyStatusSuffix,
	)

	isPublic, err := h.Backend.GetMultiRegionAccessPointPolicyStatus(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName  xml.Name `xml:"GetMultiRegionAccessPointPolicyStatusResult"`
		IsPublic bool     `xml:"PolicyStatus>IsPublic"`
	}{IsPublic: isPublic})
}

// handleGetMultiRegionAccessPointRoutes. GetMultiRegionAccessPointRoutesOutput's
// Routes field is a LIST of MultiRegionAccessPointRoute
// (Bucket/Region/TrafficDialPercentage), wrapped as
// "<Routes><Route>...</Route></Routes>" -- NOT a flat string (confirmed via
// awsRestxml_deserializeDocumentRouteList, whose member name is "Route").
// This backend stores each MRAP's routing config as an opaque raw XML blob
// (mrapRoutes), so the real per-route structure is captured/replayed as
// raw inner XML nested under "<Routes>" rather than flattened to escaped
// character data.
func (h *Handler) handleGetMultiRegionAccessPointRoutes(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathMRAPInstancePrefix),
		"/routes",
	)

	routes, err := h.Backend.GetMultiRegionAccessPointRoutes(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	resp := struct {
		Routes  *describeJobInnerXML `xml:"Routes,omitempty"`
		XMLName xml.Name             `xml:"GetMultiRegionAccessPointRoutesResult"`
		Mrap    string               `xml:"Mrap"`
	}{
		Mrap: name,
	}
	if routes != "" {
		resp.Routes = &describeJobInnerXML{Raw: routes}
	}

	return writeXML(c, resp)
}

// ---- MRAP Routes (submit) ----

// submitMRAPRoutesRequestXML mirrors SubmitMultiRegionAccessPointRoutesInput's
// real wire shape: the field is named "RouteUpdates", NOT "Routes"
// (confirmed via
// awsRestxml_serializeOpDocumentSubmitMultiRegionAccessPointRoutesInput),
// and it is a list of "<Route>" entries, not a flat string. A previous
// version of this handler expected a "<Routes>" child element, which a
// real aws-sdk-go-v2 client's request never sends -- SubmitMultiRegionAccessPointRoutes
// silently stored an empty routing update for every real caller. The
// payload is captured as raw inner XML (createJobXMLCapture) to preserve
// the real per-route Bucket/Region/TrafficDialPercentage structure.
type submitMRAPRoutesRequestXML struct {
	XMLName      xml.Name            `xml:"SubmitMultiRegionAccessPointRoutesRequest"`
	RouteUpdates createJobXMLCapture `xml:"RouteUpdates"`
}

func (h *Handler) handleSubmitMultiRegionAccessPointRoutes(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	mrapName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathMRAPInstancePrefix),
		"/routes",
	)

	var body submitMRAPRoutesRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	if err := h.Backend.SubmitMultiRegionAccessPointRoutes(accountID, mrapName, body.RouteUpdates.Raw); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
