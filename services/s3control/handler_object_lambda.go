package s3control

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	pathObjectLambdaPrefix              = "/v20180820/accesspointforobjectlambda/"
	pathAccessPointsForObjectLambdaList = "/v20180820/accesspointforobjectlambda"
)

// extractObjectLambdaOp handles object lambda access point operations.
func extractObjectLambdaOp(path, method string) string {
	if isSimplePath(pathObjectLambdaPrefix, path) {
		switch method {
		case http.MethodPut:
			return "CreateAccessPointForObjectLambda"
		case http.MethodGet:
			return "GetAccessPointForObjectLambda"
		case http.MethodDelete:
			return "DeleteAccessPointForObjectLambda"
		}

		return ""
	}

	return extractObjectLambdaPolicyOp(path, method)
}

// extractObjectLambdaPolicyOp handles object lambda policy and configuration operations.
func extractObjectLambdaPolicyOp(path, method string) string {
	switch {
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/policy") && method == http.MethodGet:
		return "GetAccessPointPolicyForObjectLambda"
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/policy") && method == http.MethodPut:
		return "PutAccessPointPolicyForObjectLambda"
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/policy") && method == http.MethodDelete:
		return "DeleteAccessPointPolicyForObjectLambda"
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/policyStatus") && method == http.MethodGet:
		return "GetAccessPointPolicyStatusForObjectLambda"
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/configuration") && method == http.MethodGet:
		return "GetAccessPointConfigurationForObjectLambda"
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/configuration") && method == http.MethodPut:
		return "PutAccessPointConfigurationForObjectLambda"
	}

	return ""
}

// dispatchObjectLambdaOps handles object lambda access point operations.
func (h *Handler) dispatchObjectLambdaOps(c *echo.Context, path, method string) (bool, error) {
	switch {
	case isSimplePath(pathObjectLambdaPrefix, path):
		return h.dispatchObjectLambdaRootMethod(c, method)
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/policy"):
		return h.dispatchObjectLambdaPolicyMethod(c, method)
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/policyStatus") && method == http.MethodGet:
		return true, h.handleGetAccessPointPolicyStatusForObjectLambda(c)
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/configuration"):
		return h.dispatchObjectLambdaConfigMethod(c, method)
	}

	return false, nil
}

func (h *Handler) dispatchObjectLambdaRootMethod(c *echo.Context, method string) (bool, error) {
	switch method {
	case http.MethodPut:
		return true, h.handleCreateAccessPointForObjectLambda(c)
	case http.MethodGet:
		return true, h.handleGetAccessPointForObjectLambda(c)
	case http.MethodDelete:
		return true, h.handleDeleteAccessPointForObjectLambda(c)
	}

	return false, nil
}

func (h *Handler) dispatchObjectLambdaPolicyMethod(c *echo.Context, method string) (bool, error) {
	switch method {
	case http.MethodGet:
		return true, h.handleGetAccessPointPolicyForObjectLambda(c)
	case http.MethodPut:
		return true, h.handlePutAccessPointPolicyForObjectLambda(c)
	case http.MethodDelete:
		return true, h.handleDeleteAccessPointPolicyForObjectLambda(c)
	}

	return false, nil
}

func (h *Handler) dispatchObjectLambdaConfigMethod(c *echo.Context, method string) (bool, error) {
	switch method {
	case http.MethodGet:
		return true, h.handleGetAccessPointConfigurationForObjectLambda(c)
	case http.MethodPut:
		return true, h.handlePutAccessPointConfigurationForObjectLambda(c)
	}

	return false, nil
}

// --- CreateAccessPointForObjectLambda handler ---

type createAccessPointForObjectLambdaResponseXML struct {
	XMLName                    xml.Name `xml:"CreateAccessPointForObjectLambdaResult"`
	ObjectLambdaAccessPointArn string   `xml:"ObjectLambdaAccessPointArn"`
}

func (h *Handler) handleCreateAccessPointForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathObjectLambdaPrefix)

	ap := h.Backend.CreateAccessPointForObjectLambda(accountID, name)

	return writeXML(c, createAccessPointForObjectLambdaResponseXML{
		ObjectLambdaAccessPointArn: ap.ObjectLambdaAccessPointArn,
	})
}

// handleGetAccessPointForObjectLambda. GetAccessPointForObjectLambdaOutput
// in the real SDK has NO ObjectLambdaAccessPointArn field at all -- its
// only members are Alias, CreationDate, Name, and
// PublicAccessBlockConfiguration (confirmed via
// aws-sdk-go-v2/service/s3control's GetAccessPointForObjectLambdaOutput and
// its deserializer, which only recognizes those four elements). A previous
// version of this handler fabricated an ObjectLambdaAccessPointArn element
// here. Alias/CreationDate/PublicAccessBlockConfiguration are omitted
// (GAP, not fabricated): ObjectLambdaAccessPoint in this backend tracks
// none of the three (see models.go).
func (h *Handler) handleGetAccessPointForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathObjectLambdaPrefix)

	ap, err := h.Backend.GetAccessPointForObjectLambda(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"GetAccessPointForObjectLambdaResult"`
		Name    string   `xml:"Name"`
	}{
		Name: ap.Name,
	})
}

func (h *Handler) handleDeleteAccessPointForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathObjectLambdaPrefix)

	if err := h.Backend.DeleteAccessPointForObjectLambda(accountID, name); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListAccessPointsForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	aps := h.Backend.ListAccessPointsForObjectLambda(accountID)
	// olAPItem mirrors aws-sdk-go-v2's types.ObjectLambdaAccessPoint. Its
	// real Alias field (an *ObjectLambdaAccessPointAlias, auto-generated by
	// AWS in its own "<random>-ol-s3alias" format, distinct from regular
	// access points' "<name>-<accountid>-s3alias" scheme -- see
	// CreateAccessPoint in access_points.go) is omitted here (GAP, not
	// fabricated): ObjectLambdaAccessPoint (models.go) tracks no alias data
	// at all for these APs, and the real generation algorithm is not
	// documented/verified closely enough to synthesize one safely.
	type olAPItem struct {
		Name                       string `xml:"Name"`
		ObjectLambdaAccessPointArn string `xml:"ObjectLambdaAccessPointArn"`
	}
	items := make([]olAPItem, 0, len(aps))
	for _, ap := range aps {
		items = append(
			items,
			olAPItem{Name: ap.Name, ObjectLambdaAccessPointArn: ap.ObjectLambdaAccessPointArn},
		)
	}

	page, tok := s3cPaginate(items, nextToken, maxResults)

	return writeXML(c, struct {
		XMLName                     xml.Name   `xml:"ListAccessPointsForObjectLambdaResult"`
		NextToken                   string     `xml:"NextToken,omitempty"`
		ObjectLambdaAccessPointList []olAPItem `xml:"ObjectLambdaAccessPointList>ObjectLambdaAccessPoint"`
	}{ObjectLambdaAccessPointList: page, NextToken: tok})
}

func (h *Handler) handleGetAccessPointPolicyForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathObjectLambdaPrefix),
		"/policy",
	)

	policy, err := h.Backend.GetAccessPointPolicyForObjectLambda(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"GetAccessPointPolicyForObjectLambdaResult"`
		Policy  string   `xml:"Policy"`
	}{Policy: policy})
}

type putAccessPointPolicyForObjectLambdaRequestXML struct {
	XMLName xml.Name `xml:"PutAccessPointPolicyForObjectLambdaRequest"`
	Policy  string   `xml:"Policy"`
}

func (h *Handler) handlePutAccessPointPolicyForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathObjectLambdaPrefix),
		"/policy",
	)

	var body putAccessPointPolicyForObjectLambdaRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	if err := h.Backend.PutAccessPointPolicyForObjectLambda(accountID, name, body.Policy); err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"PutAccessPointPolicyForObjectLambdaResult"`
	}{})
}

func (h *Handler) handleDeleteAccessPointPolicyForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathObjectLambdaPrefix),
		"/policy",
	)

	if err := h.Backend.DeleteAccessPointPolicyForObjectLambda(accountID, name); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetAccessPointPolicyStatusForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathObjectLambdaPrefix),
		"/policyStatus",
	)

	isPublic, err := h.Backend.GetAccessPointPolicyStatusForObjectLambda(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName  xml.Name `xml:"GetAccessPointPolicyStatusForObjectLambdaResult"`
		IsPublic bool     `xml:"PolicyStatus>IsPublic"`
	}{IsPublic: isPublic})
}

// handleGetAccessPointConfigurationForObjectLambda. The real
// GetAccessPointConfigurationForObjectLambdaOutput wraps its payload under
// "<Configuration>", NOT "<ObjectLambdaConfiguration>" -- confirmed via
// awsRestxml_deserializeOpDocumentGetAccessPointConfigurationForObjectLambdaOutput,
// which only recognizes "Configuration" at the top level (the real
// ObjectLambdaConfiguration type -- SupportingAccessPoint,
// TransformationConfigurations, AllowedFeatures,
// CloudWatchMetricsEnabled -- lives INSIDE that element, it does not name
// it). A previous version of this handler used "ObjectLambdaConfiguration"
// as the wrapper key, which a real client's field-matching loop would
// silently skip (same wrong-envelope-key bug class as
// ListCallerAccessGrants' "AccessGrantsList", see handler_access_grants.go).
// The configuration payload itself is captured/replayed as raw inner XML
// (createJobXMLCapture, shared with CreateJob's Manifest/Operation/Report)
// rather than a plain string, so a real client's nested
// TransformationConfigurations/AllowedFeatures structure round-trips
// intact instead of being flattened to concatenated character data.
func (h *Handler) handleGetAccessPointConfigurationForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathObjectLambdaPrefix),
		"/configuration",
	)

	cfg, err := h.Backend.GetAccessPointConfigurationForObjectLambda(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	resp := struct {
		Configuration *describeJobInnerXML `xml:"Configuration,omitempty"`
		XMLName       xml.Name             `xml:"GetAccessPointConfigurationForObjectLambdaResult"`
	}{}
	if cfg != "" {
		resp.Configuration = &describeJobInnerXML{Raw: cfg}
	}

	return writeXML(c, resp)
}

type putAPConfigForObjectLambdaRequestXML struct {
	XMLName       xml.Name            `xml:"PutAccessPointConfigurationForObjectLambdaRequest"`
	Configuration createJobXMLCapture `xml:"Configuration"`
}

func (h *Handler) handlePutAccessPointConfigurationForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathObjectLambdaPrefix),
		"/configuration",
	)

	var body putAPConfigForObjectLambdaRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	if err := h.Backend.PutAccessPointConfigurationForObjectLambda(
		accountID, name, body.Configuration.Raw,
	); err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"PutAccessPointConfigurationForObjectLambdaResult"`
	}{})
}
