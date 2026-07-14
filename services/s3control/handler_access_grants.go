package s3control

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	pathAccessGrantsInstance = "/v20180820/accessgrantsinstance"
	pathIdentityCenter       = "/v20180820/accessgrantsinstance/identitycenter"
	pathAccessGrant          = "/v20180820/accessgrantsinstance/grant"
	pathAccessGrantsLocation = "/v20180820/accessgrantsinstance/location"

	// Additional path constants for access grants operations.
	pathAccessGrantsInstances              = "/v20180820/accessgrantsinstances"
	pathAccessGrantsInstanceResourcePolicy = "/v20180820/accessgrantsinstance/resourcepolicy"
	pathAccessGrantsInstancePrefix         = "/v20180820/accessgrantsinstance/"
	pathAccessGrantsLocationPrefix         = "/v20180820/accessgrantsinstance/location/"
	pathAccessGrantPrefix                  = "/v20180820/accessgrantsinstance/grant/"
	pathDataAccess                         = "/v20180820/accessgrantsinstance/dataaccess"
	// pathCallerAccessGrants matches the real aws-sdk-go-v2 ListCallerAccessGrants
	// request URI ("/v20180820/accessgrantsinstance/caller/grants"), NOT a
	// hyphenated "caller-grants" path.
	pathCallerAccessGrants = "/v20180820/accessgrantsinstance/caller/grants"
	// pathAccessGrantsList and pathAccessGrantsLocationsList are the PLURAL
	// list-operation paths ("/grants", "/locations"); the real SDK uses the
	// singular path only for Create (POST) and a distinct plural path for
	// List (GET) -- see pathAccessGrant / pathAccessGrantsLocation above.
	pathAccessGrantsList          = "/v20180820/accessgrantsinstance/grants"
	pathAccessGrantsLocationsList = "/v20180820/accessgrantsinstance/locations"
)

// isGrantsLocationPath returns true if path has the access grants location prefix
// and the remainder does NOT start with "s" (avoiding "storagelensgroup" collisions).
func isGrantsLocationPath(path string) bool {
	return strings.HasPrefix(path, pathAccessGrantsLocationPrefix) &&
		!strings.HasPrefix(strings.TrimPrefix(path, pathAccessGrantsLocationPrefix), "s")
}

// extractAccessGrantsInstanceOp handles access grants instance and identity center operations.
func extractAccessGrantsInstanceOp(path, method string) string {
	switch path {
	case pathAccessGrantsInstances:
		if method == http.MethodGet {
			return "ListAccessGrantsInstances"
		}
	case pathAccessGrantsInstance:
		switch method {
		case http.MethodPost:
			return "CreateAccessGrantsInstance"
		case http.MethodGet:
			return "GetAccessGrantsInstance"
		case http.MethodDelete:
			return "DeleteAccessGrantsInstance"
		}
	case pathAccessGrantsInstanceResourcePolicy:
		switch method {
		case http.MethodGet:
			return "GetAccessGrantsInstanceResourcePolicy"
		case http.MethodPut:
			return "PutAccessGrantsInstanceResourcePolicy"
		case http.MethodDelete:
			return "DeleteAccessGrantsInstanceResourcePolicy"
		}
	case pathIdentityCenter:
		switch method {
		case http.MethodPost:
			return "AssociateAccessGrantsIdentityCenter"
		case http.MethodDelete:
			return "DissociateAccessGrantsIdentityCenter"
		}
	}

	return ""
}

// extractAccessGrantsOp handles access grants, locations, and data access operations.
func extractAccessGrantsOp(path, method string) string {
	if op := extractAccessGrantsExactOp(path, method); op != "" {
		return op
	}

	return extractAccessGrantsPrefixOp(path, method)
}

// extractAccessGrantsExactOp handles exact-path access grants operations.
func extractAccessGrantsExactOp(path, method string) string {
	switch path {
	case pathAccessGrant:
		if method == http.MethodPost {
			return "CreateAccessGrant"
		}
	case pathAccessGrantsList:
		if method == http.MethodGet {
			return "ListAccessGrants"
		}
	case pathCallerAccessGrants:
		if method == http.MethodGet {
			return "ListCallerAccessGrants"
		}
	case pathDataAccess:
		if method == http.MethodGet {
			return "GetDataAccess"
		}
	case pathAccessGrantsLocation:
		if method == http.MethodPost {
			return "CreateAccessGrantsLocation"
		}
	case pathAccessGrantsLocationsList:
		if method == http.MethodGet {
			return "ListAccessGrantsLocations"
		}
	}

	return ""
}

// extractAccessGrantsPrefixOp handles prefix-based access grants operations.
func extractAccessGrantsPrefixOp(path, method string) string {
	switch {
	case strings.HasPrefix(path, pathAccessGrantsInstancePrefix+"prefix") && method == http.MethodGet:
		return "GetAccessGrantsInstanceForPrefix"
	case strings.HasPrefix(path, pathAccessGrantPrefix) && method == http.MethodGet:
		return "GetAccessGrant"
	case strings.HasPrefix(path, pathAccessGrantPrefix) && method == http.MethodDelete:
		return "DeleteAccessGrant"
	case isGrantsLocationPath(path) && method == http.MethodGet:
		return "GetAccessGrantsLocation"
	case isGrantsLocationPath(path) && method == http.MethodDelete:
		return "DeleteAccessGrantsLocation"
	case isGrantsLocationPath(path) && method == http.MethodPut:
		return "UpdateAccessGrantsLocation"
	}

	return ""
}

// dispatchAccessGrantsInstanceOps handles access grants instance and identity center operations.
func (h *Handler) dispatchAccessGrantsInstanceOps(c *echo.Context, path, method string) (bool, error) {
	switch path {
	case pathAccessGrantsInstances:
		if method == http.MethodGet {
			return true, h.handleListAccessGrantsInstances(c)
		}
	case pathAccessGrantsInstance:
		switch method {
		case http.MethodPost:
			return true, h.handleCreateAccessGrantsInstance(c)
		case http.MethodGet:
			return true, h.handleGetAccessGrantsInstance(c)
		case http.MethodDelete:
			return true, h.handleDeleteAccessGrantsInstance(c)
		}
	case pathAccessGrantsInstanceResourcePolicy:
		switch method {
		case http.MethodGet:
			return true, h.handleGetAccessGrantsInstanceResourcePolicy(c)
		case http.MethodPut:
			return true, h.handlePutAccessGrantsInstanceResourcePolicy(c)
		case http.MethodDelete:
			return true, h.handleDeleteAccessGrantsInstanceResourcePolicy(c)
		}
	case pathIdentityCenter:
		switch method {
		case http.MethodPost:
			return true, h.handleAssociateAccessGrantsIdentityCenter(c)
		case http.MethodDelete:
			return true, h.handleDissociateAccessGrantsIdentityCenter(c)
		}
	}

	return false, nil
}

// dispatchAccessGrantsOps handles access grants, locations, and data access operations.
func (h *Handler) dispatchAccessGrantsOps(c *echo.Context, path, method string) (bool, error) {
	if handled, err := h.dispatchAccessGrantsExactOps(c, path, method); handled {
		return true, err
	}

	return h.dispatchAccessGrantsPrefixOps(c, path, method)
}

// dispatchAccessGrantsExactOps handles exact-path access grants dispatch.
func (h *Handler) dispatchAccessGrantsExactOps(c *echo.Context, path, method string) (bool, error) {
	switch path {
	case pathAccessGrant:
		if method == http.MethodPost {
			return true, h.handleCreateAccessGrant(c)
		}
	case pathAccessGrantsList:
		if method == http.MethodGet {
			return true, h.handleListAccessGrants(c)
		}
	case pathCallerAccessGrants:
		if method == http.MethodGet {
			return true, h.handleListCallerAccessGrants(c)
		}
	case pathDataAccess:
		if method == http.MethodGet {
			return true, h.handleGetDataAccess(c)
		}
	case pathAccessGrantsLocation:
		if method == http.MethodPost {
			return true, h.handleCreateAccessGrantsLocation(c)
		}
	case pathAccessGrantsLocationsList:
		if method == http.MethodGet {
			return true, h.handleListAccessGrantsLocations(c)
		}
	}

	return false, nil
}

// dispatchAccessGrantsPrefixOps handles prefix-based access grants dispatch.
func (h *Handler) dispatchAccessGrantsPrefixOps(c *echo.Context, path, method string) (bool, error) {
	switch {
	case strings.HasPrefix(path, pathAccessGrantsInstancePrefix+"prefix") && method == http.MethodGet:
		return true, h.handleGetAccessGrantsInstanceForPrefix(c)
	case strings.HasPrefix(path, pathAccessGrantPrefix) && method == http.MethodGet:
		return true, h.handleGetAccessGrant(c)
	case strings.HasPrefix(path, pathAccessGrantPrefix) && method == http.MethodDelete:
		return true, h.handleDeleteAccessGrant(c)
	case isGrantsLocationPath(path) && method == http.MethodGet:
		return true, h.handleGetAccessGrantsLocation(c)
	case isGrantsLocationPath(path) && method == http.MethodDelete:
		return true, h.handleDeleteAccessGrantsLocation(c)
	case isGrantsLocationPath(path) && method == http.MethodPut:
		return true, h.handleUpdateAccessGrantsLocation(c)
	}

	return false, nil
}

// --- Access Grants Instance handlers ---

type createAccessGrantsInstanceRequestXML struct {
	XMLName           xml.Name `xml:"CreateAccessGrantsInstanceRequest"`
	IdentityCenterArn string   `xml:"IdentityCenterArn"`
}

type createAccessGrantsInstanceResponseXML struct {
	XMLName                      xml.Name `xml:"CreateAccessGrantsInstanceResult"`
	AccessGrantsInstanceArn      string   `xml:"AccessGrantsInstanceArn"`
	AccessGrantsInstanceID       string   `xml:"AccessGrantsInstanceId"`
	IdentityCenterArn            string   `xml:"IdentityCenterArn,omitempty"`
	IdentityCenterInstanceArn    string   `xml:"IdentityCenterInstanceArn,omitempty"`
	IdentityCenterApplicationArn string   `xml:"IdentityCenterApplicationArn,omitempty"`
}

func (h *Handler) handleCreateAccessGrantsInstance(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body createAccessGrantsInstanceRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	inst := h.Backend.CreateAccessGrantsInstance(accountID, body.IdentityCenterArn)

	return writeXML(c, createAccessGrantsInstanceResponseXML{
		AccessGrantsInstanceArn:   inst.AccessGrantsInstanceArn,
		AccessGrantsInstanceID:    inst.AccessGrantsInstanceID,
		IdentityCenterArn:         inst.IdentityCenterArn,
		IdentityCenterInstanceArn: inst.IdentityCenterInstanceArn,
	})
}

// --- ListAccessGrantsInstances handler ---

type listAccessGrantsInstancesItemXML struct {
	AccessGrantsInstanceArn string `xml:"AccessGrantsInstanceArn"`
	AccessGrantsInstanceID  string `xml:"AccessGrantsInstanceId"`
	IdentityCenterArn       string `xml:"IdentityCenterArn,omitempty"`
	CreatedAt               string `xml:"CreatedAt,omitempty"`
}

type listAccessGrantsInstancesResponseXML struct {
	XMLName               xml.Name                           `xml:"ListAccessGrantsInstancesResult"`
	NextToken             string                             `xml:"NextToken,omitempty"`
	AccessGrantsInstances []listAccessGrantsInstancesItemXML `xml:"AccessGrantsInstancesList>AccessGrantsInstance"`
}

func (h *Handler) handleListAccessGrantsInstances(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	insts := h.Backend.ListAccessGrantsInstances(accountID)
	items := make([]listAccessGrantsInstancesItemXML, 0, len(insts))
	for _, inst := range insts {
		items = append(items, listAccessGrantsInstancesItemXML{
			AccessGrantsInstanceArn: inst.AccessGrantsInstanceArn,
			AccessGrantsInstanceID:  inst.AccessGrantsInstanceID,
			IdentityCenterArn:       inst.IdentityCenterArn,
		})
	}

	page, tok := s3cPaginate(items, nextToken, maxResults)

	return writeXML(c, listAccessGrantsInstancesResponseXML{
		AccessGrantsInstances: page,
		NextToken:             tok,
	})
}

// --- AssociateAccessGrantsIdentityCenter handler ---

type associateAccessGrantsIdentityCenterRequestXML struct {
	XMLName           xml.Name `xml:"AssociateAccessGrantsIdentityCenterRequest"`
	IdentityCenterArn string   `xml:"IdentityCenterArn"`
}

func (h *Handler) handleAssociateAccessGrantsIdentityCenter(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body associateAccessGrantsIdentityCenterRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	h.Backend.AssociateAccessGrantsIdentityCenter(accountID, body.IdentityCenterArn)

	return c.NoContent(http.StatusOK)
}

// --- CreateAccessGrant handler ---

type createAccessGrantGranteeXML struct {
	GranteeType       string `xml:"GranteeType"`
	GranteeIdentifier string `xml:"GranteeIdentifier"`
}

type createAccessGrantRequestXML struct {
	XMLName                xml.Name                    `xml:"CreateAccessGrantRequest"`
	AccessGrantsLocationID string                      `xml:"AccessGrantsLocationId"`
	Permission             string                      `xml:"Permission"`
	Grantee                createAccessGrantGranteeXML `xml:"Grantee"`
	ApplicationArn         string                      `xml:"ApplicationArn"`
}

type createAccessGrantResponseXML struct {
	XMLName                xml.Name                    `xml:"CreateAccessGrantResult"`
	AccessGrantArn         string                      `xml:"AccessGrantArn"`
	AccessGrantID          string                      `xml:"AccessGrantId"`
	AccessGrantsLocationID string                      `xml:"AccessGrantsLocationId"`
	GrantScope             string                      `xml:"GrantScope"`
	Permission             string                      `xml:"Permission"`
	Grantee                createAccessGrantGranteeXML `xml:"Grantee,omitempty"`
	ApplicationArn         string                      `xml:"ApplicationArn,omitempty"`
}

func (h *Handler) handleCreateAccessGrant(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body createAccessGrantRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	grant, err := h.Backend.CreateAccessGrant(
		accountID,
		body.AccessGrantsLocationID,
		body.Grantee.GranteeType,
		body.Grantee.GranteeIdentifier,
		body.Permission,
		body.ApplicationArn,
	)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, createAccessGrantResponseXML{
		AccessGrantArn:         grant.AccessGrantArn,
		AccessGrantID:          grant.AccessGrantID,
		AccessGrantsLocationID: grant.AccessGrantsLocationID,
		GrantScope:             grant.GrantScope,
		Permission:             grant.Permission,
		Grantee: createAccessGrantGranteeXML{
			GranteeType:       grant.GranteeType,
			GranteeIdentifier: grant.GranteeIdentifier,
		},
		ApplicationArn: grant.ApplicationArn,
	})
}

// --- CreateAccessGrantsLocation handler ---

type createAccessGrantsLocationRequestXML struct {
	XMLName       xml.Name `xml:"CreateAccessGrantsLocationRequest"`
	LocationScope string   `xml:"LocationScope"`
	IAMRoleArn    string   `xml:"IAMRoleArn"`
}

type createAccessGrantsLocationResponseXML struct {
	XMLName                 xml.Name `xml:"CreateAccessGrantsLocationResult"`
	AccessGrantsLocationArn string   `xml:"AccessGrantsLocationArn"`
	AccessGrantsLocationID  string   `xml:"AccessGrantsLocationId"`
	LocationScope           string   `xml:"LocationScope"`
	IAMRoleArn              string   `xml:"IAMRoleArn"`
}

func (h *Handler) handleCreateAccessGrantsLocation(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body createAccessGrantsLocationRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	if body.IAMRoleArn == "" {
		return writeXMLErrorCode(c, http.StatusBadRequest, "InvalidRequest", "IAMRoleArn is required")
	}

	loc := h.Backend.CreateAccessGrantsLocation(accountID, body.LocationScope, body.IAMRoleArn)

	return writeXML(c, createAccessGrantsLocationResponseXML{
		AccessGrantsLocationArn: loc.AccessGrantsLocationArn,
		AccessGrantsLocationID:  loc.AccessGrantsLocationID,
		LocationScope:           loc.LocationScope,
		IAMRoleArn:              loc.IAMRoleArn,
	})
}

// ---- Access Grants Instance ----

type getAccessGrantsInstanceResponseXML struct {
	XMLName                 xml.Name `xml:"GetAccessGrantsInstanceResult"`
	AccessGrantsInstanceArn string   `xml:"AccessGrantsInstanceArn"`
	AccessGrantsInstanceID  string   `xml:"AccessGrantsInstanceId"`
	IdentityCenterArn       string   `xml:"IdentityCenterArn,omitempty"`
	CreatedAt               string   `xml:"CreatedAt,omitempty"`
}

func (h *Handler) handleGetAccessGrantsInstance(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	inst, err := h.Backend.GetAccessGrantsInstance(accountID)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getAccessGrantsInstanceResponseXML{
		AccessGrantsInstanceArn: inst.AccessGrantsInstanceArn,
		AccessGrantsInstanceID:  inst.AccessGrantsInstanceID,
		IdentityCenterArn:       inst.IdentityCenterArn,
		CreatedAt:               inst.CreatedAt,
	})
}

func (h *Handler) handleDeleteAccessGrantsInstance(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	if err := h.Backend.DeleteAccessGrantsInstance(accountID); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}

type getAccessGrantsInstanceResourcePolicyResponseXML struct {
	XMLName xml.Name `xml:"GetAccessGrantsInstanceResourcePolicyResult"`
	Policy  string   `xml:"Policy"`
}

func (h *Handler) handleGetAccessGrantsInstanceResourcePolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	policy, err := h.Backend.GetAccessGrantsInstanceResourcePolicy(accountID)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getAccessGrantsInstanceResourcePolicyResponseXML{Policy: policy})
}

type putAccessGrantsInstanceResourcePolicyRequestXML struct {
	XMLName xml.Name `xml:"PutAccessGrantsInstanceResourcePolicyRequest"`
	Policy  string   `xml:"Policy"`
}

func (h *Handler) handlePutAccessGrantsInstanceResourcePolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body putAccessGrantsInstanceResourcePolicyRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	h.Backend.PutAccessGrantsInstanceResourcePolicy(accountID, body.Policy)

	return writeXML(c, struct {
		XMLName xml.Name `xml:"PutAccessGrantsInstanceResourcePolicyResult"`
		Policy  string   `xml:"Policy"`
	}{Policy: body.Policy})
}

func (h *Handler) handleDeleteAccessGrantsInstanceResourcePolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	h.Backend.DeleteAccessGrantsInstanceResourcePolicy(accountID)

	return c.String(http.StatusNoContent, "")
}

func (h *Handler) handleDissociateAccessGrantsIdentityCenter(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	h.Backend.DissociateAccessGrantsIdentityCenter(accountID)

	return c.String(http.StatusNoContent, "")
}

type getAccessGrantsInstanceForPrefixResponseXML struct {
	XMLName                 xml.Name `xml:"GetAccessGrantsInstanceForPrefixResult"`
	AccessGrantsInstanceArn string   `xml:"AccessGrantsInstanceArn"`
}

func (h *Handler) handleGetAccessGrantsInstanceForPrefix(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	prefix := c.Request().URL.Query().Get("s3prefix")

	inst, err := h.Backend.GetAccessGrantsInstanceForPrefix(accountID, prefix)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getAccessGrantsInstanceForPrefixResponseXML{
		AccessGrantsInstanceArn: inst.AccessGrantsInstanceArn,
	})
}

// ---- Access Grants CRUD ----

type getAccessGrantResponseXML struct {
	XMLName           xml.Name `xml:"GetAccessGrantResult"`
	AccessGrantID     string   `xml:"AccessGrantId"`
	AccessGrantArn    string   `xml:"AccessGrantArn"`
	Permission        string   `xml:"Permission"`
	GranteeType       string   `xml:"Grantee>GranteeType"`
	GranteeIdentifier string   `xml:"Grantee>GranteeIdentifier"`
}

func (h *Handler) handleGetAccessGrant(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	grantID := strings.TrimPrefix(c.Request().URL.Path, pathAccessGrantPrefix)

	grant, err := h.Backend.GetAccessGrant(accountID, grantID)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getAccessGrantResponseXML{
		AccessGrantID:     grant.AccessGrantID,
		AccessGrantArn:    grant.AccessGrantArn,
		Permission:        grant.Permission,
		GranteeType:       grant.GranteeType,
		GranteeIdentifier: grant.GranteeIdentifier,
	})
}

func (h *Handler) handleDeleteAccessGrant(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	grantID := strings.TrimPrefix(c.Request().URL.Path, pathAccessGrantPrefix)

	if err := h.Backend.DeleteAccessGrant(accountID, grantID); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}

type listAccessGrantItemXML struct {
	AccessGrantID string `xml:"AccessGrantId"`
	Permission    string `xml:"Permission"`
	GrantScope    string `xml:"GrantScope,omitempty"`
}

type listAccessGrantsResponseXML struct {
	XMLName      xml.Name                 `xml:"ListAccessGrantsResult"`
	NextToken    string                   `xml:"NextToken,omitempty"`
	AccessGrants []listAccessGrantItemXML `xml:"AccessGrantsList>AccessGrant"`
}

func (h *Handler) handleListAccessGrants(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	q := c.Request().URL.Query()
	locationScope := q.Get("locationscope")
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	grants := h.Backend.ListAccessGrants(accountID, locationScope)
	items := make([]listAccessGrantItemXML, 0, len(grants))
	for _, g := range grants {
		items = append(items, listAccessGrantItemXML{
			AccessGrantID: g.AccessGrantID,
			Permission:    g.Permission,
			GrantScope:    g.GrantScope,
		})
	}

	page, tok := s3cPaginate(items, nextToken, maxResults)

	return writeXML(c, listAccessGrantsResponseXML{AccessGrants: page, NextToken: tok})
}

func (h *Handler) handleListCallerAccessGrants(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	grants := h.Backend.ListCallerAccessGrants(accountID)
	items := make([]listAccessGrantItemXML, 0, len(grants))
	for _, g := range grants {
		items = append(items, listAccessGrantItemXML{
			AccessGrantID: g.AccessGrantID,
			Permission:    g.Permission,
		})
	}

	page, tok := s3cPaginate(items, nextToken, maxResults)

	return writeXML(c, struct {
		XMLName      xml.Name                 `xml:"ListCallerAccessGrantsResult"`
		NextToken    string                   `xml:"NextToken,omitempty"`
		AccessGrants []listAccessGrantItemXML `xml:"AccessGrantsList>AccessGrant"`
	}{AccessGrants: page, NextToken: tok})
}

type getAccessGrantsLocationResponseXML struct {
	XMLName                 xml.Name `xml:"GetAccessGrantsLocationResult"`
	AccessGrantsLocationID  string   `xml:"AccessGrantsLocationId"`
	AccessGrantsLocationArn string   `xml:"AccessGrantsLocationArn"`
	LocationScope           string   `xml:"LocationScope"`
	IAMRoleArn              string   `xml:"IAMRoleArn"`
}

func (h *Handler) handleGetAccessGrantsLocation(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	locationID := strings.TrimPrefix(c.Request().URL.Path, pathAccessGrantsLocationPrefix)

	loc, err := h.Backend.GetAccessGrantsLocation(accountID, locationID)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getAccessGrantsLocationResponseXML{
		AccessGrantsLocationID:  loc.AccessGrantsLocationID,
		AccessGrantsLocationArn: loc.AccessGrantsLocationArn,
		LocationScope:           loc.LocationScope,
		IAMRoleArn:              loc.IAMRoleArn,
	})
}

func (h *Handler) handleDeleteAccessGrantsLocation(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	locationID := strings.TrimPrefix(c.Request().URL.Path, pathAccessGrantsLocationPrefix)

	if err := h.Backend.DeleteAccessGrantsLocation(accountID, locationID); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}

type updateAccessGrantsLocationRequestXML struct {
	XMLName    xml.Name `xml:"UpdateAccessGrantsLocationRequest"`
	IAMRoleArn string   `xml:"IAMRoleArn"`
}

func (h *Handler) handleUpdateAccessGrantsLocation(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	locationID := strings.TrimPrefix(c.Request().URL.Path, pathAccessGrantsLocationPrefix)

	var body updateAccessGrantsLocationRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	loc, err := h.Backend.UpdateAccessGrantsLocation(accountID, locationID, body.IAMRoleArn)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getAccessGrantsLocationResponseXML{
		AccessGrantsLocationID:  loc.AccessGrantsLocationID,
		AccessGrantsLocationArn: loc.AccessGrantsLocationArn,
		LocationScope:           loc.LocationScope,
		IAMRoleArn:              loc.IAMRoleArn,
	})
}

type listAccessGrantsLocationItemXML struct {
	AccessGrantsLocationID string `xml:"AccessGrantsLocationId"`
	LocationScope          string `xml:"LocationScope"`
}

func (h *Handler) handleListAccessGrantsLocations(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	locs := h.Backend.ListAccessGrantsLocations(accountID)
	items := make([]listAccessGrantsLocationItemXML, 0, len(locs))
	for _, loc := range locs {
		items = append(items, listAccessGrantsLocationItemXML{
			AccessGrantsLocationID: loc.AccessGrantsLocationID,
			LocationScope:          loc.LocationScope,
		})
	}

	page, tok := s3cPaginate(items, nextToken, maxResults)

	return writeXML(c, struct {
		XMLName   xml.Name                          `xml:"ListAccessGrantsLocationsResult"`
		NextToken string                            `xml:"NextToken,omitempty"`
		Locations []listAccessGrantsLocationItemXML `xml:"AccessGrantsLocationsList>AccessGrantsLocation"`
	}{Locations: page, NextToken: tok})
}

func (h *Handler) handleGetDataAccess(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	target := c.Request().URL.Query().Get("target")
	permission := c.Request().URL.Query().Get("permission")

	url, err := h.Backend.GetDataAccess(accountID, target, permission)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName     xml.Name `xml:"GetDataAccessResult"`
		Credentials struct {
			AccessKeyID     string `xml:"AccessKeyId"`
			SecretAccessKey string `xml:"SecretAccessKey"`
		} `xml:"Credentials"`
		MatchedGrantTarget string `xml:"MatchedGrantTarget"`
	}{
		MatchedGrantTarget: url,
	})
}
