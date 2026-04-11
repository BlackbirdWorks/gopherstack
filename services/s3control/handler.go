package s3control

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	// defaultAccountID is used when no account ID is provided in the request header.
	defaultAccountID = "default"

	// Path constants for S3 Control operations.
	pathPublicAccessBlock    = "/configuration/publicAccessBlock"
	pathAccessGrantsInstance = "/v20180820/accessgrantsinstance"
	pathIdentityCenter       = "/v20180820/accessgrantsinstance/identitycenter"
	pathAccessGrant          = "/v20180820/accessgrantsinstance/grant"
	pathAccessGrantsLocation = "/v20180820/accessgrantsinstance/location"
	pathAccessPointPrefix    = "/v20180820/accesspoint/"
	pathObjectLambdaPrefix   = "/v20180820/accesspointforobjectlambda/"
	pathBucketPrefix         = "/v20180820/bucket/"
	pathJobs                 = "/v20180820/jobs"
	pathMRAPCreate           = "/v20180820/async-requests/mrap/create"
	pathStorageLensGroup     = "/v20180820/storagelensgroup"
)

// Handler is the Echo HTTP handler for S3 Control operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new S3 Control handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "S3Control" }

// GetSupportedOperations returns the list of supported S3 Control operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AssociateAccessGrantsIdentityCenter",
		"CreateAccessGrant",
		"CreateAccessGrantsInstance",
		"CreateAccessGrantsLocation",
		"CreateAccessPoint",
		"CreateAccessPointForObjectLambda",
		"CreateBucket",
		"CreateJob",
		"CreateMultiRegionAccessPoint",
		"CreateStorageLensGroup",
		"DeletePublicAccessBlock",
		"GetPublicAccessBlock",
		"PutPublicAccessBlock",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "s3" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this S3 Control instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches S3 Control requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, "/v20180820/")
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the S3 Control operation from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()
	path := r.URL.Path
	method := r.Method

	if strings.HasSuffix(path, pathPublicAccessBlock) {
		return extractPublicAccessBlockOp(method)
	}

	return extractNewOpsOperation(path, method)
}

func extractPublicAccessBlockOp(method string) string {
	switch method {
	case http.MethodGet:
		return "GetPublicAccessBlock"
	case http.MethodPut:
		return "PutPublicAccessBlock"
	case http.MethodDelete:
		return "DeletePublicAccessBlock"
	}

	return "Unknown"
}

func extractNewOpsOperation(path, method string) string {
	switch {
	case path == pathAccessGrantsInstance && method == http.MethodPost:
		return "CreateAccessGrantsInstance"
	case path == pathIdentityCenter && method == http.MethodPost:
		return "AssociateAccessGrantsIdentityCenter"
	case path == pathAccessGrant && method == http.MethodPost:
		return "CreateAccessGrant"
	case path == pathAccessGrantsLocation && method == http.MethodPost:
		return "CreateAccessGrantsLocation"
	}

	return extractResourceOpsOperation(path, method)
}

func extractResourceOpsOperation(path, method string) string {
	switch {
	case strings.HasPrefix(path, pathAccessPointPrefix) && method == http.MethodPut:
		return "CreateAccessPoint"
	case strings.HasPrefix(path, pathObjectLambdaPrefix) && method == http.MethodPut:
		return "CreateAccessPointForObjectLambda"
	case strings.HasPrefix(path, pathBucketPrefix) && method == http.MethodPut:
		return "CreateBucket"
	case path == pathJobs && method == http.MethodPost:
		return "CreateJob"
	case path == pathMRAPCreate && method == http.MethodPost:
		return "CreateMultiRegionAccessPoint"
	case path == pathStorageLensGroup && method == http.MethodPost:
		return "CreateStorageLensGroup"
	}

	return "Unknown"
}

// ExtractResource returns the account ID from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return c.Request().Header.Get("X-Amz-Account-Id")
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		path := r.URL.Path
		method := r.Method

		if strings.HasSuffix(path, pathPublicAccessBlock) {
			return h.dispatchPublicAccessBlock(c, method)
		}

		return h.dispatchNewOps(c, path, method)
	}
}

func (h *Handler) dispatchPublicAccessBlock(c *echo.Context, method string) error {
	switch method {
	case http.MethodGet:
		return h.handleGetPublicAccessBlock(c)
	case http.MethodPut:
		return h.handlePutPublicAccessBlock(c)
	case http.MethodDelete:
		return h.handleDeletePublicAccessBlock(c)
	}

	return c.String(http.StatusNotFound, "not found")
}

func (h *Handler) dispatchNewOps(c *echo.Context, path, method string) error {
	switch {
	case path == pathAccessGrantsInstance && method == http.MethodPost:
		return h.handleCreateAccessGrantsInstance(c)
	case path == pathIdentityCenter && method == http.MethodPost:
		return h.handleAssociateAccessGrantsIdentityCenter(c)
	case path == pathAccessGrant && method == http.MethodPost:
		return h.handleCreateAccessGrant(c)
	case path == pathAccessGrantsLocation && method == http.MethodPost:
		return h.handleCreateAccessGrantsLocation(c)
	}

	return h.dispatchResourceOps(c, path, method)
}

func (h *Handler) dispatchResourceOps(c *echo.Context, path, method string) error {
	switch {
	case strings.HasPrefix(path, pathAccessPointPrefix) && method == http.MethodPut:
		return h.handleCreateAccessPoint(c)
	case strings.HasPrefix(path, pathObjectLambdaPrefix) && method == http.MethodPut:
		return h.handleCreateAccessPointForObjectLambda(c)
	case strings.HasPrefix(path, pathBucketPrefix) && method == http.MethodPut:
		return h.handleCreateBucket(c)
	case path == pathJobs && method == http.MethodPost:
		return h.handleCreateJob(c)
	case path == pathMRAPCreate && method == http.MethodPost:
		return h.handleCreateMultiRegionAccessPoint(c)
	case path == pathStorageLensGroup && method == http.MethodPost:
		return h.handleCreateStorageLensGroup(c)
	}

	return c.String(http.StatusNotFound, "not found")
}

func accountIDFromRequest(c *echo.Context) string {
	accountID := c.Request().Header.Get("X-Amz-Account-Id")
	if accountID == "" {
		return defaultAccountID
	}

	return accountID
}

// handleBackendError maps backend sentinel errors to appropriate HTTP responses.
func handleBackendError(c *echo.Context, err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.String(http.StatusNotFound, err.Error())
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.String(http.StatusBadRequest, err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.String(http.StatusConflict, err.Error())
	default:
		return c.String(http.StatusInternalServerError, err.Error())
	}
}

// decodeXML decodes the request body into v, treating EOF as an empty-body (not an error).
func decodeXML(c *echo.Context, v any) error {
	if err := xml.NewDecoder(c.Request().Body).Decode(v); err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	return nil
}

func writeXML(c *echo.Context, v any) error {
	data, err := xml.Marshal(v)
	if err != nil {
		return c.String(http.StatusInternalServerError, "marshal error")
	}

	return c.Blob(http.StatusOK, "application/xml", append([]byte(xml.Header), data...))
}

// --- public access block handlers ---

type publicAccessBlockConfigurationXML struct {
	XMLName               xml.Name `xml:"PublicAccessBlockConfiguration"`
	BlockPublicAcls       bool     `xml:"BlockPublicAcls"`
	IgnorePublicAcls      bool     `xml:"IgnorePublicAcls"`
	BlockPublicPolicy     bool     `xml:"BlockPublicPolicy"`
	RestrictPublicBuckets bool     `xml:"RestrictPublicBuckets"`
}

func (h *Handler) handleGetPublicAccessBlock(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	cfg, err := h.Backend.GetPublicAccessBlock(accountID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.String(http.StatusNotFound, "NoSuchPublicAccessBlockConfiguration")
		}

		return c.String(http.StatusInternalServerError, err.Error())
	}

	out := publicAccessBlockConfigurationXML{
		BlockPublicAcls:       cfg.BlockPublicAcls,
		IgnorePublicAcls:      cfg.IgnorePublicAcls,
		BlockPublicPolicy:     cfg.BlockPublicPolicy,
		RestrictPublicBuckets: cfg.RestrictPublicBuckets,
	}

	return writeXML(c, out)
}

func (h *Handler) handlePutPublicAccessBlock(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body publicAccessBlockConfigurationXML
	if err := xml.NewDecoder(c.Request().Body).Decode(&body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	h.Backend.PutPublicAccessBlock(PublicAccessBlock{
		AccountID:             accountID,
		BlockPublicAcls:       body.BlockPublicAcls,
		IgnorePublicAcls:      body.IgnorePublicAcls,
		BlockPublicPolicy:     body.BlockPublicPolicy,
		RestrictPublicBuckets: body.RestrictPublicBuckets,
	})

	return c.NoContent(http.StatusCreated)
}

func (h *Handler) handleDeletePublicAccessBlock(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	if err := h.Backend.DeletePublicAccessBlock(accountID); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.String(http.StatusNotFound, "NoSuchPublicAccessBlockConfiguration")
		}

		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
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
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	inst := h.Backend.CreateAccessGrantsInstance(accountID, body.IdentityCenterArn)

	return writeXML(c, createAccessGrantsInstanceResponseXML{
		AccessGrantsInstanceArn:   inst.AccessGrantsInstanceArn,
		AccessGrantsInstanceID:    inst.AccessGrantsInstanceID,
		IdentityCenterArn:         inst.IdentityCenterArn,
		IdentityCenterInstanceArn: inst.IdentityCenterInstanceArn,
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
		return c.String(http.StatusBadRequest, "invalid request body")
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
		return c.String(http.StatusBadRequest, "invalid request body")
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
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	loc := h.Backend.CreateAccessGrantsLocation(accountID, body.LocationScope, body.IAMRoleArn)

	return writeXML(c, createAccessGrantsLocationResponseXML{
		AccessGrantsLocationArn: loc.AccessGrantsLocationArn,
		AccessGrantsLocationID:  loc.AccessGrantsLocationID,
		LocationScope:           loc.LocationScope,
		IAMRoleArn:              loc.IAMRoleArn,
	})
}

// --- CreateAccessPoint handler ---

type createAccessPointRequestXML struct {
	XMLName xml.Name `xml:"CreateAccessPointRequest"`
	Bucket  string   `xml:"Bucket"`
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
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	ap := h.Backend.CreateAccessPoint(accountID, name, body.Bucket)

	return writeXML(c, createAccessPointResponseXML{
		AccessPointArn: ap.AccessPointArn,
		Alias:          ap.Alias,
	})
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

// --- CreateJob handler ---

type createJobRequestXML struct {
	XMLName            xml.Name `xml:"CreateJobRequest"`
	ClientRequestToken string   `xml:"ClientRequestToken"`
	Description        string   `xml:"Description"`
	RoleArn            string   `xml:"RoleArn"`
	Priority           int32    `xml:"Priority"`
}

type createJobResponseXML struct {
	XMLName xml.Name `xml:"CreateJobResult"`
	JobID   string   `xml:"JobId"`
}

func (h *Handler) handleCreateJob(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body createJobRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	job, err := h.Backend.CreateJob(accountID, body.RoleArn, body.Priority)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, createJobResponseXML{
		JobID: job.JobID,
	})
}

// --- CreateMultiRegionAccessPoint handler ---

type createMRAPDetailsXML struct {
	Name string `xml:"Name"`
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
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	req := h.Backend.CreateMultiRegionAccessPoint(accountID, body.Details.Name, body.ClientToken)

	return writeXML(c, createMRAPResponseXML{
		RequestTokenARN: req.RequestTokenARN,
	})
}

// --- CreateStorageLensGroup handler ---

type createStorageLensGroupStorageLensGroupXML struct {
	Name string `xml:"Name"`
}

type createStorageLensGroupRequestXML struct {
	XMLName          xml.Name                                  `xml:"CreateStorageLensGroupRequest"`
	StorageLensGroup createStorageLensGroupStorageLensGroupXML `xml:"StorageLensGroup"`
}

func (h *Handler) handleCreateStorageLensGroup(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body createStorageLensGroupRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	h.Backend.CreateStorageLensGroup(accountID, body.StorageLensGroup.Name)

	return c.NoContent(http.StatusCreated)
}
