package s3control

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	// defaultAccountID is used when no account ID is provided in the request header.
	defaultAccountID = "default"

	// pathPublicAccessBlock is the path suffix for the public access block sub-resource.
	pathPublicAccessBlock = "/configuration/publicAccessBlock"

	// opUnknown is the sentinel for unrecognized operations.
	opUnknown = "Unknown"
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
	ops := supportedAccessGrantOps()
	ops = append(ops, supportedAccessPointAndBucketOps()...)
	ops = append(ops, supportedJobMRAPStorageLensOps()...)

	return ops
}

func supportedAccessGrantOps() []string {
	return []string{
		// Public access block
		"DeletePublicAccessBlock", "GetPublicAccessBlock", "PutPublicAccessBlock",
		// Access Grants Instance
		"AssociateAccessGrantsIdentityCenter", "CreateAccessGrantsInstance",
		"DeleteAccessGrantsInstance", "DeleteAccessGrantsInstanceResourcePolicy",
		"DissociateAccessGrantsIdentityCenter", "GetAccessGrantsInstance",
		"GetAccessGrantsInstanceForPrefix", "GetAccessGrantsInstanceResourcePolicy",
		"ListAccessGrantsInstances", "PutAccessGrantsInstanceResourcePolicy",
		// Access Grants
		"CreateAccessGrant", "DeleteAccessGrant", "GetAccessGrant",
		"GetDataAccess", "ListAccessGrants", "ListCallerAccessGrants",
		// Access Grants Locations
		"CreateAccessGrantsLocation", "DeleteAccessGrantsLocation",
		"GetAccessGrantsLocation", "ListAccessGrantsLocations", "UpdateAccessGrantsLocation",
	}
}

func supportedAccessPointAndBucketOps() []string {
	return []string{
		// Access Points. NOTE: aws-sdk-go-v2/service/s3control has no
		// GetAccessPointPublicAccessBlock / PutAccessPointPublicAccessBlock /
		// DeleteAccessPointPublicAccessBlock operations -- do not add them
		// back. PublicAccessBlockConfiguration for an access point travels
		// inline on CreateAccessPoint/GetAccessPoint instead (see
		// handler_access_points.go).
		"CreateAccessPoint", "DeleteAccessPoint", "GetAccessPoint",
		"GetAccessPointPolicy", "GetAccessPointPolicyStatus",
		"GetAccessPointScope",
		"ListAccessPoints", "ListAccessPointsForDirectoryBuckets",
		"PutAccessPointPolicy", "PutAccessPointScope",
		"DeleteAccessPointPolicy", "DeleteAccessPointScope",
		// Object Lambda Access Points
		"CreateAccessPointForObjectLambda", "DeleteAccessPointForObjectLambda",
		"DeleteAccessPointPolicyForObjectLambda", "GetAccessPointConfigurationForObjectLambda",
		"GetAccessPointForObjectLambda", "GetAccessPointPolicyForObjectLambda",
		"GetAccessPointPolicyStatusForObjectLambda", "ListAccessPointsForObjectLambda",
		"PutAccessPointConfigurationForObjectLambda", "PutAccessPointPolicyForObjectLambda",
		// Outposts Buckets
		"CreateBucket", "DeleteBucket", "DeleteBucketLifecycleConfiguration",
		"DeleteBucketPolicy", "DeleteBucketReplication", "DeleteBucketTagging",
		"GetBucket", "GetBucketLifecycleConfiguration", "GetBucketPolicy",
		"GetBucketReplication", "GetBucketTagging", "GetBucketVersioning",
		"ListRegionalBuckets", "PutBucketLifecycleConfiguration", "PutBucketPolicy",
		"PutBucketReplication", "PutBucketTagging", "PutBucketVersioning",
	}
}

func supportedJobMRAPStorageLensOps() []string {
	return []string{
		// Batch Jobs
		"CreateJob", "DeleteJobTagging", "DescribeJob", "GetJobTagging",
		"ListJobs", "PutJobTagging", "UpdateJobPriority", "UpdateJobStatus",
		// MRAP
		"CreateMultiRegionAccessPoint", "DeleteMultiRegionAccessPoint",
		"DescribeMultiRegionAccessPointOperation", "GetMultiRegionAccessPoint",
		"GetMultiRegionAccessPointPolicy", "GetMultiRegionAccessPointPolicyStatus",
		"GetMultiRegionAccessPointRoutes", "ListMultiRegionAccessPoints",
		"PutMultiRegionAccessPointPolicy", "SubmitMultiRegionAccessPointRoutes",
		// Storage Lens
		"DeleteStorageLensConfiguration", "DeleteStorageLensConfigurationTagging",
		"GetStorageLensConfiguration", "GetStorageLensConfigurationTagging",
		"ListStorageLensConfigurations", "PutStorageLensConfiguration",
		"PutStorageLensConfigurationTagging",
		// Storage Lens Groups
		"CreateStorageLensGroup", "DeleteStorageLensGroup", "GetStorageLensGroup",
		"ListStorageLensGroups", "UpdateStorageLensGroup",
		// Resource Tags
		"ListTagsForResource", "TagResource", "UntagResource",
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
	return h.IAMOpFromRequest(c.Request())
}

// IAMAction returns the IAM action for an S3 Control HTTP request.
func (h *Handler) IAMAction(r *http.Request) string {
	op := h.IAMOpFromRequest(r)
	if op == "" || op == opUnknown {
		return ""
	}

	if strings.HasSuffix(r.URL.Path, pathPublicAccessBlock) {
		return "s3:" + op
	}

	return "s3control:" + op
}

// IAMOpFromRequest extracts the operation name from an HTTP request.
func (h *Handler) IAMOpFromRequest(r *http.Request) string {
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

// isSimplePath returns true if path has the given prefix and the remainder
// contains no "/" (i.e., it is a direct resource path, not a sub-resource).
func isSimplePath(prefix, path string) bool {
	return strings.HasPrefix(path, prefix) &&
		!strings.Contains(strings.TrimPrefix(path, prefix), "/")
}

// isPrefixSuffix returns true if path has the given prefix AND suffix.
func isPrefixSuffix(prefix, path, suffix string) bool {
	return strings.HasPrefix(path, prefix) && strings.HasSuffix(path, suffix)
}

// extractNewOpsOperation routes access grants and access point operations.
func extractNewOpsOperation(path, method string) string {
	if op := extractAccessGrantsInstanceOp(path, method); op != "" {
		return op
	}

	if op := extractAccessGrantsOp(path, method); op != "" {
		return op
	}

	return extractAccessPointOpsOperation(path, method)
}

// extractAccessPointOpsOperation routes access point and object lambda operations.
func extractAccessPointOpsOperation(path, method string) string {
	if op := extractAccessPointCRUDOp(path, method); op != "" {
		return op
	}

	if op := extractObjectLambdaOp(path, method); op != "" {
		return op
	}

	return extractBucketOpsOperation(path, method)
}

// extractBucketOpsOperation routes bucket and sub-resource operations.
func extractBucketOpsOperation(path, method string) string {
	if op := extractBucketCRUDOps(path, method); op != "" {
		return op
	}

	if op := extractBucketSubResourceOps(path, method); op != "" {
		return op
	}

	return extractJobMRAPStorageLensOps(path, method)
}

// extractJobMRAPStorageLensOps routes job, MRAP, and storage lens operations.
func extractJobMRAPStorageLensOps(path, method string) string {
	if op := extractJobOps(path, method); op != "" {
		return op
	}

	if op := extractMRAPOps(path, method); op != "" {
		return op
	}

	return extractStorageLensTagOps(path, method)
}

// extractStorageLensTagOps routes storage lens and tagging operations.
func extractStorageLensTagOps(path, method string) string {
	if op := extractStorageLensOps(path, method); op != "" {
		return op
	}

	return extractTagOps(path, method)
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

	return writeXMLErrorCode(c, http.StatusNotFound, "NotFound", "not found")
}

// dispatchNewOps handles access grants instance and access point operations.
func (h *Handler) dispatchNewOps(c *echo.Context, path, method string) error {
	if handled, err := h.dispatchAccessGrantsInstanceOps(c, path, method); handled {
		return err
	}

	if handled, err := h.dispatchAccessGrantsOps(c, path, method); handled {
		return err
	}

	return h.dispatchAccessPointOps(c, path, method)
}

// dispatchAccessPointOps handles access point and object lambda operations.
func (h *Handler) dispatchAccessPointOps(c *echo.Context, path, method string) error {
	if handled, err := h.dispatchAccessPointCRUDOps(c, path, method); handled {
		return err
	}

	if handled, err := h.dispatchObjectLambdaOps(c, path, method); handled {
		return err
	}

	return h.dispatchBucketOps(c, path, method)
}

// dispatchBucketOps handles bucket CRUD and sub-resource operations.
func (h *Handler) dispatchBucketOps(c *echo.Context, path, method string) error {
	if handled, err := h.dispatchBucketCRUDStubs(c, path, method); handled {
		return err
	}

	if handled, err := h.dispatchBucketSubResourceStubs(c, path, method); handled {
		return err
	}

	return h.dispatchJobMRAPStorageLensOps(c, path, method)
}

// dispatchJobMRAPStorageLensOps handles job, MRAP, and storage lens dispatch.
func (h *Handler) dispatchJobMRAPStorageLensOps(c *echo.Context, path, method string) error {
	if handled, err := h.dispatchJobOps(c, path, method); handled {
		return err
	}

	if handled, err := h.dispatchMRAPDispatchOps(c, path, method); handled {
		return err
	}

	return h.dispatchStorageLensTagOps(c, path, method)
}

// dispatchStorageLensTagOps handles storage lens and tagging dispatch.
func (h *Handler) dispatchStorageLensTagOps(c *echo.Context, path, method string) error {
	if handled, err := h.dispatchStorageLensDispatch(c, path, method); handled {
		return err
	}

	return h.dispatchTagDispatch(c, path, method)
}

func accountIDFromRequest(c *echo.Context) string {
	accountID := c.Request().Header.Get("X-Amz-Account-Id")
	if accountID == "" {
		return defaultAccountID
	}

	return accountID
}

// handleBackendError maps backend sentinel errors to an AWS REST-XML error
// response. Backend errors are created via awserr.New(code, sentinel) (see
// e.g. errAccessPointNotFound in errors.go), so err.Error() IS the
// AWS error code (e.g. "NoSuchAccessPoint") -- it is used as both Code and
// Message since these backend sentinels don't carry a separate human message.
func handleBackendError(c *echo.Context, err error) error {
	if err == nil {
		return nil
	}

	code := err.Error()

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return writeXMLErrorCode(c, http.StatusNotFound, code, code)
	case errors.Is(err, awserr.ErrInvalidParameter):
		return writeXMLErrorCode(c, http.StatusBadRequest, code, code)
	case errors.Is(err, awserr.ErrAlreadyExists):
		return writeXMLErrorCode(c, http.StatusConflict, code, code)
	default:
		return writeXMLErrorCode(c, http.StatusInternalServerError, "InternalServiceException", code)
	}
}

// writeXMLErrorCode writes the <ErrorResponse><Error><Code>/<Message>...
// envelope with the given HTTP status. aws-sdk-go-v2/service/s3control's
// deserializers all call s3shared.GetErrorResponseComponents with
// IsWrappedWithErrorTag: true (deserializers.go, every awsRestxml_deserializeOpError*
// function), which decodes "Error>Code"/"Error>Message" -- i.e. Code/Message
// nested one level under a wrapping root, the same shape ProtocolQueryXML
// already produces (confirmed against s3shared's own wrappedXMLErrorResponse
// test fixture in xml_utils_test.go). ProtocolRestXML's bare top-level
// <Error> (used here previously) has no element matching that "Error>Code"
// path, so every s3control error response decoded as a real client's generic
// smithy.GenericAPIError{Code: "UnknownError"} instead of the real code.
func writeXMLErrorCode(c *echo.Context, status int, code, message string) error {
	return awserr.Write(c, awserr.ProtocolQueryXML, awserr.APIError{
		Code:       code,
		Message:    message,
		HTTPStatus: status,
	})
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
		return writeXMLErrorCode(c, http.StatusInternalServerError, "InternalServiceException", "marshal error")
	}

	return c.Blob(http.StatusOK, "application/xml", append([]byte(xml.Header), data...))
}

// s3cPaginate applies integer-offset pagination over a slice of items.
// It reads an integer offset from nextToken and caps results at maxResults.
func s3cPaginate[T any](items []T, nextToken string, maxResults int) ([]T, string) {
	if len(items) == 0 {
		return items, ""
	}

	start := 0
	if nextToken != "" {
		if idx, err := strconv.Atoi(nextToken); err == nil && idx > 0 && idx < len(items) {
			start = idx
		}
	}

	if maxResults <= 0 {
		return items[start:], ""
	}

	end := start + maxResults
	if end >= len(items) {
		return items[start:], ""
	}

	return items[start:end], strconv.Itoa(end)
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
		return handleBackendError(c, err)
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
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
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
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
