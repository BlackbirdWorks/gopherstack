package glacier

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	opInitiateJob                 = "InitiateJob"
	opDescribeJob                 = "DescribeJob"
	opListJobs                    = "ListJobs"
	opGetJobOutput                = "GetJobOutput"
	opSetVaultNotifications       = "SetVaultNotifications"
	opGetVaultNotifications       = "GetVaultNotifications"
	opDeleteVaultNotifications    = "DeleteVaultNotifications"
	opSetVaultAccessPolicy        = "SetVaultAccessPolicy"
	opGetVaultAccessPolicy        = "GetVaultAccessPolicy"
	opDeleteVaultAccessPolicy     = "DeleteVaultAccessPolicy"
	opAddTagsToVault              = "AddTagsToVault"
	opListTagsForVault            = "ListTagsForVault"
	opRemoveTagsFromVault         = "RemoveTagsFromVault"
	opSetDataRetrievalPolicy      = "SetDataRetrievalPolicy"
	opInitiateMultipartUpload     = "InitiateMultipartUpload"
	opUploadMultipartPart         = "UploadMultipartPart"
	opCompleteMultipartUpload     = "CompleteMultipartUpload"
	opAbortMultipartUpload        = "AbortMultipartUpload"
	opListMultipartUploads        = "ListMultipartUploads"
	opListParts                   = "ListParts"
	opListProvisionedCapacity     = "ListProvisionedCapacity"
	opPurchaseProvisionedCapacity = "PurchaseProvisionedCapacity"
)

const (
	// minVaultPathSegments is the minimum segments in a path to contain a vault name.
	minVaultPathSegments = 3
	// minPoliciesPathSegments is the minimum segments for policies paths.
	minPoliciesPathSegments = 3
	// minRouteSegments is the minimum path segments required to route a request.
	minRouteSegments = 2
	// routeSplitParts is the max split parts when parsing the route prefix.
	routeSplitParts = 3
	// minJobPathSegments is the minimum segments for job paths.
	minJobPathSegments = 5
	// lockIDLength is the length of the generated vault lock ID.
	lockIDLength = 32
	// resourceSplitParts is the max parts when splitting a resource string.
	resourceSplitParts = 2
	// sha256HexLen is the expected byte length of a hex-encoded SHA-256 tree hash.
	sha256HexLen = 64
	// defaultInventoryFormat is the inventory output format used when none is specified.
	defaultInventoryFormat = "JSON"
	// treeHashLeafSize is the block size for SHA-256 tree-hash computation (1 MiB).
	treeHashLeafSize = 1 << 20
	// maxSingleUploadBytes is the maximum body size for a single UploadArchive request (4 GiB).
	maxSingleUploadBytes = 4 << 30
	// maxDescriptionLen is the maximum byte length of an archive description.
	maxDescriptionLen = 1024
	// minDescriptionChar is the minimum printable ASCII char allowed in descriptions.
	minDescriptionChar = 32
	// maxDescriptionChar is the maximum printable ASCII char allowed in descriptions.
	maxDescriptionChar = 126
	// requestIDLength is the number of random chars in an X-Amzn-Requestid value.
	requestIDLength = 32
	// minListLimit is the minimum allowed ?limit value for ListVaults.
	minListLimit = 1
	// maxListVaultsLimit is the maximum allowed ?limit value for ListVaults.
	maxListVaultsLimit = 50
	// defaultListVaultsLimit is ListVaults' documented default when ?limit is omitted.
	defaultListVaultsLimit = 10
	// maxListJobsLimit is the maximum allowed ?limit value for ListJobs.
	maxListJobsLimit = 1000
	// defaultListJobsLimit is ListJobs' documented default when ?limit is omitted.
	defaultListJobsLimit = 50
	// maxListUploadsLimit is the maximum allowed ?limit for ListMultipartUploads / ListParts.
	maxListUploadsLimit = 1000
	// defaultListUploadsLimit is ListMultipartUploads'/ListParts' documented default
	// when ?limit is omitted.
	defaultListUploadsLimit = 50
	// maxVaultNameLen is the maximum length of a vault name.
	maxVaultNameLen = 255
)

const (
	// opGetDataRetrievalPolicy is the operation name for GetDataRetrievalPolicy.
	opGetDataRetrievalPolicy = "GetDataRetrievalPolicy"
	// opInitiateVaultLock is the operation name for InitiateVaultLock.
	opInitiateVaultLock = "InitiateVaultLock"
	// opAbortVaultLock is the operation name for AbortVaultLock.
	opAbortVaultLock = "AbortVaultLock"
	// opCompleteVaultLock is the operation name for CompleteVaultLock.
	opCompleteVaultLock = "CompleteVaultLock"
	// opGetVaultLock is the operation name for GetVaultLock.
	opGetVaultLock = "GetVaultLock"

	opCreateVault   = "CreateVault"
	opDescribeVault = "DescribeVault"
	opDeleteVault   = "DeleteVault"
	opListVaults    = "ListVaults"
	opUploadArchive = "UploadArchive"
	opDeleteArchive = "DeleteArchive"
)

// Handler is the HTTP handler for the Glacier REST API.
type Handler struct {
	Backend       StorageBackend
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new Glacier handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend:       backend,
		DefaultRegion: "us-east-1",
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Glacier" }

// GetSupportedOperations returns the list of supported Glacier operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateVault,
		opDescribeVault,
		opDeleteVault,
		opListVaults,
		opUploadArchive,
		opDeleteArchive,
		opInitiateJob,
		opDescribeJob,
		opListJobs,
		opGetJobOutput,
		opSetVaultNotifications,
		opGetVaultNotifications,
		opDeleteVaultNotifications,
		opSetVaultAccessPolicy,
		opGetVaultAccessPolicy,
		opDeleteVaultAccessPolicy,
		opAddTagsToVault,
		opListTagsForVault,
		opRemoveTagsFromVault,
		"InitiateVaultLock",
		"AbortVaultLock",
		"CompleteVaultLock",
		"GetVaultLock",
		"GetDataRetrievalPolicy",
		opSetDataRetrievalPolicy,
		opInitiateMultipartUpload,
		opUploadMultipartPart,
		opCompleteMultipartUpload,
		opAbortMultipartUpload,
		opListMultipartUploads,
		opListParts,
		opListProvisionedCapacity,
		opPurchaseProvisionedCapacity,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "glacier" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Glacier instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches Glacier REST API requests.
// Glacier uses paths like /{accountId}/vaults/... where accountId is "-" or a real account ID.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path
		segs := strings.SplitN(strings.TrimPrefix(path, "/"), "/", routeSplitParts)

		if len(segs) < minRouteSegments {
			return false
		}

		// Check that the second segment is "vaults", "policies", or "provisioned-capacity"
		// Glacier paths: /{accountId}/vaults, /{accountId}/policies, /{accountId}/provisioned-capacity
		return segs[1] == "vaults" || segs[1] == "policies" || segs[1] == "provisioned-capacity"
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the Glacier operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := parseGlacierPath(c.Request().Method, c.Request().URL.Path, c.Request().URL.RawQuery)

	return op
}

// ExtractResource extracts the vault name or resource ID from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	segs := strings.Split(strings.TrimPrefix(c.Request().URL.Path, "/"), "/")
	if len(segs) >= minVaultPathSegments {
		return segs[2]
	}

	return ""
}

// Handler returns the Echo handler function for Glacier requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		c.Response().Header().Set("X-Amzn-Requestid", generateID(requestIDLength))

		method := c.Request().Method
		path := c.Request().URL.Path
		query := c.Request().URL.RawQuery

		op, resource := parseGlacierPath(method, path, query)
		if op == "" {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "not found")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "glacier: failed to read request body", "error", err)

			return h.writeError(
				c,
				http.StatusInternalServerError,
				"ServiceUnavailableException",
				"failed to read request body",
			)
		}

		log.DebugContext(ctx, "glacier request", "op", op, "resource", resource)

		return h.dispatch(c, op, resource, body)
	}
}

// resolveAccountID returns h.AccountID when pathAccountID is "-" or empty,
// otherwise returns pathAccountID verbatim (multi-account / STS scenarios).
func (h *Handler) resolveAccountID(pathAccountID string) string {
	if pathAccountID == "-" || pathAccountID == "" {
		return h.AccountID
	}

	return pathAccountID
}

// parseGlacierPath parses a Glacier HTTP method + path into an operation name and resource key.
//

func parseGlacierPath(method, path, query string) (string, string) {
	// Path format: /{accountId}/vaults/{vaultName}[/subresource[/id][/output]]
	segs := strings.Split(strings.TrimPrefix(path, "/"), "/")

	if len(segs) < minRouteSegments {
		return "", ""
	}

	accountID := segs[0]
	topLevel := segs[1]

	if topLevel == "policies" {
		return parsePoliciesPath(method, segs)
	}

	if topLevel == "provisioned-capacity" {
		return parseProvisionedCapacityPath(method, accountID)
	}

	if topLevel != "vaults" {
		return "", ""
	}

	// /{accountId}/vaults
	if len(segs) == 2 { //nolint:mnd // exactly 2 segments means list vaults
		if method == http.MethodGet {
			return opListVaults, accountID
		}

		return "", ""
	}

	vaultName := segs[2]

	// /{accountId}/vaults/{vaultName}
	if len(segs) == minVaultPathSegments {
		switch method {
		case http.MethodPut:
			return opCreateVault, vaultName
		case http.MethodGet:
			return opDescribeVault, vaultName
		case http.MethodDelete:
			return opDeleteVault, vaultName
		}

		return "", ""
	}

	subPath := segs[3]

	return parseVaultSubPath(method, segs, vaultName, subPath, query)
}

// parsePoliciesPath handles /{accountId}/policies/* paths.
func parsePoliciesPath(method string, segs []string) (string, string) {
	if len(segs) < minPoliciesPathSegments {
		return "", ""
	}

	if segs[2] == "data-retrieval" {
		switch method {
		case http.MethodGet:
			return "GetDataRetrievalPolicy", ""
		case http.MethodPut:
			return opSetDataRetrievalPolicy, ""
		}
	}

	return "", ""
}

// parseProvisionedCapacityPath handles /{accountId}/provisioned-capacity.
func parseProvisionedCapacityPath(method, accountID string) (string, string) {
	switch method {
	case http.MethodGet:
		return opListProvisionedCapacity, accountID
	case http.MethodPost:
		return opPurchaseProvisionedCapacity, accountID
	}

	return "", ""
}

// parseVaultSubPath handles paths beyond /{accountId}/vaults/{vaultName}/.
//

func parseVaultSubPath(
	method string,
	segs []string,
	vaultName, subPath, query string,
) (string, string) {
	switch subPath {
	case "archives":
		return parseArchivesPath(method, segs, vaultName)
	case "jobs":
		return parseJobsPath(method, segs, vaultName)
	case "multipart-uploads":
		return parseMultipartUploadsPath(method, segs, vaultName)
	case "tags":
		return parseTagsPath(method, query, vaultName)
	case "notification-configuration":
		return parseNotificationPath(method, vaultName)
	case "access-policy":
		return parseAccessPolicyPath(method, vaultName)
	case "lock-policy":
		return parseLockPolicyPath(method, segs, vaultName)
	}

	return "", ""
}

// parseArchivesPath handles /{accountId}/vaults/{vaultName}/archives[/{archiveId}].
func parseArchivesPath(method string, segs []string, vaultName string) (string, string) {
	if len(segs) == 4 { //nolint:mnd // 4 segs = /account/vaults/name/archives
		if method == http.MethodPost {
			return opUploadArchive, vaultName
		}

		return "", ""
	}

	archiveID := segs[4]

	if method == http.MethodDelete {
		return opDeleteArchive, vaultName + "/" + archiveID
	}

	return "", ""
}

// parseJobsPath handles /{accountId}/vaults/{vaultName}/jobs[/{jobId}[/output]].
func parseJobsPath(method string, segs []string, vaultName string) (string, string) {
	if len(segs) == 4 { //nolint:mnd // 4 segs = /account/vaults/name/jobs
		switch method {
		case http.MethodPost:
			return opInitiateJob, vaultName
		case http.MethodGet:
			return opListJobs, vaultName
		}

		return "", ""
	}

	jobID := segs[4]

	if len(segs) == minJobPathSegments {
		if method == http.MethodGet {
			return opDescribeJob, vaultName + "/" + jobID
		}

		return "", ""
	}

	if len(segs) >= 6 && segs[5] == "output" {
		if method == http.MethodGet {
			return opGetJobOutput, vaultName + "/" + jobID
		}
	}

	return "", ""
}

// parseMultipartUploadsPath handles /{accountId}/vaults/{vaultName}/multipart-uploads[/{uploadId}].
func parseMultipartUploadsPath(method string, segs []string, vaultName string) (string, string) {
	if len(segs) == 4 { //nolint:mnd // 4 segs = /account/vaults/name/multipart-uploads
		switch method {
		case http.MethodPost:
			return opInitiateMultipartUpload, vaultName
		case http.MethodGet:
			return opListMultipartUploads, vaultName
		}

		return "", ""
	}

	uploadID := segs[4]

	switch method {
	case http.MethodPut:
		return opUploadMultipartPart, vaultName + "/" + uploadID
	case http.MethodPost:
		return opCompleteMultipartUpload, vaultName + "/" + uploadID
	case http.MethodDelete:
		return opAbortMultipartUpload, vaultName + "/" + uploadID
	case http.MethodGet:
		return opListParts, vaultName + "/" + uploadID
	}

	return "", ""
}

// parseTagsPath handles /{accountId}/vaults/{vaultName}/tags?operation=add|remove.
func parseTagsPath(method, query, vaultName string) (string, string) {
	switch method {
	case http.MethodPost:
		if strings.Contains(query, "operation=add") {
			return opAddTagsToVault, vaultName
		}

		if strings.Contains(query, "operation=remove") {
			return opRemoveTagsFromVault, vaultName
		}
	case http.MethodGet:
		return opListTagsForVault, vaultName
	}

	return "", ""
}

// parseNotificationPath handles /{accountId}/vaults/{vaultName}/notification-configuration.
func parseNotificationPath(method, vaultName string) (string, string) {
	switch method {
	case http.MethodPut:
		return opSetVaultNotifications, vaultName
	case http.MethodGet:
		return opGetVaultNotifications, vaultName
	case http.MethodDelete:
		return opDeleteVaultNotifications, vaultName
	}

	return "", ""
}

// parseAccessPolicyPath handles /{accountId}/vaults/{vaultName}/access-policy.
func parseAccessPolicyPath(method, vaultName string) (string, string) {
	switch method {
	case http.MethodPut:
		return opSetVaultAccessPolicy, vaultName
	case http.MethodGet:
		return opGetVaultAccessPolicy, vaultName
	case http.MethodDelete:
		return opDeleteVaultAccessPolicy, vaultName
	}

	return "", ""
}

// parseLockPolicyPath handles /{accountId}/vaults/{vaultName}/lock-policy[/{lockId}].
func parseLockPolicyPath(method string, segs []string, vaultName string) (string, string) {
	if len(segs) == 4 { //nolint:mnd // 4 segs = /account/vaults/name/lock-policy
		switch method {
		case http.MethodGet:
			return opGetVaultLock, vaultName
		case http.MethodPost:
			return opInitiateVaultLock, vaultName
		case http.MethodDelete:
			return opAbortVaultLock, vaultName
		}

		return "", ""
	}

	if len(segs) >= 5 && method == http.MethodPost {
		return opCompleteVaultLock, vaultName + "/" + segs[4]
	}

	return "", ""
}

// extractVaultName extracts just the vault name from a resource string (which may be "vaultName/id").
func extractVaultName(resource string) string {
	parts := strings.SplitN(resource, "/", resourceSplitParts)

	return parts[0]
}

// extractSubID extracts the second part of a resource string "vaultName/id".
func extractSubID(resource string) string {
	parts := strings.SplitN(resource, "/", resourceSplitParts)
	if len(parts) < resourceSplitParts {
		return ""
	}

	return parts[1]
}

// dispatchVaultOps routes vault CRUD operations.
func (h *Handler) dispatchVaultOps(c *echo.Context, op, resource string) (bool, error) {
	switch op {
	case opCreateVault:
		return true, h.handleCreateVault(c, resource)
	case opDescribeVault:
		return true, h.handleDescribeVault(c, resource)
	case opDeleteVault:
		return true, h.handleDeleteVault(c, resource)
	case opListVaults:
		return true, h.handleListVaults(c, resource)
	}

	return false, nil
}

// dispatchArchiveAndJobOps routes archive and job operations.
func (h *Handler) dispatchArchiveAndJobOps(
	c *echo.Context,
	op, resource string,
	body []byte,
) (bool, error) {
	switch op {
	case opUploadArchive:
		return true, h.handleUploadArchive(c, resource, body)
	case opDeleteArchive:
		return true, h.handleDeleteArchive(c, extractVaultName(resource), extractSubID(resource))
	case opInitiateJob:
		return true, h.handleInitiateJob(c, resource, body)
	case opDescribeJob:
		return true, h.handleDescribeJob(c, extractVaultName(resource), extractSubID(resource))
	case opListJobs:
		return true, h.handleListJobs(c, resource)
	case opGetJobOutput:
		return true, h.handleGetJobOutput(c, extractVaultName(resource), extractSubID(resource))
	}

	return false, nil
}

// dispatchTagsAndPoliciesOps routes tag, notification, and access-policy operations.
func (h *Handler) dispatchTagsAndPoliciesOps(
	c *echo.Context,
	op, resource string,
	body []byte,
) (bool, error) {
	switch op {
	case opSetVaultNotifications:
		return true, h.handleSetVaultNotifications(c, resource, body)
	case opGetVaultNotifications:
		return true, h.handleGetVaultNotifications(c, resource)
	case opDeleteVaultNotifications:
		return true, h.handleDeleteVaultNotifications(c, resource)
	case opSetVaultAccessPolicy:
		return true, h.handleSetVaultAccessPolicy(c, resource, body)
	case opGetVaultAccessPolicy:
		return true, h.handleGetVaultAccessPolicy(c, resource)
	case opDeleteVaultAccessPolicy:
		return true, h.handleDeleteVaultAccessPolicy(c, resource)
	case opAddTagsToVault:
		return true, h.handleAddTagsToVault(c, resource, body)
	case opListTagsForVault:
		return true, h.handleListTagsForVault(c, resource)
	case opRemoveTagsFromVault:
		return true, h.handleRemoveTagsFromVault(c, resource, body)
	}

	return false, nil
}

// dispatchMultipartAndCapacityOps routes multipart upload and provisioned capacity operations.
func (h *Handler) dispatchMultipartAndCapacityOps(
	c *echo.Context,
	op, resource string,
	body []byte,
) (bool, error) {
	switch op {
	case opInitiateMultipartUpload:
		return true, h.handleInitiateMultipartUpload(c, resource, body)
	case opUploadMultipartPart:
		return true, h.handleUploadMultipartPart(
			c,
			extractVaultName(resource),
			extractSubID(resource),
			body,
		)
	case opCompleteMultipartUpload:
		return true, h.handleCompleteMultipartUpload(
			c,
			extractVaultName(resource),
			extractSubID(resource),
			body,
		)
	case opAbortMultipartUpload:
		return true, h.handleAbortMultipartUpload(
			c,
			extractVaultName(resource),
			extractSubID(resource),
		)
	case opListMultipartUploads:
		return true, h.handleListMultipartUploads(c, resource)
	case opListParts:
		return true, h.handleListParts(c, extractVaultName(resource), extractSubID(resource))
	case opListProvisionedCapacity:
		return true, h.handleListProvisionedCapacity(c, resource)
	case opPurchaseProvisionedCapacity:
		return true, h.handlePurchaseProvisionedCapacity(c, resource)
	}

	return false, nil
}

// dispatch routes a parsed operation to the appropriate handler.
func (h *Handler) dispatch(c *echo.Context, op, resource string, body []byte) error {
	if handled, err := h.dispatchVaultOps(c, op, resource); handled {
		return err
	}

	if handled, err := h.dispatchArchiveAndJobOps(c, op, resource, body); handled {
		return err
	}

	if handled, err := h.dispatchTagsAndPoliciesOps(c, op, resource, body); handled {
		return err
	}

	switch op {
	case opInitiateVaultLock, opAbortVaultLock, opCompleteVaultLock, opGetVaultLock:
		return h.handleVaultLock(c, op, resource, body)
	case opGetDataRetrievalPolicy, opSetDataRetrievalPolicy:
		return h.handleDataRetrievalPolicy(c, op, body)
	}

	if handled, err := h.dispatchMultipartAndCapacityOps(c, op, resource, body); handled {
		return err
	}

	return h.writeError(
		c,
		http.StatusNotFound,
		"ResourceNotFoundException",
		"unknown operation: "+op,
	)
}

func encodeMarker(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

func decodeMarker(s string) string {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return s
	}

	return string(b)
}

// validateDescription returns an error if s contains non-printable ASCII or exceeds 1024 bytes.
func validateDescription(s string) error {
	if len(s) > maxDescriptionLen {
		return fmt.Errorf("%w: exceeds %d characters", ErrDescriptionTooLong, maxDescriptionLen)
	}

	for i := range len(s) {
		if s[i] < minDescriptionChar || s[i] > maxDescriptionChar {
			return fmt.Errorf("%w: 0x%02x at position %d", ErrDescriptionChar, s[i], i)
		}
	}

	return nil
}

// writeError writes a Glacier-format JSON error response.
// Both "code" and "__type" are set so AWS SDK versions that key on either field work correctly.
func (h *Handler) writeError(c *echo.Context, status int, code, message string) error {
	return c.JSON(status, errorResponse{
		Code:      code,
		Message:   message,
		Type:      "Client",
		TypeAlias: code,
	})
}

// writeBackendError maps a backend error to an HTTP error response.
func (h *Handler) writeBackendError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrVaultNotFound):
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	case errors.Is(err, ErrArchiveNotFound):
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	case errors.Is(err, ErrJobNotFound):
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	case errors.Is(err, ErrUploadNotFound):
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	case errors.Is(err, ErrVaultNotEmpty):
		return h.writeError(c, http.StatusConflict, "ConflictException", err.Error())
	case errors.Is(err, ErrResourceInUse):
		return h.writeError(c, http.StatusConflict, "ResourceInUseException", err.Error())
	case errors.Is(err, ErrLockConflict):
		return h.writeError(c, http.StatusConflict, "InvalidParameterValueException", err.Error())
	case errors.Is(err, ErrLockAlreadyLocked):
		return h.writeError(c, http.StatusConflict, "InvalidParameterValueException", err.Error())
	case errors.Is(err, ErrTooManyTags):
		return h.writeError(c, http.StatusBadRequest, "LimitExceededException", err.Error())
	case errors.Is(err, ErrProvisionedCapacityLimit):
		return h.writeError(c, http.StatusBadRequest, "LimitExceededException", err.Error())
	case errors.Is(err, ErrInvalidTag):
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	case errors.Is(err, ErrValidation):
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	case errors.Is(err, ErrMissingParameter):
		return h.writeError(c, http.StatusBadRequest, "MissingParameterValueException", err.Error())
	case errors.Is(err, ErrVaultLockDenied):
		return h.writeError(c, http.StatusForbidden, "AccessDeniedException", err.Error())
	}

	return h.writeError(
		c,
		http.StatusInternalServerError,
		"ServiceUnavailableException",
		err.Error(),
	)
}

// Reset clears all backend state and the handler-level archive data store.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
