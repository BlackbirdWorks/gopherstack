package glacier

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

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
	// maxListJobsLimit is the maximum allowed ?limit value for ListJobs.
	maxListJobsLimit = 1000
	// maxListUploadsLimit is the maximum allowed ?limit for ListMultipartUploads / ListParts.
	maxListUploadsLimit = 1000
)

// Handler-level sentinel errors used as wrapping targets to satisfy err113.
var (
	// ErrDescriptionTooLong is returned when an archive description exceeds maxDescriptionLen.
	ErrDescriptionTooLong = errors.New("description too long")
	// ErrDescriptionChar is returned when an archive description contains a non-printable character.
	ErrDescriptionChar = errors.New("description contains invalid character")
	// ErrLimitOutOfRange is returned when a ?limit query param is out of the allowed range.
	ErrLimitOutOfRange = errors.New("limit out of range")
	// ErrInvalidStrategy is returned when a DataRetrievalPolicy strategy is not recognised.
	ErrInvalidStrategy = errors.New("invalid data retrieval strategy")
	// ErrBytesPerHourRequired is returned when BytesPerHour strategy omits the BytesPerHour value.
	ErrBytesPerHourRequired = errors.New(
		"BytesPerHour strategy requires a positive BytesPerHour value",
	)
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
	archiveData   map[string][]byte
	AccountID     string
	DefaultRegion string
	archiveMu     sync.RWMutex
}

// NewHandler creates a new Glacier handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend:     backend,
		archiveData: make(map[string][]byte),
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

// ----------------------------------------
// Vault handlers
// ----------------------------------------

func (h *Handler) handleCreateVault(c *echo.Context, vaultName string) error {
	v, err := h.Backend.CreateVault(h.AccountID, h.DefaultRegion, vaultName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	c.Response().Header().Set("Location", vaultLocation(h.AccountID, vaultName))
	c.Response().Header().Set("X-Amzn-Requestid", "glacier-create-vault")

	return c.JSON(http.StatusCreated, createVaultResponse{
		Location: vaultLocation(h.AccountID, v.VaultName),
	})
}

func (h *Handler) handleDescribeVault(c *echo.Context, vaultName string) error {
	v, err := h.Backend.DescribeVault(h.AccountID, h.DefaultRegion, vaultName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, toDescribeVaultResponse(v))
}

func (h *Handler) handleDeleteVault(c *echo.Context, vaultName string) error {
	if err := h.Backend.DeleteVault(h.AccountID, h.DefaultRegion, vaultName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListVaults(c *echo.Context, accountID string) error {
	resolved := h.resolveAccountID(accountID)
	vaults := h.Backend.ListVaults(resolved, h.DefaultRegion)
	items := make([]describeVaultResponse, 0, len(vaults))

	for _, v := range vaults {
		items = append(items, toDescribeVaultResponse(v))
	}

	// Support `marker` pagination: start listing after this vault name.
	marker := c.QueryParam("marker")

	if marker != "" {
		start := 0

		for start < len(items) && items[start].VaultName != marker {
			start++
		}

		if start < len(items) {
			items = items[start+1:]
		} else {
			items = nil
		}
	}

	// Support `limit` to cap the number of results returned. AWS: 1-50.
	limitStr := c.QueryParam("limit")

	var nextMarker *string

	if limitStr != "" {
		n, err := strconv.Atoi(limitStr)
		if err != nil || n < minListLimit || n > maxListVaultsLimit {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidParameterValueException",
				fmt.Sprintf(
					"%v: must be between %d and %d",
					ErrLimitOutOfRange,
					minListLimit,
					maxListVaultsLimit,
				),
			)
		}

		if n < len(items) {
			last := items[n-1].VaultName
			nextMarker = &last
			items = items[:n]
		}
	}

	return c.JSON(http.StatusOK, listVaultsResponse{
		Marker:    nextMarker,
		VaultList: items,
	})
}

// toDescribeVaultResponse converts a vault to a describe vault response.
func toDescribeVaultResponse(v *Vault) describeVaultResponse {
	return describeVaultResponse{
		VaultARN:          v.VaultARN,
		VaultName:         v.VaultName,
		CreationDate:      v.CreationDate,
		LastInventoryDate: v.LastInventoryDate,
		NumberOfArchives:  v.NumberOfArchives,
		SizeInBytes:       v.SizeInBytes,
	}
}

// ----------------------------------------
// Archive handlers
// ----------------------------------------

func (h *Handler) handleUploadArchive(c *echo.Context, vaultName string, body []byte) error {
	// Enforce 4 GiB single-upload limit before allocating.
	if int64(len(body)) > maxSingleUploadBytes {
		return h.writeError(c, http.StatusRequestEntityTooLarge, "InvalidParameterValueException",
			"archive exceeds maximum single-upload size of 4 GiB")
	}

	description := c.Request().Header.Get("X-Amz-Archive-Description")
	if err := validateDescription(description); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	clientChecksum := c.Request().Header.Get("X-Amz-Sha256-Tree-Hash")

	// Compute the real tree-hash from the body.
	computed := computeTreeHash(body)

	// If the client supplied a checksum, verify it matches.
	if clientChecksum != "" {
		if len(clientChecksum) != sha256HexLen {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
				"X-Amz-Sha256-Tree-Hash must be a 64-character hex string")
		}

		if _, hexErr := hex.DecodeString(clientChecksum); hexErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
				"X-Amz-Sha256-Tree-Hash contains invalid hex characters")
		}

		if clientChecksum != computed {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
				"X-Amz-Sha256-Tree-Hash mismatch: computed "+computed)
		}
	}

	size := int64(len(body))

	a, err := h.Backend.UploadArchive(
		h.AccountID,
		h.DefaultRegion,
		vaultName,
		description,
		computed,
		size,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	// Store archive bytes so ArchiveRetrieval job output can return them.
	if len(body) > 0 {
		h.archiveMu.Lock()
		h.archiveData[a.ArchiveID] = body
		h.archiveMu.Unlock()
	}

	location := "/" + h.AccountID + "/vaults/" + vaultName + "/archives/" + a.ArchiveID

	c.Response().Header().Set("X-Amz-Archive-Id", a.ArchiveID)
	c.Response().Header().Set("X-Amz-Sha256-Tree-Hash", a.SHA256TreeHash)
	c.Response().Header().Set("Location", location)

	return c.JSON(http.StatusCreated, uploadArchiveResponse{
		ArchiveID: a.ArchiveID,
		Checksum:  a.SHA256TreeHash,
		Location:  location,
	})
}

// computeLeafHashes returns SHA-256 hashes of successive 1 MiB blocks of data.
func computeLeafHashes(data []byte) [][]byte {
	if len(data) == 0 {
		h := sha256.Sum256(nil)

		return [][]byte{h[:]}
	}

	var hashes [][]byte

	for i := 0; i < len(data); i += treeHashLeafSize {
		end := min(i+treeHashLeafSize, len(data))
		sum := sha256.Sum256(data[i:end])
		hashes = append(hashes, sum[:])
	}

	return hashes
}

// reduceTreeHashes iteratively pair-hashes adjacent entries until one remains.
func reduceTreeHashes(hashes [][]byte) []byte {
	const pairStep = 2

	for len(hashes) > 1 {
		next := make([][]byte, 0, (len(hashes)+1)/pairStep)

		for i := 0; i < len(hashes); i += pairStep {
			if i+1 >= len(hashes) {
				next = append(next, hashes[i])

				continue
			}

			combined := make([]byte, len(hashes[i])+len(hashes[i+1]))
			copy(combined, hashes[i])
			copy(combined[len(hashes[i]):], hashes[i+1])
			sum := sha256.Sum256(combined)
			next = append(next, sum[:])
		}

		hashes = next
	}

	return hashes[0]
}

// computeTreeHash returns the SHA-256 tree-hash of data as a lowercase hex string.
func computeTreeHash(data []byte) string {
	leaves := computeLeafHashes(data)

	return hex.EncodeToString(reduceTreeHashes(leaves))
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

func (h *Handler) handleDeleteArchive(c *echo.Context, vaultName, archiveID string) error {
	if err := h.Backend.DeleteArchive(h.AccountID, h.DefaultRegion, vaultName, archiveID); err != nil {
		return h.writeBackendError(c, err)
	}

	// Remove stored bytes so they don't accumulate in memory.
	h.archiveMu.Lock()
	delete(h.archiveData, archiveID)
	h.archiveMu.Unlock()

	return c.NoContent(http.StatusNoContent)
}

// ----------------------------------------
// Job handlers
// ----------------------------------------

func (h *Handler) handleInitiateJob(c *echo.Context, vaultName string, body []byte) error {
	var req initiateJobRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"invalid request body: "+err.Error(),
		)
	}

	j, err := h.Backend.InitiateJob(h.AccountID, h.DefaultRegion, vaultName, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	location := "/" + h.AccountID + "/vaults/" + vaultName + "/jobs/" + j.JobID

	c.Response().Header().Set("X-Amz-Job-Id", j.JobID)
	c.Response().Header().Set("Location", location)

	return c.JSON(http.StatusAccepted, initiateJobResponse{
		JobID:    j.JobID,
		Location: location,
	})
}

func (h *Handler) handleDescribeJob(c *echo.Context, vaultName, jobID string) error {
	j, err := h.Backend.DescribeJob(h.AccountID, h.DefaultRegion, vaultName, jobID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, toDescribeJobResponse(j))
}

func (h *Handler) handleListJobs(c *echo.Context, vaultName string) error {
	jobs, err := h.Backend.ListJobs(h.AccountID, h.DefaultRegion, vaultName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	// Optional query filters: ?completed=true|false and ?statuscode=InProgress|Succeeded|Failed
	completedFilter := c.QueryParam("completed")
	statuscodeFilter := c.QueryParam("statuscode")

	items := make([]describeJobResponse, 0, len(jobs))

	for _, j := range jobs {
		if completedFilter != "" {
			want := completedFilter == "true"
			if j.Completed != want {
				continue
			}
		}

		if statuscodeFilter != "" && j.StatusCode != statuscodeFilter {
			continue
		}

		items = append(items, toDescribeJobResponse(j))
	}

	items, nextMarker, pErr := paginateJobList(c, items)
	if pErr != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			pErr.Error(),
		)
	}

	return c.JSON(http.StatusOK, listJobsResponse{
		Marker:  nextMarker,
		JobList: items,
	})
}

// paginateJobList applies marker+limit pagination to a slice of job responses.
func paginateJobList(
	c *echo.Context,
	items []describeJobResponse,
) ([]describeJobResponse, *string, error) {
	if marker := c.QueryParam("marker"); marker != "" {
		start := 0

		for start < len(items) && items[start].JobID != marker {
			start++
		}

		if start < len(items) {
			items = items[start+1:]
		} else {
			items = nil
		}
	}

	limitStr := c.QueryParam("limit")
	if limitStr == "" {
		return items, nil, nil
	}

	n, err := strconv.Atoi(limitStr)
	if err != nil || n < minListLimit || n > maxListJobsLimit {
		return nil, nil, fmt.Errorf(
			"%w: must be between %d and %d",
			ErrLimitOutOfRange,
			minListLimit,
			maxListJobsLimit,
		)
	}

	if n >= len(items) {
		return items, nil, nil
	}

	last := items[n-1].JobID

	return items[:n], &last, nil
}

func (h *Handler) handleGetJobOutput(c *echo.Context, vaultName, jobID string) error {
	j, err := h.Backend.DescribeJob(h.AccountID, h.DefaultRegion, vaultName, jobID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	if j.SHA256TreeHash != "" {
		c.Response().Header().Set("X-Amz-Sha256-Tree-Hash", j.SHA256TreeHash)
	}

	c.Response().Header().Set("Accept-Ranges", "bytes")

	if j.Action == jobTypeInventoryRetrieval {
		return h.handleInventoryJobOutput(c, j, vaultName)
	}

	return h.handleArchiveJobOutput(c, j)
}

// handleInventoryJobOutput returns the vault inventory as JSON or CSV.
func (h *Handler) handleInventoryJobOutput(c *echo.Context, j *Job, vaultName string) error {
	archives, listErr := h.Backend.ListArchives(h.AccountID, h.DefaultRegion, vaultName)
	if listErr != nil {
		archives = []*Archive{} // degrade gracefully
	}

	if j.InventoryFormat != "" && j.InventoryFormat != defaultInventoryFormat {
		return h.writeInventoryCSV(c, j, archives)
	}

	return h.writeInventoryJSON(c, j, archives)
}

type inventoryArchiveItem struct {
	ArchiveID          string `json:"ArchiveId"`
	ArchiveDescription string `json:"ArchiveDescription"`
	CreationDate       string `json:"CreationDate"`
	SHA256TreeHash     string `json:"SHA256TreeHash"`
	Size               int64  `json:"Size"`
}

func (h *Handler) writeInventoryJSON(c *echo.Context, j *Job, archives []*Archive) error {
	items := make([]inventoryArchiveItem, 0, len(archives))

	for _, a := range archives {
		items = append(items, inventoryArchiveItem{
			ArchiveID:          a.ArchiveID,
			ArchiveDescription: a.Description,
			CreationDate:       a.CreationDate,
			Size:               a.Size,
			SHA256TreeHash:     a.SHA256TreeHash,
		})
	}

	payload, err := json.Marshal(map[string]any{
		"VaultARN":      j.VaultARN,
		"InventoryDate": j.CompletionDate,
		"ArchiveList":   items,
	})
	if err != nil {
		return h.writeError(
			c,
			http.StatusInternalServerError,
			"ServiceUnavailableException",
			"failed to encode inventory",
		)
	}

	c.Response().Header().Set("Content-Type", "application/json")
	c.Response().
		Header().
		Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)))

	return h.serveWithRange(c, payload)
}

func (h *Handler) writeInventoryCSV(c *echo.Context, j *Job, archives []*Archive) error {
	var buf bytes.Buffer

	buf.WriteString("ArchiveId,ArchiveDescription,CreationDate,Size,SHA256TreeHash\n")

	for _, a := range archives {
		fmt.Fprintf(&buf, "%s,%q,%s,%d,%s\n",
			a.ArchiveID, a.Description, a.CreationDate, a.Size, a.SHA256TreeHash)
	}

	payload := buf.Bytes()

	c.Response().Header().Set("Content-Type", "text/csv")
	c.Response().
		Header().
		Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)))

	_ = j // suppress unused warning

	return h.serveWithRange(c, payload)
}

// handleArchiveJobOutput streams stored archive bytes with Range support.
func (h *Handler) handleArchiveJobOutput(c *echo.Context, j *Job) error {
	c.Response().Header().Set("Content-Type", "application/octet-stream")

	h.archiveMu.RLock()
	data, hasData := h.archiveData[j.ArchiveID]
	h.archiveMu.RUnlock()

	if !hasData {
		// Archive data not stored (uploaded before handler restart). Return empty stub.
		if j.ArchiveSizeInBytes > 0 {
			c.Response().Header().Set(
				"Content-Range",
				fmt.Sprintf("bytes 0-%d/%d", j.ArchiveSizeInBytes-1, j.ArchiveSizeInBytes),
			)
		}

		return c.NoContent(http.StatusOK)
	}

	c.Response().Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data)))

	return h.serveWithRange(c, data)
}

// serveWithRange serves payload with optional HTTP Range support.
func (h *Handler) serveWithRange(c *echo.Context, payload []byte) error {
	rangeHeader := c.Request().Header.Get("Range")

	if rangeHeader == "" {
		c.Response().WriteHeader(http.StatusOK)
		_, err := io.Copy(c.Response(), bytes.NewReader(payload))

		return err
	}

	// Parse "bytes=start-end" range header.
	const rangePrefix = "bytes="
	if !strings.HasPrefix(rangeHeader, rangePrefix) {
		return h.writeError(
			c,
			http.StatusRequestedRangeNotSatisfiable,
			"InvalidRange",
			"invalid Range header",
		)
	}

	const rangeParts = 2 // start and end
	parts := strings.SplitN(rangeHeader[len(rangePrefix):], "-", rangeParts)
	if len(parts) != rangeParts {
		return h.writeError(
			c,
			http.StatusRequestedRangeNotSatisfiable,
			"InvalidRange",
			"invalid Range header",
		)
	}

	start, err1 := strconv.ParseInt(parts[0], 10, 64)
	end, err2 := strconv.ParseInt(parts[1], 10, 64)

	total := int64(len(payload))

	if err1 != nil || err2 != nil || start < 0 || end < start || end >= total {
		return h.writeError(c, http.StatusRequestedRangeNotSatisfiable, "InvalidRange",
			fmt.Sprintf("Range %s not satisfiable for %d-byte resource", rangeHeader, total))
	}

	chunk := payload[start : end+1]
	c.Response().Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	c.Response().WriteHeader(http.StatusPartialContent)

	_, err := io.Copy(c.Response(), bytes.NewReader(chunk))

	return err
}

// toDescribeJobResponse converts a job to a describe job response.
func toDescribeJobResponse(j *Job) describeJobResponse {
	resp := describeJobResponse{
		JobID:           j.JobID,
		JobDescription:  j.JobDescription,
		Action:          j.Action,
		ArchiveID:       j.ArchiveID,
		InventoryFormat: j.InventoryFormat,
		VaultARN:        j.VaultARN,
		CreationDate:    j.CreationDate,
		Completed:       j.Completed,
		StatusCode:      j.StatusCode,
		StatusMessage:   j.StatusMessage,
		Tier:            j.Tier,
	}

	if j.ArchiveSizeInBytes > 0 {
		size := j.ArchiveSizeInBytes
		resp.ArchiveSizeInBytes = &size
	}

	if j.InventorySizeInBytes > 0 {
		size := j.InventorySizeInBytes
		resp.InventorySizeInBytes = &size
	}

	if j.SHA256TreeHash != "" {
		resp.SHA256TreeHash = j.SHA256TreeHash
	}

	if j.Completed {
		resp.CompletionDate = j.CompletionDate
	}

	return resp
}

// ----------------------------------------
// Notification handlers
// ----------------------------------------

func (h *Handler) handleSetVaultNotifications(
	c *echo.Context,
	vaultName string,
	body []byte,
) error {
	var req vaultNotificationConfig
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"invalid request body: "+err.Error(),
		)
	}

	if err := h.Backend.SetVaultNotifications(
		h.AccountID,
		h.DefaultRegion,
		vaultName,
		req.SNSTopic,
		req.Events,
	); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetVaultNotifications(c *echo.Context, vaultName string) error {
	snsTopic, events, err := h.Backend.GetVaultNotifications(
		h.AccountID,
		h.DefaultRegion,
		vaultName,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	if snsTopic == "" {
		return h.writeError(
			c,
			http.StatusNotFound,
			"ResourceNotFoundException",
			"vault notification configuration not found",
		)
	}

	return c.JSON(http.StatusOK, vaultNotificationConfig{
		SNSTopic: snsTopic,
		Events:   events,
	})
}

func (h *Handler) handleDeleteVaultNotifications(c *echo.Context, vaultName string) error {
	if err := h.Backend.DeleteVaultNotifications(h.AccountID, h.DefaultRegion, vaultName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ----------------------------------------
// Access policy handlers
// ----------------------------------------

func (h *Handler) handleSetVaultAccessPolicy(c *echo.Context, vaultName string, body []byte) error {
	var req vaultAccessPolicy
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"invalid request body: "+err.Error(),
		)
	}

	if err := h.Backend.SetVaultAccessPolicy(h.AccountID, h.DefaultRegion, vaultName, req.Policy); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetVaultAccessPolicy(c *echo.Context, vaultName string) error {
	policy, err := h.Backend.GetVaultAccessPolicy(h.AccountID, h.DefaultRegion, vaultName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	if policy == "" {
		return h.writeError(
			c,
			http.StatusNotFound,
			"ResourceNotFoundException",
			"vault access policy not found",
		)
	}

	return c.JSON(http.StatusOK, vaultAccessPolicy{Policy: policy})
}

func (h *Handler) handleDeleteVaultAccessPolicy(c *echo.Context, vaultName string) error {
	if err := h.Backend.DeleteVaultAccessPolicy(h.AccountID, h.DefaultRegion, vaultName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ----------------------------------------
// Tag handlers
// ----------------------------------------

func (h *Handler) handleAddTagsToVault(c *echo.Context, vaultName string, body []byte) error {
	var req addTagsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"invalid request body: "+err.Error(),
		)
	}

	if err := h.Backend.AddTagsToVault(h.AccountID, h.DefaultRegion, vaultName, req.Tags); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListTagsForVault(c *echo.Context, vaultName string) error {
	tags, err := h.Backend.ListTagsForVault(h.AccountID, h.DefaultRegion, vaultName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsResponse{Tags: tags})
}

func (h *Handler) handleRemoveTagsFromVault(c *echo.Context, vaultName string, body []byte) error {
	var req removeTagsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"invalid request body: "+err.Error(),
		)
	}

	if err := h.Backend.RemoveTagsFromVault(h.AccountID, h.DefaultRegion, vaultName, req.TagKeys); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ----------------------------------------
// Vault lock handlers
// ----------------------------------------

func (h *Handler) handleVaultLock(c *echo.Context, op, resource string, body []byte) error {
	vaultName := extractVaultName(resource)

	switch op {
	case opAbortVaultLock:
		if err := h.Backend.AbortVaultLock(h.AccountID, h.DefaultRegion, vaultName); err != nil {
			return h.writeBackendError(c, err)
		}

		return c.NoContent(http.StatusNoContent)
	case opInitiateVaultLock:
		var req vaultLockPolicyRequest
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}

		lockID := generateID(lockIDLength)
		if err := h.Backend.SetVaultLock(h.AccountID, h.DefaultRegion, vaultName, req.Policy, lockID); err != nil {
			return h.writeBackendError(c, err)
		}

		return c.JSON(http.StatusCreated, map[string]string{"lockId": lockID})
	case opCompleteVaultLock:
		lockID := extractSubID(resource)
		if err := h.Backend.CompleteVaultLock(h.AccountID, h.DefaultRegion, vaultName, lockID); err != nil {
			return h.writeBackendError(c, err)
		}

		return c.NoContent(http.StatusNoContent)
	case opGetVaultLock:
		return h.handleGetVaultLock(c, vaultName)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetVaultLock(c *echo.Context, vaultName string) error {
	lock, err := h.Backend.GetVaultLock(h.AccountID, h.DefaultRegion, vaultName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getVaultLockResponse{
		Policy:         lock.Policy,
		State:          lock.State,
		CreationDate:   lock.CreationDate,
		ExpirationDate: lock.ExpirationDate,
	})
}

// dataRetrievalRule is a single rule in the data retrieval policy.
// BytesPerHour pointer comes first so the struct fits in 16 pointer bytes.
type dataRetrievalRule struct {
	BytesPerHour *int64 `json:"BytesPerHour,omitempty"`
	Strategy     string `json:"Strategy"`
}

// dataRetrievalPolicyBody wraps the Rules slice in the AWS request/response envelope.
type dataRetrievalPolicyBody struct {
	Rules []dataRetrievalRule `json:"Rules"`
}

// dataRetrievalPolicyRequest is the outer envelope for SetDataRetrievalPolicy.
type dataRetrievalPolicyRequest struct {
	Policy dataRetrievalPolicyBody `json:"Policy"`
}

// handleDataRetrievalPolicy handles GetDataRetrievalPolicy and SetDataRetrievalPolicy.
func (h *Handler) handleDataRetrievalPolicy(c *echo.Context, op string, body []byte) error {
	if op == opGetDataRetrievalPolicy {
		return h.handleGetDataRetrievalPolicy(c)
	}

	return h.handleSetDataRetrievalPolicy(c, body)
}

func (h *Handler) handleGetDataRetrievalPolicy(c *echo.Context) error {
	policy := h.Backend.GetDataRetrievalPolicy(h.AccountID)
	if policy == "" {
		return c.JSON(http.StatusOK, map[string]any{
			"Policy": map[string]any{
				"Rules": []map[string]string{
					{"Strategy": "FreeTier"},
				},
			},
		})
	}

	var parsed any
	if err := json.Unmarshal([]byte(policy), &parsed); err == nil {
		return c.JSON(http.StatusOK, parsed)
	}

	return c.JSON(http.StatusOK, map[string]any{"Policy": policy})
}

func (h *Handler) handleSetDataRetrievalPolicy(c *echo.Context, body []byte) error {
	var req dataRetrievalPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"invalid data retrieval policy: "+err.Error())
	}

	if vErr := validateDataRetrievalRules(req.Policy.Rules); vErr != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			vErr.Error(),
		)
	}

	h.Backend.SetDataRetrievalPolicy(h.AccountID, body)

	return c.NoContent(http.StatusNoContent)
}

// validateDataRetrievalRules validates the Rules slice of a data retrieval policy.
func validateDataRetrievalRules(rules []dataRetrievalRule) error {
	validStrategies := map[string]bool{"None": true, "FreeTier": true, "BytesPerHour": true}

	for _, r := range rules {
		if !validStrategies[r.Strategy] {
			return fmt.Errorf(
				"%w: %q; must be None, FreeTier, or BytesPerHour",
				ErrInvalidStrategy,
				r.Strategy,
			)
		}

		if r.Strategy == "BytesPerHour" && (r.BytesPerHour == nil || *r.BytesPerHour <= 0) {
			return ErrBytesPerHourRequired
		}
	}

	return nil
}

// ----------------------------------------
// Multipart upload handlers
// ----------------------------------------

func (h *Handler) handleInitiateMultipartUpload(c *echo.Context, vaultName string, _ []byte) error {
	description := c.Request().Header.Get("X-Amz-Archive-Description")
	if err := validateDescription(description); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	partSizeStr := c.Request().Header.Get("X-Amz-Part-Size")

	var partSize int64

	if partSizeStr != "" {
		n, err := parseInt64Header(partSizeStr)
		if err != nil {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidParameterValueException",
				"invalid X-Amz-Part-Size header",
			)
		}

		partSize = n
	}

	up, err := h.Backend.InitiateMultipartUpload(
		h.AccountID,
		h.DefaultRegion,
		vaultName,
		description,
		partSize,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	location := "/" + h.AccountID + "/vaults/" + vaultName + "/multipart-uploads/" + up.MultipartUploadID
	c.Response().Header().Set("Location", location)
	c.Response().Header().Set("X-Amz-Multipart-Upload-Id", up.MultipartUploadID)

	// AWS returns the chosen part size in the response so clients can verify.
	if up.PartSizeInBytes > 0 {
		c.Response().Header().Set("X-Amz-Part-Size", strconv.FormatInt(up.PartSizeInBytes, 10))
	}

	return c.JSON(http.StatusCreated, initiateMultipartUploadResponse{
		Location:          location,
		MultipartUploadID: up.MultipartUploadID,
	})
}

func (h *Handler) handleUploadMultipartPart(
	c *echo.Context,
	vaultName, uploadID string,
	_ []byte,
) error {
	rangeHeader := c.Request().Header.Get("Content-Range")
	checksum := c.Request().Header.Get("X-Amz-Sha256-Tree-Hash")

	if err := h.Backend.UploadMultipartPart(
		h.AccountID, h.DefaultRegion, vaultName, uploadID, rangeHeader, checksum,
	); err != nil {
		return h.writeBackendError(c, err)
	}

	c.Response().Header().Set("X-Amz-Sha256-Tree-Hash", checksum)

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCompleteMultipartUpload(
	c *echo.Context,
	vaultName, uploadID string,
	_ []byte,
) error {
	archiveSizeStr := c.Request().Header.Get("X-Amz-Archive-Size")
	checksum := c.Request().Header.Get("X-Amz-Sha256-Tree-Hash")

	var archiveSize int64

	if archiveSizeStr != "" {
		n, err := parseInt64Header(archiveSizeStr)
		if err != nil {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidParameterValueException",
				"invalid X-Amz-Archive-Size header",
			)
		}

		archiveSize = n
	}

	a, err := h.Backend.CompleteMultipartUpload(
		h.AccountID, h.DefaultRegion, vaultName, uploadID, checksum, archiveSize,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	location := "/" + h.AccountID + "/vaults/" + vaultName + "/archives/" + a.ArchiveID
	c.Response().Header().Set("X-Amz-Archive-Id", a.ArchiveID)
	c.Response().Header().Set("X-Amz-Sha256-Tree-Hash", a.SHA256TreeHash)
	c.Response().Header().Set("Location", location)

	return c.JSON(http.StatusCreated, completeMultipartUploadResponse{
		ArchiveID: a.ArchiveID,
		Checksum:  a.SHA256TreeHash,
		Location:  location,
	})
}

func (h *Handler) handleAbortMultipartUpload(c *echo.Context, vaultName, uploadID string) error {
	if err := h.Backend.AbortMultipartUpload(h.AccountID, h.DefaultRegion, vaultName, uploadID); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListMultipartUploads(c *echo.Context, vaultName string) error {
	ups := h.Backend.ListMultipartUploads(h.AccountID, h.DefaultRegion, vaultName)
	items := make([]MultipartUpload, 0, len(ups))

	for _, up := range ups {
		items = append(items, *up)
	}

	items, nextMarker, pErr := paginateUploadList(c, items)
	if pErr != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			pErr.Error(),
		)
	}

	return c.JSON(http.StatusOK, listMultipartUploadsResponse{
		Marker:      nextMarker,
		UploadsList: items,
	})
}

// paginateUploadList applies marker+limit pagination to a multipart-upload slice.
func paginateUploadList(
	c *echo.Context,
	items []MultipartUpload,
) ([]MultipartUpload, *string, error) {
	if marker := c.QueryParam("marker"); marker != "" {
		start := 0

		for start < len(items) && items[start].MultipartUploadID != marker {
			start++
		}

		if start < len(items) {
			items = items[start+1:]
		} else {
			items = nil
		}
	}

	limitStr := c.QueryParam("limit")
	if limitStr == "" {
		return items, nil, nil
	}

	n, err := strconv.Atoi(limitStr)
	if err != nil || n < minListLimit || n > maxListUploadsLimit {
		return nil, nil, fmt.Errorf(
			"%w: must be between %d and %d",
			ErrLimitOutOfRange,
			minListLimit,
			maxListUploadsLimit,
		)
	}

	if n >= len(items) {
		return items, nil, nil
	}

	last := items[n-1].MultipartUploadID

	return items[:n], &last, nil
}

func (h *Handler) handleListParts(c *echo.Context, vaultName, uploadID string) error {
	resp, err := h.Backend.ListParts(h.AccountID, h.DefaultRegion, vaultName, uploadID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	// Apply marker+limit pagination to the parts list.
	parts, nextMarker, pErr := paginatePartList(c, resp.Parts)
	if pErr != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			pErr.Error(),
		)
	}

	resp.Parts = parts
	resp.Marker = nextMarker

	return c.JSON(http.StatusOK, resp)
}

// paginatePartList applies marker+limit pagination to a parts slice.
// Marker is compared to RangeInBytes of each part.
func paginatePartList(c *echo.Context, parts []MultipartPart) ([]MultipartPart, *string, error) {
	if marker := c.QueryParam("marker"); marker != "" {
		start := 0

		for start < len(parts) && parts[start].RangeInBytes != marker {
			start++
		}

		if start < len(parts) {
			parts = parts[start+1:]
		} else {
			parts = nil
		}
	}

	limitStr := c.QueryParam("limit")
	if limitStr == "" {
		return parts, nil, nil
	}

	n, err := strconv.Atoi(limitStr)
	if err != nil || n < minListLimit || n > maxListUploadsLimit {
		return nil, nil, fmt.Errorf(
			"%w: must be between %d and %d",
			ErrLimitOutOfRange,
			minListLimit,
			maxListUploadsLimit,
		)
	}

	if n >= len(parts) {
		return parts, nil, nil
	}

	last := parts[n-1].RangeInBytes

	return parts[:n], &last, nil
}

// ----------------------------------------
// Provisioned capacity handlers
// ----------------------------------------

func (h *Handler) handleListProvisionedCapacity(c *echo.Context, accountID string) error {
	caps := h.Backend.ListProvisionedCapacity(accountID)
	items := make([]ProvisionedCapacity, 0, len(caps))

	for _, item := range caps {
		items = append(items, *item)
	}

	return c.JSON(http.StatusOK, listProvisionedCapacityResponse{
		ProvisionedCapacityList: items,
	})
}

func (h *Handler) handlePurchaseProvisionedCapacity(c *echo.Context, accountID string) error {
	provCap, err := h.Backend.PurchaseProvisionedCapacity(accountID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	c.Response().Header().Set("X-Amz-Capacity-Id", provCap.CapacityID)

	return c.JSON(http.StatusCreated, purchaseProvisionedCapacityResponse{
		CapacityID: provCap.CapacityID,
	})
}

// parseInt64Header parses an integer value from a header string.
func parseInt64Header(s string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(s), 10, 64)
}

// ----------------------------------------
// Error helpers
// ----------------------------------------

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
		return h.writeError(c, http.StatusConflict, "InvalidParameterValueException", err.Error())
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

	h.archiveMu.Lock()
	h.archiveData = make(map[string][]byte)
	h.archiveMu.Unlock()
}
