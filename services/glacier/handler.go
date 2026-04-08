package glacier

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
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
)

// Handler is the HTTP handler for the Glacier REST API.
type Handler struct {
	Backend       StorageBackend
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new Glacier handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Glacier" }

// GetSupportedOperations returns the list of supported Glacier operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateVault",
		"DescribeVault",
		"DeleteVault",
		"ListVaults",
		"UploadArchive",
		"DeleteArchive",
		"InitiateJob",
		"DescribeJob",
		"ListJobs",
		"GetJobOutput",
		"SetVaultNotifications",
		"GetVaultNotifications",
		"DeleteVaultNotifications",
		"SetVaultAccessPolicy",
		"GetVaultAccessPolicy",
		"DeleteVaultAccessPolicy",
		"AddTagsToVault",
		"ListTagsForVault",
		"RemoveTagsFromVault",
		"InitiateVaultLock",
		"AbortVaultLock",
		"CompleteVaultLock",
		"GetVaultLock",
		"GetDataRetrievalPolicy",
		"SetDataRetrievalPolicy",
		"InitiateMultipartUpload",
		"UploadMultipartPart",
		"CompleteMultipartUpload",
		"AbortMultipartUpload",
		"ListMultipartUploads",
		"ListParts",
		"ListProvisionedCapacity",
		"PurchaseProvisionedCapacity",
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
			return "ListVaults", accountID
		}

		return "", ""
	}

	vaultName := segs[2]

	// /{accountId}/vaults/{vaultName}
	if len(segs) == minVaultPathSegments {
		switch method {
		case http.MethodPut:
			return "CreateVault", vaultName
		case http.MethodGet:
			return "DescribeVault", vaultName
		case http.MethodDelete:
			return "DeleteVault", vaultName
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
			return "SetDataRetrievalPolicy", ""
		}
	}

	return "", ""
}

// parseProvisionedCapacityPath handles /{accountId}/provisioned-capacity.
func parseProvisionedCapacityPath(method, accountID string) (string, string) {
	switch method {
	case http.MethodGet:
		return "ListProvisionedCapacity", accountID
	case http.MethodPost:
		return "PurchaseProvisionedCapacity", accountID
	}

	return "", ""
}

// parseVaultSubPath handles paths beyond /{accountId}/vaults/{vaultName}/.
//

func parseVaultSubPath(method string, segs []string, vaultName, subPath, query string) (string, string) {
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
			return "UploadArchive", vaultName
		}

		return "", ""
	}

	archiveID := segs[4]

	if method == http.MethodDelete {
		return "DeleteArchive", vaultName + "/" + archiveID
	}

	return "", ""
}

// parseJobsPath handles /{accountId}/vaults/{vaultName}/jobs[/{jobId}[/output]].
func parseJobsPath(method string, segs []string, vaultName string) (string, string) {
	if len(segs) == 4 { //nolint:mnd // 4 segs = /account/vaults/name/jobs
		switch method {
		case http.MethodPost:
			return "InitiateJob", vaultName
		case http.MethodGet:
			return "ListJobs", vaultName
		}

		return "", ""
	}

	jobID := segs[4]

	if len(segs) == minJobPathSegments {
		if method == http.MethodGet {
			return "DescribeJob", vaultName + "/" + jobID
		}

		return "", ""
	}

	if len(segs) >= 6 && segs[5] == "output" {
		if method == http.MethodGet {
			return "GetJobOutput", vaultName + "/" + jobID
		}
	}

	return "", ""
}

// parseMultipartUploadsPath handles /{accountId}/vaults/{vaultName}/multipart-uploads[/{uploadId}].
func parseMultipartUploadsPath(method string, segs []string, vaultName string) (string, string) {
	if len(segs) == 4 { //nolint:mnd // 4 segs = /account/vaults/name/multipart-uploads
		switch method {
		case http.MethodPost:
			return "InitiateMultipartUpload", vaultName
		case http.MethodGet:
			return "ListMultipartUploads", vaultName
		}

		return "", ""
	}

	uploadID := segs[4]

	switch method {
	case http.MethodPut:
		return "UploadMultipartPart", vaultName + "/" + uploadID
	case http.MethodPost:
		return "CompleteMultipartUpload", vaultName + "/" + uploadID
	case http.MethodDelete:
		return "AbortMultipartUpload", vaultName + "/" + uploadID
	case http.MethodGet:
		return "ListParts", vaultName + "/" + uploadID
	}

	return "", ""
}

// parseTagsPath handles /{accountId}/vaults/{vaultName}/tags?operation=add|remove.
func parseTagsPath(method, query, vaultName string) (string, string) {
	switch method {
	case http.MethodPost:
		if strings.Contains(query, "operation=add") {
			return "AddTagsToVault", vaultName
		}

		if strings.Contains(query, "operation=remove") {
			return "RemoveTagsFromVault", vaultName
		}
	case http.MethodGet:
		return "ListTagsForVault", vaultName
	}

	return "", ""
}

// parseNotificationPath handles /{accountId}/vaults/{vaultName}/notification-configuration.
func parseNotificationPath(method, vaultName string) (string, string) {
	switch method {
	case http.MethodPut:
		return "SetVaultNotifications", vaultName
	case http.MethodGet:
		return "GetVaultNotifications", vaultName
	case http.MethodDelete:
		return "DeleteVaultNotifications", vaultName
	}

	return "", ""
}

// parseAccessPolicyPath handles /{accountId}/vaults/{vaultName}/access-policy.
func parseAccessPolicyPath(method, vaultName string) (string, string) {
	switch method {
	case http.MethodPut:
		return "SetVaultAccessPolicy", vaultName
	case http.MethodGet:
		return "GetVaultAccessPolicy", vaultName
	case http.MethodDelete:
		return "DeleteVaultAccessPolicy", vaultName
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
	case "CreateVault":
		return true, h.handleCreateVault(c, resource)
	case "DescribeVault":
		return true, h.handleDescribeVault(c, resource)
	case "DeleteVault":
		return true, h.handleDeleteVault(c, resource)
	case "ListVaults":
		return true, h.handleListVaults(c)
	}

	return false, nil
}

// dispatchArchiveAndJobOps routes archive and job operations.
func (h *Handler) dispatchArchiveAndJobOps(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case "UploadArchive":
		return true, h.handleUploadArchive(c, resource, body)
	case "DeleteArchive":
		return true, h.handleDeleteArchive(c, extractVaultName(resource), extractSubID(resource))
	case "InitiateJob":
		return true, h.handleInitiateJob(c, resource, body)
	case "DescribeJob":
		return true, h.handleDescribeJob(c, extractVaultName(resource), extractSubID(resource))
	case "ListJobs":
		return true, h.handleListJobs(c, resource)
	case "GetJobOutput":
		return true, h.handleGetJobOutput(c, extractVaultName(resource), extractSubID(resource))
	}

	return false, nil
}

// dispatchTagsAndPoliciesOps routes tag, notification, and access-policy operations.
func (h *Handler) dispatchTagsAndPoliciesOps(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case "SetVaultNotifications":
		return true, h.handleSetVaultNotifications(c, resource, body)
	case "GetVaultNotifications":
		return true, h.handleGetVaultNotifications(c, resource)
	case "DeleteVaultNotifications":
		return true, h.handleDeleteVaultNotifications(c, resource)
	case "SetVaultAccessPolicy":
		return true, h.handleSetVaultAccessPolicy(c, resource, body)
	case "GetVaultAccessPolicy":
		return true, h.handleGetVaultAccessPolicy(c, resource)
	case "DeleteVaultAccessPolicy":
		return true, h.handleDeleteVaultAccessPolicy(c, resource)
	case "AddTagsToVault":
		return true, h.handleAddTagsToVault(c, resource, body)
	case "ListTagsForVault":
		return true, h.handleListTagsForVault(c, resource)
	case "RemoveTagsFromVault":
		return true, h.handleRemoveTagsFromVault(c, resource, body)
	}

	return false, nil
}

// dispatchMultipartAndCapacityOps routes multipart upload and provisioned capacity operations.
func (h *Handler) dispatchMultipartAndCapacityOps(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case "InitiateMultipartUpload":
		return true, h.handleInitiateMultipartUpload(c, resource, body)
	case "UploadMultipartPart":
		return true, h.handleUploadMultipartPart(c, extractVaultName(resource), extractSubID(resource), body)
	case "CompleteMultipartUpload":
		return true, h.handleCompleteMultipartUpload(c, extractVaultName(resource), extractSubID(resource), body)
	case "AbortMultipartUpload":
		return true, h.handleAbortMultipartUpload(c, extractVaultName(resource), extractSubID(resource))
	case "ListMultipartUploads":
		return true, h.handleListMultipartUploads(c, resource)
	case "ListParts":
		return true, h.handleListParts(c, extractVaultName(resource), extractSubID(resource))
	case "ListProvisionedCapacity":
		return true, h.handleListProvisionedCapacity(c, resource)
	case "PurchaseProvisionedCapacity":
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
	case opGetDataRetrievalPolicy, "SetDataRetrievalPolicy":
		return h.handleDataRetrievalPolicy(c, op, body)
	}

	if handled, err := h.dispatchMultipartAndCapacityOps(c, op, resource, body); handled {
		return err
	}

	return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "unknown operation: "+op)
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

func (h *Handler) handleListVaults(c *echo.Context) error {
	vaults := h.Backend.ListVaults(h.AccountID, h.DefaultRegion)
	items := make([]describeVaultResponse, 0, len(vaults))

	for _, v := range vaults {
		items = append(items, toDescribeVaultResponse(v))
	}

	return c.JSON(http.StatusOK, listVaultsResponse{
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
	description := c.Request().Header.Get("X-Amz-Archive-Description")
	checksum := c.Request().Header.Get("X-Amz-Sha256-Tree-Hash")

	if checksum == "" {
		checksum = "0000000000000000000000000000000000000000000000000000000000000000"
	}

	size := int64(len(body))

	a, err := h.Backend.UploadArchive(h.AccountID, h.DefaultRegion, vaultName, description, checksum, size)
	if err != nil {
		return h.writeBackendError(c, err)
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

func (h *Handler) handleDeleteArchive(c *echo.Context, vaultName, archiveID string) error {
	if err := h.Backend.DeleteArchive(h.AccountID, h.DefaultRegion, vaultName, archiveID); err != nil {
		return h.writeBackendError(c, err)
	}

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
	jobs := h.Backend.ListJobs(h.AccountID, h.DefaultRegion, vaultName)
	items := make([]describeJobResponse, 0, len(jobs))

	for _, j := range jobs {
		items = append(items, toDescribeJobResponse(j))
	}

	return c.JSON(http.StatusOK, listJobsResponse{
		JobList: items,
	})
}

func (h *Handler) handleGetJobOutput(c *echo.Context, vaultName, jobID string) error {
	j, err := h.Backend.DescribeJob(h.AccountID, h.DefaultRegion, vaultName, jobID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	if j.SHA256TreeHash != "" {
		c.Response().Header().Set("X-Amz-Sha256-Tree-Hash", j.SHA256TreeHash)
	}

	// Return a minimal inventory JSON for InventoryRetrieval jobs, empty body for ArchiveRetrieval.
	if j.Action == "InventoryRetrieval" {
		inventoryJSON := `{"VaultARN":"` + j.VaultARN + `","InventoryDate":"` + j.CompletionDate + `","ArchiveList":[]}`
		c.Response().Header().Set("Content-Type", "application/json")

		return c.String(http.StatusOK, inventoryJSON)
	}

	c.Response().Header().Set("Content-Type", "application/octet-stream")

	return c.String(http.StatusOK, "")
}

// toDescribeJobResponse converts a job to a describe job response.
func toDescribeJobResponse(j *Job) describeJobResponse {
	resp := describeJobResponse{
		JobID:          j.JobID,
		JobDescription: j.JobDescription,
		Action:         j.Action,
		ArchiveID:      j.ArchiveID,
		VaultARN:       j.VaultARN,
		CreationDate:   j.CreationDate,
		Completed:      j.Completed,
		StatusCode:     j.StatusCode,
		StatusMessage:  j.StatusMessage,
		Tier:           j.Tier,
	}

	if j.ArchiveSizeInBytes > 0 {
		size := j.ArchiveSizeInBytes
		resp.ArchiveSizeInBytes = &size
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

func (h *Handler) handleSetVaultNotifications(c *echo.Context, vaultName string, body []byte) error {
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
	snsTopic, events, err := h.Backend.GetVaultNotifications(h.AccountID, h.DefaultRegion, vaultName)
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
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "vault access policy not found")
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

// handleDataRetrievalPolicy handles GetDataRetrievalPolicy and SetDataRetrievalPolicy.
func (h *Handler) handleDataRetrievalPolicy(c *echo.Context, op string, body []byte) error {
	if op == opGetDataRetrievalPolicy {
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

	h.Backend.SetDataRetrievalPolicy(h.AccountID, body)

	return c.NoContent(http.StatusNoContent)
}

// ----------------------------------------
// Multipart upload handlers
// ----------------------------------------

func (h *Handler) handleInitiateMultipartUpload(c *echo.Context, vaultName string, _ []byte) error {
	description := c.Request().Header.Get("X-Amz-Archive-Description")
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

	up, err := h.Backend.InitiateMultipartUpload(h.AccountID, h.DefaultRegion, vaultName, description, partSize)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	location := "/" + h.AccountID + "/vaults/" + vaultName + "/multipart-uploads/" + up.MultipartUploadID
	c.Response().Header().Set("Location", location)
	c.Response().Header().Set("X-Amz-Multipart-Upload-Id", up.MultipartUploadID)

	return c.JSON(http.StatusCreated, initiateMultipartUploadResponse{
		Location:          location,
		MultipartUploadID: up.MultipartUploadID,
	})
}

func (h *Handler) handleUploadMultipartPart(c *echo.Context, vaultName, uploadID string, _ []byte) error {
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

	return c.JSON(http.StatusOK, listMultipartUploadsResponse{
		UploadsList: items,
	})
}

func (h *Handler) handleListParts(c *echo.Context, vaultName, uploadID string) error {
	resp, err := h.Backend.ListParts(h.AccountID, h.DefaultRegion, vaultName, uploadID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, resp)
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
func (h *Handler) writeError(c *echo.Context, status int, code, message string) error {
	return c.JSON(status, errorResponse{
		Code:    code,
		Message: message,
		Type:    "Client",
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
	case errors.Is(err, ErrValidation):
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	return h.writeError(c, http.StatusInternalServerError, "ServiceUnavailableException", err.Error())
}

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
