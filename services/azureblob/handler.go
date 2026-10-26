package azureblob

import (
	"context"
	"crypto/rand"
	"encoding/xml"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

// azureBlobVersion is the x-ms-version value echoed on every response. It is
// a plausible, well-formed Azure Storage REST API version string -- picked so
// azure-sdk-for-go's blob client parses it without erroring, not because
// gopherstack implements that exact API version's full feature set.
const azureBlobVersion = "2021-08-06"

// blockBlobType is the only x-ms-blob-type this MVP accepts/serves (see
// PARITY.md known gaps: no append/page blob support).
const blockBlobType = "BlockBlob"

// Operation name constants used for metrics (ExtractOperation) and
// GetSupportedOperations.
const (
	opListContainers         = "ListContainers"
	opGetServiceProperties   = "GetServiceProperties"
	opCreateContainer        = "CreateContainer"
	opDeleteContainer        = "DeleteContainer"
	opListBlobs              = "ListBlobs"
	opGetContainerProperties = "GetContainerProperties"
	opPutBlob                = "PutBlob"
	opSetBlobProperties      = "SetBlobProperties"
	opGetBlob                = "GetBlob"
	opGetBlobProperties      = "GetBlobProperties"
	opDeleteBlob             = "DeleteBlob"
	opSetBlobMetadata        = "SetBlobMetadata"
	opGetBlobMetadata        = "GetBlobMetadata"
	opSetBlobTier            = "SetBlobTier"
	opSnapshotBlob           = "SnapshotBlob"
	opCopyBlob               = "CopyBlob"
	opPutBlock               = "PutBlock"
	opPutBlockFromURL        = "PutBlockFromURL"
	opPutBlockList           = "PutBlockList"
	opGetBlockList           = "GetBlockList"
	opLeaseBlob              = "LeaseBlob"
	opLeaseContainer         = "LeaseContainer"
	opSetContainerMetadata   = "SetContainerMetadata"
	opGetContainerACL        = "GetContainerACL"
	opSetContainerACL        = "SetContainerACL"
	unknownOperation         = "Unknown"
)

// Handler is the Echo HTTP handler for Azure Blob Storage operations.
type Handler struct {
	Backend StorageBackend
	srvMu   *lockmetrics.RWMutex
	srv     *http.Server
	// Endpoint is e.g. "http://127.0.0.1:10000" -- used to build
	// ServiceEndpoint in list responses.
	Endpoint string
	// Port is the TCP port StartWorker binds. Set from Settings at Init time
	// (see provider.go); defaults to DefaultPort. Unlike a per-resource
	// ephemeral allocation, this is a single fixed, protocol-conventional
	// port (mirroring services/iot's MQTT broker) -- there is no fallback
	// pool, so StartWorker fails fast if it's unavailable rather than
	// silently binding a different port.
	Port int
	// EnforcePublicAccess makes anonymous requests honour container
	// public-access levels; off, anonymous access is unrestricted.
	EnforcePublicAccess bool
}

// NewHandler creates a new Azure Blob Handler. Port defaults to DefaultPort;
// callers (typically provider.go) override it from Settings.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend: backend,
		Port:    DefaultPort,
		srvMu:   lockmetrics.New("azureblob.server"),
	}
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
	_ service.Resettable       = (*Handler)(nil)
)

// Name returns the service name.
func (h *Handler) Name() string { return "AzureBlob" }

// GetSupportedOperations returns the list of supported Azure Blob operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opListContainers,
		opGetServiceProperties,
		opCreateContainer,
		opDeleteContainer,
		opListBlobs,
		opGetContainerProperties,
		opPutBlob,
		opSetBlobProperties,
		opGetBlob,
		opGetBlobProperties,
		opDeleteBlob,
		opSetBlobMetadata,
		opGetBlobMetadata,
		opSetBlobTier,
		opSnapshotBlob,
		opCopyBlob,
		opPutBlock,
		opPutBlockFromURL,
		opPutBlockList,
		opGetBlockList,
		opLeaseBlob,
		opLeaseContainer,
		opSetContainerMetadata,
		opGetContainerACL,
		opSetContainerACL,
	}
}

// RouteMatcher exists only to satisfy service.Registerable's interface
// contract: AzureBlob deliberately never matches on the shared AWS
// single-port Router. It runs on its own dedicated listener started by
// StartWorker (see provider.go for the full rationale). AzureBlob's
// Provider IS registered in cli.go's getMostRecentServiceProviders like
// every other service -- startBackgroundWorkers calls StartWorker via the
// service.BackgroundWorker interface regardless of routing, which is how
// the dedicated listener comes up. Only RouteMatcher itself is inert, kept
// so *Handler satisfies service.Registerable.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(*echo.Context) bool { return false }
}

// MatchPriority returns the routing priority for the AzureBlob handler.
// Irrelevant in practice since RouteMatcher never matches; 0 (lowest) is
// the safe default.
func (h *Handler) MatchPriority() int { return 0 }

// ExtractOperation extracts the Azure Blob operation name from the request,
// for metrics labeling.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return operationFor(c.Request())
}

// ExtractResource extracts the container/blob resource identifier from the
// request path, for metrics labeling.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, container, blob := splitPath(c.Request().URL.Path)
	if blob != "" {
		return container + "/" + blob
	}

	return container
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Handler returns the Echo handler function for Azure Blob operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()

		h.setCommonHeaders(c)
		h.checkAuth(r)

		account, container, blob := splitPath(r.URL.Path)
		if account == "" {
			return h.writeError(c, http.StatusBadRequest, "InvalidUri",
				"The requested URI does not represent any resource on the server.")
		}

		if denied, err := h.denyAnonymous(c, container, blob); denied {
			return err
		}

		switch {
		case container == "":
			return h.handleAccountLevel(c)
		case blob == "":
			return h.handleContainerLevel(c, container)
		default:
			return h.handleBlobLevel(c, container, blob)
		}
	}
}

// checkAuth is intentionally permissive: it neither requires nor
// cryptographically verifies the Authorization header, matching this repo's
// permissive-by-default auth philosophy (see services/s3/sigv4.go). Any
// structurally-present "SharedKey ..." header, or its absence, is accepted.
//
// pkgs/azureauth.ParseAuthorizationHeader is used to prove a real Azure SDK's
// Authorization header round-trips through this package (account name /
// scheme extraction), but a malformed or absent header is still accepted --
// matching services/s3's own opt-in verification stance. Rejecting invalid
// signatures via azureauth.VerifySharedKey is deliberately deferred past M0
// (see AZURE.md section 5): it needs to be exercised against real SDK
// request shapes first, the same way S3's WithPresignValidation is opt-in
// rather than on-by-default.
func (h *Handler) checkAuth(r *http.Request) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return // anonymous; accepted by design at this milestone
	}

	if _, ok := azureauth.ParseAuthorizationHeader(authHeader); !ok {
		// Structurally malformed; still accepted at this milestone, but
		// logged so the gap is visible rather than silently swallowed.
		logger.Load(r.Context()).DebugContext(r.Context(), "azureblob: malformed Authorization header accepted at M0")
	}
}

// setCommonHeaders sets the headers real Azure SDKs expect on every
// response, success or error.
func (h *Handler) setCommonHeaders(c *echo.Context) {
	hdr := c.Response().Header()
	hdr.Set("X-Ms-Version", azureBlobVersion)
	hdr.Set("X-Ms-Request-Id", newRequestID())
	hdr.Set("Date", time.Now().UTC().Format(http.TimeFormat))
}

// newRequestID generates a plausible request-id (UUID-shaped, not
// cryptographically meaningful) for the x-ms-request-id header.
func newRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "00000000-0000-0000-0000-000000000000"
	}

	return fmt.Sprintf("%x-%x-%x-%x-%x", buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}

// maxPathSegments bounds splitPath's strings.SplitN call: account, container,
// and everything else as blob (blob names may contain "/" themselves).
const maxPathSegments = 3

// blobSegmentIndex is the index (and, as a length check, the minimum part
// count) at which a blob segment is present in splitPath's parts slice.
const blobSegmentIndex = 2

// splitPath splits an Azure Blob REST path ("/<account>/<container>/<blob>")
// into its three components. blob may itself contain "/" (Azure blob names
// may include virtual-directory separators), so it is never split further.
func splitPath(p string) (string, string, string) {
	p = strings.TrimPrefix(p, "/")
	if p == "" {
		return "", "", ""
	}

	parts := strings.SplitN(p, "/", maxPathSegments)
	account := parts[0]

	var container, blob string

	if len(parts) > 1 {
		container = parts[1]
	}

	if len(parts) > blobSegmentIndex {
		blob = parts[blobSegmentIndex]
	}

	return account, container, blob
}

// Query-parameter names and values shared by operationFor and the actual
// request handlers (handleAccountLevel/handleContainerLevel).
const (
	queryRestype = "restype"
	queryComp    = "comp"

	restypeContainer = "container"
	restypeService   = "service"
	compList         = "list"
	compProperties   = "properties"
	compMetadata     = "metadata"
	compACL          = "acl"
	compTier         = "tier"
	compSnapshot     = "snapshot"
)

// operationFor determines the Azure Blob operation name for a request, for
// metrics labeling. Mirrors the dispatch logic in handleAccountLevel/
// handleContainerLevel/handleBlobLevel without side effects. Split into one
// helper per path level (account/container/blob) to keep each branch small.
func operationFor(r *http.Request) string {
	_, container, blob := splitPath(r.URL.Path)

	switch {
	case blob != "":
		return blobOperationFor(r)
	case container != "":
		return containerOperationFor(r)
	default:
		return accountOperationFor(r)
	}
}

// accountOperationFor covers the two account-level operations: List
// Containers (GET /<account>?comp=list) and Get Blob Service Properties
// (GET /<account>?restype=service&comp=properties).
func accountOperationFor(r *http.Request) string {
	q := r.URL.Query()

	switch {
	case r.Method == http.MethodGet && q.Get(queryComp) == compList:
		return opListContainers
	case r.Method == http.MethodGet && q.Get(queryRestype) == restypeService && q.Get(queryComp) == compProperties:
		return opGetServiceProperties
	default:
		return unknownOperation
	}
}

// containerOperationFor covers the four container-scoped operations:
// Create Container, Delete Container, List Blobs, and Get Container
// Properties.
func containerOperationFor(r *http.Request) string {
	q := r.URL.Query()
	if q.Get(queryRestype) != restypeContainer {
		return unknownOperation
	}

	comp := q.Get(queryComp)

	switch r.Method {
	case http.MethodPut:
		switch comp {
		case compMetadata:
			return opSetContainerMetadata
		case compACL:
			return opSetContainerACL
		case compLease:
			return opLeaseContainer
		case "":
			return opCreateContainer
		}
	case http.MethodDelete:
		return opDeleteContainer
	case http.MethodGet, http.MethodHead:
		return containerReadOp(r.Method, comp)
	}

	return unknownOperation
}

func containerReadOp(method, comp string) string {
	switch {
	case method == http.MethodGet && comp == compList:
		return opListBlobs
	case method == http.MethodGet && comp == compACL:
		return opGetContainerACL
	case comp == "" || comp == compMetadata:
		return opGetContainerProperties
	default:
		return unknownOperation
	}
}

// blobOperationFor covers the five blob-scoped operations (mirrors
// handleBlobLevel).
func blobOperationFor(r *http.Request) string {
	comp := r.URL.Query().Get(queryComp)

	switch r.Method {
	case http.MethodPut:
		return blobPutOp(comp, r.Header.Get(headerCopySource) != "")
	case http.MethodGet:
		switch comp {
		case compMetadata:
			return opGetBlobMetadata
		case compBlockList:
			return opGetBlockList
		}

		return opGetBlob
	case http.MethodHead:
		if comp == compMetadata {
			return opGetBlobMetadata
		}

		return opGetBlobProperties
	case http.MethodDelete:
		return opDeleteBlob
	default:
		return unknownOperation
	}
}

func blobPutOp(comp string, copying bool) string {
	switch {
	case comp == compBlock && copying:
		return opPutBlockFromURL
	case comp == compBlock:
		return opPutBlock
	case comp == compBlockList:
		return opPutBlockList
	case comp == compLease:
		return opLeaseBlob
	case comp == compProperties:
		return opSetBlobProperties
	case comp == compMetadata:
		return opSetBlobMetadata
	case comp == compTier:
		return opSetBlobTier
	case comp == compSnapshot:
		return opSnapshotBlob
	case copying:
		return opCopyBlob
	default:
		return opPutBlob
	}
}

// serviceEndpoint returns the ServiceEndpoint attribute value for
// EnumerationResults responses.
func (h *Handler) serviceEndpoint() string {
	if h.Endpoint != "" {
		return h.Endpoint
	}

	return fmt.Sprintf("http://127.0.0.1:%d", h.Port)
}

// handleAccountLevel serves GET /<account>?comp=list (List Containers) and
// GET /<account>?restype=service&comp=properties (Get Blob Service
// Properties).
func (h *Handler) handleAccountLevel(c *echo.Context) error {
	r := c.Request()

	if r.Method == http.MethodGet && c.QueryParam(queryRestype) == restypeService &&
		c.QueryParam(queryComp) == compProperties {
		return h.writeXML(c, http.StatusOK, storageServiceProperties{})
	}

	if r.Method != http.MethodGet || c.QueryParam(queryComp) != compList {
		return h.writeError(c, http.StatusBadRequest, "InvalidQueryParameterValue",
			"A query parameter is not supported for this operation.")
	}

	q := c.QueryParams()
	opts := ListContainersOptions{Prefix: q.Get("prefix"), Marker: q.Get("marker")}

	if raw := q.Get("maxresults"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return h.writeError(c, http.StatusBadRequest, "InvalidQueryParameterValue",
				"Value for one of the query parameters specified in the request URI is invalid.")
		}

		opts.MaxResults = min(n, maxListResults)
	}

	page, err := h.Backend.ListContainersPage(opts)
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidQueryParameterValue",
			"Value for one of the query parameters specified in the request URI is invalid.")
	}

	result := enumerationResults{
		ServiceEndpoint: h.serviceEndpoint(),
		Prefix:          opts.Prefix,
		Marker:          opts.Marker,
		NextMarker:      page.NextMarker,
		Containers:      &containersList{Container: make([]containerEntry, 0, len(page.Containers))},
	}

	if opts.MaxResults > 0 {
		result.MaxResults = strconv.Itoa(opts.MaxResults)
	}

	withMetadata := false

	for inc := range strings.SplitSeq(q.Get("include"), ",") {
		if strings.TrimSpace(inc) == compMetadata {
			withMetadata = true
		}
	}

	for _, ci := range page.Containers {
		entry := containerEntry{
			Name: ci.Name,
			Properties: containerProperties{
				LastModified: ci.LastModified.Format(http.TimeFormat),
				Etag:         ci.ETag,
				PublicAccess: ci.PublicAccess,
				LeaseStatus:  ci.Lease.Status,
				LeaseState:   ci.Lease.State,
				LeaseDur:     ci.Lease.Duration,
			},
		}

		if withMetadata {
			entry.Metadata = metadataElem(ci.Metadata)
		}

		result.Containers.Container = append(result.Containers.Container, entry)
	}

	return h.writeXML(c, http.StatusOK, result)
}

// handleContainerLevel serves the three container-scoped operations: Create
// Container, Delete Container, and List Blobs.
func (h *Handler) handleContainerLevel(c *echo.Context, container string) error {
	switch containerOperationFor(c.Request()) {
	case opCreateContainer:
		return h.createContainer(c, container)
	case opDeleteContainer:
		return h.deleteContainer(c, container)
	case opListBlobs:
		return h.listBlobs(c, container)
	case opLeaseContainer:
		return h.leaseContainer(c, container)
	case opGetContainerProperties:
		return h.getContainerProperties(c, container)
	case opSetContainerMetadata:
		return h.setContainerMetadata(c, container)
	case opGetContainerACL:
		return h.getContainerACL(c, container)
	case opSetContainerACL:
		return h.setContainerACL(c, container)
	default:
		return h.writeError(c, http.StatusBadRequest, "InvalidQueryParameterValue",
			"A query parameter is not supported for this operation.")
	}
}

func (h *Handler) deleteContainer(c *echo.Context, container string) error {
	pre, apiErr := precondFromRequest(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	if err := h.Backend.DeleteContainer(container, pre); err != nil {
		return h.writeContainerError(c, err)
	}

	return c.NoContent(http.StatusAccepted)
}

// handleBlobLevel dispatches the blob-scoped operations.
func (h *Handler) handleBlobLevel(c *echo.Context, container, blob string) error {
	switch blobOperationFor(c.Request()) {
	case opSetBlobProperties:
		return h.setBlobProperties(c, container, blob)
	case opSetBlobMetadata:
		return h.setBlobMetadata(c, container, blob)
	case opGetBlobMetadata:
		return h.getBlobMetadata(c, container, blob)
	case opSetBlobTier:
		return h.setBlobTier(c, container, blob)
	case opSnapshotBlob:
		return h.snapshotBlob(c, container, blob)
	case opCopyBlob:
		return h.copyBlob(c, container, blob)
	case opPutBlock, opPutBlockFromURL, opPutBlockList, opGetBlockList, opLeaseBlob:
		return h.handleBlockAndLease(c, container, blob)
	case opPutBlob:
		return h.putBlob(c, container, blob)
	case opGetBlob:
		return h.getBlob(c, container, blob)
	case opGetBlobProperties:
		return h.headBlob(c, container, blob)
	case opDeleteBlob:
		return h.deleteBlob(c, container, blob)
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "UnsupportedHttpVerb",
			"The resource doesn't support the specified HTTP verb.")
	}
}

func (h *Handler) handleBlockAndLease(c *echo.Context, container, blob string) error {
	switch blobOperationFor(c.Request()) {
	case opPutBlockList:
		return h.putBlockList(c, container, blob)
	case opGetBlockList:
		return h.getBlockList(c, container, blob)
	case opLeaseBlob:
		return h.leaseBlob(c, container, blob)
	default:
		return h.putBlock(c, container, blob)
	}
}

func (h *Handler) putBlob(c *echo.Context, container, blob string) error {
	r := c.Request()

	if r.Header.Get("X-Ms-Blob-Type") != blockBlobType {
		return h.writeError(c, http.StatusBadRequest, "InvalidHeaderValue",
			"The value for one of the HTTP headers is not in the correct format "+
				"(x-ms-blob-type must be BlockBlob; only block blobs are supported).")
	}

	attrs, apiErr := blobAttrsFromRequest(r)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	if attrs.ContentType == "" {
		attrs.ContentType = r.Header.Get("Content-Type")
	}

	attrs.AccessTier = r.Header.Get(headerAccessTier)
	if attrs.AccessTier != "" && !validTier(attrs.AccessTier) {
		return h.writeAPIError(c, errInvalidHeader(headerAccessTier))
	}

	pre, apiErr := precondFromRequest(r)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	body, err := httputils.ReadBody(r)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError",
			"Failed to read request body.")
	}

	if apiErr = verifyTransactional(r, body, "Content-Md5", headerContentCRC64); apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	info, err := h.Backend.PutBlobWith(container, blob, body, attrs, pre)
	if err != nil {
		return h.writeContainerError(c, err)
	}

	hdr := c.Response().Header()
	hdr.Set("ETag", info.ETag)
	hdr.Set("Last-Modified", info.LastModified.Format(http.TimeFormat))
	hdr.Set("Content-MD5", md5Header(body))
	hdr.Set(headerServerEncry, "true")

	return c.NoContent(http.StatusCreated)
}

func (h *Handler) getBlob(c *echo.Context, container, blob string) error {
	r := c.Request()

	pre, apiErr := precondFromRequest(r)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	info, data, err := h.Backend.GetBlobAt(container, blob, c.QueryParam(querySnapshot), pre)
	if err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	if info.AccessTier == TierArchive {
		return h.writeError(c, http.StatusConflict, "BlobArchived",
			"This operation is not permitted on an archived blob.")
	}

	h.setBlobHeaders(c, info)

	// x-ms-range takes precedence over Range; the Python and JS SDKs send only x-ms-range.
	rangeHeader := r.Header.Get("X-Ms-Range")
	if rangeHeader == "" {
		rangeHeader = r.Header.Get("Range")
	}

	if rangeHeader == "" {
		if info.ContentMD5 != "" {
			c.Response().Header().Set("Content-MD5", info.ContentMD5)
		}

		return c.Blob(http.StatusOK, contentTypeOrDefault(info.ContentType), data)
	}

	start, end, ok := parseRange(rangeHeader, int64(len(data)))
	if !ok {
		c.Response().Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(data)))

		return h.writeError(c, http.StatusRequestedRangeNotSatisfiable, "InvalidRange",
			"The range specified is invalid for the current size of the resource.")
	}

	hdr := c.Response().Header()
	hdr.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	hdr.Set("Content-Length", strconv.FormatInt(end-start+1, 10))

	return c.Blob(http.StatusPartialContent, contentTypeOrDefault(info.ContentType), data[start:end+1])
}

func (h *Handler) headBlob(c *echo.Context, container, blob string) error {
	pre, apiErr := precondFromRequest(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	info, err := h.Backend.HeadBlobAt(container, blob, c.QueryParam(querySnapshot), pre)
	if err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	h.setBlobHeaders(c, info)

	if info.ContentMD5 != "" {
		c.Response().Header().Set("Content-MD5", info.ContentMD5)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) deleteBlob(c *echo.Context, container, blob string) error {
	mode := c.Request().Header.Get(headerDeleteSnapshots)
	if mode != "" && mode != DeleteSnapshotsInclude && mode != DeleteSnapshotsOnly {
		return h.writeAPIError(c, errInvalidHeader(headerDeleteSnapshots))
	}

	pre, apiErr := precondFromRequest(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	err := h.Backend.DeleteBlobAt(container, blob, c.QueryParam(querySnapshot), mode, pre)
	if errors.Is(err, ErrSnapshotsPresent) {
		return h.writeError(c, http.StatusConflict, "SnapshotsPresent",
			"This operation is not permitted because the blob has snapshots.")
	}

	if err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	return c.NoContent(http.StatusAccepted)
}

// writeBlobNotFoundError maps a StorageBackend not-found error (either the
// container or the blob may be missing) to the corresponding Azure error code.
func (h *Handler) writeBlobNotFoundError(c *echo.Context, err error) error {
	if done, werr := h.writeBackendError(c, err); done {
		return werr
	}

	if errors.Is(err, ErrContainerNotFound) {
		return h.writeError(c, http.StatusNotFound, "ContainerNotFound",
			"The specified container does not exist.")
	}

	return h.writeError(c, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist.")
}

// setBlobHeaders sets the headers common to Get Blob and Get Blob Properties.
func (h *Handler) setBlobHeaders(c *echo.Context, info BlobInfo) {
	hdr := c.Response().Header()
	hdr.Set("ETag", info.ETag)
	hdr.Set("Last-Modified", info.LastModified.Format(http.TimeFormat))
	hdr.Set("Content-Length", strconv.FormatInt(info.ContentLength, 10))
	hdr.Set("X-Ms-Blob-Type", blockBlobType)
	hdr.Set("Accept-Ranges", "bytes")
	setMetadataHeaders(hdr, info.Metadata)
	setLeaseHeaders(hdr, info.Lease)

	for name, val := range map[string]string{
		"Content-Type":        info.ContentType,
		"Content-Encoding":    info.ContentEncoding,
		"Content-Language":    info.ContentLanguage,
		"Content-Disposition": info.ContentDisposition,
		"Cache-Control":       info.CacheControl,
		"X-Ms-Access-Tier":    info.AccessTier,
		"X-Ms-Snapshot":       info.Snapshot,
	} {
		if val != "" {
			hdr.Set(name, val)
		}
	}

	if info.AccessTierInferred {
		hdr.Set("X-Ms-Access-Tier-Inferred", "true")
	}

	if !info.AccessTierChanged.IsZero() {
		hdr.Set("X-Ms-Access-Tier-Change-Time", info.AccessTierChanged.Format(http.TimeFormat))
	}
}

func contentTypeOrDefault(ct string) string {
	if ct == "" {
		return "application/octet-stream"
	}

	return ct
}

// parseRange parses an HTTP "Range: bytes=start-end" header (also supporting
// the open-ended "bytes=start-" and suffix "bytes=-N" forms) against a
// resource of the given size. Only a single range is supported (Azure Get
// Blob does not support multi-range requests). Returns ok=false if the
// header is absent, malformed, or unsatisfiable for size.
func parseRange(header string, size int64) (int64, int64, bool) {
	const prefix = "bytes="

	spec, found := strings.CutPrefix(header, prefix)
	if !found {
		return 0, 0, false
	}

	// Reject multi-range requests (a comma indicates more than one range);
	// this backend only serves the first/only range.
	if strings.Contains(spec, ",") {
		return 0, 0, false
	}

	before, after, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false
	}

	if before == "" {
		// Suffix form: "bytes=-N" means the last N bytes.
		n, err := strconv.ParseInt(after, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}

		if n > size {
			n = size
		}

		return size - n, size - 1, size > 0
	}

	start, err := strconv.ParseInt(before, 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false
	}

	if after == "" {
		return start, size - 1, true
	}

	end, err := strconv.ParseInt(after, 10, 64)
	if err != nil || end < start {
		return 0, 0, false
	}

	if end >= size {
		end = size - 1
	}

	return start, end, true
}

// writeXML marshals v and writes it as the response body. v is marshaled
// without an XML header/prolog: echo's XMLBlob prepends xml.Header itself,
// so marshaling one here would duplicate it (see services/sqs/PARITY.md for
// the same trap hit and fixed there).
func (h *Handler) writeXML(c *echo.Context, status int, v any) error {
	body, err := xml.Marshal(v)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to marshal response.")
	}

	return c.XMLBlob(status, body)
}

// writeError writes a standard Azure Storage REST error body.
func (h *Handler) writeError(c *echo.Context, status int, code, message string) error {
	return h.writeXML(c, status, azureError{Code: code, Message: message})
}

// Timeouts for the dedicated Blob http.Server. ReadHeaderTimeout alone only
// bounds how long a client may take to send headers; without ReadTimeout and
// IdleTimeout a slow-body client (e.g. a stalled PUT blob upload) or a client
// that opens a connection and never closes it can hold a handler goroutine
// and connection open indefinitely (a Slowloris-style resource exhaustion).
// WriteTimeout is deliberately not set: it would bound the full
// request-to-response-complete duration, which for a large GetBlob download
// depends on the client's own read rate, not just server-side work.
const (
	azureBlobReadHeaderTimeout = 10 * time.Second
	azureBlobReadTimeout       = 60 * time.Second
	azureBlobIdleTimeout       = 120 * time.Second
)

// StartWorker binds the dedicated Blob listener and starts serving on it.
// See provider.go's Provider doc comment for why AzureBlob needs its own
// listener instead of registering into the shared AWS Router.
//
// Binding is synchronous: net.Listen returns before StartWorker does, so a
// bind failure is returned to the caller directly instead of only being
// logged from the background goroutine after startup has already reported
// success. This mirrors services/iot's MQTT broker (services/iot/broker.go),
// gopherstack's existing precedent for a service with a fixed,
// protocol-conventional default port: bind exactly the configured port
// (h.Port, from Settings -- see settings.go/provider.go) and fail fast if
// that's unavailable, rather than silently falling back into the shared
// --port-range-start/--port-range-end PortAlloc pool used for on-demand
// ephemeral resources elsewhere (Lambda function URLs, ElastiCache). A
// fallback there would be just as surprising as picking a different default
// port number outright: either way, an SDK relying on the well-known default
// (UseDevelopmentStorage=true-style config) would silently end up talking to
// the wrong port. Failing fast surfaces the conflict instead.
func (h *Handler) StartWorker(ctx context.Context) error {
	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", h.Port))
	if err != nil {
		return fmt.Errorf("azureblob: bind port %d: %w", h.Port, err)
	}

	e := echo.New()
	e.Use(logger.EchoMiddleware(logger.Load(ctx)))
	e.Any("/*", telemetry.WrapEchoHandler("AzureBlob", h.Handler(), h))

	srv := &http.Server{
		Handler:           e,
		ReadHeaderTimeout: azureBlobReadHeaderTimeout,
		ReadTimeout:       azureBlobReadTimeout,
		IdleTimeout:       azureBlobIdleTimeout,
	}

	h.srvMu.Lock("StartWorker")
	h.srv = srv
	h.srvMu.Unlock()

	workerCtx := logger.WithWorker(ctx, "azureblob", "listener")
	log := logger.Load(workerCtx)

	log.InfoContext(workerCtx, "azureblob: starting dedicated listener", "port", h.Port)

	go func() {
		if serveErr := srv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(workerCtx, "azureblob: listener stopped", "error", serveErr)
		}
	}()

	return nil
}

// Shutdown stops the dedicated Blob listener. A graceful Shutdown error
// (e.g. its context expiring before active connections finish) is logged
// and followed by Close, which forcibly closes the listener and any
// remaining idle/active connections; any Close error is logged too rather
// than leaving the listener to leak silently.
func (h *Handler) Shutdown(ctx context.Context) {
	h.srvMu.Lock("Shutdown")
	srv := h.srv
	h.srv = nil
	h.srvMu.Unlock()

	if srv == nil {
		return
	}

	log := logger.Load(ctx)

	if err := srv.Shutdown(ctx); err != nil {
		log.ErrorContext(ctx, "azureblob: graceful shutdown failed, forcing close", "error", err)

		if closeErr := srv.Close(); closeErr != nil {
			log.ErrorContext(ctx, "azureblob: forced close also failed", "error", closeErr)
		}
	}
}
