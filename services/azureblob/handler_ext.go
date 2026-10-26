package azureblob

import (
	"encoding/base64"
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	headerMetaPrefix      = "X-Ms-Meta-"
	headerAccessTier      = "X-Ms-Access-Tier"
	headerPublicAccess    = "X-Ms-Blob-Public-Access"
	headerCopySource      = "X-Ms-Copy-Source"
	headerDeleteSnapshots = "X-Ms-Delete-Snapshots"

	querySnapshot = "snapshot"

	maxListResults       = 5000
	md5Len               = 16
	maxSignedIdentifiers = 5
	maxIdentifierLen     = 64
)

type apiError struct {
	code, message string
	status        int
}

func errInvalidHeader(name string) *apiError {
	return &apiError{
		status: http.StatusBadRequest, code: "InvalidHeaderValue",
		message: "The value for the " + strings.ToLower(name) + " header is not in the correct format.",
	}
}

func (h *Handler) writeAPIError(c *echo.Context, e *apiError) error {
	return h.writeError(c, e.status, e.code, e.message)
}

func validTier(t string) bool {
	switch t {
	case TierHot, TierCool, TierCold, TierArchive:
		return true
	default:
		return false
	}
}

func validPublicAccess(v string) bool {
	return v == "" || v == AccessBlob || v == AccessContainer
}

func validMetadataName(n string) bool {
	if n == "" {
		return false
	}

	for i := range len(n) {
		ch := n[i]
		letter := ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '_'

		if !letter && (i == 0 || ch < '0' || ch > '9') {
			return false
		}
	}

	return true
}

// metadataFromRequest collects x-ms-meta-* headers; names are lowercased as
// Go canonicalizes header casing. Returns nil when none are present.
func metadataFromRequest(r *http.Request) (map[string]string, *apiError) {
	var md map[string]string

	for name, vals := range r.Header {
		rest, ok := strings.CutPrefix(name, headerMetaPrefix)
		if !ok || len(vals) == 0 {
			continue
		}

		if !validMetadataName(rest) {
			return nil, &apiError{
				status: http.StatusBadRequest, code: "InvalidMetadata",
				message: "The metadata specified is invalid. It has characters that are not permitted.",
			}
		}

		if md == nil {
			md = make(map[string]string)
		}

		md[strings.ToLower(rest)] = vals[0]
	}

	return md, nil
}

func setMetadataHeaders(hdr http.Header, md map[string]string) {
	for k, v := range md {
		hdr.Set(headerMetaPrefix+k, v)
	}
}

// blobAttrsFromRequest reads the x-ms-blob-* content-property headers and
// x-ms-meta-* metadata.
func blobAttrsFromRequest(r *http.Request) (BlobAttrs, *apiError) {
	md, apiErr := metadataFromRequest(r)
	if apiErr != nil {
		return BlobAttrs{}, apiErr
	}

	attrs := BlobAttrs{
		Metadata:           md,
		ContentType:        r.Header.Get("X-Ms-Blob-Content-Type"),
		ContentEncoding:    r.Header.Get("X-Ms-Blob-Content-Encoding"),
		ContentLanguage:    r.Header.Get("X-Ms-Blob-Content-Language"),
		ContentDisposition: r.Header.Get("X-Ms-Blob-Content-Disposition"),
		CacheControl:       r.Header.Get("X-Ms-Blob-Cache-Control"),
		ContentMD5:         r.Header.Get("X-Ms-Blob-Content-Md5"),
	}

	if attrs.ContentMD5 != "" {
		if raw, err := base64.StdEncoding.DecodeString(attrs.ContentMD5); err != nil || len(raw) != md5Len {
			return BlobAttrs{}, errInvalidHeader("x-ms-blob-content-md5")
		}
	}

	return attrs, nil
}

func errResourceNotFound() *apiError {
	return &apiError{
		status: http.StatusNotFound, code: "ResourceNotFound", message: "The specified resource does not exist.",
	}
}

// denyAnonymous enforces public-access levels on requests carrying neither an
// Authorization header nor a SAS signature. It reports whether the request
// was rejected, in which case the error response has been written.
func (h *Handler) denyAnonymous(c *echo.Context, container, blob string) (bool, error) {
	r := c.Request()
	if !h.EnforcePublicAccess || r.Header.Get("Authorization") != "" || c.QueryParam("sig") != "" {
		return false, nil
	}

	if (r.Method != http.MethodGet && r.Method != http.MethodHead) || container == "" {
		return true, h.writeError(c, http.StatusUnauthorized, "NoAuthenticationInformation",
			"Server failed to authenticate the request. "+
				"Please refer to the information in the www-authenticate header.")
	}

	info, err := h.Backend.GetContainer(container)
	if err != nil {
		return true, h.writeAPIError(c, errResourceNotFound())
	}

	comp := c.QueryParam(queryComp)
	metaOrBody := comp == "" || comp == compMetadata

	allowed := info.PublicAccess == AccessContainer && (comp == compList || metaOrBody)
	if blob != "" {
		allowed = info.PublicAccess != "" && metaOrBody
	}

	if allowed {
		return false, nil
	}

	return true, h.writeAPIError(c, errResourceNotFound())
}

func (h *Handler) setContainerHeaders(c *echo.Context, ci ContainerInfo) {
	hdr := c.Response().Header()
	hdr.Set("Last-Modified", ci.LastModified.Format(http.TimeFormat))
	hdr.Set("ETag", ci.ETag)
	setLeaseHeaders(hdr, ci.Lease)
	setMetadataHeaders(hdr, ci.Metadata)

	if ci.PublicAccess != "" {
		hdr.Set(headerPublicAccess, ci.PublicAccess)
	}
}

func (h *Handler) writeContainerNotFound(c *echo.Context) error {
	return h.writeError(c, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist.")
}

func (h *Handler) createContainer(c *echo.Context, container string) error {
	r := c.Request()

	md, apiErr := metadataFromRequest(r)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	access := r.Header.Get(headerPublicAccess)
	if !validPublicAccess(access) {
		return h.writeAPIError(c, errInvalidHeader(headerPublicAccess))
	}

	ci, err := h.Backend.CreateContainerWith(container, ContainerOptions{Metadata: md, PublicAccess: access})
	if err != nil {
		if errors.Is(err, ErrContainerAlreadyExists) {
			return h.writeError(c, http.StatusConflict, "ContainerAlreadyExists",
				"The specified container already exists.")
		}

		return h.writeError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	hdr := c.Response().Header()
	hdr.Set("ETag", ci.ETag)
	hdr.Set("Last-Modified", ci.LastModified.Format(http.TimeFormat))

	return c.NoContent(http.StatusCreated)
}

// getContainerProperties serves Get Container Properties / Metadata (GET or HEAD).
func (h *Handler) getContainerProperties(c *echo.Context, container string) error {
	pre, apiErr := precondFromRequest(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	ci, err := h.Backend.GetContainer(container, pre)
	if err != nil {
		return h.writeContainerError(c, err)
	}

	h.setContainerHeaders(c, ci)

	return c.NoContent(http.StatusOK)
}

func (h *Handler) setContainerMetadata(c *echo.Context, container string) error {
	md, apiErr := metadataFromRequest(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	pre, apiErr := precondFromRequest(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	ci, err := h.Backend.SetContainerMetadata(container, md, pre)
	if err != nil {
		return h.writeContainerError(c, err)
	}

	c.Response().Header().Set("ETag", ci.ETag)
	c.Response().Header().Set("Last-Modified", ci.LastModified.Format(http.TimeFormat))

	return c.NoContent(http.StatusOK)
}

func (h *Handler) getContainerACL(c *echo.Context, container string) error {
	pre, apiErr := precondFromRequest(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	ci, err := h.Backend.GetContainer(container, pre)
	if err != nil {
		return h.writeContainerError(c, err)
	}

	c.Response().Header().Set("ETag", ci.ETag)
	c.Response().Header().Set("Last-Modified", ci.LastModified.Format(http.TimeFormat))

	if ci.PublicAccess != "" {
		c.Response().Header().Set(headerPublicAccess, ci.PublicAccess)
	}

	out := signedIdentifiers{Identifier: make([]signedIdentifier, 0, len(ci.ACL))}
	for _, id := range ci.ACL {
		out.Identifier = append(out.Identifier, signedIdentifier{
			ID:           id.ID,
			AccessPolicy: accessPolicy{Start: id.Start, Expiry: id.Expiry, Permission: id.Permission},
		})
	}

	return h.writeXML(c, http.StatusOK, out)
}

func (h *Handler) setContainerACL(c *echo.Context, container string) error {
	r := c.Request()

	access := r.Header.Get(headerPublicAccess)
	if !validPublicAccess(access) {
		return h.writeAPIError(c, errInvalidHeader(headerPublicAccess))
	}

	body, err := httputils.ReadBody(r)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	var parsed signedIdentifiers
	if len(strings.TrimSpace(string(body))) > 0 {
		if xmlErr := xml.Unmarshal(body, &parsed); xmlErr != nil || len(parsed.Identifier) > maxSignedIdentifiers {
			return h.writeError(c, http.StatusBadRequest, "InvalidXmlDocument",
				"XML specified is not syntactically valid.")
		}
	}

	ids := make([]SignedIdentifier, 0, len(parsed.Identifier))

	for _, p := range parsed.Identifier {
		if p.ID == "" || len(p.ID) > maxIdentifierLen {
			return h.writeError(c, http.StatusBadRequest, "InvalidXmlDocument",
				"XML specified is not syntactically valid.")
		}

		ids = append(ids, SignedIdentifier{
			ID: p.ID, Start: p.AccessPolicy.Start, Expiry: p.AccessPolicy.Expiry, Permission: p.AccessPolicy.Permission,
		})
	}

	pre, apiErr := precondFromRequest(r)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	ci, err := h.Backend.SetContainerACL(container, access, ids, pre)
	if err != nil {
		return h.writeContainerError(c, err)
	}

	c.Response().Header().Set("ETag", ci.ETag)
	c.Response().Header().Set("Last-Modified", ci.LastModified.Format(http.TimeFormat))

	return c.NoContent(http.StatusOK)
}

func (h *Handler) listBlobs(c *echo.Context, container string) error {
	q := c.QueryParams()
	opts := ListBlobsOptions{
		Prefix:    q.Get("prefix"),
		Delimiter: q.Get("delimiter"),
		Marker:    q.Get("marker"),
	}

	for inc := range strings.SplitSeq(q.Get("include"), ",") {
		switch strings.TrimSpace(inc) {
		case "snapshots":
			opts.IncludeSnapshots = true
		case compTags:
			opts.IncludeTags = true
		}
	}

	if raw := q.Get("maxresults"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return h.writeError(c, http.StatusBadRequest, "InvalidQueryParameterValue",
				"Value for one of the query parameters specified in the request URI is invalid.")
		}

		opts.MaxResults = min(n, maxListResults)
	}

	page, err := h.Backend.ListBlobsPage(container, opts)
	if errors.Is(err, ErrInvalidMarker) {
		return h.writeError(c, http.StatusBadRequest, "InvalidQueryParameterValue",
			"Value for one of the query parameters specified in the request URI is invalid.")
	}

	if err != nil {
		return h.writeContainerNotFound(c)
	}

	echoed := listEcho{
		Prefix: opts.Prefix, Marker: opts.Marker, Delimiter: opts.Delimiter,
		MaxResults: opts.MaxResults, IncludeTags: opts.IncludeTags,
	}

	return c.XMLBlob(http.StatusOK, encodeBlobList(h.serviceEndpoint(), container, echoed, page))
}

func (h *Handler) setBlobProperties(c *echo.Context, container, blob string) error {
	attrs, apiErr := blobAttrsFromRequest(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	pre, apiErr := precondFromRequest(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	info, err := h.Backend.SetBlobProperties(container, blob, attrs, pre)
	if err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	setWriteHeaders(c, info)

	return c.NoContent(http.StatusOK)
}

func setWriteHeaders(c *echo.Context, info BlobInfo) {
	c.Response().Header().Set("ETag", info.ETag)
	c.Response().Header().Set("Last-Modified", info.LastModified.Format(http.TimeFormat))
}

func (h *Handler) setBlobMetadata(c *echo.Context, container, blob string) error {
	md, apiErr := metadataFromRequest(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	pre, apiErr := precondFromRequest(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	info, err := h.Backend.SetBlobMetadata(container, blob, md, pre)
	if err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	setWriteHeaders(c, info)

	return c.NoContent(http.StatusOK)
}

func (h *Handler) getBlobMetadata(c *echo.Context, container, blob string) error {
	pre, apiErr := precondFromRequest(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	info, err := h.Backend.HeadBlobAt(container, blob, c.QueryParam(querySnapshot), pre)
	if err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	setWriteHeaders(c, info)
	setMetadataHeaders(c.Response().Header(), info.Metadata)

	return c.NoContent(http.StatusOK)
}

func (h *Handler) setBlobTier(c *echo.Context, container, blob string) error {
	tier := c.Request().Header.Get(headerAccessTier)
	if tier == "" {
		return h.writeError(c, http.StatusBadRequest, "MissingRequiredHeader",
			"An HTTP header that's mandatory for this request is not specified.")
	}

	if !validTier(tier) {
		return h.writeAPIError(c, errInvalidHeader(headerAccessTier))
	}

	pre, apiErr := precondFromRequest(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	prev, _, err := h.Backend.SetBlobTier(container, blob, tier, pre)
	if err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	if prev == TierArchive && tier != TierArchive {
		return c.NoContent(http.StatusAccepted)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) snapshotBlob(c *echo.Context, container, blob string) error {
	md, apiErr := metadataFromRequest(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	pre, apiErr := precondFromRequest(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	info, err := h.Backend.SnapshotBlob(container, blob, md, pre)
	if err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	setWriteHeaders(c, info)
	c.Response().Header().Set("X-Ms-Snapshot", info.Snapshot)

	return c.NoContent(http.StatusCreated)
}

func (h *Handler) copyBlob(c *echo.Context, container, blob string) error {
	r := c.Request()

	md, apiErr := metadataFromRequest(r)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	tags, apiErr := tagsFromHeader(r)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	src, err := url.Parse(r.Header.Get(headerCopySource))
	if err != nil {
		return h.writeAPIError(c, errInvalidHeader(headerCopySource))
	}

	reqAccount, _, _ := splitPath(r.URL.Path)
	srcAccount, srcContainer, srcBlob := splitPath(src.Path)

	if srcAccount != reqAccount || srcContainer == "" || srcBlob == "" {
		return h.writeError(c, http.StatusNotFound, "CannotVerifyCopySource",
			"Could not verify the copy source within the specified time.")
	}

	pre, apiErr := precondFromRequest(r)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	info, err := h.Backend.CopyBlob(
		srcContainer, srcBlob, src.Query().Get(querySnapshot), container, blob, md, tags, pre,
	)
	if err != nil {
		if done, werr := h.writeBackendError(c, err); done {
			return werr
		}

		if errors.Is(err, ErrContainerNotFound) && srcContainer != container {
			if _, cErr := h.Backend.GetContainer(container); cErr != nil {
				return h.writeContainerNotFound(c)
			}
		}

		return h.writeError(c, http.StatusNotFound, "CannotVerifyCopySource",
			"Could not verify the copy source within the specified time.")
	}

	setWriteHeaders(c, info)
	c.Response().Header().Set("X-Ms-Copy-Id", newRequestID())
	c.Response().Header().Set("X-Ms-Copy-Status", "success")

	return c.NoContent(http.StatusAccepted)
}
