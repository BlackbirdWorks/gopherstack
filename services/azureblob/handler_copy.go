package azureblob

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

// MaxCopySourceBytes bounds the body fetched from a copy source URL.
const MaxCopySourceBytes = 256 << 20

var (
	errCopySourceTooLarge  = errors.New("azureblob: copy source exceeds size limit")
	errCopySourceStatus    = errors.New("azureblob: copy source answered with an error status")
	errCopyDestinationDeny = errors.New("azureblob: copy source address is not permitted")
)

const copyFetchTimeout = 60 * time.Second

// SetAllowExternalCopy lets copy sources resolve to non-loopback addresses; by default only loopback is reachable.
func (h *Handler) SetAllowExternalCopy(allow bool) { h.allowExternalCopy.Store(allow) }

func newCopyClient(allow func() bool) *http.Client {
	tr := httpclient.NewTransport()
	tr.Proxy = nil
	tr.DialContext = httpclient.LoopbackDialer(allow, errCopyDestinationDeny).DialContext

	return &http.Client{
		Timeout:       copyFetchTimeout,
		Transport:     tr,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func (h *Handler) copyHTTP() *http.Client { return h.copyClient }

// sourceForRecord is the copy source with its query dropped except the snapshot, so a SAS never reaches properties.
func sourceForRecord(u *url.URL) string {
	c := *u
	c.User = nil

	q := url.Values{}
	if s := u.Query().Get(querySnapshot); s != "" {
		q.Set(querySnapshot, s)
	}

	c.RawQuery = q.Encode()

	return c.String()
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
	if err != nil || !validSourceScheme(src.Scheme) || src.Host == "" {
		return h.writeAPIError(c, errInvalidHeader(headerCopySource))
	}

	pre, apiErr := precondFromRequest(r)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	req := CopyRequest{Metadata: md, Tags: tags, SourceURL: sourceForRecord(src)}
	reqAccount, _, _ := splitPath(r.URL.Path)
	srcAccount, srcContainer, srcBlob := splitPath(src.Path)

	if srcAccount != reqAccount {
		return h.copyFromURL(c, container, blob, src, req, pre)
	}

	if srcContainer == "" || srcBlob == "" {
		return h.writeError(c, http.StatusNotFound, "CannotVerifyCopySource",
			"Could not verify the copy source within the specified time.")
	}

	info, err := h.Backend.CopyBlob(srcContainer, srcBlob, src.Query().Get(querySnapshot), container, blob, req, pre)
	if err != nil {
		return h.writeCopyError(c, err, srcContainer, container)
	}

	return h.writeCopyAccepted(c, info)
}

func (h *Handler) writeCopyError(c *echo.Context, err error, srcContainer, container string) error {
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

func (h *Handler) writeCopyAccepted(c *echo.Context, info BlobInfo) error {
	setWriteHeaders(c, info)
	c.Response().Header().Set("X-Ms-Copy-Id", info.CopyID)
	c.Response().Header().Set("X-Ms-Copy-Status", info.CopyStatus)

	return c.NoContent(http.StatusAccepted)
}

// copyFromURL fetches a source of another account or host. Source authorisation is the SAS in the URL; no
// credentials of the caller are forwarded.
func (h *Handler) copyFromURL(
	c *echo.Context, container, blob string, src *url.URL, req CopyRequest, pre Precond,
) error {
	r := c.Request()

	if _, err := h.Backend.GetContainer(container); err != nil {
		return h.writeContainerNotFound(c)
	}

	ext, status, err := h.fetchCopySource(r, src)
	if err != nil {
		if errors.Is(err, errCopySourceTooLarge) {
			return h.writeError(c, http.StatusBadRequest, "InvalidHeaderValue",
				"The copy source exceeds the maximum size the emulator copies.")
		}

		return h.writeError(c, copySourceStatus(status), "CannotVerifyCopySource",
			"Could not verify the copy source within the specified time.")
	}

	info, err := h.Backend.CopyBlobData(container, blob, ext, req, pre)
	if err != nil {
		return h.writeCopyError(c, err, "", container)
	}

	return h.writeCopyAccepted(c, info)
}

func copySourceStatus(upstream int) int {
	if upstream == http.StatusForbidden || upstream == http.StatusNotFound {
		return upstream
	}

	return http.StatusNotFound
}

func (h *Handler) fetchCopySource(r *http.Request, src *url.URL) (ExternalBlob, int, error) {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, src.String(), nil)
	if err != nil {
		return ExternalBlob{}, 0, err
	}

	resp, err := h.copyHTTP().Do(req)
	if err != nil {
		return ExternalBlob{}, 0, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ExternalBlob{}, resp.StatusCode, errCopySourceStatus
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxCopySourceBytes+1))
	if err != nil {
		return ExternalBlob{}, 0, err
	}

	if int64(len(data)) > MaxCopySourceBytes {
		return ExternalBlob{}, 0, errCopySourceTooLarge
	}

	ext := ExternalBlob{
		Data: data,
		Attrs: BlobAttrs{
			ContentType:        resp.Header.Get("Content-Type"),
			ContentEncoding:    resp.Header.Get("Content-Encoding"),
			ContentLanguage:    resp.Header.Get("Content-Language"),
			ContentDisposition: resp.Header.Get("Content-Disposition"),
			CacheControl:       resp.Header.Get("Cache-Control"),
			ContentMD5:         resp.Header.Get("Content-MD5"),
		},
	}

	for name, vals := range resp.Header {
		rest, ok := strings.CutPrefix(name, headerMetaPrefix)
		if !ok || len(vals) == 0 || !validMetadataName(rest) {
			continue
		}

		if ext.Metadata == nil {
			ext.Metadata = make(map[string]string)
		}

		ext.Metadata[strings.ToLower(rest)] = vals[0]
	}

	return ext, http.StatusOK, nil
}

func validSourceScheme(s string) bool { return s == "http" || s == "https" }
