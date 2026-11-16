package azureblob

import (
	"encoding/xml"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	headerBlobContentLength = "X-Ms-Blob-Content-Length"
	headerSequenceNumber    = "X-Ms-Blob-Sequence-Number"
	headerPageWrite         = "X-Ms-Page-Write"
	headerRehydratePriority = "X-Ms-Rehydrate-Priority"
	headerCopyAction        = "X-Ms-Copy-Action"
	headerAppendPos         = "X-Ms-Blob-Condition-Appendpos"
	pageWriteUpdate         = "update"
	pageWriteClear          = "clear"
	headerMaxSize           = "X-Ms-Blob-Condition-Maxsize"
	queryCopyID             = "copyid"
)

// blobKindFromRequest reads x-ms-blob-type and its type-specific headers into attrs.
func blobKindFromRequest(r *http.Request, attrs *BlobAttrs) *apiError {
	attrs.BlobType = r.Header.Get("X-Ms-Blob-Type")

	switch attrs.BlobType {
	case BlobTypeBlock, BlobTypeAppend:
		return nil
	case BlobTypePage:
		n, err := strconv.ParseInt(r.Header.Get(headerBlobContentLength), 10, 64)
		if err != nil || n < 0 || n%PageSize != 0 {
			return errInvalidHeader(headerBlobContentLength)
		}

		if n > MaxPageBlobLength {
			return &apiError{
				status: http.StatusBadRequest, code: "InvalidHeaderValue",
				message: "The page blob size exceeds the emulator limit of 256 MiB.",
			}
		}

		attrs.PageLength = n

		if raw := r.Header.Get(headerSequenceNumber); raw != "" {
			if attrs.SequenceNumber, err = strconv.ParseInt(raw, 10, 64); err != nil || attrs.SequenceNumber < 0 {
				return errInvalidHeader(headerSequenceNumber)
			}
		}

		return nil
	default:
		return errInvalidHeader("x-ms-blob-type")
	}
}

func setKindHeaders(hdr http.Header, info BlobInfo) {
	switch info.BlobType {
	case BlobTypeAppend:
		hdr.Set("X-Ms-Blob-Committed-Block-Count", strconv.Itoa(info.CommittedBlocks))
	case BlobTypePage:
		hdr.Set(headerSequenceNumber, strconv.FormatInt(info.SequenceNumber, 10))
	}

	if info.ArchiveStatus != "" {
		hdr.Set("X-Ms-Archive-Status", info.ArchiveStatus)
	}

	if info.CopyID == "" {
		return
	}

	hdr.Set("X-Ms-Copy-Id", info.CopyID)
	hdr.Set("X-Ms-Copy-Status", info.CopyStatus)
	hdr.Set("X-Ms-Copy-Source", info.CopySource)
	hdr.Set("X-Ms-Copy-Progress", info.CopyProgress)
	hdr.Set("X-Ms-Copy-Completion-Time", info.CopyCompletion.Format(http.TimeFormat))
}

func (h *Handler) appendBlock(c *echo.Context, container, blob string) error {
	r := c.Request()

	var opts AppendOptions

	for hdrName, dst := range map[string]**int64{headerAppendPos: &opts.AppendPos, headerMaxSize: &opts.MaxSize} {
		raw := r.Header.Get(hdrName)
		if raw == "" {
			continue
		}

		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			return h.writeAPIError(c, errInvalidHeader(hdrName))
		}

		*dst = &n
	}

	pre, apiErr := precondFromRequest(r)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	body, apiErr := h.readLimitedBody(r, MaxAppendBlockSize)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	if apiErr = verifyTransactional(r, body, "Content-Md5", headerContentCRC64); apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	res, err := h.Backend.AppendBlock(container, blob, body, opts, pre)
	if err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	hdr := c.Response().Header()
	setWriteHeaders(c, res.Info)
	hdr.Set("Content-MD5", md5Header(body))
	hdr.Set("X-Ms-Blob-Append-Offset", strconv.FormatInt(res.Offset, 10))
	hdr.Set("X-Ms-Blob-Committed-Block-Count", strconv.Itoa(res.Info.CommittedBlocks))
	hdr.Set(headerServerEncry, "true")

	return c.NoContent(http.StatusCreated)
}

func errBodyTooLarge() *apiError {
	return &apiError{
		status: http.StatusRequestEntityTooLarge, code: "RequestBodyTooLarge",
		message: "The size of the request body exceeds the maximum size permitted.",
	}
}

func (h *Handler) readLimitedBody(r *http.Request, limit int64) ([]byte, *apiError) {
	if r.ContentLength > limit {
		return nil, errBodyTooLarge()
	}

	body, err := httputils.ReadBody(r)
	if err != nil {
		return nil, &apiError{
			status: http.StatusInternalServerError, code: "InternalError", message: "Failed to read request body.",
		}
	}

	if int64(len(body)) > limit {
		return nil, errBodyTooLarge()
	}

	return body, nil
}

// parseByteRange parses "bytes=a-b" (x-ms-range / Range) into inclusive bounds.
func parseByteRange(v string) (int64, int64, bool) {
	spec, ok := strings.CutPrefix(v, "bytes=")
	if !ok {
		return 0, 0, false
	}

	lo, hi, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, false
	}

	start, err1 := strconv.ParseInt(lo, 10, 64)
	end, err2 := strconv.ParseInt(hi, 10, 64)

	return start, end, err1 == nil && err2 == nil
}

func requestRange(r *http.Request) string {
	if v := r.Header.Get("X-Ms-Range"); v != "" {
		return v
	}

	return r.Header.Get("Range")
}

func (h *Handler) putPage(c *echo.Context, container, blob string) error {
	r := c.Request()

	start, end, ok := parseByteRange(requestRange(r))
	if !ok {
		return h.writeAPIError(c, errInvalidHeader("x-ms-range"))
	}

	action := strings.ToLower(r.Header.Get(headerPageWrite))
	if action != pageWriteUpdate && action != pageWriteClear {
		return h.writeAPIError(c, errInvalidHeader(headerPageWrite))
	}

	pre, apiErr := precondFromRequest(r)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	var body []byte

	if action == pageWriteUpdate {
		if body, apiErr = h.readLimitedBody(r, MaxPageWrite); apiErr != nil {
			return h.writeAPIError(c, apiErr)
		}

		if int64(len(body)) != end-start+1 {
			return h.writeAPIError(c, errInvalidHeader("Content-Length"))
		}

		if apiErr = verifyTransactional(r, body, "Content-Md5", headerContentCRC64); apiErr != nil {
			return h.writeAPIError(c, apiErr)
		}
	}

	info, err := h.Backend.PutPage(container, blob, start, end, body, action == pageWriteClear, pre)
	if err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	setWriteHeaders(c, info)
	c.Response().Header().Set(headerSequenceNumber, strconv.FormatInt(info.SequenceNumber, 10))

	if action == pageWriteUpdate {
		c.Response().Header().Set("Content-MD5", md5Header(body))
		c.Response().Header().Set(headerServerEncry, "true")
	}

	return c.NoContent(http.StatusCreated)
}

type pageRangeXML struct {
	Start int64 `xml:"Start"`
	End   int64 `xml:"End"`
}

type pageListXML struct {
	XMLName xml.Name       `xml:"PageList"`
	Ranges  []pageRangeXML `xml:"PageRange"`
}

func (h *Handler) getPageRanges(c *echo.Context, container, blob string) error {
	r := c.Request()

	var from, to *int64

	if raw := requestRange(r); raw != "" {
		lo, hi, ok := parseByteRange(raw)
		if !ok {
			return h.writeAPIError(c, errInvalidHeader("x-ms-range"))
		}

		from, to = &lo, &hi
	}

	pre, apiErr := precondFromRequest(r)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	ranges, info, err := h.Backend.GetPageRanges(container, blob, c.QueryParam(querySnapshot), from, to, pre)
	if err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	out := pageListXML{Ranges: make([]pageRangeXML, len(ranges))}
	for i, pr := range ranges {
		out.Ranges[i] = pageRangeXML(pr)
	}

	hdr := c.Response().Header()
	hdr.Set("ETag", info.ETag)
	hdr.Set("Last-Modified", info.LastModified.Format(http.TimeFormat))
	hdr.Set("X-Ms-Blob-Content-Length", strconv.FormatInt(info.ContentLength, 10))

	return h.writeXML(c, http.StatusOK, out)
}

func (h *Handler) abortCopy(c *echo.Context, container, blob string) error {
	r := c.Request()

	if !strings.EqualFold(r.Header.Get(headerCopyAction), "abort") {
		return h.writeAPIError(c, errInvalidHeader(headerCopyAction))
	}

	copyID := c.QueryParam(queryCopyID)
	if copyID == "" {
		return h.writeAPIError(c, &apiError{
			status: http.StatusBadRequest, code: "MissingRequiredQueryParameter",
			message: "A required query parameter was not specified for this request (copyid).",
		})
	}

	pre, apiErr := precondFromRequest(r)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	if err := h.Backend.AbortCopy(container, blob, copyID, pre); err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) setServiceProperties(c *echo.Context) error {
	body, err := io.ReadAll(io.LimitReader(c.Request().Body, maxServicePropsXML+1))
	if err != nil || len(body) > maxServicePropsXML {
		return h.writeError(c, http.StatusBadRequest, "InvalidXmlDocument",
			"XML specified is not syntactically valid.")
	}

	var sp ServiceProperties
	if err = xml.Unmarshal(body, &sp); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidXmlDocument",
			"XML specified is not syntactically valid.")
	}

	if err = h.Backend.SetServiceProperties(sp); err != nil {
		if done, werr := h.writeBackendError(c, err); done {
			return werr
		}

		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to store properties.")
	}

	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) undeleteBlob(c *echo.Context, container, blob string) error {
	if err := h.Backend.UndeleteBlob(container, blob); err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
