package azureblob

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	queryBlockID      = "blockid"
	queryBlockType    = "blocklisttype"
	compBlock         = "block"
	compBlockList     = "blocklist"
	compLease         = "lease"
	headerBlobLength  = "X-Ms-Blob-Content-Length"
	headerServerEncry = "X-Ms-Request-Server-Encrypted"
)

type blockListRequest struct {
	XMLName xml.Name           `xml:"BlockList"`
	Items   []blockListReqItem `xml:",any"`
}

type blockListReqItem struct {
	XMLName xml.Name
	ID      string `xml:",chardata"`
}

type blockListResponse struct {
	XMLName     xml.Name      `xml:"BlockList"`
	Committed   *blockEntries `xml:"CommittedBlocks"`
	Uncommitted *blockEntries `xml:"UncommittedBlocks"`
}

type blockEntries struct {
	Block []blockXML `xml:"Block"`
}

type blockXML struct {
	Name string `xml:"Name"`
	Size int64  `xml:"Size"`
}

func blockEntriesXML(in []BlockEntry) *blockEntries {
	if in == nil {
		return nil
	}

	out := &blockEntries{Block: make([]blockXML, len(in))}
	for i, e := range in {
		out.Block[i] = blockXML{Name: e.Name, Size: e.Size}
	}

	return out
}

func (h *Handler) readBlockBody(c *echo.Context) ([]byte, *apiError) {
	r := c.Request()
	if r.ContentLength > MaxBlockSize {
		return nil, &apiError{
			status: http.StatusRequestEntityTooLarge, code: "RequestBodyTooLarge",
			message: "The size of the request body exceeds the maximum size permitted.",
		}
	}

	body, err := httputils.ReadBody(r)
	if err != nil {
		return nil, &apiError{
			status: http.StatusInternalServerError, code: "InternalError", message: "Failed to read request body.",
		}
	}

	return body, nil
}

func (h *Handler) putBlock(c *echo.Context, container, blob string) error {
	r := c.Request()

	blockID := strings.ReplaceAll(c.QueryParam(queryBlockID), " ", "+")
	if blockID == "" {
		return h.writeAPIError(c, &apiError{
			status: http.StatusBadRequest, code: "MissingRequiredQueryParameter",
			message: "A required query parameter was not specified for this request (blockid).",
		})
	}

	pre, apiErr := precondFromRequest(r)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	var body []byte

	if r.Header.Get(headerCopySource) != "" {
		src, srcErr := h.readCopySourceRange(c, pre)
		if srcErr != nil {
			return srcErr
		}

		body = src
	} else {
		var readErr *apiError
		if body, readErr = h.readBlockBody(c); readErr != nil {
			return h.writeAPIError(c, readErr)
		}

		if apiErr = verifyTransactional(r, body, "Content-Md5", headerContentCRC64); apiErr != nil {
			return h.writeAPIError(c, apiErr)
		}
	}

	if err := h.Backend.PutBlock(container, blob, blockID, body, pre); err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	hdr := c.Response().Header()
	hdr.Set("Content-MD5", md5Header(body))
	hdr.Set(headerContentCRC64, crc64Header(body))
	hdr.Set(headerServerEncry, "true")

	return c.NoContent(http.StatusCreated)
}

// readCopySourceRange serves Put Block From URL: it reads the bytes of an
// own-account blob URL (optionally x-ms-source-range) after checking the
// source conditions and checksums. A non-nil result error is already written.
func (h *Handler) readCopySourceRange(c *echo.Context, pre Precond) ([]byte, error) {
	r := c.Request()

	src, err := url.Parse(r.Header.Get(headerCopySource))
	if err != nil || (src.Scheme != "http" && src.Scheme != "https") {
		return nil, h.writeAPIError(c, errInvalidHeader(headerCopySource))
	}

	reqAccount, _, _ := splitPath(r.URL.Path)
	srcAccount, srcContainer, srcBlob := splitPath(src.Path)

	if srcAccount != reqAccount || srcContainer == "" || srcBlob == "" {
		return nil, h.writeError(c, http.StatusNotFound, "CannotVerifyCopySource",
			"Could not verify the copy source within the specified time.")
	}

	var srcPre Precond
	if pre.Source != nil {
		srcPre = *pre.Source
	}

	_, data, err := h.Backend.GetBlobAt(srcContainer, srcBlob, src.Query().Get(querySnapshot), srcPre)
	if err != nil {
		err = asSourceCondition(err)
		if done, werr := h.writeBackendError(c, err); done {
			return nil, werr
		}

		return nil, h.writeError(c, http.StatusNotFound, "CannotVerifyCopySource",
			"Could not verify the copy source within the specified time.")
	}

	if rng := r.Header.Get(headerSourceRange); rng != "" {
		start, end, ok := parseRange(rng, int64(len(data)))
		if !ok {
			return nil, h.writeError(c, http.StatusRequestedRangeNotSatisfiable, "InvalidRange",
				"The range specified is invalid for the current size of the resource.")
		}

		data = data[start : end+1]
	}

	if apiErr := verifyTransactional(r, data, headerSourceMD5, headerSourceCRC64); apiErr != nil {
		return nil, h.writeAPIError(c, apiErr)
	}

	return data, nil
}

func (h *Handler) putBlockList(c *echo.Context, container, blob string) error {
	r := c.Request()

	pre, apiErr := precondFromRequest(r)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	attrs, apiErr := blobAttrsFromRequest(r)
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	attrs.AccessTier = r.Header.Get(headerAccessTier)
	if attrs.AccessTier != "" && !validTier(attrs.AccessTier) {
		return h.writeAPIError(c, errInvalidHeader(headerAccessTier))
	}

	body, readErr := h.readBlockBody(c)
	if readErr != nil {
		return h.writeAPIError(c, readErr)
	}

	if apiErr = verifyTransactional(r, body, "Content-Md5", headerContentCRC64); apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	var req blockListRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidXmlDocument", "XML specified is not syntactically valid.")
	}

	refs := make([]BlockRef, 0, len(req.Items))

	for _, it := range req.Items {
		kind := it.XMLName.Local
		if (kind != BlockKindCommitted && kind != BlockKindUncommitted && kind != BlockKindLatest) || it.ID == "" {
			return h.writeError(c, http.StatusBadRequest, "InvalidXmlDocument",
				"XML specified is not syntactically valid.")
		}

		refs = append(refs, BlockRef{ID: strings.TrimSpace(it.ID), Kind: kind})
	}

	info, err := h.Backend.CommitBlockList(container, blob, refs, attrs, pre)
	if err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	setWriteHeaders(c, info)
	c.Response().Header().Set("Content-MD5", md5Header(body))
	c.Response().Header().Set(headerServerEncry, "true")

	return c.NoContent(http.StatusCreated)
}

func (h *Handler) getBlockList(c *echo.Context, container, blob string) error {
	listType := strings.ToLower(c.QueryParam(queryBlockType))

	switch listType {
	case "":
		listType = BlockListCommitted
	case BlockListCommitted, BlockListUncommitted, BlockListAll:
	default:
		return h.writeError(c, http.StatusBadRequest, "InvalidQueryParameterValue",
			"Value for one of the query parameters specified in the request URI is invalid.")
	}

	pre, apiErr := precondFromRequest(c.Request())
	if apiErr != nil {
		return h.writeAPIError(c, apiErr)
	}

	bl, err := h.Backend.GetBlockList(container, blob, c.QueryParam(querySnapshot), listType, pre)
	if err != nil {
		return h.writeBlobNotFoundError(c, err)
	}

	if bl.HasBlob {
		setWriteHeaders(c, bl.Blob)
		c.Response().Header().Set(headerBlobLength, strconv.FormatInt(bl.Blob.ContentLength, 10))
	}

	return h.writeXML(c, http.StatusOK, blockListResponse{
		Committed: blockEntriesXML(bl.Committed), Uncommitted: blockEntriesXML(bl.Uncommitted),
	})
}
