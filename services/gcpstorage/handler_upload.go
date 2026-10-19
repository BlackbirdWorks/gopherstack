package gcpstorage

import (
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

const statusClientClosedRequest = 499

func (h *Handler) insertObject(c *echo.Context, rt route) error {
	q := c.QueryParams()
	r := c.Request()

	pre, apiErr := parsePreconds(q, "if")
	if apiErr != nil {
		return apiErr
	}

	switch q.Get("uploadType") {
	case "media":
		data, err := readBody(c)
		if err != nil {
			return err
		}

		meta := Patch{}
		if ct := r.Header.Get("Content-Type"); ct != "" {
			meta[keyContentType] = marshalRaw(ct)
		}

		if ce := q.Get(keyContentEncoding); ce != "" {
			meta[keyContentEncoding] = marshalRaw(ce)
		}

		return h.finishInsert(c, rt.bucket, q.Get("name"), data, meta, pre)
	case "multipart":
		return h.multipartInsert(c, rt.bucket, pre)
	case "resumable":
		return h.startResumable(c, rt.bucket, pre)
	default:
		return errInvalid("Invalid uploadType.")
	}
}

func (h *Handler) finishInsert(c *echo.Context, bucket, name string, data []byte, meta Patch, pre Preconds) error {
	if name == "" {
		name = meta.str("name")
	}

	if name == "" {
		return errRequired("name")
	}

	o, err := h.Backend.PutObject(bucket, name, data, meta, pre)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, objectResource(baseURL(c.Request()), o))
}

func (h *Handler) multipartInsert(c *echo.Context, bucket string, pre Preconds) error {
	r := c.Request()

	mt, params, parseErr := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if parseErr != nil || !strings.HasPrefix(mt, "multipart/") || params["boundary"] == "" {
		return errInvalid("Content-Type must be multipart/related with a boundary.")
	}

	mr := multipart.NewReader(http.MaxBytesReader(c.Response(), r.Body, maxBodyBytes), params["boundary"])

	metaPart, partErr := mr.NextPart()
	if partErr != nil {
		return errInvalid("Multipart upload is missing the metadata part.")
	}

	metaBytes, _ := io.ReadAll(metaPart)

	meta, apiErr := decodePatch(metaBytes)
	if apiErr != nil {
		return apiErr
	}

	mediaPart, partErr := mr.NextPart()
	if partErr != nil {
		return errInvalid("Multipart upload is missing the media part.")
	}

	data, readErr := io.ReadAll(mediaPart)
	if readErr != nil {
		return errInvalid("Could not read the media part.")
	}

	if _, ok := meta[keyContentType]; !ok {
		if ct := mediaPart.Header.Get("Content-Type"); ct != "" {
			meta[keyContentType] = marshalRaw(ct)
		}
	}

	return h.finishInsert(c, bucket, c.QueryParam("name"), data, meta, pre)
}

func (h *Handler) startResumable(c *echo.Context, bucket string, pre Preconds) error {
	r := c.Request()

	meta, err := readPatch(c)
	if err != nil {
		return err
	}

	if _, ok := meta[keyContentType]; !ok {
		if ct := r.Header.Get("X-Upload-Content-Type"); ct != "" {
			meta[keyContentType] = marshalRaw(ct)
		}
	}

	name := c.QueryParam("name")
	if name == "" {
		name = meta.str("name")
	}

	if name == "" {
		return errRequired("name")
	}

	id, err := h.Backend.StartUpload(bucket, name, meta, pre)
	if err != nil {
		return err
	}

	loc := baseURL(r) + "/upload/storage/v1/b/" + bucket + "/o?uploadType=resumable&upload_id=" + id
	c.Response().Header().Set("Location", loc)
	c.Response().Header().Set("X-Guploader-Uploadid", id)

	return c.NoContent(http.StatusOK)
}

// parseContentRange parses "bytes a-b/total", "bytes */total" and "bytes a-b/*".
// offset is -1 for a status query; total is -1 when unknown.
func parseContentRange(h string) (int64, int64, bool) {
	spec, ok := strings.CutPrefix(strings.TrimSpace(h), "bytes ")
	if !ok {
		return 0, 0, false
	}

	rng, totalStr, ok := strings.Cut(spec, "/")
	if !ok {
		return 0, 0, false
	}

	total := int64(-1)

	if totalStr != "*" {
		n, err := strconv.ParseInt(totalStr, 10, 64)
		if err != nil {
			return 0, 0, false
		}

		total = n
	}

	if rng == "*" {
		return -1, total, true
	}

	startStr, _, ok := strings.Cut(rng, "-")
	if !ok {
		return 0, 0, false
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		return 0, 0, false
	}

	return start, total, true
}

func (h *Handler) resumableUpload(c *echo.Context, _ route) error {
	id := c.QueryParam("upload_id")
	if id == "" {
		return errRequired("upload_id")
	}

	chunk, err := readBody(c)
	if err != nil {
		return err
	}

	offset, total := int64(0), int64(len(chunk))

	if cr := c.Request().Header.Get("Content-Range"); cr != "" {
		var ok bool
		if offset, total, ok = parseContentRange(cr); !ok {
			return errInvalid("Invalid Content-Range header.")
		}

		if offset < 0 {
			offset = 0
		}
	}

	prog, err := h.Backend.AppendUpload(id, offset, chunk, total)
	if err != nil {
		return err
	}

	c.Response().Header().Set("X-Guploader-Uploadid", id)

	if prog.Object != nil {
		return c.JSON(http.StatusOK, objectResource(baseURL(c.Request()), prog.Object))
	}

	if prog.Received > 0 {
		c.Response().Header().Set("Range", "bytes=0-"+strconv.FormatInt(prog.Received-1, 10))
	}

	if c.Request().Header.Get("X-Guploader-No-308") == "yes" {
		c.Response().Header().Set("X-Http-Status-Code-Override", "308")

		return c.NoContent(http.StatusOK)
	}

	return c.NoContent(http.StatusPermanentRedirect)
}

func (h *Handler) cancelUpload(c *echo.Context, _ route) error {
	if err := h.Backend.CancelUpload(c.QueryParam("upload_id")); err != nil {
		return err
	}

	return c.NoContent(statusClientClosedRequest)
}
