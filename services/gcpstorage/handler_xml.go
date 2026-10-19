package gcpstorage

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const metaHeaderPrefix = "X-Goog-Meta-"

// serveObject writes o as a media response, honouring Range and conditional headers.
func serveObject(c *echo.Context, o *Object) {
	hdr := c.Response().Header()
	hdr.Set("Content-Type", o.ContentType)
	hdr.Set("X-Goog-Generation", itoa64(o.Generation))
	hdr.Set("X-Goog-Metageneration", itoa64(o.Metageneration))
	hdr.Set("X-Goog-Stored-Content-Length", itoa64(o.Size()))
	hdr.Set("X-Goog-Storage-Class", o.StorageClass)
	hdr.Set("Accept-Ranges", "bytes")

	enc := o.ContentEncoding
	if enc != "" {
		hdr.Set("Content-Encoding", enc)
	} else {
		enc = "identity"
	}

	hdr.Set("X-Goog-Stored-Content-Encoding", enc)

	hash := "crc32c=" + o.CRC32C
	if o.MD5 != "" {
		hash += ",md5=" + o.MD5

		if raw, err := base64.StdEncoding.DecodeString(o.MD5); err == nil {
			hdr.Set("ETag", `"`+hex.EncodeToString(raw)+`"`)
		}
	} else {
		hdr.Set("ETag", `"`+objectETag(o)+`"`)
	}

	hdr.Set("X-Goog-Hash", hash)

	for k, v := range map[string]string{
		"Cache-Control": o.CacheControl, "Content-Disposition": o.ContentDisposition,
		"Content-Language": o.ContentLanguage,
	} {
		if v != "" {
			hdr.Set(k, v)
		}
	}

	for k, v := range o.Metadata {
		hdr.Set(metaHeaderPrefix+k, v)
	}

	http.ServeContent(c.Response(), c.Request(), "", o.Updated, bytes.NewReader(o.Data))
}

func (h *Handler) xmlGetObject(c *echo.Context, rt route) error {
	gen, apiErr := parseGeneration(c.QueryParams(), "generation")
	if apiErr != nil {
		return apiErr
	}

	pre := xmlPreconds(c.Request().Header)

	o, err := h.Backend.GetObject(rt.bucket, rt.object, gen, pre)
	if err != nil {
		return err
	}

	serveObject(c, o)

	return nil
}

func xmlPreconds(hdr http.Header) Preconds {
	var pre Preconds

	for key, dst := range map[string]**int64{
		"X-Goog-If-Generation-Match":     &pre.GenMatch,
		"X-Goog-If-Metageneration-Match": &pre.MetaMatch,
	} {
		if v := hdr.Get(key); v != "" {
			_ = parseOptInt(map[string][]string{"v": {v}}, "v", dst)
		}
	}

	return pre
}

func (h *Handler) xmlPutObject(c *echo.Context, rt route) error {
	r := c.Request()

	data, err := readBody(c)
	if err != nil {
		return err
	}

	meta := Patch{}

	for k, v := range map[string]string{
		keyContentType: r.Header.Get("Content-Type"), keyContentEncoding: r.Header.Get("Content-Encoding"),
		keyCacheControl: r.Header.Get("Cache-Control"), keyContentDisposition: r.Header.Get("Content-Disposition"),
		keyContentLanguage: r.Header.Get("Content-Language"), keyStorageClass: r.Header.Get("X-Goog-Storage-Class"),
	} {
		if v != "" {
			meta[k] = marshalRaw(v)
		}
	}

	custom := map[string]string{}

	for k, v := range r.Header {
		if len(v) > 0 && strings.HasPrefix(k, metaHeaderPrefix) {
			custom[strings.ToLower(k[len(metaHeaderPrefix):])] = v[0]
		}
	}

	if len(custom) > 0 {
		meta[keyMetadata] = marshalRaw(custom)
	}

	o, err := h.Backend.PutObject(rt.bucket, rt.object, data, meta, xmlPreconds(r.Header))
	if err != nil {
		return err
	}

	hdr := c.Response().Header()
	hdr.Set("X-Goog-Generation", itoa64(o.Generation))
	hdr.Set("X-Goog-Metageneration", itoa64(o.Metageneration))
	hdr.Set("X-Goog-Hash", "crc32c="+o.CRC32C+",md5="+o.MD5)

	if raw, decErr := base64.StdEncoding.DecodeString(o.MD5); decErr == nil {
		hdr.Set("ETag", `"`+hex.EncodeToString(raw)+`"`)
	}

	hdr.Set("Last-Modified", o.Updated.UTC().Format(http.TimeFormat))

	return c.NoContent(http.StatusOK)
}

func (h *Handler) xmlDeleteObject(c *echo.Context, rt route) error {
	gen, apiErr := parseGeneration(c.QueryParams(), "generation")
	if apiErr != nil {
		return apiErr
	}

	if err := h.Backend.DeleteObject(rt.bucket, rt.object, gen, xmlPreconds(c.Request().Header)); err != nil {
		return err
	}

	return c.NoContent(http.StatusNoContent)
}
