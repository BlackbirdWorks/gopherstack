package gcpstorage

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

func acceptsGzip(h http.Header) bool {
	for _, v := range h.Values("Accept-Encoding") {
		for part := range strings.SplitSeq(v, ",") {
			name, params, _ := strings.Cut(strings.TrimSpace(part), ";")
			if !strings.EqualFold(strings.TrimSpace(name), "gzip") {
				continue
			}

			q := strings.ReplaceAll(strings.ToLower(params), " ", "")

			return q != "q=0" && q != "q=0.0" && q != "q=0.00" && q != "q=0.000"
		}
	}

	return false
}

// transcodes reports whether a read of o is decompressed on the fly (decompressive transcoding).
func transcodes(r *http.Request, o *Object) bool {
	if !strings.EqualFold(o.ContentEncoding, "gzip") || acceptsGzip(r.Header) {
		return false
	}

	return !strings.Contains(strings.ToLower(o.CacheControl), "no-transform")
}

// serveTranscoded writes o gunzipped without Content-Encoding or Content-Length and ignores Range.
func serveTranscoded(c *echo.Context, o *Object) {
	zr, err := gzip.NewReader(bytes.NewReader(o.Data))
	if err != nil {
		http.ServeContent(c.Response(), c.Request(), "", o.Updated, bytes.NewReader(o.Data))

		return
	}

	plain, err := io.ReadAll(zr)
	if err != nil {
		http.ServeContent(c.Response(), c.Request(), "", o.Updated, bytes.NewReader(o.Data))

		return
	}

	hdr := c.Response().Header()
	hdr.Del("Content-Encoding")
	hdr.Del("Accept-Ranges")
	hdr.Set("X-Goog-Stored-Content-Encoding", "gzip")
	hdr.Set("X-Goog-Stored-Content-Length", strconv.Itoa(len(o.Data)))
	hdr.Set("Vary", "Accept-Encoding")
	c.Response().WriteHeader(http.StatusOK)

	if c.Request().Method == http.MethodHead {
		return
	}

	rc := http.NewResponseController(c.Response())
	_ = rc.Flush()
	_, _ = c.Response().Write(plain)
}
