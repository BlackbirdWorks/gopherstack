package connect

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/ctxval"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// ObjectReader reads S3 objects; resolvers that implement it let CreateAttachedFile copy from S3.
type ObjectReader interface {
	ReadObject(ctx context.Context, bucket, key string) ([]byte, error)
}

var baseURLKey = ctxval.NewKey[string]("connect-base-url") //nolint:gochecknoglobals // context key

func requestBaseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}

	if p := r.Header.Get("X-Forwarded-Proto"); p == "http" || p == "https" {
		scheme = p
	}

	return scheme + "://" + r.Host
}

func (b *Backend) baseURL(ctx context.Context) string {
	if u, ok := baseURLKey.Get(ctx); ok && u != "" {
		return u
	}

	_, region := b.scope(ctx)

	return "https://connect." + region + ".amazonaws.com"
}

func (b *Backend) sign(op, inst, fileID string, expires int64) string {
	m := hmac.New(sha256.New, b.urlSecret)
	_, _ = fmt.Fprintf(m, "%s|%s|%s|%d", op, inst, fileID, expires)

	return hex.EncodeToString(m.Sum(nil))
}

// contentURL builds a pre-signed URL of the emulator's attachment endpoint.
func (b *Backend) contentURL(ctx context.Context, op, inst, fileID string, ttl time.Duration) *URLMetadata {
	expires := b.now().Add(ttl)
	q := url.Values{
		"op": {op}, "expires": {strconv.FormatInt(expires.Unix(), 10)},
		"signature": {b.sign(op, inst, fileID, expires.Unix())},
	}

	return &URLMetadata{
		URL: b.baseURL(
			ctx,
		) + attachedContentPath + url.PathEscape(
			inst,
		) + "/" + url.PathEscape(
			fileID,
		) + "?" + q.Encode(),
		URLExpiry:        isoTime(expires),
		HeadersToInclude: map[string]string{},
	}
}

func forbiddenErr(format string, args ...any) error {
	return apiErr(http.StatusForbidden, "AccessDenied", format, args...)
}

func (b *Backend) checkContentURL(op string, q url.Values, inst, fileID string) error {
	exp, err := strconv.ParseInt(q.Get("expires"), 10, 64)
	if err != nil || q.Get("op") != op {
		return forbiddenErr("the pre-signed URL is malformed")
	}

	if !hmac.Equal([]byte(q.Get("signature")), []byte(b.sign(op, inst, fileID, exp))) {
		return forbiddenErr("the pre-signed URL signature does not match")
	}

	if !b.now().Before(time.Unix(exp, 0)) {
		return forbiddenErr("the pre-signed URL has expired")
	}

	return nil
}

// PutAttachedContent stores the uploaded content of a file that is still processing.
func (b *Backend) PutAttachedContent(inst, fileID string, q url.Values, body io.Reader) error {
	if err := b.checkContentURL("put", q, inst, fileID); err != nil {
		return err
	}

	b.mu.RLock("PutAttachedContent")
	r, err := b.attachedFiles.get(inst, fileID)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	if r.Status != fileStatusProcessing {
		return apiErr(http.StatusConflict, "InvalidRequest", "file %q no longer accepts uploads", fileID)
	}

	data, err := io.ReadAll(io.LimitReader(body, r.Size+1))
	if err != nil {
		return apiErr(http.StatusBadRequest, "IncompleteBody", "the upload could not be read")
	}

	b.mu.Lock("PutAttachedContent")
	defer b.mu.Unlock()

	r, err = b.attachedFiles.get(inst, fileID)
	if err != nil {
		return err
	}

	r.Content, r.Uploaded = data, true

	return nil
}

// GetAttachedContent returns the content and name of an approved file.
func (b *Backend) GetAttachedContent(inst, fileID string, q url.Values) ([]byte, string, error) {
	if err := b.checkContentURL("get", q, inst, fileID); err != nil {
		return nil, "", err
	}

	b.mu.RLock("GetAttachedContent")
	defer b.mu.RUnlock()

	r, err := b.attachedFiles.get(inst, fileID)
	if err != nil {
		return nil, "", err
	}

	if r.Status != fileStatusApproved {
		return nil, "", forbiddenErr("file %q is not approved", fileID)
	}

	return append([]byte(nil), r.Content...), r.Name, nil
}

// serveAttachedContent handles the pre-signed upload (PUT) and download (GET) URLs.
func (h *Handler) serveAttachedContent(c *echo.Context) error {
	r, w := c.Request(), c.Response()
	inst, fileID, ok := strings.Cut(strings.TrimPrefix(restroute.EscapedPath(r), attachedContentPath), "/")

	var err error

	if inst, err = url.PathUnescape(inst); err == nil && ok {
		fileID, err = url.PathUnescape(fileID)
	}

	if err != nil || !ok {
		return c.NoContent(http.StatusNotFound)
	}

	switch r.Method {
	case http.MethodPut:
		if err = h.Backend.PutAttachedContent(inst, fileID, r.URL.Query(), r.Body); err != nil {
			return contentError(c, err)
		}

		return c.NoContent(http.StatusOK)
	case http.MethodGet:
		if r.URL.Query().Get("op") == promptOp {
			data, perr := h.Backend.GetPromptContent(r.Context(), inst, fileID, r.URL.Query())
			if perr != nil {
				return contentError(c, perr)
			}

			return c.Blob(http.StatusOK, "audio/wav", data)
		}

		data, name, gerr := h.Backend.GetAttachedContent(inst, fileID, r.URL.Query())
		if gerr != nil {
			return contentError(c, gerr)
		}

		w.Header().Set("Content-Disposition", "attachment; filename="+strconv.Quote(name))

		return c.Blob(http.StatusOK, "application/octet-stream", data)
	default:
		return c.NoContent(http.StatusMethodNotAllowed)
	}
}

func contentError(c *echo.Context, err error) error {
	if re, ok := errors.AsType[*restroute.Error](err); ok {
		return c.String(re.Status, re.Code+": "+re.Message)
	}

	return c.String(http.StatusInternalServerError, err.Error())
}

// Handler returns the Echo handler.
func (h *Handler) Handler() echo.HandlerFunc {
	serve := h.server.Handler()

	return func(c *echo.Context) error {
		r := c.Request()
		if strings.HasPrefix(restroute.EscapedPath(r), attachedContentPath) {
			return h.serveAttachedContent(c)
		}

		c.SetRequest(r.WithContext(baseURLKey.Set(r.Context(), requestBaseURL(r))))

		return serve(c)
	}
}
