package gcpbigquery

import (
	"bytes"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	uploadMultipart = "multipart"
	uploadResumable = "resumable"
	maxSessions     = 256
)

type uploadSession struct {
	body    map[string]any
	project string
	data    []byte
}

type uploadSessions struct {
	mu   *lockmetrics.RWMutex
	byID map[string]*uploadSession
}

func newUploadSessions() *uploadSessions {
	return &uploadSessions{mu: lockmetrics.New("gcpbigquery.uploads"), byID: map[string]*uploadSession{}}
}

func (u *uploadSessions) reset() {
	u.mu.Lock("reset")
	defer u.mu.Unlock()

	u.byID = map[string]*uploadSession{}
}

func (u *uploadSessions) create(project string, body map[string]any) (string, error) {
	u.mu.Lock("create")
	defer u.mu.Unlock()

	if len(u.byID) >= maxSessions {
		return "", newErr(http.StatusTooManyRequests, "rateLimitExceeded", "Too many open upload sessions")
	}

	id := awsid.NewN("up_", 32) //nolint:mnd // session id length
	u.byID[id] = &uploadSession{project: project, body: body}

	return id, nil
}

func (u *uploadSessions) get(id string) (*uploadSession, bool) {
	u.mu.RLock("get")
	defer u.mu.RUnlock()

	s, ok := u.byID[id]

	return s, ok
}

func (u *uploadSessions) drop(id string) {
	u.mu.Lock("drop")
	defer u.mu.Unlock()

	delete(u.byID, id)
}

// uploadJob serves jobs.insert with media: multipart/related, or a resumable session.
func (h *Handler) uploadJob(c *echo.Context, rt route) error {
	r := c.Request()

	switch {
	case r.Method == http.MethodPut || c.QueryParam("upload_id") != "":
		return h.resumeUpload(c)
	case c.QueryParam("uploadType") == uploadResumable:
		return h.startUpload(c, rt)
	case c.QueryParam("uploadType") == uploadMultipart:
		return h.multipartUpload(c, rt)
	}

	return errInvalid("uploadType must be multipart or resumable")
}

func (h *Handler) multipartUpload(c *echo.Context, rt route) error {
	r := c.Request()

	mt, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(mt, "multipart/") {
		return errInvalid("Multipart upload requires a multipart/related Content-Type")
	}

	mr := multipart.NewReader(http.MaxBytesReader(c.Response(), r.Body, maxBodyBytes), params["boundary"])

	meta, err := mr.NextPart()
	if err != nil {
		return errInvalid("Multipart upload is missing the job metadata part")
	}

	metaBytes, err := io.ReadAll(meta)
	if err != nil {
		return errInvalid("Cannot read the job metadata part")
	}

	body, err := decodeObject(metaBytes)
	if err != nil {
		return err
	}

	media, err := mr.NextPart()
	if err != nil {
		return errInvalid("Multipart upload is missing the media part")
	}

	data, err := io.ReadAll(media)
	if err != nil {
		return errInvalid("Cannot read the media part")
	}

	out, err := h.Backend.InsertJob(r.Context(), rt.project, body, nonNil(data))
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, out)
}

func nonNil(b []byte) []byte {
	if b == nil {
		return []byte{}
	}

	return b
}

func (h *Handler) startUpload(c *echo.Context, rt route) error {
	body, err := decodeBody(c)
	if err != nil {
		return err
	}

	id, err := h.uploads.create(rt.project, body)
	if err != nil {
		return err
	}

	r := c.Request()
	scheme := "http"

	if r.TLS != nil {
		scheme = "https"
	}

	q := url.Values{"uploadType": {uploadResumable}, "upload_id": {id}}
	loc := scheme + "://" + r.Host + r.URL.Path + "?" + q.Encode()
	c.Response().Header().Set("Location", loc)

	return c.NoContent(http.StatusOK)
}

type byteRange struct {
	first int64
	total int64
	ok    bool
}

// contentRange parses "bytes first-last/total"; first is -1 for a status query and total is -1 when unknown.
func contentRange(h string) byteRange {
	rest, found := strings.CutPrefix(strings.TrimSpace(h), "bytes ")
	if !found {
		return byteRange{}
	}

	span, tot, found := strings.Cut(rest, "/")
	if !found {
		return byteRange{}
	}

	r := byteRange{total: -1, ok: true}

	if tot != "*" {
		n, err := strconv.ParseInt(tot, 10, 64)
		if err != nil {
			return byteRange{}
		}

		r.total = n
	}

	if span == "*" {
		r.first = -1

		return r
	}

	a, _, found := strings.Cut(span, "-")
	if !found {
		return byteRange{}
	}

	first, err := strconv.ParseInt(a, 10, 64)
	if err != nil {
		return byteRange{}
	}

	r.first = first

	return r
}

func (h *Handler) resumeUpload(c *echo.Context) error {
	r := c.Request()
	id := c.QueryParam("upload_id")

	sess, ok := h.uploads.get(id)
	if !ok {
		return errNotFound("Upload session %s was not found", id)
	}

	data, err := io.ReadAll(http.MaxBytesReader(c.Response(), r.Body, maxBodyBytes))
	if err != nil {
		return newErr(http.StatusRequestEntityTooLarge, "uploadTooLarge", "Request body is too large or unreadable.")
	}

	br := contentRange(r.Header.Get("Content-Range"))
	total := br.total

	if br.ok && br.first >= 0 {
		if br.first != int64(len(sess.data)) {
			return errInvalid("Unexpected upload offset %d; the session holds %d bytes", br.first, len(sess.data))
		}

		sess.data = append(sess.data, data...)
	} else if !br.ok || len(data) > 0 {
		sess.data = append(sess.data, data...)
		total = int64(len(sess.data))
	}

	if br.ok && (total < 0 || int64(len(sess.data)) < total) {
		return resumeIncomplete(c, len(sess.data))
	}

	h.uploads.drop(id)

	out, err := h.Backend.InsertJob(r.Context(), sess.project, sess.body, nonNil(bytes.Clone(sess.data)))
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, out)
}

func resumeIncomplete(c *echo.Context, have int) error {
	if have > 0 {
		c.Response().Header().Set("Range", "bytes=0-"+strconv.Itoa(have-1))
	}

	return c.NoContent(http.StatusPermanentRedirect)
}
