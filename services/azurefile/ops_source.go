package azurefile

import (
	"errors"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/sparse"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

var errBadSourceURL = errors.New("azurefile: bad source url")

func parseSourceURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errBadSourceURL
	}

	return u, nil
}

func errCannotVerifySource() *apiError {
	return newErr(http.StatusForbidden, "CannotVerifyCopySource",
		"Server failed to authenticate the copy source or the source is not served by this emulator.")
}

// sourceData is a resolved copy/range source. For file sources File holds a
// clone sharing the immutable extents; for blobs Blob holds the bytes.
type sourceData struct {
	File *node
	SDDL string
	Type string
	Blob []byte
}

func (s *sourceData) size() int64 {
	if s.File != nil {
		return s.File.Size
	}

	return int64(len(s.Blob))
}

func (s *sourceData) read(start, n int64) []byte {
	if s.File != nil {
		return s.File.Data.Read(start, n)
	}

	return s.Blob[start : start+n]
}

func port(hostport string) string {
	_, p, err := net.SplitHostPort(hostport)
	if err != nil {
		return ""
	}

	return p
}

// sourceKind classifies a source URL as served by this listener ("file"),
// the emulator's blob listener ("blob"), or neither.
func (h *Handler) sourceKind(rc *reqCtx, u *url.URL) string {
	labels := strings.Split(u.Hostname(), ".")
	if len(labels) >= minHostLabels {
		switch strings.ToLower(labels[1]) {
		case kindFile:
			return kindFile
		case kindBlob:
			return kindBlob
		}
	}

	switch p := port(u.Host); {
	case p == "":
	case p == port(rc.r.Host):
		return kindFile
	case h.BlobPort != 0 && p == strconv.Itoa(h.BlobPort):
		return kindBlob
	}

	return ""
}

func sasExpired(u *url.URL, now time.Time) bool {
	se := u.Query().Get("se")
	if se == "" {
		return false
	}

	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04Z", "2006-01-02"} {
		if t, err := time.Parse(layout, se); err == nil {
			return !now.Before(t)
		}
	}

	return false
}

func (h *Handler) readSource(rc *reqCtx, raw string) (*sourceData, *apiError) {
	u, err := parseSourceURL(raw)
	if err != nil {
		return nil, errInvalidHeader(headerCopySource)
	}

	if sasExpired(u, rc.now) {
		return nil, errCannotVerifySource()
	}

	switch h.sourceKind(rc, u) {
	case kindFile:
		return h.readFileSource(u)
	case kindBlob:
		return h.readBlobSource(u)
	}

	return nil, errCannotVerifySource()
}

func (h *Handler) readFileSource(u *url.URL) (*sourceData, *apiError) {
	acct, shr, path, _, ok := splitHostPath(u.Host, u.Path)
	if !ok {
		return nil, errInvalidHeader(headerCopySource)
	}

	h.Backend.mu.RLock("ReadSource")
	defer h.Backend.mu.RUnlock()

	sh, aerr := h.Backend.peek(acct).findShare(shr, u.Query().Get("sharesnapshot"), false)
	if aerr != nil {
		return nil, notFoundSource(aerr)
	}

	n, aerr := sh.lookup(path)
	if aerr != nil {
		return nil, notFoundSource(aerr)
	}

	if n.Dir {
		return nil, errTypeMismatch()
	}

	return &sourceData{File: n.clone(), SDDL: sh.Permissions[n.PermKey], Type: n.ContentType}, nil
}

func notFoundSource(e *apiError) *apiError {
	if e.Status == http.StatusNotFound {
		return newErr(http.StatusNotFound, "CannotVerifyCopySource", "The copy source does not exist.")
	}

	return e
}

func (h *Handler) readBlobSource(u *url.URL) (*sourceData, *apiError) {
	if h.Blobs == nil {
		return nil, errCannotVerifySource()
	}

	_, rest, _ := strings.Cut(strings.TrimPrefix(u.Path, "/"), "/")
	if labels := strings.Split(
		u.Hostname(),
		".",
	); len(labels) >= minHostLabels &&
		strings.EqualFold(labels[1], kindBlob) {
		rest = strings.TrimPrefix(u.Path, "/")
	}

	container, blob, _ := strings.Cut(rest, "/")

	data, ctype, err := h.Blobs.ReadBlob(container, blob, u.Query().Get("snapshot"))
	if err != nil {
		return nil, newErr(http.StatusNotFound, "CannotVerifyCopySource", "The copy source does not exist.")
	}

	return &sourceData{Blob: data, Type: ctype}, nil
}

func (h *Handler) putRangeFromURL(rc *reqCtx) (*resp, *apiError) {
	start, end, ok := parseRange(rc.hdr("x-ms-range"), 0, false)
	if !ok {
		return nil, errInvalidHeader("x-ms-range")
	}

	length := end - start + 1
	if length > maxRangeBytes {
		return nil, newErr(http.StatusRequestEntityTooLarge, "RequestBodyTooLarge",
			"The size of the request body exceeds the maximum permissible limit.")
	}

	src, aerr := h.readSource(rc, rc.hdr(headerCopySource))
	if aerr != nil {
		return nil, aerr
	}

	srcStart := int64(0)

	if raw := rc.hdr("x-ms-source-range"); raw != "" {
		var srcEnd int64

		if srcStart, srcEnd, ok = parseRange(raw, src.size(), false); !ok || srcEnd-srcStart+1 != length {
			return nil, errInvalidHeader("x-ms-source-range")
		}
	}

	if srcStart+length > src.size() {
		return nil, errInvalidRange()
	}

	return h.writeRange(rc, start, length, src.read(srcStart, length), false)
}

func (h *Handler) copyFile(rc *reqCtx) (*resp, *apiError) {
	rawSrc := rc.hdr(headerCopySource)

	src, aerr := h.readSource(rc, rawSrc)
	if aerr != nil {
		return nil, aerr
	}

	md, aerr := metadataFromHeaders(rc.r.Header)
	if aerr != nil {
		return nil, aerr
	}

	b := h.Backend
	b.mu.Lock("CopyFile")
	defer b.mu.Unlock()

	sh, aerr := b.peek(rc.acct).findShare(rc.share, rc.q.Get("sharesnapshot"), true)
	if aerr != nil {
		return nil, aerr
	}

	parent, leaf, aerr := sh.parentOf(rc.path)
	if aerr != nil {
		return nil, aerr
	}

	n := b.newNode(parent, leaf, false, rc.now)

	if old := parent.child(leaf); old != nil {
		if aerr = checkCopyTarget(rc, old); aerr != nil {
			return nil, aerr
		}

		n.ID, n.Lease = old.ID, old.Lease
	}

	fillFromSource(n, src, md)

	if aerr = sh.applySMB(n, copySMB(rc, src), true, rc.now); aerr != nil {
		return nil, aerr
	}

	u, _ := parseSourceURL(rawSrc)
	u.RawQuery = ""
	n.Copy = &copyInfo{ID: uuid.NewString(), Source: u.String(), Status: "success", Completed: rc.now}
	parent.Children[strs.Fold(leaf)] = n

	rs := newResp(http.StatusAccepted)
	rs.hdr.Set("ETag", n.ETag)
	rs.hdr.Set("Last-Modified", fmtHTTP(n.LastModified))
	rs.hdr.Set("X-Ms-Copy-Id", n.Copy.ID)
	rs.hdr.Set("X-Ms-Copy-Status", n.Copy.Status)

	return rs, nil
}

func checkCopyTarget(rc *reqCtx, old *node) *apiError {
	if old.Dir {
		return errTypeMismatch()
	}

	if aerr := old.Lease.check(rc.hdr("x-ms-lease-id"), rc.now, true); aerr != nil {
		return aerr
	}

	if strings.Contains(old.Attributes, attrReadOnly) &&
		!strings.EqualFold(rc.hdr("x-ms-file-copy-ignore-readonly"), "true") {
		return readOnlyErr()
	}

	return nil
}

func fillFromSource(n *node, src *sourceData, md map[string]string) {
	n.Size = src.size()

	if src.File != nil {
		f := src.File
		n.Data, n.MD5 = f.Data.Clone(), f.MD5
		n.ContentType, n.Encoding, n.Language = f.ContentType, f.Encoding, f.Language
		n.CacheControl, n.Disposition = f.CacheControl, f.Disposition
		n.Metadata = f.Metadata
	} else {
		n.Data = sparse.Extents(nil).Write(0, src.Blob)
		n.ContentType = src.Type
		n.MD5 = md5Of(src.Blob)
	}

	if md != nil {
		n.Metadata = md
	}
}

// copySMB resolves the "source" tokens of Copy File's SMB headers.
func copySMB(rc *reqCtx, src *sourceData) smbIn {
	in := smbFromHeaders(rc)
	f := src.File

	if f == nil {
		return in
	}

	if strs.Equal(in.Attributes, "source") {
		in.Attributes = strings.ReplaceAll(strings.ReplaceAll(f.Attributes, attrDirectory, ""), "||", "|")
	}

	if strs.Equal(in.Creation, "source") {
		in.Creation = fmtISO(f.Created)
	}

	if strs.Equal(in.LastWrite, "source") {
		in.LastWrite = fmtISO(f.LastWrite)
	}

	if strs.Equal(in.Change, "source") {
		in.Change = fmtISO(f.Changed)
	}

	if in.Permission == "" && in.PermKey == "" && src.SDDL != "" &&
		!strs.Equal(rc.hdr("x-ms-file-permission-copy-mode"), "override") {
		in.Permission = src.SDDL
	}

	return in
}

func (h *Handler) abortCopy(rc *reqCtx) (*resp, *apiError) {
	id := rc.q.Get("copyid")
	if id == "" {
		return nil, errMissingHeader("copyid")
	}

	h.Backend.mu.RLock("AbortCopy")
	defer h.Backend.mu.RUnlock()

	_, n, aerr := h.lookupFile(rc, true)
	if aerr != nil {
		return nil, aerr
	}

	if n.Copy == nil {
		return nil, newErr(
			http.StatusConflict,
			"NoPendingCopyOperation",
			"There is currently no pending copy operation.",
		)
	}

	if n.Copy.ID != id {
		return nil, newErr(http.StatusConflict, "CopyIdMismatch",
			"The specified copy ID did not match the copy ID for the destination file.")
	}

	return nil, newErr(http.StatusConflict, "NoPendingCopyOperation", "There is currently no pending copy operation.")
}
