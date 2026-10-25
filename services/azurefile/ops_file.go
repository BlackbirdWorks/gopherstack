package azurefile

import (
	"bytes"
	"crypto/md5" //nolint:gosec // Content-MD5 is an integrity header, not a security control
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/sparse"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	maxFileSize   = int64(4) << 40
	maxRangeBytes = int64(maxBodyBytes)
)

func errInvalidRange() *apiError {
	return newErr(http.StatusRequestedRangeNotSatisfiable, "InvalidRange",
		"The range specified is invalid for the current size of the resource.")
}

func md5Of(b []byte) []byte {
	sum := md5.Sum(b) //nolint:gosec // see import

	return sum[:]
}

// parseRange parses "bytes=a-b" (or "bytes=a-" when open allows it).
func parseRange(v string, size int64, open bool) (int64, int64, bool) {
	spec, ok := strings.CutPrefix(strings.TrimSpace(v), "bytes=")
	if !ok {
		return 0, 0, false
	}

	a, b, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, false
	}

	start, err := strconv.ParseInt(a, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false
	}

	if b == "" {
		return start, size - 1, open
	}

	end, err := strconv.ParseInt(b, 10, 64)
	if err != nil || end < start {
		return 0, 0, false
	}

	return start, end, true
}

func (h *Handler) lookupFile(rc *reqCtx, write bool) (*share, *node, *apiError) {
	sh, aerr := h.Backend.peek(rc.acct).findShare(rc.share, rc.q.Get("sharesnapshot"), write)
	if aerr != nil {
		return nil, nil, aerr
	}

	n, aerr := sh.lookup(rc.path)
	if aerr != nil {
		return nil, nil, aerr
	}

	if n.Dir {
		return nil, nil, errTypeMismatch()
	}

	return sh, n, nil
}

func setFileHeaders(hdr http.Header, n *node, rc *reqCtx) {
	setNodeHeaders(hdr, n)

	ct := n.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}

	hdr.Set("Content-Type", ct)

	for k, v := range map[string]string{
		"Content-Encoding": n.Encoding, "Content-Language": n.Language,
		"Cache-Control": n.CacheControl, "Content-Disposition": n.Disposition,
	} {
		if v != "" {
			hdr.Set(k, v)
		}
	}

	if len(n.MD5) > 0 {
		hdr.Set("Content-MD5", base64.StdEncoding.EncodeToString(n.MD5))
	}

	hdr.Set("X-Ms-Type", "File")
	hdr.Set("X-Ms-Server-Encrypted", "true")
	n.Lease.setHeaders(hdr, rc.now)
	setMetadataHeaders(hdr, n.Metadata)

	if c := n.Copy; c != nil {
		hdr.Set("X-Ms-Copy-Id", c.ID)
		hdr.Set("X-Ms-Copy-Status", c.Status)
		hdr.Set("X-Ms-Copy-Source", c.Source)
		hdr.Set("X-Ms-Copy-Progress", fmt.Sprintf("%d/%d", n.Size, n.Size))
		hdr.Set("X-Ms-Copy-Completion-Time", fmtHTTP(c.Completed))
	}
}

// contentProps are the x-ms-content-* style headers of create/set-properties.
type contentProps struct {
	Type, Encoding, Language, CacheControl, Disposition string
	MD5                                                 []byte
	Present                                             bool
}

func parseContentProps(rc *reqCtx) (contentProps, *apiError) {
	c := contentProps{
		Type: rc.hdr("x-ms-content-type"), Encoding: rc.hdr("x-ms-content-encoding"),
		Language: rc.hdr("x-ms-content-language"), CacheControl: rc.hdr("x-ms-cache-control"),
		Disposition: rc.hdr("x-ms-content-disposition"),
	}

	if v := rc.hdr("x-ms-content-md5"); v != "" {
		sum, err := base64.StdEncoding.DecodeString(v)
		if err != nil || len(sum) != md5.Size {
			return c, errInvalidHeader("x-ms-content-md5")
		}

		c.MD5 = sum
	}

	c.Present = c.Type != "" || c.Encoding != "" || c.Language != "" || c.CacheControl != "" ||
		c.Disposition != "" || c.MD5 != nil

	return c, nil
}

func (c contentProps) applyTo(n *node) {
	n.ContentType, n.Encoding, n.Language = c.Type, c.Encoding, c.Language
	n.CacheControl, n.Disposition, n.MD5 = c.CacheControl, c.Disposition, c.MD5
}

// createFileInput is the validated header set of Create File.
type createFileInput struct {
	md   map[string]string
	cp   contentProps
	size int64
}

func parseCreateFile(rc *reqCtx) (createFileInput, *apiError) {
	var in createFileInput

	raw := rc.hdr("x-ms-content-length")
	if raw == "" {
		return in, errMissingHeader("x-ms-content-length")
	}

	size, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || size < 0 || size > maxFileSize {
		return in, errInvalidHeader("x-ms-content-length")
	}

	if t := rc.hdr("x-ms-type"); t != "" && !strings.EqualFold(t, "file") {
		return in, errInvalidHeader("x-ms-type")
	}

	in.size = size

	var aerr *apiError

	if in.md, aerr = metadataFromHeaders(rc.r.Header); aerr != nil {
		return in, aerr
	}

	in.cp, aerr = parseContentProps(rc)

	return in, aerr
}

func (h *Handler) createFile(rc *reqCtx) (*resp, *apiError) {
	in, aerr := parseCreateFile(rc)
	if aerr != nil {
		return nil, aerr
	}

	b := h.Backend
	b.mu.Lock("CreateFile")
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
		if old.Dir {
			return nil, errTypeMismatch()
		}

		if aerr = old.Lease.check(rc.hdr("x-ms-lease-id"), rc.now, true); aerr != nil {
			return nil, aerr
		}

		if strings.Contains(old.Attributes, attrReadOnly) {
			return nil, readOnlyErr()
		}

		n.ID, n.Lease = old.ID, old.Lease
	}

	n.Size, n.Metadata = in.size, in.md
	in.cp.applyTo(n)

	if aerr = sh.applySMB(n, smbFromHeaders(rc), true, rc.now); aerr != nil {
		return nil, aerr
	}

	parent.Children[strs.Fold(leaf)] = n

	return nodeResp(http.StatusCreated, n), nil
}

func (h *Handler) getFileProps(rc *reqCtx) (*resp, *apiError) {
	h.Backend.mu.RLock("GetFileProperties")
	defer h.Backend.mu.RUnlock()

	_, n, aerr := h.lookupFile(rc, false)
	if aerr != nil {
		return nil, aerr
	}

	if id := rc.hdr("x-ms-lease-id"); id != "" {
		if aerr = n.Lease.check(id, rc.now, true); aerr != nil {
			return nil, aerr
		}
	}

	rs := newResp(http.StatusOK)
	setFileHeaders(rs.hdr, n, rc)
	rs.hdr.Set("Content-Length", strconv.FormatInt(n.Size, 10))

	return rs, nil
}

func (h *Handler) getFile(rc *reqCtx) (*resp, *apiError) {
	h.Backend.mu.RLock("GetFile")
	defer h.Backend.mu.RUnlock()

	_, n, aerr := h.lookupFile(rc, false)
	if aerr != nil {
		return nil, aerr
	}

	if id := rc.hdr("x-ms-lease-id"); id != "" {
		if aerr = n.Lease.check(id, rc.now, true); aerr != nil {
			return nil, aerr
		}
	}

	rs := newResp(http.StatusOK)
	setFileHeaders(rs.hdr, n, rc)
	applyResponseOverrides(rs.hdr, rc)
	rs.hdr.Set("Accept-Ranges", "bytes")

	raw := rc.hdr("x-ms-range")
	if raw == "" {
		raw = rc.hdr("Range")
	}

	start, end := int64(0), n.Size-1
	partial := false

	if raw != "" {
		var ok bool

		if start, end, ok = parseRange(raw, n.Size, true); !ok {
			return nil, errInvalidRange()
		}

		if start >= n.Size {
			return nil, errInvalidRange()
		}

		end = min(end, n.Size-1)
		partial = true
	}

	length := max(end-start+1, 0)
	if length <= maxRangeBytes {
		rs.body = n.Data.Read(start, length)
	} else {
		rs.stream = sparse.NewReader(n.Data, start, end)
		rs.hdr.Set("Content-Length", strconv.FormatInt(length, 10))
	}

	if partial {
		rs.status = http.StatusPartialContent
		rs.hdr.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, n.Size))
		rs.hdr.Del("Content-MD5")
	}

	if strings.EqualFold(rc.hdr("x-ms-range-get-content-md5"), "true") {
		if !partial || int64(len(rs.body)) > maxRangeBytes {
			return nil, errInvalidHeader("x-ms-range-get-content-md5")
		}

		rs.hdr.Set("Content-MD5", base64.StdEncoding.EncodeToString(md5Of(rs.body)))
	}

	rs.ctype = rs.hdr.Get("Content-Type")

	return rs, nil
}

// applyResponseOverrides honours the SAS rsc* response-header overrides.
func applyResponseOverrides(hdr http.Header, rc *reqCtx) {
	for q, name := range map[string]string{
		"rscc": "Cache-Control", "rscd": "Content-Disposition", "rsce": "Content-Encoding",
		"rscl": "Content-Language", "rsct": "Content-Type",
	} {
		if v := rc.q.Get(q); v != "" {
			hdr.Set(name, v)
		}
	}
}

func (h *Handler) deleteFile(rc *reqCtx) (*resp, *apiError) {
	b := h.Backend
	b.mu.Lock("DeleteFile")
	defer b.mu.Unlock()

	sh, n, aerr := h.lookupFile(rc, true)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = n.Lease.check(rc.hdr("x-ms-lease-id"), rc.now, true); aerr != nil {
		return nil, aerr
	}

	if strings.Contains(n.Attributes, attrReadOnly) {
		return nil, readOnlyErr()
	}

	parent, leaf, aerr := sh.parentOf(rc.path)
	if aerr != nil {
		return nil, aerr
	}

	delete(parent.Children, strs.Fold(leaf))

	return newResp(http.StatusAccepted), nil
}

func (h *Handler) setFileProps(rc *reqCtx) (*resp, *apiError) {
	cp, aerr := parseContentProps(rc)
	if aerr != nil {
		return nil, aerr
	}

	var newSize *int64

	if v := rc.hdr("x-ms-content-length"); v != "" {
		s, err := strconv.ParseInt(v, 10, 64)
		if err != nil || s < 0 || s > maxFileSize {
			return nil, errInvalidHeader("x-ms-content-length")
		}

		newSize = &s
	}

	b := h.Backend
	b.mu.Lock("SetFileProperties")
	defer b.mu.Unlock()

	sh, n, aerr := h.lookupFile(rc, true)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = n.Lease.check(rc.hdr("x-ms-lease-id"), rc.now, true); aerr != nil {
		return nil, aerr
	}

	if cp.Present {
		cp.applyTo(n)
	}

	if newSize != nil && *newSize != n.Size {
		n.Data = n.Data.Truncate(*newSize)
		n.Size = *newSize
	}

	if aerr = sh.applySMB(n, smbFromHeaders(rc), false, rc.now); aerr != nil {
		return nil, aerr
	}

	n.ETag, n.LastModified = b.etag(), rc.now

	return nodeResp(http.StatusOK, n), nil
}

func (h *Handler) setFileMetadata(rc *reqCtx) (*resp, *apiError) {
	md, aerr := metadataFromHeaders(rc.r.Header)
	if aerr != nil {
		return nil, aerr
	}

	b := h.Backend
	b.mu.Lock("SetFileMetadata")
	defer b.mu.Unlock()

	_, n, aerr := h.lookupFile(rc, true)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = n.Lease.check(rc.hdr("x-ms-lease-id"), rc.now, true); aerr != nil {
		return nil, aerr
	}

	n.Metadata = md
	n.ETag, n.LastModified = b.etag(), rc.now

	rs := newResp(http.StatusOK)
	rs.hdr.Set("ETag", n.ETag)
	rs.hdr.Set("X-Ms-Request-Server-Encrypted", "true")

	return rs, nil
}

func (h *Handler) putRange(rc *reqCtx) (*resp, *apiError) {
	raw := rc.hdr("x-ms-range")
	if raw == "" {
		return nil, errMissingHeader("x-ms-range")
	}

	start, end, ok := parseRange(raw, 0, false)
	if !ok {
		return nil, errInvalidHeader("x-ms-range")
	}

	clearOp := false

	switch strs.Fold(rc.hdr(headerWrite)) {
	case "update":
	case "clear":
		clearOp = true
	default:
		return nil, errInvalidHeader(headerWrite)
	}

	length := end - start + 1
	if length > maxRangeBytes {
		return nil, newErr(http.StatusRequestEntityTooLarge, "RequestBodyTooLarge",
			"The size of the request body exceeds the maximum permissible limit.")
	}

	var data []byte

	if !clearOp {
		var aerr *apiError
		if data, aerr = readRangeBody(rc, length); aerr != nil {
			return nil, aerr
		}
	}

	return h.writeRange(rc, start, length, data, clearOp)
}

// readRangeBody reads a Put Range payload and checks its length and Content-MD5.
func readRangeBody(rc *reqCtx, length int64) ([]byte, *apiError) {
	data, aerr := rc.body()
	if aerr != nil {
		return nil, aerr
	}

	if int64(len(data)) != length {
		return nil, errInvalidHeader("Content-Length")
	}

	if v := rc.hdr("Content-MD5"); v != "" {
		sum, err := base64.StdEncoding.DecodeString(v)
		if err != nil || !bytes.Equal(sum, md5Of(data)) {
			return nil, newErr(http.StatusBadRequest, "Md5Mismatch",
				"The MD5 value specified in the request did not match with the MD5 value calculated by the server.")
		}
	}

	return data, nil
}

func (h *Handler) writeRange(rc *reqCtx, start, length int64, data []byte, clearOp bool) (*resp, *apiError) {
	b := h.Backend
	b.mu.Lock("PutRange")
	defer b.mu.Unlock()

	_, n, aerr := h.lookupFile(rc, true)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = n.Lease.check(rc.hdr("x-ms-lease-id"), rc.now, true); aerr != nil {
		return nil, aerr
	}

	if start+length > n.Size {
		return nil, errInvalidRange()
	}

	if clearOp {
		n.Data = n.Data.Clear(start, length)
	} else {
		n.Data = n.Data.Write(start, data)
	}

	n.MD5 = nil

	if !strs.Equal(rc.hdr("x-ms-file-last-write-time"), "preserve") {
		n.LastWrite = rc.now
	}

	n.ETag, n.LastModified = b.etag(), rc.now

	rs := newResp(http.StatusCreated)
	rs.hdr.Set("ETag", n.ETag)
	rs.hdr.Set("Last-Modified", fmtHTTP(n.LastModified))
	rs.hdr.Set("X-Ms-Request-Server-Encrypted", "true")
	rs.hdr.Set("X-Ms-File-Last-Write-Time", fmtISO(n.LastWrite))

	if !clearOp {
		rs.hdr.Set("Content-MD5", base64.StdEncoding.EncodeToString(md5Of(data)))
	}

	return rs, nil
}

type rangeXML struct {
	Start int64 `xml:"Start"`
	End   int64 `xml:"End"`
}

type rangesXML struct {
	XMLName xml.Name   `xml:"Ranges"`
	Ranges  []rangeXML `xml:"Range"`
	Clear   []rangeXML `xml:"ClearRange"`
}

func toRangeXML(rs []sparse.Range) []rangeXML {
	out := make([]rangeXML, 0, len(rs))
	for _, r := range rs {
		out = append(out, rangeXML(r))
	}

	return out
}

func (h *Handler) listRanges(rc *reqCtx) (*resp, *apiError) {
	h.Backend.mu.RLock("ListRanges")
	defer h.Backend.mu.RUnlock()

	_, n, aerr := h.lookupFile(rc, false)
	if aerr != nil {
		return nil, aerr
	}

	if id := rc.hdr("x-ms-lease-id"); id != "" {
		if aerr = n.Lease.check(id, rc.now, true); aerr != nil {
			return nil, aerr
		}
	}

	from, to := int64(0), n.Size-1

	if raw := rc.hdr("x-ms-range"); raw != "" {
		var ok bool
		if from, to, ok = parseRange(raw, n.Size, true); !ok {
			return nil, errInvalidRange()
		}

		to = min(to, n.Size-1)
	}

	out := rangesXML{}

	if prev := rc.q.Get("prevsharesnapshot"); prev != "" {
		ps, perr0 := h.Backend.peek(rc.acct).findShare(rc.share, prev, false)
		if perr0 != nil {
			return nil, perr0
		}

		pn, perr := ps.lookup(rc.path)
		if perr != nil || pn.Dir {
			out.Ranges = toRangeXML(n.Data.Ranges(from, to))
		} else {
			changed, cleared := n.Data.Diff(pn.Data, max(pn.Size, n.Size))
			out.Ranges = toRangeXML(clipRanges(changed, from, to))
			out.Clear = toRangeXML(clipRanges(cleared, from, to))
		}
	} else {
		out.Ranges = toRangeXML(n.Data.Ranges(from, to))
	}

	rs := newResp(http.StatusOK).xml(out)
	rs.hdr.Set("ETag", n.ETag)
	rs.hdr.Set("Last-Modified", fmtHTTP(n.LastModified))
	rs.hdr.Set("X-Ms-Content-Length", strconv.FormatInt(n.Size, 10))

	return rs, nil
}

func clipRanges(in []sparse.Range, from, to int64) []sparse.Range {
	var out []sparse.Range

	for _, r := range in {
		lo, hi := max(r.Start, from), min(r.End, to)
		if lo <= hi {
			out = append(out, sparse.Range{Start: lo, End: hi})
		}
	}

	return out
}

func (h *Handler) leaseFile(rc *reqCtx) (*resp, *apiError) {
	req, aerr := parseLeaseRequest(rc)
	if aerr != nil {
		return nil, aerr
	}

	h.Backend.mu.Lock("LeaseFile")
	defer h.Backend.mu.Unlock()

	_, n, aerr := h.lookupFile(rc, true)
	if aerr != nil {
		return nil, aerr
	}

	res, aerr := n.Lease.apply(req, rc.now, true)
	if aerr != nil {
		return nil, aerr
	}

	return leaseResp(res, n.ETag, n.LastModified), nil
}
