package azurefile

import (
	"encoding/xml"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

func smbFromHeaders(rc *reqCtx) smbIn {
	return smbIn{
		Attributes: rc.hdr("x-ms-file-attributes"), Creation: rc.hdr("x-ms-file-creation-time"),
		LastWrite: rc.hdr("x-ms-file-last-write-time"), Change: rc.hdr("x-ms-file-change-time"),
		Permission: rc.hdr("x-ms-file-permission"), PermKey: rc.hdr("x-ms-file-permission-key"),
	}
}

// setNodeHeaders writes the ETag, Last-Modified and SMB property headers.
func setNodeHeaders(hdr http.Header, n *node) {
	hdr.Set("ETag", n.ETag)
	hdr.Set("Last-Modified", fmtHTTP(n.LastModified))
	hdr.Set("X-Ms-File-Permission-Key", n.PermKey)
	hdr.Set("X-Ms-File-Attributes", n.Attributes)
	hdr.Set("X-Ms-File-Creation-Time", fmtISO(n.Created))
	hdr.Set("X-Ms-File-Last-Write-Time", fmtISO(n.LastWrite))
	hdr.Set("X-Ms-File-Change-Time", fmtISO(n.Changed))
	hdr.Set("X-Ms-File-Id", n.ID)
	hdr.Set("X-Ms-File-Parent-Id", n.ParentID)
}

func nodeResp(status int, n *node) *resp {
	rs := newResp(status)
	setNodeHeaders(rs.hdr, n)
	rs.hdr.Set("X-Ms-Request-Server-Encrypted", "true")

	return rs
}

func (n *node) child(name string) *node { return n.Children[strs.Fold(name)] }

func (h *Handler) createDir(rc *reqCtx) (*resp, *apiError) {
	md, aerr := metadataFromHeaders(rc.r.Header)
	if aerr != nil {
		return nil, aerr
	}

	b := h.Backend
	b.mu.Lock("CreateDirectory")
	defer b.mu.Unlock()

	sh, aerr := b.peek(rc.acct).findShare(rc.share, rc.q.Get("sharesnapshot"), true)
	if aerr != nil {
		return nil, aerr
	}

	parent, leaf, aerr := sh.parentOf(rc.path)
	if aerr != nil {
		return nil, aerr
	}

	if parent.child(leaf) != nil {
		return nil, errAlreadyExists()
	}

	n := b.newNode(parent, leaf, true, rc.now)
	n.Metadata = md

	if aerr = sh.applySMB(n, smbFromHeaders(rc), true, rc.now); aerr != nil {
		return nil, aerr
	}

	parent.Children[strs.Fold(leaf)] = n

	return nodeResp(http.StatusCreated, n), nil
}

func (h *Handler) lookupDir(rc *reqCtx, write bool) (*share, *node, *apiError) {
	sh, aerr := h.Backend.peek(rc.acct).findShare(rc.share, rc.q.Get("sharesnapshot"), write)
	if aerr != nil {
		return nil, nil, aerr
	}

	n, aerr := sh.lookup(rc.path)
	if aerr != nil {
		return nil, nil, aerr
	}

	if !n.Dir {
		return nil, nil, errTypeMismatch()
	}

	return sh, n, nil
}

func (h *Handler) getDirProps(rc *reqCtx) (*resp, *apiError) {
	h.Backend.mu.RLock("GetDirectoryProperties")
	defer h.Backend.mu.RUnlock()

	_, n, aerr := h.lookupDir(rc, false)
	if aerr != nil {
		return nil, aerr
	}

	rs := newResp(http.StatusOK)
	setNodeHeaders(rs.hdr, n)
	setMetadataHeaders(rs.hdr, n.Metadata)
	rs.hdr.Set("X-Ms-Server-Encrypted", "true")

	return rs, nil
}

func (h *Handler) deleteDir(rc *reqCtx) (*resp, *apiError) {
	b := h.Backend
	b.mu.Lock("DeleteDirectory")
	defer b.mu.Unlock()

	sh, aerr := b.peek(rc.acct).findShare(rc.share, rc.q.Get("sharesnapshot"), true)
	if aerr != nil {
		return nil, aerr
	}

	parent, leaf, aerr := sh.parentOf(rc.path)
	if aerr != nil {
		return nil, newErr(http.StatusBadRequest, "InvalidInput", "The root directory cannot be deleted.")
	}

	n := parent.child(leaf)
	if n == nil {
		return nil, errResourceNotFound()
	}

	if !n.Dir {
		return nil, errTypeMismatch()
	}

	if len(n.Children) > 0 {
		return nil, newErr(http.StatusConflict, "DirectoryNotEmpty", "The specified directory is not empty.")
	}

	delete(parent.Children, strs.Fold(leaf))

	return newResp(http.StatusAccepted), nil
}

func (h *Handler) setDirProps(rc *reqCtx) (*resp, *apiError) {
	b := h.Backend
	b.mu.Lock("SetDirectoryProperties")
	defer b.mu.Unlock()

	sh, n, aerr := h.lookupDir(rc, true)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = sh.applySMB(n, smbFromHeaders(rc), false, rc.now); aerr != nil {
		return nil, aerr
	}

	n.ETag, n.LastModified = b.etag(), rc.now

	return nodeResp(http.StatusOK, n), nil
}

func (h *Handler) setDirMetadata(rc *reqCtx) (*resp, *apiError) {
	md, aerr := metadataFromHeaders(rc.r.Header)
	if aerr != nil {
		return nil, aerr
	}

	b := h.Backend
	b.mu.Lock("SetDirectoryMetadata")
	defer b.mu.Unlock()

	_, n, aerr := h.lookupDir(rc, true)
	if aerr != nil {
		return nil, aerr
	}

	n.Metadata = md
	n.ETag, n.LastModified = b.etag(), rc.now

	rs := newResp(http.StatusOK)
	rs.hdr.Set("ETag", n.ETag)
	rs.hdr.Set("X-Ms-Request-Server-Encrypted", "true")

	return rs, nil
}

type listPropsXML struct {
	ContentLength  *int64 `xml:"Content-Length,omitempty"`
	CreationTime   string `xml:"CreationTime,omitempty"`
	LastAccessTime string `xml:"LastAccessTime,omitempty"`
	LastWriteTime  string `xml:"LastWriteTime,omitempty"`
	ChangeTime     string `xml:"ChangeTime,omitempty"`
	LastModified   string `xml:"Last-Modified,omitempty"`
	ETag           string `xml:"Etag,omitempty"`
}

type listEntryXML struct {
	Properties    *listPropsXML `xml:"Properties,omitempty"`
	Name          string        `xml:"Name"`
	FileID        string        `xml:"FileId,omitempty"`
	Attributes    string        `xml:"Attributes,omitempty"`
	PermissionKey string        `xml:"PermissionKey,omitempty"`
}

type listFilesXML struct {
	MaxResults   *int     `xml:"MaxResults,omitempty"`
	XMLName      xml.Name `xml:"EnumerationResults"`
	Endpoint     string   `xml:"ServiceEndpoint,attr"`
	ShareName    string   `xml:"ShareName,attr"`
	Snapshot     string   `xml:"ShareSnapshot,attr,omitempty"`
	DirectoryPth string   `xml:"DirectoryPath,attr"`
	Prefix       string   `xml:"Prefix"`
	Marker       string   `xml:"Marker"`
	NextMarker   string   `xml:"NextMarker"`
	Entries      struct {
		Directories []listEntryXML `xml:"Directory"`
		Files       []listEntryXML `xml:"File"`
	} `xml:"Entries"`
}

func (h *Handler) listFilesDirs(rc *reqCtx) (*resp, *apiError) {
	include := map[string]bool{}

	for v := range strings.SplitSeq(rc.q.Get("include"), ",") {
		include[strs.Fold(strings.TrimSpace(v))] = true
	}

	extended := strings.EqualFold(rc.hdr("x-ms-file-extended-info"), "true")

	maxResults := 0

	if v := rc.q.Get("maxresults"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > defaultMaxResults {
			return nil, errInvalidQuery("maxresults")
		}

		maxResults = n
	}

	h.Backend.mu.RLock("ListFilesAndDirectories")
	defer h.Backend.mu.RUnlock()

	_, dir, aerr := h.lookupDir(rc, false)
	if aerr != nil {
		return nil, aerr
	}

	prefix := strs.Fold(rc.q.Get("prefix"))
	kids := make([]*node, 0, len(dir.Children))

	for _, c := range dir.Children {
		if strings.HasPrefix(strs.Fold(c.Name), prefix) {
			kids = append(kids, c)
		}
	}

	sort.Slice(kids, func(i, j int) bool {
		if kids[i].Dir != kids[j].Dir {
			return kids[i].Dir
		}

		return strs.Fold(kids[i].Name) < strs.Fold(kids[j].Name)
	})

	pg := page.New(kids, rc.q.Get("marker"), maxResults, defaultMaxResults)

	out := listFilesXML{
		Endpoint: rc.endpoint + "/", ShareName: rc.share, Snapshot: rc.q.Get("sharesnapshot"),
		DirectoryPth: strings.Trim(rc.path, "/"), Prefix: rc.q.Get("prefix"), Marker: rc.q.Get("marker"),
		NextMarker: pg.Next,
	}
	if maxResults > 0 {
		out.MaxResults = &maxResults
	}

	for _, c := range pg.Data {
		e := dirEntry(c, include, extended)
		if c.Dir {
			out.Entries.Directories = append(out.Entries.Directories, e)
		} else {
			out.Entries.Files = append(out.Entries.Files, e)
		}
	}

	return newResp(http.StatusOK).xml(out), nil
}

func dirEntry(c *node, include map[string]bool, extended bool) listEntryXML {
	e := listEntryXML{Name: c.Name}

	if !c.Dir {
		e.Properties = &listPropsXML{ContentLength: &c.Size}
	}

	if extended {
		e.FileID = c.ID
	}

	if include["timestamps"] {
		if e.Properties == nil {
			e.Properties = &listPropsXML{}
		}

		e.Properties.CreationTime, e.Properties.LastAccessTime = fmtISO(c.Created), fmtISO(c.LastWrite)
		e.Properties.LastWriteTime, e.Properties.ChangeTime = fmtISO(c.LastWrite), fmtISO(c.Changed)
		e.Properties.LastModified = fmtHTTP(c.LastModified)
	}

	if include["etag"] {
		if e.Properties == nil {
			e.Properties = &listPropsXML{}
		}

		e.Properties.ETag = c.ETag
		e.Properties.LastModified = fmtHTTP(c.LastModified)
	}

	if include["attributes"] {
		e.Attributes = c.Attributes
	}

	if include["permission-key"] || include["permissionkey"] {
		e.PermissionKey = c.PermKey
	}

	return e
}

type handlesXML struct {
	XMLName    xml.Name `xml:"EnumerationResults"`
	Entries    struct{} `xml:"Entries"`
	NextMarker string   `xml:"NextMarker"`
}

// listHandles reports no open handles: the emulator holds no SMB sessions.
func (h *Handler) listHandles(rc *reqCtx) (*resp, *apiError) {
	h.Backend.mu.RLock("ListHandles")
	defer h.Backend.mu.RUnlock()

	sh, aerr := h.Backend.peek(rc.acct).findShare(rc.share, rc.q.Get("sharesnapshot"), false)
	if aerr != nil {
		return nil, aerr
	}

	if _, aerr = sh.lookup(rc.path); aerr != nil {
		return nil, aerr
	}

	return newResp(http.StatusOK).xml(handlesXML{}), nil
}

func (h *Handler) forceCloseHandles(rc *reqCtx) (*resp, *apiError) {
	if rc.hdr("x-ms-handle-id") == "" {
		return nil, errMissingHeader("x-ms-handle-id")
	}

	h.Backend.mu.RLock("ForceCloseHandles")
	defer h.Backend.mu.RUnlock()

	sh, aerr := h.Backend.peek(rc.acct).findShare(rc.share, rc.q.Get("sharesnapshot"), false)
	if aerr != nil {
		return nil, aerr
	}

	if _, aerr = sh.lookup(rc.path); aerr != nil {
		return nil, aerr
	}

	rs := newResp(http.StatusOK)
	rs.hdr.Set("X-Ms-Number-Of-Handles-Closed", "0")
	rs.hdr.Set("X-Ms-Number-Of-Handles-Failed", "0")
	rs.hdr.Set("X-Ms-Marker", "")

	return rs, nil
}

func (h *Handler) renameDir(rc *reqCtx) (*resp, *apiError) { return h.rename(rc, true) }

func (h *Handler) renameFile(rc *reqCtx) (*resp, *apiError) { return h.rename(rc, false) }

// sourceShareAndPath resolves a rename source (full URL or share-relative path).
func sourceShareAndPath(rc *reqCtx, raw string) (string, string, bool) {
	if !strings.Contains(raw, "://") {
		shr, path, _ := strings.Cut(strings.TrimPrefix(raw, "/"), "/")
		if i := strings.IndexByte(path, '?'); i >= 0 {
			path = path[:i]
		}

		return rc.share, strings.TrimPrefix(path, "/"), shr == "" || strs.Equal(shr, rc.share)
	}

	u, err := parseSourceURL(raw)
	if err != nil {
		return "", "", false
	}

	acct, shr, path, _, ok := splitHostPath(u.Host, u.Path)

	return shr, path, ok && strs.Equal(acct, rc.acct) && strs.Equal(shr, rc.share)
}

func (h *Handler) rename(rc *reqCtx, dir bool) (*resp, *apiError) {
	raw := rc.hdr("x-ms-file-rename-source")
	if raw == "" {
		return nil, errMissingHeader("x-ms-file-rename-source")
	}

	_, srcPath, ok := sourceShareAndPath(rc, raw)
	if !ok {
		return nil, errInvalidHeader("x-ms-file-rename-source")
	}

	md, aerr := metadataFromHeaders(rc.r.Header)
	if aerr != nil {
		return nil, aerr
	}

	b := h.Backend
	b.mu.Lock("Rename")
	defer b.mu.Unlock()

	sh, aerr := b.peek(rc.acct).findShare(rc.share, rc.q.Get("sharesnapshot"), true)
	if aerr != nil {
		return nil, aerr
	}

	srcParent, srcLeaf, aerr := sh.parentOf(srcPath)
	if aerr != nil {
		return nil, errInvalidHeader("x-ms-file-rename-source")
	}

	src := srcParent.child(srcLeaf)
	if src == nil {
		return nil, errResourceNotFound()
	}

	if src.Dir != dir {
		return nil, errTypeMismatch()
	}

	dstParent, dstLeaf, aerr := sh.parentOf(rc.path)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = h.prepareRenameTarget(rc, sh, src, dstParent, dstLeaf, srcPath); aerr != nil {
		return nil, aerr
	}

	delete(srcParent.Children, strs.Fold(srcLeaf))
	src.Name, src.ParentID = dstLeaf, dstParent.ID
	dstParent.Children[strs.Fold(dstLeaf)] = src

	if md != nil {
		src.Metadata = md
	}

	if v := rc.hdr("x-ms-content-type"); v != "" && !dir {
		src.ContentType = v
	}

	if aerr = sh.applySMB(src, smbFromHeaders(rc), false, rc.now); aerr != nil {
		return nil, aerr
	}

	src.ETag, src.LastModified, src.Changed = b.etag(), rc.now, rc.now

	return nodeResp(http.StatusOK, src), nil
}

// prepareRenameTarget enforces lease, read-only, replace and cycle rules.
func (h *Handler) prepareRenameTarget(rc *reqCtx, sh *share, src, dstParent *node, dstLeaf, srcPath string) *apiError {
	if !src.Dir {
		if aerr := src.Lease.check(rc.hdr("x-ms-source-lease-id"), rc.now, true); aerr != nil {
			return aerr
		}
	}

	if src.Dir &&
		strings.HasPrefix(strs.Fold(strings.Trim(rc.path, "/"))+"/", strs.Fold(strings.Trim(srcPath, "/"))+"/") &&
		!strs.Equal(strings.Trim(rc.path, "/"), strings.Trim(srcPath, "/")) {
		return newErr(http.StatusBadRequest, "InvalidInput", "A directory cannot be moved into itself.")
	}

	existing := dstParent.child(dstLeaf)
	if existing == nil || existing == src {
		return nil
	}

	if !strings.EqualFold(rc.hdr("x-ms-file-rename-replace-if-exists"), "true") {
		return errAlreadyExists()
	}

	if existing.Dir || src.Dir {
		return errTypeMismatch()
	}

	if aerr := existing.Lease.check(rc.hdr("x-ms-destination-lease-id"), rc.now, true); aerr != nil {
		return aerr
	}

	if strings.Contains(existing.Attributes, attrReadOnly) &&
		!strings.EqualFold(rc.hdr("x-ms-file-rename-ignore-readonly"), "true") {
		return readOnlyErr()
	}

	_ = sh

	return nil
}

func readOnlyErr() *apiError {
	return newErr(
		http.StatusConflict,
		"ReadOnlyAttribute",
		"The specified resource is read-only and cannot be modified at this time.",
	)
}
