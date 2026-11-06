package azurefile

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	restypeHardLink = "hardlink"
	restypeSymLink  = "symboliclink"

	nfsRegular   = "Regular"
	nfsDirectory = "Directory"
	nfsSymLink   = "SymLink"

	defaultFileMode  = "0664"
	defaultDirMode   = "0755"
	defaultOwnerID   = "0"
	maxLinkTextBytes = 2048
	octalBase        = 8
	maxModeBits      = 0o7777
	symbolicModeLen  = 9
	modeDigits       = 4
)

// nfsProps are the POSIX properties of a node on an NFS share.
type nfsProps struct {
	Mode     string `json:"mode"`
	Owner    string `json:"owner"`
	Group    string `json:"group"`
	Type     string `json:"type"`
	LinkText string `json:"linkText,omitempty"`
	Links    int    `json:"links"`
}

func (sh *share) isNFS() bool { return strs.Equal(sh.Protocols, protoNFS) }

// nfsIn is the raw x-ms-mode/-owner/-group header set.
type nfsIn struct{ Mode, Owner, Group, Type string }

func nfsFromHeaders(rc *reqCtx) nfsIn {
	return nfsIn{
		Mode: rc.hdr("x-ms-mode"), Owner: rc.hdr("x-ms-owner"), Group: rc.hdr("x-ms-group"),
		Type: rc.hdr("x-ms-file-file-type"),
	}
}

// normalizeMode accepts a 3-4 digit octal mode or a rwxrwxrwx string and returns four octal digits.
func normalizeMode(raw string) (string, bool) {
	if len(raw) == symbolicModeLen {
		bits := 0

		for i, c := range raw {
			bits <<= 1

			if c == rune("rwxrwxrwx"[i]) {
				bits |= 1
			} else if c != '-' {
				return "", false
			}
		}

		return fmtMode(bits), true
	}

	v, err := strconv.ParseUint(raw, octalBase, 32)
	if err != nil || v > maxModeBits || len(raw) < 3 || len(raw) > 4 {
		return "", false
	}

	return fmtMode(int(v)), true
}

func fmtMode(bits int) string {
	s := strconv.FormatInt(int64(bits), octalBase)

	return strings.Repeat("0", modeDigits-len(s)) + s
}

func validNFSID(v string) bool {
	_, err := strconv.ParseUint(v, 10, 32)

	return err == nil
}

// initNFS gives n its NFS properties on create, and applies any mode, owner and group headers.
func (sh *share) initNFS(n *node, in nfsIn, create bool) *apiError {
	if !sh.isNFS() {
		return nil
	}

	if n.NFS == nil || create {
		p := &nfsProps{Mode: defaultFileMode, Owner: defaultOwnerID, Group: defaultOwnerID, Type: nfsRegular, Links: 1}
		if n.Dir {
			p.Mode, p.Type, p.Links = defaultDirMode, nfsDirectory, 2
		}

		if n.NFS != nil {
			p.Links = n.NFS.Links
		}

		n.NFS = p
	}

	if create && in.Type != "" && (n.Dir || !strs.Equal(in.Type, nfsRegular)) {
		return errInvalidHeader("x-ms-file-file-type")
	}

	if in.Mode != "" {
		m, ok := normalizeMode(in.Mode)
		if !ok {
			return errInvalidHeader("x-ms-mode")
		}

		n.NFS.Mode = m
	}

	for _, f := range []struct {
		dst  *string
		name string
		val  string
	}{{&n.NFS.Owner, "x-ms-owner", in.Owner}, {&n.NFS.Group, "x-ms-group", in.Group}} {
		if f.val == "" {
			continue
		}

		if !validNFSID(f.val) {
			return errInvalidHeader(f.name)
		}

		*f.dst = f.val
	}

	return nil
}

func setNFSHeaders(hdr http.Header, n *node) {
	hdr.Set("X-Ms-Mode", n.NFS.Mode)
	hdr.Set("X-Ms-Owner", n.NFS.Owner)
	hdr.Set("X-Ms-Group", n.NFS.Group)
	hdr.Set("X-Ms-File-File-Type", n.NFS.Type)
	hdr.Set("X-Ms-Link-Count", strconv.Itoa(n.NFS.Links))
}

// eachFileID calls fn for every file node in the tree that carries id.
func (n *node) eachFileID(id string, fn func(*node)) {
	for _, c := range n.Children {
		switch {
		case c.Dir:
			c.eachFileID(id, fn)
		case c.ID == id:
			fn(c)
		}
	}
}

// syncLinks copies n's shared state to every other hard link of the same file.
func (sh *share) syncLinks(n *node) {
	if n.NFS == nil || n.NFS.Links <= 1 {
		return
	}

	sh.Root.eachFileID(n.ID, func(o *node) {
		if o == n {
			return
		}

		c := n.clone()
		c.Name, c.ParentID, c.Lease = o.Name, o.ParentID, o.Lease
		*o = *c
	})
}

// setLinkCount records the hard link count on every link of a file.
func (sh *share) setLinkCount(id string, count int) {
	sh.Root.eachFileID(id, func(o *node) { o.NFS.Links = count })
}

func errNotNFS() *apiError {
	return newErr(http.StatusBadRequest, "InvalidUri", "This operation is only supported on NFS shares.")
}

func (h *Handler) routeLink(rc *reqCtx, restype string) (string, opFn) {
	switch {
	case rc.path == "":
	case rc.r.Method == http.MethodPut && restype == restypeHardLink:
		return opCreateHardLink, h.createHardLink
	case rc.r.Method == http.MethodPut:
		return opCreateSymLink, h.createSymLink
	case rc.r.Method == http.MethodGet && restype == restypeSymLink:
		return opGetSymLink, h.getSymLink
	}

	return unknownOperation, nil
}

func linkResp(n *node) *resp {
	rs := nodeResp(http.StatusCreated, n)
	delete(rs.hdr, "X-Ms-Request-Server-Encrypted")

	return rs
}

func (h *Handler) createHardLink(rc *reqCtx) (*resp, *apiError) {
	target := strings.Trim(rc.hdr("x-ms-file-target-file"), "/")
	if target == "" {
		return nil, errMissingHeader("x-ms-file-target-file")
	}

	b := h.Backend
	b.mu.Lock("CreateHardLink")
	defer b.mu.Unlock()

	sh, aerr := b.peek(rc.acct).findShare(rc.share, "", true)
	if aerr != nil {
		return nil, aerr
	}

	if !sh.isNFS() {
		return nil, errNotNFS()
	}

	src, aerr := sh.lookup(target)
	if aerr != nil {
		return nil, aerr
	}

	if src.Dir || src.NFS == nil || src.NFS.Type != nfsRegular {
		return nil, newErr(http.StatusBadRequest, "XNfs4FileTypeUnsupportedException",
			"The operation isn't supported on the specified NFS file because of its file type.")
	}

	parent, leaf, aerr := sh.parentOf(rc.path)
	if aerr != nil {
		return nil, aerr
	}

	if parent.child(leaf) != nil {
		return nil, errAlreadyExists()
	}

	ln := src.clone()
	ln.Name, ln.ParentID, ln.Lease, ln.Changed = leaf, parent.ID, leaseInfo{}, rc.now
	parent.Children[strs.Fold(leaf)] = ln

	sh.setLinkCount(src.ID, src.NFS.Links+1)

	return linkResp(ln), nil
}

func (h *Handler) createSymLink(rc *reqCtx) (*resp, *apiError) {
	text := rc.hdr("x-ms-link-text")
	if text == "" {
		return nil, errMissingHeader("x-ms-link-text")
	}

	if len(text) > maxLinkTextBytes {
		return nil, errInvalidHeader("x-ms-link-text")
	}

	md, aerr := metadataFromHeaders(rc.r.Header)
	if aerr != nil {
		return nil, aerr
	}

	b := h.Backend
	b.mu.Lock("CreateSymbolicLink")
	defer b.mu.Unlock()

	sh, aerr := b.peek(rc.acct).findShare(rc.share, "", true)
	if aerr != nil {
		return nil, aerr
	}

	if !sh.isNFS() {
		return nil, errNotNFS()
	}

	parent, leaf, aerr := sh.parentOf(rc.path)
	if aerr != nil {
		return nil, aerr
	}

	if parent.child(leaf) != nil {
		return nil, errAlreadyExists()
	}

	n := b.newNode(parent, leaf, false, rc.now)
	n.Metadata, n.Size = md, int64(len(text))

	if aerr = sh.applySMB(
		n,
		smbIn{Creation: rc.hdr("x-ms-file-creation-time"), LastWrite: rc.hdr("x-ms-file-last-write-time")},
		true,
		rc.now,
	); aerr != nil {
		return nil, aerr
	}

	in := nfsFromHeaders(rc)
	in.Mode, in.Type = "", ""

	if aerr = sh.initNFS(n, in, true); aerr != nil {
		return nil, aerr
	}

	n.NFS.Type, n.NFS.LinkText, n.NFS.Mode = nfsSymLink, text, "0777"
	parent.Children[strs.Fold(leaf)] = n

	return linkResp(n), nil
}

func (h *Handler) getSymLink(rc *reqCtx) (*resp, *apiError) {
	h.Backend.mu.RLock("GetSymbolicLink")
	defer h.Backend.mu.RUnlock()

	_, n, aerr := h.lookupFile(rc, false)
	if aerr != nil {
		return nil, aerr
	}

	if n.NFS == nil || n.NFS.Type != nfsSymLink {
		return nil, errTypeMismatch()
	}

	rs := newResp(http.StatusOK)
	rs.hdr.Set("ETag", n.ETag)
	rs.hdr.Set("Last-Modified", fmtHTTP(n.LastModified))
	rs.hdr.Set("X-Ms-Link-Text", n.NFS.LinkText)

	return rs, nil
}
