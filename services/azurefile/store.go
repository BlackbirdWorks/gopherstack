package azurefile

import (
	"fmt"
	"hash/fnv"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// defaultSDDL is the permission applied when a request asks to "inherit".
const defaultSDDL = "O:S-1-5-21-2127521184-1604012920-1887927527-21560G:S-1-5-21-2127521184-1604012920-1887927527-513" +
	"D:AI(A;;FA;;;SY)(A;;FA;;;BA)(A;;0x1200a9;;;S-1-5-21-397955417-626881126-188441444-512)"

const (
	maxSDDLLen     = 8192
	maxNameLen     = 255
	maxPathDepth   = 250
	maxMetadataLen = 8192
)

// InMemoryBackend holds every account's shares behind one coarse lock.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	accounts map[string]*account
	seq      uint64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu:       lockmetrics.New("azurefile"),
		accounts: make(map[string]*account),
		seq:      uint64(time.Now().UnixNano()),
	}
}

// Reset drops all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.accounts = make(map[string]*account)
}

func (b *InMemoryBackend) next() uint64 {
	b.seq++

	return b.seq
}

func (b *InMemoryBackend) etag() string {
	return fmt.Sprintf("\"0x%X\"", b.next())
}

func (b *InMemoryBackend) newNodeID() string { return strconv.FormatUint(b.next(), 10) }

// acct returns the named account, creating it. Caller holds the write lock.
func (b *InMemoryBackend) acct(name string) *account {
	key := strs.Fold(name)
	a, ok := b.accounts[key]

	if !ok {
		a = &account{Shares: make(map[string]*share)}
		b.accounts[key] = a
	}

	return a
}

// peek returns the named account or nil. Caller holds at least the read lock.
func (b *InMemoryBackend) peek(name string) *account { return b.accounts[strs.Fold(name)] }

func (a *account) props() *serviceProps {
	if a.Props == nil {
		a.Props = defaultServiceProps()
	}

	return a.Props
}

// findShare resolves a share (or one of its snapshots). Writes to a snapshot are refused.
func (a *account) findShare(name, snap string, write bool) (*share, *apiError) {
	if a == nil {
		return nil, errShareNotFound()
	}

	sh, ok := a.Shares[strs.Fold(name)]
	if !ok {
		return nil, errShareNotFound()
	}

	if snap == "" {
		return sh, nil
	}

	if write {
		return nil, errReadOnlyShareSnapshot()
	}

	s, ok := sh.Snapshots[snap]
	if !ok {
		return nil, errShareNotFound()
	}

	return s, nil
}

func validShareName(n string) bool {
	const minLen, maxLen = 3, 63
	if len(n) < minLen || len(n) > maxLen || n[0] == '-' || n[len(n)-1] == '-' || strings.Contains(n, "--") {
		return false
	}

	for _, c := range n {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}

	return true
}

func validSegment(s string) bool {
	if s == "" || len(s) > maxNameLen || s == "." || s == ".." {
		return false
	}

	for _, c := range s {
		if c < 0x20 || strings.ContainsRune(`"\/:|<>*?`, c) {
			return false
		}
	}

	return true
}

// splitFilePath splits "a/b/c" into validated segments; "" yields none.
func splitFilePath(p string) ([]string, *apiError) {
	p = strings.Trim(p, "/")
	if p == "" {
		return nil, nil
	}

	segs := strings.Split(p, "/")
	if len(segs) > maxPathDepth {
		return nil, errInvalidPath()
	}

	for _, s := range segs {
		if !validSegment(s) {
			return nil, errInvalidPath()
		}
	}

	return segs, nil
}

// lookup finds the node at path under sh.
func (sh *share) lookup(path string) (*node, *apiError) {
	segs, err := splitFilePath(path)
	if err != nil {
		return nil, err
	}

	cur := sh.Root

	for _, s := range segs {
		if !cur.Dir {
			return nil, errResourceNotFound()
		}

		next, ok := cur.Children[strs.Fold(s)]
		if !ok {
			return nil, errResourceNotFound()
		}

		cur = next
	}

	return cur, nil
}

// parentOf finds the parent directory node and the leaf name for path.
func (sh *share) parentOf(path string) (*node, string, *apiError) {
	segs, err := splitFilePath(path)
	if err != nil {
		return nil, "", err
	}

	if len(segs) == 0 {
		return nil, "", errAlreadyExists()
	}

	cur := sh.Root

	for _, s := range segs[:len(segs)-1] {
		next, ok := cur.Children[strs.Fold(s)]
		if !ok || !next.Dir {
			return nil, "", errParentNotFound()
		}

		cur = next
	}

	return cur, segs[len(segs)-1], nil
}

func (b *InMemoryBackend) newNode(parent *node, name string, dir bool, now time.Time) *node {
	n := &node{
		Name: name, Dir: dir, ID: b.newNodeID(), Created: now, LastWrite: now, Changed: now,
		LastModified: now, ETag: b.etag(), Attributes: attrArchive,
	}

	if parent != nil {
		n.ParentID = parent.ID
	}

	if dir {
		n.Attributes = attrDirectory
		n.Children = make(map[string]*node)
	}

	return n
}

func (b *InMemoryBackend) newShare(name string, now time.Time) *share {
	root := b.newNode(nil, "", true, now)
	root.ID = "0"

	return &share{
		Name: name, Created: now, LastModified: now, ETag: b.etag(), Quota: defaultQuotaGiB,
		Tier: defaultTier, Protocols: protoSMB, Root: root, TierChanged: now,
		Permissions: make(map[string]string),
	}
}

func permKeyFor(sddl string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(sddl))
	a := h.Sum64()
	_, _ = h.Write([]byte("#"))

	return strconv.FormatUint(a, 10) + "*" + strconv.FormatUint(h.Sum64(), 10)
}

func (sh *share) addPermission(sddl string) string {
	if sh.Permissions == nil {
		sh.Permissions = make(map[string]string)
	}

	key := permKeyFor(sddl)
	sh.Permissions[key] = sddl

	return key
}

// resolvePerm turns the permission headers into a stored key. existing is the
// node's current key ("" on create).
func (sh *share) resolvePerm(perm, key, existing string) (string, *apiError) {
	if perm == "" && (strs.Equal(key, "preserve") || strs.Equal(key, "inherit")) {
		perm, key = key, ""
	}

	if perm != "" && key != "" {
		return "", errInvalidHeader("x-ms-file-permission")
	}

	switch {
	case key != "":
		if _, ok := sh.Permissions[key]; !ok {
			return "", errInvalidHeader("x-ms-file-permission-key")
		}

		return key, nil
	case perm == "" || strs.Equal(perm, "inherit") || strs.Equal(perm, "preserve"):
		if existing != "" && !strs.Equal(perm, "inherit") {
			return existing, nil
		}

		return sh.addPermission(defaultSDDL), nil
	case len(perm) > maxSDDLLen:
		return "", errInvalidHeader("x-ms-file-permission")
	}

	return sh.addPermission(perm), nil
}

// smbIn is the raw SMB property header set of a create/set-properties request.
type smbIn struct {
	Attributes string
	Creation   string
	LastWrite  string
	Change     string
	Permission string
	PermKey    string
}

func parseISO(raw string) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339Nano, raw)

	return t.UTC(), err == nil
}

func resolveTime(raw string, existing, now time.Time, create bool) (time.Time, *apiError) {
	switch {
	case strs.Equal(raw, "preserve"):
		if create {
			return now, nil
		}

		return existing, nil
	case raw == "" && !create:
		return existing, nil
	case raw == "" || strs.Equal(raw, "now"):
		return now, nil
	}

	t, ok := parseISO(raw)
	if !ok {
		return time.Time{}, errInvalidHeader("x-ms-file-creation-time")
	}

	return t, nil
}

func attrOrder() []string {
	return []string{
		attrReadOnly,
		"Hidden",
		"System",
		attrDirectory,
		attrArchive,
		"Temporary",
		"Offline",
		"NotContentIndexed",
		"NoScrubData",
	}
}

// parseAttrSet parses "A|B" into a set of canonical attribute names.
func parseAttrSet(raw string) (map[string]bool, *apiError) {
	set := map[string]bool{}

	for p := range strings.SplitSeq(raw, "|") {
		p = strings.TrimSpace(p)
		if strs.Equal(p, attrNone) {
			continue
		}

		idx := slices.IndexFunc(attrOrder(), func(a string) bool { return strs.Equal(p, a) })
		if idx < 0 {
			return nil, errInvalidHeader("x-ms-file-attributes")
		}

		set[attrOrder()[idx]] = true
	}

	return set, nil
}

func normalizeAttrs(raw string, dir, create bool) (string, *apiError) {
	if raw == "" || strs.Equal(raw, "preserve") {
		return "", nil
	}

	set, aerr := parseAttrSet(raw)
	if aerr != nil {
		return "", aerr
	}

	switch {
	case dir:
		set[attrDirectory] = true
	case set[attrDirectory]:
		return "", errInvalidHeader("x-ms-file-attributes")
	case create && len(set) == 0:
		set[attrArchive] = true
	}

	var out []string

	for _, a := range attrOrder() {
		if set[a] {
			out = append(out, a)
		}
	}

	if len(out) == 0 {
		return attrNone, nil
	}

	return strings.Join(out, "|"), nil
}

// applySMB applies SMB headers to n. create selects create-time defaults.
func (sh *share) applySMB(n *node, in smbIn, create bool, now time.Time) *apiError {
	attrs, err := normalizeAttrs(in.Attributes, n.Dir, create)
	if err != nil {
		return err
	}

	if attrs != "" {
		n.Attributes = attrs
	}

	key, err := sh.resolvePerm(in.Permission, in.PermKey, n.PermKey)
	if err != nil {
		return err
	}

	n.PermKey = key

	if n.Created, err = resolveTime(in.Creation, n.Created, now, create); err != nil {
		return err
	}

	if n.LastWrite, err = resolveTime(in.LastWrite, n.LastWrite, now, create); err != nil {
		return err
	}

	if in.Change != "" && !strs.Equal(in.Change, "now") && !strs.Equal(in.Change, "preserve") {
		n.Changed, err = resolveTime(in.Change, n.Changed, now, false)

		return err
	}

	return nil
}

func validateMetadata(md map[string]string) *apiError {
	total := 0

	for k, v := range md {
		if k == "" || (k[0] >= '0' && k[0] <= '9') {
			return newErr(http.StatusBadRequest, "InvalidMetadata", "The metadata specified is invalid.")
		}

		for _, c := range k {
			if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' {
				return newErr(http.StatusBadRequest, "InvalidMetadata", "The metadata specified is invalid.")
			}
		}

		total += len(k) + len(v)
	}

	if total > maxMetadataLen {
		return newErr(http.StatusBadRequest, "MetadataTooLarge", "The metadata specified exceeds the maximum size.")
	}

	return nil
}
