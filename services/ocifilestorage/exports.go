package ocifilestorage

import (
	"net/netip"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	accessRW        = "READ_WRITE"
	accessRO        = "READ_ONLY"
	squashNone      = "NONE"
	squashRoot      = "ROOT"
	squashAll       = "ALL"
	anonDefault     = 65534
	maxUIDGID       = 4294967295
	maxExportOpts   = 100
	maxExportPath   = 1024
	maxPathElement  = 255
	maxExportsPerMT = 100
	authSys         = "SYS"
)

//nolint:gochecknoglobals // immutable lookup table
var validAuth = []string{authSys, "KRB5", "KRB5I", "KRB5P"}

type createExportIn struct {
	IsIdmapGroupsForSysAuth *bool           `json:"isIdmapGroupsForSysAuth"`
	ExportSetID             string          `json:"exportSetId"`
	FileSystemID            string          `json:"fileSystemId"`
	Path                    string          `json:"path"`
	ExportOptions           []clientOptions `json:"exportOptions"`
	Locks                   []lock          `json:"locks"`
}

type updateExportIn struct {
	IsIdmapGroupsForSysAuth *bool            `json:"isIdmapGroupsForSysAuth"`
	ExportOptions           *[]clientOptions `json:"exportOptions"`
}

// ExportFilter selects exports in a list.
type ExportFilter struct {
	Compartment, ExportSet, FileSystem, State, ID string
}

func (b *InMemoryBackend) exportView(e *entry[*Export]) Export {
	v := *e.W
	v.base = cloneBase(e.W.base)
	v.ExportOptions = slices.Clone(e.W.ExportOptions)

	return v
}

func exportSummary(v Export) map[string]any {
	return project(v, "exportSetId", "fileSystemId", "id", "lifecycleState", "path", "timeCreated",
		"isIdmapGroupsForSysAuth", "locks")
}

func validExportPath(p string) *APIError {
	if p == "" {
		return errMissing("path is required")
	}

	if len(p) > maxExportPath || !strings.HasPrefix(p, "/") || (len(p) > 1 && strings.HasSuffix(p, "/")) {
		return errParam("path must start with a slash, must not end with one and must be at most 1024 bytes long")
	}

	if p == "/" {
		return nil
	}

	for el := range strings.SplitSeq(p[1:], "/") {
		if el == "" || el == "." || el == ".." || len(el) > maxPathElement || strings.ContainsRune(el, 0) {
			return errParam("path elements must be non-empty, at most 255 bytes, and not . or ..")
		}
	}

	return nil
}

// pathsOverlap reports whether one export path is the other or an ancestor of it.
func pathsOverlap(a, c string) bool {
	if a == "/" || c == "/" {
		return true
	}

	return a == c || strings.HasPrefix(a, c+"/") || strings.HasPrefix(c, a+"/")
}

// normalizeOptions validates client options and fills the documented per-field defaults.
func normalizeOptions(opts []clientOptions, kerberosOn bool) ([]clientOptions, *APIError) {
	if len(opts) > maxExportOpts {
		return nil, errParam("an export has at most 100 exportOptions")
	}

	out := make([]clientOptions, 0, len(opts))

	for _, o := range opts {
		n, aerr := normalizeOption(o, kerberosOn)
		if aerr != nil {
			return nil, aerr
		}

		out = append(out, n)
	}

	return out, nil
}

func normalizeOption(o clientOptions, kerberosOn bool) (clientOptions, *APIError) {
	if o.Source == "" && o.Access == "" && o.IdentitySquash == "" && o.RequirePrivilegedSourcePort == nil {
		return o, nil
	}

	if !validSource(o.Source) {
		return o, errParam("exportOptions source must be a single IPv4 or IPv6 address or CIDR block")
	}

	if o.Access == "" {
		o.Access = accessRW
	}

	if o.IdentitySquash == "" {
		o.IdentitySquash = squashRoot
	}

	if o.RequirePrivilegedSourcePort == nil {
		yes := true
		o.RequirePrivilegedSourcePort = &yes
	}

	if o.IsAnonymousAccessAllowed == nil {
		no := false
		o.IsAnonymousAccessAllowed = &no
	}

	if o.AllowedAuth == nil {
		o.AllowedAuth = []string{authSys}
	}

	o.AnonymousUID, o.AnonymousGID = defaultID(o.AnonymousUID), defaultID(o.AnonymousGID)

	return o, checkOption(o, kerberosOn)
}

func checkOption(o clientOptions, kerberosOn bool) *APIError {
	if o.Access != accessRW && o.Access != accessRO {
		return errParam("exportOptions access must be READ_WRITE or READ_ONLY")
	}

	if !slices.Contains([]string{squashNone, squashRoot, squashAll}, o.IdentitySquash) {
		return errParam("exportOptions identitySquash must be NONE, ROOT or ALL")
	}

	for _, id := range []*int64{o.AnonymousUID, o.AnonymousGID} {
		if *id < 0 || *id > maxUIDGID {
			return errParam("anonymousUid and anonymousGid must be between 0 and 4294967295")
		}
	}

	for _, a := range o.AllowedAuth {
		if !slices.Contains(validAuth, a) {
			return errParam("allowedAuth entries must be SYS, KRB5, KRB5I or KRB5P")
		}

		if a != authSys && !kerberosOn {
			return errParam("Kerberos authentication requires Kerberos to be enabled on the mount target")
		}
	}

	return nil
}

func defaultID(p *int64) *int64 {
	v := int64(anonDefault)
	if p != nil {
		v = *p
	}

	return &v
}

func validSource(s string) bool {
	if _, err := netip.ParsePrefix(s); err == nil {
		return true
	}

	_, err := netip.ParseAddr(s)

	return err == nil
}

func defaultOptions() []clientOptions {
	no, uid := false, int64(anonDefault)
	gid := uid

	return []clientOptions{{
		Source: "0.0.0.0/0", RequirePrivilegedSourcePort: &no, Access: accessRW, IdentitySquash: squashNone,
		AnonymousUID: &uid, AnonymousGID: &gid, IsAnonymousAccessAllowed: &no, AllowedAuth: []string{authSys},
	}}
}

func (b *InMemoryBackend) kerberosOn(setID string) bool {
	for _, mt := range b.mts {
		if mt.W.ExportSetID == setID {
			return mt.W.Kerberos != nil && mt.W.Kerberos.IsKerberosEnabled
		}
	}

	return false
}

// CreateExport exports a file system through an export set.
func (b *InMemoryBackend) CreateExport(in createExportIn, token string) (Export, string, error) {
	if in.ExportSetID == "" || in.FileSystemID == "" {
		return Export{}, "", errMissing("exportSetId and fileSystemId are required")
	}

	if aerr := validExportPath(in.Path); aerr != nil {
		return Export{}, "", aerr
	}

	b.mu.Lock("CreateExport")
	defer b.mu.Unlock()

	if id := b.retryHit(kindExport, token); id != "" {
		if e, ok := b.exps[id]; ok {
			return b.exportView(e), e.Etag, nil
		}
	}

	set, fs, aerr := b.exportTargets(in)
	if aerr != nil {
		return Export{}, "", aerr
	}

	opts := defaultOptions()

	if in.ExportOptions != nil {
		if opts, aerr = normalizeOptions(in.ExportOptions, b.kerberosOn(set.W.ID)); aerr != nil {
			return Export{}, "", aerr
		}
	}

	for _, l := range in.Locks {
		if aerr = checkLock(l); aerr != nil {
			return Export{}, "", aerr
		}
	}

	x := &Export{
		base: b.newBase(kindExport, tagsIn{}, in.Locks), ExportSetID: set.W.ID, FileSystemID: fs.W.ID, Path: in.Path,
		ExportOptions: opts, IsIdmapGroupsForSysAuth: in.IsIdmapGroupsForSysAuth,
	}
	e := put(b, b.exps, x, stActive)
	b.remember(kindExport, token, x.ID)

	return b.exportView(e), e.Etag, nil
}

func (b *InMemoryBackend) exportTargets(in createExportIn) (*entry[*ExportSet], *entry[*FileSystem], *APIError) {
	set, aerr := b.sets.usable(in.ExportSetID)
	if aerr != nil {
		return nil, nil, aerr
	}

	fs, aerr := b.fss.usable(in.FileSystemID)
	if aerr != nil {
		return nil, nil, aerr
	}

	if set.W.State == stDeleting || fs.W.State != stActive {
		return nil, nil, errState("The export set or file system is not available for exporting")
	}

	if set.W.AvailabilityDomain != fs.W.AvailabilityDomain {
		return nil, nil, errParam("The file system and the export set must be in the same availability domain")
	}

	count := 0

	for _, x := range b.exps {
		if x.W.ExportSetID != set.W.ID || x.W.State == stDeleted || x.W.State == stDeleting {
			continue
		}

		count++

		if x.W.FileSystemID == fs.W.ID {
			return nil, nil, errConflict("The file system is already exported through this export set")
		}

		if pathsOverlap(x.W.Path, in.Path) {
			return nil, nil, errConflict("The path " + in.Path + " conflicts with the existing export path " + x.W.Path)
		}
	}

	if count >= maxExportsPerMT {
		return nil, nil, errLimit("An export set holds at most 100 exports")
	}

	return set, fs, nil
}

// GetExport returns one export.
func (b *InMemoryBackend) GetExport(id string) (Export, string, error) {
	b.mu.RLock("GetExport")
	defer b.mu.RUnlock()

	e, aerr := b.exps.get(id)
	if aerr != nil {
		return Export{}, "", aerr
	}

	return b.exportView(e), e.Etag, nil
}

// ListExports lists exports by compartment, export set or file system.
func (b *InMemoryBackend) ListExports(f ExportFilter, o ListOpts) (page.Page[map[string]any], error) {
	b.mu.RLock("ListExports")
	defer b.mu.RUnlock()

	if f.Compartment != "" {
		if aerr := b.checkCompartment(f.Compartment); aerr != nil {
			return page.Page[map[string]any]{}, aerr
		}
	}

	keep := func(e *entry[*Export]) bool {
		w := e.W

		if f.Compartment != "" {
			set, ok := b.sets[w.ExportSetID]
			if !ok || set.W.CompartmentID != f.Compartment {
				return false
			}
		}

		return (f.ExportSet == "" || w.ExportSetID == f.ExportSet) &&
			(f.FileSystem == "" || w.FileSystemID == f.FileSystem) &&
			(f.State == "" || w.State == f.State) && (f.ID == "" || w.ID == f.ID)
	}

	return listOf(b.exps, keep, o, func(w *Export) string { return w.Path },
		func(e *entry[*Export]) map[string]any { return exportSummary(b.exportView(e)) }), nil
}

// UpdateExport replaces an export's client options.
func (b *InMemoryBackend) UpdateExport(id, ifMatch string, override bool, in updateExportIn) (Export, string, error) {
	b.mu.Lock("UpdateExport")
	defer b.mu.Unlock()

	e, aerr := b.exps.usable(id)
	if aerr != nil {
		return Export{}, "", aerr
	}

	if aerr = firstErr(checkEtag(e.Etag, ifMatch), guardChange(&e.W.base, override)); aerr != nil {
		return Export{}, "", aerr
	}

	if in.ExportOptions != nil {
		opts, nerr := normalizeOptions(*in.ExportOptions, b.kerberosOn(e.W.ExportSetID))
		if nerr != nil {
			return Export{}, "", nerr
		}

		e.W.ExportOptions = opts
	}

	if in.IsIdmapGroupsForSysAuth != nil {
		e.W.IsIdmapGroupsForSysAuth = in.IsIdmapGroupsForSysAuth
	}

	e.Etag = newEtag()

	return b.exportView(e), e.Etag, nil
}

// DeleteExport deletes an export; the file system keeps its data.
func (b *InMemoryBackend) DeleteExport(id, ifMatch string, override bool) error {
	b.mu.Lock("DeleteExport")
	defer b.mu.Unlock()

	e, aerr := b.exps.usable(id)
	if aerr != nil {
		return aerr
	}

	if aerr = firstErr(checkEtag(e.Etag, ifMatch), guardDelete(&e.W.base, override)); aerr != nil {
		return aerr
	}

	begin(b, e, stDeleting, stDeleted)

	return nil
}

// ExportMount describes one export served by a mount target.
type ExportMount struct {
	ExportID       string
	Path           string
	FileSystemID   string
	Options        []clientOptions
	ForceRO        bool
	MaxFsStatBytes int64
	MaxFsStatFiles int64
}

// mountExports lists the live exports of a mount target for its NFS server.
func (b *InMemoryBackend) mountExports(mtID string) []ExportMount {
	b.mu.RLock("mountExports")
	defer b.mu.RUnlock()

	mt, ok := b.mts[mtID]
	if !ok {
		return nil
	}

	var out []ExportMount

	for _, x := range b.exps {
		if x.W.ExportSetID != mt.W.ExportSetID || x.W.State != stActive {
			continue
		}

		var statBytes, statFiles int64
		if set, found := b.sets[x.W.ExportSetID]; found {
			statBytes, statFiles = deref(set.W.MaxFsStatBytes, 0), deref(set.W.MaxFsStatFiles, 0)
		}

		out = append(out, ExportMount{
			MaxFsStatBytes: statBytes, MaxFsStatFiles: statFiles,
			ExportID: x.W.ID, Path: x.W.Path, FileSystemID: x.W.FileSystemID, Options: slices.Clone(x.W.ExportOptions),
			ForceRO: b.isReplicationTarget(x.W.FileSystemID),
		})
	}

	return out
}
