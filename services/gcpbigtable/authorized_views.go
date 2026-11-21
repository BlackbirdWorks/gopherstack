package gcpbigtable

import (
	"encoding/hex"
	"slices"
	"strings"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const defaultViewPage = 100

func (b *InMemoryBackend) newEtag() string { return hex.EncodeToString(b.nextEtag()) }

func (b *InMemoryBackend) viewLocked(name string) (*instRec, *tableRec, *viewRec, string, error) {
	inst, tbl, id, err := parseChild(name, "authorizedViews")
	if err != nil {
		return nil, nil, nil, "", err
	}

	rec, err := b.instanceLocked(inst)
	if err != nil {
		return nil, nil, nil, "", err
	}

	t, ok := rec.tables[tbl]
	if !ok {
		return nil, nil, nil, "", errTableNotFound(tableName(inst, tbl))
	}

	v, ok := t.views[id]
	if !ok {
		return nil, nil, nil, "", errNotFound("AuthorizedView %q not found.", name)
	}

	return rec, t, v, id, nil
}

func validateViewSpec(t *tableRec, av *adminpb.AuthorizedView) error {
	sv := av.GetSubsetView()
	if sv == nil {
		return errInvalid("authorized_view.subset_view is required.")
	}

	for fam, fs := range sv.GetFamilySubsets() {
		if _, ok := t.pb.GetColumnFamilies()[fam]; !ok {
			return errInvalid("Column family %q in family_subsets does not exist in the table.", fam)
		}

		if fs == nil {
			sv.FamilySubsets[fam] = &adminpb.AuthorizedView_FamilySubsets{}
		}
	}

	return nil
}

func viewView(v *adminpb.AuthorizedView, view, def adminpb.AuthorizedView_ResponseView) *adminpb.AuthorizedView {
	if view == adminpb.AuthorizedView_RESPONSE_VIEW_UNSPECIFIED {
		view = def
	}

	switch view {
	case adminpb.AuthorizedView_NAME_ONLY:
		return &adminpb.AuthorizedView{Name: v.GetName()}
	case adminpb.AuthorizedView_BASIC:
		return &adminpb.AuthorizedView{
			Name: v.GetName(), DeletionProtection: v.GetDeletionProtection(), Etag: v.GetEtag(),
		}
	default:
		return clone(v)
	}
}

// CreateAuthorizedView creates a subset view of a table.
func (b *InMemoryBackend) CreateAuthorizedView(
	in *adminpb.CreateAuthorizedViewRequest,
) (*longrunningpb.Operation, error) {
	id := in.GetAuthorizedViewId()
	if !tableIDRe.MatchString(id) || len(id) > maxTableIDLen {
		return nil, errInvalid(
			"Invalid authorized view id %q: must match [_a-zA-Z0-9][-_.a-zA-Z0-9]* and be at most %d characters.",
			id, maxTableIDLen,
		)
	}

	if in.GetAuthorizedView() == nil {
		return nil, errInvalid("authorized_view is required.")
	}

	b.mu.Lock("CreateAuthorizedView")
	defer b.mu.Unlock()

	rec, t, _, err := b.tableLocked(in.GetParent())
	if err != nil {
		return nil, err
	}

	if _, exists := t.views[id]; exists {
		return nil, errExists("AuthorizedView %q already exists.", in.GetParent()+"/authorizedViews/"+id)
	}

	pb := clone(in.GetAuthorizedView())
	if err = validateViewSpec(t, pb); err != nil {
		return nil, err
	}

	pb.Name = in.GetParent() + "/authorizedViews/" + id
	pb.Etag = b.newEtag()
	t.views[id] = &viewRec{pb: pb}

	return b.finish(rec.pb.GetName(), &adminpb.CreateAuthorizedViewMetadata{
		OriginalRequest: in, RequestTime: b.ts(), FinishTime: b.ts(),
	}, clone(pb))
}

// GetAuthorizedView returns a view; BASIC (the default) omits the subset, FULL includes it.
func (b *InMemoryBackend) GetAuthorizedView(in *adminpb.GetAuthorizedViewRequest) (*adminpb.AuthorizedView, error) {
	b.mu.RLock("GetAuthorizedView")
	defer b.mu.RUnlock()

	_, _, v, _, err := b.viewLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	return viewView(v.pb, in.GetView(), adminpb.AuthorizedView_BASIC), nil
}

// ListAuthorizedViews lists the views of a table (NAME_ONLY by default).
func (b *InMemoryBackend) ListAuthorizedViews(
	in *adminpb.ListAuthorizedViewsRequest,
) (*adminpb.ListAuthorizedViewsResponse, error) {
	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	b.mu.RLock("ListAuthorizedViews")
	defer b.mu.RUnlock()

	_, t, _, err := b.tableLocked(in.GetParent())
	if err != nil {
		return nil, err
	}

	all := make([]*adminpb.AuthorizedView, 0, len(t.views))
	for _, id := range collections.SortedKeys(t.views) {
		all = append(all, viewView(t.views[id].pb, in.GetView(), adminpb.AuthorizedView_NAME_ONLY))
	}

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultViewPage)

	return &adminpb.ListAuthorizedViewsResponse{AuthorizedViews: pg.Data, NextPageToken: pg.Next}, nil
}

var updatableViewPaths = []string{ //nolint:gochecknoglobals // immutable catalog
	"subset_view", "subset_view.row_prefixes", "subset_view.family_subsets", fieldDeletionProtection,
}

func viewMaskPaths(in *adminpb.UpdateAuthorizedViewRequest) ([]string, error) {
	paths := in.GetUpdateMask().GetPaths()

	switch {
	case slices.Contains(paths, "*"):
		return []string{"subset_view", fieldDeletionProtection}, nil
	case len(paths) == 0:
		var out []string
		if in.GetAuthorizedView().GetSubsetView() != nil {
			out = append(out, "subset_view")
		}

		if in.GetAuthorizedView().GetDeletionProtection() {
			out = append(out, fieldDeletionProtection)
		}

		return out, nil
	}

	for _, p := range paths {
		if !slices.Contains(updatableViewPaths, p) {
			return nil, errInvalid("AuthorizedView field %q cannot be updated.", p)
		}
	}

	return paths, nil
}

// UpdateAuthorizedView updates the masked fields; a stale etag aborts the call.
func (b *InMemoryBackend) UpdateAuthorizedView(
	in *adminpb.UpdateAuthorizedViewRequest,
) (*longrunningpb.Operation, error) {
	paths, err := viewMaskPaths(in)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateAuthorizedView")
	defer b.mu.Unlock()

	rec, t, v, _, err := b.viewLocked(in.GetAuthorizedView().GetName())
	if err != nil {
		return nil, err
	}

	if e := in.GetAuthorizedView().GetEtag(); e != "" && e != v.pb.GetEtag() {
		return nil, errAborted("AuthorizedView etag %q does not match the current etag.", e)
	}

	next := clone(v.pb)
	if err = applyMask(next, in.GetAuthorizedView(), paths); err != nil {
		return nil, err
	}

	if err = validateViewSpec(t, next); err != nil {
		return nil, err
	}

	next.Etag = b.newEtag()
	v.pb = next

	return b.finish(rec.pb.GetName(), &adminpb.UpdateAuthorizedViewMetadata{
		OriginalRequest: in, RequestTime: b.ts(), FinishTime: b.ts(),
	}, clone(next))
}

// DeleteAuthorizedView removes a view unless it is protected.
func (b *InMemoryBackend) DeleteAuthorizedView(in *adminpb.DeleteAuthorizedViewRequest) (*emptypb.Empty, error) {
	b.mu.Lock("DeleteAuthorizedView")
	defer b.mu.Unlock()

	_, t, v, id, err := b.viewLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	if e := in.GetEtag(); e != "" && e != v.pb.GetEtag() {
		return nil, errAborted("AuthorizedView etag %q does not match the current etag.", e)
	}

	if v.pb.GetDeletionProtection() {
		return nil, errPrecondition("AuthorizedView %q has deletion protection enabled.", in.GetName())
	}

	delete(t.views, id)

	return &emptypb.Empty{}, nil
}

func (t *tableRec) protectedView() string {
	for _, id := range collections.SortedKeys(t.views) {
		if t.views[id].pb.GetDeletionProtection() {
			return t.views[id].pb.GetName()
		}
	}

	return ""
}

// viewScope restricts a data-plane request to the subset an authorized view exposes.
type viewScope struct {
	t  *tableRec
	sv *adminpb.AuthorizedView_SubsetView
}

func (s *viewScope) allowsRow(key string) bool {
	for _, p := range s.sv.GetRowPrefixes() {
		if strings.HasPrefix(key, string(p)) {
			return true
		}
	}

	return false
}

func (s *viewScope) allowsCell(fam, qual string) bool {
	fs, ok := s.sv.GetFamilySubsets()[fam]
	if !ok {
		return false
	}

	for _, q := range fs.GetQualifiers() {
		if string(q) == qual {
			return true
		}
	}

	for _, p := range fs.GetQualifierPrefixes() {
		if strings.HasPrefix(qual, string(p)) {
			return true
		}
	}

	return false
}

func (s *viewScope) allowsFamily(fam string) bool {
	fs, ok := s.sv.GetFamilySubsets()[fam]
	if !ok {
		return false
	}

	return slices.ContainsFunc(fs.GetQualifierPrefixes(), func(p []byte) bool { return len(p) == 0 })
}

func (s *viewScope) allowsWholeRow() bool {
	for fam := range s.t.pb.GetColumnFamilies() {
		if !s.allowsFamily(fam) {
			return false
		}
	}

	return true
}

func (s *viewScope) filterCells(cells []fcell) []fcell {
	return slices.DeleteFunc(slices.Clone(cells), func(c fcell) bool { return !s.allowsCell(c.fam, c.qual) })
}

func (s *viewScope) checkRow(key []byte) error {
	if !s.allowsRow(string(key)) {
		return errDenied("Row key %q is outside the authorized view.", key)
	}

	return nil
}

func (s *viewScope) checkCell(fam string, qual []byte) error {
	if !s.allowsCell(fam, string(qual)) {
		return errDenied("Column %s:%q is outside the authorized view.", fam, qual)
	}

	return nil
}

func (s *viewScope) checkMutations(muts []*btpb.Mutation) error {
	for _, m := range muts {
		if err := s.checkMutation(m); err != nil {
			return err
		}
	}

	return nil
}

func (s *viewScope) checkMutation(m *btpb.Mutation) error {
	switch mt := m.GetMutation().(type) {
	case *btpb.Mutation_SetCell_:
		return s.checkCell(mt.SetCell.GetFamilyName(), mt.SetCell.GetColumnQualifier())
	case *btpb.Mutation_AddToCell_:
		return s.checkCell(mt.AddToCell.GetFamilyName(), mt.AddToCell.GetColumnQualifier().GetRawValue())
	case *btpb.Mutation_MergeToCell_:
		return s.checkCell(mt.MergeToCell.GetFamilyName(), mt.MergeToCell.GetColumnQualifier().GetRawValue())
	case *btpb.Mutation_DeleteFromColumn_:
		return s.checkCell(mt.DeleteFromColumn.GetFamilyName(), mt.DeleteFromColumn.GetColumnQualifier())
	case *btpb.Mutation_DeleteFromFamily_:
		if !s.allowsFamily(mt.DeleteFromFamily.GetFamilyName()) {
			return errDenied("Family %q is not fully inside the authorized view.", mt.DeleteFromFamily.GetFamilyName())
		}
	case *btpb.Mutation_DeleteFromRow_:
		if !s.allowsWholeRow() {
			return errDenied("DeleteFromRow needs every column family fully inside the authorized view.")
		}
	}

	return nil
}

func (s *viewScope) filterSamples(in []*btpb.SampleRowKeysResponse) []*btpb.SampleRowKeysResponse {
	out := make([]*btpb.SampleRowKeysResponse, 0, len(in))

	for _, sm := range in {
		if len(sm.GetRowKey()) == 0 || s.allowsRow(string(sm.GetRowKey())) {
			out = append(out, sm)
		}
	}

	return out
}

// The helpers below accept a nil scope, which means a request addressed to the table itself.

func (s *viewScope) check(key []byte, muts []*btpb.Mutation) error {
	if s == nil {
		return nil
	}

	if err := s.checkRow(key); err != nil {
		return err
	}

	return s.checkMutations(muts)
}

func (s *viewScope) checkRules(key []byte, rules []*btpb.ReadModifyWriteRule) error {
	if s == nil {
		return nil
	}

	if err := s.checkRow(key); err != nil {
		return err
	}

	for _, r := range rules {
		if err := s.checkCell(r.GetFamilyName(), r.GetColumnQualifier()); err != nil {
			return err
		}
	}

	return nil
}

func (s *viewScope) visible(cells []fcell) []fcell {
	if s == nil {
		return cells
	}

	return s.filterCells(cells)
}

func (s *viewScope) visibleKeys(keys []string) []string {
	if s == nil {
		return keys
	}

	return slices.DeleteFunc(slices.Clone(keys), func(k string) bool { return !s.allowsRow(k) })
}

func (s *viewScope) visibleSamples(in []*btpb.SampleRowKeysResponse) []*btpb.SampleRowKeysResponse {
	if s == nil {
		return in
	}

	return s.filterSamples(in)
}
