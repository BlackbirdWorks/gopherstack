package gcpbigtable

import (
	"slices"
	"strings"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type viewMsg interface {
	proto.Message
	GetQuery() string
	GetEtag() string
	GetDeletionProtection() bool
}

// viewKind carries what differs between the instance-level view resources.
type viewKind[T viewMsg] struct {
	store func(*instRec) map[string]T
	stamp func(v T, name, etag string)
	kind  string
	label string
}

func validViewID(id string) error {
	if !tableIDRe.MatchString(id) || len(id) > maxTableIDLen {
		return errInvalid("Invalid view id %q: must match [_a-zA-Z0-9][-_.a-zA-Z0-9]* and be at most %d characters.",
			id, maxTableIDLen)
	}

	return nil
}

func validQuery(q string) error {
	if strings.TrimSpace(q) == "" {
		return errInvalid("query is required.")
	}

	return nil
}

func viewPaths(paths []string, allowed ...string) ([]string, error) {
	if len(paths) == 0 || slices.Contains(paths, "*") {
		return allowed, nil
	}

	for _, p := range paths {
		if !slices.Contains(allowed, p) {
			return nil, errInvalid("Field %q cannot be updated.", p)
		}
	}

	return paths, nil
}

func (k viewKind[T]) locate(b *InMemoryBackend, name string) (*instRec, string, T, error) {
	var zero T

	inst, id, err := parseInstanceChild(name, k.kind)
	if err != nil {
		return nil, "", zero, err
	}

	rec, err := b.instanceLocked(inst)
	if err != nil {
		return nil, "", zero, err
	}

	cur, ok := k.store(rec)[id]
	if !ok {
		return nil, "", zero, errNotFound("%s %q not found.", k.label, name)
	}

	return rec, id, cur, nil
}

func (k viewKind[T]) create(
	b *InMemoryBackend, req proto.Message, parent, id string, in T, isNil bool,
) (*longrunningpb.Operation, error) {
	if err := validViewID(id); err != nil {
		return nil, err
	}

	if isNil {
		return nil, errInvalid("The %s is required.", k.label)
	}

	if err := validQuery(in.GetQuery()); err != nil {
		return nil, err
	}

	pb := clone(in)

	b.mu.Lock("Create" + k.label)
	defer b.mu.Unlock()

	rec, err := b.instanceLocked(parent)
	if err != nil {
		return nil, err
	}

	if _, exists := k.store(rec)[id]; exists {
		return nil, errExists("%s %q already exists.", k.label, parent+"/"+k.kind+"/"+id)
	}

	k.stamp(pb, parent+"/"+k.kind+"/"+id, b.newEtag())
	k.store(rec)[id] = pb

	return b.finish(parent, k.operationMeta("Create", req, b.ts(), b.ts()), clone(pb))
}

func (k viewKind[T]) get(b *InMemoryBackend, name string) (T, error) {
	b.mu.RLock("Get" + k.label)
	defer b.mu.RUnlock()

	_, _, cur, err := k.locate(b, name)
	if err != nil {
		var zero T

		return zero, err
	}

	return clone(cur), nil
}

func (k viewKind[T]) list(b *InMemoryBackend, parent, token string, size int32) ([]T, string, error) {
	if size < 0 {
		return nil, "", errInvalid("page_size must not be negative.")
	}

	b.mu.RLock("List" + k.label)
	defer b.mu.RUnlock()

	rec, err := b.instanceLocked(parent)
	if err != nil {
		return nil, "", err
	}

	items := k.store(rec)
	all := make([]T, 0, len(items))

	for _, id := range collections.SortedKeys(items) {
		all = append(all, clone(items[id]))
	}

	pg := page.New(all, token, int(size), defaultViewPage)

	return pg.Data, pg.Next, nil
}

func (k viewKind[T]) update(
	b *InMemoryBackend, req proto.Message, src T, paths []string, validate func(T) error,
) (*longrunningpb.Operation, error) {
	b.mu.Lock("Update" + k.label)
	defer b.mu.Unlock()

	rec, id, cur, err := k.locate(b, nameOf(src))
	if err != nil {
		return nil, err
	}

	if e := src.GetEtag(); e != "" && e != cur.GetEtag() {
		return nil, errAborted("%s etag %q does not match the current etag.", k.label, e)
	}

	next := clone(cur)
	if err = applyMask(next, src, paths); err != nil {
		return nil, err
	}

	if err = validate(next); err != nil {
		return nil, err
	}

	k.stamp(next, nameOf(cur), b.newEtag())
	k.store(rec)[id] = next

	return b.finish(rec.pb.GetName(), k.operationMeta("Update", req, b.ts(), b.ts()), clone(next))
}

func (k viewKind[T]) remove(b *InMemoryBackend, name, etag string) (*emptypb.Empty, error) {
	b.mu.Lock("Delete" + k.label)
	defer b.mu.Unlock()

	rec, id, cur, err := k.locate(b, name)
	if err != nil {
		return nil, err
	}

	if etag != "" && etag != cur.GetEtag() {
		return nil, errAborted("%s etag %q does not match the current etag.", k.label, etag)
	}

	if cur.GetDeletionProtection() {
		return nil, errPrecondition("%s %q has deletion protection enabled.", k.label, name)
	}

	delete(k.store(rec), id)

	return &emptypb.Empty{}, nil
}

func nameOf(m proto.Message) string {
	fd := m.ProtoReflect().Descriptor().Fields().ByName("name")

	return m.ProtoReflect().Get(fd).String()
}

func (b *InMemoryBackend) logicalKind() viewKind[*adminpb.LogicalView] {
	return viewKind[*adminpb.LogicalView]{
		kind: "logicalViews", label: "LogicalView",
		store: func(r *instRec) map[string]*adminpb.LogicalView { return r.logical },
		stamp: func(v *adminpb.LogicalView, name, etag string) { v.Name, v.Etag = name, etag },
	}
}

func (b *InMemoryBackend) materializedKind() viewKind[*adminpb.MaterializedView] {
	return viewKind[*adminpb.MaterializedView]{
		kind: "materializedViews", label: "MaterializedView",
		store: func(r *instRec) map[string]*adminpb.MaterializedView { return r.matviews },
		stamp: func(v *adminpb.MaterializedView, name, etag string) { v.Name, v.Etag = name, etag },
	}
}

// operationMeta builds the Create<Label>Metadata or Update<Label>Metadata message of a finished operation.
func (k viewKind[T]) operationMeta(op string, req proto.Message, s, e *timestamppb.Timestamp) proto.Message {
	mt, err := protoregistry.GlobalTypes.FindMessageByName(
		protoreflect.FullName("google.bigtable.admin.v2." + op + k.label + "Metadata"))
	if err != nil {
		return nil
	}

	m := mt.New()
	fields := m.Descriptor().Fields()
	m.Set(fields.ByName("original_request"), protoreflect.ValueOfMessage(req.ProtoReflect()))
	m.Set(fields.ByName("start_time"), protoreflect.ValueOfMessage(s.ProtoReflect()))
	m.Set(fields.ByName("end_time"), protoreflect.ValueOfMessage(e.ProtoReflect()))

	return m.Interface()
}

// CreateLogicalView stores a named query over the instance's tables.
func (b *InMemoryBackend) CreateLogicalView(in *adminpb.CreateLogicalViewRequest) (*longrunningpb.Operation, error) {
	lv := in.GetLogicalView()

	return b.logicalKind().create(b, in, in.GetParent(), in.GetLogicalViewId(), lv, lv == nil)
}

// GetLogicalView returns one logical view.
func (b *InMemoryBackend) GetLogicalView(in *adminpb.GetLogicalViewRequest) (*adminpb.LogicalView, error) {
	return b.logicalKind().get(b, in.GetName())
}

// ListLogicalViews lists the logical views of an instance.
func (b *InMemoryBackend) ListLogicalViews(
	in *adminpb.ListLogicalViewsRequest,
) (*adminpb.ListLogicalViewsResponse, error) {
	items, next, err := b.logicalKind().list(b, in.GetParent(), in.GetPageToken(), in.GetPageSize())
	if err != nil {
		return nil, err
	}

	return &adminpb.ListLogicalViewsResponse{LogicalViews: items, NextPageToken: next}, nil
}

// UpdateLogicalView changes the masked fields; a stale etag aborts the call.
func (b *InMemoryBackend) UpdateLogicalView(in *adminpb.UpdateLogicalViewRequest) (*longrunningpb.Operation, error) {
	paths, err := viewPaths(in.GetUpdateMask().GetPaths(), "query", fieldDeletionProtection)
	if err != nil {
		return nil, err
	}

	return b.logicalKind().update(b, in, in.GetLogicalView(), paths, func(v *adminpb.LogicalView) error {
		return validQuery(v.GetQuery())
	})
}

// DeleteLogicalView removes a logical view unless it is protected.
func (b *InMemoryBackend) DeleteLogicalView(in *adminpb.DeleteLogicalViewRequest) (*emptypb.Empty, error) {
	return b.logicalKind().remove(b, in.GetName(), in.GetEtag())
}

// CreateMaterializedView stores a named continuously-maintained query.
func (b *InMemoryBackend) CreateMaterializedView(
	in *adminpb.CreateMaterializedViewRequest,
) (*longrunningpb.Operation, error) {
	return b.materializedKind().create(
		b, in, in.GetParent(), in.GetMaterializedViewId(), in.GetMaterializedView(), in.GetMaterializedView() == nil,
	)
}

// GetMaterializedView returns one materialized view.
func (b *InMemoryBackend) GetMaterializedView(
	in *adminpb.GetMaterializedViewRequest,
) (*adminpb.MaterializedView, error) {
	return b.materializedKind().get(b, in.GetName())
}

// ListMaterializedViews lists the materialized views of an instance.
func (b *InMemoryBackend) ListMaterializedViews(
	in *adminpb.ListMaterializedViewsRequest,
) (*adminpb.ListMaterializedViewsResponse, error) {
	items, next, err := b.materializedKind().list(b, in.GetParent(), in.GetPageToken(), in.GetPageSize())
	if err != nil {
		return nil, err
	}

	return &adminpb.ListMaterializedViewsResponse{MaterializedViews: items, NextPageToken: next}, nil
}

// UpdateMaterializedView changes deletion protection; the query is immutable.
func (b *InMemoryBackend) UpdateMaterializedView(
	in *adminpb.UpdateMaterializedViewRequest,
) (*longrunningpb.Operation, error) {
	paths, err := viewPaths(in.GetUpdateMask().GetPaths(), fieldDeletionProtection)
	if err != nil {
		return nil, err
	}

	return b.materializedKind().update(b, in, in.GetMaterializedView(), paths, func(*adminpb.MaterializedView) error {
		return nil
	})
}

// DeleteMaterializedView removes a materialized view unless it is protected.
func (b *InMemoryBackend) DeleteMaterializedView(in *adminpb.DeleteMaterializedViewRequest) (*emptypb.Empty, error) {
	return b.materializedKind().remove(b, in.GetName(), in.GetEtag())
}
