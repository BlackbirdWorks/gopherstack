package gcpfirestore

import (
	"context"
	"strings"

	"cloud.google.com/go/firestore/apiv1/admin/adminpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	cgSegment   = "collectionGroups"
	minFields   = 2
	ancestorAll = "*"
	filterAnc   = "indexConfig.usesAncestorConfig:false"
	filterTTL   = "ttlConfig:*"
)

// collectionGroupOf validates projects/p/databases/d/collectionGroups/cg and returns the database and group.
func collectionGroupOf(name string) (string, string, error) {
	parts := strings.Split(name, "/")
	if len(parts) < 6 || parts[4] != cgSegment || parts[5] == "" {
		return "", "", errInvalid("Invalid collection group name %q.", name)
	}

	db := strings.Join(parts[:4], "/")
	if err := validateDatabase(db); err != nil {
		return "", "", err
	}

	return db, parts[5], nil
}

func indexFieldMode(f *adminpb.Index_IndexField) int {
	switch f.GetValueMode().(type) {
	case *adminpb.Index_IndexField_Order_, *adminpb.Index_IndexField_ArrayConfig_,
		*adminpb.Index_IndexField_VectorConfig_, *adminpb.Index_IndexField_SearchConfig_:
		return 1
	}

	return 0
}

func validateIndex(idx *adminpb.Index) error {
	if idx.GetQueryScope() == adminpb.Index_QUERY_SCOPE_UNSPECIFIED {
		return errInvalid("index.query_scope is required.")
	}

	vector := false

	for _, f := range idx.GetFields() {
		if f.GetFieldPath() == "" || indexFieldMode(f) == 0 {
			return errInvalid(
				"Every index field needs a field_path and exactly one of order, array_config or vector_config.",
			)
		}

		if f.GetVectorConfig() != nil {
			vector = true
		}
	}

	if len(idx.GetFields()) < minFields && !vector {
		return errInvalid("A composite index needs at least %d fields.", minFields)
	}

	return nil
}

func sameIndex(a, b *adminpb.Index) bool {
	if a.GetQueryScope() != b.GetQueryScope() || a.GetApiScope() != b.GetApiScope() ||
		len(a.GetFields()) != len(b.GetFields()) {
		return false
	}

	for i := range a.GetFields() {
		if !proto.Equal(a.GetFields()[i], b.GetFields()[i]) {
			return false
		}
	}

	return true
}

func (s *adminServer) CreateIndex(_ context.Context, in *adminpb.CreateIndexRequest) (*longrunningpb.Operation, error) {
	db, cg, err := collectionGroupOf(in.GetParent())
	if err != nil {
		return nil, err
	}

	if err = validateIndex(in.GetIndex()); err != nil {
		return nil, err
	}

	b := s.b
	b.mu.Lock("CreateIndex")
	defer b.mu.Unlock()

	if _, err = b.databaseLocked(db); err != nil {
		return nil, err
	}

	prefix := db + "/" + cgSegment + "/" + cg + "/indexes/"
	for k, have := range b.admin.indexes {
		if strings.HasPrefix(k, prefix) && sameIndex(have, in.GetIndex()) {
			return nil, errAlreadyExists("Index %q already exists.", k)
		}
	}

	idx := cloneMsg(in.GetIndex())
	idx.Name, idx.State = prefix+randHex(idBytes), adminpb.Index_READY
	b.admin.indexes[idx.GetName()] = idx

	return b.finishOpLocked(db, &adminpb.IndexOperationMetadata{
		Index: idx.GetName(), State: adminpb.OperationState_SUCCESSFUL, StartTime: b.nowTS(), EndTime: b.nowTS(),
	}, idx)
}

func (s *adminServer) GetIndex(_ context.Context, in *adminpb.GetIndexRequest) (*adminpb.Index, error) {
	s.b.mu.RLock("GetIndex")
	defer s.b.mu.RUnlock()

	idx, ok := s.b.admin.indexes[in.GetName()]
	if !ok {
		return nil, errNotFound("Index %q does not exist.", in.GetName())
	}

	return cloneMsg(idx), nil
}

func (s *adminServer) ListIndexes(
	_ context.Context,
	in *adminpb.ListIndexesRequest,
) (*adminpb.ListIndexesResponse, error) {
	db, cg, err := collectionGroupOf(in.GetParent())
	if err != nil {
		return nil, err
	}

	if in.GetFilter() != "" {
		return nil, errInvalid("ListIndexes filters are not supported.")
	}

	s.b.mu.RLock("ListIndexes")
	all := sortedByName(s.b.admin.indexes, db+"/"+cgSegment+"/"+cg+"/indexes/")
	s.b.mu.RUnlock()

	pg, err := pageOf(all, in.GetPageToken(), in.GetPageSize())
	if err != nil {
		return nil, err
	}

	out := make([]*adminpb.Index, len(pg.Data))
	for i, idx := range pg.Data {
		out[i] = cloneMsg(idx)
	}

	return &adminpb.ListIndexesResponse{Indexes: out, NextPageToken: pg.Next}, nil
}

func (s *adminServer) DeleteIndex(_ context.Context, in *adminpb.DeleteIndexRequest) (*emptypb.Empty, error) {
	s.b.mu.Lock("DeleteIndex")
	defer s.b.mu.Unlock()

	if _, ok := s.b.admin.indexes[in.GetName()]; !ok {
		return nil, errNotFound("Index %q does not exist.", in.GetName())
	}

	delete(s.b.admin.indexes, in.GetName())

	return &emptypb.Empty{}, nil
}

func fieldPathOf(name string) (string, string, error) {
	db, _, err := collectionGroupOf(name)
	if err != nil {
		return "", "", err
	}

	parts := strings.Split(name, "/")
	if len(parts) != 8 || parts[6] != "fields" || parts[7] == "" {
		return "", "", errInvalid("Invalid field name %q.", name)
	}

	return db, parts[7], nil
}

func defaultField(name, fieldPath string) *adminpb.Field {
	if fieldPath != ancestorAll {
		return &adminpb.Field{Name: name, IndexConfig: &adminpb.Field_IndexConfig{
			UsesAncestorConfig: true, AncestorField: name[:strings.LastIndex(name, "/")+1] + ancestorAll,
		}}
	}

	mk := func(scope adminpb.Index_QueryScope, mode *adminpb.Index_IndexField) *adminpb.Index {
		return &adminpb.Index{QueryScope: scope, State: adminpb.Index_READY, Fields: []*adminpb.Index_IndexField{
			{FieldPath: ancestorAll, ValueMode: mode.GetValueMode()},
		}}
	}

	asc := &adminpb.Index_IndexField{
		ValueMode: &adminpb.Index_IndexField_Order_{Order: adminpb.Index_IndexField_ASCENDING},
	}
	desc := &adminpb.Index_IndexField{
		ValueMode: &adminpb.Index_IndexField_Order_{Order: adminpb.Index_IndexField_DESCENDING},
	}
	arr := &adminpb.Index_IndexField{
		ValueMode: &adminpb.Index_IndexField_ArrayConfig_{ArrayConfig: adminpb.Index_IndexField_CONTAINS},
	}

	return &adminpb.Field{Name: name, IndexConfig: &adminpb.Field_IndexConfig{Indexes: []*adminpb.Index{
		mk(adminpb.Index_COLLECTION, asc),
		mk(adminpb.Index_COLLECTION, desc),
		mk(adminpb.Index_COLLECTION, arr),
		mk(
			adminpb.Index_COLLECTION_GROUP,
			asc,
		),
		mk(adminpb.Index_COLLECTION_GROUP, desc),
		mk(adminpb.Index_COLLECTION_GROUP, arr),
	}}}
}

func (s *adminServer) GetField(_ context.Context, in *adminpb.GetFieldRequest) (*adminpb.Field, error) {
	if _, _, err := fieldPathOf(in.GetName()); err != nil {
		return nil, err
	}

	s.b.mu.RLock("GetField")
	defer s.b.mu.RUnlock()

	if f, ok := s.b.admin.fields[in.GetName()]; ok {
		return cloneMsg(f), nil
	}

	_, fp, _ := fieldPathOf(in.GetName())

	return defaultField(in.GetName(), fp), nil
}

func (s *adminServer) ListFields(
	_ context.Context,
	in *adminpb.ListFieldsRequest,
) (*adminpb.ListFieldsResponse, error) {
	db, cg, err := collectionGroupOf(in.GetParent())
	if err != nil {
		return nil, err
	}

	if f := in.GetFilter(); f != filterAnc && f != filterTTL {
		return nil, errInvalid("ListFields only supports the filters %q and %q.", filterAnc, filterTTL)
	}

	s.b.mu.RLock("ListFields")
	all := sortedByName(s.b.admin.fields, db+"/"+cgSegment+"/"+cg+"/fields/")
	s.b.mu.RUnlock()

	kept := all[:0:0]

	for _, f := range all {
		if in.GetFilter() == filterTTL && f.GetTtlConfig() == nil {
			continue
		}

		if in.GetFilter() == filterAnc && (f.GetIndexConfig() == nil || f.GetIndexConfig().GetUsesAncestorConfig()) {
			continue
		}

		kept = append(kept, f)
	}

	pg, err := pageOf(kept, in.GetPageToken(), in.GetPageSize())
	if err != nil {
		return nil, err
	}

	out := make([]*adminpb.Field, len(pg.Data))
	for i, f := range pg.Data {
		out[i] = cloneMsg(f)
	}

	return &adminpb.ListFieldsResponse{Fields: out, NextPageToken: pg.Next}, nil
}

func (s *adminServer) UpdateField(_ context.Context, in *adminpb.UpdateFieldRequest) (*longrunningpb.Operation, error) {
	name := in.GetField().GetName()

	db, fp, err := fieldPathOf(name)
	if err != nil {
		return nil, err
	}

	b := s.b
	b.mu.Lock("UpdateField")
	defer b.mu.Unlock()

	if _, err = b.databaseLocked(db); err != nil {
		return nil, err
	}

	cur, ok := b.admin.fields[name]
	if !ok {
		cur = defaultField(name, fp)
	}

	next := cloneMsg(cur)
	paths := in.GetUpdateMask().GetPaths()

	if len(paths) == 0 {
		paths = []string{"index_config", "ttl_config"}
	}

	for _, p := range paths {
		switch p {
		case "index_config":
			next.IndexConfig = cloneMsg(in.GetField().GetIndexConfig())
			if next.GetIndexConfig() == nil {
				next.IndexConfig = defaultField(name, fp).GetIndexConfig()
			}
		case "ttl_config":
			next.TtlConfig = in.GetField().GetTtlConfig()
			if next.GetTtlConfig() != nil {
				next.TtlConfig.State = adminpb.Field_TtlConfig_ACTIVE
			}
		default:
			return nil, errInvalid("Field mask path %q is not supported.", p)
		}
	}

	b.admin.fields[name] = next

	return b.finishOpLocked(db, &adminpb.FieldOperationMetadata{
		Field: name, State: adminpb.OperationState_SUCCESSFUL, StartTime: b.nowTS(), EndTime: b.nowTS(),
	}, next)
}
