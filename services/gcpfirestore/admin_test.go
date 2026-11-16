package gcpfirestore_test

import (
	"testing"

	"cloud.google.com/go/firestore/apiv1/admin/adminpb"
	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const projRoot = "projects/" + project

type adminEnv struct {
	*env
	admin adminpb.FirestoreAdminClient
	ops   longrunningpb.OperationsClient
}

func newAdminEnv(t *testing.T) *adminEnv {
	t.Helper()

	e := newEnv(t)

	return &adminEnv{
		env:   e,
		admin: adminpb.NewFirestoreAdminClient(e.conn),
		ops:   longrunningpb.NewOperationsClient(e.conn),
	}
}

func (a *adminEnv) createDB(t *testing.T, id string, protect bool) *longrunningpb.Operation {
	t.Helper()

	db := &adminpb.Database{LocationId: "nam5"}
	if protect {
		db.DeleteProtectionState = adminpb.Database_DELETE_PROTECTION_ENABLED
	}

	op, err := a.admin.CreateDatabase(
		t.Context(),
		&adminpb.CreateDatabaseRequest{Parent: projRoot, DatabaseId: id, Database: db},
	)
	require.NoError(t, err)

	return op
}

func TestAdminDatabaseLifecycle(t *testing.T) {
	t.Parallel()

	a := newAdminEnv(t)
	ctx := t.Context()

	op := a.createDB(t, "analytics", false)
	assert.True(t, op.GetDone())
	assert.Contains(t, op.GetName(), projRoot+"/databases/analytics/operations/")

	_, err := a.admin.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent: projRoot, DatabaseId: "analytics", Database: &adminpb.Database{LocationId: "nam5"},
	})
	assert.Equal(t, codes.AlreadyExists, code(err))

	_, err = a.admin.CreateDatabase(
		ctx,
		&adminpb.CreateDatabaseRequest{Parent: projRoot, DatabaseId: "nolocation", Database: &adminpb.Database{}},
	)
	assert.Equal(t, codes.InvalidArgument, code(err))

	db, err := a.admin.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: projRoot + "/databases/analytics"})
	require.NoError(t, err)
	assert.Equal(t, adminpb.Database_FIRESTORE_NATIVE, db.GetType())
	assert.Equal(t, "nam5", db.GetLocationId())
	assert.NotEmpty(t, db.GetUid())

	_, err = a.admin.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: projRoot + "/databases/missing"})
	assert.Equal(t, codes.NotFound, code(err))

	def, err := a.admin.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: dbRoot})
	require.NoError(t, err)
	assert.Equal(t, dbRoot, def.GetName())

	list, err := a.admin.ListDatabases(ctx, &adminpb.ListDatabasesRequest{Parent: projRoot})
	require.NoError(t, err)

	names := make([]string, 0, len(list.GetDatabases()))
	for _, d := range list.GetDatabases() {
		names = append(names, d.GetName())
	}

	assert.Equal(t, []string{dbRoot, projRoot + "/databases/analytics"}, names)

	_, err = a.admin.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database: &adminpb.Database{
			Name:                  db.GetName(),
			DeleteProtectionState: adminpb.Database_DELETE_PROTECTION_ENABLED,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"delete_protection_state"}},
	})
	require.NoError(t, err)

	_, err = a.admin.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{Name: db.GetName()})
	assert.Equal(t, codes.FailedPrecondition, code(err), "delete protection blocks deletion")

	_, err = a.admin.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database: &adminpb.Database{
			Name:                  db.GetName(),
			DeleteProtectionState: adminpb.Database_DELETE_PROTECTION_DISABLED,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"delete_protection_state"}},
	})
	require.NoError(t, err)

	_, err = a.admin.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{Name: db.GetName(), Etag: "stale"})
	assert.Equal(t, codes.Aborted, code(err))

	_, err = a.admin.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{Name: db.GetName()})
	require.NoError(t, err)

	_, err = a.admin.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: db.GetName()})
	assert.Equal(t, codes.NotFound, code(err))
}

func TestAdminDeleteDatabaseRemovesDocuments(t *testing.T) {
	t.Parallel()

	a := newAdminEnv(t)
	a.createDB(t, "scratch", false)

	other := projRoot + "/databases/scratch"
	w := setWrite("x/1", "v")
	w.GetUpdate().Name = other + "/documents/x/1"

	_, err := a.raw.Commit(t.Context(), commitOf(other, w))
	require.NoError(t, err)

	_, err = a.admin.DeleteDatabase(t.Context(), &adminpb.DeleteDatabaseRequest{Name: other})
	require.NoError(t, err)

	a.createDB(t, "scratch", false)

	got, err := a.raw.GetDocument(t.Context(), getDoc(other+"/documents/x/1"))
	assert.Nil(t, got)
	assert.Equal(t, codes.NotFound, code(err))
}

func TestAdminIndexes(t *testing.T) {
	t.Parallel()

	a := newAdminEnv(t)
	ctx := t.Context()
	parent := dbRoot + "/collectionGroups/users"

	asc := func(f string) *adminpb.Index_IndexField {
		return &adminpb.Index_IndexField{
			FieldPath: f,
			ValueMode: &adminpb.Index_IndexField_Order_{Order: adminpb.Index_IndexField_ASCENDING},
		}
	}
	valid := func() *adminpb.Index {
		return &adminpb.Index{
			QueryScope: adminpb.Index_COLLECTION,
			Fields:     []*adminpb.Index_IndexField{asc("a"), asc("b")},
		}
	}

	bad := []*adminpb.Index{
		{Fields: []*adminpb.Index_IndexField{asc("a"), asc("b")}},
		{QueryScope: adminpb.Index_COLLECTION, Fields: []*adminpb.Index_IndexField{asc("a")}},
		{QueryScope: adminpb.Index_COLLECTION, Fields: []*adminpb.Index_IndexField{asc("a"), {FieldPath: "b"}}},
	}

	for _, idx := range bad {
		_, err := a.admin.CreateIndex(ctx, &adminpb.CreateIndexRequest{Parent: parent, Index: idx})
		assert.Equal(t, codes.InvalidArgument, code(err))
	}

	op, err := a.admin.CreateIndex(ctx, &adminpb.CreateIndexRequest{Parent: parent, Index: valid()})
	require.NoError(t, err)
	assert.True(t, op.GetDone())

	_, err = a.admin.CreateIndex(ctx, &adminpb.CreateIndexRequest{Parent: parent, Index: valid()})
	assert.Equal(t, codes.AlreadyExists, code(err))

	list, err := a.admin.ListIndexes(ctx, &adminpb.ListIndexesRequest{Parent: parent})
	require.NoError(t, err)
	require.Len(t, list.GetIndexes(), 1)
	assert.Equal(t, adminpb.Index_READY, list.GetIndexes()[0].GetState())

	got, err := a.admin.GetIndex(ctx, &adminpb.GetIndexRequest{Name: list.GetIndexes()[0].GetName()})
	require.NoError(t, err)
	assert.Equal(t, list.GetIndexes()[0].GetName(), got.GetName())

	_, err = a.admin.ListIndexes(ctx, &adminpb.ListIndexesRequest{Parent: parent, Filter: "state:READY"})
	assert.Equal(t, codes.InvalidArgument, code(err))

	other, err := a.admin.ListIndexes(ctx, &adminpb.ListIndexesRequest{Parent: dbRoot + "/collectionGroups/other"})
	require.NoError(t, err)
	assert.Empty(t, other.GetIndexes())

	_, err = a.admin.DeleteIndex(ctx, &adminpb.DeleteIndexRequest{Name: got.GetName()})
	require.NoError(t, err)

	_, err = a.admin.GetIndex(ctx, &adminpb.GetIndexRequest{Name: got.GetName()})
	assert.Equal(t, codes.NotFound, code(err))
}

func TestAdminFields(t *testing.T) {
	t.Parallel()

	a := newAdminEnv(t)
	ctx := t.Context()
	name := dbRoot + "/collectionGroups/users/fields/email"

	def, err := a.admin.GetField(ctx, &adminpb.GetFieldRequest{Name: name})
	require.NoError(t, err)
	assert.True(t, def.GetIndexConfig().GetUsesAncestorConfig())

	star, err := a.admin.GetField(ctx, &adminpb.GetFieldRequest{Name: dbRoot + "/collectionGroups/users/fields/*"})
	require.NoError(t, err)
	assert.NotEmpty(t, star.GetIndexConfig().GetIndexes())

	_, err = a.admin.UpdateField(ctx, &adminpb.UpdateFieldRequest{
		Field:      &adminpb.Field{Name: name, IndexConfig: &adminpb.Field_IndexConfig{}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"index_config"}},
	})
	require.NoError(t, err)

	_, err = a.admin.UpdateField(ctx, &adminpb.UpdateFieldRequest{
		Field: &adminpb.Field{
			Name:      dbRoot + "/collectionGroups/users/fields/expires",
			TtlConfig: &adminpb.Field_TtlConfig{},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"ttl_config"}},
	})
	require.NoError(t, err)

	exempt, err := a.admin.ListFields(ctx, &adminpb.ListFieldsRequest{
		Parent: dbRoot + "/collectionGroups/users", Filter: "indexConfig.usesAncestorConfig:false",
	})
	require.NoError(t, err)
	require.Len(t, exempt.GetFields(), 1)
	assert.Equal(t, name, exempt.GetFields()[0].GetName())

	ttl, err := a.admin.ListFields(
		ctx,
		&adminpb.ListFieldsRequest{Parent: dbRoot + "/collectionGroups/users", Filter: "ttlConfig:*"},
	)
	require.NoError(t, err)
	require.Len(t, ttl.GetFields(), 1)
	assert.Equal(t, adminpb.Field_TtlConfig_ACTIVE, ttl.GetFields()[0].GetTtlConfig().GetState())

	_, err = a.admin.ListFields(ctx, &adminpb.ListFieldsRequest{Parent: dbRoot + "/collectionGroups/users"})
	assert.Equal(t, codes.InvalidArgument, code(err))
}

func TestAdminExportImport(t *testing.T) {
	t.Parallel()

	a := newAdminEnv(t)
	ctx := t.Context()
	a.seed(t, "keep", map[string]map[string]any{"a": {"v": 1}, "b": {"v": 2}})
	a.seed(t, "skip", map[string]map[string]any{"c": {"v": 3}})

	_, err := a.admin.ExportDocuments(ctx, &adminpb.ExportDocumentsRequest{Name: dbRoot, OutputUriPrefix: "not-a-uri"})
	assert.Equal(t, codes.InvalidArgument, code(err))

	op, err := a.admin.ExportDocuments(ctx, &adminpb.ExportDocumentsRequest{
		Name: dbRoot, OutputUriPrefix: "gs://bucket/backups", CollectionIds: []string{"keep"},
	})
	require.NoError(t, err)
	require.True(t, op.GetDone())

	var resp adminpb.ExportDocumentsResponse
	require.NoError(t, op.GetResponse().UnmarshalTo(&resp))
	assert.Contains(t, resp.GetOutputUriPrefix(), "gs://bucket/backups/")
	assert.Contains(t, resp.GetOutputUriPrefix(), ".overall_export_metadata")

	for _, id := range []string{"a", "b"} {
		_, derr := a.client.Collection("keep").Doc(id).Delete(ctx)
		require.NoError(t, derr)
	}

	_, err = a.admin.ImportDocuments(
		ctx,
		&adminpb.ImportDocumentsRequest{Name: dbRoot, InputUriPrefix: "gs://bucket/none"},
	)
	assert.Equal(t, codes.NotFound, code(err))

	_, err = a.admin.ImportDocuments(
		ctx,
		&adminpb.ImportDocumentsRequest{Name: dbRoot, InputUriPrefix: resp.GetOutputUriPrefix()},
	)
	require.NoError(t, err)

	snap, err := a.client.Collection("keep").Doc("b").Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), snap.Data()["v"])

	_, err = a.client.Collection("skip").Doc("c").Get(ctx)
	require.NoError(t, err, "collections outside the export are untouched")
}

func TestAdminBulkDelete(t *testing.T) {
	t.Parallel()

	a := newAdminEnv(t)
	a.seed(t, "gone", map[string]map[string]any{"a": {"v": 1}, "b": {"v": 2}})
	a.seed(t, "stay", map[string]map[string]any{"c": {"v": 3}})

	op, err := a.admin.BulkDeleteDocuments(
		t.Context(),
		&adminpb.BulkDeleteDocumentsRequest{Name: dbRoot, CollectionIds: []string{"gone"}},
	)
	require.NoError(t, err)
	assert.True(t, op.GetDone())

	assert.Empty(t, ids(t, a.client.Collection("gone").Documents(t.Context())))
	assert.Equal(t, []string{"c"}, ids(t, a.client.Collection("stay").Documents(t.Context())))
}

func TestOperationsService(t *testing.T) {
	t.Parallel()

	a := newAdminEnv(t)
	ctx := t.Context()
	op := a.createDB(t, "opsdb", false)

	got, err := a.ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: op.GetName()})
	require.NoError(t, err)
	assert.True(t, got.GetDone())

	waited, err := a.ops.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{Name: op.GetName()})
	require.NoError(t, err)
	assert.Equal(t, op.GetName(), waited.GetName())

	list, err := a.ops.ListOperations(ctx, &longrunningpb.ListOperationsRequest{Name: projRoot + "/databases/opsdb"})
	require.NoError(t, err)
	assert.Len(t, list.GetOperations(), 1)

	_, err = a.ops.CancelOperation(ctx, &longrunningpb.CancelOperationRequest{Name: op.GetName()})
	require.NoError(t, err)

	_, err = a.ops.DeleteOperation(ctx, &longrunningpb.DeleteOperationRequest{Name: op.GetName()})
	require.NoError(t, err)

	_, err = a.ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: op.GetName()})
	assert.Equal(t, codes.NotFound, code(err))
}

func TestAdminStateSurvivesRestore(t *testing.T) {
	t.Parallel()

	a := newAdminEnv(t)
	ctx := t.Context()
	a.createDB(t, "kept", true)

	idx := &adminpb.Index{QueryScope: adminpb.Index_COLLECTION, Fields: []*adminpb.Index_IndexField{
		{FieldPath: "a", ValueMode: &adminpb.Index_IndexField_Order_{Order: adminpb.Index_IndexField_ASCENDING}},
		{FieldPath: "b", ValueMode: &adminpb.Index_IndexField_Order_{Order: adminpb.Index_IndexField_DESCENDING}},
	}}
	_, err := a.admin.CreateIndex(ctx, &adminpb.CreateIndexRequest{Parent: dbRoot + "/collectionGroups/c", Index: idx})
	require.NoError(t, err)

	a.seed(t, "d", map[string]map[string]any{"x": {"v": 1}})
	_, err = a.admin.ExportDocuments(ctx, &adminpb.ExportDocumentsRequest{Name: dbRoot, OutputUriPrefix: "gs://b/p"})
	require.NoError(t, err)

	b := newAdminEnv(t)
	require.NoError(t, b.handler.Backend.Restore(ctx, a.handler.Backend.Snapshot(ctx)))

	db, err := b.admin.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: projRoot + "/databases/kept"})
	require.NoError(t, err)
	assert.Equal(t, adminpb.Database_DELETE_PROTECTION_ENABLED, db.GetDeleteProtectionState())

	list, err := b.admin.ListIndexes(ctx, &adminpb.ListIndexesRequest{Parent: dbRoot + "/collectionGroups/c"})
	require.NoError(t, err)
	assert.Len(t, list.GetIndexes(), 1)

	ops, err := b.ops.ListOperations(ctx, &longrunningpb.ListOperationsRequest{Name: dbRoot})
	require.NoError(t, err)
	assert.NotEmpty(t, ops.GetOperations())
}

func commitOf(db string, w ...*firestorepb.Write) *firestorepb.CommitRequest {
	return &firestorepb.CommitRequest{Database: db, Writes: w}
}

func getDoc(name string) *firestorepb.GetDocumentRequest {
	return &firestorepb.GetDocumentRequest{Name: name}
}
