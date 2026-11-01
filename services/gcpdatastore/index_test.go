package gcpdatastore_test

import (
	"testing"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/admin/apiv1/adminpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func prop(name string, desc bool) *adminpb.Index_IndexedProperty {
	d := adminpb.Index_ASCENDING
	if desc {
		d = adminpb.Index_DESCENDING
	}

	return &adminpb.Index_IndexedProperty{Name: name, Direction: d}
}

func (e *env) createIndex(
	t *testing.T,
	kind string,
	anc adminpb.Index_AncestorMode,
	props ...*adminpb.Index_IndexedProperty,
) *adminpb.Index {
	t.Helper()

	op, err := e.admin.CreateIndex(t.Context(), &adminpb.CreateIndexRequest{
		ProjectId: project, Index: &adminpb.Index{Kind: kind, Ancestor: anc, Properties: props},
	})
	require.NoError(t, err)
	require.True(t, op.GetDone())

	idx := &adminpb.Index{}
	require.NoError(t, op.GetResponse().UnmarshalTo(idx))

	return idx
}

func TestCompositeIndexEnforcement(t *testing.T) {
	t.Parallel()

	parent := datastore.NameKey("List", "l", nil)

	tests := []struct {
		query  *datastore.Query
		name   string
		index  []*adminpb.Index_IndexedProperty
		anc    adminpb.Index_AncestorMode
		needed bool
	}{
		{datastore.NewQuery("Task").Order("Priority"), "single order", nil, 0, false},
		{datastore.NewQuery("Task").Order("-Priority"), "single desc order", nil, 0, false},
		{
			datastore.NewQuery("Task").FilterField("Done", "=", true).FilterField("Priority", "=", 1),
			"multiple equality",
			nil,
			0,
			false,
		},
		{
			datastore.NewQuery("Task").FilterField("Priority", ">", 1).Order("Priority"),
			"inequality sorted by itself",
			nil,
			0,
			false,
		},
		{
			datastore.NewQuery("Task").Ancestor(parent).FilterField("Done", "=", true),
			"ancestor and equality",
			nil,
			0,
			false,
		},
		{
			datastore.NewQuery("Task").FilterField("Done", "=", true).Order("Priority"),
			"equality and sort",
			[]*adminpb.Index_IndexedProperty{prop("Done", false), prop("Priority", false)}, adminpb.Index_NONE, true,
		},
		{
			datastore.NewQuery("Task").Order("Done").Order("-Priority"),
			"two sorts",
			[]*adminpb.Index_IndexedProperty{prop("Done", false), prop("Priority", true)}, adminpb.Index_NONE, true,
		},
		{
			datastore.NewQuery("Task").FilterField("Done", "=", true).FilterField("Priority", ">", 1),
			"equality and inequality",
			[]*adminpb.Index_IndexedProperty{prop("Done", false), prop("Priority", false)}, adminpb.Index_NONE, true,
		},
		{
			datastore.NewQuery("Task").Ancestor(parent).FilterField("Priority", ">", 1),
			"ancestor and inequality",
			[]*adminpb.Index_IndexedProperty{prop("Priority", false)}, adminpb.Index_ALL_ANCESTORS, true,
		},
		{
			datastore.NewQuery("Task").Order("-__key__"),
			"descending key",
			[]*adminpb.Index_IndexedProperty{prop("__key__", true)}, adminpb.Index_NONE, true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			_, err := e.client.GetAll(t.Context(), tt.query.KeysOnly(), nil)

			if !tt.needed {
				assert.NoError(t, err)

				return
			}

			require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
			assert.Contains(t, err.Error(), "no matching index found")
			assert.Contains(t, err.Error(), "kind: Task")

			idx := e.createIndex(t, "Task", tt.anc, tt.index...)
			assert.Equal(t, adminpb.Index_READY, idx.GetState())

			_, err = e.client.GetAll(t.Context(), tt.query.KeysOnly(), nil)
			require.NoError(t, err)

			_, err = e.admin.DeleteIndex(
				t.Context(),
				&adminpb.DeleteIndexRequest{ProjectId: project, IndexId: idx.GetIndexId()},
			)
			require.NoError(t, err)

			_, err = e.client.GetAll(t.Context(), tt.query.KeysOnly(), nil)
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		})
	}
}

func TestIndexAdmin(t *testing.T) {
	t.Parallel()

	t.Run("create get list delete", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		first := e.createIndex(t, "A", adminpb.Index_NONE, prop("x", false), prop("y", true))
		second := e.createIndex(t, "B", adminpb.Index_ALL_ANCESTORS, prop("z", false))
		assert.NotEqual(t, first.GetIndexId(), second.GetIndexId())

		got, err := e.admin.GetIndex(
			t.Context(),
			&adminpb.GetIndexRequest{ProjectId: project, IndexId: first.GetIndexId()},
		)
		require.NoError(t, err)
		assert.Equal(t, "A", got.GetKind())
		assert.Equal(t, adminpb.Index_DESCENDING, got.GetProperties()[1].GetDirection())

		page1, err := e.admin.ListIndexes(t.Context(), &adminpb.ListIndexesRequest{ProjectId: project, PageSize: 1})
		require.NoError(t, err)
		require.Len(t, page1.GetIndexes(), 1)
		require.NotEmpty(t, page1.GetNextPageToken())
		assert.Equal(t, first.GetIndexId(), page1.GetIndexes()[0].GetIndexId())

		page2, err := e.admin.ListIndexes(
			t.Context(),
			&adminpb.ListIndexesRequest{ProjectId: project, PageSize: 1, PageToken: page1.GetNextPageToken()},
		)
		require.NoError(t, err)
		require.Len(t, page2.GetIndexes(), 1)
		assert.Empty(t, page2.GetNextPageToken())

		op, err := e.admin.DeleteIndex(
			t.Context(),
			&adminpb.DeleteIndexRequest{ProjectId: project, IndexId: first.GetIndexId()},
		)
		require.NoError(t, err)
		assert.True(t, op.GetDone())

		_, err = e.admin.GetIndex(
			t.Context(),
			&adminpb.GetIndexRequest{ProjectId: project, IndexId: first.GetIndexId()},
		)
		assert.Equal(t, codes.NotFound, status.Code(err))

		ops := longrunningpb.NewOperationsClient(e.conn)
		got2, err := ops.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: op.GetName()})
		require.NoError(t, err)
		assert.True(t, got2.GetDone())
	})

	t.Run("invalid and duplicate", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		e.createIndex(t, "A", adminpb.Index_NONE, prop("x", false), prop("y", false))

		tests := []struct {
			idx  *adminpb.Index
			name string
			code codes.Code
		}{
			{
				&adminpb.Index{
					Kind:       "A",
					Ancestor:   adminpb.Index_NONE,
					Properties: []*adminpb.Index_IndexedProperty{prop("x", false), prop("y", false)},
				},
				"duplicate",
				codes.AlreadyExists,
			},
			{
				&adminpb.Index{
					Ancestor:   adminpb.Index_NONE,
					Properties: []*adminpb.Index_IndexedProperty{prop("x", false)},
				},
				"no kind",
				codes.InvalidArgument,
			},
			{
				&adminpb.Index{Kind: "A", Properties: []*adminpb.Index_IndexedProperty{prop("x", false)}},
				"no ancestor mode",
				codes.InvalidArgument,
			},
			{&adminpb.Index{Kind: "A", Ancestor: adminpb.Index_NONE}, "no properties", codes.InvalidArgument},
			{
				&adminpb.Index{
					Kind:       "A",
					Ancestor:   adminpb.Index_NONE,
					Properties: []*adminpb.Index_IndexedProperty{{Name: "x"}},
				},
				"no direction",
				codes.InvalidArgument,
			},
		}

		for _, tt := range tests {
			_, err := e.admin.CreateIndex(t.Context(), &adminpb.CreateIndexRequest{ProjectId: project, Index: tt.idx})
			assert.Equal(t, tt.code, status.Code(err), tt.name)
		}

		_, err := e.admin.DeleteIndex(t.Context(), &adminpb.DeleteIndexRequest{ProjectId: project, IndexId: "999"})
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("enforcement can be switched off", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		e.handler.Backend.SetEnforceIndexes(false)

		_, err := e.client.GetAll(t.Context(), datastore.NewQuery("Task").Order("A").Order("B").KeysOnly(), nil)
		assert.NoError(t, err)
	})
}
