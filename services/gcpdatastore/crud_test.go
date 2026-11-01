package gcpdatastore_test

import (
	"strings"
	"testing"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, e *env)
		name string
	}{
		{func(t *testing.T, e *env) {
			t.Helper()

			k := datastore.NameKey("Task", "one", nil)
			_, err := e.client.Put(t.Context(), k, &task{Name: "one", Priority: 7, Tags: []string{"p", "q"}, Note: "n"})
			require.NoError(t, err)

			var got task
			require.NoError(t, e.client.Get(t.Context(), k, &got))
			assert.Equal(t, task{Name: "one", Priority: 7, Tags: []string{"p", "q"}, Note: "n"}, got)

			require.NoError(t, e.client.Delete(t.Context(), k))
			assert.ErrorIs(t, e.client.Get(t.Context(), k, &got), datastore.ErrNoSuchEntity)
		}, "put get delete"},
		{func(t *testing.T, e *env) {
			t.Helper()

			k, err := e.client.Put(t.Context(), datastore.IncompleteKey("Task", nil), &task{Name: "auto"})
			require.NoError(t, err)
			assert.NotZero(t, k.ID)

			var got task
			require.NoError(t, e.client.Get(t.Context(), k, &got))
			assert.Equal(t, "auto", got.Name)
		}, "incomplete key allocates id"},
		{func(t *testing.T, e *env) {
			t.Helper()

			e.seed(t, "Task", map[string]task{"a": {}, "b": {}})

			keys := []*datastore.Key{
				datastore.NameKey(
					"Task",
					"a",
					nil,
				),
				datastore.NameKey("Task", "nope", nil),
				datastore.NameKey("Task", "b", nil),
			}
			dst := make([]task, 3)
			err := e.client.GetMulti(t.Context(), keys, dst)

			var me datastore.MultiError
			require.ErrorAs(t, err, &me)
			require.NoError(t, me[0])
			require.ErrorIs(t, me[1], datastore.ErrNoSuchEntity)
			require.NoError(t, me[2])
			assert.Equal(t, "b", dst[2].Name)
		}, "getmulti reports missing per key"},
		{func(t *testing.T, e *env) {
			t.Helper()

			keys := []*datastore.Key{datastore.NameKey("Task", "a", nil), datastore.NameKey("Task", "b", nil)}
			_, err := e.client.PutMulti(t.Context(), keys, []task{{Name: "a"}, {Name: "b"}})
			require.NoError(t, err)
			require.NoError(t, e.client.DeleteMulti(t.Context(), keys))

			n, err := e.client.Count(t.Context(), datastore.NewQuery("Task"))
			require.NoError(t, err)
			assert.Zero(t, n)
		}, "putmulti and deletemulti"},
		{func(t *testing.T, e *env) {
			t.Helper()

			parent := datastore.NameKey("List", "l1", nil)
			child := datastore.NameKey("Task", "c", parent)
			nsKey := datastore.NameKey("Task", "c", nil)
			nsKey.Namespace = "other"

			_, err := e.client.Put(t.Context(), child, &task{Name: "default-ns"})
			require.NoError(t, err)
			_, err = e.client.Put(t.Context(), nsKey, &task{Name: "other-ns"})
			require.NoError(t, err)

			var a, b task
			require.NoError(t, e.client.Get(t.Context(), child, &a))
			require.NoError(t, e.client.Get(t.Context(), nsKey, &b))
			assert.Equal(t, "default-ns", a.Name)
			assert.Equal(t, "other-ns", b.Name)
		}, "ancestor paths and namespaces are separate"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newEnv(t))
		})
	}
}

func TestInvalidRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		key  *datastore.Key
		name string
		code codes.Code
	}{
		{datastore.NameKey("__bad__", "x", nil), "reserved kind", codes.InvalidArgument},
		{datastore.NameKey("Task", "__bad__", nil), "reserved name", codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			_, err := e.client.Put(t.Context(), tt.key, &task{})
			assert.Equal(t, tt.code, status.Code(err))
		})
	}

	t.Run("oversized indexed string", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		_, err := e.raw.Commit(t.Context(), &datastorepb.CommitRequest{
			ProjectId: project, Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL,
			Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: &datastorepb.Entity{
				Key: &datastorepb.Key{
					Path: []*datastorepb.Key_PathElement{
						{Kind: "Task", IdType: &datastorepb.Key_PathElement_Name{Name: "x"}},
					},
				},
				Properties: map[string]*datastorepb.Value{"s": {
					ValueType: &datastorepb.Value_StringValue{StringValue: strings.Repeat("x", 1600)},
				}},
			}}}},
		})
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})
}
