package gcpdatastore_test

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newLooseEnv(t *testing.T) *env {
	t.Helper()

	e := newEnv(t)
	e.handler.Backend.SetEnforceIndexes(false)
	e.seed(t, "Task", seedTasks())

	return e
}

func (e *env) keys(t *testing.T, q *datastore.Query, keysOnly bool) []*datastore.Key {
	t.Helper()

	var dst []task
	if keysOnly {
		keys, err := e.client.GetAll(t.Context(), q, nil)
		require.NoError(t, err)

		return keys
	}

	keys, err := e.client.GetAll(t.Context(), q, &dst)
	require.NoError(t, err)

	return keys
}

func TestQueryFilters(t *testing.T) {
	t.Parallel()

	prop := func(n, op string, v any) datastore.PropertyFilter {
		return datastore.PropertyFilter{FieldName: n, Operator: op, Value: v}
	}

	tests := []struct {
		query *datastore.Query
		name  string
		want  []string
	}{
		{datastore.NewQuery("Task"), "all by key", []string{"a", "b", "c", "d", "e"}},
		{datastore.NewQuery("Task").FilterField("Done", "=", true), "equality", []string{"a", "c", "e"}},
		{datastore.NewQuery("Task").FilterField("Priority", ">", 2), "greater than", []string{"c", "d", "e"}},
		{datastore.NewQuery("Task").FilterField("Priority", "<=", 2), "less or equal", []string{"a", "b"}},
		{datastore.NewQuery("Task").FilterField("Priority", "!=", 3), "not equal", []string{"a", "b", "d", "e"}},
		{datastore.NewQuery("Task").FilterField("Priority", "in", []any{1, 5}), "in", []string{"a", "e"}},
		{datastore.NewQuery("Task").FilterField("Priority", "not-in", []any{1, 2}), "not in", []string{"c", "d", "e"}},
		{datastore.NewQuery("Task").FilterField("Tags", "=", "y"), "array membership", []string{"a", "b"}},
		{datastore.NewQuery("Task").FilterField("Score", ">", 3.0), "double range", []string{"c", "d", "e"}},
		{datastore.NewQuery("Task").FilterField("Priority", ">", 1.5), "int and double never mix", nil},
		{datastore.NewQuery("Task").FilterField("Note", "=", "first"), "unindexed property matches nothing", nil},
		{datastore.NewQuery("Task").FilterField("Missing", "=", "x"), "missing property", nil},
		{
			datastore.NewQuery("Task").FilterEntity(datastore.OrFilter{Filters: []datastore.EntityFilter{
				prop("Priority", "<", 2), prop("Priority", ">", 4),
			}}),
			"or on one property", []string{"a", "e"},
		},
		{
			datastore.NewQuery("Task").FilterEntity(datastore.OrFilter{Filters: []datastore.EntityFilter{
				prop("Done", "=", false), prop("Tags", "=", "z"),
			}}),
			"or across properties", []string{"b", "c", "d"},
		},
		{
			datastore.NewQuery("Task").FilterEntity(datastore.AndFilter{Filters: []datastore.EntityFilter{
				prop("Done", "=", true), prop("Priority", ">", 1),
			}}),
			"and equality and range", []string{"c", "e"},
		},
		{datastore.NewQuery("Task").Order("-Priority"), "descending order", []string{"e", "d", "c", "b", "a"}},
		{datastore.NewQuery("Task").Order("-Priority").Limit(2), "limit", []string{"e", "d"}},
		{datastore.NewQuery("Task").Offset(1).Limit(2), "offset and limit", []string{"b", "c"}},
		{datastore.NewQuery("Task").Order("-Priority").Offset(3), "offset past most", []string{"b", "a"}},
		{
			datastore.NewQuery("Task").Order("Priority").Order("-Score"),
			"multiple orders",
			[]string{"a", "b", "c", "d", "e"},
		},
		{datastore.NewQuery("Task").EventualConsistency(), "eventual", []string{"a", "b", "c", "d", "e"}},
		{datastore.NewQuery("Task").KeysOnly().FilterField("Done", "=", true), "keys only", []string{"a", "c", "e"}},
		{datastore.NewQuery("Other"), "other kind", nil},
		{datastore.NewQuery("Task").Order("Missing"), "order on missing property excludes", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newLooseEnv(t)

			keys := e.keys(t, tt.query, tt.name == "keys only")
			assert.ElementsMatch(t, tt.want, names(keys))
		})
	}
}

func TestQueryOrderExact(t *testing.T) {
	t.Parallel()

	tests := []struct {
		query *datastore.Query
		name  string
		want  []string
	}{
		{datastore.NewQuery("Task").Order("-Priority"), "priority desc", []string{"e", "d", "c", "b", "a"}},
		{datastore.NewQuery("Task").Order("Score"), "score asc", []string{"a", "b", "c", "d", "e"}},
		{
			datastore.NewQuery("Task").FilterField("Priority", ">", 1).Order("Priority"),
			"range ordered",
			[]string{"b", "c", "d", "e"},
		},
		{datastore.NewQuery("Task").Order("-__key__"), "key desc", []string{"e", "d", "c", "b", "a"}},
		{datastore.NewQuery("Task").Offset(1).Limit(2).Order("-Priority"), "offset limit desc", []string{"d", "c"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newLooseEnv(t)

			keys := e.keys(t, tt.query, false)
			assert.Equal(t, tt.want, names(keys))
		})
	}
}

func TestQueryAncestorAndKindless(t *testing.T) {
	t.Parallel()

	e := newLooseEnv(t)
	parent := datastore.NameKey("List", "l1", nil)

	for _, n := range []string{"x", "y"} {
		_, err := e.client.Put(t.Context(), datastore.NameKey("Item", n, parent), &task{Name: n, Priority: 1})
		require.NoError(t, err)
	}

	_, err := e.client.Put(
		t.Context(),
		datastore.NameKey("Item", "z", datastore.NameKey("List", "l2", nil)),
		&task{Name: "z"},
	)
	require.NoError(t, err)

	keys := e.keys(t, datastore.NewQuery("Item").Ancestor(parent), false)
	assert.Equal(t, []string{"x", "y"}, names(keys))

	keys, err = e.client.GetAll(t.Context(), datastore.NewQuery("").Ancestor(parent).KeysOnly(), nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"x", "y"}, names(keys))

	keys = e.keys(t, datastore.NewQuery("Item").Ancestor(parent).FilterField("Priority", "=", 1), false)
	assert.Equal(t, []string{"x", "y"}, names(keys))

	keys = e.keys(
		t,
		datastore.NewQuery("Item").FilterField("__key__", ">", datastore.NameKey("Item", "x", parent)),
		false,
	)
	assert.ElementsMatch(t, []string{"y", "z"}, names(keys))

	_, err = e.client.GetAll(t.Context(), datastore.NewQuery("").FilterField("Priority", "=", 1).KeysOnly(), nil)
	assert.Error(t, err)
}

func TestQueryProjection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		query *datastore.Query
		name  string
		want  []string
	}{
		{datastore.NewQuery("Task").Project("Priority").Order("Priority"), "scalar", []string{"1", "2", "3", "4", "5"}},
		{
			datastore.NewQuery("Task").Project("Tags").Order("Tags"),
			"array expands per element",
			[]string{"x", "x", "y", "y", "z"},
		},
		{
			datastore.NewQuery("Task").Project("Tags").Order("Tags").Distinct(),
			"array distinct",
			[]string{"x", "y", "z"},
		},
		{datastore.NewQuery("Task").Project("Done").DistinctOn("Done"), "distinct on", []string{"true", "false"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newLooseEnv(t)

			var got []task

			_, err := e.client.GetAll(t.Context(), tt.query, &got)
			require.NoError(t, err)

			vals := make([]string, 0, len(got))
			for _, g := range got {
				switch {
				case len(g.Tags) > 0:
					vals = append(vals, g.Tags[0])
				case tt.name == "scalar":
					vals = append(vals, strconv.Itoa(g.Priority))
				default:
					vals = append(vals, strconv.FormatBool(g.Done))
				}
			}

			if tt.name == "distinct on" {
				assert.ElementsMatch(t, tt.want, vals)
			} else {
				assert.Equal(t, tt.want, vals)
			}
		})
	}
}

func TestQueryCursors(t *testing.T) {
	t.Parallel()

	t.Run("page by cursor", func(t *testing.T) {
		t.Parallel()

		e := newLooseEnv(t)

		var (
			all    []string
			cursor datastore.Cursor
			have   bool
		)

		for range 10 {
			q := datastore.NewQuery("Task").Order("-Priority").Limit(2)
			if have {
				q = q.Start(cursor)
			}

			it := e.client.Run(t.Context(), q)
			n := 0

			for {
				var tk task

				_, err := it.Next(&tk)
				if errors.Is(err, iterator.Done) {
					break
				}

				require.NoError(t, err)

				all = append(all, tk.Name)
				n++
			}

			next, err := it.Cursor()
			require.NoError(t, err)

			if n == 0 {
				break
			}

			cursor, have = next, true
		}

		assert.Equal(t, []string{"e", "d", "c", "b", "a"}, all)
	})

	t.Run("end cursor bounds the page", func(t *testing.T) {
		t.Parallel()

		e := newLooseEnv(t)
		it := e.client.Run(t.Context(), datastore.NewQuery("Task").Limit(3))

		var tk task
		for {
			if _, err := it.Next(&tk); err != nil {
				break
			}
		}

		end, err := it.Cursor()
		require.NoError(t, err)

		keys := e.keys(t, datastore.NewQuery("Task").End(end), false)
		assert.Equal(t, []string{"a", "b", "c"}, names(keys))
	})

	t.Run("large result set spans batches", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		e.handler.Backend.SetEnforceIndexes(false)

		keys := make([]*datastore.Key, 650)
		vals := make([]task, 650)

		for i := range keys {
			keys[i] = datastore.NameKey("Big", fmt.Sprintf("k%04d", i), nil)
			vals[i] = task{Priority: i}
		}

		_, err := e.client.PutMulti(t.Context(), keys, vals)
		require.NoError(t, err)

		got, err := e.client.GetAll(t.Context(), datastore.NewQuery("Big").KeysOnly(), nil)
		require.NoError(t, err)
		require.Len(t, got, 650)
		assert.Equal(t, "k0000", got[0].Name)
		assert.Equal(t, "k0649", got[649].Name)

		got, err = e.client.GetAll(t.Context(), datastore.NewQuery("Big").KeysOnly().Offset(310).Limit(5), nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"k0310", "k0311", "k0312", "k0313", "k0314"}, names(got))
	})

	t.Run("invalid cursor", func(t *testing.T) {
		t.Parallel()

		e := newLooseEnv(t)
		_, err := e.raw.RunQuery(t.Context(), &datastorepb.RunQueryRequest{
			ProjectId: project,
			QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
				Kind:        []*datastorepb.KindExpression{{Name: "Task"}},
				StartCursor: []byte("zzz"),
			}},
		})
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})
}

func TestMetadataQueries(t *testing.T) {
	t.Parallel()

	e := newLooseEnv(t)

	_, err := e.client.Put(t.Context(), datastore.NameKey("Other", "o", nil), &task{})
	require.NoError(t, err)

	keys, err := e.client.GetAll(t.Context(), datastore.NewQuery("__kind__").KeysOnly(), nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"Other", "Task"}, names(keys))

	nk := datastore.NameKey("Task", "ns", nil)
	nk.Namespace = "tenant"
	_, err = e.client.Put(t.Context(), nk, &task{})
	require.NoError(t, err)

	keys, err = e.client.GetAll(t.Context(), datastore.NewQuery("__namespace__").KeysOnly(), nil)
	require.NoError(t, err)
	assert.Len(t, keys, 2)
}
