package cosmosdb_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func seedQueryBackend(t *testing.T) *cosmosdb.InMemoryBackend {
	t.Helper()

	b := cosmosdb.NewInMemoryBackend()
	_, err := b.CreateDatabase("db")
	require.NoError(t, err)
	_, err = b.CreateContainer("db", cosmosdb.ContainerSpec{ID: "c", PartitionKeyPath: "/pk"})
	require.NoError(t, err)

	for _, raw := range []string{
		`{"id":"b","pk":"x","n":2,"name":"two","nested":{"a":[1,2,{"z":true}]}}`,
		`{"id":"a","pk":"y","n":10,"name":"ten"}`,
		`{"id":"c","pk":"x","n":1}`,
		`{"id":"a","pk":"x","n":5,"name":"five"}`,
	} {
		_, err = b.CreateDocument("db", "c", decodeJSONBody(t, raw), false)
		require.NoError(t, err)
	}

	return b
}

func TestQueryDocuments_MatchesExecuteQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		query  string
		params []cosmosdb.QueryParameter
	}{
		{name: "star", query: "SELECT * FROM c"},
		{name: "where_num", query: "SELECT * FROM c WHERE c.n > 1"},
		{
			name:   "where_param",
			query:  "SELECT * FROM c WHERE c.name = @n",
			params: []cosmosdb.QueryParameter{{Name: "@n", Value: "ten"}},
		},
		{name: "projection", query: "SELECT c.id, c.n AS num, c.nested.a FROM c"},
		{name: "order_desc", query: "SELECT c.id FROM c ORDER BY c.n DESC"},
		{name: "order_two_keys", query: "SELECT c.id, c.pk FROM c ORDER BY c.pk, c.n DESC"},
		{name: "top_offset", query: "SELECT * FROM c ORDER BY c.n OFFSET 1 LIMIT 2"},
		{name: "top", query: "SELECT TOP 2 * FROM c"},
		{name: "sys_ts", query: "SELECT c.id, c._ts FROM c WHERE c._ts > 0"},
		{name: "sys_rid", query: "SELECT c._rid, c._self, c._etag FROM c ORDER BY c._rid"},
		{name: "sys_where_etag", query: "SELECT c.id FROM c WHERE c._etag != 'x' AND c._attachments = 'attachments/'"},
		{name: "whole_row", query: "SELECT c FROM c WHERE c.n = 2"},
		{name: "is_null", query: "SELECT c.id FROM c WHERE c.name IS NULL"},
		{name: "not_missing", query: "SELECT c.id FROM c WHERE NOT (c.name = 'ten')"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			t.Parallel()

			b := seedQueryBackend(t)

			docs, err := b.ListDocuments("db", "c")
			require.NoError(t, err)

			want, err := cosmosdb.ExecuteQuery(tt.query, tt.params, docs)
			require.NoError(t, err)

			got, err := b.QueryDocuments("db", "c", tt.query, tt.params)
			require.NoError(t, err)

			assert.Equal(t, want, got)
		})
	}
}

func TestQueryDocuments_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		db      string
		coll    string
		query   string
	}{
		{name: "parse", db: "db", coll: "c", query: "SELEC nope", wantErr: cosmosdb.ErrQueryParse},
		{name: "no_db", db: "x", coll: "c", query: "SELECT * FROM c", wantErr: cosmosdb.ErrDatabaseNotFound},
		{name: "no_coll", db: "db", coll: "x", query: "SELECT * FROM c", wantErr: cosmosdb.ErrContainerNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			t.Parallel()

			_, err := seedQueryBackend(t).QueryDocuments(tt.db, tt.coll, tt.query, nil)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestQueryDocuments_SeesWrites(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(t *testing.T, b *cosmosdb.InMemoryBackend)
		name   string
		want   []string
	}{
		{
			name: "create",
			want: []string{"a", "a", "b", "c", "d"},
			mutate: func(t *testing.T, b *cosmosdb.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateDocument("db", "c", decodeJSONBody(t, `{"id":"d","pk":"x"}`), false)
				require.NoError(t, err)
			},
		},
		{name: "delete", want: []string{"a", "a", "c"}, mutate: func(t *testing.T, b *cosmosdb.InMemoryBackend) {
			t.Helper()

			require.NoError(t, b.DeleteDocument("db", "c", `"x"`, "b", ""))
		}},
		{
			name: "upsert_existing",
			want: []string{"a", "a", "b", "c"},
			mutate: func(t *testing.T, b *cosmosdb.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateDocument("db", "c", decodeJSONBody(t, `{"id":"b","pk":"x","n":99}`), true)
				require.NoError(t, err)
			},
		},
		{name: "replace", want: []string{"a", "a", "b", "c"}, mutate: func(t *testing.T, b *cosmosdb.InMemoryBackend) {
			t.Helper()

			_, err := b.ReplaceDocument("db", "c", `"x"`, "c", decodeJSONBody(t, `{"id":"c","pk":"x","n":99}`), "")
			require.NoError(t, err)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			t.Parallel()

			b := seedQueryBackend(t)
			ids := func() []string {
				rows, err := b.QueryDocuments("db", "c", "SELECT * FROM c", nil)
				require.NoError(t, err)

				out := make([]string, 0, len(rows))
				for _, r := range rows {
					out = append(out, r["id"].(string))
				}

				return out
			}

			require.Equal(t, []string{"a", "a", "b", "c"}, ids())
			tt.mutate(t, b)
			assert.Equal(t, tt.want, ids())

			if tt.name == "replace" || tt.name == "upsert_existing" {
				rows, err := b.QueryDocuments("db", "c", "SELECT * FROM c WHERE c.n = 99", nil)
				require.NoError(t, err)
				assert.Len(t, rows, 1)
			}
		})
	}
}

func TestListDocuments_BodyIsIsolated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "mutating_result_leaves_store"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			t.Parallel()

			b := seedQueryBackend(t)
			docs, err := b.ListDocuments("db", "c")
			require.NoError(t, err)

			for _, d := range docs {
				d.Body["n"] = "mutated"
				if nested, ok := d.Body["nested"].(map[string]any); ok {
					nested["a"] = nil
				}
			}

			again, err := b.ListDocuments("db", "c")
			require.NoError(t, err)

			for _, d := range again {
				assert.NotEqual(t, "mutated", d.Body["n"], tt.name)
			}

			got, err := b.GetDocument("db", "c", `"x"`, "b")
			require.NoError(t, err)
			assert.NotNil(t, got.Body["nested"].(map[string]any)["a"])
		})
	}
}

func TestEtagFor_FormatsHexNanos(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ts   time.Time
		name string
	}{
		{name: "epoch", ts: time.Unix(0, 0)},
		{name: "tiny", ts: time.Unix(0, 255)},
		{name: "now_like", ts: time.Date(2026, 10, 10, 1, 2, 3, 456, time.UTC)},
		{name: "pre_epoch", ts: time.Unix(-5, 0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			t.Parallel()

			assert.Equal(t, fmt.Sprintf("%q", fmt.Sprintf("%016x", tt.ts.UnixNano())), cosmosdb.EtagFor(tt.ts))
		})
	}
}
