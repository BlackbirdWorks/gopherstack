package odatatable_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/odatatable"
)

func queryRowKeys(t *testing.T, b *odatatable.InMemoryBackend) []string {
	t.Helper()

	got, err := b.QueryEntities("t", nil, 0)
	require.NoError(t, err)

	keys := make([]string, 0, len(got))
	for _, e := range got {
		keys = append(keys, e.PartitionKey+"/"+e.RowKey)
	}

	return keys
}

func TestQueryEntities_OrderCacheInvalidation(t *testing.T) {
	t.Parallel()

	props := map[string]odatatable.EntityProperty{"A": {Type: odatatable.EdmInt32, Value: int32(1)}}

	tests := []struct {
		mutate func(t *testing.T, b *odatatable.InMemoryBackend)
		name   string
		want   []string
	}{
		{
			name: "insert",
			want: []string{"a/1", "b/2", "c/3"},
			mutate: func(t *testing.T, b *odatatable.InMemoryBackend) {
				t.Helper()

				_, err := b.InsertEntity("t", "a", "1", props)
				require.NoError(t, err)
			},
		},
		{
			name: "replace_upsert",
			want: []string{"a/1", "b/2", "c/3"},
			mutate: func(t *testing.T, b *odatatable.InMemoryBackend) {
				t.Helper()

				_, err := b.ReplaceEntity("t", "a", "1", props, "")
				require.NoError(t, err)
			},
		},
		{
			name: "merge_upsert",
			want: []string{"a/1", "b/2", "c/3"},
			mutate: func(t *testing.T, b *odatatable.InMemoryBackend) {
				t.Helper()

				_, err := b.MergeEntity("t", "a", "1", props, "")
				require.NoError(t, err)
			},
		},
		{name: "delete", want: []string{"c/3"}, mutate: func(t *testing.T, b *odatatable.InMemoryBackend) {
			t.Helper()

			require.NoError(t, b.DeleteEntity("t", "b", "2", odatatable.IfMatchAny))
		}},
		{
			name: "replace_existing",
			want: []string{"b/2", "c/3"},
			mutate: func(t *testing.T, b *odatatable.InMemoryBackend) {
				t.Helper()

				_, err := b.ReplaceEntity("t", "b", "2", props, "")
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			t.Parallel()

			b := odatatable.NewInMemoryBackend("test", testSnapshotVersion)
			odatatable.SetNowFunc(b, func() time.Time { return time.Unix(1, 0) })
			require.NoError(t, b.CreateTable("t"))

			for _, k := range [][2]string{{"c", "3"}, {"b", "2"}} {
				_, err := b.InsertEntity("t", k[0], k[1], props)
				require.NoError(t, err)
			}

			assert.Equal(t, []string{"b/2", "c/3"}, queryRowKeys(t, b))
			tt.mutate(t, b)
			assert.Equal(t, tt.want, queryRowKeys(t, b))
		})
	}
}

func TestEtagFor_MatchesQueryEscapedLayout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ts   time.Time
		want string
	}{
		{
			name: "utc",
			ts:   time.Date(2024, 1, 2, 3, 4, 5, 123456700, time.UTC),
			want: `W/"datetime'2024-01-02T03%3A04%3A05.1234567Z'"`,
		},
		{
			name: "zero_frac",
			ts:   time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
			want: `W/"datetime'2024-12-31T23%3A59%3A59.0000000Z'"`,
		},
		{
			name: "offset_zone",
			ts:   time.Date(2024, 1, 2, 3, 4, 5, 0, time.FixedZone("x", 3600)),
			want: `W/"datetime'2024-01-02T02%3A04%3A05.0000000Z'"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()

			t.Parallel()

			assert.Equal(t, tt.want, odatatable.EtagFor(tt.ts))
		})
	}
}
