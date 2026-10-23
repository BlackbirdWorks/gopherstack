package odatatable_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/odatatable"
)

func strProps(v string) map[string]odatatable.EntityProperty {
	return map[string]odatatable.EntityProperty{"V": {Type: odatatable.EdmString, Value: v}}
}

func TestInMemoryBackend_ExecuteBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		check     func(t *testing.T, b *odatatable.InMemoryBackend)
		name      string
		ops       []odatatable.BatchOp
		wantIndex int
	}{
		{
			name: "all operations commit",
			ops: []odatatable.BatchOp{
				{Kind: odatatable.BatchInsert, PartitionKey: "p", RowKey: "new", Props: strProps("n")},
				{
					Kind:         odatatable.BatchReplace,
					PartitionKey: "p",
					RowKey:       "rep",
					Props:        strProps("r"),
					IfMatch:      odatatable.IfMatchAny,
				},
				{
					Kind:         odatatable.BatchMerge,
					PartitionKey: "p",
					RowKey:       "mrg",
					Props: map[string]odatatable.EntityProperty{
						"W": {Type: odatatable.EdmString, Value: "w"},
					},
				},
				{Kind: odatatable.BatchDelete, PartitionKey: "p", RowKey: "del", IfMatch: odatatable.IfMatchAny},
			},
			check: func(t *testing.T, b *odatatable.InMemoryBackend) {
				t.Helper()

				_, err := b.GetEntity("t", "p", "new")
				require.NoError(t, err)

				_, err = b.GetEntity("t", "p", "del")
				require.ErrorIs(t, err, odatatable.ErrEntityNotFound)

				rep, err := b.GetEntity("t", "p", "rep")
				require.NoError(t, err)
				assert.NotContains(t, rep.Properties, "Old")

				mrg, err := b.GetEntity("t", "p", "mrg")
				require.NoError(t, err)
				assert.Contains(t, mrg.Properties, "Old")
				assert.Contains(t, mrg.Properties, "W")
			},
		},
		{
			name: "failure rolls everything back",
			ops: []odatatable.BatchOp{
				{Kind: odatatable.BatchInsert, PartitionKey: "p", RowKey: "new", Props: strProps("n")},
				{Kind: odatatable.BatchDelete, PartitionKey: "p", RowKey: "del", IfMatch: odatatable.IfMatchAny},
				{
					Kind:         odatatable.BatchReplace,
					PartitionKey: "p",
					RowKey:       "rep",
					Props:        strProps("r"),
					IfMatch:      "bad-etag",
				},
			},
			wantErr: odatatable.ErrETagMismatch, wantIndex: 2,
			check: func(t *testing.T, b *odatatable.InMemoryBackend) {
				t.Helper()

				_, err := b.GetEntity("t", "p", "new")
				require.ErrorIs(t, err, odatatable.ErrEntityNotFound)

				_, err = b.GetEntity("t", "p", "del")
				require.NoError(t, err)

				rep, err := b.GetEntity("t", "p", "rep")
				require.NoError(t, err)
				assert.Contains(t, rep.Properties, "Old")
			},
		},
		{
			name: "insert of an existing entity",
			ops: []odatatable.BatchOp{
				{Kind: odatatable.BatchInsert, PartitionKey: "p", RowKey: "rep", Props: strProps("n")},
			},
			wantErr: odatatable.ErrEntityAlreadyExists,
		},
		{
			name: "upsert creates a missing entity",
			ops: []odatatable.BatchOp{
				{Kind: odatatable.BatchReplace, PartitionKey: "p", RowKey: "ghost", Props: strProps("g")},
			},
		},
		{
			name: "conditional update of a missing entity",
			ops: []odatatable.BatchOp{
				{
					Kind:         odatatable.BatchMerge,
					PartitionKey: "p",
					RowKey:       "ghost",
					Props:        strProps("g"),
					IfMatch:      odatatable.IfMatchAny,
				},
			},
			wantErr: odatatable.ErrEntityNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := odatatable.NewInMemoryBackend("test", testSnapshotVersion)
			require.NoError(t, b.CreateTable("t"))

			old := map[string]odatatable.EntityProperty{"Old": {Type: odatatable.EdmString, Value: "o"}}
			for _, rk := range []string{"rep", "mrg", "del"} {
				_, err := b.InsertEntity("t", "p", rk, old)
				require.NoError(t, err)
			}

			infos, err := b.ExecuteBatch("t", tt.ops)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				var be *odatatable.BatchError

				require.ErrorAs(t, err, &be)
				assert.Equal(t, tt.wantIndex, be.Index)
			} else {
				require.NoError(t, err)
				assert.Len(t, infos, len(tt.ops))
			}

			if tt.check != nil {
				tt.check(t, b)
			}
		})
	}

	_, err := odatatable.NewInMemoryBackend("test", testSnapshotVersion).ExecuteBatch("none", nil)
	require.ErrorIs(t, err, odatatable.ErrTableNotFound)
}
