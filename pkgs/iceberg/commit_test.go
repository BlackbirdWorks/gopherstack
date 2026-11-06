package iceberg_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
)

func TestPlanCommitRemovesAndDeletes(t *testing.T) {
	t.Parallel()

	now := time.UnixMilli(1_700_000_000_000)
	meta := iceberg.NewTableMetadata(iceberg.NewParams{
		Location: "s3://b/tbl", Schema: testSchema(t),
		Spec: iceberg.PartitionSpec{Fields: []iceberg.PartitionField{}}, Now: now,
	})

	store := map[string][]byte{}
	read := func(path string) ([]iceberg.ManifestEntry, error) { return iceberg.ReadManifest(store[path]) }
	file := func(name string) iceberg.DataFile {
		return iceberg.DataFile{Path: "s3://b/tbl/data/" + name, Format: "PARQUET", RecordCount: 4, FileSize: 40, Partition: map[string]any{}}
	}

	cur := meta
	loc := ""

	var list []iceberg.ManifestFile

	step := func(ch iceberg.Change, at time.Duration) *iceberg.AppendPlan {
		plan, err := cur.PlanCommit(loc, list, read, ch, now.Add(at))
		require.NoError(t, err)

		for _, o := range plan.Objects {
			store[o.Location] = o.Body
		}

		snap, _ := plan.Metadata.CurrentSnapshot()
		list, err = iceberg.ReadManifestList(store[snap.ManifestList])
		require.NoError(t, err)

		cur, loc = plan.Metadata, plan.MetadataLocation

		return plan
	}

	step(iceberg.Change{Operation: iceberg.OpAppend, AddData: []iceberg.DataFile{file("a"), file("b")}}, time.Second)

	del := iceberg.DataFile{Path: "s3://b/tbl/data/d1", Format: "PARQUET", RecordCount: 2, FileSize: 10, Partition: map[string]any{}}
	step(iceberg.Change{Operation: iceberg.OpDelete, AddDeletes: []iceberg.DataFile{del}}, 2*time.Second)

	snap, _ := cur.CurrentSnapshot()
	assert.Equal(t, "1", snap.Summary["total-delete-files"])
	assert.Equal(t, "2", snap.Summary["total-position-deletes"])
	assert.Equal(t, "8", snap.Summary["total-records"])

	step(iceberg.Change{
		Operation: iceberg.OpReplace, AddData: []iceberg.DataFile{file("c")},
		RemoveData:    map[string]bool{"s3://b/tbl/data/a": true},
		RemoveDeletes: map[string]bool{del.Path: true},
	}, 3*time.Second)

	snap, _ = cur.CurrentSnapshot()
	assert.Equal(t, "8", snap.Summary["total-records"])
	assert.Equal(t, "0", snap.Summary["total-delete-files"])
	assert.Equal(t, "2", snap.Summary["total-data-files"])
	assert.Equal(t, "1", snap.Summary["deleted-data-files"])
	assert.Equal(t, "replace", snap.Summary["operation"])

	live := map[string]int{}

	for _, mf := range list {
		entries, err := iceberg.ReadManifest(store[mf.Path])
		require.NoError(t, err)

		for _, e := range entries {
			live[e.DataFile.Path] += map[int]int{iceberg.StatusDeleted: 100, iceberg.StatusExisting: 1, iceberg.StatusAdded: 1}[e.Status]
		}
	}

	assert.Equal(t, 100, live["s3://b/tbl/data/a"])
	assert.Equal(t, 1, live["s3://b/tbl/data/b"])
	assert.Equal(t, 1, live["s3://b/tbl/data/c"])
	assert.Equal(t, 100, live[del.Path])
}

func TestSchemaEvolution(t *testing.T) {
	t.Parallel()

	meta := iceberg.NewTableMetadata(iceberg.NewParams{
		Location: "s3://b/tbl", Schema: testSchema(t),
		Spec: iceberg.PartitionSpec{Fields: []iceberg.PartitionField{{Name: "category", Transform: "identity", SourceID: 2, FieldID: 1000}}},
		Now:  time.UnixMilli(1),
	})
	s := meta.CurrentSchema()

	tests := []struct {
		err  error
		do   func() (iceberg.Schema, error)
		name string
		want []string
	}{
		{name: "add", want: []string{"id", "category", "ts", "x"}, do: func() (iceberg.Schema, error) {
			out, _, err := meta.WithColumn(s, "x", "int", "")

			return out, err
		}},
		{name: "add dup", err: iceberg.ErrColumnExists, do: func() (iceberg.Schema, error) {
			out, _, err := meta.WithColumn(s, "ID", "int", "")

			return out, err
		}},
		{name: "drop", want: []string{"id", "category"}, do: func() (iceberg.Schema, error) { return meta.WithoutColumn(s, "ts") }},
		{name: "drop partition source", err: iceberg.ErrColumnInUse, do: func() (iceberg.Schema, error) { return meta.WithoutColumn(s, "category") }},
		{name: "promote", want: []string{"id", "category", "ts"}, do: func() (iceberg.Schema, error) { return meta.WithRenamedColumn(s, "id", "id", "bigint") }},
		{name: "bad promote", err: iceberg.ErrBadPromotion, do: func() (iceberg.Schema, error) { return meta.WithRenamedColumn(s, "id", "id", "int") }},
		{name: "move first", want: []string{"ts", "id", "category"}, do: func() (iceberg.Schema, error) { return meta.WithMovedColumn(s, "ts", true, "") }},
		{name: "move after", want: []string{"id", "ts", "category"}, do: func() (iceberg.Schema, error) { return meta.WithMovedColumn(s, "ts", false, "id") }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := tc.do()
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)

				return
			}

			require.NoError(t, err)

			var names []string
			for _, f := range out.Fields {
				names = append(names, f.Name)
			}

			assert.Equal(t, tc.want, names)
		})
	}
}
