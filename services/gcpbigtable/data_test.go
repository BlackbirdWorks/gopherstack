package gcpbigtable_test

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

type seedCell struct {
	row, fam, col, val string
	ts                 bigtable.Timestamp
}

func put(t *testing.T, tbl *bigtable.Table, cells ...seedCell) {
	t.Helper()

	for _, c := range cells {
		m := bigtable.NewMutation()
		m.Set(c.fam, c.col, c.ts, []byte(c.val))
		require.NoError(t, tbl.Apply(t.Context(), c.row, m))
	}
}

func flat(rows []bigtable.Row) []string {
	var out []string

	for _, r := range rows {
		fams := make([]string, 0, len(r))
		for f := range r {
			fams = append(fams, f)
		}

		slices.Sort(fams)

		for _, f := range fams {
			for _, it := range r[f] {
				s := fmt.Sprintf("%s|%s|%d|%s", it.Row, it.Column, it.Timestamp, it.Value)
				if len(it.Labels) > 0 {
					s += "|" + strings.Join(it.Labels, ",")
				}

				out = append(out, s)
			}
		}
	}

	return out
}

func readAll(t *testing.T, tbl *bigtable.Table, set bigtable.RowSet, opts ...bigtable.ReadOption) []string {
	t.Helper()

	var rows []bigtable.Row

	require.NoError(t, tbl.ReadRows(t.Context(), set, func(r bigtable.Row) bool {
		rows = append(rows, r)

		return true
	}, opts...))

	return flat(rows)
}

func seedFilterTable(t *testing.T) *bigtable.Table {
	t.Helper()

	tbl := newEnv(t).seeded(t, "t", "cf1", "cf2")
	put(t, tbl,
		seedCell{"r1", "cf1", "a", "a1", 1000}, seedCell{"r1", "cf1", "a", "a2", 2000}, seedCell{"r1", "cf1", "a", "a3", 3000},
		seedCell{"r1", "cf1", "b", "b1", 1000}, seedCell{"r1", "cf2", "c", "c1", 1000},
		seedCell{"r2", "cf1", "a", "x1", 1000}, seedCell{"r2", "cf2", "c", "c2", 2000},
		seedCell{"r3", "cf1", "b", "b3", 1000},
	)

	return tbl
}

func TestReadRowsFilters(t *testing.T) {
	t.Parallel()

	tbl := seedFilterTable(t)

	tests := []struct {
		filter bigtable.Filter
		name   string
		want   []string
	}{
		{bigtable.PassAllFilter(), "pass all", []string{
			"r1|cf1:a|3000|a3", "r1|cf1:a|2000|a2", "r1|cf1:a|1000|a1", "r1|cf1:b|1000|b1", "r1|cf2:c|1000|c1",
			"r2|cf1:a|1000|x1", "r2|cf2:c|2000|c2", "r3|cf1:b|1000|b3",
		}},
		{bigtable.BlockAllFilter(), "block all", nil},
		{bigtable.FamilyFilter("cf2"), "family", []string{"r1|cf2:c|1000|c1", "r2|cf2:c|2000|c2"}},
		{bigtable.FamilyFilter("cf[12]"), "family regex whole match", []string{
			"r1|cf1:a|3000|a3", "r1|cf1:a|2000|a2", "r1|cf1:a|1000|a1", "r1|cf1:b|1000|b1", "r1|cf2:c|1000|c1",
			"r2|cf1:a|1000|x1", "r2|cf2:c|2000|c2", "r3|cf1:b|1000|b3",
		}},
		{bigtable.FamilyFilter("cf"), "family regex is anchored", nil},
		{bigtable.ColumnFilter("a"), "column", []string{
			"r1|cf1:a|3000|a3", "r1|cf1:a|2000|a2", "r1|cf1:a|1000|a1", "r2|cf1:a|1000|x1",
		}},
		{bigtable.ValueFilter("a."), "value regex", []string{"r1|cf1:a|3000|a3", "r1|cf1:a|2000|a2", "r1|cf1:a|1000|a1"}},
		{bigtable.LatestNFilter(1), "latest one", []string{
			"r1|cf1:a|3000|a3", "r1|cf1:b|1000|b1", "r1|cf2:c|1000|c1", "r2|cf1:a|1000|x1", "r2|cf2:c|2000|c2", "r3|cf1:b|1000|b3",
		}},
		{bigtable.LatestNFilter(2), "latest two", []string{
			"r1|cf1:a|3000|a3", "r1|cf1:a|2000|a2", "r1|cf1:b|1000|b1", "r1|cf2:c|1000|c1",
			"r2|cf1:a|1000|x1", "r2|cf2:c|2000|c2", "r3|cf1:b|1000|b3",
		}},
		{bigtable.TimestampRangeFilterMicros(2000, 3000), "timestamp range end exclusive", []string{
			"r1|cf1:a|2000|a2", "r2|cf2:c|2000|c2",
		}},
		{bigtable.TimestampRangeFilterMicros(2000, 0), "timestamp range open end", []string{
			"r1|cf1:a|3000|a3", "r1|cf1:a|2000|a2", "r2|cf2:c|2000|c2",
		}},
		{bigtable.CellsPerRowLimitFilter(2), "cells per row limit", []string{
			"r1|cf1:a|3000|a3", "r1|cf1:a|2000|a2", "r2|cf1:a|1000|x1", "r2|cf2:c|2000|c2", "r3|cf1:b|1000|b3",
		}},
		{bigtable.CellsPerRowOffsetFilter(3), "cells per row offset", []string{"r1|cf1:b|1000|b1", "r1|cf2:c|1000|c1"}},
		{bigtable.ChainFilters(bigtable.FamilyFilter("cf2"), bigtable.StripValueFilter()), "strip value", []string{
			"r1|cf2:c|1000|", "r2|cf2:c|2000|",
		}},
		{bigtable.ChainFilters(bigtable.FamilyFilter("cf1"), bigtable.LatestNFilter(1)), "chain family then latest", []string{
			"r1|cf1:a|3000|a3", "r1|cf1:b|1000|b1", "r2|cf1:a|1000|x1", "r3|cf1:b|1000|b3",
		}},
		{bigtable.InterleaveFilters(bigtable.ColumnFilter("a"), bigtable.ColumnFilter("b")), "interleave columns", []string{
			"r1|cf1:a|3000|a3", "r1|cf1:a|2000|a2", "r1|cf1:a|1000|a1", "r1|cf1:b|1000|b1", "r2|cf1:a|1000|x1", "r3|cf1:b|1000|b3",
		}},
		{
			bigtable.ChainFilters(bigtable.RowKeyFilter("r3"), bigtable.InterleaveFilters(bigtable.PassAllFilter(), bigtable.PassAllFilter())),
			"interleave keeps duplicates",
			[]string{"r3|cf1:b|1000|b3", "r3|cf1:b|1000|b3"},
		},
		{
			bigtable.ConditionFilter(bigtable.ValueFilter("c2"), bigtable.LabelFilter("hit"), bigtable.LabelFilter("miss")),
			"condition labels",
			[]string{
				"r1|cf1:a|3000|a3|miss", "r1|cf1:a|2000|a2|miss", "r1|cf1:a|1000|a1|miss", "r1|cf1:b|1000|b1|miss", "r1|cf2:c|1000|c1|miss",
				"r2|cf1:a|1000|x1|hit", "r2|cf2:c|2000|c2|hit", "r3|cf1:b|1000|b3|miss",
			},
		},
		{bigtable.ConditionFilter(bigtable.ValueFilter("c2"), bigtable.PassAllFilter(), nil), "condition without false branch", []string{
			"r2|cf1:a|1000|x1", "r2|cf2:c|2000|c2",
		}},
		{bigtable.RowKeyFilter("r[12]"), "row key regex", []string{
			"r1|cf1:a|3000|a3", "r1|cf1:a|2000|a2", "r1|cf1:a|1000|a1", "r1|cf1:b|1000|b1", "r1|cf2:c|1000|c1",
			"r2|cf1:a|1000|x1", "r2|cf2:c|2000|c2",
		}},
		{bigtable.ValueRangeFilter([]byte("b"), []byte("c")), "value range", []string{"r1|cf1:b|1000|b1", "r3|cf1:b|1000|b3"}},
		{bigtable.ColumnRangeFilter("cf1", "a", "b"), "column range end exclusive", []string{
			"r1|cf1:a|3000|a3", "r1|cf1:a|2000|a2", "r1|cf1:a|1000|a1", "r2|cf1:a|1000|x1",
		}},
		{bigtable.ColumnRangeFilter("cf1", "b", ""), "column range open end", []string{"r1|cf1:b|1000|b1", "r3|cf1:b|1000|b3"}},
		{
			bigtable.ChainFilters(bigtable.FamilyFilter("cf1"), bigtable.ColumnFilter("b"), bigtable.LabelFilter("lbl")), "label",
			[]string{"r1|cf1:b|1000|b1|lbl", "r3|cf1:b|1000|b3|lbl"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, readAll(t, tbl, bigtable.RowRange{}, bigtable.RowFilter(tt.filter)))
		})
	}
}

func TestReadRowsSelection(t *testing.T) {
	t.Parallel()

	tbl := newEnv(t).seeded(t, "t", "cf")

	for _, k := range []string{"a", "b", "b1", "c", "d"} {
		put(t, tbl, seedCell{k, "cf", "q", "v-" + k, 1000})
	}

	keys := func(rows []string) []string {
		out := make([]string, len(rows))
		for i, r := range rows {
			out[i] = strings.SplitN(r, "|", 2)[0] //nolint:mnd // key|rest
		}

		return out
	}

	tests := []struct {
		set  bigtable.RowSet
		name string
		opts []bigtable.ReadOption
		want []string
	}{
		{bigtable.RowRange{}, "all rows", nil, []string{"a", "b", "b1", "c", "d"}},
		{bigtable.RowList{"c", "a", "missing", "a"}, "row list sorted and deduped", nil, []string{"a", "c"}},
		{bigtable.NewRange("b", "d"), "closed open range", nil, []string{"b", "b1", "c"}},
		{bigtable.NewClosedRange("b", "d"), "closed range", nil, []string{"b", "b1", "c", "d"}},
		{bigtable.NewOpenRange("b", "d"), "open range", nil, []string{"b1", "c"}},
		{bigtable.NewOpenClosedRange("b", "d"), "open closed range", nil, []string{"b1", "c", "d"}},
		{bigtable.PrefixRange("b"), "prefix", nil, []string{"b", "b1"}},
		{bigtable.InfiniteRange("c"), "infinite", nil, []string{"c", "d"}},
		{bigtable.SingleRow("b1"), "single row", nil, []string{"b1"}},
		{
			bigtable.RowRangeList{bigtable.NewRange("a", "b"), bigtable.NewRange("c", "d"), bigtable.NewRange("a", "c")},
			"overlapping ranges", nil,
			[]string{"a", "b", "b1", "c"},
		},
		{bigtable.RowRange{}, "limit", []bigtable.ReadOption{bigtable.LimitRows(2)}, []string{"a", "b"}},
		{bigtable.RowRange{}, "reverse", []bigtable.ReadOption{bigtable.ReverseScan()}, []string{"d", "c", "b1", "b", "a"}},
		{
			bigtable.PrefixRange("b"), "reverse limited prefix",
			[]bigtable.ReadOption{bigtable.ReverseScan(), bigtable.LimitRows(1)},
			[]string{"b1"},
		},
		{bigtable.RowList{"a", "b", "c"}, "limit counts returned rows", []bigtable.ReadOption{
			bigtable.RowFilter(bigtable.RowKeyFilter("[bc]")), bigtable.LimitRows(1),
		}, []string{"b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, keys(readAll(t, tbl, tt.set, tt.opts...)))
		})
	}
}

func TestMutations(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) *bigtable.Table {
		t.Helper()

		tbl := newEnv(t).seeded(t, "t", "cf", "other")
		put(t, tbl,
			seedCell{"r", "cf", "a", "a1", 1000}, seedCell{"r", "cf", "a", "a2", 2000}, seedCell{"r", "cf", "a", "a3", 3000},
			seedCell{"r", "cf", "b", "b1", 1000}, seedCell{"r", "other", "o", "o1", 1000})

		return tbl
	}

	tests := []struct {
		mut  func(m *bigtable.Mutation)
		name string
		want []string
	}{
		{func(m *bigtable.Mutation) { m.Set("cf", "a", 2000, []byte("new")) }, "overwrite same timestamp", []string{
			"r|cf:a|3000|a3", "r|cf:a|2000|new", "r|cf:a|1000|a1", "r|cf:b|1000|b1", "r|other:o|1000|o1",
		}},
		{func(m *bigtable.Mutation) { m.DeleteCellsInColumn("cf", "a") }, "delete column", []string{
			"r|cf:b|1000|b1", "r|other:o|1000|o1",
		}},
		{func(m *bigtable.Mutation) { m.DeleteTimestampRange("cf", "a", 1000, 3000) }, "delete timestamp range", []string{
			"r|cf:a|3000|a3", "r|cf:b|1000|b1", "r|other:o|1000|o1",
		}},
		{func(m *bigtable.Mutation) { m.DeleteCellsInFamily("cf") }, "delete family", []string{"r|other:o|1000|o1"}},
		{func(m *bigtable.Mutation) { m.DeleteRow() }, "delete row", nil},
		{func(m *bigtable.Mutation) { m.DeleteRow(); m.Set("cf", "z", 5000, []byte("z")) }, "delete row then set", []string{
			"r|cf:z|5000|z",
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tbl := setup(t)
			m := bigtable.NewMutation()
			tt.mut(m)
			require.NoError(t, tbl.Apply(t.Context(), "r", m))

			assert.Equal(t, tt.want, readAll(t, tbl, bigtable.RowRange{}))
		})
	}
}

func TestMutationErrors(t *testing.T) {
	t.Parallel()

	tbl := newEnv(t).seeded(t, "t", "cf")

	tests := []struct {
		mut  func(m *bigtable.Mutation)
		name string
		row  string
		want codes.Code
	}{
		{func(m *bigtable.Mutation) { m.Set("nofam", "a", 1000, []byte("v")) }, "unknown family", "r", codes.NotFound},
		{func(m *bigtable.Mutation) { m.DeleteCellsInFamily("nofam") }, "delete unknown family", "r", codes.NotFound},
		{func(m *bigtable.Mutation) { m.Set("cf", "a", 1000, []byte("v")) }, "empty row key", "", codes.InvalidArgument},
		{func(m *bigtable.Mutation) { m.AddIntToCell("cf", "a", 1000, 1) }, "aggregate mutation", "r", codes.Unimplemented},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := bigtable.NewMutation()
			tt.mut(m)
			assert.Equal(t, tt.want, code(tbl.Apply(t.Context(), tt.row, m)))
		})
	}
}

func TestServerTimestamp(t *testing.T) {
	t.Parallel()

	tbl := newEnv(t).seeded(t, "t", "cf")
	m := bigtable.NewMutation()
	m.Set("cf", "a", bigtable.ServerTime, []byte("v"))
	require.NoError(t, tbl.Apply(t.Context(), "r", m))

	row, err := tbl.ReadRow(t.Context(), "r")
	require.NoError(t, err)
	require.Len(t, row["cf"], 1)
	assert.Positive(t, int64(row["cf"][0].Timestamp))
	assert.Zero(t, int64(row["cf"][0].Timestamp)%1000, "millisecond granularity")
}

func TestApplyBulk(t *testing.T) {
	t.Parallel()

	tbl := newEnv(t).seeded(t, "t", "cf")

	ok := bigtable.NewMutation()
	ok.Set("cf", "a", 1000, []byte("v"))

	bad := bigtable.NewMutation()
	bad.Set("missing", "a", 1000, []byte("v"))

	errs, err := tbl.ApplyBulk(t.Context(), []string{"r1", "r2", "r3"}, []*bigtable.Mutation{ok, bad, ok})
	require.NoError(t, err)
	require.Len(t, errs, 3)
	require.NoError(t, errs[0])
	assert.Equal(t, codes.NotFound, code(errs[1]))
	require.NoError(t, errs[2])

	assert.Equal(t, []string{"r1|cf:a|1000|v", "r3|cf:a|1000|v"}, readAll(t, tbl, bigtable.RowRange{}))
}

func TestMutationIsAtomic(t *testing.T) {
	t.Parallel()

	tbl := newEnv(t).seeded(t, "t", "cf")

	m := bigtable.NewMutation()
	m.Set("cf", "a", 1000, []byte("v"))
	m.Set("missing", "a", 1000, []byte("v"))
	require.Error(t, tbl.Apply(t.Context(), "r", m))

	assert.Empty(t, readAll(t, tbl, bigtable.RowRange{}))
}

func TestCheckAndMutate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pred        bigtable.Filter
		name        string
		want        []string
		wantMatched bool
	}{
		{bigtable.ValueFilter("v1"), "predicate matches", []string{"r|cf:a|1000|v1", "r|cf:t|1000|yes"}, true},
		{bigtable.ValueFilter("nope"), "predicate misses", []string{"r|cf:a|1000|v1", "r|cf:f|1000|no"}, false},
		{nil, "nil predicate matches any cell", []string{"r|cf:a|1000|v1", "r|cf:t|1000|yes"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tbl := newEnv(t).seeded(t, "t", "cf")
			put(t, tbl, seedCell{"r", "cf", "a", "v1", 1000})

			yes := bigtable.NewMutation()
			yes.Set("cf", "t", 1000, []byte("yes"))

			no := bigtable.NewMutation()
			no.Set("cf", "f", 1000, []byte("no"))

			var matched bool

			cond := bigtable.NewCondMutation(tt.pred, yes, no)
			require.NoError(t, tbl.Apply(t.Context(), "r", cond, bigtable.GetCondMutationResult(&matched)))
			assert.Equal(t, tt.wantMatched, matched)
			assert.Equal(t, tt.want, readAll(t, tbl, bigtable.RowRange{}))
		})
	}

	t.Run("missing row without predicate does not match", func(t *testing.T) {
		t.Parallel()

		tbl := newEnv(t).seeded(t, "t", "cf")
		yes := bigtable.NewMutation()
		yes.Set("cf", "t", 1000, []byte("yes"))

		no := bigtable.NewMutation()
		no.Set("cf", "f", 1000, []byte("no"))

		var matched bool

		require.NoError(t, tbl.Apply(t.Context(), "absent", bigtable.NewCondMutation(nil, yes, no), bigtable.GetCondMutationResult(&matched)))
		assert.False(t, matched)
		assert.Equal(t, []string{"absent|cf:f|1000|no"}, readAll(t, tbl, bigtable.RowRange{}))
	})
}

func TestReadModifyWrite(t *testing.T) {
	t.Parallel()

	tbl := newEnv(t).seeded(t, "t", "cf")

	rmw := bigtable.NewReadModifyWrite()
	rmw.AppendValue("cf", "log", []byte("a"))
	rmw.Increment("cf", "n", 5)

	row, err := tbl.ApplyReadModifyWrite(t.Context(), "r", rmw)
	require.NoError(t, err)
	require.Len(t, row["cf"], 2)
	assert.Equal(t, "cf:log", row["cf"][0].Column)
	assert.Equal(t, []byte("a"), row["cf"][0].Value)
	assert.Equal(t, "cf:n", row["cf"][1].Column)
	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 5}, row["cf"][1].Value)

	rmw = bigtable.NewReadModifyWrite()
	rmw.AppendValue("cf", "log", []byte("bc"))
	rmw.Increment("cf", "n", -2)
	rmw.Increment("cf", "n", 10)
	row, err = tbl.ApplyReadModifyWrite(t.Context(), "r", rmw)
	require.NoError(t, err)
	assert.Equal(t, []byte("abc"), row["cf"][0].Value)
	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 13}, row["cf"][1].Value, "later rules see earlier ones")

	t.Run("increment of a non 64-bit value", func(t *testing.T) {
		t.Parallel()

		bad := bigtable.NewReadModifyWrite()
		bad.Increment("cf", "log", 1)
		_, ierr := tbl.ApplyReadModifyWrite(t.Context(), "r", bad)
		assert.Equal(t, codes.FailedPrecondition, code(ierr))
	})

	t.Run("unknown family", func(t *testing.T) {
		t.Parallel()

		bad := bigtable.NewReadModifyWrite()
		bad.AppendValue("nofam", "x", []byte("v"))
		_, ierr := tbl.ApplyReadModifyWrite(t.Context(), "r2", bad)
		assert.Equal(t, codes.NotFound, code(ierr))
	})
}

func TestSampleRowKeys(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)
	require.NoError(t, e.ac.CreatePresplitTable(t.Context(), "t", []string{"g", "p"}))
	require.NoError(t, e.ac.CreateColumnFamily(t.Context(), "t", "cf"))

	tbl := e.client(t).Open("t")
	put(t, tbl, seedCell{"a", "cf", "q", "12345", 1000}, seedCell{"h", "cf", "q", "12", 1000}, seedCell{"z", "cf", "q", "1", 1000})

	keys, err := tbl.SampleRowKeys(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []string{"g", "p"}, keys)
}

func TestRowKeyAndQualifierLimits(t *testing.T) {
	t.Parallel()

	tbl := newEnv(t).seeded(t, "t", "cf")

	long := bigtable.NewMutation()
	long.Set("cf", "q", 1000, []byte("v"))
	assert.Equal(t, codes.InvalidArgument, code(tbl.Apply(t.Context(), strings.Repeat("k", 4097), long)))
	require.NoError(t, tbl.Apply(t.Context(), strings.Repeat("k", 4096), long))

	q := bigtable.NewMutation()
	q.Set("cf", strings.Repeat("q", 16385), 1000, []byte("v"))
	assert.Equal(t, codes.InvalidArgument, code(tbl.Apply(t.Context(), "r", q)))
}

func TestInvalidFilters(t *testing.T) {
	t.Parallel()

	tbl := newEnv(t).seeded(t, "t", "cf")
	put(t, tbl, seedCell{"r", "cf", "a", "v", 1000})

	tests := []struct {
		filter bigtable.Filter
		name   string
	}{
		{bigtable.ChainFilters(bigtable.PassAllFilter()), "chain of one"},
		{bigtable.InterleaveFilters(bigtable.PassAllFilter()), "interleave of one"},
		{bigtable.ValueFilter("("), "bad regex"},
		{bigtable.ChainFilters(bigtable.LabelFilter("a"), bigtable.LabelFilter("b")), "two labels in a chain"},
		{bigtable.LabelFilter("UPPER"), "bad label"},
		{bigtable.ColumnRangeFilter("", "a", "b"), "column range without family"},
		{bigtable.RowSampleFilter(2), "row sample out of range"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tbl.ReadRows(t.Context(), bigtable.RowRange{}, func(bigtable.Row) bool { return true },
				bigtable.RowFilter(tt.filter))
			assert.Equal(t, codes.InvalidArgument, code(err))
		})
	}
}

func TestRowSample(t *testing.T) {
	t.Parallel()

	tbl := newEnv(t).seeded(t, "t", "cf")

	for i := range 200 {
		put(t, tbl, seedCell{"r" + strconv.Itoa(1000+i), "cf", "q", "v", 1000})
	}

	n := len(readAll(t, tbl, bigtable.RowRange{}, bigtable.RowFilter(bigtable.RowSampleFilter(0.5))))
	assert.Greater(t, n, 40)
	assert.Less(t, n, 160)
}

func TestReadMissingTable(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)

	tbl := e.client(t).Open("nope")
	err := tbl.ReadRows(t.Context(), bigtable.RowRange{}, func(bigtable.Row) bool { return true })
	assert.Equal(t, codes.NotFound, code(err))
}
