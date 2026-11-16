package gcpbigtable_test

import (
	"encoding/binary"
	"math"
	"testing"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func aggFamily(a bigtable.Aggregator) bigtable.Family {
	return bigtable.Family{ValueType: bigtable.AggregateType{
		Input: bigtable.Int64Type{Encoding: bigtable.BigEndianBytesEncoding{}}, Aggregator: a,
	}}
}

func be(v int64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, uint64(v))

	return out
}

func (e *env) aggTable(t *testing.T) *bigtable.Table {
	t.Helper()

	tbl := e.seeded(t, "t", "plain")

	for name, a := range map[string]bigtable.Aggregator{
		"sum": bigtable.SumAggregator{}, "max": bigtable.MaxAggregator{}, "min": bigtable.MinAggregator{},
	} {
		require.NoError(t, e.ac.CreateColumnFamilyWithConfig(t.Context(), "t", name, aggFamily(a)))
	}

	return tbl
}

func TestAggregateCells(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(m *bigtable.Mutation)
		name string
		fam  string
		want int64
	}{
		{name: "sum", fam: "sum", ops: func(m *bigtable.Mutation) {
			m.AddIntToCell("sum", "c", 1000, 5)
			m.AddIntToCell("sum", "c", 1000, 7)
			m.AddIntToCell("sum", "c", 1000, -2)
		}, want: 10},
		{name: "max", fam: "max", ops: func(m *bigtable.Mutation) {
			m.AddIntToCell("max", "c", 1000, 3)
			m.AddIntToCell("max", "c", 1000, 9)
			m.AddIntToCell("max", "c", 1000, 4)
		}, want: 9},
		{name: "min", fam: "min", ops: func(m *bigtable.Mutation) {
			m.AddIntToCell("min", "c", 1000, 3)
			m.AddIntToCell("min", "c", 1000, -2)
			m.AddIntToCell("min", "c", 1000, 4)
		}, want: -2},
		{name: "merge raw state", fam: "sum", ops: func(m *bigtable.Mutation) {
			m.AddIntToCell("sum", "c", 1000, 4)
			m.MergeBytesToCell("sum", "c", 1000, be(6))
		}, want: 10},
		{name: "sum saturates high", fam: "sum", ops: func(m *bigtable.Mutation) {
			m.AddIntToCell("sum", "c", 1000, math.MaxInt64)
			m.AddIntToCell("sum", "c", 1000, 1)
		}, want: math.MaxInt64},
		{name: "sum saturates low", fam: "sum", ops: func(m *bigtable.Mutation) {
			m.AddIntToCell("sum", "c", 1000, math.MinInt64)
			m.AddIntToCell("sum", "c", 1000, -1)
		}, want: math.MinInt64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tbl := newEnv(t).aggTable(t)
			m := bigtable.NewMutation()
			tt.ops(m)
			require.NoError(t, tbl.Apply(t.Context(), "r", m))

			row, err := tbl.ReadRow(t.Context(), "r")
			require.NoError(t, err)
			require.Len(t, row[tt.fam], 1)
			assert.Equal(t, be(tt.want), row[tt.fam][0].Value)
			assert.Equal(t, bigtable.Timestamp(1000), row[tt.fam][0].Timestamp)
		})
	}
}

func TestAggregateSeparateTimestamps(t *testing.T) {
	t.Parallel()

	tbl := newEnv(t).aggTable(t)

	for _, ts := range []bigtable.Timestamp{1000, 2000, 1000} {
		m := bigtable.NewMutation()
		m.AddIntToCell("sum", "c", ts, 1)
		require.NoError(t, tbl.Apply(t.Context(), "r", m))
	}

	row, err := tbl.ReadRow(t.Context(), "r")
	require.NoError(t, err)
	require.Len(t, row["sum"], 2)
	assert.Equal(t, be(1), row["sum"][0].Value)
	assert.Equal(t, be(2), row["sum"][1].Value)
}

func TestAggregateRejections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut  func(m *bigtable.Mutation)
		name string
	}{
		{func(m *bigtable.Mutation) { m.Set("sum", "c", 1000, []byte("v")) }, "set cell on aggregate"},
		{func(m *bigtable.Mutation) { m.AddIntToCell("plain", "c", 1000, 1) }, "add on plain family"},
		{func(m *bigtable.Mutation) { m.MergeBytesToCell("sum", "c", 1000, []byte("short")) }, "short merge state"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tbl := newEnv(t).aggTable(t)
			m := bigtable.NewMutation()
			tt.mut(m)
			assert.Equal(t, codes.InvalidArgument, code(tbl.Apply(t.Context(), "r", m)))

			row, err := tbl.ReadRow(t.Context(), "r")
			require.NoError(t, err)
			assert.Empty(t, row)
		})
	}
}

func TestAggregateWireValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.aggTable(t)

	tablePath := "projects/" + project + "/instances/" + instance + "/tables/t"
	dc := btpb.NewBigtableClient(e.conn)

	val := func(v int64) *btpb.Value { return &btpb.Value{Kind: &btpb.Value_IntValue{IntValue: v}} }
	qual := &btpb.Value{Kind: &btpb.Value_RawValue{RawValue: []byte("c")}}
	micros := func(v int64) *btpb.Value {
		return &btpb.Value{Kind: &btpb.Value_RawTimestampMicros{RawTimestampMicros: v}}
	}

	tests := []struct {
		mut  *btpb.Mutation_AddToCell
		name string
	}{
		{
			&btpb.Mutation_AddToCell{FamilyName: "sum", ColumnQualifier: qual, Timestamp: micros(1500), Input: val(1)},
			"granularity",
		},
		{
			&btpb.Mutation_AddToCell{FamilyName: "sum", ColumnQualifier: qual, Timestamp: micros(-1), Input: val(1)},
			"server time",
		},
		{&btpb.Mutation_AddToCell{FamilyName: "sum", ColumnQualifier: qual, Timestamp: micros(1000)}, "null input"},
		{
			&btpb.Mutation_AddToCell{
				FamilyName:      "sum",
				ColumnQualifier: val(1),
				Timestamp:       micros(1000),
				Input:           val(1),
			},
			"qualifier kind",
		},
		{
			&btpb.Mutation_AddToCell{FamilyName: "sum", ColumnQualifier: qual, Timestamp: val(1000), Input: val(1)},
			"timestamp kind",
		},
		{&btpb.Mutation_AddToCell{
			FamilyName: "sum", ColumnQualifier: qual, Timestamp: micros(1000),
			Input: &btpb.Value{Kind: &btpb.Value_StringValue{StringValue: "x"}},
		}, "string input"},
		{
			&btpb.Mutation_AddToCell{FamilyName: "nope", ColumnQualifier: qual, Timestamp: micros(1000), Input: val(1)},
			"unknown family",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := dc.MutateRow(t.Context(), &btpb.MutateRowRequest{
				TableName: tablePath, RowKey: []byte("r"),
				Mutations: []*btpb.Mutation{{Mutation: &btpb.Mutation_AddToCell_{AddToCell: tt.mut}}},
			})
			assert.Contains(t, []codes.Code{codes.InvalidArgument, codes.NotFound}, code(err))
			assert.NotEqual(t, codes.OK, code(err))
		})
	}

	_, err := dc.ReadModifyWriteRow(t.Context(), &btpb.ReadModifyWriteRowRequest{
		TableName: tablePath, RowKey: []byte("r"),
		Rules: []*btpb.ReadModifyWriteRule{
			{
				FamilyName:      "sum",
				ColumnQualifier: []byte("c"),
				Rule:            &btpb.ReadModifyWriteRule_IncrementAmount{IncrementAmount: 1},
			},
		},
	})
	assert.Equal(t, codes.InvalidArgument, code(err), "read-modify-write on an aggregate family")
}

func TestAggregateFamilyValidation(t *testing.T) {
	t.Parallel()

	i64 := func(enc *adminpb.Type_Int64_Encoding) *adminpb.Type {
		return &adminpb.Type{Kind: &adminpb.Type_Int64Type{Int64Type: &adminpb.Type_Int64{Encoding: enc}}}
	}
	be := &adminpb.Type_Int64_Encoding{
		Encoding: &adminpb.Type_Int64_Encoding_BigEndianBytes_{
			BigEndianBytes: &adminpb.Type_Int64_Encoding_BigEndianBytes{},
		},
	}
	ordered := &adminpb.Type_Int64_Encoding{
		Encoding: &adminpb.Type_Int64_Encoding_OrderedCodeBytes_{
			OrderedCodeBytes: &adminpb.Type_Int64_Encoding_OrderedCodeBytes{},
		},
	}
	sum := &adminpb.Type_Aggregate_Sum_{Sum: &adminpb.Type_Aggregate_Sum{}}

	tests := []struct {
		agg  *adminpb.Type_Aggregate
		name string
		want codes.Code
	}{
		{&adminpb.Type_Aggregate{InputType: i64(be), Aggregator: sum}, "ok", codes.OK},
		{&adminpb.Type_Aggregate{InputType: i64(nil), Aggregator: sum}, "no encoding", codes.InvalidArgument},
		{&adminpb.Type_Aggregate{InputType: i64(ordered), Aggregator: sum}, "ordered code", codes.InvalidArgument},
		{&adminpb.Type_Aggregate{InputType: i64(be)}, "no aggregator", codes.InvalidArgument},
		{
			&adminpb.Type_Aggregate{
				InputType:  &adminpb.Type{Kind: &adminpb.Type_BytesType{BytesType: &adminpb.Type_Bytes{}}},
				Aggregator: sum,
			},
			"bytes input",
			codes.InvalidArgument,
		},
		{
			&adminpb.Type_Aggregate{
				InputType:  i64(be),
				Aggregator: &adminpb.Type_Aggregate_Max_{Max: &adminpb.Type_Aggregate_Max{}},
			},
			"max",
			codes.OK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.seeded(t, "t", "plain")

			tc := adminpb.NewBigtableTableAdminClient(e.conn)
			out, err := tc.ModifyColumnFamilies(t.Context(), &adminpb.ModifyColumnFamiliesRequest{
				Name: "projects/" + project + "/instances/" + instance + "/tables/t",
				Modifications: []*adminpb.ModifyColumnFamiliesRequest_Modification{
					{
						Id: "agg",
						Mod: &adminpb.ModifyColumnFamiliesRequest_Modification_Create{Create: &adminpb.ColumnFamily{
							ValueType: &adminpb.Type{Kind: &adminpb.Type_AggregateType{AggregateType: tt.agg}},
						}},
					},
				},
			})
			require.Equal(t, tt.want, code(err))

			if tt.want == codes.OK {
				state := out.GetColumnFamilies()["agg"].GetValueType().GetAggregateType().GetStateType()
				assert.NotNil(t, state.GetInt64Type(), "state type equals the input type")
			}
		})
	}
}

func TestAggregateFamilyImmutable(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.aggTable(t)

	err := e.ac.UpdateFamily(t.Context(), "t", "plain", bigtable.Family{ValueType: bigtable.AggregateType{
		Input: bigtable.Int64Type{Encoding: bigtable.BigEndianBytesEncoding{}}, Aggregator: bigtable.SumAggregator{},
	}})
	assert.Equal(t, codes.InvalidArgument, code(err), "a plain family cannot become an aggregate")
}
