package gcpbigtable_test

import (
	"encoding/base64"
	"io"
	"strconv"
	"testing"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const tablePath = "projects/" + project + "/instances/" + instance + "/tables/t"

func readRaw(t *testing.T, e *env, ctx func() (metadata.MD, bool), req *btpb.ReadRowsRequest) ([]*btpb.ReadRowsResponse, error) {
	t.Helper()

	c := t.Context()
	if md, ok := ctx(); ok {
		c = metadata.NewOutgoingContext(c, md)
	}

	stream, err := btpb.NewBigtableClient(e.conn).ReadRows(c, req)
	require.NoError(t, err)

	var out []*btpb.ReadRowsResponse

	for {
		resp, rerr := stream.Recv()
		if rerr == io.EOF {
			return out, nil
		}

		if rerr != nil {
			return out, rerr
		}

		out = append(out, resp)
	}
}

func noMD() (metadata.MD, bool) { return nil, false }

func TestReadRowsChunkEncoding(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tbl := e.seeded(t, "t", "a", "b")
	put(t, tbl,
		seedCell{"r1", "a", "q1", "v1", 2000}, seedCell{"r1", "a", "q1", "v0", 1000}, seedCell{"r1", "a", "q2", "w", 1000},
		seedCell{"r1", "b", "q1", "x", 1000}, seedCell{"r2", "a", "q1", "y", 1000})

	resps, err := readRaw(t, e, noMD, &btpb.ReadRowsRequest{TableName: tablePath})
	require.NoError(t, err)
	require.Len(t, resps, 2)

	chunks := resps[0].GetChunks()
	require.Len(t, chunks, 4)

	assert.Equal(t, "r1", string(chunks[0].GetRowKey()))
	assert.Equal(t, "a", chunks[0].GetFamilyName().GetValue())
	assert.Equal(t, "q1", string(chunks[0].GetQualifier().GetValue()))
	assert.EqualValues(t, 2000, chunks[0].GetTimestampMicros())

	assert.Nil(t, chunks[1].GetRowKey(), "same column: only the timestamp and value change")
	assert.Nil(t, chunks[1].GetFamilyName())
	assert.Nil(t, chunks[1].GetQualifier())
	assert.EqualValues(t, 1000, chunks[1].GetTimestampMicros())

	assert.Nil(t, chunks[2].GetFamilyName(), "same family: qualifier only")
	assert.Equal(t, "q2", string(chunks[2].GetQualifier().GetValue()))

	assert.Equal(t, "b", chunks[3].GetFamilyName().GetValue(), "a new family always names its qualifier")
	assert.Equal(t, "q1", string(chunks[3].GetQualifier().GetValue()))
	assert.True(t, chunks[3].GetCommitRow())

	for _, c := range chunks[:3] {
		assert.False(t, c.GetCommitRow())
	}

	assert.Equal(t, "r2", string(resps[1].GetChunks()[0].GetRowKey()))
}

func TestLastScannedRowKey(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tbl := e.seeded(t, "t", "a")
	put(t, tbl, seedCell{"r1", "a", "q", "keep", 1000}, seedCell{"r2", "a", "q", "drop", 1000}, seedCell{"r3", "a", "q", "keep", 1000})

	req := &btpb.ReadRowsRequest{
		TableName: tablePath,
		Filter:    &btpb.RowFilter{Filter: &btpb.RowFilter_ValueRegexFilter{ValueRegexFilter: []byte("keep")}},
	}

	flags, err := proto.Marshal(&btpb.FeatureFlags{LastScannedRowResponses: true})
	require.NoError(t, err)

	withFlag := func() (metadata.MD, bool) {
		return metadata.Pairs("bigtable-features", base64.StdEncoding.EncodeToString(flags)), true
	}

	tests := []struct {
		md   func() (metadata.MD, bool)
		name string
		want []string
	}{
		{noMD, "without the flag filtered rows are silent", []string{"chunk:r1", "chunk:r3"}},
		{withFlag, "with the flag filtered rows are reported", []string{"chunk:r1", "scanned:r2", "chunk:r3"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resps, rerr := readRaw(t, e, tt.md, req)
			require.NoError(t, rerr)

			var got []string

			for _, r := range resps {
				if len(r.GetChunks()) > 0 {
					got = append(got, "chunk:"+string(r.GetChunks()[0].GetRowKey()))
				} else if k := r.GetLastScannedRowKey(); k != nil {
					got = append(got, "scanned:"+string(k))
				}
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestReadRowsRequestStats(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tbl := e.seeded(t, "t", "a")
	put(t, tbl, seedCell{"r1", "a", "q", "v", 1000}, seedCell{"r1", "a", "q", "w", 2000}, seedCell{"r2", "a", "q", "v", 1000})

	resps, err := readRaw(t, e, noMD, &btpb.ReadRowsRequest{
		TableName:        tablePath,
		RequestStatsView: btpb.ReadRowsRequest_REQUEST_STATS_FULL,
		Filter:           &btpb.RowFilter{Filter: &btpb.RowFilter_CellsPerColumnLimitFilter{CellsPerColumnLimitFilter: 1}},
	})
	require.NoError(t, err)

	stats := resps[len(resps)-1].GetRequestStats().GetFullReadStatsView().GetReadIterationStats()
	require.NotNil(t, stats)
	assert.EqualValues(t, 2, stats.GetRowsSeenCount())
	assert.EqualValues(t, 2, stats.GetRowsReturnedCount())
	assert.EqualValues(t, 3, stats.GetCellsSeenCount())
	assert.EqualValues(t, 2, stats.GetCellsReturnedCount())
}

func TestReadRowsValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.seeded(t, "t", "a")

	rng := func(r *btpb.RowRange) *btpb.RowSet { return &btpb.RowSet{RowRanges: []*btpb.RowRange{r}} }

	tests := []struct {
		req  *btpb.ReadRowsRequest
		name string
		want codes.Code
	}{
		{&btpb.ReadRowsRequest{TableName: tablePath, RowsLimit: -1}, "negative limit", codes.InvalidArgument},
		{&btpb.ReadRowsRequest{TableName: tablePath, Rows: rng(&btpb.RowRange{
			StartKey: &btpb.RowRange_StartKeyClosed{StartKeyClosed: []byte("z")},
			EndKey:   &btpb.RowRange_EndKeyOpen{EndKeyOpen: []byte("a")},
		})}, "inverted range", codes.InvalidArgument},
		{&btpb.ReadRowsRequest{TableName: tablePath, Filter: &btpb.RowFilter{}}, "empty filter", codes.InvalidArgument},
		{&btpb.ReadRowsRequest{TableName: tablePath, Filter: &btpb.RowFilter{
			Filter: &btpb.RowFilter_PassAllFilter{PassAllFilter: false},
		}}, "pass all false", codes.InvalidArgument},
		{&btpb.ReadRowsRequest{TableName: tablePath, Filter: &btpb.RowFilter{
			Filter: &btpb.RowFilter_Condition_{Condition: &btpb.RowFilter_Condition{
				PredicateFilter: &btpb.RowFilter{Filter: &btpb.RowFilter_Sink{Sink: true}},
				TrueFilter:      &btpb.RowFilter{Filter: &btpb.RowFilter_PassAllFilter{PassAllFilter: true}},
			}},
		}}, "sink inside condition", codes.InvalidArgument},
		{&btpb.ReadRowsRequest{TableName: "bogus"}, "bad table name", codes.InvalidArgument},
		{&btpb.ReadRowsRequest{AuthorizedViewName: tablePath + "/authorizedViews/v"}, "authorized view", codes.Unimplemented},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := readRaw(t, e, noMD, tt.req)
			assert.Equal(t, tt.want, code(err))
		})
	}
}

func TestSinkFilter(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tbl := e.seeded(t, "t", "A", "B")
	put(t, tbl, seedCell{"r", "A", "x", "1", 1000}, seedCell{"r", "A", "y", "2", 2000}, seedCell{"r", "B", "y", "4", 4000})

	label := func(l string) *btpb.RowFilter {
		return &btpb.RowFilter{Filter: &btpb.RowFilter_ApplyLabelTransformer{ApplyLabelTransformer: l}}
	}
	pass := &btpb.RowFilter{Filter: &btpb.RowFilter_PassAllFilter{PassAllFilter: true}}
	sink := &btpb.RowFilter{Filter: &btpb.RowFilter_Sink{Sink: true}}
	fam := &btpb.RowFilter{Filter: &btpb.RowFilter_FamilyNameRegexFilter{FamilyNameRegexFilter: "A"}}
	qual := &btpb.RowFilter{Filter: &btpb.RowFilter_ColumnQualifierRegexFilter{ColumnQualifierRegexFilter: []byte("y")}}
	chain := func(f ...*btpb.RowFilter) *btpb.RowFilter {
		return &btpb.RowFilter{Filter: &btpb.RowFilter_Chain_{Chain: &btpb.RowFilter_Chain{Filters: f}}}
	}
	inter := func(f ...*btpb.RowFilter) *btpb.RowFilter {
		return &btpb.RowFilter{Filter: &btpb.RowFilter_Interleave_{Interleave: &btpb.RowFilter_Interleave{Filters: f}}}
	}

	resps, err := readRaw(t, e, noMD, &btpb.ReadRowsRequest{
		TableName: tablePath,
		Filter:    chain(fam, inter(pass, chain(label("foo"), sink)), qual),
	})
	require.NoError(t, err)
	require.Len(t, resps, 1)

	var got []string

	for _, c := range resps[0].GetChunks() {
		got = append(got, string(c.GetValue())+":"+strconv.Itoa(len(c.GetLabels())))
	}

	assert.Equal(t, []string{"1:1", "2:0", "2:1"}, got,
		"the sink keeps labelled copies of cells the qualifier filter later drops")
}

func TestPingAndWarm(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)

	tests := []struct {
		name string
		req  *btpb.PingAndWarmRequest
		want codes.Code
	}{
		{"existing instance", &btpb.PingAndWarmRequest{Name: "projects/" + project + "/instances/" + instance}, codes.OK},
		{"missing instance", &btpb.PingAndWarmRequest{Name: "projects/" + project + "/instances/ghost-inst"}, codes.NotFound},
		{"malformed name", &btpb.PingAndWarmRequest{Name: "nonsense"}, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := btpb.NewBigtableClient(e.conn).PingAndWarm(t.Context(), tt.req)
			assert.Equal(t, tt.want, code(err))
		})
	}
}

func TestSampleRowKeysOffsets(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)
	require.NoError(t, e.ac.CreatePresplitTable(t.Context(), "t", []string{"m"}))
	require.NoError(t, e.ac.CreateColumnFamily(t.Context(), "t", "cf"))

	tbl := e.client(t).Open("t")
	put(t, tbl, seedCell{"a", "cf", "q", "12345", 1000}, seedCell{"z", "cf", "q", "123", 1000})

	stream, err := btpb.NewBigtableClient(e.conn).SampleRowKeys(t.Context(), &btpb.SampleRowKeysRequest{TableName: tablePath})
	require.NoError(t, err)

	var got []*btpb.SampleRowKeysResponse

	for {
		r, rerr := stream.Recv()
		if rerr == io.EOF {
			break
		}

		require.NoError(t, rerr)

		got = append(got, r)
	}

	require.Len(t, got, 2)
	assert.Equal(t, "m", string(got[0].GetRowKey()))
	assert.EqualValues(t, len("cfq12345"), got[0].GetOffsetBytes())
	assert.Empty(t, got[1].GetRowKey(), "the last sample is the end of the table")
	assert.EqualValues(t, len("cfq12345")+len("cfq123"), got[1].GetOffsetBytes())
}

func TestTableViews(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.seeded(t, "t", "cf")

	tc := adminpb.NewBigtableTableAdminClient(e.conn)

	tests := []struct {
		name     string
		view     adminpb.Table_View
		families bool
		states   bool
	}{
		{"unspecified is schema", adminpb.Table_VIEW_UNSPECIFIED, true, false},
		{"name only", adminpb.Table_NAME_ONLY, false, false},
		{"schema", adminpb.Table_SCHEMA_VIEW, true, false},
		{"replication", adminpb.Table_REPLICATION_VIEW, false, true},
		{"encryption", adminpb.Table_ENCRYPTION_VIEW, false, true},
		{"full", adminpb.Table_FULL, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tc.GetTable(t.Context(), &adminpb.GetTableRequest{Name: tablePath, View: tt.view})
			require.NoError(t, err)
			assert.Equal(t, tablePath, got.GetName())
			assert.Equal(t, tt.families, len(got.GetColumnFamilies()) > 0)
			assert.Equal(t, tt.states, len(got.GetClusterStates()) > 0)

			if tt.states {
				assert.Equal(t, adminpb.Table_ClusterState_READY, got.GetClusterStates()[cluster].GetReplicationState())
			}
		})
	}

	list, err := tc.ListTables(t.Context(), &adminpb.ListTablesRequest{
		Parent: "projects/" + project + "/instances/" + instance,
	})
	require.NoError(t, err)
	require.Len(t, list.GetTables(), 1)
	assert.Empty(t, list.GetTables()[0].GetColumnFamilies(), "listing defaults to names only")
}

func TestListTablesPaging(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)

	for _, id := range []string{"t1", "t2", "t3"} {
		require.NoError(t, e.ac.CreateTable(t.Context(), id))
	}

	tc := adminpb.NewBigtableTableAdminClient(e.conn)
	parent := "projects/" + project + "/instances/" + instance

	p1, err := tc.ListTables(t.Context(), &adminpb.ListTablesRequest{Parent: parent, PageSize: 2})
	require.NoError(t, err)
	require.Len(t, p1.GetTables(), 2)
	require.NotEmpty(t, p1.GetNextPageToken())

	p2, err := tc.ListTables(t.Context(), &adminpb.ListTablesRequest{Parent: parent, PageSize: 2, PageToken: p1.GetNextPageToken()})
	require.NoError(t, err)
	require.Len(t, p2.GetTables(), 1)
	assert.Empty(t, p2.GetNextPageToken())
}

func TestUnsupportedSurface(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.seeded(t, "t", "cf")

	tc := adminpb.NewBigtableTableAdminClient(e.conn)
	ic := adminpb.NewBigtableInstanceAdminClient(e.conn)
	parent := "projects/" + project + "/instances/" + instance

	_, err := tc.ModifyColumnFamilies(t.Context(), &adminpb.ModifyColumnFamiliesRequest{
		Name: tablePath,
		Modifications: []*adminpb.ModifyColumnFamiliesRequest_Modification{{
			Id: "agg",
			Mod: &adminpb.ModifyColumnFamiliesRequest_Modification_Create{Create: &adminpb.ColumnFamily{
				ValueType: &adminpb.Type{Kind: &adminpb.Type_AggregateType{AggregateType: &adminpb.Type_Aggregate{
					InputType:  &adminpb.Type{Kind: &adminpb.Type_Int64Type{Int64Type: &adminpb.Type_Int64{}}},
					Aggregator: &adminpb.Type_Aggregate_Sum_{Sum: &adminpb.Type_Aggregate_Sum{}},
				}}},
			}},
		}},
	})
	assert.Equal(t, codes.Unimplemented, code(err), "aggregate families")

	_, err = tc.SnapshotTable(t.Context(), &adminpb.SnapshotTableRequest{Name: tablePath})
	assert.Equal(t, codes.Unimplemented, code(err))

	_, err = tc.CreateAuthorizedView(t.Context(), &adminpb.CreateAuthorizedViewRequest{Parent: tablePath})
	assert.Equal(t, codes.Unimplemented, code(err))

	_, err = ic.CreateLogicalView(t.Context(), &adminpb.CreateLogicalViewRequest{Parent: parent})
	assert.Equal(t, codes.Unimplemented, code(err))

	_, err = tc.ListBackups(t.Context(), &adminpb.ListBackupsRequest{Parent: parent + "/clusters/" + cluster, Filter: "x"})
	assert.Equal(t, codes.Unimplemented, code(err))
}

func TestOperations(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)

	ic := adminpb.NewBigtableInstanceAdminClient(e.conn)
	ops := longrunningpb.NewOperationsClient(e.conn)
	parent := "projects/" + project + "/instances/" + instance

	op, err := ic.CreateCluster(t.Context(), &adminpb.CreateClusterRequest{
		Parent: parent, ClusterId: "second",
		Cluster: &adminpb.Cluster{Location: "projects/" + project + "/locations/us-east1-b", ServeNodes: 1},
	})
	require.NoError(t, err)
	assert.True(t, op.GetDone())
	assert.Contains(t, op.GetName(), parent+"/operations/")

	var meta adminpb.CreateClusterMetadata
	require.NoError(t, op.GetMetadata().UnmarshalTo(&meta))
	assert.Equal(t, "second", meta.GetOriginalRequest().GetClusterId())

	var cl adminpb.Cluster
	require.NoError(t, op.GetResponse().UnmarshalTo(&cl))
	assert.Equal(t, parent+"/clusters/second", cl.GetName())

	got, err := ops.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: op.GetName()})
	require.NoError(t, err)
	assert.Equal(t, op.GetName(), got.GetName())

	list, err := ops.ListOperations(t.Context(), &longrunningpb.ListOperationsRequest{Name: parent})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(list.GetOperations()), 2, "instance creation and cluster creation")

	_, err = ops.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: parent + "/operations/ghost"})
	assert.Equal(t, codes.NotFound, code(err))
}
