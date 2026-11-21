package gcpbigtable_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/services/gcpbigtable"
)

func csNow() time.Time { return time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC) }

func streamEnv(t *testing.T) (*env, *bigtable.Table, btpb.BigtableClient) {
	t.Helper()

	b := gcpbigtable.NewInMemoryBackend()
	b.SetClock(csNow)

	e := newEnvWithBackend(t, b)
	e.createInstance(t, instance)

	conf := tableConf("t", "cf")
	conf.ChangeStreamRetention = bigtable.ChangeStreamRetention(48 * time.Hour)
	require.NoError(t, e.ac.CreateTableFromConf(t.Context(), conf))

	return e, e.client(t).Open("t"), btpb.NewBigtableClient(e.conn)
}

const tableRes = "projects/" + project + "/instances/" + instance + "/tables/t"

func wholeTable() *btpb.StreamPartition {
	return &btpb.StreamPartition{RowRange: &btpb.RowRange{
		StartKey: &btpb.RowRange_StartKeyClosed{StartKeyClosed: []byte("")},
	}}
}

func drain(t *testing.T, s btpb.Bigtable_ReadChangeStreamClient) []*btpb.ReadChangeStreamResponse {
	t.Helper()

	var out []*btpb.ReadChangeStreamResponse

	for {
		r, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return out
		}

		require.NoError(t, err)
		out = append(out, r)
	}
}

func changes(rs []*btpb.ReadChangeStreamResponse) []*btpb.ReadChangeStreamResponse_DataChange {
	var out []*btpb.ReadChangeStreamResponse_DataChange

	for _, r := range rs {
		if dc := r.GetDataChange(); dc != nil {
			out = append(out, dc)
		}
	}

	return out
}

func TestReadChangeStreamDeliversCommittedChanges(t *testing.T) {
	t.Parallel()

	_, tbl, dc := streamEnv(t)
	put(t, tbl, seedCell{"a1", "cf", "q", "v1", 1000}, seedCell{"b1", "cf", "q", "v2", 2000})

	del := bigtable.NewMutation()
	del.DeleteRow()
	require.NoError(t, tbl.Apply(t.Context(), "a1", del))

	tests := []struct {
		part *btpb.StreamPartition
		name string
		keys []string
	}{
		{name: "whole table", part: wholeTable(), keys: []string{"a1", "b1", "a1"}},
		{name: "range", keys: []string{"b1"}, part: &btpb.StreamPartition{RowRange: &btpb.RowRange{
			StartKey: &btpb.RowRange_StartKeyClosed{StartKeyClosed: []byte("b")},
			EndKey:   &btpb.RowRange_EndKeyOpen{EndKeyOpen: []byte("c")},
		}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, err := dc.ReadChangeStream(t.Context(), &btpb.ReadChangeStreamRequest{
				TableName: tableRes, Partition: tt.part,
				StartFrom: &btpb.ReadChangeStreamRequest_StartTime{StartTime: timestamppb.New(csNow().Add(-time.Hour))},
				EndTime:   timestamppb.New(csNow()),
			})
			require.NoError(t, err)

			got := changes(drain(t, s))

			keys := make([]string, len(got))
			for i, c := range got {
				keys[i] = string(c.GetRowKey())
				assert.Equal(t, btpb.ReadChangeStreamResponse_DataChange_USER, c.GetType())
				assert.Equal(t, cluster, c.GetSourceClusterId())
				assert.True(t, c.GetDone())
				assert.NotEmpty(t, c.GetToken())
				assert.Equal(t, csNow().UnixMicro(), c.GetCommitTimestamp().AsTime().UnixMicro())
			}

			assert.Equal(t, tt.keys, keys)
		})
	}

	s, err := dc.ReadChangeStream(t.Context(), &btpb.ReadChangeStreamRequest{
		TableName: tableRes, Partition: wholeTable(),
		StartFrom: &btpb.ReadChangeStreamRequest_StartTime{StartTime: timestamppb.New(csNow().Add(-time.Hour))},
		EndTime:   timestamppb.New(csNow()),
	})
	require.NoError(t, err)

	all := changes(drain(t, s))
	require.Len(t, all, 3)
	assert.Equal(t, []byte("v1"), all[0].GetChunks()[0].GetMutation().GetSetCell().GetValue())
	assert.NotNil(t, all[2].GetChunks()[0].GetMutation().GetDeleteFromRow())
}

func TestReadChangeStreamResumesFromToken(t *testing.T) {
	t.Parallel()

	_, tbl, dc := streamEnv(t)
	put(t, tbl, seedCell{"a1", "cf", "q", "v1", 1000}, seedCell{"a2", "cf", "q", "v2", 2000})

	first, err := dc.ReadChangeStream(t.Context(), &btpb.ReadChangeStreamRequest{
		TableName: tableRes, Partition: wholeTable(),
		StartFrom: &btpb.ReadChangeStreamRequest_StartTime{StartTime: timestamppb.New(csNow().Add(-time.Hour))},
		EndTime:   timestamppb.New(csNow()),
	})
	require.NoError(t, err)

	got := changes(drain(t, first))
	require.Len(t, got, 2)

	put(t, tbl, seedCell{"a3", "cf", "q", "v3", 3000})

	resumed, err := dc.ReadChangeStream(t.Context(), &btpb.ReadChangeStreamRequest{
		TableName: tableRes, Partition: wholeTable(),
		StartFrom: &btpb.ReadChangeStreamRequest_ContinuationTokens{ContinuationTokens: &btpb.StreamContinuationTokens{
			Tokens: []*btpb.StreamContinuationToken{{Partition: wholeTable(), Token: got[0].GetToken()}},
		}},
		EndTime: timestamppb.New(csNow()),
	})
	require.NoError(t, err)

	keys := make([]string, 0, 3)
	for _, c := range changes(drain(t, resumed)) {
		keys = append(keys, string(c.GetRowKey()))
	}

	assert.Equal(t, []string{"a2", "a3"}, keys)
}

func TestReadChangeStreamLiveDelivery(t *testing.T) {
	t.Parallel()

	_, tbl, dc := streamEnv(t)

	s, err := dc.ReadChangeStream(t.Context(), &btpb.ReadChangeStreamRequest{
		TableName: tableRes, Partition: wholeTable(),
		StartFrom:         &btpb.ReadChangeStreamRequest_StartTime{StartTime: timestamppb.New(csNow())},
		HeartbeatDuration: durationpb.New(10 * time.Millisecond),
	})
	require.NoError(t, err)

	hb, err := s.Recv()
	require.NoError(t, err)
	require.NotNil(t, hb.GetHeartbeat())
	assert.NotEmpty(t, hb.GetHeartbeat().GetContinuationToken().GetToken())

	put(t, tbl, seedCell{"live", "cf", "q", "v", 1000})

	for {
		r, rerr := s.Recv()
		require.NoError(t, rerr)

		if c := r.GetDataChange(); c != nil {
			assert.Equal(t, "live", string(c.GetRowKey()))

			return
		}
	}
}

func TestReadChangeStreamErrors(t *testing.T) {
	t.Parallel()

	start := &btpb.ReadChangeStreamRequest_StartTime{StartTime: timestamppb.New(csNow().Add(-time.Hour))}

	tests := []struct {
		req  *btpb.ReadChangeStreamRequest
		name string
		want codes.Code
	}{
		{
			name: "no partition", want: codes.InvalidArgument,
			req: &btpb.ReadChangeStreamRequest{TableName: tableRes, StartFrom: start},
		},
		{
			name: "no start", want: codes.InvalidArgument,
			req: &btpb.ReadChangeStreamRequest{TableName: tableRes, Partition: wholeTable()},
		},
		{name: "retention exceeded", want: codes.InvalidArgument, req: &btpb.ReadChangeStreamRequest{
			TableName: tableRes, Partition: wholeTable(),
			StartFrom: &btpb.ReadChangeStreamRequest_StartTime{
				StartTime: timestamppb.New(csNow().Add(-72 * time.Hour)),
			},
		}},
		{name: "bad token", want: codes.InvalidArgument, req: &btpb.ReadChangeStreamRequest{
			TableName: tableRes, Partition: wholeTable(),
			StartFrom: &btpb.ReadChangeStreamRequest_ContinuationTokens{
				ContinuationTokens: &btpb.StreamContinuationTokens{
					Tokens: []*btpb.StreamContinuationToken{{Partition: wholeTable(), Token: "!!"}},
				},
			},
		}},
		{name: "bad heartbeat", want: codes.InvalidArgument, req: &btpb.ReadChangeStreamRequest{
			TableName: tableRes, Partition: wholeTable(), StartFrom: start,
			HeartbeatDuration: durationpb.New(-time.Second),
		}},
		{name: "missing table", want: codes.NotFound, req: &btpb.ReadChangeStreamRequest{
			TableName: tableRes + "x", Partition: wholeTable(), StartFrom: start,
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, _, dc := streamEnv(t)

			s, err := dc.ReadChangeStream(t.Context(), tt.req)
			require.NoError(t, err)

			_, err = s.Recv()
			assert.Equal(t, tt.want, code(err))
		})
	}
}

func TestChangeStreamNeedsEnabledTable(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	_ = e.seeded(t, "plain", "cf")
	dc := btpb.NewBigtableClient(e.conn)
	res := "projects/" + project + "/instances/" + instance + "/tables/plain"

	s, err := dc.ReadChangeStream(t.Context(), &btpb.ReadChangeStreamRequest{
		TableName: res, Partition: wholeTable(),
		StartFrom: &btpb.ReadChangeStreamRequest_StartTime{StartTime: timestamppb.New(time.Now())},
	})
	require.NoError(t, err)

	_, err = s.Recv()
	assert.Equal(t, codes.FailedPrecondition, code(err))

	p, err := dc.GenerateInitialChangeStreamPartitions(
		t.Context(),
		&btpb.GenerateInitialChangeStreamPartitionsRequest{TableName: res},
	)
	require.NoError(t, err)

	_, err = p.Recv()
	assert.Equal(t, codes.FailedPrecondition, code(err))
}

func TestGenerateInitialChangeStreamPartitions(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createInstance(t, instance)

	conf := tableConf("t", "cf")
	conf.ChangeStreamRetention = bigtable.ChangeStreamRetention(48 * time.Hour)
	conf.SplitKeys = []string{"g", "p"}
	require.NoError(t, e.ac.CreateTableFromConf(t.Context(), conf))

	s, err := btpb.NewBigtableClient(e.conn).GenerateInitialChangeStreamPartitions(t.Context(),
		&btpb.GenerateInitialChangeStreamPartitionsRequest{TableName: tableRes})
	require.NoError(t, err)

	var got [][2]string

	for {
		r, rerr := s.Recv()
		if errors.Is(rerr, io.EOF) {
			break
		}

		require.NoError(t, rerr)

		rr := r.GetPartition().GetRowRange()
		got = append(got, [2]string{string(rr.GetStartKeyClosed()), string(rr.GetEndKeyOpen())})
	}

	assert.Equal(t, [][2]string{{"", "g"}, {"g", "p"}, {"p", ""}}, got)
}

func TestChangeStreamPersistence(t *testing.T) {
	t.Parallel()

	src, tbl, _ := streamEnv(t)
	put(t, tbl, seedCell{"a1", "cf", "q", "v1", 1000})

	b := gcpbigtable.NewInMemoryBackend()
	b.SetClock(csNow)

	dst := newEnvWithBackend(t, b)
	require.NoError(t, dst.handler.Backend.Restore(t.Context(), src.handler.Backend.Snapshot(t.Context())))

	s, err := btpb.NewBigtableClient(dst.conn).ReadChangeStream(t.Context(), &btpb.ReadChangeStreamRequest{
		TableName: tableRes, Partition: wholeTable(),
		StartFrom: &btpb.ReadChangeStreamRequest_StartTime{StartTime: timestamppb.New(csNow().Add(-time.Hour))},
		EndTime:   timestamppb.New(csNow()),
	})
	require.NoError(t, err)

	got := changes(drain(t, s))
	require.Len(t, got, 1)
	assert.Equal(t, "a1", string(got[0].GetRowKey()))
}
