package gcpfirestore_test

import (
	"testing"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func setWrite(name, value string) *firestorepb.Write {
	return &firestorepb.Write{Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{
		Name:   docRoot + "/" + name,
		Fields: map[string]*firestorepb.Value{"v": {ValueType: &firestorepb.Value_StringValue{StringValue: value}}},
	}}}
}

func TestWriteStream(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()

	stream, err := e.raw.Write(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&firestorepb.WriteRequest{Database: dbRoot}))

	hs, err := stream.Recv()
	require.NoError(t, err)
	require.NotEmpty(t, hs.GetStreamId())

	require.NoError(t, stream.Send(&firestorepb.WriteRequest{
		StreamId: hs.GetStreamId(), StreamToken: hs.GetStreamToken(),
		Writes: []*firestorepb.Write{setWrite("ws/a", "1"), setWrite("ws/b", "2")},
	}))

	r1, err := stream.Recv()
	require.NoError(t, err)
	assert.Len(t, r1.GetWriteResults(), 2)
	assert.NotNil(t, r1.GetCommitTime())
	assert.NotEqual(t, hs.GetStreamToken(), r1.GetStreamToken())

	require.NoError(t, stream.Send(&firestorepb.WriteRequest{
		StreamId:    hs.GetStreamId(),
		StreamToken: r1.GetStreamToken(),
		Writes:      []*firestorepb.Write{setWrite("ws/c", "3")},
	}))

	r2, err := stream.Recv()
	require.NoError(t, err)
	assert.NotEqual(t, r1.GetStreamToken(), r2.GetStreamToken())
	require.NoError(t, stream.CloseSend())

	for _, id := range []string{"a", "b", "c"} {
		snap, gerr := e.client.Collection("ws").Doc(id).Get(ctx)
		require.NoError(t, gerr)
		assert.NotEmpty(t, snap.Data()["v"])
	}

	resumed, err := e.raw.Write(ctx)
	require.NoError(t, err)
	require.NoError(t, resumed.Send(&firestorepb.WriteRequest{
		Database: dbRoot, StreamId: hs.GetStreamId(), StreamToken: r1.GetStreamToken(),
	}))

	replay, err := resumed.Recv()
	require.NoError(t, err)
	assert.Equal(t, r2.GetStreamToken(), replay.GetStreamToken(), "only the unacknowledged response is replayed")
	assert.Len(t, replay.GetWriteResults(), 1)
}

func TestWriteStreamErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req  *firestorepb.WriteRequest
		name string
		want codes.Code
	}{
		{
			name: "first request carries writes", want: codes.InvalidArgument,
			req: &firestorepb.WriteRequest{Database: dbRoot, Writes: []*firestorepb.Write{setWrite("x/1", "v")}},
		},
		{
			name: "unknown stream",
			req:  &firestorepb.WriteRequest{Database: dbRoot, StreamId: "nope"},
			want: codes.NotFound,
		},
		{name: "bad database", req: &firestorepb.WriteRequest{Database: "bad"}, want: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			stream, err := e.raw.Write(t.Context())
			require.NoError(t, err)
			require.NoError(t, stream.Send(tt.req))

			_, err = stream.Recv()
			assert.Equal(t, tt.want, code(err))
		})
	}
}

func TestWriteStreamPreconditionFails(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.seed(t, "dup", map[string]map[string]any{"x": {"v": "1"}})

	stream, err := e.raw.Write(t.Context())
	require.NoError(t, err)
	require.NoError(t, stream.Send(&firestorepb.WriteRequest{Database: dbRoot}))

	hs, err := stream.Recv()
	require.NoError(t, err)

	w := setWrite("dup/x", "2")
	w.CurrentDocument = &firestorepb.Precondition{ConditionType: &firestorepb.Precondition_Exists{Exists: false}}
	require.NoError(
		t,
		stream.Send(&firestorepb.WriteRequest{StreamId: hs.GetStreamId(), Writes: []*firestorepb.Write{w}}),
	)

	_, err = stream.Recv()
	assert.Equal(t, codes.AlreadyExists, code(err))
}
