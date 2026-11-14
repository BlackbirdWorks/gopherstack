package gcplogging_test

import (
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestWriteTimestampBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		offset time.Duration
		code   codes.Code
	}{
		{name: "within a day", offset: 23 * time.Hour, code: codes.OK},
		{name: "past is accepted", offset: -90 * 24 * time.Hour, code: codes.OK},
		{name: "beyond a day", offset: 25 * time.Hour, code: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			_, err := e.raw(t).WriteLogEntries(t.Context(), &loggingpb.WriteLogEntriesRequest{
				Resource: global(),
				Entries:  []*loggingpb.LogEntry{textEntry("ts", "x", 0, e.clk.now().Add(tt.offset))},
			})
			assert.Equal(t, tt.code, status.Code(err))
		})
	}
}

func TestWriteTruncatesLabels(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	in := textEntry("lbl", "x", 0, e.clk.now())
	in.Labels = map[string]string{strings.Repeat("k", 600): strings.Repeat("v", 70<<10), "ok": "fine"}
	e.write(t, in)

	got := e.list(
		t,
		&loggingpb.ListLogEntriesRequest{ResourceNames: []string{proj}, Filter: `logName="` + proj + `/logs/lbl"`},
	)
	require.Len(t, got, 1)
	assert.Equal(t, "fine", got[0].GetLabels()["ok"])
	assert.Equal(t, strings.Repeat("v", 64<<10), got[0].GetLabels()[strings.Repeat("k", 512)])
	assert.Len(t, got[0].GetLabels(), 2)
}

func TestListLogEntriesPageSize(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	entries := make([]*loggingpb.LogEntry, 0, 60)

	for range 60 {
		entries = append(entries, textEntry("pg", "x", 0, e.clk.now()))
	}

	e.write(t, entries...)

	tests := []struct {
		name string
		size int32
		code codes.Code
		want int
	}{
		{name: "default is 50", size: 0, code: codes.OK, want: 50},
		{name: "explicit", size: 7, code: codes.OK, want: 7},
		{name: "negative", size: -1, code: codes.InvalidArgument},
		{name: "above max", size: 1001, code: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := e.raw(t).ListLogEntries(t.Context(), &loggingpb.ListLogEntriesRequest{
				ResourceNames: []string{proj}, PageSize: tt.size,
			}).Next()
			require.Equal(t, tt.code, status.Code(err))

			if tt.code == codes.OK {
				assert.NotNil(t, resp)
			}
		})
	}
}

func TestTailBufferWindowBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		window time.Duration
		code   codes.Code
	}{
		{name: "max", window: time.Minute, code: codes.OK},
		{name: "above max", window: time.Minute + time.Millisecond, code: codes.InvalidArgument},
		{name: "negative", window: -time.Second, code: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			stream, err := e.raw(t).TailLogEntries(t.Context())
			require.NoError(t, err)
			require.NoError(t, stream.Send(&loggingpb.TailLogEntriesRequest{
				ResourceNames: []string{proj}, BufferWindow: durationpb.New(tt.window),
			}))

			if tt.code != codes.OK {
				_, err = stream.Recv()
				assert.Equal(t, tt.code, status.Code(err))
			}
		})
	}
}
