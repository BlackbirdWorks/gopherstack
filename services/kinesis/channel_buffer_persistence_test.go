package kinesis //nolint:testpackage // seeds the unexported channelBuffers directly.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChannelBuffers_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		buf  *channelBuffer
		name string
		want bool
	}{
		{name: "records", buf: &channelBuffer{records: [][]byte{[]byte("a"), []byte("b")}}, want: true},
		{
			name: "failed",
			buf: &channelBuffer{failed: []failedChannelRecord{
				{streamARN: "arn", shardID: "shardId-0", sequenceNumber: "1", errorMessage: "bad"},
			}},
			want: true,
		},
		{name: "empty", buf: &channelBuffer{}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := NewInMemoryBackend()
			src.channelBuffers["chan-arn"] = tt.buf

			dst := NewInMemoryBackend()
			require.NoError(t, dst.Restore(context.Background(), src.Snapshot(context.Background())))

			got, ok := dst.channelBuffers["chan-arn"]
			assert.Equal(t, tt.want, ok)

			if tt.want {
				assert.Equal(t, tt.buf.records, got.records)
				assert.Equal(t, tt.buf.failed, got.failed)
			}
		})
	}
}
