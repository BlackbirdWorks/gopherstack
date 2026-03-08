package firehose

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildS3Key(t *testing.T) {
	t.Parallel()

	ts := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		name       string
		prefix     string
		streamName string
		wantPrefix string
		wantSuffix string
	}{
		{
			name:       "no_prefix",
			prefix:     "",
			streamName: "my-stream",
			wantPrefix: "2024/03/15/10/",
			wantSuffix: "my-stream-2024-03-15-10-30-00",
		},
		{
			name:       "with_prefix",
			prefix:     "logs",
			streamName: "my-stream",
			wantPrefix: "logs/2024/03/15/10/",
			wantSuffix: "my-stream-2024-03-15-10-30-00",
		},
		{
			name:       "prefix_with_trailing_slash",
			prefix:     "data/",
			streamName: "events",
			wantPrefix: "data/2024/03/15/10/",
			wantSuffix: "events-2024-03-15-10-30-00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			key := buildS3Key(tt.prefix, tt.streamName, ts)
			assert.True(t, strings.HasPrefix(key, tt.wantPrefix), "key=%q wantPrefix=%q", key, tt.wantPrefix)
			assert.True(t, strings.HasSuffix(key, tt.wantSuffix), "key=%q wantSuffix=%q", key, tt.wantSuffix)
		})
	}
}

func TestBucketFromARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arn      string
		wantName string
	}{
		{
			name:     "standard_s3_arn",
			arn:      "arn:aws:s3:::my-bucket",
			wantName: "my-bucket",
		},
		{
			name:     "arn_with_hyphens",
			arn:      "arn:aws:s3:::my-bucket-name",
			wantName: "my-bucket-name",
		},
		{
			name:     "plain_bucket_name",
			arn:      "my-bucket",
			wantName: "my-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := bucketFromARN(tt.arn)
			assert.Equal(t, tt.wantName, got)
		})
	}
}

func TestGzipCompress(t *testing.T) {
	t.Parallel()

	data := []byte("hello world, this is a test of gzip compression")
	compressed, err := gzipCompress(data)
	require.NoError(t, err)
	assert.NotEmpty(t, compressed)
	// GZIP magic bytes.
	assert.Equal(t, []byte{0x1f, 0x8b}, compressed[:2])
	assert.Less(t, len(compressed), len(data)+100) // compressed may be larger for small data, allow overhead
}
