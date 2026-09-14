package kinesis

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildChannelObjectKey(t *testing.T) {
	t.Parallel()

	ch := &Channel{ChannelName: "orders", ChannelID: "abc123"}
	at := time.Date(2026, 7, 20, 14, 30, 0, 0, time.UTC)

	tests := []struct {
		name            string
		tmpl            string
		compressionType string
		wantPrefix      string
		wantSuffix      string
	}{
		{
			name:            "default_template_gzip",
			tmpl:            "",
			compressionType: channelCompressionGzip,
			wantPrefix:      "kinesis-channel/orders/abc123/2026/07/20/14/orders-abc123-2026-07-20-14-30-",
			wantSuffix:      ".gz",
		},
		{
			name:            "default_template_none_has_no_extension",
			tmpl:            "",
			compressionType: channelCompressionNone,
			wantPrefix:      "kinesis-channel/orders/abc123/2026/07/20/14/orders-abc123-2026-07-20-14-30-",
			wantSuffix:      "",
		},
		{
			name:            "custom_template_hive_style_partitioning",
			tmpl:            "data/stream=!{stream-name}/!{yyyy}-!{MM}-!{dd}/!{HH}!{mm}!{extension:.json}",
			compressionType: channelCompressionZstd,
			wantPrefix:      "data/stream=my-stream/2026-07-20/1430",
			wantSuffix:      ".json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key := buildChannelObjectKey(tt.tmpl, ch, "my-stream", tt.compressionType, at)

			assert.True(t, strings.HasPrefix(key, tt.wantPrefix), "key %q should start with %q", key, tt.wantPrefix)
			assert.True(t, strings.HasSuffix(key, tt.wantSuffix), "key %q should end with %q", key, tt.wantSuffix)
		})
	}
}

func TestBuildChannelObjectKey_UniqueSuffix(t *testing.T) {
	t.Parallel()

	ch := &Channel{ChannelName: "orders", ChannelID: "abc123"}
	at := time.Date(2026, 7, 20, 14, 30, 0, 0, time.UTC)

	key1 := buildChannelObjectKey("", ch, "my-stream", channelCompressionNone, at)
	key2 := buildChannelObjectKey("", ch, "my-stream", channelCompressionNone, at)

	assert.NotEqual(t, key1, key2, "each expansion must append a distinct unique suffix")
}

func TestCompressChannelBody(t *testing.T) {
	t.Parallel()

	body := []byte("hello channel delivery world")

	tests := []struct {
		decompress      func(t *testing.T, data []byte) []byte
		name            string
		compressionType string
		wantEncoding    string
	}{
		{
			name:            "none",
			compressionType: channelCompressionNone,
			wantEncoding:    "",
			decompress:      func(_ *testing.T, data []byte) []byte { return data },
		},
		{
			name:            "gzip",
			compressionType: channelCompressionGzip,
			wantEncoding:    "gzip",
			decompress: func(t *testing.T, data []byte) []byte {
				t.Helper()
				r, err := gzip.NewReader(bytes.NewReader(data))
				require.NoError(t, err)
				out, err := io.ReadAll(r)
				require.NoError(t, err)

				return out
			},
		},
		{
			name:            "zstd",
			compressionType: channelCompressionZstd,
			wantEncoding:    "zstd",
			decompress: func(t *testing.T, data []byte) []byte {
				t.Helper()
				r, err := zstd.NewReader(bytes.NewReader(data))
				require.NoError(t, err)
				defer r.Close()
				out, err := io.ReadAll(r)
				require.NoError(t, err)

				return out
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			compressed, encoding, err := compressChannelBody(body, tt.compressionType)
			require.NoError(t, err)

			if tt.wantEncoding == "" {
				assert.Nil(t, encoding)
			} else {
				require.NotNil(t, encoding)
				assert.Equal(t, tt.wantEncoding, *encoding)
			}

			assert.Equal(t, body, tt.decompress(t, compressed))
		})
	}
}

func TestValidateChannelRecord(t *testing.T) {
	t.Parallel()

	invalidUTF8 := []byte{0xff, 0xfe, 0xfd}

	tests := []struct {
		name       string
		formatType string
		data       []byte
		wantValid  bool
	}{
		{name: "string_valid_utf8", formatType: recordFormatTypeString, data: []byte("hello"), wantValid: true},
		{name: "string_invalid_utf8", formatType: recordFormatTypeString, data: invalidUTF8, wantValid: false},
		{name: "json_valid", formatType: recordFormatTypeJSON, data: []byte(`{"a":1}`), wantValid: true},
		{name: "json_invalid", formatType: recordFormatTypeJSON, data: []byte(`{not json`), wantValid: false},
		{name: "byte_array_never_validated", formatType: recordFormatTypeByteArray, data: invalidUTF8, wantValid: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			errMsg := validateChannelRecord(tt.formatType, tt.data)
			if tt.wantValid {
				assert.Empty(t, errMsg)
			} else {
				assert.NotEmpty(t, errMsg)
			}
		})
	}
}

func TestChannelFlushDue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		buf              *channelBuffer
		name             string
		freshnessSeconds int
		want             bool
	}{
		{
			name:             "empty_buffer_never_due",
			buf:              &channelBuffer{lastFlush: time.Now().Add(-time.Hour)},
			freshnessSeconds: 300,
			want:             false,
		},
		{
			name: "within_window_not_due",
			buf: &channelBuffer{
				records:   [][]byte{[]byte("r")},
				lastFlush: time.Now(),
			},
			freshnessSeconds: 300,
			want:             false,
		},
		{
			name: "past_window_due",
			buf: &channelBuffer{
				records:   [][]byte{[]byte("r")},
				lastFlush: time.Now().Add(-10 * time.Minute),
			},
			freshnessSeconds: 300,
			want:             true,
		},
		{
			name: "failed_only_buffer_also_due",
			buf: &channelBuffer{
				failed:    []failedChannelRecord{{errorMessage: "bad"}},
				lastFlush: time.Now().Add(-10 * time.Minute),
			},
			freshnessSeconds: 300,
			want:             true,
		},
		{
			name: "zero_freshness_uses_default_300s",
			buf: &channelBuffer{
				records:   [][]byte{[]byte("r")},
				lastFlush: time.Now().Add(-10 * time.Minute),
			},
			freshnessSeconds: 0,
			want:             true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, channelFlushDue(tt.buf, tt.freshnessSeconds))
		})
	}
}
