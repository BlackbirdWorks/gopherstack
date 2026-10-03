package compress_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/compress"
)

func allEnabled(compress.Encoding) bool { return true }

func TestNegotiate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		want   compress.Encoding
		values []string
		only   []compress.Encoding
	}{
		{name: "no header", want: ""},
		{name: "identity", values: []string{"identity"}, want: ""},
		{name: "empty value", values: []string{""}, want: ""},
		{name: "gzip", values: []string{"gzip"}, want: compress.Gzip},
		{name: "tie prefers zstd", values: []string{"gzip, br, zstd"}, want: compress.Zstd},
		{name: "tie br over gzip", values: []string{"gzip, br"}, want: compress.Brotli},
		{name: "browser", values: []string{"gzip, deflate, br, zstd"}, want: compress.Zstd},
		{name: "q beats preference", values: []string{"zstd;q=0.5, gzip;q=0.9"}, want: compress.Gzip},
		{name: "q zero excluded", values: []string{"zstd;q=0, br"}, want: compress.Brotli},
		{name: "all excluded", values: []string{"zstd;q=0, br;q=0, gzip;q=0"}, want: ""},
		{name: "star", values: []string{"*"}, want: compress.Zstd},
		{name: "star q zero", values: []string{"*;q=0"}, want: ""},
		{name: "star with exclusion", values: []string{"zstd;q=0, *"}, want: compress.Brotli},
		{name: "identity preferred", values: []string{"gzip;q=0.5, identity;q=1"}, want: ""},
		{name: "identity q0 still compresses", values: []string{"gzip, identity;q=0"}, want: compress.Gzip},
		{name: "unsupported only", values: []string{"deflate, compress"}, want: ""},
		{name: "unsupported plus gzip", values: []string{"deflate, gzip;q=0.1"}, want: compress.Gzip},
		{name: "x-gzip alias", values: []string{"x-gzip"}, want: compress.Gzip},
		{name: "case and spaces", values: []string{" GZIP ; Q=0.8 "}, want: compress.Gzip},
		{name: "bad q ignored", values: []string{"gzip;q=abc, br"}, want: compress.Brotli},
		{name: "q clamped", values: []string{"gzip;q=7"}, want: compress.Gzip},
		{name: "multiple header lines", values: []string{"gzip", "br"}, want: compress.Brotli},
		{
			name:   "disabled zstd",
			values: []string{"zstd, gzip"},
			only:   []compress.Encoding{compress.Gzip},
			want:   compress.Gzip,
		},
		{
			name:   "star respects disabled",
			values: []string{"*"},
			only:   []compress.Encoding{compress.Gzip},
			want:   compress.Gzip,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			enabled := allEnabled
			if tt.only != nil {
				enabled = func(e compress.Encoding) bool { return slices.Contains(tt.only, e) }
			}

			assert.Equal(t, tt.want, compress.Negotiate(tt.values, enabled))
		})
	}
}

func TestCompressible(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ct   string
		want bool
	}{
		{"text/html; charset=utf-8", true},
		{"text/css", true},
		{"application/json", true},
		{"application/javascript", true},
		{"application/x-amz-json-1.0", true},
		{"application/x-amz-json-1.1", true},
		{"application/xml", true},
		{"image/svg+xml", true},
		{"application/wasm", true},
		{"application/cbor", true},
		{"application/problem+json", true},
		{"APPLICATION/JSON", true},
		{"text/event-stream", false},
		{"application/vnd.amazon.eventstream", false},
		{"application/grpc", false},
		{"application/grpc+proto", false},
		{"application/connect+json", false},
		{"application/connect+proto", false},
		{"application/octet-stream", false},
		{"image/png", false},
		{"application/zip", false},
		{"font/woff2", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.ct, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, compress.Compressible(tt.ct))
		})
	}
}
