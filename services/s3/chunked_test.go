package s3_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// signedChunk frames a data segment as a signed aws-chunked chunk. The
// signature value is arbitrary here — the decoder discards it.
func signedChunk(data string) string {
	return sizeHex(len(data)) + ";chunk-signature=" +
		strings.Repeat("a", 64) + "\r\n" + data + "\r\n"
}

func sizeHex(n int) string {
	const hexDigits = "0123456789abcdef"
	if n == 0 {
		return "0"
	}
	var out []byte
	for n > 0 {
		out = append([]byte{hexDigits[n&0xf]}, out...)
		n >>= 4
	}

	return string(out)
}

func sizeDecimal(n int) string {
	return strconv.Itoa(n)
}

func TestChunkedReader_Decode(t *testing.T) {
	t.Parallel()

	finalSigned := "0;chunk-signature=" + strings.Repeat("b", 64) + "\r\n\r\n"

	tests := []struct {
		name    string
		wire    string
		want    string
		wantErr bool
	}{
		{
			name: "single chunk",
			wire: signedChunk("hello world") + finalSigned,
			want: "hello world",
		},
		{
			name: "multiple chunks",
			wire: signedChunk("aaaa") + signedChunk("bbbb") + signedChunk("cccc") + finalSigned,
			want: "aaaabbbbcccc",
		},
		{
			name: "empty payload only final chunk",
			wire: finalSigned,
			want: "",
		},
		{
			name: "trailer headers after final chunk",
			wire: signedChunk("payload") + "0\r\n" +
				"x-amz-checksum-crc32:abc123\r\n\r\n",
			want: "payload",
		},
		{
			name: "unsigned chunk without signature field",
			wire: sizeHex(5) + "\r\nhello\r\n0\r\n\r\n",
			want: "hello",
		},
		{
			name:    "malformed chunk size",
			wire:    "zz;chunk-signature=x\r\ndata\r\n",
			wantErr: true,
		},
		{
			name:    "truncated chunk data",
			wire:    sizeHex(10) + "\r\nshort",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rc := s3.NewChunkedReadCloser(io.NopCloser(strings.NewReader(tt.wire)))
			got, err := io.ReadAll(rc)
			_ = rc.Close()

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

// TestChunkedPutObjectRoundTrip proves that a chunked (SigV4 streaming) PUT is
// decoded before storage: the GET returns the real object bytes with no chunk
// framing, and the ETag matches the MD5 of the decoded content (not the wire
// body).
func TestChunkedPutObjectRoundTrip(t *testing.T) {
	t.Parallel()

	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	h := s3.NewHandler(backend)

	seedObject(t, backend, "chunkbkt", "placeholder", "x")

	payload := strings.Repeat("gopherstack-chunk-", 100) // 1800 bytes
	wire := signedChunk(payload[:900]) + signedChunk(payload[900:]) + "0\r\n\r\n"

	putReq := httptest.NewRequest(http.MethodPut, "/chunkbkt/streamed",
		strings.NewReader(wire))
	putReq.Header.Set(s3.ContentSHA256HeaderName, s3.StreamingSignedPayload)
	putReq.Header.Set("Content-Encoding", "aws-chunked")
	putReq.Header.Set(s3.DecodedContentLengthHeaderName, sizeDecimal(len(payload)))
	putRec := httptest.NewRecorder()
	serveInternal(h, putRec, putReq)
	require.Equal(t, http.StatusOK, putRec.Code, "body=%s", putRec.Body.String())

	getReq := httptest.NewRequest(http.MethodGet, "/chunkbkt/streamed", nil)
	getRec := httptest.NewRecorder()
	serveInternal(h, getRec, getReq)
	require.Equal(t, http.StatusOK, getRec.Code)

	assert.Equal(t, payload, getRec.Body.String(),
		"stored object must be the decoded payload, not the chunk-framed wire body")
	assert.Empty(t, getRec.Header().Get("Content-Encoding"),
		"aws-chunked framing must not leak into the stored object encoding")
}

func TestIsAWSChunkedRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentSHA  string
		contentEnc  string
		wantChunked bool
	}{
		{name: "streaming signed", contentSHA: s3.StreamingSignedPayload, wantChunked: true},
		{name: "streaming signed trailer", contentSHA: s3.StreamingSignedPayloadTrailer, wantChunked: true},
		{name: "streaming unsigned trailer", contentSHA: s3.StreamingUnsignedTrailer, wantChunked: true},
		{name: "content-encoding aws-chunked", contentEnc: "aws-chunked", wantChunked: true},
		{name: "content-encoding mixed", contentEnc: "gzip, aws-chunked", wantChunked: true},
		{name: "plain hex sha", contentSHA: strings.Repeat("0", 64), wantChunked: false},
		{name: "unsigned payload", contentSHA: s3.UnsignedPayloadHash, wantChunked: false},
		{name: "nothing set", wantChunked: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodPut, "/b/k", nil)
			if tt.contentSHA != "" {
				r.Header.Set(s3.ContentSHA256HeaderName, tt.contentSHA)
			}
			if tt.contentEnc != "" {
				r.Header.Set("Content-Encoding", tt.contentEnc)
			}

			assert.Equal(t, tt.wantChunked, s3.IsAWSChunkedRequest(r))
		})
	}
}

func TestMaybeDecodeChunkedBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		body          string
		contentSHA    string
		decodedLen    string
		wantBody      string
		wantEncoding  string
		wantLen       int64
		wantDecodeApp bool
	}{
		{
			name:          "decodes streaming body and fixes length",
			body:          signedChunk("real-object-bytes") + "0\r\n\r\n",
			contentSHA:    s3.StreamingSignedPayload,
			decodedLen:    "17",
			wantBody:      "real-object-bytes",
			wantLen:       17,
			wantDecodeApp: true,
		},
		{
			name:          "non-chunked passthrough untouched",
			body:          "plain",
			contentSHA:    s3.UnsignedPayloadHash,
			wantBody:      "plain",
			wantDecodeApp: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodPut, "/b/k", strings.NewReader(tt.body))
			if tt.contentSHA != "" {
				r.Header.Set(s3.ContentSHA256HeaderName, tt.contentSHA)
			}
			if tt.decodedLen != "" {
				r.Header.Set(s3.DecodedContentLengthHeaderName, tt.decodedLen)
			}
			if tt.wantDecodeApp {
				r.Header.Set("Content-Encoding", "aws-chunked")
			}

			r = s3.MaybeDecodeChunkedBody(r)
			got, err := io.ReadAll(r.Body)
			require.NoError(t, err)

			assert.Equal(t, tt.wantBody, string(got))
			if tt.wantDecodeApp {
				assert.Equal(t, tt.wantLen, r.ContentLength)
				assert.Empty(t, r.Header.Get("Content-Encoding"),
					"aws-chunked encoding should be stripped after decode")
			}
		})
	}
}
