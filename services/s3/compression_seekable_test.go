package s3_test

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

const seekFrame = 256 << 10

type rangeCase struct {
	name       string
	header     string
	start, end int
	status     int
}

func span(name string, start, end int) rangeCase {
	return rangeCase{name, fmt.Sprintf("bytes=%d-%d", start, end), start, end, http.StatusPartialContent}
}

func rangeCases(size int) []rangeCase {
	last := size - 1

	return []rangeCase{
		span("first byte", 0, 0),
		span("last byte", last, last),
		span("whole", 0, last),
		span("4k head", 0, 4095),
		span("4k middle", size/2, size/2+4095),
		span("4k tail", last-4095, last),
		span("frame boundary end", 0, seekFrame-1),
		span("frame boundary start", seekFrame, seekFrame+9),
		span("straddle boundary", seekFrame-5, seekFrame+5),
		span("spans many frames", seekFrame/2, 3*seekFrame+17),
		{"suffix", "bytes=-1000", size - 1000, last, http.StatusPartialContent},
		{"suffix one", "bytes=-1", last, last, http.StatusPartialContent},
		{"suffix over size", fmt.Sprintf("bytes=-%d", size+10), 0, last, http.StatusPartialContent},
		{"open", fmt.Sprintf("bytes=%d-", size-70000), size - 70000, last, http.StatusPartialContent},
		{"open from zero", "bytes=0-", 0, last, http.StatusPartialContent},
		{"end clamped", fmt.Sprintf("bytes=%d-%d", size-10, size+500), size - 10, last, http.StatusPartialContent},
		{"unsatisfiable", fmt.Sprintf("bytes=%d-", size), 0, 0, http.StatusRequestedRangeNotSatisfiable},
		{"malformed ignored", "bytes=9-3", 0, last, http.StatusOK},
		{"unknown unit ignored", "items=0-3", 0, last, http.StatusOK},
	}
}

func seekPayload(size int) []byte {
	return benchPayload("json", size)
}

func TestZstdSeekable_DecompressRange(t *testing.T) {
	t.Parallel()

	size := 3*seekFrame + 777 + (1 << 20)
	data := seekPayload(size)
	c := &s3.ZstdCompressor{}

	blob, err := c.Compress(data)
	require.NoError(t, err)
	require.Less(t, len(blob), size)

	t.Run("full decodes", func(t *testing.T) {
		t.Parallel()

		got, dErr := c.Decompress(blob)
		require.NoError(t, dErr)
		assert.True(t, bytes.Equal(data, got))
	})

	t.Run("plain zstd reader decodes", func(t *testing.T) {
		t.Parallel()

		dec, dErr := zstd.NewReader(nil)
		require.NoError(t, dErr)
		defer dec.Close()

		got, dErr := dec.DecodeAll(blob, nil)
		require.NoError(t, dErr)
		assert.True(t, bytes.Equal(data, got))
	})

	for _, tt := range rangeCases(size) {
		if tt.status != http.StatusPartialContent {
			continue
		}

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok, rErr := c.DecompressRange(blob, int64(size), int64(tt.start), int64(tt.end))
			require.NoError(t, rErr)
			require.True(t, ok)
			assert.True(t, bytes.Equal(data[tt.start:tt.end+1], got))
		})
	}

	t.Run("every frame edge", func(t *testing.T) {
		t.Parallel()

		for f := seekFrame; f < size; f += seekFrame {
			for _, d := range []int{-1, 0, 1} {
				pos := f + d
				got, ok, rErr := c.DecompressRange(blob, int64(size), int64(pos), int64(pos))
				require.NoError(t, rErr)
				require.True(t, ok)
				require.Equal(t, data[pos], got[0], "pos %d", pos)
			}
		}
	})
}

func TestZstdSeekable_NotIndexed(t *testing.T) {
	t.Parallel()

	data := seekPayload(512 << 10)
	single, err := (&s3.ZstdCompressor{}).Compress(data)
	require.NoError(t, err)

	gz, err := (&s3.GzipCompressor{}).Compress(data)
	require.NoError(t, err)

	tests := []struct {
		name string
		blob []byte
		size int64
	}{
		{"single frame zstd", single, int64(len(data))},
		{"legacy gzip", gz, int64(len(data))},
		{"empty", nil, 0},
		{"garbage", []byte("not a blob at all, not compressed"), 33},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, ok, rErr := (&s3.ZstdCompressor{}).DecompressRange(tt.blob, tt.size, 0, 0)
			require.NoError(t, rErr)
			assert.False(t, ok)
		})
	}
}

func TestZstdSeekable_CorruptIndex(t *testing.T) {
	t.Parallel()

	size := 3*seekFrame + 100 + (1 << 20)
	data := seekPayload(size)
	c := &s3.ZstdCompressor{}

	blob, err := c.Compress(data)
	require.NoError(t, err)

	n := len(blob)
	frames := int(binary.LittleEndian.Uint32(blob[n-9:]))
	tableStart := n - 9 - frames*8

	tests := []struct {
		name  string
		off   int
		val   uint32
		trunc int
	}{
		{"bad seekable magic", n - 4, 0, 0},
		{"reserved descriptor bits", n - 5, 0x04, 0},
		{"zero frames", n - 9, 0, 0},
		{"huge frame count", n - 9, 1 << 30, 0},
		{"frame count off by one", n - 9, uint32(frames + 1), 0},
		{"bad skippable size", tableStart - 4, 5, 0},
		{"bad skippable magic", tableStart - 8, 0, 0},
		{"raw size mismatch", tableStart + 4, 1, 0},
		{"huge raw size", tableStart + 4, 1 << 31, 0},
		{"comp size past blob", tableStart, 1 << 30, 0},
		{"comp size below magic", tableStart, 2, 0},
		{"truncated footer", 0, 0, n - 3},
		{"truncated blob", 0, 0, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mutated := bytes.Clone(blob)
			switch {
			case tt.trunc > 0:
				mutated = mutated[:tt.trunc]
			case tt.name == "reserved descriptor bits":
				mutated[tt.off] = byte(tt.val)
			default:
				binary.LittleEndian.PutUint32(mutated[tt.off:], tt.val)
			}

			_, ok, rErr := c.DecompressRange(mutated, int64(size), 5, 100)
			assert.False(t, ok)
			require.NoError(t, rErr)

			assert.NotPanics(t, func() { _, _ = c.Decompress(mutated) })
		})
	}

	t.Run("corrupt frame fails only its window", func(t *testing.T) {
		t.Parallel()

		mutated := bytes.Clone(blob)
		off := int(binary.LittleEndian.Uint32(mutated[tableStart:])) // start of frame 1
		for i := range 64 {
			mutated[off+8+i] ^= 0xa5
		}

		_, ok, rErr := c.DecompressRange(mutated, int64(size), int64(seekFrame+3), int64(seekFrame+9))
		assert.True(t, ok)
		require.Error(t, rErr)

		got, ok, rErr := c.DecompressRange(mutated, int64(size), 0, 99)
		require.NoError(t, rErr)
		require.True(t, ok)
		assert.True(t, bytes.Equal(data[:100], got))
	})
}

type seekFixture struct {
	handler *s3.S3Handler
	backend *s3.InMemoryBackend
	data    []byte
}

func putSeekPlain(t *testing.T, c s3.Compressor, size int) seekFixture {
	t.Helper()

	backend := s3.NewInMemoryBackend(c).WithSkipMultipartSizeCheck()
	handler := s3.NewHandler(backend).WithJanitor(s3.Settings{})
	mustCreateBucket(t, backend, "bkt")

	data := seekPayload(size)
	mustPutObject(t, backend, "bkt", "obj", data)

	return seekFixture{handler: handler, backend: backend, data: data}
}

func putSeekMultipart(t *testing.T, partSizes []int) seekFixture {
	t.Helper()

	backend := s3.NewInMemoryBackend(&s3.ZstdCompressor{}).WithSkipMultipartSizeCheck()
	handler := s3.NewHandler(backend).WithJanitor(s3.Settings{})
	mustCreateBucket(t, backend, "bkt")

	total := 0
	for _, p := range partSizes {
		total += p
	}

	data := seekPayload(total)
	up, err := backend.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
		Bucket: aws.String("bkt"), Key: aws.String("obj"),
	})
	require.NoError(t, err)

	done := make([]sdkCompletedPart, 0, len(partSizes))

	pos := 0
	for i, sz := range partSizes {
		out, uErr := backend.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
			Bucket: aws.String("bkt"), Key: aws.String("obj"), UploadId: up.UploadId,
			PartNumber: aws.Int32(int32(i + 1)), Body: bytes.NewReader(data[pos : pos+sz]),
		})
		require.NoError(t, uErr)

		done = append(done, completedPart(int32(i+1), out.ETag))
		pos += sz
	}

	_, err = backend.CompleteMultipartUpload(t.Context(), completeInput("bkt", "obj", up.UploadId, done))
	require.NoError(t, err)

	return seekFixture{handler: handler, backend: backend, data: data}
}

func getRange(t *testing.T, h *s3.S3Handler, header string, extra map[string]string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/bkt/obj", nil)
	if header != "" {
		req.Header.Set("Range", header)
	}

	for k, v := range extra {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	serveS3Handler(h, rec, req)

	return rec
}

func TestHandler_RangeGetCompressedObjects(t *testing.T) {
	t.Parallel()

	big := 3*seekFrame + 777 + (1 << 20)
	small := make([]int, 40)
	for i := range small {
		small[i] = 30000 + i*7
	}

	tests := []struct {
		comp  s3.Compressor
		name  string
		parts []int
		size  int
	}{
		{&s3.ZstdCompressor{}, "seekable zstd", nil, big},
		{&s3.ZstdCompressor{}, "single frame zstd", nil, 900 << 10},
		{&s3.GzipCompressor{}, "legacy gzip compressor", nil, big},
		{nil, "uncompressed", nil, big},
		{nil, "multipart across frames", []int{seekFrame + 123, 2*seekFrame + 9, 1 << 20, 4000}, 0},
		{nil, "multipart aligned", []int{1 << 20, 1 << 20, 1 << 20}, 0},
		{nil, "multipart many small parts", small, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var fx seekFixture
			if tt.parts != nil {
				fx = putSeekMultipart(t, tt.parts)
			} else {
				fx = putSeekPlain(t, tt.comp, tt.size)
			}

			size := len(fx.data)
			full := getRange(t, fx.handler, "", nil)
			require.Equal(t, http.StatusOK, full.Code)
			require.True(t, bytes.Equal(fx.data, full.Body.Bytes()))

			if !strings.HasPrefix(tt.name, "multipart") {
				sum := md5.Sum(fx.data)
				assert.Equal(t, hex.EncodeToString(sum[:]), strings.Trim(full.Header().Get("ETag"), "\""))
			}

			for _, rc := range rangeCases(size) {
				rec := getRange(t, fx.handler, rc.header, nil)
				require.Equal(t, rc.status, rec.Code, rc.name)

				switch rc.status {
				case http.StatusPartialContent:
					require.True(t, bytes.Equal(fx.data[rc.start:rc.end+1], rec.Body.Bytes()), rc.name)
					assert.Equal(
						t,
						fmt.Sprintf("bytes %d-%d/%d", rc.start, rc.end, size),
						rec.Header().Get("Content-Range"),
					)
					assert.Equal(t, strconv.Itoa(rc.end-rc.start+1), rec.Header().Get("Content-Length"))
					assert.Equal(t, full.Header().Get("ETag"), rec.Header().Get("ETag"), rc.name)
				case http.StatusOK:
					require.True(t, bytes.Equal(fx.data, rec.Body.Bytes()), rc.name)
				}
			}
		})
	}
}

func TestHandler_RangeGetConditionals(t *testing.T) {
	t.Parallel()

	fx := putSeekPlain(t, &s3.ZstdCompressor{}, 2<<20)
	etag := getRange(t, fx.handler, "", nil).Header().Get("ETag")

	tests := []struct {
		headers map[string]string
		name    string
		status  int
		full    bool
	}{
		{map[string]string{"If-Range": etag}, "if-range match", http.StatusPartialContent, false},
		{map[string]string{"If-Range": `"other"`}, "if-range mismatch", http.StatusOK, true},
		{map[string]string{"If-Match": `"other"`}, "if-match fails", http.StatusPreconditionFailed, false},
		{map[string]string{"If-None-Match": etag}, "if-none-match", http.StatusNotModified, false},
		{map[string]string{"If-Match": etag}, "if-match passes", http.StatusPartialContent, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := getRange(t, fx.handler, "bytes=100-199", tt.headers)
			require.Equal(t, tt.status, rec.Code)

			switch {
			case tt.full:
				assert.True(t, bytes.Equal(fx.data, rec.Body.Bytes()))
			case tt.status == http.StatusPartialContent:
				assert.True(t, bytes.Equal(fx.data[100:200], rec.Body.Bytes()))
			}
		})
	}
}

func TestMultipart_StoresSeekableFrames(t *testing.T) {
	t.Parallel()

	fx := putSeekMultipart(t, []int{seekFrame + 123, 2*seekFrame + 9, 1 << 20})
	stored := s3.PeekStoredBytes(fx.backend, "bkt", "obj")
	require.True(t, bytes.HasPrefix(stored, []byte(zstdMagic)))

	got, ok, err := (&s3.ZstdCompressor{}).DecompressRange(stored, int64(len(fx.data)), 10, 20)
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, bytes.Equal(fx.data[10:21], got))

	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()

	all, err := dec.DecodeAll(stored, nil)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(fx.data, all))
}

func TestHandler_UploadPartCopyRangeSeekable(t *testing.T) {
	t.Parallel()

	size := 3*seekFrame + 777 + (1 << 20)
	tests := []struct {
		name       string
		header     string
		start, end int
		wantStatus int
	}{
		{"head", "bytes=0-99", 0, 99, http.StatusOK},
		{
			"middle across frames",
			fmt.Sprintf("bytes=%d-%d", seekFrame-50, 2*seekFrame+50),
			seekFrame - 50,
			2*seekFrame + 50,
			http.StatusOK,
		},
		{"tail", fmt.Sprintf("bytes=%d-%d", size-100, size-1), size - 100, size - 1, http.StatusOK},
		{"end clamped", fmt.Sprintf("bytes=%d-%d", size-100, size+100), size - 100, size - 1, http.StatusOK},
		{"past end", fmt.Sprintf("bytes=%d-%d", size, size+10), 0, 0, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := putSeekPlain(t, &s3.ZstdCompressor{}, size)

			recInit := httptest.NewRecorder()
			serveS3Handler(fx.handler, recInit, httptest.NewRequest(http.MethodPost, "/bkt/dst?uploads", nil))
			require.Equal(t, http.StatusOK, recInit.Code)

			var initResp s3.InitiateMultipartUploadResult
			require.NoError(t, xml.NewDecoder(recInit.Body).Decode(&initResp))

			reqPart := httptest.NewRequest(http.MethodPut, "/bkt/dst?partNumber=1&uploadId="+initResp.UploadID, nil)
			reqPart.Header.Set("X-Amz-Copy-Source", "/bkt/obj")
			reqPart.Header.Set("X-Amz-Copy-Source-Range", tt.header)
			recPart := httptest.NewRecorder()
			serveS3Handler(fx.handler, recPart, reqPart)
			require.Equal(t, tt.wantStatus, recPart.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			var res s3.UploadPartCopyResult
			require.NoError(t, xml.NewDecoder(recPart.Body).Decode(&res))

			body := "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>" + res.ETag +
				"</ETag></Part></CompleteMultipartUpload>"
			recDone := httptest.NewRecorder()
			serveS3Handler(fx.handler, recDone,
				httptest.NewRequest(http.MethodPost, "/bkt/dst?uploadId="+initResp.UploadID, strings.NewReader(body)))
			require.Equal(t, http.StatusOK, recDone.Code)

			recGet := httptest.NewRecorder()
			serveS3Handler(fx.handler, recGet, httptest.NewRequest(http.MethodGet, "/bkt/dst", nil))
			require.Equal(t, http.StatusOK, recGet.Code)
			assert.True(t, bytes.Equal(fx.data[tt.start:tt.end+1], recGet.Body.Bytes()))
		})
	}
}

func TestCopyObject_SeekableSourceKeepsBytes(t *testing.T) {
	t.Parallel()

	fx := putSeekPlain(t, &s3.ZstdCompressor{}, 2<<20)

	req := httptest.NewRequest(http.MethodPut, "/bkt/copy", nil)
	req.Header.Set("X-Amz-Copy-Source", "/bkt/obj")
	req.Header.Set("X-Amz-Copy-Source-Range", "bytes=0-9")
	rec := httptest.NewRecorder()
	serveS3Handler(fx.handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	recGet := httptest.NewRecorder()
	serveS3Handler(fx.handler, recGet, httptest.NewRequest(http.MethodGet, "/bkt/copy", nil))
	require.Equal(t, http.StatusOK, recGet.Code)
	assert.True(t, bytes.Equal(fx.data, recGet.Body.Bytes()))
}

func TestBackend_SeekableSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	size := 2<<20 + 5
	backend := s3.NewInMemoryBackend(&s3.ZstdCompressor{})
	mustCreateBucket(t, backend, "bkt")

	data := seekPayload(size)
	mustPutObject(t, backend, "bkt", "obj", data)

	snap := backend.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := s3.NewInMemoryBackend(&s3.ZstdCompressor{})
	require.NoError(t, fresh.Restore(t.Context(), snap))

	handler := s3.NewHandler(fresh).WithJanitor(s3.Settings{})
	rec := getRange(t, handler, "bytes=1000000-1000999", nil)
	require.Equal(t, http.StatusPartialContent, rec.Code)
	assert.True(t, bytes.Equal(data[1000000:1001000], rec.Body.Bytes()))

	full, err := fresh.GetObject(t.Context(), &sdk_s3.GetObjectInput{Bucket: aws.String("bkt"), Key: aws.String("obj")})
	require.NoError(t, err)

	got, err := io.ReadAll(full.Body)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(data, got))
}
