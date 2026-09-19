package s3_test

// Golden byte-equality fixtures for the object-write perf sweep (gzip
// compression level + pre-sized body buffers). These tests only exercise the
// public HTTP handler, so the same file runs unmodified against the
// pre-change code (a detached worktree at the commit before this sweep) to
// capture testdata/*.golden.json, and against the optimized code to assert
// byte-for-byte identical responses.
//
// To regenerate the golden files: S3_GOLDEN_UPDATE=1 go test -run TestGolden ./services/s3/...

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

func goldenPath(name string) string {
	return filepath.Join("testdata", name)
}

// checkGolden writes got to testdata/name when S3_GOLDEN_UPDATE=1 is set,
// otherwise asserts got is byte-identical to the committed golden file.
func checkGolden(t *testing.T, name string, got []byte) {
	t.Helper()

	var buf bytes.Buffer
	require.NoError(t, json.Indent(&buf, got, "", "  "))
	pretty := buf.Bytes()

	path := goldenPath(name)
	if os.Getenv("S3_GOLDEN_UPDATE") != "" {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(path, pretty, 0o600))

		return
	}

	want, err := os.ReadFile(path)
	require.NoError(t, err, "golden file %s missing -- run with S3_GOLDEN_UPDATE=1 first", path)
	require.Equal(t, string(want), string(pretty), "response for %s changed", name)
}

// goldenHTTPResponse is a serializable snapshot of an httptest recorder,
// restricted to headers that are deterministic given fixed input (excludes
// Last-Modified/Date/X-Amz-Request-Id).
type goldenHTTPResponse struct {
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
	Status  int               `json:"status"`
}

func snapshotResponse(rec *httptest.ResponseRecorder, headerNames []string) goldenHTTPResponse {
	headers := make(map[string]string, len(headerNames))
	for _, h := range headerNames {
		if v := rec.Header().Get(h); v != "" {
			headers[h] = v
		}
	}

	return goldenHTTPResponse{
		Status:  rec.Code,
		Headers: headers,
		Body:    rec.Body.String(),
	}
}

func marshalGolden(t *testing.T, v any) []byte {
	t.Helper()

	data, err := json.Marshal(v)
	require.NoError(t, err)

	return data
}

var lastModifiedElemRE = regexp.MustCompile(`<LastModified>[^<]*</LastModified>`)

// normalizeListXML strips the one inherently time-variant field ListObjectsV2
// emits (LastModified is wall-clock time at PutObject) so the golden fixture
// compares everything else byte-for-byte.
func normalizeListXML(body string) string {
	return lastModifiedElemRE.ReplaceAllString(body, "<LastModified>NORMALIZED</LastModified>")
}

// TestGolden_GetObject_CompressedRoundTrip verifies GetObject's body and
// response headers are unchanged for an object above the compression
// threshold, across the gzip-level and buffer-presizing perf changes.
func TestGolden_GetObject_CompressedRoundTrip(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "golden-get")

	data := bytes.Repeat([]byte("gopherstack-golden-perf-sweep-payload-"), 2048) // > compression threshold
	mustPutObject(t, backend, "golden-get", "obj", data)

	req := httptest.NewRequest(http.MethodGet, "/golden-get/obj", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, data, rec.Body.Bytes(), "GetObject body must round-trip exactly")

	snap := snapshotResponse(rec, []string{"ETag", "Content-Type", "Content-Length", "Accept-Ranges"})
	checkGolden(t, "get_object_compressed.golden.json", marshalGolden(t, snap))
}

// TestGolden_GetObject_MultipartAssembled covers the CompleteMultipartUpload
// assembly path directly (assembleMultipartData / GzipCompressor.Compress),
// which is where the profiled hot spot lived.
func TestGolden_GetObject_MultipartAssembled(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "golden-mpu")

	partData := bytes.Repeat([]byte("b"), 5*1024*1024)

	createRec := doRequest(handler, http.MethodPost, "/golden-mpu/obj?uploads", nil, nil)
	require.Equal(t, http.StatusOK, createRec.Code)

	var initResult s3.InitiateMultipartUploadResult
	require.NoError(t, xml.NewDecoder(createRec.Body).Decode(&initResult))
	uploadID := initResult.UploadID

	partPath := "/golden-mpu/obj?partNumber=1&uploadId=" + uploadID
	partRec := doRequest(handler, http.MethodPut, partPath, bytes.NewReader(partData), nil)
	require.Equal(t, http.StatusOK, partRec.Code)
	etag := partRec.Header().Get("ETag")

	completeBody := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` +
		etag + `</ETag></Part></CompleteMultipartUpload>`
	completePath := "/golden-mpu/obj?uploadId=" + uploadID
	completeRec := doRequest(handler, http.MethodPost, completePath, bytes.NewReader([]byte(completeBody)), nil)
	require.Equal(t, http.StatusOK, completeRec.Code)

	getReq := httptest.NewRequest(http.MethodGet, "/golden-mpu/obj", nil)
	getRec := httptest.NewRecorder()
	serveS3Handler(handler, getRec, getReq)

	require.Equal(t, http.StatusOK, getRec.Code)
	require.Equal(t, partData, getRec.Body.Bytes(), "assembled multipart body must round-trip exactly")

	snap := snapshotResponse(getRec, []string{"ETag", "Content-Type", "Content-Length"})
	checkGolden(t, "get_object_multipart_assembled.golden.json", marshalGolden(t, snap))
}

// TestGolden_HeadObject verifies HeadObject's response headers are unchanged.
func TestGolden_HeadObject(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "golden-head")
	mustPutObject(t, backend, "golden-head", "obj", bytes.Repeat([]byte("z"), 4096))

	req := httptest.NewRequest(http.MethodHead, "/golden-head/obj", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	snap := snapshotResponse(rec, []string{
		"ETag", "Content-Type", "Content-Length", "Accept-Ranges", "X-Amz-Storage-Class",
	})
	checkGolden(t, "head_object.golden.json", marshalGolden(t, snap))
}

// TestGolden_ListObjectsV2_PrefixDelimiterMaxKeys verifies ListObjectsV2's XML
// is unchanged (Contents ordering, ETags, CommonPrefixes, truncation), with
// the one wall-clock field (LastModified) normalized out.
func TestGolden_ListObjectsV2_PrefixDelimiterMaxKeys(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "golden-list")

	for i := range 25 {
		mustPutObject(t, backend, "golden-list", keyFor(i), []byte("x"))
	}

	req := httptest.NewRequest(
		http.MethodGet, "/golden-list?list-type=2&prefix=dir1/&delimiter=/&max-keys=5", nil,
	)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	normalized := normalizeListXML(rec.Body.String())
	checkGolden(t, "list_objects_v2_prefix_delimiter.golden.xml.json", marshalGolden(t, goldenHTTPResponse{
		Status:  rec.Code,
		Headers: map[string]string{"Content-Type": rec.Header().Get("Content-Type")},
		Body:    normalized,
	}))
}

func keyFor(i int) string {
	dir := "dir0"
	if i%2 == 0 {
		dir = "dir1"
	}

	return dir + "/key-" + strconv.Itoa(i)
}
