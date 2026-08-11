package omics_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/omics"
)

func TestOmics_UploadReadSetPart_And_GetReadSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *omics.Handler)
		name string
	}{
		{
			name: "UploadReadSetPart missing partNumber returns 400",
			run: func(t *testing.T, h *omics.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "s"})
				require.Equal(t, http.StatusCreated, rec.Code)
				var storeResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
				storeID := storeResp["id"].(string)

				rec2 := doRequestRaw(t, h, http.MethodPut,
					fmt.Sprintf("/sequencestore/%s/upload/fakeid/part", storeID),
					"application/octet-stream",
					[]byte("data"),
				)
				assert.Equal(t, http.StatusBadRequest, rec2.Code)
			},
		},
		{
			name: "UploadReadSetPart unknown upload returns 404",
			run: func(t *testing.T, h *omics.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "s"})
				require.Equal(t, http.StatusCreated, rec.Code)
				var storeResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
				storeID := storeResp["id"].(string)

				rec2 := doRequestRaw(t, h, http.MethodPut,
					fmt.Sprintf("/sequencestore/%s/upload/fakeid/part?partNumber=1&partSource=SOURCE1", storeID),
					"application/octet-stream",
					[]byte("data"),
				)
				assert.Equal(t, http.StatusNotFound, rec2.Code)
			},
		},
		{
			name: "UploadReadSetPart returns real SHA256 checksum",
			run: func(t *testing.T, h *omics.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "s"})
				require.Equal(t, http.StatusCreated, rec.Code)
				var storeResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
				storeID := storeResp["id"].(string)

				rec2 := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/sequencestore/%s/upload", storeID),
					map[string]any{"name": "rs", "sequenceType": "GENERIC"},
				)
				require.Equal(t, http.StatusCreated, rec2.Code)
				var uploadResp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &uploadResp))
				uploadID := uploadResp["uploadId"].(string)

				payload := []byte("ACGT sequence data")
				expectedSum := sha256.Sum256(payload)
				expectedChecksum := hex.EncodeToString(expectedSum[:])

				rec3 := doRequestRaw(t, h, http.MethodPut,
					fmt.Sprintf("/sequencestore/%s/upload/%s/part?partNumber=1&partSource=SOURCE1", storeID, uploadID),
					"application/octet-stream",
					payload,
				)
				require.Equal(t, http.StatusOK, rec3.Code)
				var partResp map[string]any
				require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &partResp))
				assert.Equal(t, expectedChecksum, partResp["checksum"])
				assert.Equal(t, "SHA256", partResp["checksumAlgorithm"])
			},
		},
		{
			name: "GetReadSet streams bytes after complete multipart upload",
			run: func(t *testing.T, h *omics.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "s"})
				require.Equal(t, http.StatusCreated, rec.Code)
				var storeResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
				storeID := storeResp["id"].(string)

				rec2 := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/sequencestore/%s/upload", storeID),
					map[string]any{"name": "rs", "sequenceType": "GENERIC"},
				)
				require.Equal(t, http.StatusCreated, rec2.Code)
				var uploadResp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &uploadResp))
				uploadID := uploadResp["uploadId"].(string)

				payload := []byte("hello omics")
				rec3 := doRequestRaw(t, h, http.MethodPut,
					fmt.Sprintf("/sequencestore/%s/upload/%s/part?partNumber=1&partSource=SOURCE1", storeID, uploadID),
					"application/octet-stream",
					payload,
				)
				require.Equal(t, http.StatusOK, rec3.Code)

				rec4 := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/sequencestore/%s/upload/%s/complete", storeID, uploadID),
					nil,
				)
				require.Equal(t, http.StatusOK, rec4.Code)
				var rsResp map[string]any
				require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &rsResp))
				rsID := rsResp["id"].(string)

				rec5 := doRequestRaw(t, h, http.MethodGet,
					fmt.Sprintf("/sequencestore/%s/readset/%s", storeID, rsID),
					"", nil,
				)
				require.Equal(t, http.StatusOK, rec5.Code)
				assert.Equal(t, payload, rec5.Body.Bytes())
			},
		},
		{
			name: "GetReadSet returns 404 for unknown read set",
			run: func(t *testing.T, h *omics.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "s"})
				require.Equal(t, http.StatusCreated, rec.Code)
				var storeResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
				storeID := storeResp["id"].(string)

				rec2 := doRequestRaw(t, h, http.MethodGet,
					fmt.Sprintf("/sequencestore/%s/readset/doesnotexist", storeID),
					"", nil,
				)
				assert.Equal(t, http.StatusNotFound, rec2.Code)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t, newTestHandler(t))
		})
	}
}

// TestCreateMultipartReadSetUpload_WireShape verifies CreateMultipartReadSetUpload's
// request/response use the real wire field names (field-diffed against
// CreateMultipartReadSetUploadInput/Output): "sourceFileType" not the
// previously-invented "sequenceType", and the required sampleId/subjectId
// fields round-trip. There is no real "status" field on this resource.
func TestCreateMultipartReadSetUpload_WireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	storeRec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "store-1"})
	require.Equal(t, http.StatusCreated, storeRec.Code)

	var store map[string]any
	require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &store))
	storeID := store["id"].(string)

	rec := doRequest(t, h, http.MethodPost, "/sequencestore/"+storeID+"/upload", map[string]any{
		"name":           "upload-1",
		"sourceFileType": "FASTQ",
		"sampleId":       "sample-1",
		"subjectId":      "subject-1",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "FASTQ", resp["sourceFileType"])
	assert.Equal(t, "sample-1", resp["sampleId"])
	assert.Equal(t, "subject-1", resp["subjectId"])
	assert.Nil(t, resp["sequenceType"], "sequenceType is not a real wire key")
	assert.Nil(t, resp["status"], "there is no real status field on this resource")
}

// TestReadSetMetadata_FileTypeField verifies a completed read set's metadata
// serializes its file type under the real GetReadSetMetadataOutput wire key
// "fileType" (confirmed against the SDK deserializer), not the previously
// invented key "sequenceType".
func TestReadSetMetadata_FileTypeField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	storeRec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "store-1"})
	require.Equal(t, http.StatusCreated, storeRec.Code)

	var store map[string]any
	require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &store))
	storeID := store["id"].(string)

	jobRec := doRequest(t, h, http.MethodPost, "/sequencestore/"+storeID+"/importjob", map[string]any{
		"roleArn": "arn:aws:iam::000000000000:role/role",
		"sources": []map[string]any{{"sourceFileType": "BAM", "name": "read-1"}},
	})
	require.Equal(t, http.StatusCreated, jobRec.Code)

	listRec := doRequest(t, h, http.MethodPost, "/sequencestore/"+storeID+"/readsets", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	readSets, ok := listResp["readSets"].([]any)
	require.True(t, ok)
	require.Len(t, readSets, 1)

	rs := readSets[0].(map[string]any)
	assert.Equal(t, "BAM", rs["fileType"])
	assert.Nil(t, rs["sequenceType"], "sequenceType is not a real wire key")
}

// TestReadSetMetadata_FilesField_ImportJob verifies StartReadSetImportJob
// populates the optional Files sub-object (real AWS
// GetReadSetMetadataOutput.Files) for each accepted source, instead of
// leaving it entirely unmodeled.
func TestReadSetMetadata_FilesField_ImportJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source      map[string]any
		name        string
		wantSource2 bool
	}{
		{
			name: "single source populates files.source1 only",
			source: map[string]any{
				"sourceFileType": "BAM",
				"name":           "read-1",
				"sourceFiles":    map[string]any{"source1": "s3://bucket/r1.bam"},
			},
		},
		{
			name: "paired sources populate files.source1 and files.source2",
			source: map[string]any{
				"sourceFileType": "FASTQ",
				"name":           "read-2",
				"sourceFiles": map[string]any{
					"source1": "s3://bucket/r1_1.fq",
					"source2": "s3://bucket/r1_2.fq",
				},
			},
			wantSource2: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			storeRec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "s"})
			require.Equal(t, http.StatusCreated, storeRec.Code)

			var store map[string]any
			require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &store))
			storeID := store["id"].(string)

			jobRec := doRequest(t, h, http.MethodPost, "/sequencestore/"+storeID+"/importjob", map[string]any{
				"roleArn": "arn:aws:iam::000000000000:role/role",
				"sources": []map[string]any{tc.source},
			})
			require.Equal(t, http.StatusCreated, jobRec.Code)

			listRec := doRequest(t, h, http.MethodPost, "/sequencestore/"+storeID+"/readsets", nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
			readSets, ok := listResp["readSets"].([]any)
			require.True(t, ok)
			require.Len(t, readSets, 1)

			rs := readSets[0].(map[string]any)
			files, ok := rs["files"].(map[string]any)
			require.True(t, ok, "files sub-object must be present")
			assert.Contains(t, files, "source1")

			_, hasSource2 := files["source2"]
			assert.Equal(t, tc.wantSource2, hasSource2)
		})
	}
}

// TestReadSetMetadata_FilesField_MultipartUpload verifies
// CompleteMultipartReadSetUpload derives Files.source1/source2 from the
// genuinely uploaded part bytes (contentLength/totalParts), not fabricated
// values -- unlike the import-job path, real bytes are available here.
func TestReadSetMetadata_FilesField_MultipartUpload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		source2     []byte
		wantSource2 bool
		wantLen1    int
		wantParts1  int
	}{
		{
			name:       "source1 only",
			wantLen1:   len("hello omics"),
			wantParts1: 1,
		},
		{
			name:        "source1 and source2",
			source2:     []byte("mate pair data"),
			wantSource2: true,
			wantLen1:    len("hello omics"),
			wantParts1:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			storeRec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "s"})
			require.Equal(t, http.StatusCreated, storeRec.Code)

			var store map[string]any
			require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &store))
			storeID := store["id"].(string)

			uploadRec := doRequest(t, h, http.MethodPost,
				fmt.Sprintf("/sequencestore/%s/upload", storeID),
				map[string]any{"name": "rs", "sourceFileType": "FASTQ"},
			)
			require.Equal(t, http.StatusCreated, uploadRec.Code)

			var uploadResp map[string]any
			require.NoError(t, json.Unmarshal(uploadRec.Body.Bytes(), &uploadResp))
			uploadID := uploadResp["uploadId"].(string)

			part1Rec := doRequestRaw(t, h, http.MethodPut,
				fmt.Sprintf("/sequencestore/%s/upload/%s/part?partNumber=1&partSource=SOURCE1", storeID, uploadID),
				"application/octet-stream",
				[]byte("hello omics"),
			)
			require.Equal(t, http.StatusOK, part1Rec.Code)

			if tc.wantSource2 {
				part2Rec := doRequestRaw(t, h, http.MethodPut,
					fmt.Sprintf("/sequencestore/%s/upload/%s/part?partNumber=1&partSource=SOURCE2", storeID, uploadID),
					"application/octet-stream",
					tc.source2,
				)
				require.Equal(t, http.StatusOK, part2Rec.Code)
			}

			completeRec := doRequest(t, h, http.MethodPost,
				fmt.Sprintf("/sequencestore/%s/upload/%s/complete", storeID, uploadID),
				nil,
			)
			require.Equal(t, http.StatusOK, completeRec.Code)

			var rs map[string]any
			require.NoError(t, json.Unmarshal(completeRec.Body.Bytes(), &rs))
			files, ok := rs["files"].(map[string]any)
			require.True(t, ok, "files sub-object must be present")

			source1, ok := files["source1"].(map[string]any)
			require.True(t, ok, "files.source1 must be present")
			assert.InDelta(t, float64(tc.wantLen1), source1["contentLength"], 0)
			assert.InDelta(t, float64(tc.wantParts1), source1["totalParts"], 0)

			source2, hasSource2 := files["source2"].(map[string]any)
			require.Equal(t, tc.wantSource2, hasSource2)

			if tc.wantSource2 {
				assert.InDelta(t, float64(len(tc.source2)), source2["contentLength"], 0)
				assert.InDelta(t, float64(1), source2["totalParts"], 0)
			}
		})
	}
}
