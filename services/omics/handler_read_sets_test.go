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
