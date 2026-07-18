package ecr_test

// layers_test.go — verifies layers.go: InitiateLayerUpload (abandoned-upload
// pruning), UploadLayerPart (part sequencing), CompleteLayerUpload (repo
// existence, LayerAlreadyExistsException), BatchCheckLayerAvailability, and
// GetDownloadUrlForLayer, at both the backend and HTTP-handler level.

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func TestInitiateLayerUpload_PrunesAbandonedUploads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		abandoned int
	}{
		{name: "one abandoned", abandoned: 1},
		{name: "many abandoned", abandoned: 6},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := ecr.NewInMemoryBackend("123456789012", "us-east-1", "ecr.local")

			_, err := b.CreateRepository(ctx, "repo", "MUTABLE", false, "", "")
			require.NoError(t, err)

			for range tc.abandoned {
				_, err = b.InitiateLayerUpload(ctx, "repo")
				require.NoError(t, err)
			}
			require.Equal(t, tc.abandoned, b.LayerUploadCount())

			// Age the abandoned uploads past the TTL, then start a new one. The
			// stale uploads must be pruned, leaving only the fresh session.
			b.AgeAllLayerUploadsForTest(ecr.LayerUploadTTLForTest + 1)

			_, err = b.InitiateLayerUpload(ctx, "repo")
			require.NoError(t, err)

			require.Equal(t, 1, b.LayerUploadCount(),
				"abandoned layer uploads must be pruned on InitiateLayerUpload")
		})
	}
}

func TestUploadLayerPart_KeepsActiveUploadAlive(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := ecr.NewInMemoryBackend("123456789012", "us-east-1", "ecr.local")

	_, err := b.CreateRepository(ctx, "repo", "MUTABLE", false, "", "")
	require.NoError(t, err)

	init, err := b.InitiateLayerUpload(ctx, "repo")
	require.NoError(t, err)

	// Age the upload, but then send a part — activity must refresh CreatedAt.
	b.AgeAllLayerUploadsForTest(ecr.LayerUploadTTLForTest + 1)
	_, err = b.UploadLayerPart(ctx, "repo", init.UploadID, 0, -1, []byte("data"))
	require.NoError(t, err)

	// A subsequent initiate must NOT prune the refreshed upload.
	_, err = b.InitiateLayerUpload(ctx, "repo")
	require.NoError(t, err)

	require.Equal(t, 2, b.LayerUploadCount(),
		"an actively-uploading session must survive pruning")
}

func TestLayerUploadFlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name: "valid_layer_completes",
			data: []byte("fake-layer-data"),
		},
		{
			name:    "empty_upload_no_error",
			data:    []byte{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			makeRepo(t, b, "layer-repo")

			init, err := b.InitiateLayerUpload(context.Background(), "layer-repo")
			require.NoError(t, err)
			assert.NotEmpty(t, init.UploadID)

			if len(tt.data) > 0 {
				_, err = b.UploadLayerPart(context.Background(), "layer-repo", init.UploadID,
					0, int64(len(tt.data))-1, tt.data)
				require.NoError(t, err)
			}

			result, err := b.CompleteLayerUpload(context.Background(), "layer-repo", init.UploadID, nil)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				// Only non-empty uploads produce a digest.
				if len(tt.data) > 0 {
					assert.NotEmpty(t, result.LayerDigest)
				}
			}
		})
	}
}

func TestBatchCheckLayerAvailability_Backend(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	makeRepo(t, b, "bchk-repo")

	init, err := b.InitiateLayerUpload(context.Background(), "bchk-repo")
	require.NoError(t, err)
	_, err = b.UploadLayerPart(context.Background(), "bchk-repo", init.UploadID, 0, 9, []byte("layer-data"))
	require.NoError(t, err)
	result, err := b.CompleteLayerUpload(context.Background(), "bchk-repo", init.UploadID, nil)
	require.NoError(t, err)
	digest := result.LayerDigest

	avail, failures, err := b.BatchCheckLayerAvailability(context.Background(), "bchk-repo",
		[]string{digest, "sha256:nonexistent"})
	require.NoError(t, err)

	assert.Len(t, avail, 1, "one known layer must be AVAILABLE")
	assert.Equal(t, "AVAILABLE", avail[0].LayerAvailability)
	assert.Equal(t, digest, avail[0].LayerDigest)

	assert.Len(t, failures, 1, "unknown layer must produce a failure")
	assert.Equal(t, "sha256:nonexistent", failures[0].LayerDigest)
}

func Test_CompleteLayerUpload_Validation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		setup      func(t *testing.T) (h *ecr.Handler, repoName string)
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name: "repository not found",
			setup: func(t *testing.T) (*ecr.Handler, string) {
				t.Helper()

				return newAccuracyHandler(), "does-not-exist"
			},
			wantStatus: http.StatusNotFound,
			wantType:   "RepositoryNotFoundException",
		},
		{
			name: "duplicate layer digest rejected",
			setup: func(t *testing.T) (*ecr.Handler, string) {
				t.Helper()

				h := newAccuracyHandler()
				mustCreateRepo(t, h, "dup-layer-repo")

				rec := doAccuracy(t, h, "CompleteLayerUpload", map[string]any{
					"repositoryName": "dup-layer-repo",
					"uploadId":       "upload-1",
					"layerDigests":   []string{"sha256:aaaa"},
				})
				require.Equal(
					t, http.StatusOK, rec.Code, "seed CompleteLayerUpload failed: %s", rec.Body.String(),
				)

				return h, "dup-layer-repo"
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "LayerAlreadyExistsException",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, repoName := tc.setup(t)

			rec := doAccuracy(t, h, "CompleteLayerUpload", map[string]any{
				"repositoryName": repoName,
				"uploadId":       "upload-1",
				"layerDigests":   []string{"sha256:aaaa"},
			})

			assert.Equal(t, tc.wantStatus, rec.Code)
			out := parseAccuracy(t, rec)
			assert.Equal(t, tc.wantType, out["__type"])
		})
	}
}

func Test_CompleteLayerUpload_FreshDigestSucceeds(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "fresh-layer-repo")

	rec := doAccuracy(t, h, "CompleteLayerUpload", map[string]any{
		"repositoryName": "fresh-layer-repo",
		"uploadId":       "upload-fresh",
		"layerDigests":   []string{"sha256:bbbb"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func Test_UploadLayerPart_PartSequencing(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		wantType       string
		firstBytes     []int64
		blobs          []string
		wantLastStatus int
	}{
		{
			name:           "consecutive parts accepted",
			firstBytes:     []int64{0, 4},
			blobs:          []string{"AQIDBA==", "BQYHCA=="}, // 4 bytes each
			wantLastStatus: http.StatusOK,
		},
		{
			name:           "first part must start at zero",
			firstBytes:     []int64{4},
			blobs:          []string{"AQIDBA=="},
			wantLastStatus: http.StatusBadRequest,
			wantType:       "InvalidLayerPartException",
		},
		{
			name:           "gap between parts rejected",
			firstBytes:     []int64{0, 10},
			blobs:          []string{"AQIDBA==", "BQYHCA=="},
			wantLastStatus: http.StatusBadRequest,
			wantType:       "InvalidLayerPartException",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyHandler()
			mustCreateRepo(t, h, "seq-repo")

			initRec := doAccuracy(t, h, "InitiateLayerUpload", map[string]any{
				"repositoryName": "seq-repo",
			})
			require.Equal(t, http.StatusOK, initRec.Code)
			uploadID, _ := parseAccuracy(t, initRec)["uploadId"].(string)
			require.NotEmpty(t, uploadID)

			rec := initRec
			for i, firstByte := range tc.firstBytes {
				rec = doAccuracy(t, h, "UploadLayerPart", map[string]any{
					"repositoryName": "seq-repo",
					"uploadId":       uploadID,
					"partFirstByte":  firstByte,
					"partLastByte":   firstByte + 3,
					"layerPartBlob":  tc.blobs[i],
				})
			}

			assert.Equal(t, tc.wantLastStatus, rec.Code)
			if tc.wantType != "" {
				out := parseAccuracy(t, rec)
				assert.Equal(t, tc.wantType, out["__type"])
			}
		})
	}
}

func TestECR_BatchCheckLayerAvailability(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		repositoryName string
		layerDigests   []string
		wantStatus     int
		wantLayers     int
		wantFailures   int
		preUpload      bool
	}{
		{
			name:           "no layers uploaded returns failures",
			repositoryName: "my-repo",
			layerDigests:   []string{"sha256:abc123"},
			wantStatus:     http.StatusOK,
			wantLayers:     0,
			wantFailures:   1,
		},
		{
			name:           "uploaded layer is available",
			preUpload:      true,
			repositoryName: "my-repo",
			layerDigests:   []string{"sha256:abc123"},
			wantStatus:     http.StatusOK,
			wantLayers:     1,
			wantFailures:   0,
		},
		{
			name:           "empty digests returns empty results",
			repositoryName: "my-repo",
			layerDigests:   []string{},
			wantStatus:     http.StatusOK,
			wantLayers:     0,
			wantFailures:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Repository must exist for BatchCheckLayerAvailability.
			rec0 := doECRRequest(
				t,
				h,
				"CreateRepository",
				map[string]any{"repositoryName": tt.repositoryName},
			)
			require.Equal(t, http.StatusOK, rec0.Code)

			if tt.preUpload {
				rec := doECRRequest(t, h, "CompleteLayerUpload", map[string]any{
					"repositoryName": tt.repositoryName,
					"uploadId":       "upload-1",
					"layerDigests":   []string{"sha256:abc123"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doECRRequest(t, h, "BatchCheckLayerAvailability", map[string]any{
				"repositoryName": tt.repositoryName,
				"layerDigests":   tt.layerDigests,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			out := parseAccuracy(t, rec)
			assert.Len(t, out["layers"], tt.wantLayers)
			assert.Len(t, out["failures"], tt.wantFailures)
		})
	}
}

func TestECR_CompleteLayerUpload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		repositoryName string
		uploadID       string
		wantDigest     string
		layerDigests   []string
		wantStatus     int
	}{
		{
			name:           "completes upload and returns digest",
			repositoryName: "my-repo",
			uploadID:       "upload-123",
			layerDigests:   []string{"sha256:abc123"},
			wantStatus:     http.StatusOK,
			wantDigest:     "sha256:abc123",
		},
		{
			name:           "empty digests still completes",
			repositoryName: "my-repo",
			uploadID:       "upload-456",
			layerDigests:   []string{},
			wantStatus:     http.StatusOK,
			wantDigest:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// The repository must exist: real ECR returns RepositoryNotFoundException
			// for CompleteLayerUpload against an unknown repository.
			createRec := doECRRequest(
				t, h, "CreateRepository", map[string]any{"repositoryName": tt.repositoryName},
			)
			require.Equal(t, http.StatusOK, createRec.Code)

			rec := doECRRequest(t, h, "CompleteLayerUpload", map[string]any{
				"repositoryName": tt.repositoryName,
				"uploadId":       tt.uploadID,
				"layerDigests":   tt.layerDigests,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			out := parseAccuracy(t, rec)
			assert.Equal(t, tt.wantDigest, out["layerDigest"])
			assert.Equal(t, tt.repositoryName, out["repositoryName"])
			assert.Equal(t, tt.uploadID, out["uploadId"])
		})
	}
}

func TestECR_RestoreClearsInFlightLayerUploads(t *testing.T) {
	t.Parallel()

	backend := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)
	h := ecr.NewHandler(backend, nil)

	rec := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "restore-repo"})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(
		t,
		h,
		"InitiateLayerUpload",
		map[string]any{"repositoryName": "restore-repo"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	upload := parseAccuracy(t, rec)
	snapshot := h.Snapshot(t.Context())
	require.NotEmpty(t, snapshot)
	require.NoError(t, h.Restore(t.Context(), snapshot))

	rec = doECRRequest(t, h, "UploadLayerPart", map[string]any{
		"repositoryName": "restore-repo",
		"uploadId":       upload["uploadId"],
		"partFirstByte":  0,
		"partLastByte":   0,
		"layerPartBlob":  "AQ==",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatchCheckLayerAvailability_NonExistentRepo_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "BatchCheckLayerAvailability", map[string]any{
		"repositoryName": "does-not-exist",
		"layerDigests":   []string{"sha256:aabbcc"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code,
		"BatchCheckLayerAvailability must return 404 for non-existent repository")
}

func TestBatchCheckLayerAvailability_ExistingRepo_AvailableLayer(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "layer-repo-accuracy")

	// Upload a layer then check its availability.
	doAccuracy(t, h, "CompleteLayerUpload", map[string]any{
		"repositoryName": "layer-repo-accuracy",
		"uploadId":       "upload-abc",
		"layerDigests":   []string{"sha256:deadbeef"},
	})

	rec := doAccuracy(t, h, "BatchCheckLayerAvailability", map[string]any{
		"repositoryName": "layer-repo-accuracy",
		"layerDigests":   []string{"sha256:deadbeef"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	layers, _ := out["layers"].([]any)
	require.Len(t, layers, 1)
	layer := layers[0].(map[string]any)
	assert.Equal(t, "sha256:deadbeef", layer["layerDigest"])
	assert.Equal(t, "AVAILABLE", layer["layerAvailability"])
}

func TestBatchCheckLayerAvailability_ExistingRepo_MissingLayer(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "empty-layer-repo")

	rec := doAccuracy(t, h, "BatchCheckLayerAvailability", map[string]any{
		"repositoryName": "empty-layer-repo",
		"layerDigests":   []string{"sha256:nothere"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	layers, _ := out["layers"].([]any)
	failures, _ := out["failures"].([]any)
	assert.Empty(t, layers, "no available layers expected")
	require.Len(t, failures, 1)
	failure := failures[0].(map[string]any)
	assert.Equal(t, "sha256:nothere", failure["layerDigest"])
}

func TestLayerUpload_PartSize_Tracked(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "upload-size-repo")

	initRec := doAccuracy(t, h, "InitiateLayerUpload", map[string]any{
		"repositoryName": "upload-size-repo",
	})
	require.Equal(t, http.StatusOK, initRec.Code)
	initOut := parseAccuracy(t, initRec)
	uploadID, _ := initOut["uploadId"].(string)
	require.NotEmpty(t, uploadID)

	// Upload returns lastByteReceived.
	blob := make([]byte, 100)
	uploadRec := doAccuracy(t, h, "UploadLayerPart", map[string]any{
		"repositoryName": "upload-size-repo",
		"uploadId":       uploadID,
		"partFirstByte":  0,
		"partLastByte":   99,
		"layerPartBlob":  blob,
	})
	require.Equal(t, http.StatusOK, uploadRec.Code)
	uploadOut := parseAccuracy(t, uploadRec)
	assert.InDelta(t, float64(99), uploadOut["lastByteReceived"], 0,
		"lastByteReceived must equal partLastByte")
}

func TestInitiateLayerUpload_NonExistentRepo_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "InitiateLayerUpload", map[string]any{
		"repositoryName": "ghost",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestInitiateLayerUpload_ReturnsUploadIdAndPartSize(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "upload-meta")

	rec := doAccuracy(t, h, "InitiateLayerUpload", map[string]any{
		"repositoryName": "upload-meta",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.NotEmpty(t, out["uploadId"], "uploadId must be returned")
	partSize, _ := out["partSize"].(float64)
	assert.Greater(t, partSize, float64(0), "partSize must be positive")
}

func TestCompleteLayerUpload_Makes_Layer_Available(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "complete-upload")

	initRec := doAccuracy(t, h, "InitiateLayerUpload", map[string]any{
		"repositoryName": "complete-upload",
	})
	require.Equal(t, http.StatusOK, initRec.Code)
	uploadID, _ := parseAccuracy(t, initRec)["uploadId"].(string)

	digest := "sha256:cafebabe"
	completeRec := doAccuracy(t, h, "CompleteLayerUpload", map[string]any{
		"repositoryName": "complete-upload",
		"uploadId":       uploadID,
		"layerDigests":   []string{digest},
	})
	require.Equal(t, http.StatusOK, completeRec.Code)

	checkRec := doAccuracy(t, h, "BatchCheckLayerAvailability", map[string]any{
		"repositoryName": "complete-upload",
		"layerDigests":   []string{digest},
	})
	require.Equal(t, http.StatusOK, checkRec.Code)
	out := parseAccuracy(t, checkRec)
	layers, _ := out["layers"].([]any)
	require.Len(t, layers, 1)
	assert.Equal(t, "AVAILABLE", layers[0].(map[string]any)["layerAvailability"])
}

func TestBatchCheckLayerAvailability_AllAvailable(t *testing.T) {
	t.Parallel()

	b := newAccuracyBackend()
	_, err := b.CreateRepository(context.Background(), "layer-repo-2", "MUTABLE", false, "", "")
	require.NoError(t, err)

	digest := "sha256:aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	_, err = b.CompleteLayerUpload(context.Background(), "layer-repo-2", "upload-1", []string{digest})
	require.NoError(t, err)

	h := ecr.NewHandler(b, nil)
	rec := doAccuracy(t, h, "BatchCheckLayerAvailability", map[string]any{
		"repositoryName": "layer-repo-2",
		"layerDigests":   []string{digest},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	layers, _ := out["layers"].([]any)
	failures, _ := out["failures"].([]any)

	require.Len(t, layers, 1)
	require.Empty(t, failures)
	layer := layers[0].(map[string]any)
	assert.Equal(t, "AVAILABLE", layer["layerAvailability"])
	assert.Equal(t, digest, layer["layerDigest"])
}

func TestBatchCheckLayerAvailability_Mixed(t *testing.T) {
	t.Parallel()

	b := newAccuracyBackend()
	_, err := b.CreateRepository(context.Background(), "mixed-layer-repo", "MUTABLE", false, "", "")
	require.NoError(t, err)

	presentDigest := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	missingDigest := "sha256:9999999999999999999999999999999999999999999999999999999999999999"
	_, err = b.CompleteLayerUpload(context.Background(), "mixed-layer-repo", "upload-x", []string{presentDigest})
	require.NoError(t, err)

	h := ecr.NewHandler(b, nil)
	rec := doAccuracy(t, h, "BatchCheckLayerAvailability", map[string]any{
		"repositoryName": "mixed-layer-repo",
		"layerDigests":   []string{presentDigest, missingDigest},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	layers, _ := out["layers"].([]any)
	failures, _ := out["failures"].([]any)

	assert.Len(t, layers, 1, "one present layer must appear in layers")
	assert.Len(t, failures, 1, "one missing layer must appear in failures")
	failure := failures[0].(map[string]any)
	assert.Equal(t, missingDigest, failure["layerDigest"])
	assert.Equal(t, "MissingLayerDigest", failure["failureCode"])
}

func TestBatchCheckLayerAvailability_RepoNotFound_Errors(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	rec := doAccuracy(t, h, "BatchCheckLayerAvailability", map[string]any{
		"repositoryName": "does-not-exist",
		"layerDigests":   []string{"sha256:abcdef"},
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

func TestGetDownloadUrlForLayer_URLFormat(t *testing.T) {
	t.Parallel()

	b := newAccuracyBackend()
	_, err := b.CreateRepository(context.Background(), "download-repo", "MUTABLE", false, "", "")
	require.NoError(t, err)

	digest := "sha256:deadbeef00000000000000000000000000000000000000000000000000000000"
	_, err = b.CompleteLayerUpload(context.Background(), "download-repo", "upload-dl", []string{digest})
	require.NoError(t, err)

	h := ecr.NewHandler(b, nil)
	rec := doAccuracy(t, h, "GetDownloadUrlForLayer", map[string]any{
		"repositoryName": "download-repo",
		"layerDigest":    digest,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	downloadURL, _ := out["downloadUrl"].(string)
	assert.NotEmpty(t, downloadURL, "downloadUrl must be present")
	assert.Contains(t, downloadURL, "download-repo",
		"downloadUrl must contain the repository name")
	assert.Contains(t, downloadURL, digest,
		"downloadUrl must contain the layer digest")
	assert.Equal(t, digest, out["layerDigest"],
		"layerDigest in response must match the requested digest")
}

func TestGetDownloadUrlForLayer_MissingLayer_Errors(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "missing-layer-repo")

	rec := doAccuracy(t, h, "GetDownloadUrlForLayer", map[string]any{
		"repositoryName": "missing-layer-repo",
		"layerDigest":    "sha256:0000000000000000000000000000000000000000000000000000000000000000",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code,
		"GetDownloadUrlForLayer for non-existent layer must error")
}

func TestGetDownloadURLForLayer_LayerErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantType   string
		wantCode   int
		repoExists bool
	}{
		{
			name:       "repo_not_found",
			repoExists: false,
			wantCode:   http.StatusNotFound,
			wantType:   "RepositoryNotFoundException",
		},
		{
			name:       "layer_not_in_repo_returns_LayerInaccessibleException",
			repoExists: true,
			wantCode:   http.StatusBadRequest,
			wantType:   "LayerInaccessibleException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newHandlerWithBackend()
			ctx := context.Background()

			if tt.repoExists {
				_, err := b.CreateRepository(ctx, "myrepo", "MUTABLE", false, "", "")
				require.NoError(t, err)
			}

			rec := doAccuracy(t, h, "GetDownloadUrlForLayer", map[string]any{
				"repositoryName": "myrepo",
				"layerDigest":    "sha256:" + strings.Repeat("a", 64),
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			out := parseAccuracy(t, rec)
			assert.Equal(t, tt.wantType, out["__type"])
		})
	}
}
