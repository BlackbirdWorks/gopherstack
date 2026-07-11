package ecr_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

// ── CompleteLayerUpload: FK validation + LayerAlreadyExistsException ────────

// Test_CompleteLayerUpload_Validation covers two gaps found in the
// parity-sweep-3 audit: CompleteLayerUpload never validated that the target
// repository exists (every other backend op does), and never rejected
// re-completing a layer digest that was already registered as available,
// which real ECR rejects with LayerAlreadyExistsException.
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

// Test_CompleteLayerUpload_FreshDigestSucceeds is a control case ensuring the
// new LayerAlreadyExistsException check does not reject the first, legitimate
// completion of a digest.
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

// ── UploadLayerPart: part sequencing ─────────────────────────────────────────

// Test_UploadLayerPart_PartSequencing covers the "part sequencing" gap called
// out in the parity-sweep-3 audit: AWS requires each part's first byte to be
// consecutive to the last byte already received in the same upload session,
// rejecting gaps/overlaps with InvalidLayerPartException.
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

// ── PutImage: imageDigest vs manifest validation ─────────────────────────────

// Test_PutImage_DigestValidation covers the ImageDigestDoesNotMatchException
// gap found in the parity-sweep-3 audit: real ECR validates a caller-supplied
// imageDigest against the digest it computes from the manifest and rejects a
// mismatch, but the emulator previously trusted whatever digest the client sent.
func Test_PutImage_DigestValidation(t *testing.T) {
	t.Parallel()

	const manifest = `{"schemaVersion":2,"digest-check":true}`

	cases := []struct {
		name        string
		imageDigest string
		wantType    string
		wantStatus  int
	}{
		{
			name:        "no digest supplied - server computes it",
			imageDigest: "",
			wantStatus:  http.StatusOK,
		},
		{
			name: "mismatched digest rejected",
			imageDigest: "sha256:" +
				"0000000000000000000000000000000000000000000000000000000000000000",
			wantStatus: http.StatusBadRequest,
			wantType:   "ImageDigestDoesNotMatchException",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyHandler()
			mustCreateRepo(t, h, "digest-check-repo")

			body := map[string]any{
				"repositoryName": "digest-check-repo",
				"imageManifest":  manifest,
				"imageTag":       "v1",
			}
			if tc.imageDigest != "" {
				body["imageDigest"] = tc.imageDigest
			}

			rec := doAccuracy(t, h, "PutImage", body)
			assert.Equal(t, tc.wantStatus, rec.Code)

			if tc.wantType != "" {
				out := parseAccuracy(t, rec)
				assert.Equal(t, tc.wantType, out["__type"])
			}
		})
	}
}

// Test_PutImage_DigestValidation_MatchingDigestAccepted verifies that a
// caller-supplied imageDigest which genuinely matches the manifest's computed
// sha256 is accepted (the validation only rejects a real mismatch).
func Test_PutImage_DigestValidation_MatchingDigestAccepted(t *testing.T) {
	t.Parallel()

	const manifest = `{"schemaVersion":2,"exact":"match"}`

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "digest-match-repo")

	// First push without an explicit digest so the server tells us the real one.
	digest := mustPutImage(t, h, "digest-match-repo", "v1", manifest)

	// Re-push the same manifest, this time supplying the (correct) digest
	// explicitly, as a real client that already knows it would.
	rec := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "digest-match-repo",
		"imageManifest":  manifest,
		"imageTag":       "v2",
		"imageDigest":    digest,
	})
	assert.Equal(
		t, http.StatusOK, rec.Code, "matching imageDigest must be accepted: %s", rec.Body.String(),
	)
}
