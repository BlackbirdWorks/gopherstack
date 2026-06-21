package ecr_test

// handler_refinement2_test.go — ECR DescribeImages filter.tagStatus parity (go-6lvhj)
//
// AWS ECR DescribeImages accepts filter: { tagStatus: "TAGGED" | "UNTAGGED" | "ANY" }.
// Previously the filter field was silently ignored — all images were returned regardless.
// These tests verify that filter.tagStatus now narrows the result set, matching real AWS.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func newRef2Handler() *ecr.Handler {
	return ecr.NewHandler(ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000"), nil)
}

// TestRefinement2_DescribeImages_FilterTagStatus verifies filter.tagStatus narrows results.
func TestRefinement2_DescribeImages_FilterTagStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tagStatus string
		wantCount int
	}{
		{
			name:      "tagged_only_returns_only_tagged",
			tagStatus: "TAGGED",
			wantCount: 1,
		},
		{
			name:      "untagged_only_returns_only_untagged",
			tagStatus: "UNTAGGED",
			wantCount: 1,
		},
		{
			name:      "any_returns_all",
			tagStatus: "ANY",
			wantCount: 2,
		},
		{
			name:      "empty_filter_returns_all",
			tagStatus: "",
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRef2Handler()
			mustCreateRepo(t, h, "filter-repo")

			// Push a tagged image.
			taggedDigest := mustPutImage(t, h, "filter-repo", "v1.0", `{"tagged":true}`)

			// Push an untagged image (no tag field).
			untaggedRec := doAccuracy(t, h, "PutImage", map[string]any{
				"repositoryName": "filter-repo",
				"imageManifest":  `{"untagged":true}`,
			})
			require.Equal(t, http.StatusOK, untaggedRec.Code)
			var untaggedResp struct {
				Image struct {
					ImageDigest string `json:"imageDigest"`
				} `json:"image"`
			}
			require.NoError(t, json.Unmarshal(untaggedRec.Body.Bytes(), &untaggedResp))
			untaggedDigest := untaggedResp.Image.ImageDigest
			require.NotEmpty(t, untaggedDigest)

			body := map[string]any{
				"repositoryName": "filter-repo",
			}
			if tt.tagStatus != "" {
				body["filter"] = map[string]any{"tagStatus": tt.tagStatus}
			}

			rec := doAccuracy(t, h, "DescribeImages", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				ImageDetails []map[string]any `json:"imageDetails"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.ImageDetails, tt.wantCount, "tagStatus=%q", tt.tagStatus)

			// Spot-check which digest shows up when filtering TAGGED.
			if tt.tagStatus == "TAGGED" {
				require.Len(t, resp.ImageDetails, 1)
				assert.Equal(t, taggedDigest, resp.ImageDetails[0]["imageDigest"])
			}

			// Spot-check which digest shows up when filtering UNTAGGED.
			if tt.tagStatus == "UNTAGGED" {
				require.Len(t, resp.ImageDetails, 1)
				assert.Equal(t, untaggedDigest, resp.ImageDetails[0]["imageDigest"])
			}
		})
	}
}

// TestRefinement2_DescribeImages_FilterIgnoredWhenImageIDsProvided verifies that
// filter.tagStatus is ignored (AWS behaviour) when specific imageIds are given.
func TestRefinement2_DescribeImages_FilterIgnoredWhenImageIDsProvided(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tagStatus string
		wantCount int
	}{
		{
			name:      "untagged_filter_with_explicit_digest_still_returns_image",
			tagStatus: "TAGGED",
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRef2Handler()
			mustCreateRepo(t, h, "id-filter-repo")

			// Push an untagged image (no tag).
			rec := doAccuracy(t, h, "PutImage", map[string]any{
				"repositoryName": "id-filter-repo",
				"imageManifest":  `{"content":"only"}`,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			var putResp struct {
				Image struct {
					ImageDigest string `json:"imageDigest"`
				} `json:"image"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
			digest := putResp.Image.ImageDigest

			// When imageIds are specified, filter.tagStatus is not applied.
			descRec := doAccuracy(t, h, "DescribeImages", map[string]any{
				"repositoryName": "id-filter-repo",
				"imageIds":       []map[string]any{{"imageDigest": digest}},
				"filter":         map[string]any{"tagStatus": tt.tagStatus},
			})
			require.Equal(t, http.StatusOK, descRec.Code)

			var resp struct {
				ImageDetails []map[string]any `json:"imageDetails"`
			}
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
			assert.Len(t, resp.ImageDetails, tt.wantCount)
		})
	}
}

// TestRefinement2_DescribeImages_FilterTagStatus_EmptyRepo verifies filter on empty repo returns empty list.
func TestRefinement2_DescribeImages_FilterTagStatus_EmptyRepo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tagStatus string
	}{
		{name: "tagged_on_empty_repo", tagStatus: "TAGGED"},
		{name: "untagged_on_empty_repo", tagStatus: "UNTAGGED"},
		{name: "any_on_empty_repo", tagStatus: "ANY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRef2Handler()
			mustCreateRepo(t, h, "empty-filter-repo")

			rec := doAccuracy(t, h, "DescribeImages", map[string]any{
				"repositoryName": "empty-filter-repo",
				"filter":         map[string]any{"tagStatus": tt.tagStatus},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				ImageDetails []map[string]any `json:"imageDetails"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Empty(t, resp.ImageDetails, "tagStatus=%q on empty repo must return empty list", tt.tagStatus)
		})
	}
}
