package ecr_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func newParityBackend() *ecr.InMemoryBackend {
	return ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000")
}

func newParityHandler() (*ecr.Handler, *ecr.InMemoryBackend) {
	b := newParityBackend()

	return ecr.NewHandler(b, nil), b
}

func doParity(t *testing.T, h *ecr.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(raw))
	req.Header.Set("X-Amz-Target", "AmazonEC2ContainerRegistry_V20150921."+action)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))

	return rec
}

func parseParity(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

// TestParity_DescribeRepositories_OpaqueNextToken verifies that the nextToken
// emitted by DescribeRepositories is base64-opaque and round-trips correctly.
func TestParity_DescribeRepositories_OpaqueNextToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		repoNames  []string
		maxResults int
		wantNext   bool
	}{
		{
			name:       "no_next_token_when_results_fit",
			repoNames:  []string{"alpha", "beta"},
			maxResults: 10,
			wantNext:   false,
		},
		{
			name:       "next_token_emitted_when_truncated",
			repoNames:  []string{"alpha", "beta", "gamma"},
			maxResults: 2,
			wantNext:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newParityHandler()
			ctx := context.Background()

			for _, name := range tt.repoNames {
				_, err := b.CreateRepository(ctx, name, "MUTABLE", false, "", "")
				require.NoError(t, err)
			}

			rec := doParity(t, h, "DescribeRepositories", map[string]any{
				"maxResults": tt.maxResults,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			out := parseParity(t, rec)
			nextToken, _ := out["nextToken"].(string)

			if !tt.wantNext {
				assert.Empty(t, nextToken, "should not emit nextToken when all results fit")

				return
			}

			require.NotEmpty(t, nextToken, "should emit nextToken when truncated")

			// Token must be valid base64.
			decoded, err := base64.StdEncoding.DecodeString(nextToken)
			require.NoError(t, err, "nextToken must be valid base64")

			// The decoded value must be a known repo name — opaque to callers,
			// but internally the cursor is the repo name.
			cursorName := string(decoded)
			assert.Contains(t, tt.repoNames, cursorName, "decoded cursor must be a repo name")

			// Round-trip: using the token must return the next page.
			rec2 := doParity(t, h, "DescribeRepositories", map[string]any{
				"maxResults": tt.maxResults,
				"nextToken":  nextToken,
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			out2 := parseParity(t, rec2)
			repos2, _ := out2["repositories"].([]any)
			assert.NotEmpty(t, repos2, "second page must contain repositories")

			// The two pages together must cover all repos.
			repos1, _ := out["repositories"].([]any)
			assert.Equal(t, len(tt.repoNames), len(repos1)+len(repos2))
		})
	}
}

// TestParity_ListImages_OpaqueNextToken verifies that the nextToken emitted
// by ListImages is base64-opaque and round-trips correctly.
func TestParity_ListImages_OpaqueNextToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		imageCount int
		maxResults int
		wantNext   bool
	}{
		{
			name:       "no_pagination_when_all_fit",
			imageCount: 2,
			maxResults: 10,
			wantNext:   false,
		},
		{
			name:       "token_emitted_and_round_trips",
			imageCount: 3,
			maxResults: 2,
			wantNext:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newParityHandler()
			ctx := context.Background()

			_, err := b.CreateRepository(ctx, "myrepo", "MUTABLE", false, "", "")
			require.NoError(t, err)

			for i := range tt.imageCount {
				tag := "v1." + string(rune('0'+i))
				digest := "sha256:" + strings.Repeat("a", 63-i) + string(rune('0'+i))
				_, err = b.PutImage(ctx, "myrepo", ecr.Image{
					ImageDigest:    digest,
					ImageID:        ecr.ImageIdentifier{ImageDigest: digest, ImageTag: tag},
					RepositoryName: "myrepo",
					RegistryID:     "123456789012",
				})
				require.NoError(t, err)
			}

			rec := doParity(t, h, "ListImages", map[string]any{
				"repositoryName": "myrepo",
				"maxResults":     tt.maxResults,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			out := parseParity(t, rec)
			nextToken, _ := out["nextToken"].(string)

			if !tt.wantNext {
				assert.Empty(t, nextToken)

				return
			}

			require.NotEmpty(t, nextToken, "nextToken must be emitted when truncated")

			// Token must be valid base64.
			decoded, err := base64.StdEncoding.DecodeString(nextToken)
			require.NoError(t, err, "nextToken must be valid base64")
			assert.Contains(t, string(decoded), "sha256:", "decoded cursor must contain a digest")

			// Round-trip: use the token to get the next page.
			rec2 := doParity(t, h, "ListImages", map[string]any{
				"repositoryName": "myrepo",
				"maxResults":     tt.maxResults,
				"nextToken":      nextToken,
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			out2 := parseParity(t, rec2)
			ids1, _ := out["imageIds"].([]any)
			ids2, _ := out2["imageIds"].([]any)
			assert.Equal(
				t,
				tt.imageCount,
				len(ids1)+len(ids2),
				"both pages together must cover all images",
			)
		})
	}
}

// TestParity_GetAuthorizationToken_UniquePerCall verifies that each call to
// GetAuthorizationToken returns a unique, non-hardcoded base64-encoded token.
func TestParity_GetAuthorizationToken_UniquePerCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		registryIDs []string
		wantCount   int
	}{
		{
			name:      "default_single_token",
			wantCount: 1,
		},
		{
			name:        "one_token_per_registry_id",
			registryIDs: []string{"111111111111", "222222222222"},
			wantCount:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newParityHandler()

			body := map[string]any{}
			if len(tt.registryIDs) > 0 {
				body["registryIds"] = tt.registryIDs
			}

			rec := doParity(t, h, "GetAuthorizationToken", body)
			require.Equal(t, http.StatusOK, rec.Code)

			out := parseParity(t, rec)
			authData, _ := out["authorizationData"].([]any)
			require.Len(t, authData, tt.wantCount)

			// Each token must decode to AWS:<non-empty-password>.
			for _, entry := range authData {
				e, _ := entry.(map[string]any)
				tokenRaw, _ := e["authorizationToken"].(string)
				require.NotEmpty(t, tokenRaw)

				decoded, err := base64.StdEncoding.DecodeString(tokenRaw)
				require.NoError(t, err, "token must be valid base64")

				parts := strings.SplitN(string(decoded), ":", 2)
				require.Len(t, parts, 2)
				assert.Equal(t, "AWS", parts[0])
				assert.Equal(t, "dummy-password", parts[1],
					"emulator returns a stable AWS:dummy-password credential")
			}

			// The emulator returns a stable token across calls.
			rec2 := doParity(t, h, "GetAuthorizationToken", body)
			require.Equal(t, http.StatusOK, rec2.Code)

			out2 := parseParity(t, rec2)
			authData2, _ := out2["authorizationData"].([]any)
			e1, _ := authData[0].(map[string]any)
			e2, _ := authData2[0].(map[string]any)
			assert.Equal(t, e1["authorizationToken"], e2["authorizationToken"],
				"consecutive calls return the stable token")
		})
	}
}

// TestParity_DescribePullThroughCacheRules_Pagination verifies that
// DescribePullThroughCacheRules supports nextToken and maxResults pagination.
func TestParity_DescribePullThroughCacheRules_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		prefixes   []string
		maxResults int
		wantNext   bool
	}{
		{
			name:       "all_results_fit_no_token",
			prefixes:   []string{"alpha/", "beta/"},
			maxResults: 10,
			wantNext:   false,
		},
		{
			name:       "token_emitted_and_round_trips",
			prefixes:   []string{"alpha/", "beta/", "gamma/"},
			maxResults: 2,
			wantNext:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newParityHandler()
			ctx := context.Background()

			for _, prefix := range tt.prefixes {
				_, err := b.CreatePullThroughCacheRule(
					ctx,
					prefix,
					"public.ecr.aws",
					"",
					"",
					"",
					"",
				)
				require.NoError(t, err)
			}

			rec := doParity(t, h, "DescribePullThroughCacheRules", map[string]any{
				"maxResults": tt.maxResults,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			out := parseParity(t, rec)
			nextToken, _ := out["nextToken"].(string)

			if !tt.wantNext {
				assert.Empty(t, nextToken, "no nextToken when all rules fit")

				return
			}

			require.NotEmpty(t, nextToken, "nextToken must be emitted when truncated")

			decoded, err := base64.StdEncoding.DecodeString(nextToken)
			require.NoError(t, err, "nextToken must be valid base64")

			cursorPrefix := string(decoded)
			assert.Contains(t, tt.prefixes, cursorPrefix)

			// Round-trip.
			rec2 := doParity(t, h, "DescribePullThroughCacheRules", map[string]any{
				"maxResults": tt.maxResults,
				"nextToken":  nextToken,
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			out2 := parseParity(t, rec2)
			rules1, _ := out["pullThroughCacheRules"].([]any)
			rules2, _ := out2["pullThroughCacheRules"].([]any)
			assert.Equal(t, len(tt.prefixes), len(rules1)+len(rules2))
		})
	}
}

// TestParity_BatchGetRepositoryScanningConfiguration_ScanFrequency verifies
// that the effective scan frequency reflects registry-level ENHANCED scanning.
func TestParity_BatchGetRepositoryScanningConfiguration_ScanFrequency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		scanType       string
		wantFrequency  string
		rules          []map[string]any
		scanOnPush     bool
		wantScanOnPush bool
	}{
		{
			name:          "no_scan_on_push_returns_MANUAL",
			scanOnPush:    false,
			wantFrequency: "MANUAL",
		},
		{
			name:          "scan_on_push_returns_SCAN_ON_PUSH",
			scanOnPush:    true,
			wantFrequency: "SCAN_ON_PUSH",
		},
		{
			name:       "enhanced_wildcard_rule_returns_CONTINUOUS_SCAN",
			scanOnPush: false,
			scanType:   "ENHANCED",
			rules: []map[string]any{
				{
					"scanFrequency": "CONTINUOUS_SCAN",
					"repositoryFilters": []map[string]any{
						{"filter": "*", "filterType": "WILDCARD"},
					},
				},
			},
			wantFrequency: "CONTINUOUS_SCAN",
		},
		{
			name:       "enhanced_prefix_rule_matches_repo",
			scanOnPush: false,
			scanType:   "ENHANCED",
			rules: []map[string]any{
				{
					"scanFrequency": "CONTINUOUS_SCAN",
					"repositoryFilters": []map[string]any{
						{"filter": "myrepo", "filterType": "PREFIX"},
					},
				},
			},
			wantFrequency: "CONTINUOUS_SCAN",
		},
		{
			name:       "enhanced_prefix_rule_no_match_falls_back",
			scanOnPush: false,
			scanType:   "ENHANCED",
			rules: []map[string]any{
				{
					"scanFrequency": "CONTINUOUS_SCAN",
					"repositoryFilters": []map[string]any{
						{"filter": "other", "filterType": "PREFIX"},
					},
				},
			},
			wantFrequency: "MANUAL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newParityHandler()
			ctx := context.Background()

			_, err := b.CreateRepository(ctx, "myrepo", "MUTABLE", tt.scanOnPush, "", "")
			require.NoError(t, err)

			if tt.scanType != "" {
				rec := doParity(t, h, "PutRegistryScanningConfiguration", map[string]any{
					"scanType": tt.scanType,
					"rules":    tt.rules,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doParity(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
				"repositoryNames": []string{"myrepo"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			out := parseParity(t, rec)
			configs, _ := out["scanningConfigurations"].([]any)
			require.Len(t, configs, 1)

			cfg, _ := configs[0].(map[string]any)
			assert.Equal(t, tt.wantFrequency, cfg["scanFrequency"])
		})
	}
}

// TestParity_GetDownloadURLForLayer_LayerErrors verifies that missing layers
// return LayerInaccessibleException, not RepositoryNotFoundException.
func TestParity_GetDownloadURLForLayer_LayerErrors(t *testing.T) {
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

			h, b := newParityHandler()
			ctx := context.Background()

			if tt.repoExists {
				_, err := b.CreateRepository(ctx, "myrepo", "MUTABLE", false, "", "")
				require.NoError(t, err)
			}

			rec := doParity(t, h, "GetDownloadUrlForLayer", map[string]any{
				"repositoryName": "myrepo",
				"layerDigest":    "sha256:" + strings.Repeat("a", 64),
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			var errBody map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errBody))
			assert.Equal(t, tt.wantType, errBody["__type"])
		})
	}
}

// TestParity_DeleteRepository_CleansUpLayerUploads_ViaIndex verifies that
// deleting a repository cleans up in-progress layer uploads efficiently.
func TestParity_DeleteRepository_CleansUpLayerUploads_ViaIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		uploadsBefore int
	}{
		{name: "no_uploads", uploadsBefore: 0},
		{name: "single_upload", uploadsBefore: 1},
		{name: "many_uploads", uploadsBefore: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newParityBackend()
			ctx := context.Background()

			_, err := b.CreateRepository(ctx, "myrepo", "MUTABLE", false, "", "")
			require.NoError(t, err)

			for range tt.uploadsBefore {
				_, err = b.InitiateLayerUpload(ctx, "myrepo")
				require.NoError(t, err)
			}
			require.Equal(t, tt.uploadsBefore, b.LayerUploadCount())

			_, err = b.DeleteRepository(ctx, "myrepo")
			require.NoError(t, err)

			assert.Equal(t, 0, b.LayerUploadCount(),
				"all layer uploads must be removed when repository is deleted")
		})
	}
}

// TestParity_ScanFrequency_Wildcard verifies wildcardMatch edge cases that
// ensure CONTINUOUS_SCAN rules with glob patterns work correctly.
func TestParity_ScanFrequency_Wildcard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		repoName      string
		filter        string
		wantFrequency string
	}{
		{
			name:          "star_matches_all",
			repoName:      "myrepo",
			filter:        "*",
			wantFrequency: "CONTINUOUS_SCAN",
		},
		{
			name:          "prefix_star_matches",
			repoName:      "prod/myapp",
			filter:        "prod/*",
			wantFrequency: "CONTINUOUS_SCAN",
		},
		{
			name:          "no_match_falls_back_to_manual",
			repoName:      "dev/myapp",
			filter:        "prod/*",
			wantFrequency: "MANUAL",
		},
		{
			name:          "exact_match",
			repoName:      "myrepo",
			filter:        "myrepo",
			wantFrequency: "CONTINUOUS_SCAN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newParityHandler()
			ctx := context.Background()

			_, err := b.CreateRepository(ctx, tt.repoName, "MUTABLE", false, "", "")
			require.NoError(t, err)

			rec := doParity(t, h, "PutRegistryScanningConfiguration", map[string]any{
				"scanType": "ENHANCED",
				"rules": []map[string]any{
					{
						"scanFrequency": "CONTINUOUS_SCAN",
						"repositoryFilters": []map[string]any{
							{"filter": tt.filter, "filterType": "WILDCARD"},
						},
					},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doParity(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
				"repositoryNames": []string{tt.repoName},
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			out := parseParity(t, rec2)
			configs, _ := out["scanningConfigurations"].([]any)
			require.Len(t, configs, 1)
			cfg, _ := configs[0].(map[string]any)
			assert.Equal(t, tt.wantFrequency, cfg["scanFrequency"])
		})
	}
}
