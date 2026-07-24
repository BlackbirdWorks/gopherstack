package ecr_test

// image_scanning_test.go — verifies image_scanning.go: StartImageScan,
// DescribeImageScanFindings (including ScanNotFoundException precedence),
// BatchGetRepositoryScanningConfiguration/effective scan frequency, and
// Put/GetRegistryScanningConfiguration/PutImageScanningConfiguration.
// scan.go's finding-generation internals are covered separately in
// scan_test.go.

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func TestScanNotFoundException_UnscannedImage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		name      string
		scanFirst bool
		wantErr   bool
	}{
		{
			name:      "unscanned_returns_ScanNotFoundException",
			scanFirst: false,
			wantErr:   true,
			wantErrIs: ecr.ErrScanNotFoundException,
		},
		{
			name:      "scanned_returns_findings",
			scanFirst: true,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			b.CreateRepoInternal("my-repo")

			digest := "sha256:aabbcc"
			img := makeImage(digest, "v1.0")
			b.AddImageInternal("my-repo", img)

			if tt.scanFirst {
				_, err := b.StartImageScan(context.Background(), "my-repo",
					ecr.ImageIdentifier{ImageDigest: digest})
				require.NoError(t, err)
			}

			_, _, err := b.DescribeImageScanFindings(context.Background(), "my-repo",
				ecr.ImageIdentifier{ImageDigest: digest}, 0, "")

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestStartImageScan_ThenDescribeFindings(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("scan-repo")

	digest := "sha256:deadbeef"
	b.AddImageInternal("scan-repo", makeImage(digest, "latest"))

	result, err := b.StartImageScan(context.Background(), "scan-repo",
		ecr.ImageIdentifier{ImageDigest: digest})
	require.NoError(t, err)
	assert.Equal(t, "scan-repo", result.RepositoryName)

	findings, _, err := b.DescribeImageScanFindings(context.Background(), "scan-repo",
		ecr.ImageIdentifier{ImageDigest: digest}, 0, "")
	require.NoError(t, err)
	assert.Equal(t, "COMPLETE", findings.Status)
	assert.Equal(t, digest, findings.ImageID.ImageDigest)
}

func TestScanNotFoundException_HTTPHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECRRequest(t, h, "CreateRepository", map[string]any{
		"repositoryName": "scan-http-repo",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Push an image so the repo is non-empty — the scan check only fires on existing images.
	rec = doECRRequest(t, h, "PutImage", map[string]any{
		"repositoryName": "scan-http-repo",
		"imageManifest":  `{"schemaVersion":2}`,
		"imageTag":       "v1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	putResp := parseAccuracy(t, rec)
	image := putResp["image"].(map[string]any)
	imageID := image["imageId"].(map[string]any)
	digest := imageID["imageDigest"].(string)

	// DescribeImageScanFindings without calling StartImageScan first must return ScanNotFoundException.
	rec = doECRRequest(t, h, "DescribeImageScanFindings", map[string]any{
		"repositoryName": "scan-http-repo",
		"imageId":        map[string]string{"imageDigest": digest},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ScanNotFoundException")
}

func TestImageScanning_BASIC_StartAndGet(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "scan-basic")
	d := mustPutManifest(t, h, "scan-basic", "v1", `{"schemaVersion":2}`)

	scanRec := doAccuracy(t, h, "StartImageScan", map[string]any{
		"repositoryName": "scan-basic",
		"imageId":        map[string]any{"imageDigest": d},
	})
	require.Equal(t, http.StatusOK, scanRec.Code)

	scanOut := parseAccuracy(t, scanRec)
	status, _ := scanOut["imageScanStatus"].(map[string]any)
	assert.NotEmpty(t, status["status"], "imageScanStatus.status must be set")

	findRec := doAccuracy(t, h, "DescribeImageScanFindings", map[string]any{
		"repositoryName": "scan-basic",
		"imageId":        map[string]any{"imageDigest": d},
	})
	require.Equal(t, http.StatusOK, findRec.Code)

	findOut := parseAccuracy(t, findRec)
	assert.NotEmpty(t, findOut["repositoryName"])
	assert.Equal(t, "scan-basic", findOut["repositoryName"])
}

// TestDescribeImageScanFindings_ImageScanCompletedAt_WireShape locks the
// imageScanFindings.imageScanCompletedAt wire field over the JSON boundary:
// the real ECR SDK deserializer (awsAwsjson11_deserializeDocumentImageScanFindings)
// reads the key "imageScanCompletedAt" as a JSON Number via
// smithytime.ParseEpochSeconds — not "completedAt" as an RFC3339 string.
func TestDescribeImageScanFindings_ImageScanCompletedAt_WireShape(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "scan-wire-shape")
	d := mustPutManifest(t, h, "scan-wire-shape", "v1", `{"schemaVersion":2}`)

	doAccuracy(t, h, "StartImageScan", map[string]any{
		"repositoryName": "scan-wire-shape",
		"imageId":        map[string]any{"imageDigest": d},
	})

	findRec := doAccuracy(t, h, "DescribeImageScanFindings", map[string]any{
		"repositoryName": "scan-wire-shape",
		"imageId":        map[string]any{"imageDigest": d},
	})
	require.Equal(t, http.StatusOK, findRec.Code)

	findOut := parseAccuracy(t, findRec)
	findings, ok := findOut["imageScanFindings"].(map[string]any)
	require.True(t, ok, "imageScanFindings must be an object")

	_, hasOldKey := findings["completedAt"]
	assert.False(t, hasOldKey, "the wire key must be imageScanCompletedAt, not completedAt")

	completedAt, ok := findings["imageScanCompletedAt"].(float64)
	require.True(t, ok, "imageScanCompletedAt must be a JSON number, got %T", findings["imageScanCompletedAt"])
	assert.Positive(t, completedAt)
}

func TestImageScanning_StartByScanByTag(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "scan-by-tag")
	mustPutManifest(t, h, "scan-by-tag", "prod", `{"schemaVersion":2}`)

	scanRec := doAccuracy(t, h, "StartImageScan", map[string]any{
		"repositoryName": "scan-by-tag",
		"imageId":        map[string]any{"imageTag": "prod"},
	})
	require.Equal(t, http.StatusOK, scanRec.Code)

	findRec := doAccuracy(t, h, "DescribeImageScanFindings", map[string]any{
		"repositoryName": "scan-by-tag",
		"imageId":        map[string]any{"imageTag": "prod"},
	})
	require.Equal(t, http.StatusOK, findRec.Code)
}

func TestRegistryScanningConfiguration_ENHANCED(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	putRec := doAccuracy(t, h, "PutRegistryScanningConfiguration", map[string]any{
		"scanType": "ENHANCED",
		"rules": []map[string]any{
			{
				"scanFrequency": "CONTINUOUS_SCAN",
				"repositoryFilters": []map[string]any{
					{"filter": "*", "filterType": "WILDCARD"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doAccuracy(t, h, "GetRegistryScanningConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, getRec.Code)

	out := parseAccuracy(t, getRec)
	cfg, _ := out["scanningConfiguration"].(map[string]any)
	assert.Equal(t, "ENHANCED", cfg["scanType"],
		"ENHANCED scan type must be persisted and returned")
	rules, _ := cfg["rules"].([]any)
	assert.Len(t, rules, 1, "scanning rules must be persisted")
}

func TestRegistryScanningConfiguration_BASIC_Default(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	getRec := doAccuracy(t, h, "GetRegistryScanningConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, getRec.Code)

	out := parseAccuracy(t, getRec)
	cfg, _ := out["scanningConfiguration"].(map[string]any)
	assert.Equal(t, "BASIC", cfg["scanType"],
		"default scan type must be BASIC")
}

func TestPutImageScanningConfiguration_UpdatesRepo(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "scan-cfg")

	putRec := doAccuracy(t, h, "PutImageScanningConfiguration", map[string]any{
		"repositoryName": "scan-cfg",
		"imageScanningConfiguration": map[string]any{
			"scanOnPush": true,
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	out := parseAccuracy(t, putRec)
	cfg, _ := out["imageScanningConfiguration"].(map[string]any)
	assert.Equal(t, true, cfg["scanOnPush"])
}

func TestBatchGetRepositoryScanningConfiguration_MultipleRepos(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName":             "scan-r1",
		"imageScanningConfiguration": map[string]any{"scanOnPush": true},
	})
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName":             "scan-r2",
		"imageScanningConfiguration": map[string]any{"scanOnPush": false},
	})

	rec := doAccuracy(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
		"repositoryNames": []string{"scan-r1", "scan-r2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	configs, _ := out["scanningConfigurations"].([]any)
	require.Len(t, configs, 2)

	cfgMap := map[string]map[string]any{}
	for _, c := range configs {
		cfg := c.(map[string]any)
		cfgMap[cfg["repositoryName"].(string)] = cfg
	}

	assert.Equal(t, "SCAN_ON_PUSH", cfgMap["scan-r1"]["scanFrequency"])
	assert.Equal(t, "MANUAL", cfgMap["scan-r2"]["scanFrequency"])
}

func TestBatchGetRepositoryScanningConfiguration_UnknownRepo_Failure(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "known-scan")

	rec := doAccuracy(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
		"repositoryNames": []string{"known-scan", "unknown-scan"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	configs, _ := out["scanningConfigurations"].([]any)
	failures, _ := out["failures"].([]any)
	assert.Len(t, configs, 1, "known repo returns config")
	assert.Len(t, failures, 1, "unknown repo returns failure")
	failure := failures[0].(map[string]any)
	assert.Equal(t, "unknown-scan", failure["repositoryName"])
}

func TestBatchGetRepositoryScanningConfiguration_AllExist(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "scan-repo-a")
	mustCreateRepo(t, h, "scan-repo-b")

	rec := doAccuracy(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
		"repositoryNames": []string{"scan-repo-a", "scan-repo-b"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	configs, _ := out["scanningConfigurations"].([]any)
	failures, _ := out["failures"].([]any)

	assert.Len(t, configs, 2, "both repos should return scanning configurations")
	assert.Empty(t, failures)
}

func TestBatchGetRepositoryScanningConfiguration_MixedExistence(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "scan-exists")

	rec := doAccuracy(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
		"repositoryNames": []string{"scan-exists", "scan-missing"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	configs, _ := out["scanningConfigurations"].([]any)
	failures, _ := out["failures"].([]any)

	assert.Len(t, configs, 1, "only existing repo should appear in scanningConfigurations")
	assert.Len(t, failures, 1, "non-existent repo should appear in failures")

	failure := failures[0].(map[string]any)
	assert.Equal(t, "scan-missing", failure["repositoryName"])
	assert.NotEmpty(t, failure["failureCode"])
}

func TestBatchGetRepositoryScanningConfiguration_ScanOnPush_Reflected(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "scan-sop-repo",
		"imageScanningConfiguration": map[string]any{
			"scanOnPush": true,
		},
	})

	rec := doAccuracy(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
		"repositoryNames": []string{"scan-sop-repo"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	configs, _ := out["scanningConfigurations"].([]any)
	require.Len(t, configs, 1)

	cfg := configs[0].(map[string]any)
	assert.Equal(t, true, cfg["scanOnPush"])
}

func TestPutImageScanningConfiguration_EnableDisable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		initialSOP bool
		updatedSOP bool
	}{
		{name: "enable_scan_on_push", initialSOP: false, updatedSOP: true},
		{name: "disable_scan_on_push", initialSOP: true, updatedSOP: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyHandler()
			doAccuracy(t, h, "CreateRepository", map[string]any{
				"repositoryName": tt.name,
				"imageScanningConfiguration": map[string]any{
					"scanOnPush": tt.initialSOP,
				},
			})

			rec := doAccuracy(t, h, "PutImageScanningConfiguration", map[string]any{
				"repositoryName": tt.name,
				"imageScanningConfiguration": map[string]any{
					"scanOnPush": tt.updatedSOP,
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			out := parseAccuracy(t, rec)
			cfg, _ := out["imageScanningConfiguration"].(map[string]any)
			assert.Equal(t, tt.updatedSOP, cfg["scanOnPush"])
			assert.Equal(t, tt.name, out["repositoryName"])
		})
	}
}

func TestPutImageScanningConfiguration_PersistsViaDescribeRepositories(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "persist-scan-repo")

	doAccuracy(t, h, "PutImageScanningConfiguration", map[string]any{
		"repositoryName": "persist-scan-repo",
		"imageScanningConfiguration": map[string]any{
			"scanOnPush": true,
		},
	})

	rec := doAccuracy(t, h, "DescribeRepositories", map[string]any{
		"repositoryNames": []string{"persist-scan-repo"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repos, _ := out["repositories"].([]any)
	require.Len(t, repos, 1)

	repo := repos[0].(map[string]any)
	cfg, _ := repo["imageScanningConfiguration"].(map[string]any)
	assert.Equal(t, true, cfg["scanOnPush"], "PutImageScanningConfiguration must persist via DescribeRepositories")
}

func TestPutRegistryScanningConfiguration_ScanTypeEnhanced(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	rec := doAccuracy(t, h, "PutRegistryScanningConfiguration", map[string]any{
		"scanType": "ENHANCED",
		"rules": []map[string]any{
			{
				"scanFrequency": "CONTINUOUS_SCAN",
				"repositoryFilters": []map[string]any{
					{"filter": "*", "filterType": "WILDCARD"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	cfg, _ := out["scanningConfiguration"].(map[string]any)
	assert.Equal(t, "ENHANCED", cfg["scanType"])
	rules, _ := cfg["rules"].([]any)
	require.Len(t, rules, 1)
}

func TestPutRegistryScanningConfiguration_PersistedViaGet(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	doAccuracy(t, h, "PutRegistryScanningConfiguration", map[string]any{
		"scanType": "ENHANCED",
		"rules":    []map[string]any{},
	})

	rec := doAccuracy(t, h, "GetRegistryScanningConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	cfg, _ := out["scanningConfiguration"].(map[string]any)
	assert.Equal(t, "ENHANCED", cfg["scanType"],
		"PutRegistryScanningConfiguration must persist via GetRegistryScanningConfiguration")
}

func TestPutRegistryScanningConfiguration_DefaultScanTypeBasic(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	// A fresh backend must return BASIC as the default scan type.
	rec := doAccuracy(t, h, "GetRegistryScanningConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	cfg, _ := out["scanningConfiguration"].(map[string]any)
	assert.Equal(t, "BASIC", cfg["scanType"],
		"default scan type must be BASIC before any PutRegistryScanningConfiguration call")
}

func TestBatchGetRepositoryScanningConfiguration_ScanFrequency_ScanOnPush(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "scan-on-push-repo",
		"imageScanningConfiguration": map[string]any{
			"scanOnPush": true,
		},
	})

	rec := doAccuracy(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
		"repositoryNames": []string{"scan-on-push-repo"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	configs, _ := out["scanningConfigurations"].([]any)
	require.Len(t, configs, 1)
	cfg := configs[0].(map[string]any)
	assert.Equal(t, "SCAN_ON_PUSH", cfg["scanFrequency"],
		"scanFrequency must be SCAN_ON_PUSH when scanOnPush=true")
}

func TestBatchGetRepositoryScanningConfiguration_ScanFrequency_Manual(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "manual-scan-repo")

	rec := doAccuracy(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
		"repositoryNames": []string{"manual-scan-repo"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	configs, _ := out["scanningConfigurations"].([]any)
	require.Len(t, configs, 1)
	cfg := configs[0].(map[string]any)
	assert.Equal(t, "MANUAL", cfg["scanFrequency"],
		"scanFrequency must be MANUAL when scanOnPush=false")
}

func TestStartImageScan_And_GetFindings(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "scan-repo-accuracy")
	digest := mustPutImage(t, h, "scan-repo-accuracy", "latest", `{"schemaVersion":2}`)

	scanRec := doAccuracy(t, h, "StartImageScan", map[string]any{
		"repositoryName": "scan-repo-accuracy",
		"imageId":        map[string]any{"imageDigest": digest},
	})
	require.Equal(t, http.StatusOK, scanRec.Code)
	scanOut := parseAccuracy(t, scanRec)
	status, _ := scanOut["imageScanStatus"].(map[string]any)
	assert.NotEmpty(t, status["status"])

	findRec := doAccuracy(t, h, "DescribeImageScanFindings", map[string]any{
		"repositoryName": "scan-repo-accuracy",
		"imageId":        map[string]any{"imageDigest": digest},
	})
	require.Equal(t, http.StatusOK, findRec.Code)
	findOut := parseAccuracy(t, findRec)
	assert.Equal(t, "scan-repo-accuracy", findOut["repositoryName"])
}

func TestBatchGetRepositoryScanningConfiguration_ScanFrequency(t *testing.T) {
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

			h, b := newHandlerWithBackend()
			ctx := context.Background()

			_, err := b.CreateRepository(ctx, "myrepo", "MUTABLE", tt.scanOnPush, "", "")
			require.NoError(t, err)

			if tt.scanType != "" {
				rec := doAccuracy(t, h, "PutRegistryScanningConfiguration", map[string]any{
					"scanType": tt.scanType,
					"rules":    tt.rules,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doAccuracy(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
				"repositoryNames": []string{"myrepo"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			out := parseAccuracy(t, rec)
			configs, _ := out["scanningConfigurations"].([]any)
			require.Len(t, configs, 1)

			cfg, _ := configs[0].(map[string]any)
			assert.Equal(t, tt.wantFrequency, cfg["scanFrequency"])
		})
	}
}

func TestScanFrequency_Wildcard(t *testing.T) {
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

			h, b := newHandlerWithBackend()
			ctx := context.Background()

			_, err := b.CreateRepository(ctx, tt.repoName, "MUTABLE", false, "", "")
			require.NoError(t, err)

			rec := doAccuracy(t, h, "PutRegistryScanningConfiguration", map[string]any{
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

			rec2 := doAccuracy(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
				"repositoryNames": []string{tt.repoName},
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			out := parseAccuracy(t, rec2)
			configs, _ := out["scanningConfigurations"].([]any)
			require.Len(t, configs, 1)
			cfg, _ := configs[0].(map[string]any)
			assert.Equal(t, tt.wantFrequency, cfg["scanFrequency"])
		})
	}
}

func TestECR_BatchGetRepositoryScanningConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		setup           func(*ecr.Handler)
		repositoryNames []string
		wantStatus      int
		wantConfigs     int
		wantFailures    int
	}{
		{
			name:            "nonexistent repository returns failure",
			repositoryNames: []string{"nonexistent"},
			wantStatus:      http.StatusOK,
			wantConfigs:     0,
			wantFailures:    1,
		},
		{
			name: "existing repository returns config",
			setup: func(h *ecr.Handler) {
				rec := doECRRequest(
					t,
					h,
					"CreateRepository",
					map[string]any{"repositoryName": "scan-repo"},
				)
				require.Equal(t, http.StatusOK, rec.Code)
			},
			repositoryNames: []string{"scan-repo"},
			wantStatus:      http.StatusOK,
			wantConfigs:     1,
			wantFailures:    0,
		},
		{
			name:            "empty list returns empty results",
			repositoryNames: []string{},
			wantStatus:      http.StatusOK,
			wantConfigs:     0,
			wantFailures:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doECRRequest(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
				"repositoryNames": tt.repositoryNames,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			out := parseAccuracy(t, rec)
			assert.Len(t, out["scanningConfigurations"], tt.wantConfigs)
			assert.Len(t, out["failures"], tt.wantFailures)
		})
	}
}
