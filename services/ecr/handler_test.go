package ecr_test

// handler_test.go — core Handler/dispatch mechanics for handler.go: Name,
// GetSupportedOperations, MatchPriority, RouteMatcher, ExtractOperation,
// ExtractResource, unknown-action/error-classification dispatch, Reset,
// Snapshot/Restore wiring at the Handler level, Provider.Init, and lazy
// endpoint initialization. Per-family HTTP-level tests live in each family's
// own test file (e.g. repositories_test.go, images_test.go); this file also
// hosts the shared HTTP test helpers (doECRRequest/doAccuracy and friends)
// used package-wide, since nearly every family test file needs them.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ecrsdk "github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ecr"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
	testEndpoint  = "localhost:8000"
)

// ── shared test helpers (used across nearly every family test file) ────────

func newTestHandler(t *testing.T) *ecr.Handler {
	t.Helper()

	backend := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)

	return ecr.NewHandler(backend, nil)
}

func newTestECRClient(t *testing.T, h *ecr.Handler) *ecrsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return ecrsdk.NewFromConfig(cfg, func(o *ecrsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// doECRRequest fires a synthetic ECR control-plane request against h.
func doECRRequest(
	t *testing.T,
	h *ecr.Handler,
	action string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonEC2ContainerRegistry_V20150921."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err = h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// newAccuracyBackend creates a fresh InMemoryBackend for testing.
func newAccuracyBackend() *ecr.InMemoryBackend {
	return ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000")
}

// newAccuracyHandler creates a fresh Handler backed by a fresh InMemoryBackend.
func newAccuracyHandler() *ecr.Handler {
	return ecr.NewHandler(newAccuracyBackend(), nil)
}

// newHandlerWithBackend creates a fresh Handler and returns both it and its
// backing InMemoryBackend, for tests that need to inspect/mutate backend state
// directly alongside issuing HTTP requests.
func newHandlerWithBackend() (*ecr.Handler, *ecr.InMemoryBackend) {
	b := newAccuracyBackend()

	return ecr.NewHandler(b, nil), b
}

// doAccuracy fires a synthetic ECR control-plane request against h and
// returns the raw response. Equivalent to doECRRequest but without the
// explicit Content-Type header.
func doAccuracy(t *testing.T, h *ecr.Handler, action string, body any) *httptest.ResponseRecorder {
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

// parseAccuracy decodes rec's JSON body into a generic map.
func parseAccuracy(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

// mustCreateRepo creates repository name on h, failing the test on error.
func mustCreateRepo(t *testing.T, h *ecr.Handler, name string) {
	t.Helper()
	rec := doAccuracy(t, h, "CreateRepository", map[string]any{"repositoryName": name})
	require.Equal(t, http.StatusOK, rec.Code, "CreateRepository %s failed: %s", name, rec.Body.String())
}

// mustPutImage pushes an image to repoName:tag with manifest, returning its digest.
func mustPutImage(t *testing.T, h *ecr.Handler, repoName, tag, manifest string) string {
	t.Helper()
	rec := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": repoName,
		"imageManifest":  manifest,
		"imageTag":       tag,
	})
	require.Equal(t, http.StatusOK, rec.Code, "PutImage failed: %s", rec.Body.String())
	out := parseAccuracy(t, rec)
	img, _ := out["image"].(map[string]any)
	imageID, _ := img["imageId"].(map[string]any)

	return imageID["imageDigest"].(string)
}

// ── core dispatch tests ──────────────────────────────────────────────────────

func TestECR_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "ECR", h.Name())
}

func TestECR_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateRepository")
	assert.Contains(t, ops, "DescribeRepositories")
	assert.Contains(t, ops, "DeleteRepository")
	assert.Contains(t, ops, "GetAuthorizationToken")
}

func TestECR_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

func TestECR_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		path      string
		wantMatch bool
	}{
		{
			name:      "matching control plane target",
			target:    "AmazonEC2ContainerRegistry_V20150921.CreateRepository",
			path:      "/",
			wantMatch: true,
		},
		{
			name:      "non-matching target",
			target:    "OtherService.Action",
			path:      "/",
			wantMatch: false,
		},
		{
			name:      "v2 path without registry enabled",
			target:    "",
			path:      "/v2/",
			wantMatch: false,
		},
		{
			name:      "v2 prefix path should not match s3control-like paths",
			target:    "",
			path:      "/v20180820/bucket",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}

func TestECR_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		target   string
		want     string
		path     string
		useLocal bool
	}{
		{
			name:   "create repository action",
			target: "AmazonEC2ContainerRegistry_V20150921.CreateRepository",
			want:   "CreateRepository",
		},
		{
			name:   "empty target",
			target: "",
			want:   "Unknown",
		},
		{
			name:   "other service target",
			target: "OtherService.Action",
			want:   "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()

			path := "/"
			if tt.path != "" {
				path = tt.path
			}

			req := httptest.NewRequest(http.MethodPost, path, nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestECR_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "with repositoryName",
			body: `{"repositoryName":"my-repo"}`,
			want: "my-repo",
		},
		{
			name: "with repositoryNames",
			body: `{"repositoryNames":["repo-a","repo-b"]}`,
			want: "repo-a",
		},
		{
			name: "empty body",
			body: `{}`,
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(tt.body)))
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestECR_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECRRequest(t, h, "UnknownAction", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "UnknownOperationException")
}

func TestECR_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &ecr.Provider{}
	assert.Equal(t, "ECR", p.Name())

	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

// TestProviderInit_NilAppContext verifies that Init returns ErrNilAppContext when appCtx is nil.
func TestProviderInit_NilAppContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
	}{
		{name: "nil_ctx_returns_err", wantErr: ecr.ErrNilAppContext},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &ecr.Provider{}
			_, err := p.Init(nil)
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestECR_Persistence(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "persist-me"})
	require.Equal(t, http.StatusOK, rec.Code)

	snapshot := h.Snapshot(t.Context())
	require.NotEmpty(t, snapshot)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(t.Context(), snapshot))

	rec2 := doECRRequest(t, h2, "DescribeRepositories", map[string]any{})
	require.Equal(t, http.StatusOK, rec2.Code)

	resp := parseAccuracy(t, rec2)
	repos, ok := resp["repositories"].([]any)
	require.True(t, ok)
	assert.Len(t, repos, 1)
}

// TestECR_LazyEndpointInit verifies that a Handler with an empty backend
// endpoint sets it from the first request's Host header.
func TestECR_LazyEndpointInit(t *testing.T) {
	t.Parallel()

	backend := ecr.NewInMemoryBackend(testAccountID, testRegion, "")
	h := ecr.NewHandler(backend, nil)

	// First request — Host header provides the server address.
	bodyBytes, err := json.Marshal(map[string]any{"repositoryName": "lazy-repo"})
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Host = "localhost:9999"
	req.Header.Set("X-Amz-Target", "AmazonEC2ContainerRegistry_V20150921.CreateRepository")
	rec := httptest.NewRecorder()

	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseAccuracy(t, rec)
	repo, ok := resp["repository"].(map[string]any)
	require.True(t, ok)
	// RepositoryURI must use the lazily-set endpoint from the Host header.
	assert.Contains(t, repo["repositoryUri"].(string), "localhost:9999")
}

func TestHandlerReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		repo string
	}{
		{name: "resets_after_create", repo: "to-reset"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newHandlerWithBackend()
			rec := doAccuracy(t, h, "CreateRepository", map[string]any{"repositoryName": tt.repo})
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, 1, b.RepositoryCount())

			h.Reset()
			assert.Equal(t, 0, b.RepositoryCount())
		})
	}
}

func TestHandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		action     string
		wantStatus int
	}{
		{
			name:       "describe_repositories",
			action:     "DescribeRepositories",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name:       "get_auth_token",
			action:     "GetAuthorizationToken",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyHandler()
			rec := doAccuracy(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestErrorValidationMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		action     string
		wantStatus int
	}{
		{
			name:       "create_repository_empty_name",
			action:     "CreateRepository",
			body:       map[string]any{"repositoryName": ""},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete_nonexistent_repository",
			action:     "DeleteRepository",
			body:       map[string]any{"repositoryName": "no-such-repo"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_lifecycle_policy_no_repo",
			action:     "DeleteLifecyclePolicy",
			body:       map[string]any{"repositoryName": "ghost"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_registry_policy_none_set",
			action:     "DeleteRegistryPolicy",
			body:       map[string]any{},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyHandler()
			rec := doAccuracy(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestGetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		wantOp string
	}{
		{name: "put_lifecycle_policy", wantOp: "PutLifecyclePolicy"},
		{name: "put_registry_policy", wantOp: "PutRegistryPolicy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyHandler()
			ops := h.GetSupportedOperations()
			assert.True(t, slices.Contains(ops, tt.wantOp), "expected %s in supported operations", tt.wantOp)
		})
	}
}

// TestECR_MissingSDKOperations is a broad smoke test exercising every ECR SDK
// operation end-to-end through the dispatcher, catching regressions where an
// operation is missing from the ops map or returns a non-200 by default.
func TestECR_MissingSDKOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECRRequest(t, h, "CreateRepository", map[string]any{
		"repositoryName": "sdk-repo",
		"imageScanningConfiguration": map[string]any{
			"scanOnPush": true,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doECRRequest(t, h, "PutImage", map[string]any{
		"repositoryName": "sdk-repo",
		"imageManifest":  `{"schemaVersion":2}`,
		"imageTag":       "latest",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	putImage := parseAccuracy(t, rec)
	image := putImage["image"].(map[string]any)
	imageID := image["imageId"].(map[string]any)

	for _, action := range []string{
		"DescribeImages",
		"ListImages",
		"StartImageScan",
		"DescribeImageScanFindings",
		"DescribeImageSigningStatus",
		"DescribeImageReplicationStatus",
		"UpdateImageStorageClass",
		"ListImageReferrers",
	} {
		body := map[string]any{"repositoryName": "sdk-repo"}
		if action != "DescribeImages" && action != "ListImages" {
			body["imageId"] = imageID
		}

		if action == "DescribeImages" {
			body["imageIds"] = []any{imageID}
		}

		if action == "UpdateImageStorageClass" {
			body["targetStorageClass"] = "ARCHIVE"
		}

		if action == "ListImageReferrers" {
			body["subjectId"] = imageID
			delete(body, "imageId")
		}

		rec = doECRRequest(t, h, action, body)
		assert.Equal(t, http.StatusOK, rec.Code, action)
	}

	rec = doECRRequest(t, h, "PutLifecyclePolicy", map[string]any{
		"repositoryName":      "sdk-repo",
		"lifecyclePolicyText": `{"rules":[]}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	for _, action := range []string{"GetLifecyclePolicy", "StartLifecyclePolicyPreview", "GetLifecyclePolicyPreview"} {
		rec = doECRRequest(t, h, action, map[string]any{"repositoryName": "sdk-repo"})
		assert.Equal(t, http.StatusOK, rec.Code, action)
	}

	rec = doECRRequest(t, h, "InitiateLayerUpload", map[string]any{"repositoryName": "sdk-repo"})
	require.Equal(t, http.StatusOK, rec.Code)
	upload := parseAccuracy(t, rec)
	rec = doECRRequest(t, h, "UploadLayerPart", map[string]any{
		"repositoryName": "sdk-repo",
		"uploadId":       upload["uploadId"],
		"partFirstByte":  0,
		"partLastByte":   3,
		"layerPartBlob":  "AQIDBA==",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h, "CompleteLayerUpload", map[string]any{
		"repositoryName": "sdk-repo",
		"uploadId":       upload["uploadId"],
		"layerDigests":   []string{"sha256:layer"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h, "GetDownloadUrlForLayer", map[string]any{
		"repositoryName": "sdk-repo",
		"layerDigest":    "sha256:layer",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doECRRequest(t, h, "SetRepositoryPolicy", map[string]any{
		"repositoryName": "sdk-repo",
		"policyText":     `{"Version":"2012-10-17","Statement":[]}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	for _, action := range []string{"GetRepositoryPolicy", "DeleteRepositoryPolicy"} {
		rec = doECRRequest(t, h, action, map[string]any{"repositoryName": "sdk-repo"})
		assert.Equal(t, http.StatusOK, rec.Code, action)
	}

	for _, action := range []string{"DescribeRegistry", "GetRegistryScanningConfiguration"} {
		rec = doECRRequest(t, h, action, map[string]any{})
		assert.Equal(t, http.StatusOK, rec.Code, action)
	}
	rec = doECRRequest(
		t,
		h,
		"PutRegistryScanningConfiguration",
		map[string]any{"scanType": "BASIC"},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h, "PutReplicationConfiguration", map[string]any{
		"replicationConfiguration": map[string]any{"rules": []any{}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doECRRequest(t, h, "PutRegistryPolicy", map[string]any{"policyText": "{}"})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h, "GetRegistryPolicy", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doECRRequest(t, h, "PutSigningConfiguration", map[string]any{
		"signingConfiguration": map[string]any{"rules": []any{}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	for _, action := range []string{"GetSigningConfiguration", "DeleteSigningConfiguration"} {
		rec = doECRRequest(t, h, action, map[string]any{})
		assert.Equal(t, http.StatusOK, rec.Code, action)
	}

	rec = doECRRequest(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "docker-hub",
		"upstreamRegistryUrl": "registry-1.docker.io",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	for _, action := range []string{
		"DescribePullThroughCacheRules",
		"UpdatePullThroughCacheRule",
		"ValidatePullThroughCacheRule",
	} {
		rec = doECRRequest(t, h, action, map[string]any{"ecrRepositoryPrefix": "docker-hub"})
		assert.Equal(t, http.StatusOK, rec.Code, action)
	}

	rec = doECRRequest(t, h, "CreateRepositoryCreationTemplate", map[string]any{
		"prefix":             "team/",
		"imageTagMutability": "MUTABLE",
		"resourceTags": []map[string]any{
			{"Key": "env", "Value": "test"},
			{"Key": "team", "Value": "platform"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	templateOut := parseAccuracy(t, rec)
	template := templateOut["repositoryCreationTemplate"].(map[string]any)
	require.IsType(t, []any{}, template["resourceTags"])
	for _, action := range []string{
		"DescribeRepositoryCreationTemplates",
		"UpdateRepositoryCreationTemplate",
		"DeleteRepositoryCreationTemplate",
	} {
		rec = doECRRequest(t, h, action, map[string]any{"prefix": "team/"})
		assert.Equal(t, http.StatusOK, rec.Code, action)
	}

	for _, action := range []string{
		"PutImageScanningConfiguration",
		"PutImageTagMutability",
	} {
		body := map[string]any{"repositoryName": "sdk-repo"}
		if action == "PutImageScanningConfiguration" {
			body["imageScanningConfiguration"] = map[string]any{"scanOnPush": false}
		} else {
			body["imageTagMutability"] = "IMMUTABLE"
		}

		rec = doECRRequest(t, h, action, body)
		assert.Equal(t, http.StatusOK, rec.Code, action)
	}

	rec = doECRRequest(t, h, "PutAccountSetting", map[string]any{
		"name":  "BASIC_SCAN_TYPE_VERSION",
		"value": "AWS_NATIVE",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h, "GetAccountSetting", map[string]any{"name": "BASIC_SCAN_TYPE_VERSION"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doECRRequest(t, h, "RegisterPullTimeUpdateExclusion", map[string]any{
		"principalArn": "arn:aws:iam::000000000000:role/example",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h, "ListPullTimeUpdateExclusions", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	exclusionsOut := parseAccuracy(t, rec)
	exclusions := exclusionsOut["pullTimeUpdateExclusions"].([]any)
	require.Len(t, exclusions, 1)
	assert.Equal(t, "arn:aws:iam::000000000000:role/example", exclusions[0])
	rec = doECRRequest(t, h, "DeregisterPullTimeUpdateExclusion", map[string]any{
		"principalArn": "arn:aws:iam::000000000000:role/example",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestECR_SDKClient_NewOperations drives the handler through the real
// aws-sdk-go-v2 ECR client (not synthetic JSON requests) to catch wire-shape
// regressions across a broad swath of newer operations.
func TestECR_SDKClient_NewOperations(t *testing.T) {
	t.Parallel()

	backend := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)
	h := ecr.NewHandler(backend, nil)
	client := newTestECRClient(t, h)
	ctx := context.Background()

	createRepoOut, err := client.CreateRepository(ctx, &ecrsdk.CreateRepositoryInput{
		RepositoryName: aws.String("sdk-client-repo"),
	})
	require.NoError(t, err)
	require.NotNil(t, createRepoOut.Repository)
	require.NotNil(t, createRepoOut.Repository.RegistryId)
	assert.Equal(t, testAccountID, *createRepoOut.Repository.RegistryId)

	putImageOut, err := client.PutImage(ctx, &ecrsdk.PutImageInput{
		ImageManifest:  aws.String(`{"schemaVersion":2}`),
		ImageTag:       aws.String("latest"),
		RepositoryName: aws.String("sdk-client-repo"),
	})
	require.NoError(t, err)
	require.NotNil(t, putImageOut.Image)
	require.NotNil(t, putImageOut.Image.ImageId)

	imageID := putImageOut.Image.ImageId
	_, err = client.PutSigningConfiguration(ctx, &ecrsdk.PutSigningConfigurationInput{
		SigningConfiguration: &types.SigningConfiguration{Rules: []types.SigningRule{{
			SigningProfileArn: aws.String(
				"arn:aws:signer:us-east-1:000000000000:/signing-profiles/test",
			),
		}}},
	})
	require.NoError(t, err)
	signingOut, err := client.DescribeImageSigningStatus(
		ctx,
		&ecrsdk.DescribeImageSigningStatusInput{
			ImageId:        imageID,
			RepositoryName: aws.String("sdk-client-repo"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, signingOut.RegistryId)
	assert.Equal(t, testAccountID, *signingOut.RegistryId)
	require.Len(t, signingOut.SigningStatuses, 1)
	assert.Equal(t, types.SigningStatusComplete, signingOut.SigningStatuses[0].Status)

	_, err = client.PutRegistryScanningConfiguration(
		ctx,
		&ecrsdk.PutRegistryScanningConfigurationInput{
			ScanType: types.ScanTypeEnhanced,
			Rules: []types.RegistryScanningRule{{
				ScanFrequency: types.ScanFrequencyScanOnPush,
				RepositoryFilters: []types.ScanningRepositoryFilter{{
					Filter:     aws.String("sdk-client-*"),
					FilterType: types.ScanningRepositoryFilterTypeWildcard,
				}},
			}},
		},
	)
	require.NoError(t, err)
	scanningOut, err := client.GetRegistryScanningConfiguration(
		ctx,
		&ecrsdk.GetRegistryScanningConfigurationInput{},
	)
	require.NoError(t, err)
	require.NotNil(t, scanningOut.ScanningConfiguration)
	assert.Equal(t, types.ScanTypeEnhanced, scanningOut.ScanningConfiguration.ScanType)
	assert.Len(t, scanningOut.ScanningConfiguration.Rules, 1)

	_, err = client.PutReplicationConfiguration(ctx, &ecrsdk.PutReplicationConfigurationInput{
		ReplicationConfiguration: &types.ReplicationConfiguration{Rules: []types.ReplicationRule{{
			Destinations: []types.ReplicationDestination{{
				Region:     aws.String("us-west-2"),
				RegistryId: aws.String(testAccountID),
			}},
		}}},
	})
	require.NoError(t, err)
	registryOut, err := client.DescribeRegistry(ctx, &ecrsdk.DescribeRegistryInput{})
	require.NoError(t, err)
	require.NotNil(t, registryOut.RegistryId)
	assert.Equal(t, testAccountID, *registryOut.RegistryId)
	require.NotNil(t, registryOut.ReplicationConfiguration)
	assert.Len(t, registryOut.ReplicationConfiguration.Rules, 1)

	_, err = client.CreatePullThroughCacheRule(ctx, &ecrsdk.CreatePullThroughCacheRuleInput{
		EcrRepositoryPrefix: aws.String("docker-hub"),
		UpstreamRegistryUrl: aws.String("registry-1.docker.io"),
	})
	require.NoError(t, err)
	rulesOut, err := client.DescribePullThroughCacheRules(
		ctx,
		&ecrsdk.DescribePullThroughCacheRulesInput{},
	)
	require.NoError(t, err)
	require.Len(t, rulesOut.PullThroughCacheRules, 1)
	assert.Equal(t, "docker-hub", *rulesOut.PullThroughCacheRules[0].EcrRepositoryPrefix)

	_, err = client.CreateRepositoryCreationTemplate(
		ctx,
		&ecrsdk.CreateRepositoryCreationTemplateInput{
			Prefix:             aws.String("team/"),
			AppliedFor:         []types.RCTAppliedFor{types.RCTAppliedForCreateOnPush},
			ImageTagMutability: types.ImageTagMutabilityMutable,
			ResourceTags: []types.Tag{
				{Key: aws.String("team"), Value: aws.String("platform")},
			},
		},
	)
	require.NoError(t, err)
	templatesOut, err := client.DescribeRepositoryCreationTemplates(
		ctx,
		&ecrsdk.DescribeRepositoryCreationTemplatesInput{},
	)
	require.NoError(t, err)
	require.Len(t, templatesOut.RepositoryCreationTemplates, 1)
	assert.Equal(t, "team/", *templatesOut.RepositoryCreationTemplates[0].Prefix)
	assert.Len(t, templatesOut.RepositoryCreationTemplates[0].ResourceTags, 1)

	_, err = client.RegisterPullTimeUpdateExclusion(
		ctx,
		&ecrsdk.RegisterPullTimeUpdateExclusionInput{
			PrincipalArn: aws.String("arn:aws:iam::000000000000:role/sdk-client"),
		},
	)
	require.NoError(t, err)
	exclusionsOut, err := client.ListPullTimeUpdateExclusions(
		ctx,
		&ecrsdk.ListPullTimeUpdateExclusionsInput{},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		[]string{"arn:aws:iam::000000000000:role/sdk-client"},
		exclusionsOut.PullTimeUpdateExclusions,
	)
}

// TestECR_NewOps_PersistenceRoundTrip verifies that Handler.Snapshot/Restore
// (as opposed to backend-direct Snapshot/Restore, covered in
// persistence_test.go) round-trips state across nearly every op family in one
// pass, through the HTTP dispatcher.
func TestECR_NewOps_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	backend := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)
	h := ecr.NewHandler(backend, nil)

	// Create a repo
	rec := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "persist-repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Complete a layer upload
	rec = doECRRequest(t, h, "CompleteLayerUpload", map[string]any{
		"repositoryName": "persist-repo",
		"uploadId":       "upload-xyz",
		"layerDigests": []string{
			"sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h, "PutImage", map[string]any{
		"repositoryName": "persist-repo",
		"imageDigest":    "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
		"imageManifest":  "{}",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create pull-through cache rule
	rec = doECRRequest(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "test-prefix",
		"upstreamRegistryUrl": "registry.example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create repository creation template
	rec = doECRRequest(t, h, "CreateRepositoryCreationTemplate", map[string]any{
		"prefix": "my-org",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doECRRequest(t, h, "PutLifecyclePolicy", map[string]any{
		"repositoryName":      "persist-repo",
		"lifecyclePolicyText": `{"rules":[]}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h, "StartLifecyclePolicyPreview", map[string]any{
		"repositoryName": "persist-repo",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h, "StartImageScan", map[string]any{
		"repositoryName": "persist-repo",
		"imageId": map[string]any{
			"imageDigest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h, "PutRegistryScanningConfiguration", map[string]any{
		"scanType": "ENHANCED",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h, "PutReplicationConfiguration", map[string]any{
		"replicationConfiguration": map[string]any{
			"rules": []map[string]any{{
				"destinations": []map[string]any{
					{"region": "us-west-2", "registryId": testAccountID},
				},
			}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h, "PutSigningConfiguration", map[string]any{
		"signingConfiguration": map[string]any{
			"rules": []map[string]any{{
				"signingProfileArn": "arn:aws:signer:us-east-1:000000000000:/signing-profiles/test",
			}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h, "PutAccountSetting", map[string]any{
		"name":  "BASIC_SCAN_TYPE_VERSION",
		"value": "AWS_NATIVE",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h, "RegisterPullTimeUpdateExclusion", map[string]any{
		"principalArn": "arn:aws:iam::000000000000:role/persist",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Seed a registry policy
	backend.SetRegistryPolicyInternal(`{"Version":"2012-10-17"}`)

	// Snapshot and restore
	snapshot := h.Snapshot(t.Context())
	require.NotEmpty(t, snapshot)

	backend2 := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)
	h2 := ecr.NewHandler(backend2, nil)
	require.NoError(t, h2.Restore(t.Context(), snapshot))

	// Verify layer availability is restored
	rec = doECRRequest(t, h2, "BatchCheckLayerAvailability", map[string]any{
		"repositoryName": "persist-repo",
		"layerDigests": []string{
			"sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Len(t, out["layers"], 1)

	// Verify cache rule is restored by deleting it
	rec = doECRRequest(t, h2, "DeletePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "test-prefix",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify registry policy is restored by deleting it
	rec = doECRRequest(t, h2, "DeleteRegistryPolicy", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doECRRequest(
		t,
		h2,
		"GetLifecyclePolicy",
		map[string]any{"repositoryName": "persist-repo"},
	)
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(
		t,
		h2,
		"GetLifecyclePolicyPreview",
		map[string]any{"repositoryName": "persist-repo"},
	)
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h2, "DescribeImageScanFindings", map[string]any{
		"repositoryName": "persist-repo",
		"imageId": map[string]any{
			"imageDigest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h2, "GetRegistryScanningConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h2, "DescribeRegistry", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h2, "GetSigningConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(
		t,
		h2,
		"GetAccountSetting",
		map[string]any{"name": "BASIC_SCAN_TYPE_VERSION"},
	)
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h2, "DeregisterPullTimeUpdateExclusion", map[string]any{
		"principalArn": "arn:aws:iam::000000000000:role/persist",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}
