package ecr_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
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

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return ecrsdk.NewFromConfig(cfg, func(o *ecrsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func doECRRequest(t *testing.T, h *ecr.Handler, action string, body any) *httptest.ResponseRecorder {
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

func TestECR_CreateRepository(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
		wantARN  bool
		wantURI  bool
	}{
		{
			name:     "success",
			input:    map[string]any{"repositoryName": "my-repo"},
			wantCode: http.StatusOK,
			wantARN:  true,
			wantURI:  true,
		},
		{
			name:     "missing repository name",
			input:    map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECRRequest(t, h, "CreateRepository", tt.input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantARN || tt.wantURI {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				repo, ok := resp["repository"].(map[string]any)
				require.True(t, ok)

				if tt.wantARN {
					assert.NotEmpty(t, repo["repositoryArn"])
				}

				if tt.wantURI {
					assert.NotEmpty(t, repo["repositoryUri"])
				}
			}
		})
	}
}

func TestECR_CreateRepository_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "my-repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "my-repo"})
	require.Equal(t, http.StatusBadRequest, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "RepositoryAlreadyExistsException")
}

func TestECR_DescribeRepositories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		repos    []string
		filter   []string
		wantCode int
		wantLen  int
	}{
		{
			name:     "list all",
			repos:    []string{"repo-a", "repo-b"},
			filter:   nil,
			wantCode: http.StatusOK,
			wantLen:  2,
		},
		{
			name:     "filter by name",
			repos:    []string{"repo-a", "repo-b"},
			filter:   []string{"repo-a"},
			wantCode: http.StatusOK,
			wantLen:  1,
		},
		{
			name:     "filter not found",
			repos:    []string{"repo-a"},
			filter:   []string{"repo-missing"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, repoName := range tt.repos {
				rec := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": repoName})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			input := map[string]any{}
			if tt.filter != nil {
				input["repositoryNames"] = tt.filter
			}

			rec := doECRRequest(t, h, "DescribeRepositories", input)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantLen > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				repos, ok := resp["repositories"].([]any)
				require.True(t, ok)
				assert.Len(t, repos, tt.wantLen)
			}
		})
	}
}

func TestECR_DeleteRepository(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		create   string
		delete   string
		wantCode int
	}{
		{
			name:     "success",
			create:   "my-repo",
			delete:   "my-repo",
			wantCode: http.StatusOK,
		},
		{
			name:     "not found",
			create:   "",
			delete:   "nonexistent",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.create != "" {
				rec := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": tt.create})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doECRRequest(t, h, "DeleteRepository", map[string]any{"repositoryName": tt.delete})
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				repo, ok := resp["repository"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.delete, repo["repositoryName"])
			}
		})
	}
}

func TestECR_GetAuthorizationToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECRRequest(t, h, "GetAuthorizationToken", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	authData, ok := resp["authorizationData"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, authData)

	entry, ok := authData[0].(map[string]any)
	require.True(t, ok)

	tokenRaw, ok := entry["authorizationToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, tokenRaw)

	decoded, err := base64.StdEncoding.DecodeString(tokenRaw)
	require.NoError(t, err)
	assert.Equal(t, "AWS:dummy-password", string(decoded))

	assert.NotZero(t, entry["expiresAt"])
}

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

	var putImage map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putImage))
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
	var upload map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &upload))
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
	rec = doECRRequest(t, h, "PutRegistryScanningConfiguration", map[string]any{"scanType": "BASIC"})
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
	var templateOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &templateOut))
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
	var exclusionsOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &exclusionsOut))
	exclusions := exclusionsOut["pullTimeUpdateExclusions"].([]any)
	require.Len(t, exclusions, 1)
	assert.Equal(t, "arn:aws:iam::000000000000:role/example", exclusions[0])
	rec = doECRRequest(t, h, "DeregisterPullTimeUpdateExclusion", map[string]any{
		"principalArn": "arn:aws:iam::000000000000:role/example",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
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

func TestECR_Provider_Init_WithLocalRegistry(t *testing.T) {
	t.Setenv("GOPHERSTACK_ENABLE_LOCAL_REGISTRY", "1")

	p := &ecr.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)

	h, ok := svc.(*ecr.Handler)
	require.True(t, ok)
	assert.True(t, h.RegistryEnabled())
}

func TestECR_RouteMatcher_V2Path_WithRegistryEnabled(t *testing.T) {
	t.Setenv("GOPHERSTACK_ENABLE_LOCAL_REGISTRY", "1")

	p := &ecr.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)

	h, ok := svc.(*ecr.Handler)
	require.True(t, ok)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/v2/", nil)
	c := e.NewContext(req, httptest.NewRecorder())
	assert.True(t, h.RouteMatcher()(c))
}

func TestECR_Handler_V2Path_ProxiesRegistry(t *testing.T) {
	t.Setenv("GOPHERSTACK_ENABLE_LOCAL_REGISTRY", "1")

	p := &ecr.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)

	h, ok := svc.(*ecr.Handler)
	require.True(t, ok)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/v2/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err = h.Handler()(c)
	require.NoError(t, err)
	// Distribution registry responds 200 for /v2/ when no auth is configured.
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestECR_ExtractOperation_V2Path_WithRegistryEnabled(t *testing.T) {
	t.Setenv("GOPHERSTACK_ENABLE_LOCAL_REGISTRY", "1")

	p := &ecr.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)

	h, ok := svc.(*ecr.Handler)
	require.True(t, ok)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/v2/", nil)
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "RegistryV2", h.ExtractOperation(c))
}

func TestECR_Backend_SetEndpoint(t *testing.T) {
	t.Parallel()

	backend := ecr.NewInMemoryBackend(testAccountID, testRegion, "")

	backend.SetEndpoint("localhost:9000")

	repo, err := backend.CreateRepository("my-repo", "", false, "", "")
	require.NoError(t, err)
	assert.Contains(t, repo.RepositoryURI, "localhost:9000")
}

func TestECR_Persistence(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "persist-me"})
	require.Equal(t, http.StatusOK, rec.Code)

	snapshot := h.Snapshot()
	require.NotEmpty(t, snapshot)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(snapshot))

	rec2 := doECRRequest(t, h2, "DescribeRepositories", map[string]any{})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

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

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	repo, ok := resp["repository"].(map[string]any)
	require.True(t, ok)
	// RepositoryURI must use the lazily-set endpoint from the Host header.
	assert.Contains(t, repo["repositoryUri"].(string), "localhost:9999")
}

// TestECR_ExtractResource_V2Path checks that ExtractResource extracts the
// repository name from the URL path instead of reading the request body.
func TestECR_ExtractResource_V2Path(t *testing.T) {
	t.Parallel()

	backend := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)
	// Pass a no-op http.Handler to enable registry mode.
	h := ecr.NewHandler(backend, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	tests := []struct {
		path string
		want string
	}{
		{path: "/v2/my-repo/manifests/latest", want: "my-repo"},
		{path: "/v2/org/app/blobs/sha256:abc", want: "org"},
		{path: "/v2/", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

// TestECR_RouteMatcher_V2Strict checks that the route matcher does not match
// paths that start with "/v2" but are not the Docker registry v2 API
// (e.g. S3Control's "/v20180820/..." paths).
func TestECR_RouteMatcher_V2Strict(t *testing.T) {
	t.Parallel()

	backend := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)
	// Pass a no-op http.Handler to enable registry mode.
	h := ecr.NewHandler(backend, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	tests := []struct {
		path      string
		wantMatch bool
	}{
		{path: "/v2/", wantMatch: true},
		{path: "/v2/my-repo/manifests/latest", wantMatch: true},
		{path: "/v2", wantMatch: true},
		{path: "/v20180820/bucket", wantMatch: false},
		{path: "/v2abc/something", wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}

func TestECR_ListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a repo first so we have an ARN.
	rec := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "tag-repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	repo := createResp["repository"].(map[string]any)
	arn := repo["repositoryArn"].(string)

	rec = doECRRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	tags, ok := out["tags"]
	require.True(t, ok, "response should contain 'tags' field")
	assert.IsType(t, []any{}, tags, "tags should be an array")
}

func TestECR_TagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECRRequest(t, h, "TagResource", map[string]any{
		"resourceArn": "arn:aws:ecr:us-east-1:000000000000:repository/my-repo",
		"tags":        []map[string]any{{"Key": "Env", "Value": "test"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestECR_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECRRequest(t, h, "UntagResource", map[string]any{
		"resourceArn": "arn:aws:ecr:us-east-1:000000000000:repository/my-repo",
		"tagKeys":     []string{"Env"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
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
			rec0 := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": tt.repositoryName})
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

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out["layers"], tt.wantLayers)
			assert.Len(t, out["failures"], tt.wantFailures)
		})
	}
}

func TestECR_BatchDeleteImage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*ecr.InMemoryBackend)
		name           string
		repositoryName string
		imageIDs       []map[string]any
		wantStatus     int
		wantDeleted    int
		wantFailures   int
	}{
		{
			name: "image not found returns failure",
			setup: func(b *ecr.InMemoryBackend) {
				b.CreateRepoInternal("my-repo")
			},
			repositoryName: "my-repo",
			imageIDs:       []map[string]any{{"imageDigest": "sha256:notfound"}},
			wantStatus:     http.StatusOK,
			wantDeleted:    0,
			wantFailures:   1,
		},
		{
			name: "empty image list returns empty results",
			setup: func(b *ecr.InMemoryBackend) {
				b.CreateRepoInternal("my-repo")
			},
			repositoryName: "my-repo",
			imageIDs:       []map[string]any{},
			wantStatus:     http.StatusOK,
			wantDeleted:    0,
			wantFailures:   0,
		},
		{
			name: "delete by digest succeeds",
			setup: func(b *ecr.InMemoryBackend) {
				b.CreateRepoInternal("my-repo")
				b.AddImageInternal("my-repo", ecr.Image{
					ImageDigest:    "sha256:abc111",
					ImageID:        ecr.ImageIdentifier{ImageDigest: "sha256:abc111"},
					RepositoryName: "my-repo",
					RegistryID:     testAccountID,
				})
			},
			repositoryName: "my-repo",
			imageIDs:       []map[string]any{{"imageDigest": "sha256:abc111"}},
			wantStatus:     http.StatusOK,
			wantDeleted:    1,
			wantFailures:   0,
		},
		{
			name: "delete by tag succeeds",
			setup: func(b *ecr.InMemoryBackend) {
				b.CreateRepoInternal("my-repo")
				b.AddImageInternal("my-repo", ecr.Image{
					ImageDigest:    "sha256:tag111",
					ImageID:        ecr.ImageIdentifier{ImageDigest: "sha256:tag111", ImageTag: "latest"},
					RepositoryName: "my-repo",
					RegistryID:     testAccountID,
				})
			},
			repositoryName: "my-repo",
			imageIDs:       []map[string]any{{"imageTag": "latest"}},
			wantStatus:     http.StatusOK,
			wantDeleted:    1,
			wantFailures:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)
			if tt.setup != nil {
				tt.setup(backend)
			}

			h := ecr.NewHandler(backend, nil)

			rec := doECRRequest(t, h, "BatchDeleteImage", map[string]any{
				"repositoryName": tt.repositoryName,
				"imageIds":       tt.imageIDs,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out["imageIds"], tt.wantDeleted)
			assert.Len(t, out["failures"], tt.wantFailures)
		})
	}
}

func TestECR_BatchGetImage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*ecr.InMemoryBackend)
		name           string
		repositoryName string
		imageIDs       []map[string]any
		wantStatus     int
		wantImages     int
		wantFailures   int
	}{
		{
			name: "image not found returns failure",
			setup: func(b *ecr.InMemoryBackend) {
				b.CreateRepoInternal("my-repo")
			},
			repositoryName: "my-repo",
			imageIDs:       []map[string]any{{"imageDigest": "sha256:notfound"}},
			wantStatus:     http.StatusOK,
			wantImages:     0,
			wantFailures:   1,
		},
		{
			name: "empty image list returns empty results",
			setup: func(b *ecr.InMemoryBackend) {
				b.CreateRepoInternal("my-repo")
			},
			repositoryName: "my-repo",
			imageIDs:       []map[string]any{},
			wantStatus:     http.StatusOK,
			wantImages:     0,
			wantFailures:   0,
		},
		{
			name: "get image by digest succeeds",
			setup: func(b *ecr.InMemoryBackend) {
				b.CreateRepoInternal("my-repo")
				b.AddImageInternal("my-repo", ecr.Image{
					ImageDigest:    "sha256:getdig",
					ImageManifest:  `{"schemaVersion":2}`,
					ImageID:        ecr.ImageIdentifier{ImageDigest: "sha256:getdig"},
					RepositoryName: "my-repo",
					RegistryID:     testAccountID,
				})
			},
			repositoryName: "my-repo",
			imageIDs:       []map[string]any{{"imageDigest": "sha256:getdig"}},
			wantStatus:     http.StatusOK,
			wantImages:     1,
			wantFailures:   0,
		},
		{
			name: "get image by tag succeeds",
			setup: func(b *ecr.InMemoryBackend) {
				b.CreateRepoInternal("my-repo")
				b.AddImageInternal("my-repo", ecr.Image{
					ImageDigest:    "sha256:gettag",
					ImageManifest:  `{"schemaVersion":2}`,
					ImageID:        ecr.ImageIdentifier{ImageDigest: "sha256:gettag", ImageTag: "stable"},
					RepositoryName: "my-repo",
					RegistryID:     testAccountID,
				})
			},
			repositoryName: "my-repo",
			imageIDs:       []map[string]any{{"imageTag": "stable"}},
			wantStatus:     http.StatusOK,
			wantImages:     1,
			wantFailures:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)
			if tt.setup != nil {
				tt.setup(backend)
			}

			h := ecr.NewHandler(backend, nil)

			rec := doECRRequest(t, h, "BatchGetImage", map[string]any{
				"repositoryName": tt.repositoryName,
				"imageIds":       tt.imageIDs,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out["images"], tt.wantImages)
			assert.Len(t, out["failures"], tt.wantFailures)
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
				rec := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "scan-repo"})
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

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out["scanningConfigurations"], tt.wantConfigs)
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

			rec := doECRRequest(t, h, "CompleteLayerUpload", map[string]any{
				"repositoryName": tt.repositoryName,
				"uploadId":       tt.uploadID,
				"layerDigests":   tt.layerDigests,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantDigest, out["layerDigest"])
			assert.Equal(t, tt.repositoryName, out["repositoryName"])
			assert.Equal(t, tt.uploadID, out["uploadId"])
		})
	}
}

func TestECR_CreatePullThroughCacheRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		ecrRepositoryPrefix string
		upstreamRegistryURL string
		wantPrefix          string
		wantStatus          int
	}{
		{
			name:                "creates rule successfully",
			ecrRepositoryPrefix: "docker-hub",
			upstreamRegistryURL: "registry-1.docker.io",
			wantStatus:          http.StatusOK,
			wantPrefix:          "docker-hub",
		},
		{
			name:                "empty prefix returns error",
			ecrRepositoryPrefix: "",
			upstreamRegistryURL: "registry-1.docker.io",
			wantStatus:          http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doECRRequest(t, h, "CreatePullThroughCacheRule", map[string]any{
				"ecrRepositoryPrefix": tt.ecrRepositoryPrefix,
				"upstreamRegistryUrl": tt.upstreamRegistryURL,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.wantPrefix, out["ecrRepositoryPrefix"])
			}
		})
	}
}

func TestECR_CreatePullThroughCacheRule_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECRRequest(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "docker-hub",
		"upstreamRegistryUrl": "registry-1.docker.io",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doECRRequest(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "docker-hub",
		"upstreamRegistryUrl": "registry-1.docker.io",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Contains(t, out["__type"], "PullThroughCacheRuleAlreadyExistsException")
}

func TestECR_CreateRepositoryCreationTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		prefix     string
		wantStatus int
	}{
		{
			name:       "creates template successfully",
			prefix:     "my-org",
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty prefix returns error",
			prefix:     "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doECRRequest(t, h, "CreateRepositoryCreationTemplate", map[string]any{
				"prefix": tt.prefix,
				"resourceTags": []map[string]any{
					{"Key": "env", "Value": "test"},
				},
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				tmpl, ok := out["repositoryCreationTemplate"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.prefix, tmpl["prefix"])
				require.IsType(t, []any{}, tmpl["resourceTags"])
			}
		})
	}
}

func TestECR_DeleteLifecyclePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(*ecr.Handler)
		repositoryName string
		wantStatus     int
	}{
		{
			name: "deletes lifecycle policy for existing repo",
			setup: func(h *ecr.Handler) {
				rec := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "policy-repo"})
				require.Equal(t, http.StatusOK, rec.Code)

				rec2 := doECRRequest(t, h, "PutLifecyclePolicy", map[string]any{
					"repositoryName":      "policy-repo",
					"lifecyclePolicyText": `{"rules":[]}`,
				})
				require.Equal(t, http.StatusOK, rec2.Code)
			},
			repositoryName: "policy-repo",
			wantStatus:     http.StatusOK,
		},
		{
			name:           "nonexistent repository returns not found",
			repositoryName: "nonexistent",
			wantStatus:     http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doECRRequest(t, h, "DeleteLifecyclePolicy", map[string]any{
				"repositoryName": tt.repositoryName,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.repositoryName, out["repositoryName"])
			}
		})
	}
}

func TestECR_DeletePullThroughCacheRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		setup               func(*ecr.Handler)
		ecrRepositoryPrefix string
		wantStatus          int
	}{
		{
			name: "deletes existing rule",
			setup: func(h *ecr.Handler) {
				rec := doECRRequest(t, h, "CreatePullThroughCacheRule", map[string]any{
					"ecrRepositoryPrefix": "docker-hub",
					"upstreamRegistryUrl": "registry-1.docker.io",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			ecrRepositoryPrefix: "docker-hub",
			wantStatus:          http.StatusOK,
		},
		{
			name:                "nonexistent rule returns not found",
			ecrRepositoryPrefix: "nonexistent",
			wantStatus:          http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doECRRequest(t, h, "DeletePullThroughCacheRule", map[string]any{
				"ecrRepositoryPrefix": tt.ecrRepositoryPrefix,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.ecrRepositoryPrefix, out["ecrRepositoryPrefix"])
			}
		})
	}
}

func TestECR_DeleteRegistryPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*ecr.InMemoryBackend)
		name       string
		wantStatus int
	}{
		{
			name:       "no registry policy returns not found",
			wantStatus: http.StatusNotFound,
		},
		{
			name: "deletes existing registry policy",
			setup: func(b *ecr.InMemoryBackend) {
				b.SetRegistryPolicyInternal(`{"Version":"2012-10-17","Statement":[]}`)
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)
			if tt.setup != nil {
				tt.setup(backend)
			}

			h := ecr.NewHandler(backend, nil)

			rec := doECRRequest(t, h, "DeleteRegistryPolicy", map[string]any{})
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out["policyText"])
				assert.Equal(t, testAccountID, out["registryId"])
			}
		})
	}
}

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
			SigningProfileArn: aws.String("arn:aws:signer:us-east-1:000000000000:/signing-profiles/test"),
		}}},
	})
	require.NoError(t, err)
	signingOut, err := client.DescribeImageSigningStatus(ctx, &ecrsdk.DescribeImageSigningStatusInput{
		ImageId:        imageID,
		RepositoryName: aws.String("sdk-client-repo"),
	})
	require.NoError(t, err)
	require.NotNil(t, signingOut.RegistryId)
	assert.Equal(t, testAccountID, *signingOut.RegistryId)
	require.Len(t, signingOut.SigningStatuses, 1)
	assert.Equal(t, types.SigningStatusComplete, signingOut.SigningStatuses[0].Status)

	_, err = client.PutRegistryScanningConfiguration(ctx, &ecrsdk.PutRegistryScanningConfigurationInput{
		ScanType: types.ScanTypeEnhanced,
		Rules: []types.RegistryScanningRule{{
			ScanFrequency: types.ScanFrequencyScanOnPush,
			RepositoryFilters: []types.ScanningRepositoryFilter{{
				Filter:     aws.String("sdk-client-*"),
				FilterType: types.ScanningRepositoryFilterTypeWildcard,
			}},
		}},
	})
	require.NoError(t, err)
	scanningOut, err := client.GetRegistryScanningConfiguration(ctx, &ecrsdk.GetRegistryScanningConfigurationInput{})
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
	rulesOut, err := client.DescribePullThroughCacheRules(ctx, &ecrsdk.DescribePullThroughCacheRulesInput{})
	require.NoError(t, err)
	require.Len(t, rulesOut.PullThroughCacheRules, 1)
	assert.Equal(t, "docker-hub", *rulesOut.PullThroughCacheRules[0].EcrRepositoryPrefix)

	_, err = client.CreateRepositoryCreationTemplate(ctx, &ecrsdk.CreateRepositoryCreationTemplateInput{
		Prefix:             aws.String("team/"),
		AppliedFor:         []types.RCTAppliedFor{types.RCTAppliedForCreateOnPush},
		ImageTagMutability: types.ImageTagMutabilityMutable,
		ResourceTags:       []types.Tag{{Key: aws.String("team"), Value: aws.String("platform")}},
	})
	require.NoError(t, err)
	templatesOut, err := client.DescribeRepositoryCreationTemplates(
		ctx,
		&ecrsdk.DescribeRepositoryCreationTemplatesInput{},
	)
	require.NoError(t, err)
	require.Len(t, templatesOut.RepositoryCreationTemplates, 1)
	assert.Equal(t, "team/", *templatesOut.RepositoryCreationTemplates[0].Prefix)
	assert.Len(t, templatesOut.RepositoryCreationTemplates[0].ResourceTags, 1)

	_, err = client.RegisterPullTimeUpdateExclusion(ctx, &ecrsdk.RegisterPullTimeUpdateExclusionInput{
		PrincipalArn: aws.String("arn:aws:iam::000000000000:role/sdk-client"),
	})
	require.NoError(t, err)
	exclusionsOut, err := client.ListPullTimeUpdateExclusions(ctx, &ecrsdk.ListPullTimeUpdateExclusionsInput{})
	require.NoError(t, err)
	assert.Equal(t, []string{"arn:aws:iam::000000000000:role/sdk-client"}, exclusionsOut.PullTimeUpdateExclusions)
}

func TestECR_BackendPutImageReturnsDefensiveCopy(t *testing.T) {
	t.Parallel()

	backend := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)
	_, err := backend.CreateRepository("copy-repo", "", false, "", "")
	require.NoError(t, err)

	img, err := backend.PutImage("copy-repo", ecr.Image{
		ImageManifest: `{"schemaVersion":2}`,
		ImageID:       ecr.ImageIdentifier{ImageTag: "latest"},
	})
	require.NoError(t, err)
	img.ImageStatus = "CORRUPTED"
	img.ImageID.ImageTag = "changed"

	stored, err := backend.DescribeImages("copy-repo", []ecr.ImageIdentifier{{ImageDigest: img.ImageDigest}})
	require.NoError(t, err)
	require.Len(t, stored, 1)
	assert.Equal(t, "ACTIVE", stored[0].ImageStatus)
	assert.Equal(t, "latest", stored[0].ImageID.ImageTag)
}

func TestECR_RestoreClearsInFlightLayerUploads(t *testing.T) {
	t.Parallel()

	backend := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)
	h := ecr.NewHandler(backend, nil)

	rec := doECRRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "restore-repo"})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h, "InitiateLayerUpload", map[string]any{"repositoryName": "restore-repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	var upload map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &upload))
	snapshot := h.Snapshot()
	require.NotEmpty(t, snapshot)
	require.NoError(t, h.Restore(snapshot))

	rec = doECRRequest(t, h, "UploadLayerPart", map[string]any{
		"repositoryName": "restore-repo",
		"uploadId":       upload["uploadId"],
		"partFirstByte":  0,
		"partLastByte":   0,
		"layerPartBlob":  "AQ==",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

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
		"layerDigests":   []string{"sha256:persist123"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h, "PutImage", map[string]any{
		"repositoryName": "persist-repo",
		"imageDigest":    "sha256:persist123",
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
		"imageId":        map[string]any{"imageDigest": "sha256:persist123"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h, "PutRegistryScanningConfiguration", map[string]any{
		"scanType": "ENHANCED",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h, "PutReplicationConfiguration", map[string]any{
		"replicationConfiguration": map[string]any{
			"rules": []map[string]any{{
				"destinations": []map[string]any{{"region": "us-west-2", "registryId": testAccountID}},
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
	snapshot := h.Snapshot()
	require.NotEmpty(t, snapshot)

	backend2 := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)
	h2 := ecr.NewHandler(backend2, nil)
	require.NoError(t, h2.Restore(snapshot))

	// Verify layer availability is restored
	rec = doECRRequest(t, h2, "BatchCheckLayerAvailability", map[string]any{
		"repositoryName": "persist-repo",
		"layerDigests":   []string{"sha256:persist123"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out["layers"], 1)

	// Verify cache rule is restored by deleting it
	rec = doECRRequest(t, h2, "DeletePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "test-prefix",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify registry policy is restored by deleting it
	rec = doECRRequest(t, h2, "DeleteRegistryPolicy", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doECRRequest(t, h2, "GetLifecyclePolicy", map[string]any{"repositoryName": "persist-repo"})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h2, "GetLifecyclePolicyPreview", map[string]any{"repositoryName": "persist-repo"})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h2, "DescribeImageScanFindings", map[string]any{
		"repositoryName": "persist-repo",
		"imageId":        map[string]any{"imageDigest": "sha256:persist123"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h2, "GetRegistryScanningConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h2, "DescribeRegistry", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h2, "GetSigningConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h2, "GetAccountSetting", map[string]any{"name": "BASIC_SCAN_TYPE_VERSION"})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doECRRequest(t, h2, "DeregisterPullTimeUpdateExclusion", map[string]any{
		"principalArn": "arn:aws:iam::000000000000:role/persist",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestECR_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	backend := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)

	assert.Equal(t, 0, backend.ImageCount())
	assert.Equal(t, 0, backend.PullThroughCacheRuleCount())
	assert.Equal(t, 0, backend.RepositoryCreationTemplateCount())
	assert.Equal(t, 0, backend.LifecyclePolicyCount())

	backend.AddImageInternal("repo1", ecr.Image{
		ImageDigest:    "sha256:count1",
		ImageID:        ecr.ImageIdentifier{ImageDigest: "sha256:count1"},
		RepositoryName: "repo1",
	})
	assert.Equal(t, 1, backend.ImageCount())

	backend.AddImageInternal("repo1", ecr.Image{
		ImageDigest:    "sha256:count2",
		ImageID:        ecr.ImageIdentifier{ImageDigest: "sha256:count2"},
		RepositoryName: "repo1",
	})
	assert.Equal(t, 2, backend.ImageCount())
}
