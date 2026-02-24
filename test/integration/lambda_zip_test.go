package integration_test

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	lambdapkg "github.com/blackbirdworks/gopherstack/lambda"
	pkglogger "github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// makeZipPayload builds an in-memory zip archive with a single handler file.
// Returns the base64-encoded payload for use in FunctionCode.ZipFile.
func makeZipPayload(t *testing.T, filename, content string) string {
	t.Helper()

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	f, err := zw.Create(filename)
	require.NoError(t, err)

	_, err = f.Write([]byte(content))
	require.NoError(t, err)

	require.NoError(t, zw.Close())

	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

// TestIntegration_Lambda_Zip_CRUD exercises CreateFunction / GetFunction / ListFunctions /
// UpdateFunctionCode / UpdateFunctionConfiguration / DeleteFunction with PackageType=Zip.
//
// No Docker daemon is required: this test uses an in-process Gopherstack Lambda handler
// backed by nil Docker and nil portalloc (invocations are not tested here — only the
// management-plane CRUD lifecycle).
func TestIntegration_Lambda_Zip_CRUD(t *testing.T) {
	t.Parallel()

	// Wire up an in-process server with the Lambda handler only.
	backend := lambdapkg.NewInMemoryBackend(
		nil, nil, lambdapkg.DefaultSettings(), "000000000000", "us-east-1",
		slog.Default(),
	)
	handler := lambdapkg.NewHandler(backend, slog.Default())
	handler.AccountID = "000000000000"
	handler.DefaultRegion = "us-east-1"

	e := echo.New()
	e.Pre(pkglogger.EchoMiddleware(slog.Default()))
	registry := service.NewRegistry(slog.Default())
	require.NoError(t, registry.Register(handler))
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	server := httptest.NewServer(e)
	t.Cleanup(server.Close)

	ctx := t.Context()
	zipB64 := makeZipPayload(t, "index.py", `def handler(event, context): return "hello"`)

	// --- Step 1: CreateFunction with PackageType=Zip ---
	createBody, err := json.Marshal(map[string]any{
		"FunctionName": "zip-crud-fn",
		"PackageType":  "Zip",
		"Runtime":      "python3.12",
		"Handler":      "index.handler",
		"Code":         map[string]string{"ZipFile": zipB64},
		"Role":         "arn:aws:iam::000000000000:role/test",
	})
	require.NoError(t, err)

	createResp, err := doLambdaRequest(ctx, http.MethodPost,
		server.URL+"/2015-03-31/functions", "application/json", bytes.NewReader(createBody))
	require.NoError(t, err)
	defer createResp.Body.Close()
	assert.Equal(t, http.StatusCreated, createResp.StatusCode, "CreateFunction should return 201")

	var created lambdapkg.FunctionConfiguration
	body, err := io.ReadAll(createResp.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(body, &created))
	assert.Equal(t, "zip-crud-fn", created.FunctionName)
	assert.Equal(t, lambdapkg.PackageTypeZip, created.PackageType)
	assert.Equal(t, "python3.12", created.Runtime)
	assert.Equal(t, "index.handler", created.Handler)

	// --- Step 2: GetFunction ---
	getResp, err := doLambdaRequest(ctx, http.MethodGet,
		server.URL+"/2015-03-31/functions/zip-crud-fn", "", nil)
	require.NoError(t, err)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusOK, getResp.StatusCode, "GetFunction should return 200")

	getBody, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	var getOut lambdapkg.GetFunctionOutput
	require.NoError(t, json.Unmarshal(getBody, &getOut))
	assert.Equal(t, lambdapkg.PackageTypeZip, getOut.Configuration.PackageType)
	require.NotNil(t, getOut.Code)
	assert.Equal(t, "S3", getOut.Code.RepositoryType) // Zip locations use S3 repository type

	// --- Step 3: ListFunctions should include the function ---
	listResp, err := doLambdaRequest(ctx, http.MethodGet,
		server.URL+"/2015-03-31/functions", "", nil)
	require.NoError(t, err)
	defer listResp.Body.Close()
	assert.Equal(t, http.StatusOK, listResp.StatusCode)

	var listOut struct {
		Functions []lambdapkg.FunctionConfiguration `json:"Functions"`
	}
	listBody, err := io.ReadAll(listResp.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(listBody, &listOut))
	require.Len(t, listOut.Functions, 1)
	assert.Equal(t, "zip-crud-fn", listOut.Functions[0].FunctionName)

	// --- Step 4: UpdateFunctionCode with new zip ---
	newZipB64 := makeZipPayload(t, "index.py", `def handler(event, context): return "v2"`)
	updateCodeBody, err := json.Marshal(map[string]any{
		"ZipFile": newZipB64,
	})
	require.NoError(t, err)
	updateCodeResp, err := doLambdaRequest(ctx, http.MethodPut,
		server.URL+"/2015-03-31/functions/zip-crud-fn/code", "application/json",
		bytes.NewReader(updateCodeBody))
	require.NoError(t, err)
	defer updateCodeResp.Body.Close()
	assert.Equal(t, http.StatusOK, updateCodeResp.StatusCode, "UpdateFunctionCode should return 200")

	// --- Step 5: UpdateFunctionConfiguration (Runtime + Handler) ---
	updateCfgBody, err := json.Marshal(map[string]any{
		"Runtime":     "python3.13",
		"Handler":     "main.handler",
		"Description": "updated",
	})
	require.NoError(t, err)
	updateCfgResp, err := doLambdaRequest(ctx, http.MethodPut,
		server.URL+"/2015-03-31/functions/zip-crud-fn/configuration", "application/json",
		bytes.NewReader(updateCfgBody))
	require.NoError(t, err)
	defer updateCfgResp.Body.Close()
	assert.Equal(t, http.StatusOK, updateCfgResp.StatusCode, "UpdateFunctionConfiguration should return 200")

	var updatedCfg lambdapkg.FunctionConfiguration
	cfgBody, err := io.ReadAll(updateCfgResp.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(cfgBody, &updatedCfg))
	assert.Equal(t, "python3.13", updatedCfg.Runtime)
	assert.Equal(t, "main.handler", updatedCfg.Handler)

	// --- Step 6: DeleteFunction ---
	delResp, err := doLambdaRequest(ctx, http.MethodDelete,
		server.URL+"/2015-03-31/functions/zip-crud-fn", "", nil)
	require.NoError(t, err)
	defer delResp.Body.Close()
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode, "DeleteFunction should return 204")

	// --- Step 7: GetFunction should return 404 after delete ---
	getAfterDel, err := doLambdaRequest(ctx, http.MethodGet,
		server.URL+"/2015-03-31/functions/zip-crud-fn", "", nil)
	require.NoError(t, err)
	defer getAfterDel.Body.Close()
	assert.Equal(t, http.StatusNotFound, getAfterDel.StatusCode, "GetFunction should return 404 after delete")
}

// TestIntegration_Lambda_Zip_ValidationErrors verifies that the handler rejects
// invalid requests for PackageType=Zip functions.
func TestIntegration_Lambda_Zip_ValidationErrors(t *testing.T) {
	t.Parallel()

	backend := lambdapkg.NewInMemoryBackend(
		nil, nil, lambdapkg.DefaultSettings(), "000000000000", "us-east-1",
		slog.Default(),
	)
	handler := lambdapkg.NewHandler(backend, slog.Default())
	handler.AccountID = "000000000000"

	e := echo.New()
	registry := service.NewRegistry(slog.Default())
	require.NoError(t, registry.Register(handler))
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	server := httptest.NewServer(e)
	t.Cleanup(server.Close)

	ctx := t.Context()

	tests := []struct {
		name       string
		body       map[string]any
		wantStatus int
	}{
		{
			name: "Zip without Runtime is rejected",
			body: map[string]any{
				"FunctionName": "no-runtime",
				"PackageType":  "Zip",
				"Code":         map[string]string{"ZipFile": makeZipPayload(t, "f.py", "x")},
				"Role":         "arn:aws:iam::123:role/r",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "Zip without code is rejected",
			body: map[string]any{
				"FunctionName": "no-code",
				"PackageType":  "Zip",
				"Runtime":      "python3.12",
				"Code":         map[string]string{},
				"Role":         "arn:aws:iam::123:role/r",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "Image with ZipFile is rejected",
			body: map[string]any{
				"FunctionName": "image-with-zip",
				"PackageType":  "Image",
				"Code":         map[string]string{"ZipFile": makeZipPayload(t, "f.py", "x")},
				"Role":         "arn:aws:iam::123:role/r",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodyJSON, err := json.Marshal(tt.body)
			require.NoError(t, err)

			resp, err := doLambdaRequest(ctx, http.MethodPost,
				server.URL+"/2015-03-31/functions", "application/json",
				bytes.NewReader(bodyJSON))
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantStatus, resp.StatusCode, "test: %s", tt.name)
		})
	}
}

// TestIntegration_Lambda_Zip_S3Code tests that a function can be created with S3 as code source.
func TestIntegration_Lambda_Zip_S3Code(t *testing.T) {
	t.Parallel()

	backend := lambdapkg.NewInMemoryBackend(
		nil, nil, lambdapkg.DefaultSettings(), "000000000000", "us-east-1",
		slog.Default(),
	)
	handler := lambdapkg.NewHandler(backend, slog.Default())
	handler.AccountID = "000000000000"

	e := echo.New()
	registry := service.NewRegistry(slog.Default())
	require.NoError(t, registry.Register(handler))
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	server := httptest.NewServer(e)
	t.Cleanup(server.Close)

	ctx := t.Context()

	// Create with S3 code reference.
	createBody, err := json.Marshal(map[string]any{
		"FunctionName": "s3-zip-fn",
		"PackageType":  "Zip",
		"Runtime":      "nodejs20.x",
		"Handler":      "index.handler",
		"Code": map[string]string{
			"S3Bucket": "my-bucket",
			"S3Key":    "functions/my-fn.zip",
		},
		"Role": "arn:aws:iam::000000000000:role/test",
	})
	require.NoError(t, err)

	createResp, err := doLambdaRequest(ctx, http.MethodPost,
		server.URL+"/2015-03-31/functions", "application/json", bytes.NewReader(createBody))
	require.NoError(t, err)
	defer createResp.Body.Close()
	assert.Equal(t, http.StatusCreated, createResp.StatusCode)

	// GetFunction should show the S3 code location.
	getResp, err := doLambdaRequest(ctx, http.MethodGet,
		server.URL+"/2015-03-31/functions/s3-zip-fn", "", nil)
	require.NoError(t, err)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusOK, getResp.StatusCode)

	getBody, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	var getOut lambdapkg.GetFunctionOutput
	require.NoError(t, json.Unmarshal(getBody, &getOut))
	assert.Equal(t, "nodejs20.x", getOut.Configuration.Runtime)
	require.NotNil(t, getOut.Code)
	assert.Equal(t, "S3", getOut.Code.RepositoryType)
}
