package lambda_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// doFnRequest drives a request through the full lambda handler and returns the recorder.
func doFnRequest(t *testing.T, h *lambda.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

// seedVersionedFunction creates a Zip function, publishes a version, and returns the backend.
func seedVersionedFunction(t *testing.T, name string) (*lambda.Handler, *lambda.InMemoryBackend) {
	t.Helper()

	backend := lambda.NewInMemoryBackend(
		nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1",
	)
	closeBackend(t, backend)
	handler := lambda.NewHandler(backend)

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: name,
		FunctionArn:  "arn:aws:lambda:us-east-1:000000000000:function:" + name,
		Runtime:      "python3.12",
		Handler:      "index.handler",
		PackageType:  lambda.PackageTypeZip,
		MemorySize:   128,
		Timeout:      3,
		State:        lambda.FunctionStateActive,
		Version:      "$LATEST",
	}))

	return handler, backend
}

// TestLambda_GetFunction_ByQualifier verifies GetFunction honours the Qualifier
// query parameter for version numbers, alias names, and $LATEST, matching real
// AWS behaviour where the returned ARN and Version reflect the qualifier.
func TestLambda_GetFunction_ByQualifier(t *testing.T) {
	t.Parallel()

	h, bk := seedVersionedFunction(t, "qfn")

	v1, err := bk.PublishVersion("qfn", "first")
	require.NoError(t, err)
	require.Equal(t, "1", v1.Version)

	_, err = bk.CreateAlias("qfn", &lambda.CreateAliasInput{
		Name:            "live",
		FunctionVersion: "1",
	})
	require.NoError(t, err)

	tests := []struct {
		name        string
		qualifier   string
		wantVersion string
		wantArnSfx  string
		wantStatus  int
	}{
		{
			name:        "latest_implicit",
			qualifier:   "",
			wantStatus:  http.StatusOK,
			wantVersion: "$LATEST",
			wantArnSfx:  ":function:qfn",
		},
		{
			name:        "latest_explicit",
			qualifier:   "$LATEST",
			wantStatus:  http.StatusOK,
			wantVersion: "$LATEST",
			wantArnSfx:  ":function:qfn",
		},
		{
			name:        "version_number",
			qualifier:   "1",
			wantStatus:  http.StatusOK,
			wantVersion: "1",
			wantArnSfx:  ":function:qfn:1",
		},
		{
			name:        "alias",
			qualifier:   "live",
			wantStatus:  http.StatusOK,
			wantVersion: "1",
			wantArnSfx:  ":function:qfn:live",
		},
		{
			name:       "unknown_version",
			qualifier:  "99",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid_qualifier",
			qualifier:  "bad qualifier!",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := "/2015-03-31/functions/qfn"
			if tc.qualifier != "" {
				path += "?Qualifier=" + url.QueryEscape(tc.qualifier)
			}

			rec := doFnRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())

			if tc.wantStatus != http.StatusOK {
				return
			}

			var out struct {
				Configuration struct {
					FunctionArn string `json:"FunctionArn"`
					Version     string `json:"Version"`
				} `json:"Configuration"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			require.Equal(t, tc.wantVersion, out.Configuration.Version)
			require.Contains(t, out.Configuration.FunctionArn, tc.wantArnSfx)
		})
	}
}

// TestLambda_GetFunctionConfiguration_ByQualifier verifies the configuration
// read path also resolves a version qualifier.
func TestLambda_GetFunctionConfiguration_ByQualifier(t *testing.T) {
	t.Parallel()

	h, bk := seedVersionedFunction(t, "cfgfn")
	_, err := bk.PublishVersion("cfgfn", "")
	require.NoError(t, err)

	rec := doFnRequest(t, h, http.MethodGet,
		"/2015-03-31/functions/cfgfn/configuration?Qualifier=1", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var cfg struct {
		FunctionArn string `json:"FunctionArn"`
		Version     string `json:"Version"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cfg))
	require.Equal(t, "1", cfg.Version)
	require.Contains(t, cfg.FunctionArn, ":function:cfgfn:1")
}

// TestLambda_CreateFunction_RuntimeValidation verifies CreateFunction rejects
// runtimes outside the AWS enum and accepts current + deprecated runtimes,
// matching real AWS which enforces an enum constraint on the 'runtime' member.
func TestLambda_CreateFunction_RuntimeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		runtime    string
		wantStatus int
	}{
		{name: "current_python", runtime: "python3.12", wantStatus: http.StatusCreated},
		{name: "current_node", runtime: "nodejs20.x", wantStatus: http.StatusCreated},
		{name: "current_provided", runtime: "provided.al2023", wantStatus: http.StatusCreated},
		{name: "deprecated_python38", runtime: "python3.8", wantStatus: http.StatusCreated},
		{name: "deprecated_go", runtime: "go1.x", wantStatus: http.StatusCreated},
		{name: "unknown_runtime", runtime: "nodejs99.x", wantStatus: http.StatusBadRequest},
		{name: "garbage_runtime", runtime: "totally-made-up", wantStatus: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := lambda.NewInMemoryBackend(
				nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1",
			)
			closeBackend(t, backend)
			h := lambda.NewHandler(backend)

			body := map[string]any{
				"FunctionName": "rt-" + tc.name,
				"PackageType":  "Zip",
				"Runtime":      tc.runtime,
				"Handler":      "index.handler",
				"Role":         "arn:aws:iam::000000000000:role/r",
				"Code":         map[string]any{"ZipFile": []byte("dummy")},
			}

			rec := doFnRequest(t, h, http.MethodPost, "/2015-03-31/functions", body)
			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
		})
	}
}
