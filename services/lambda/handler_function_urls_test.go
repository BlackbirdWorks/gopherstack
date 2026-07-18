package lambda_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

func newInMemHandlerWithPortAlloc(t *testing.T) *lambda.Handler {
	t.Helper()

	pa, err := portalloc.New(20000, 20050)
	require.NoError(t, err)

	bk := lambda.NewInMemoryBackend(nil, pa, lambda.DefaultSettings(), "000000000000", "us-east-1")
	closeBackend(t, bk)
	h := lambda.NewHandler(bk)
	h.DefaultRegion = "us-east-1"
	h.AccountID = "000000000000"

	return h
}

func mustCreateFunctionViaHandler(t *testing.T, h *lambda.Handler, name string) {
	t.Helper()

	const roleARN = "arn:aws:iam::000000000000:role/r"
	body := fmt.Sprintf(
		`{"FunctionName":%q,"PackageType":"Image","Code":{"ImageUri":"test:latest"},"Role":%q}`,
		name, roleARN,
	)
	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", body, nil)
	require.Equal(t, http.StatusCreated, rec.Code)
}

func TestFunctionUrl_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		funcName string
	}{
		{name: "create_get_delete", funcName: "url-fn"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newInMemHandlerWithPortAlloc(t)
			mustCreateFunctionViaHandler(t, h, tt.funcName)

			rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions/"+tt.funcName+"/url",
				`{"AuthType":"NONE"}`, nil)
			require.Equal(t, http.StatusCreated, rec.Code)

			var cfg lambda.FunctionURLConfig
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cfg))
			assert.NotEmpty(t, cfg.FunctionURL)
			assert.Equal(t, "NONE", cfg.AuthType)
			assert.NotEmpty(t, cfg.FunctionArn)
			assert.NotEmpty(t, cfg.CreationTime)

			rec = callHandler(t, h, http.MethodGet, "/2015-03-31/functions/"+tt.funcName+"/url", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var getCfg lambda.FunctionURLConfig
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getCfg))
			assert.Equal(t, cfg.FunctionURL, getCfg.FunctionURL)

			rec = callHandler(t, h, http.MethodDelete, "/2015-03-31/functions/"+tt.funcName+"/url", "", nil)
			require.Equal(t, http.StatusNoContent, rec.Code)

			rec = callHandler(t, h, http.MethodGet, "/2015-03-31/functions/"+tt.funcName+"/url", "", nil)
			require.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestFunctionURL_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(*testing.T, *lambda.Handler)
		method   string
		funcName string
		wantCode int
	}{
		{
			name:     "create_function_not_found",
			method:   http.MethodPost,
			funcName: "nonexistent",
			wantCode: http.StatusNotFound,
		},
		{
			name: "get_url_not_found",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "no-url-fn")
			},
			method:   http.MethodGet,
			funcName: "no-url-fn",
			wantCode: http.StatusNotFound,
		},
		{
			name: "delete_url_not_found",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "del-url-fn")
			},
			method:   http.MethodDelete,
			funcName: "del-url-fn",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newInMemHandlerWithPortAlloc(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			body := ""
			if tt.method == http.MethodPost {
				body = `{"AuthType":"NONE"}`
			}

			rec := callHandler(t, h, tt.method, "/2015-03-31/functions/"+tt.funcName+"/url", body, nil)
			require.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestFunctionUrl_HTTP_ForwardsToLambda(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		funcName  string
		portRange [2]int
	}{
		{
			name:      "creates_url_with_http",
			portRange: [2]int{20100, 20200},
			funcName:  "http-url-fn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pa, err := portalloc.New(tt.portRange[0], tt.portRange[1])
			require.NoError(t, err)

			bk := lambda.NewInMemoryBackend(
				nil,
				pa,
				lambda.DefaultSettings(),
				"000000000000",
				"us-east-1",
			)
			closeBackend(t, bk)

			fn := &lambda.FunctionConfiguration{
				FunctionName: tt.funcName,
				PackageType:  lambda.PackageTypeImage,
				ImageURI:     "test:latest",
			}
			require.NoError(t, bk.CreateFunction(fn))

			cfg, createErr := bk.CreateFunctionURLConfig(t.Context(), tt.funcName, "NONE", nil, "")
			require.NoError(t, createErr)
			assert.NotEmpty(t, cfg.FunctionURL)
			assert.Contains(t, cfg.FunctionURL, "http://")
		})
	}
}
