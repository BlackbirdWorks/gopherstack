package lambda_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

func TestPublishVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *lambda.Handler)
		name        string
		funcName    string
		body        string
		wantVersion string
		wantDesc    string
		wantCode    int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "ver-fn")
			},
			funcName:    "ver-fn",
			body:        `{"Description":"v1"}`,
			wantCode:    http.StatusCreated,
			wantVersion: "1",
			wantDesc:    "v1",
		},
		{
			name:     "function_not_found",
			funcName: "no-fn",
			body:     `{}`,
			wantCode: http.StatusNotFound,
		},
		{
			name:     "mock_backend_service_error",
			funcName: "fn",
			body:     `{}`,
			wantCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h *lambda.Handler
			if tt.name == "mock_backend_service_error" {
				h, _ = newHandler(t)
			} else {
				h = newInMemHandlerWithPortAlloc(t)
			}

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions/"+tt.funcName+"/versions", tt.body, nil)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantVersion != "" {
				var ver lambda.FunctionVersion
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ver))
				assert.Equal(t, tt.wantVersion, ver.Version)
				assert.Equal(t, tt.wantDesc, ver.Description)
				assert.Equal(t, tt.funcName, ver.FunctionName)
			}
		})
	}
}

func TestListVersionsByFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *lambda.Handler)
		name         string
		funcName     string
		wantVersions []string
		wantCode     int
		wantCount    int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "list-ver-fn")
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/list-ver-fn/versions", `{}`, nil)
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/list-ver-fn/versions", `{}`, nil)
			},
			funcName:     "list-ver-fn",
			wantCode:     http.StatusOK,
			wantCount:    3,
			wantVersions: []string{"$LATEST", "1", "2"},
		},
		{
			name:     "function_not_found",
			funcName: "nofn",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "mock_backend_service_error",
			funcName: "fn",
			wantCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h *lambda.Handler
			if tt.name == "mock_backend_service_error" {
				h, _ = newHandler(t)
			} else {
				h = newInMemHandlerWithPortAlloc(t)
			}

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := callHandler(t, h, http.MethodGet, "/2015-03-31/functions/"+tt.funcName+"/versions", "", nil)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCount > 0 {
				var out lambda.ListVersionsByFunctionOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.Versions, tt.wantCount)
				for i, v := range tt.wantVersions {
					assert.Equal(t, v, out.Versions[i].Version)
				}
			}
		})
	}
}

func TestCreateAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *lambda.Handler)
		name        string
		funcName    string
		body        string
		wantName    string
		wantVersion string
		wantCode    int
		useMock     bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "alias-fn")
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-fn/versions", `{}`, nil)
			},
			funcName:    "alias-fn",
			body:        `{"Name":"live","FunctionVersion":"1"}`,
			wantCode:    http.StatusCreated,
			wantName:    "live",
			wantVersion: "1",
		},
		{
			name: "missing_name",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "alias-missing-name-fn")
			},
			funcName: "alias-missing-name-fn",
			body:     `{"FunctionVersion":"1"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_version",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "alias-missing-ver-fn")
			},
			funcName: "alias-missing-ver-fn",
			body:     `{"Name":"v1"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "dup-alias-fn")
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/dup-alias-fn/versions", `{}`, nil)
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/dup-alias-fn/aliases",
					`{"Name":"dup","FunctionVersion":"1"}`, nil)
			},
			funcName: "dup-alias-fn",
			body:     `{"Name":"dup","FunctionVersion":"1"}`,
			wantCode: http.StatusConflict,
		},
		{
			name:     "function_not_found",
			funcName: "nofn",
			body:     `{"Name":"v1","FunctionVersion":"1"}`,
			wantCode: http.StatusNotFound,
		},
		{
			name:     "mock_backend_service_error",
			funcName: "fn",
			body:     `{"Name":"v1","FunctionVersion":"1"}`,
			wantCode: http.StatusInternalServerError,
			useMock:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h *lambda.Handler
			if tt.useMock {
				h, _ = newHandler(t)
			} else {
				h = newInMemHandlerWithPortAlloc(t)
			}

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions/"+tt.funcName+"/aliases", tt.body, nil)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantName != "" {
				var alias lambda.FunctionAlias
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &alias))
				assert.Equal(t, tt.wantName, alias.Name)
				assert.Equal(t, tt.wantVersion, alias.FunctionVersion)
			}
		})
	}
}

func TestGetAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*testing.T, *lambda.Handler)
		name      string
		funcName  string
		aliasName string
		wantName  string
		wantCode  int
		useMock   bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "getalias-fn")
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/getalias-fn/versions", `{}`, nil)
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/getalias-fn/aliases",
					`{"Name":"stable","FunctionVersion":"1"}`, nil)
			},
			funcName:  "getalias-fn",
			aliasName: "stable",
			wantCode:  http.StatusOK,
			wantName:  "stable",
		},
		{
			name: "not_found",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "noalias-fn")
			},
			funcName:  "noalias-fn",
			aliasName: "missing",
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "mock_backend_service_error",
			funcName:  "fn",
			aliasName: "v1",
			wantCode:  http.StatusInternalServerError,
			useMock:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h *lambda.Handler
			if tt.useMock {
				h, _ = newHandler(t)
			} else {
				h = newInMemHandlerWithPortAlloc(t)
			}

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := callHandler(t, h, http.MethodGet,
				"/2015-03-31/functions/"+tt.funcName+"/aliases/"+tt.aliasName, "", nil)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantName != "" {
				var alias lambda.FunctionAlias
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &alias))
				assert.Equal(t, tt.wantName, alias.Name)
			}
		})
	}
}

func TestListAliases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(*testing.T, *lambda.Handler)
		funcName  string
		wantCode  int
		wantCount int
		useMock   bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "listalias-fn")
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/listalias-fn/versions", `{}`, nil)
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/listalias-fn/aliases",
					`{"Name":"v1","FunctionVersion":"1"}`, nil)
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/listalias-fn/aliases",
					`{"Name":"v2","FunctionVersion":"1"}`, nil)
			},
			funcName:  "listalias-fn",
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
		{
			name:     "function_not_found",
			funcName: "nofn",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "mock_backend_service_error",
			funcName: "fn",
			wantCode: http.StatusInternalServerError,
			useMock:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h *lambda.Handler
			if tt.useMock {
				h, _ = newHandler(t)
			} else {
				h = newInMemHandlerWithPortAlloc(t)
			}

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := callHandler(t, h, http.MethodGet, "/2015-03-31/functions/"+tt.funcName+"/aliases", "", nil)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCount > 0 {
				var out lambda.ListAliasesOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.Aliases, tt.wantCount)
			}
		})
	}
}

func TestUpdateAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *lambda.Handler)
		name        string
		funcName    string
		aliasName   string
		body        string
		wantVersion string
		wantCode    int
		useMock     bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "updalias-fn")
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/updalias-fn/versions", `{}`, nil)
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/updalias-fn/versions", `{}`, nil)
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/updalias-fn/aliases",
					`{"Name":"prod","FunctionVersion":"1"}`, nil)
			},
			funcName:    "updalias-fn",
			aliasName:   "prod",
			body:        `{"FunctionVersion":"2"}`,
			wantCode:    http.StatusOK,
			wantVersion: "2",
		},
		{
			name: "not_found",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "updnotfound-fn")
			},
			funcName:  "updnotfound-fn",
			aliasName: "missing",
			body:      `{"FunctionVersion":"1"}`,
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "mock_backend_service_error",
			funcName:  "fn",
			aliasName: "v1",
			body:      `{"FunctionVersion":"2"}`,
			wantCode:  http.StatusInternalServerError,
			useMock:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h *lambda.Handler
			if tt.useMock {
				h, _ = newHandler(t)
			} else {
				h = newInMemHandlerWithPortAlloc(t)
			}

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := callHandler(t, h, http.MethodPut,
				"/2015-03-31/functions/"+tt.funcName+"/aliases/"+tt.aliasName, tt.body, nil)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantVersion != "" {
				var alias lambda.FunctionAlias
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &alias))
				assert.Equal(t, tt.wantVersion, alias.FunctionVersion)
			}
		})
	}
}

func TestDeleteAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(*testing.T, *lambda.Handler)
		funcName  string
		aliasName string
		wantCode  int
		verify    bool
		useMock   bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "delalias-fn")
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/delalias-fn/versions", `{}`, nil)
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/delalias-fn/aliases",
					`{"Name":"old","FunctionVersion":"1"}`, nil)
			},
			funcName:  "delalias-fn",
			aliasName: "old",
			wantCode:  http.StatusNoContent,
			verify:    true,
		},
		{
			name: "not_found",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "delnotfound-fn")
			},
			funcName:  "delnotfound-fn",
			aliasName: "missing",
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "mock_backend_service_error",
			funcName:  "fn",
			aliasName: "v1",
			wantCode:  http.StatusInternalServerError,
			useMock:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h *lambda.Handler
			if tt.useMock {
				h, _ = newHandler(t)
			} else {
				h = newInMemHandlerWithPortAlloc(t)
			}

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := callHandler(t, h, http.MethodDelete,
				"/2015-03-31/functions/"+tt.funcName+"/aliases/"+tt.aliasName, "", nil)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.verify {
				verifyRec := callHandler(t, h, http.MethodGet,
					"/2015-03-31/functions/"+tt.funcName+"/aliases/"+tt.aliasName, "", nil)
				require.Equal(t, http.StatusNotFound, verifyRec.Code)
			}
		})
	}
}

func TestInvokeWithQualifier_Alias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		funcName  string
		qualifier string
		wantCode  int
	}{
		{
			name:      "qualifier_accepted",
			funcName:  "qual-fn",
			qualifier: "live",
			wantCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newHandler(t)
			fn := &lambda.FunctionConfiguration{
				FunctionName: tt.funcName,
				PackageType:  lambda.PackageTypeImage,
			}
			require.NoError(t, bk.CreateFunction(fn))
			bk.invokeResult = []byte(`{"result":"alias-ok"}`)

			rec := callHandler(t, h, http.MethodPost,
				"/2015-03-31/functions/"+tt.funcName+"/invocations?Qualifier="+tt.qualifier,
				`{"event":"test"}`, nil)
			require.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
