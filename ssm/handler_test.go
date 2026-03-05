package ssm_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/ssm"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func newTestHandler(t *testing.T) (*ssm.Handler, *ssm.InMemoryBackend) {
	t.Helper()


	backend := ssm.NewInMemoryBackend()

	return ssm.NewHandler(backend), backend
}

func doRequest(
	t *testing.T,
	h *ssm.Handler,
	action string,
	body string,
) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()

	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	} else {
		req = httptest.NewRequest(http.MethodPost, "/", nil)
	}

	if action != "" {
		req.Header.Set("X-Amz-Target", "AmazonSSM."+action)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// --- InMemoryBackend tests ---

func TestInMemoryBackend_PutAndGet(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	putIn := &ssm.PutParameterInput{
		Name:        "db-password",
		Type:        "SecureString",
		Value:       "supersecret",
		Description: "The DB password",
	}
	putOut, err := backend.PutParameter(putIn)
	require.NoError(t, err)
	assert.Equal(t, int64(1), putOut.Version)

	getOut, err := backend.GetParameter(&ssm.GetParameterInput{
		Name:           "db-password",
		WithDecryption: true,
	})
	require.NoError(t, err)
	assert.Equal(t, "supersecret", getOut.Parameter.Value)
	assert.Equal(t, int64(1), getOut.Parameter.Version)
}

func TestInMemoryBackend_DuplicateKeyError(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, _ = backend.PutParameter(&ssm.PutParameterInput{
		Name:  "db-password",
		Type:  "SecureString",
		Value: "supersecret",
	})

	input2 := &ssm.PutParameterInput{
		Name: "db-password", Type: "String", Value: "{}", Overwrite: false,
	}
	_, duplicateErr := backend.PutParameter(input2)
	require.ErrorIs(t, duplicateErr, ssm.ErrParameterAlreadyExists)
}

func TestInMemoryBackend_Overwrite(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, _ = backend.PutParameter(&ssm.PutParameterInput{
		Name:  "db-password",
		Type:  "SecureString",
		Value: "supersecret",
	})

	putInOverwrite := &ssm.PutParameterInput{
		Name:      "db-password",
		Type:      "String",
		Value:     "newsecret",
		Overwrite: true,
	}
	putOut, err := backend.PutParameter(putInOverwrite)
	require.NoError(t, err)
	assert.Equal(t, int64(2), putOut.Version)

	getOut, err := backend.GetParameter(&ssm.GetParameterInput{Name: "db-password"})
	require.NoError(t, err)
	assert.Equal(t, "newsecret", getOut.Parameter.Value)
	assert.Equal(t, int64(2), getOut.Parameter.Version)
}

func TestInMemoryBackend_GetParameters(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, _ = backend.PutParameter(
		&ssm.PutParameterInput{Name: "db-password", Type: "String", Value: "pwd"},
	)
	_, _ = backend.PutParameter(
		&ssm.PutParameterInput{Name: "api-key", Type: "String", Value: "123"},
	)

	getParamsOut, err := backend.GetParameters(&ssm.GetParametersInput{
		Names: []string{"db-password", "api-key", "missing-key"},
	})
	require.NoError(t, err)
	assert.Len(t, getParamsOut.Parameters, 2)
	assert.Len(t, getParamsOut.InvalidParameters, 1)
	assert.Equal(t, "missing-key", getParamsOut.InvalidParameters[0])
}

func TestInMemoryBackend_ListAll(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, _ = backend.PutParameter(
		&ssm.PutParameterInput{Name: "api-key", Type: "String", Value: "123"},
	)
	_, _ = backend.PutParameter(
		&ssm.PutParameterInput{Name: "db-password", Type: "String", Value: "pwd"},
	)

	all := backend.ListAll()
	assert.Len(t, all, 2)
	assert.Equal(t, "api-key", all[0].Name)
	assert.Equal(t, "db-password", all[1].Name)
}

func TestInMemoryBackend_DeleteAll(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, _ = backend.PutParameter(
		&ssm.PutParameterInput{Name: "api-key", Type: "String", Value: "123"},
	)
	_, _ = backend.PutParameter(
		&ssm.PutParameterInput{Name: "db-password", Type: "String", Value: "pwd"},
	)

	backend.DeleteParameter(&ssm.DeleteParameterInput{Name: "api-key"})
	backend.DeleteParameter(&ssm.DeleteParameterInput{Name: "db-password"})
	assert.Empty(t, backend.ListAll())
}

func TestInMemoryBackend_DeleteParameters(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, _ = backend.PutParameter(
		&ssm.PutParameterInput{Name: "key1", Type: "String", Value: "v1"},
	)

	delOut, err := backend.DeleteParameters(
		&ssm.DeleteParametersInput{
			Names: []string{"db-password", "key1", "missing"},
		},
	)
	require.NoError(t, err)
	assert.Len(t, delOut.DeletedParameters, 1)
	assert.Len(t, delOut.InvalidParameters, 2)
	assert.Empty(t, backend.ListAll())
}

// --- Handler routing tests ---

func TestHandler_Routing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(b *ssm.InMemoryBackend)
		name            string
		method          string
		target          string
		body            string
		wantBodyContain string
		wantStatus      int
	}{
		{
			name:   "GetParameter",
			method: http.MethodPost,
			target: "AmazonSSM.GetParameter",
			body:   `{"Name":"test-param"}`,
			setup: func(b *ssm.InMemoryBackend) {
				b.PutParameter(
					&ssm.PutParameterInput{Name: "test-param", Type: "String", Value: "test-value"},
				)
			},
			wantStatus:      http.StatusOK,
			wantBodyContain: "test-value",
		},
		{
			name:            "UnknownAction",
			method:          http.MethodPost,
			target:          "AmazonSSM.FakeAction",
			body:            `{}`,
			wantStatus:      http.StatusBadRequest,
			wantBodyContain: "UnknownOperationException",
		},
		{
			name:            "MissingTarget",
			method:          http.MethodPost,
			target:          "",
			body:            `{}`,
			wantStatus:      http.StatusBadRequest,
			wantBodyContain: "Missing X-Amz-Target",
		},
		{
			name:            "GetSupportedOperations",
			method:          http.MethodGet,
			target:          "",
			body:            ``,
			wantStatus:      http.StatusOK,
			wantBodyContain: "GetParameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()

			backend := ssm.NewInMemoryBackend()
			handler := ssm.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(backend)
			}

			var req *http.Request
			if tt.body != "" {
				req = httptest.NewRequest(tt.method, "/", strings.NewReader(tt.body))
			} else {
				req = httptest.NewRequest(tt.method, "/", nil)
			}

			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := handler.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBodyContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContain)
			}
		})
	}
}

// --- Parameter History tests ---

func TestParameterHistory_InitialVersion(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	putIn := &ssm.PutParameterInput{
		Name:        "api-key",
		Type:        "String",
		Value:       "key-v1",
		Description: "API key",
	}
	putOut, err := backend.PutParameter(putIn)
	require.NoError(t, err)
	assert.Equal(t, int64(1), putOut.Version)

	historyOut, err := backend.GetParameterHistory(&ssm.GetParameterHistoryInput{
		Name: "api-key",
	})
	require.NoError(t, err)
	require.Len(t, historyOut.Parameters, 1)
	assert.Equal(t, int64(1), historyOut.Parameters[0].Version)
	assert.Equal(t, "key-v1", historyOut.Parameters[0].Value)
}

func TestParameterHistory_MultipleVersions(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	_, _ = backend.PutParameter(&ssm.PutParameterInput{
		Name: "counter", Type: "String", Value: "1",
	})
	_, _ = backend.PutParameter(&ssm.PutParameterInput{
		Name: "counter", Type: "String", Value: "2", Overwrite: true,
	})
	_, _ = backend.PutParameter(&ssm.PutParameterInput{
		Name: "counter", Type: "String", Value: "3", Overwrite: true,
	})

	historyOut, err := backend.GetParameterHistory(&ssm.GetParameterHistoryInput{
		Name: "counter",
	})
	require.NoError(t, err)
	require.Len(t, historyOut.Parameters, 3)

	assert.Equal(t, int64(3), historyOut.Parameters[0].Version)
	assert.Equal(t, "3", historyOut.Parameters[0].Value)

	assert.Equal(t, int64(2), historyOut.Parameters[1].Version)
	assert.Equal(t, "2", historyOut.Parameters[1].Value)

	assert.Equal(t, int64(1), historyOut.Parameters[2].Version)
	assert.Equal(t, "1", historyOut.Parameters[2].Value)
}

func TestParameterHistory_NotFound(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	_, err := backend.GetParameterHistory(&ssm.GetParameterHistoryInput{
		Name: "nonexistent",
	})
	require.Error(t, err)
	assert.Equal(t, ssm.ErrParameterNotFound, err)
}

func TestParameterHistory_WithMaxResults(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	for i := 1; i <= 5; i++ {
		overwrite := i > 1
		_, _ = backend.PutParameter(&ssm.PutParameterInput{
			Name:      "paginated-param",
			Type:      "String",
			Value:     "value-" + string(rune(i+'0'-1)),
			Overwrite: overwrite,
		})
	}

	maxResults := int64(2)
	historyOut, err := backend.GetParameterHistory(&ssm.GetParameterHistoryInput{
		Name:       "paginated-param",
		MaxResults: &maxResults,
	})
	require.NoError(t, err)
	require.Len(t, historyOut.Parameters, 2)

	assert.Equal(t, int64(5), historyOut.Parameters[0].Version)
	assert.Equal(t, int64(4), historyOut.Parameters[1].Version)
}

func TestParameterHistory_TypeChanges(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	_, _ = backend.PutParameter(&ssm.PutParameterInput{
		Name: "type-change", Type: "String", Value: "string-value",
	})
	_, _ = backend.PutParameter(&ssm.PutParameterInput{
		Name: "type-change", Type: "SecureString", Value: "secure-value", Overwrite: true,
	})

	historyOut, err := backend.GetParameterHistory(&ssm.GetParameterHistoryInput{
		Name: "type-change",
	})
	require.NoError(t, err)
	require.Len(t, historyOut.Parameters, 2)

	assert.Equal(t, "SecureString", historyOut.Parameters[0].Type)
	assert.Equal(t, "String", historyOut.Parameters[1].Type)
}

// --- SecureString tests ---

func TestSecureString_PutEncryption(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name: "db-password", Type: "SecureString", Value: "super-secret-password",
	})
	require.NoError(t, err)

	output, err := backend.GetParameter(&ssm.GetParameterInput{
		Name: "db-password", WithDecryption: false,
	})
	require.NoError(t, err)
	assert.Equal(t, "SecureString", output.Parameter.Type)
	assert.NotEqual(t, "super-secret-password", output.Parameter.Value)
	assert.NotEmpty(t, output.Parameter.Value)
}

func TestSecureString_GetWithDecryption(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name: "db-password", Type: "SecureString", Value: "super-secret-password",
	})
	require.NoError(t, err)

	output, err := backend.GetParameter(&ssm.GetParameterInput{
		Name: "db-password", WithDecryption: true,
	})
	require.NoError(t, err)
	assert.Equal(t, "SecureString", output.Parameter.Type)
	assert.Equal(t, "super-secret-password", output.Parameter.Value)
}

func TestSecureString_GetParametersDecryption(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	_, _ = backend.PutParameter(&ssm.PutParameterInput{
		Name: "api-key", Type: "SecureString", Value: "api-key-value",
	})
	_, _ = backend.PutParameter(&ssm.PutParameterInput{
		Name: "db-password", Type: "SecureString", Value: "db-password-value",
	})
	_, _ = backend.PutParameter(&ssm.PutParameterInput{
		Name: "environment", Type: "String", Value: "production",
	})

	outputNoDecrypt, err := backend.GetParameters(&ssm.GetParametersInput{
		Names:          []string{"api-key", "db-password", "environment"},
		WithDecryption: false,
	})
	require.NoError(t, err)
	require.Len(t, outputNoDecrypt.Parameters, 3)

	for _, param := range outputNoDecrypt.Parameters {
		if param.Type == "SecureString" {
			assert.NotContains(t, param.Value, "-value")
		}
	}

	outputWithDecrypt, err := backend.GetParameters(&ssm.GetParametersInput{
		Names:          []string{"api-key", "db-password", "environment"},
		WithDecryption: true,
	})
	require.NoError(t, err)
	require.Len(t, outputWithDecrypt.Parameters, 3)

	for _, param := range outputWithDecrypt.Parameters {
		switch param.Name {
		case "api-key":
			assert.Equal(t, "api-key-value", param.Value)
		case "db-password":
			assert.Equal(t, "db-password-value", param.Value)
		case "environment":
			assert.Equal(t, "production", param.Value)
		}
	}
}

func TestSecureString_HistoryEncryption(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	_, _ = backend.PutParameter(&ssm.PutParameterInput{
		Name: "secret", Type: "SecureString", Value: "secret-v1",
	})
	_, _ = backend.PutParameter(&ssm.PutParameterInput{
		Name: "secret", Type: "SecureString", Value: "secret-v2", Overwrite: true,
	})

	historyOutput, err := backend.GetParameterHistory(&ssm.GetParameterHistoryInput{
		Name: "secret",
	})
	require.NoError(t, err)
	require.Len(t, historyOutput.Parameters, 2)

	for _, histParam := range historyOutput.Parameters {
		assert.Equal(t, "SecureString", histParam.Type)
		assert.NotContains(t, histParam.Value, "secret-v")
	}
}

// --- GetParametersByPath tests ---

func TestGetParametersByPath_DirectChildrenOnly(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	for _, name := range []string{"/app/db/host", "/app/db/port", "/app/cache/host", "/app/config"} {
		_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
	}

	out, err := backend.GetParametersByPath(&ssm.GetParametersByPathInput{
		Path: "/app", Recursive: false,
	})
	require.NoError(t, err)
	assert.Len(t, out.Parameters, 1)
	assert.Equal(t, "/app/config", out.Parameters[0].Name)
}

func TestGetParametersByPath_Recursive(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	for _, name := range []string{"/app/db/host", "/app/db/port", "/app/cache/host", "/app/config"} {
		_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
	}

	out, err := backend.GetParametersByPath(&ssm.GetParametersByPathInput{
		Path: "/app", Recursive: true,
	})
	require.NoError(t, err)
	assert.Len(t, out.Parameters, 4)
}

func TestGetParametersByPath_Pagination(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	for i := range 5 {
		name := "/params/key" + string(rune('0'+i))
		_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
	}

	maxRes := int64(2)
	out, err := backend.GetParametersByPath(&ssm.GetParametersByPathInput{
		Path: "/params", Recursive: true, MaxResults: &maxRes,
	})
	require.NoError(t, err)
	assert.Len(t, out.Parameters, 2)
	assert.NotEmpty(t, out.NextToken)

	out2, err := backend.GetParametersByPath(&ssm.GetParametersByPathInput{
		Path: "/params", Recursive: true, MaxResults: &maxRes, NextToken: out.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, out2.Parameters, 2)
}

func TestGetParametersByPath_EmptyPath(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	out, err := backend.GetParametersByPath(&ssm.GetParametersByPathInput{
		Path: "/nonexistent", Recursive: true,
	})
	require.NoError(t, err)
	assert.Empty(t, out.Parameters)
}

func TestGetParametersByPath_WithDecryption(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, _ = backend.PutParameter(&ssm.PutParameterInput{
		Name: "/secrets/key", Type: "SecureString", Value: "plaintext",
	})

	out, err := backend.GetParametersByPath(&ssm.GetParametersByPathInput{
		Path: "/secrets", Recursive: true, WithDecryption: true,
	})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 1)
	assert.Equal(t, "plaintext", out.Parameters[0].Value)
}

// --- DescribeParameters tests ---

func TestDescribeParameters_AllParameters(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	for _, p := range []struct{ name, typ string }{
		{"/a", "String"}, {"/b", "SecureString"}, {"/c", "StringList"},
	} {
		_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: p.name, Type: p.typ, Value: "v"})
	}

	out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{})
	require.NoError(t, err)
	assert.Len(t, out.Parameters, 3)
	for _, m := range out.Parameters {
		assert.Empty(t, m.Description)
	}
}

func TestDescribeParameters_FilterByType(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	for _, p := range []struct{ name, typ string }{
		{"/a", "String"}, {"/b", "SecureString"}, {"/c", "String"},
	} {
		_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: p.name, Type: p.typ, Value: "v"})
	}

	out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
		ParameterFilters: []ssm.ParameterFilter{
			{Key: "Type", Option: "Equals", Values: []string{"String"}},
		},
	})
	require.NoError(t, err)
	assert.Len(t, out.Parameters, 2)
}

func TestDescribeParameters_FilterByNameBeginsWith(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	for _, name := range []string{"/app/db", "/app/cache", "/other/key"} {
		_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
	}

	out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
		ParameterFilters: []ssm.ParameterFilter{
			{Key: "Name", Option: "BeginsWith", Values: []string{"/app"}},
		},
	})
	require.NoError(t, err)
	assert.Len(t, out.Parameters, 2)
}

func TestDescribeParameters_Pagination(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	for i := range 5 {
		_, _ = backend.PutParameter(&ssm.PutParameterInput{
			Name: "/p" + string(rune('0'+i)), Type: "String", Value: "v",
		})
	}

	maxRes := int64(2)
	out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{MaxResults: &maxRes})
	require.NoError(t, err)
	assert.Len(t, out.Parameters, 2)
	assert.NotEmpty(t, out.NextToken)

	out2, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
		MaxResults: &maxRes, NextToken: out.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, out2.Parameters, 2)
}

func TestDescribeParameters_BeyondEnd(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
		NextToken: "9999",
	})
	require.NoError(t, err)
	assert.Empty(t, out.Parameters)
}

// --- Handler HTTP operation tests ---

func TestHandler_NewOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(b *ssm.InMemoryBackend)
		name       string
		action     string
		body       string
		wantStatus int
		wantCount  int
	}{
		{
			name:   "GetParametersByPath",
			action: "GetParametersByPath",
			body:   `{"Path":"/app","Recursive":true}`,
			setup: func(b *ssm.InMemoryBackend) {
				for _, name := range []string{"/app/db", "/app/cache", "/other/key"} {
					b.PutParameter(&ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
				}
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:   "DescribeParameters",
			action: "DescribeParameters",
			body:   `{}`,
			setup: func(b *ssm.InMemoryBackend) {
				for _, name := range []string{"/app/db", "/app/cache", "/other/key"} {
					b.PutParameter(&ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
				}
			},
			wantStatus: http.StatusOK,
			wantCount:  3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(backend)
			}

			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out struct {
				Parameters []json.RawMessage `json:"Parameters"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Parameters, tt.wantCount)
		})
	}
}

// --- Handler interface tests ---

func TestHandler_Interface(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	assert.Equal(t, "SSM", h.Name())
	assert.Equal(t, 100, h.MatchPriority())

	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AmazonSSM.GetParameter")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "GetParameter", h.ExtractOperation(c))

	req2 := httptest.NewRequest(http.MethodPost, "/", nil)
	req2.Header.Set("X-Amz-Target", "AmazonSSMNoSep")
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.Equal(t, "Unknown", h.ExtractOperation(c2))

	body := `{"Name":"/my/param"}`
	req3 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	c3 := e.NewContext(req3, httptest.NewRecorder())
	assert.Equal(t, "/my/param", h.ExtractResource(c3))

	req4 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	c4 := e.NewContext(req4, httptest.NewRecorder())
	assert.Empty(t, h.ExtractResource(c4))
}

// --- Provider tests ---

func TestProvider(t *testing.T) {
	t.Parallel()

	p := &ssm.Provider{}
	assert.Equal(t, "SSM", p.Name())


	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

// --- Handler error cases ---

func TestHandler_ErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(b *ssm.InMemoryBackend)
		name       string
		target     string
		body       string
		wantErrTyp string
		wantStatus int
	}{
		{
			name:       "ParameterNotFound",
			target:     "AmazonSSM.GetParameter",
			body:       `{"Name":"/missing/param"}`,
			wantStatus: http.StatusBadRequest,
			wantErrTyp: "ParameterNotFound",
		},
		{
			name:   "ParameterAlreadyExists",
			target: "AmazonSSM.PutParameter",
			body:   `{"Name":"/existing","Type":"String","Value":"v2","Overwrite":false}`,
			setup: func(b *ssm.InMemoryBackend) {
				b.PutParameter(
					&ssm.PutParameterInput{Name: "/existing", Type: "String", Value: "v1"},
				)
			},
			wantStatus: http.StatusBadRequest,
			wantErrTyp: "ParameterAlreadyExists",
		},
		{
			name:       "InvalidTarget",
			target:     "AmazonSSMNoSep",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()

			backend := ssm.NewInMemoryBackend()
			h := ssm.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(backend)
			}

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()

			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErrTyp != "" {
				var errResp service.JSONErrorResponse
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErrTyp, errResp.Type)
			}
		})
	}
}

// --- ParamMatchesFilter tests ---

func TestParamMatchesFilter_Options(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filters   []ssm.ParameterFilter
		wantCount int
	}{
		{
			name: "Contains",
			filters: []ssm.ParameterFilter{
				{Key: "Name", Option: "Contains", Values: []string{"db"}},
			},
			wantCount: 1,
		},
		{
			name: "UnknownKeyIgnored",
			filters: []ssm.ParameterFilter{
				{Key: "UnknownKey", Option: "Equals", Values: []string{"anything"}},
			},
			wantCount: 3,
		},
		{
			name: "DefaultOptionIsEquals",
			filters: []ssm.ParameterFilter{
				{Key: "Type", Values: []string{"SecureString"}},
			},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ssm.NewInMemoryBackend()
			_, _ = backend.PutParameter(
				&ssm.PutParameterInput{Name: "/app/db/host", Type: "String", Value: "localhost"},
			)
			_, _ = backend.PutParameter(
				&ssm.PutParameterInput{
					Name:  "/app/cache/host",
					Type:  "SecureString",
					Value: "cache",
				},
			)
			_, _ = backend.PutParameter(
				&ssm.PutParameterInput{Name: "/other/key", Type: "String", Value: "v"},
			)

			out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
				ParameterFilters: tt.filters,
			})
			require.NoError(t, err)
			assert.Len(t, out.Parameters, tt.wantCount)
		})
	}
}

// --- ParseNextToken bad token test ---

func TestParseNextToken_BadToken(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	for i := range 3 {
		_, _ = backend.PutParameter(&ssm.PutParameterInput{
			Name: "/p" + string(rune('0'+i)), Type: "String", Value: "v",
		})
	}

	out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
		NextToken: "not-a-number",
	})
	require.NoError(t, err)
	assert.Len(t, out.Parameters, 3)
}

// --- Handler HTTP via-HTTP tests ---

func TestHandler_GetParametersByPathViaHTTP(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandler(t)

	_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "/svc/a", Type: "String", Value: "1"})
	_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "/svc/b", Type: "String", Value: "2"})
	_, _ = backend.PutParameter(
		&ssm.PutParameterInput{Name: "/other/c", Type: "String", Value: "3"},
	)

	rec := doRequest(t, h, "GetParametersByPath", `{"Path":"/svc","Recursive":true}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out ssm.GetParametersByPathOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.Parameters, 2)
}

func TestHandler_DescribeParametersViaHTTP(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandler(t)

	_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "/a", Type: "String", Value: "1"})
	_, _ = backend.PutParameter(
		&ssm.PutParameterInput{Name: "/b", Type: "SecureString", Value: "2"},
	)

	rec := doRequest(t, h, "DescribeParameters", `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out ssm.DescribeParametersOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.Parameters, 2)
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPut, "/", nil)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_ParameterOpsViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(b *ssm.InMemoryBackend)
		name            string
		action          string
		body            string
		wantBodyContain string
		wantStatus      int
	}{
		{
			name:            "PutParameter",
			action:          "PutParameter",
			body:            `{"Name":"/http/put","Type":"String","Value":"v1"}`,
			wantStatus:      http.StatusOK,
			wantBodyContain: "Version",
		},
		{
			name:   "GetParameter",
			action: "GetParameter",
			body:   `{"Name":"/http/get"}`,
			setup: func(b *ssm.InMemoryBackend) {
				b.PutParameter(
					&ssm.PutParameterInput{Name: "/http/get", Type: "String", Value: "val"},
				)
			},
			wantStatus:      http.StatusOK,
			wantBodyContain: "val",
		},
		{
			name:   "GetParameters",
			action: "GetParameters",
			body:   `{"Names":["/http/a","/http/b","missing"]}`,
			setup: func(b *ssm.InMemoryBackend) {
				b.PutParameter(&ssm.PutParameterInput{Name: "/http/a", Type: "String", Value: "a"})
				b.PutParameter(&ssm.PutParameterInput{Name: "/http/b", Type: "String", Value: "b"})
			},
			wantStatus:      http.StatusOK,
			wantBodyContain: "InvalidParameters",
		},
		{
			name:   "GetParameterHistory",
			action: "GetParameterHistory",
			body:   `{"Name":"/http/hist"}`,
			setup: func(b *ssm.InMemoryBackend) {
				b.PutParameter(
					&ssm.PutParameterInput{Name: "/http/hist", Type: "String", Value: "v1"},
				)
				b.PutParameter(
					&ssm.PutParameterInput{
						Name:      "/http/hist",
						Type:      "String",
						Value:     "v2",
						Overwrite: true,
					},
				)
			},
			wantStatus:      http.StatusOK,
			wantBodyContain: "v2",
		},
		{
			name:   "DeleteParameter",
			action: "DeleteParameter",
			body:   `{"Name":"/http/del"}`,
			setup: func(b *ssm.InMemoryBackend) {
				b.PutParameter(
					&ssm.PutParameterInput{Name: "/http/del", Type: "String", Value: "v"},
				)
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "DeleteParameters",
			action: "DeleteParameters",
			body:   `{"Names":["/http/d1","missing"]}`,
			setup: func(b *ssm.InMemoryBackend) {
				b.PutParameter(&ssm.PutParameterInput{Name: "/http/d1", Type: "String", Value: "v"})
			},
			wantStatus:      http.StatusOK,
			wantBodyContain: "DeletedParameters",
		},
		{
			name:   "AddTagsToResource",
			action: "AddTagsToResource",
			body:   `{"ResourceType":"Parameter","ResourceId":"/http/tag","Tags":[{"Key":"k","Value":"v"}]}`,
			setup: func(b *ssm.InMemoryBackend) {
				b.PutParameter(
					&ssm.PutParameterInput{Name: "/http/tag", Type: "String", Value: "v"},
				)
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "RemoveTagsFromResource",
			action: "RemoveTagsFromResource",
			body:   `{"ResourceType":"Parameter","ResourceId":"/http/tag","TagKeys":["k"]}`,
			setup: func(b *ssm.InMemoryBackend) {
				b.PutParameter(
					&ssm.PutParameterInput{Name: "/http/tag", Type: "String", Value: "v"},
				)
				b.AddTagsToResource(&ssm.AddTagsToResourceInput{
					ResourceID: "/http/tag", Tags: []ssm.Tag{{Key: "k", Value: "v"}},
				})
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "ListTagsForResource",
			action: "ListTagsForResource",
			body:   `{"ResourceType":"Parameter","ResourceId":"/http/tag"}`,
			setup: func(b *ssm.InMemoryBackend) {
				b.PutParameter(
					&ssm.PutParameterInput{Name: "/http/tag", Type: "String", Value: "v"},
				)
				b.AddTagsToResource(&ssm.AddTagsToResourceInput{
					ResourceID: "/http/tag", Tags: []ssm.Tag{{Key: "k", Value: "v"}},
				})
			},
			wantStatus:      http.StatusOK,
			wantBodyContain: "TagList",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(backend)
			}

			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBodyContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContain)
			}
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	matcher := h.RouteMatcher()
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AmazonSSM.GetParameter")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.True(t, matcher(c))

	req2 := httptest.NewRequest(http.MethodPost, "/", nil)
	req2.Header.Set("X-Amz-Target", "Other.Action")
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.False(t, matcher(c2))
}

// --- ValidateParameterName tests ---

func TestValidateParameterName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		paramName string
		wantErr   bool
	}{
		{name: "valid path", paramName: "/my/param", wantErr: false},
		{name: "valid simple", paramName: "MyParam", wantErr: false},
		{name: "double slash", paramName: "/my//param", wantErr: true},
		{name: "reserved ssm", paramName: "ssm/something", wantErr: true},
		{name: "reserved aws", paramName: "aws-param", wantErr: true},
		{name: "reserved amazon", paramName: "amazon.param", wantErr: true},
		{name: "invalid char", paramName: "/my param!", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := ssm.NewInMemoryBackend()
			_, err := backend.PutParameter(&ssm.PutParameterInput{
				Name:  tc.paramName,
				Type:  "String",
				Value: "val",
			})
			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ssm.ErrValidationException)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// --- Tag operations test ---

func TestTagOperations(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandler(t)
	e := echo.New()

	_, err := backend.PutParameter(&ssm.PutParameterInput{
		Name: "my-param", Type: "String", Value: "val",
	})
	require.NoError(t, err)

	// AddTagsToResource
	addBody := `{"ResourceType":"Parameter","ResourceId":"my-param",` +
		`"Tags":[{"Key":"env","Value":"prod"},{"Key":"team","Value":"ops"}]}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(addBody))
	req.Header.Set("X-Amz-Target", "AmazonSSM.AddTagsToResource")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListTagsForResource
	listBody := `{"ResourceType":"Parameter","ResourceId":"my-param"}`
	req2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(listBody))
	req2.Header.Set("X-Amz-Target", "AmazonSSM.ListTagsForResource")
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)

	var listOut ssm.ListTagsForResourceOutput
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listOut))
	require.Len(t, listOut.TagList, 2)
	assert.Equal(t, "env", listOut.TagList[0].Key)
	assert.Equal(t, "prod", listOut.TagList[0].Value)

	// RemoveTagsFromResource
	removeBody := `{"ResourceType":"Parameter","ResourceId":"my-param","TagKeys":["env"]}`
	req3 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(removeBody))
	req3.Header.Set("X-Amz-Target", "AmazonSSM.RemoveTagsFromResource")
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Verify only team tag remains
	listOut2, err := backend.ListTagsForResource(
		&ssm.ListTagsForResourceInput{ResourceID: "my-param"},
	)
	require.NoError(t, err)
	require.Len(t, listOut2.TagList, 1)
	assert.Equal(t, "team", listOut2.TagList[0].Key)
}
