package ssm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

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
	putOut, err := backend.PutParameter(context.TODO(), putIn)
	require.NoError(t, err)
	assert.Equal(t, int64(1), putOut.Version)

	getOut, err := backend.GetParameter(context.TODO(), &ssm.GetParameterInput{
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
	_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name:  "db-password",
		Type:  "SecureString",
		Value: "supersecret",
	})

	input2 := &ssm.PutParameterInput{
		Name: "db-password", Type: "String", Value: "{}", Overwrite: false,
	}
	_, duplicateErr := backend.PutParameter(context.TODO(), input2)
	require.ErrorIs(t, duplicateErr, ssm.ErrParameterAlreadyExists)
}

func TestInMemoryBackend_Overwrite(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
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
	putOut, err := backend.PutParameter(context.TODO(), putInOverwrite)
	require.NoError(t, err)
	assert.Equal(t, int64(2), putOut.Version)

	getOut, err := backend.GetParameter(context.TODO(), &ssm.GetParameterInput{Name: "db-password"})
	require.NoError(t, err)
	assert.Equal(t, "newsecret", getOut.Parameter.Value)
	assert.Equal(t, int64(2), getOut.Parameter.Version)
}

func TestInMemoryBackend_GetParameters(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, _ = backend.PutParameter(
		context.TODO(),
		&ssm.PutParameterInput{Name: "db-password", Type: "String", Value: "pwd"},
	)
	_, _ = backend.PutParameter(
		context.TODO(),
		&ssm.PutParameterInput{Name: "api-key", Type: "String", Value: "123"},
	)

	getParamsOut, err := backend.GetParameters(context.TODO(), &ssm.GetParametersInput{
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
		context.TODO(),
		&ssm.PutParameterInput{Name: "api-key", Type: "String", Value: "123"},
	)
	_, _ = backend.PutParameter(
		context.TODO(),
		&ssm.PutParameterInput{Name: "db-password", Type: "String", Value: "pwd"},
	)

	all := backend.ListAll(context.TODO())
	assert.Len(t, all, 2)
	assert.Equal(t, "api-key", all[0].Name)
	assert.Equal(t, "db-password", all[1].Name)
}

func TestInMemoryBackend_DeleteAll(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, _ = backend.PutParameter(
		context.TODO(),
		&ssm.PutParameterInput{Name: "api-key", Type: "String", Value: "123"},
	)
	_, _ = backend.PutParameter(
		context.TODO(),
		&ssm.PutParameterInput{Name: "db-password", Type: "String", Value: "pwd"},
	)

	backend.DeleteParameter(context.TODO(), &ssm.DeleteParameterInput{Name: "api-key"})
	backend.DeleteParameter(context.TODO(), &ssm.DeleteParameterInput{Name: "db-password"})
	assert.Empty(t, backend.ListAll(context.TODO()))
}

func TestInMemoryBackend_DeleteParameters(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, _ = backend.PutParameter(
		context.TODO(),
		&ssm.PutParameterInput{Name: "key1", Type: "String", Value: "v1"},
	)

	delOut, err := backend.DeleteParameters(
		context.TODO(),
		&ssm.DeleteParametersInput{
			Names: []string{"db-password", "key1", "missing"},
		},
	)
	require.NoError(t, err)
	assert.Len(t, delOut.DeletedParameters, 1)
	assert.Len(t, delOut.InvalidParameters, 2)
	assert.Empty(t, backend.ListAll(context.TODO()))
}

// --- Handler routing tests ---

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
	putOut, err := backend.PutParameter(context.TODO(), putIn)
	require.NoError(t, err)
	assert.Equal(t, int64(1), putOut.Version)

	historyOut, err := backend.GetParameterHistory(context.TODO(), &ssm.GetParameterHistoryInput{
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

	_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name: "counter", Type: "String", Value: "1",
	})
	_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name: "counter", Type: "String", Value: "2", Overwrite: true,
	})
	_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name: "counter", Type: "String", Value: "3", Overwrite: true,
	})

	historyOut, err := backend.GetParameterHistory(context.TODO(), &ssm.GetParameterHistoryInput{
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

	_, err := backend.GetParameterHistory(context.TODO(), &ssm.GetParameterHistoryInput{
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
		_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
			Name:      "paginated-param",
			Type:      "String",
			Value:     "value-" + string(rune(i+'0'-1)),
			Overwrite: overwrite,
		})
	}

	maxResults := int64(2)
	historyOut, err := backend.GetParameterHistory(context.TODO(), &ssm.GetParameterHistoryInput{
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

	_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name: "type-change", Type: "String", Value: "string-value",
	})
	_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name: "type-change", Type: "SecureString", Value: "secure-value", Overwrite: true,
	})

	historyOut, err := backend.GetParameterHistory(context.TODO(), &ssm.GetParameterHistoryInput{
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

	_, err := backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name: "db-password", Type: "SecureString", Value: "super-secret-password",
	})
	require.NoError(t, err)

	output, err := backend.GetParameter(context.TODO(), &ssm.GetParameterInput{
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

	_, err := backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name: "db-password", Type: "SecureString", Value: "super-secret-password",
	})
	require.NoError(t, err)

	output, err := backend.GetParameter(context.TODO(), &ssm.GetParameterInput{
		Name: "db-password", WithDecryption: true,
	})
	require.NoError(t, err)
	assert.Equal(t, "SecureString", output.Parameter.Type)
	assert.Equal(t, "super-secret-password", output.Parameter.Value)
}

func TestSecureString_GetParametersDecryption(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()

	_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name: "api-key", Type: "SecureString", Value: "api-key-value",
	})
	_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name: "db-password", Type: "SecureString", Value: "db-password-value",
	})
	_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name: "environment", Type: "String", Value: "production",
	})

	outputNoDecrypt, err := backend.GetParameters(context.TODO(), &ssm.GetParametersInput{
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

	outputWithDecrypt, err := backend.GetParameters(context.TODO(), &ssm.GetParametersInput{
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

	_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name: "secret", Type: "SecureString", Value: "secret-v1",
	})
	_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name: "secret", Type: "SecureString", Value: "secret-v2", Overwrite: true,
	})

	historyOutput, err := backend.GetParameterHistory(context.TODO(), &ssm.GetParameterHistoryInput{
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
		_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
	}

	out, err := backend.GetParametersByPath(context.TODO(), &ssm.GetParametersByPathInput{
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
		_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
	}

	out, err := backend.GetParametersByPath(context.TODO(), &ssm.GetParametersByPathInput{
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
		_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
	}

	maxRes := int64(2)
	out, err := backend.GetParametersByPath(context.TODO(), &ssm.GetParametersByPathInput{
		Path: "/params", Recursive: true, MaxResults: &maxRes,
	})
	require.NoError(t, err)
	assert.Len(t, out.Parameters, 2)
	assert.NotEmpty(t, out.NextToken)

	out2, err := backend.GetParametersByPath(context.TODO(), &ssm.GetParametersByPathInput{
		Path: "/params", Recursive: true, MaxResults: &maxRes, NextToken: out.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, out2.Parameters, 2)
}

func TestGetParametersByPath_EmptyPath(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	out, err := backend.GetParametersByPath(context.TODO(), &ssm.GetParametersByPathInput{
		Path: "/nonexistent", Recursive: true,
	})
	require.NoError(t, err)
	assert.Empty(t, out.Parameters)
}

func TestGetParametersByPath_WithDecryption(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name: "/secrets/key", Type: "SecureString", Value: "plaintext",
	})

	out, err := backend.GetParametersByPath(context.TODO(), &ssm.GetParametersByPathInput{
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
		_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{Name: p.name, Type: p.typ, Value: "v"})
	}

	out, err := backend.DescribeParameters(context.TODO(), &ssm.DescribeParametersInput{})
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
		_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{Name: p.name, Type: p.typ, Value: "v"})
	}

	out, err := backend.DescribeParameters(context.TODO(), &ssm.DescribeParametersInput{
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
		_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
	}

	out, err := backend.DescribeParameters(context.TODO(), &ssm.DescribeParametersInput{
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
		_, _ = backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
			Name: "/p" + string(rune('0'+i)), Type: "String", Value: "v",
		})
	}

	maxRes := int64(2)
	out, err := backend.DescribeParameters(context.TODO(), &ssm.DescribeParametersInput{MaxResults: &maxRes})
	require.NoError(t, err)
	assert.Len(t, out.Parameters, 2)
	assert.NotEmpty(t, out.NextToken)

	out2, err := backend.DescribeParameters(context.TODO(), &ssm.DescribeParametersInput{
		MaxResults: &maxRes, NextToken: out.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, out2.Parameters, 2)
}

func TestDescribeParameters_BeyondEnd(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	out, err := backend.DescribeParameters(context.TODO(), &ssm.DescribeParametersInput{
		NextToken: "9999",
	})
	require.NoError(t, err)
	assert.Empty(t, out.Parameters)
}

// --- Handler HTTP operation tests ---

func TestHandlerParameterListingOperations(t *testing.T) {
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
					b.PutParameter(context.TODO(), &ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
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
					b.PutParameter(context.TODO(), &ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
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

// --- Tag operations test ---

func TestTagOperations(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandler(t)
	e := echo.New()

	_, err := backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
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
		context.TODO(),
		&ssm.ListTagsForResourceInput{ResourceID: "my-param"},
	)
	require.NoError(t, err)
	require.Len(t, listOut2.TagList, 1)
	assert.Equal(t, "team", listOut2.TagList[0].Key)
}
