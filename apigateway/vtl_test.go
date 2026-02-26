package apigateway_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/apigateway"
)

func TestRenderTemplate_InputBody(t *testing.T) {
	t.Parallel()

	ctx := apigateway.VTLContext{Body: `{"hello":"world"}`}
	out := apigateway.RenderTemplate(`{"body":$input.body}`, ctx)
	assert.JSONEq(t, `{"body":{"hello":"world"}}`, out)
}

func TestRenderTemplate_InputBodyEmpty(t *testing.T) {
	t.Parallel()

	ctx := apigateway.VTLContext{}
	out := apigateway.RenderTemplate(`$input.body`, ctx)
	assert.Empty(t, out)
}

func TestRenderTemplate_ContextRequestId(t *testing.T) {
	t.Parallel()

	ctx := apigateway.VTLContext{RequestID: "req-123"}
	out := apigateway.RenderTemplate(`{"id":"$context.requestId"}`, ctx)
	assert.JSONEq(t, `{"id":"req-123"}`, out)
}

func TestRenderTemplate_InputJSONRoot(t *testing.T) {
	t.Parallel()

	ctx := apigateway.VTLContext{Body: `{"name":"Alice","age":30}`}
	out := apigateway.RenderTemplate(`$input.json('$')`, ctx)
	// Root extraction should return JSON-encoded form of the object.
	assert.Contains(t, out, `"name"`)
	assert.Contains(t, out, `"Alice"`)
}

func TestRenderTemplate_InputJSONField(t *testing.T) {
	t.Parallel()

	ctx := apigateway.VTLContext{Body: `{"name":"Alice","age":30}`}
	out := apigateway.RenderTemplate(`{"name":$input.json('$.name')}`, ctx)
	assert.JSONEq(t, `{"name":"Alice"}`, out)
}

func TestRenderTemplate_InputJSONNumericField(t *testing.T) {
	t.Parallel()

	ctx := apigateway.VTLContext{Body: `{"count":42}`}
	out := apigateway.RenderTemplate(`$input.json('$.count')`, ctx)
	assert.Equal(t, `42`, out)
}

func TestRenderTemplate_InputJSONArrayIndex(t *testing.T) {
	t.Parallel()

	ctx := apigateway.VTLContext{Body: `{"items":["a","b","c"]}`}
	out := apigateway.RenderTemplate(`$input.json('$.items[1]')`, ctx)
	assert.Equal(t, `"b"`, out)
}

func TestRenderTemplate_InputPathField(t *testing.T) {
	t.Parallel()

	ctx := apigateway.VTLContext{Body: `{"username":"bob"}`}
	out := apigateway.RenderTemplate(`Hello $input.path('$.username')`, ctx)
	assert.Equal(t, `Hello bob`, out)
}

func TestRenderTemplate_InputPathBoolField(t *testing.T) {
	t.Parallel()

	ctx := apigateway.VTLContext{Body: `{"active":true}`}
	out := apigateway.RenderTemplate(`$input.path('$.active')`, ctx)
	assert.Equal(t, `true`, out)
}

func TestRenderTemplate_InputPathNumericField(t *testing.T) {
	t.Parallel()

	ctx := apigateway.VTLContext{Body: `{"count":7}`}
	out := apigateway.RenderTemplate(`$input.path('$.count')`, ctx)
	assert.Equal(t, `7`, out)
}

func TestRenderTemplate_InputPathNestedField(t *testing.T) {
	t.Parallel()

	ctx := apigateway.VTLContext{Body: `{"user":{"name":"carol"}}`}
	out := apigateway.RenderTemplate(`$input.path('$.user.name')`, ctx)
	assert.Equal(t, `carol`, out)
}

func TestRenderTemplate_InputPathMissing(t *testing.T) {
	t.Parallel()

	ctx := apigateway.VTLContext{Body: `{"a":1}`}
	out := apigateway.RenderTemplate(`$input.path('$.nonexistent')`, ctx)
	assert.Empty(t, out)
}

func TestRenderTemplate_UtilEscapeJavaScript(t *testing.T) {
	t.Parallel()

	ctx := apigateway.VTLContext{}
	out := apigateway.RenderTemplate(`$util.escapeJavaScript('hello "world"')`, ctx)
	assert.Equal(t, `hello \"world\"`, out)
}

func TestRenderTemplate_UtilEscapeJavaScriptSlash(t *testing.T) {
	t.Parallel()

	ctx := apigateway.VTLContext{}
	out := apigateway.RenderTemplate(`$util.escapeJavaScript('a/b')`, ctx)
	assert.Equal(t, `a\/b`, out)
}

func TestRenderTemplate_UtilEscapeJavaScriptNewline(t *testing.T) {
	t.Parallel()

	ctx := apigateway.VTLContext{}
	out := apigateway.RenderTemplate("$util.escapeJavaScript('line1\nline2')", ctx)
	assert.Equal(t, `line1\nline2`, out)
}

func TestRenderTemplate_Combined(t *testing.T) {
	t.Parallel()

	body := `{"action":"login","user":"dave"}`
	ctx := apigateway.VTLContext{Body: body, RequestID: "abc-999"}
	tmpl := `{"action":$input.json('$.action'),"user":"$input.path('$.user')","reqId":"$context.requestId"}`
	out := apigateway.RenderTemplate(tmpl, ctx)
	assert.JSONEq(t, `{"action":"login","user":"dave","reqId":"abc-999"}`, out)
}

func TestRenderTemplate_NoPlaceholders(t *testing.T) {
	t.Parallel()

	ctx := apigateway.VTLContext{Body: `{"x":1}`}
	plain := `{"static":"value"}`
	out := apigateway.RenderTemplate(plain, ctx)
	assert.Equal(t, plain, out)
}

func TestRenderTemplate_InvalidBodyForPath(t *testing.T) {
	t.Parallel()

	ctx := apigateway.VTLContext{Body: `not-json`}
	out := apigateway.RenderTemplate(`$input.path('$.key')`, ctx)
	assert.Empty(t, out)
}

func TestRenderTemplate_DoubleQuoteInFunction(t *testing.T) {
	t.Parallel()

	ctx := apigateway.VTLContext{Body: `{"key":"val"}`}
	out := apigateway.RenderTemplate(`$input.json("$.key")`, ctx)
	assert.Equal(t, `"val"`, out)
}
