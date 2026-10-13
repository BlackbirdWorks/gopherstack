package apigateway_test

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

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

type bodyInvoker struct {
	got      *string
	response string
}

func (b *bodyInvoker) InvokeFunction(_ context.Context, _, _ string, payload []byte) ([]byte, int, error) {
	if b.got != nil {
		*b.got = string(payload)
	}

	return []byte(b.response), http.StatusOK, nil
}

// setupMappingAPI deploys POST /m with the given integration JSON fields and integration responses.
func setupMappingAPI(
	t *testing.T, integrationFields string, responses map[string]string, invoker apigateway.LambdaInvoker,
) (*apigateway.Handler, *echo.Echo, string) {
	t.Helper()

	h := apigateway.NewHandler(apigateway.NewInMemoryBackend())
	if invoker != nil {
		h.SetLambdaInvoker(invoker)
	}

	e := echo.New()

	var created map[string]any

	rec := postWithHandler(t, h, e, "CreateRestApi", `{"name":"map-api"}`)
	require.Equal(t, http.StatusCreated, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	apiID := created["id"].(string)

	rec = postWithHandler(t, h, e, "GetResources", `{"restApiId":"`+apiID+`"}`)

	var list map[string]any

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
	rootID := list["item"].([]any)[0].(map[string]any)["id"].(string)

	rec = postWithHandler(t, h, e, "CreateResource",
		`{"restApiId":"`+apiID+`","parentId":"`+rootID+`","pathPart":"m"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var child map[string]any

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &child))
	resID := child["id"].(string)
	base := `"restApiId":"` + apiID + `","resourceId":"` + resID + `","httpMethod":"POST"`

	rec = postWithHandler(t, h, e, "PutMethod", `{`+base+`,"authorizationType":"NONE"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = postWithHandler(t, h, e, "PutIntegration", `{`+base+`,`+integrationFields+`}`)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	for status, fields := range responses {
		rec = postWithHandler(t, h, e, "PutIntegrationResponse", `{`+base+`,"statusCode":"`+status+`",`+fields+`}`)
		require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	}

	rec = postWithHandler(t, h, e, "CreateDeployment", `{"restApiId":"`+apiID+`","stageName":"prod"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	return h, e, apiID
}

func invokeMapping(
	t *testing.T, h *apigateway.Handler, e *echo.Echo, apiID, contentType, body, query string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/proxy/"+apiID+"/prod/m"+query, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))

	return rec
}

func TestProxy_MockIntegrationStatusSelection(t *testing.T) {
	t.Parallel()

	const tmpl = `"type":"MOCK","requestTemplates":{"application/json":` +
		`"{\"statusCode\": #if($input.params('fail') == 'yes')404#{else}201#end}"}`

	responses := map[string]string{
		"201": `"selectionPattern":"","responseTemplates":{"application/json":"{\"made\":\"$context.httpMethod\"}"},` +
			`"responseParameters":{"method.response.header.X-Mode":"'created'"}`,
		"404": `"selectionPattern":"404","responseTemplates":{"application/json":"{\"missing\":true}"}`,
	}

	tests := []struct {
		name       string
		query      string
		wantBody   string
		wantHeader string
		wantStatus int
	}{
		{name: "default", wantStatus: http.StatusCreated, wantBody: `{"made":"POST"}`, wantHeader: "created"},
		{
			name:       "selected by statusCode",
			query:      "?fail=yes",
			wantStatus: http.StatusNotFound,
			wantBody:   `{"missing":true}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e, apiID := setupMappingAPI(t, tmpl, responses, nil)
			rec := invokeMapping(t, h, e, apiID, "application/json", `{}`, tt.query)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantBody, rec.Body.String())
			assert.Equal(t, tt.wantHeader, rec.Header().Get("X-Mode"))
		})
	}
}

func TestProxy_LambdaMappingTemplates(t *testing.T) {
	t.Parallel()

	reqTmpl := `#set($root = $input.path('$'))\n` +
		`{\"names\":[#foreach($u in $root.users)\"$u.name\"#if($foreach.hasNext),#end#end],` +
		`\"q\":\"$input.params('q')\",\"ua\":\"$util.escapeJavaScript($root.note)\"}`
	respTmpl := `#set($r = $input.path('$'))\n{\"count\":$r.items.size(),\"first\":\"$r.items.get(0)\"}`

	var sent string

	invoker := &bodyInvoker{response: `{"items":["x","y"]}`, got: &sent}
	h, e, apiID := setupMappingAPI(
		t,
		`"type":"AWS","uri":"arn:aws:lambda:us-east-1:123:function:fn","requestTemplates":{"application/json":"`+reqTmpl+`"}`,
		map[string]string{"200": `"responseTemplates":{"application/json":"` + respTmpl + `"}`},
		invoker,
	)

	rec := invokeMapping(t, h, e, apiID, "application/json",
		`{"users":[{"name":"a"},{"name":"b"}],"note":"it's"}`, "?q=zz")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"count":2,"first":"x"}`, rec.Body.String())
	assert.Contains(t, sent, `"ua":"it\'s"`)
}

func TestProxy_PassthroughBehavior(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		behavior    string
		contentType string
		wantSent    string
		wantStatus  int
	}{
		{
			name:        "when no match passes through",
			behavior:    "WHEN_NO_MATCH",
			contentType: "text/plain",
			wantStatus:  200,
			wantSent:    "raw",
		},
		{name: "default passes through", behavior: "", contentType: "text/plain", wantStatus: 200, wantSent: "raw"},
		{name: "never rejects", behavior: "NEVER", contentType: "text/plain", wantStatus: 415},
		{name: "when no templates rejects", behavior: "WHEN_NO_TEMPLATES", contentType: "text/plain", wantStatus: 415},
		{
			name:        "template wins",
			behavior:    "NEVER",
			contentType: "application/json",
			wantStatus:  200,
			wantSent:    `{"t":1}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var sent string

			fields := `"type":"AWS","uri":"arn:aws:lambda:us-east-1:123:function:fn",` +
				`"requestTemplates":{"application/json":"{\"t\":1}"}`
			if tt.behavior != "" {
				fields += `,"passthroughBehavior":"` + tt.behavior + `"`
			}

			h, e, apiID := setupMappingAPI(t, fields, nil, &bodyInvoker{response: `{}`, got: &sent})
			rec := invokeMapping(t, h, e, apiID, tt.contentType, "raw", "")

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				assert.Equal(t, tt.wantSent, sent)
			}
		})
	}
}
