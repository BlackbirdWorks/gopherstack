package ociapigateway_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validatedSpec(policies string) string {
	return `{"routes":[{"path":"/v","methods":["POST"],` +
		`"backend":{"type":"STOCK_RESPONSE_BACKEND","status":200,"body":"ok"},` +
		`"requestPolicies":{` + policies + `}}]}`
}

func TestRequestValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		policies string
		query    string
		headers  map[string]string
		body     string
		want     int
	}{
		{
			name:     "header required missing",
			policies: `"headerValidations":{"headers":[{"name":"X-User","required":true}]}`,
			want:     http.StatusBadRequest,
		},
		{
			name:     "header required present case insensitive",
			policies: `"headerValidations":{"headers":[{"name":"X-User","required":true}]}`,
			headers:  map[string]string{"x-user": "bob"}, want: http.StatusOK,
		},
		{
			name:     "header optional missing",
			policies: `"headerValidations":{"headers":[{"name":"X-User","required":false}]}`,
			want:     http.StatusOK,
		},
		{
			name:     "permissive lets failures through",
			policies: `"headerValidations":{"validationMode":"PERMISSIVE","headers":[{"name":"X-User","required":true}]}`,
			want:     http.StatusOK,
		},
		{
			name:     "disabled lets failures through",
			policies: `"headerValidations":{"validationMode":"DISABLED","headers":[{"name":"X-User","required":true}]}`,
			want:     http.StatusOK,
		},
		{
			name:     "query required missing",
			policies: `"queryParameterValidations":{"parameters":[{"name":"state","required":true}]}`,
			want:     http.StatusBadRequest,
		},
		{
			name:     "query required present",
			policies: `"queryParameterValidations":{"parameters":[{"name":"state","required":true}]}`,
			query:    "?STATE=1", want: http.StatusOK,
		},
		{
			name:     "body required empty",
			policies: `"bodyValidation":{"required":true,"content":{"application/json":{"validationType":"NONE"}}}`,
			want:     http.StatusBadRequest,
		},
		{
			name:     "body wrong content type",
			policies: `"bodyValidation":{"required":true,"content":{"application/json":{"validationType":"NONE"}}}`,
			headers:  map[string]string{"Content-Type": "text/plain"}, body: "hi", want: http.StatusBadRequest,
		},
		{
			name:     "body exact content type with parameters",
			policies: `"bodyValidation":{"required":true,"content":{"application/json":{"validationType":"NONE"}}}`,
			headers: map[string]string{
				"Content-Type": "application/json; charset=utf-8",
			},
			body: "{}",
			want: http.StatusOK,
		},
		{
			name:     "body subtype wildcard",
			policies: `"bodyValidation":{"content":{"text/*":{"validationType":"NONE"}}}`,
			headers:  map[string]string{"Content-Type": "text/plain"}, body: "hi", want: http.StatusOK,
		},
		{
			name:     "body full wildcard",
			policies: `"bodyValidation":{"content":{"*/*":{"validationType":"NONE"}}}`,
			headers:  map[string]string{"Content-Type": "image/png"}, body: "x", want: http.StatusOK,
		},
		{
			name:     "body optional and absent",
			policies: `"bodyValidation":{"required":false,"content":{"application/json":{"validationType":"NONE"}}}`,
			want:     http.StatusOK,
		},
	}

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "validation")

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			prefix := "/v" + string(rune('a'+i))
			e.mkDeployment(t, *gw.Id, prefix, validatedSpec(tt.policies))

			r := e.call(t, *gw.Id, http.MethodPost, prefix+"/v"+tt.query, tt.headers, tt.body)
			assert.Equal(t, tt.want, r.status, r.body)

			if tt.want == http.StatusBadRequest {
				assert.Contains(t, r.body, `"code":400`)
			}
		})
	}
}

func TestRequestValidationSpecErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		policies string
	}{
		{
			"json schema validation type",
			`"bodyValidation":{"content":{"application/json":{"validationType":"JSON_SCHEMA"}}}`,
		},
		{"empty content", `"bodyValidation":{"content":{}}`},
		{"bad mode", `"headerValidations":{"validationMode":"STRICT","headers":[{"name":"a"}]}`},
		{"unnamed header", `"headerValidations":{"headers":[{"required":true}]}`},
	}

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "validation-bad")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.createDeployment(t, *gw.Id, "/bad", specFromJSON(t, validatedSpec(tt.policies)))
			require.Error(t, err)
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestHeaderValidationObjectFormFromDocs(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "validation-doc")

	body := `{"gatewayId":"` + *gw.Id + `","compartmentId":"` + testCompartment + `","pathPrefix":"/d","specification":` +
		validatedSpec(
			`"headerValidations":{"headers":{"name":"X-Username","required":true},"validationMode":"ENFORCING"}`,
		) + `}`
	e.post(t, "/deployments", body, http.StatusAccepted)
	e.settle()

	assert.Equal(t, http.StatusBadRequest, e.call(t, *gw.Id, http.MethodPost, "/d/v", nil, "").status)
	assert.Equal(t, http.StatusOK,
		e.call(t, *gw.Id, http.MethodPost, "/d/v", map[string]string{"X-Username": "u"}, "").status)
}
