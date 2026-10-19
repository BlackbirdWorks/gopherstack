package chaos_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
)

func lsServer(t *testing.T) (*echo.Echo, *chaos.FaultStore) {
	t.Helper()

	e := echo.New()
	store := chaos.NewFaultStore()
	chaos.RegisterLocalStackRoutes(e, store)
	e.Group("/aws", chaos.Middleware(store)).Any("/*", func(c *echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	return e, store
}

func lsCall(e *echo.Echo, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	return rec
}

func TestLocalStackChaosFaultsLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  string
		steps [][3]string
	}{
		{
			name:  "post_replaces",
			steps: [][3]string{{"POST", `[{"service":"s3"}]`, ""}, {"POST", `[{"service":"sqs"}]`, ""}},
			want:  `[{"service":"sqs"}]`,
		},
		{
			name:  "patch_appends",
			steps: [][3]string{{"POST", `[{"service":"s3"}]`, ""}, {"PATCH", `[{"service":"sqs"}]`, ""}},
			want:  `[{"service":"s3"},{"service":"sqs"}]`,
		},
		{
			name: "delete_exact_only",
			steps: [][3]string{
				{"POST", `[{"service":"s3","probability":0.5},{"service":"s3"}]`, ""},
				{"DELETE", `[{"service":"s3"}]`, ""},
			},
			want: `[{"service":"s3","probability":0.5}]`,
		},
		{
			name:  "empty_disables",
			steps: [][3]string{{"POST", `[{}]`, ""}, {"POST", `[]`, ""}},
			want:  `[]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, _ := lsServer(t)

			for _, s := range tt.steps {
				require.Equal(t, http.StatusOK, lsCall(e, s[0], "/_localstack/chaos/faults", s[1]).Code)
			}

			rec := lsCall(e, http.MethodGet, "/_localstack/chaos/faults", "")
			assert.JSONEq(t, tt.want, rec.Body.String())
		})
	}
}

func TestLocalStackChaosValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, method, path, body string
	}{
		{"probability_high", "POST", "/faults", `[{"probability":1.5}]`},
		{"probability_negative", "PATCH", "/faults", `[{"probability":-0.1}]`},
		{"status_out_of_range", "POST", "/faults", `[{"error":{"statusCode":99}}]`},
		{"bad_json", "POST", "/faults", `nope`},
		{"negative_latency", "POST", "/effects", `{"latency":-1}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, store := lsServer(t)
			rec := lsCall(e, tt.method, "/_localstack/chaos"+tt.path, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Empty(t, store.GetRules())
			assert.Zero(t, store.GetEffects().Latency)
		})
	}
}

func TestLocalStackChaosEffects(t *testing.T) {
	t.Parallel()

	e, store := lsServer(t)
	require.Equal(t, http.StatusOK, lsCall(e, "POST", "/_localstack/chaos/effects", `{"latency":7}`).Code)
	assert.Equal(t, 7, store.GetEffects().Latency)
	assert.JSONEq(t, `{"latency":7}`, lsCall(e, "GET", "/_localstack/chaos/effects", "").Body.String())
}

func TestLocalStackChaosInjectsPerProtocol(t *testing.T) {
	t.Parallel()

	tests := []struct {
		headers  map[string]string
		name     string
		service  string
		wantBody string
		wantType string
	}{
		{
			name: "json", service: "kinesis", headers: map[string]string{"X-Amz-Target": "Kinesis_20131202.PutRecord"},
			wantType: "application/x-amz-json-1.0", wantBody: `"__type":"Boom"`,
		},
		{
			name: "json11", service: "kinesis",
			headers:  map[string]string{"X-Amz-Target": "X.PutRecord", "Content-Type": "application/x-amz-json-1.1"},
			wantType: "application/x-amz-json-1.1", wantBody: `"__type":"Boom"`,
		},
		{
			name: "query", service: "sns",
			headers:  map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
			wantType: "application/xml", wantBody: "<ErrorResponse><Error><Type>Sender</Type><Code>Boom</Code>",
		},
		{name: "s3_xml", service: "s3", wantType: "application/xml", wantBody: "<Error><Code>Boom</Code>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, _ := lsServer(t)
			body := `[{"service":"` + tt.service + `","error":{"statusCode":429,"code":"Boom"}}]`
			require.Equal(t, http.StatusOK, lsCall(e, "POST", "/_localstack/chaos/faults", body).Code)

			req := httptest.NewRequest(http.MethodPost, "/aws/x", nil)
			req.Header.Set("Authorization", buildSigV4Auth(tt.service, "us-east-1"))

			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusTooManyRequests, rec.Code)
			assert.Contains(t, rec.Header().Get("Content-Type"), tt.wantType)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}
