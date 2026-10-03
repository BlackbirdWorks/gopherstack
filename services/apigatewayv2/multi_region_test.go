package apigatewayv2_test

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

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

const mrHome = "us-east-1"

func newRegionHandler() *apigatewayv2.Handler {
	h := apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend())
	h.EnableRegions()

	return h
}

func regionCall(t *testing.T, h *apigatewayv2.Handler, region, method, path, body string) map[string]any {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Less(t, rec.Code, http.StatusMultipleChoices, rec.Body.String())

	out := map[string]any{}
	if rec.Body.Len() > 0 && json.Unmarshal(rec.Body.Bytes(), &out) != nil {
		out["raw"] = rec.Body.String()
	}

	return out
}

func apiNames(t *testing.T, h *apigatewayv2.Handler, region string) []string {
	t.Helper()

	items, _ := regionCall(t, h, region, http.MethodGet, "/v2/apis", "")["items"].([]any)

	names := make([]string, 0, len(items))

	for _, v := range items {
		m, _ := v.(map[string]any)
		n, _ := m["name"].(string)
		names = append(names, n)
	}

	return names
}

func TestHandler_MultiRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
	}{
		{name: "two-regions", regions: []string{"us-east-1", "eu-west-1"}},
		{name: "three-regions", regions: []string{"us-east-1", "eu-west-1", "ap-south-1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler()

			for _, r := range tc.regions {
				regionCall(t, h, r, http.MethodPost, "/v2/apis", `{"name":"shared","protocolType":"HTTP"}`)
				api := regionCall(t, h, r, http.MethodPost, "/v2/apis", `{"name":"only-`+r+`","protocolType":"HTTP"}`)
				assert.Contains(t, api["apiEndpoint"], ".execute-api."+r+".")
			}

			for _, r := range tc.regions {
				assert.ElementsMatch(t, []string{"shared", "only-" + r}, apiNames(t, h, r))
			}
		})
	}
}

func TestHandler_InvokeResolvesAPIInOwningRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "home", region: mrHome},
		{name: "peer", region: "eu-west-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var payload []byte

			h := newRegionHandler()
			h.SetLambdaInvoker(
				&mockLambdaInvoker{fn: func(_ context.Context, _, _ string, p []byte) ([]byte, int, error) {
					payload = p

					return []byte(`{"statusCode":200,"body":"ok"}`), http.StatusOK, nil
				}},
			)

			api := regionCall(t, h, tc.region, http.MethodPost, "/v2/apis", `{"name":"q","protocolType":"HTTP"}`)
			id, _ := api["apiId"].(string)
			require.NotEmpty(t, id)

			integ := regionCall(t, h, tc.region, http.MethodPost, "/v2/apis/"+id+"/integrations",
				`{"integrationType":"AWS_PROXY","payloadFormatVersion":"2.0",`+
					`"integrationUri":"arn:aws:lambda:`+tc.region+`:000000000000:function:f"}`)
			iid, _ := integ["integrationId"].(string)
			require.NotEmpty(t, iid)

			regionCall(t, h, tc.region, http.MethodPost, "/v2/apis/"+id+"/routes",
				`{"routeKey":"$default","target":"integrations/`+iid+`"}`)
			regionCall(t, h, tc.region, http.MethodPost, "/v2/apis/"+id+"/stages",
				`{"stageName":"$default","autoDeploy":true}`)

			req := httptest.NewRequest(http.MethodGet, "/v2proxy/"+id+"/$default/hello", nil)
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Contains(t, string(payload), id+".execute-api."+tc.region+".amazonaws.com")

			miss := httptest.NewRecorder()
			missReq := httptest.NewRequest(http.MethodGet, "/v2proxy/nosuchapi/$default/hello", nil)
			require.NoError(t, h.Handler()(echo.New().NewContext(missReq, miss)))
			assert.Equal(t, http.StatusNotFound, miss.Code)
		})
	}
}

func TestHandler_MultiRegionPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		remote bool
	}{
		{name: "home-only-snapshot-unchanged"},
		{name: "peer-round-trips", remote: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newRegionHandler()
			regionCall(t, src, mrHome, http.MethodPost, "/v2/apis", `{"name":"home","protocolType":"HTTP"}`)

			if tc.remote {
				regionCall(t, src, "eu-west-1", http.MethodPost, "/v2/apis", `{"name":"eu","protocolType":"HTTP"}`)
			}

			snap := src.Snapshot(context.Background())
			require.NotNil(t, snap)
			assert.Equal(t, tc.remote, strings.Contains(string(snap), `"regions"`))

			dst := newRegionHandler()
			require.NoError(t, dst.Restore(context.Background(), snap))
			assert.Equal(t, []string{"home"}, apiNames(t, dst, mrHome))
			assert.Equal(t, tc.remote, len(apiNames(t, dst, "eu-west-1")) == 1)

			old := apigatewayv2.NewInMemoryBackend()
			require.NoError(t, old.Restore(context.Background(), snap))
		})
	}
}
