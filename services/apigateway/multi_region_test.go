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

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

const mrHome = "us-east-1"

func newRegionHandler() *apigateway.Handler {
	h := apigateway.NewHandler(apigateway.NewInMemoryBackend())
	h.EnableRegions()

	return h
}

func regionCall(t *testing.T, h *apigateway.Handler, region, method, path, body string) (int, string) {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

	return rec.Code, rec.Body.String()
}

func apiNames(t *testing.T, h *apigateway.Handler, region string) []string {
	t.Helper()

	code, body := regionCall(t, h, region, http.MethodGet, "/restapis", "")
	require.Equal(t, http.StatusOK, code, body)

	var out struct {
		Item []struct {
			Name string `json:"name"`
		} `json:"item"`
	}

	require.NoError(t, json.Unmarshal([]byte(body), &out))

	names := make([]string, 0, len(out.Item))
	for _, i := range out.Item {
		names = append(names, i.Name)
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
				code, body := regionCall(t, h, r, http.MethodPost, "/restapis", `{"name":"shared"}`)
				require.Less(t, code, http.StatusMultipleChoices, body)

				code, body = regionCall(t, h, r, http.MethodPost, "/restapis", `{"name":"only-`+r+`"}`)
				require.Less(t, code, http.StatusMultipleChoices, body)

				code, body = regionCall(t, h, r, http.MethodPost, "/domainnames", `{"domainName":"`+r+`.example.com"}`)
				require.Less(t, code, http.StatusMultipleChoices, body)
				assert.Contains(t, body, ":apigateway:"+r+"::")
			}

			for _, r := range tc.regions {
				assert.ElementsMatch(t, []string{"shared", "only-" + r}, apiNames(t, h, r))
			}
		})
	}
}

func deployMock(t *testing.T, bk apigateway.StorageBackend, body string) string {
	t.Helper()

	api, err := bk.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "inv"})
	require.NoError(t, err)

	res, err := bk.CreateResource(api.ID, api.RootResourceID, "widgets")
	require.NoError(t, err)

	_, err = bk.PutMethod(apigateway.PutMethodInput{
		RestAPIID: api.ID, ResourceID: res.ID, HTTPMethod: http.MethodGet, AuthorizationType: "NONE",
	})
	require.NoError(t, err)

	_, err = bk.PutIntegration(api.ID, res.ID, http.MethodGet, apigateway.PutIntegrationInput{Type: "MOCK"})
	require.NoError(t, err)

	_, err = bk.PutIntegrationResponse(api.ID, res.ID, http.MethodGet, "200",
		apigateway.PutIntegrationResponseInput{ResponseTemplates: map[string]string{"application/json": body}})
	require.NoError(t, err)

	_, err = bk.CreateDeployment(api.ID, "prod", "")
	require.NoError(t, err)

	return api.ID
}

func TestHandler_InvokeResolvesAPIInOwningRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
		path   string
	}{
		{name: "home-user-request", region: mrHome, path: "/restapis/%s/prod/_user_request_/widgets"},
		{name: "peer-user-request", region: "eu-west-1", path: "/restapis/%s/prod/_user_request_/widgets"},
		{name: "peer-proxy", region: "eu-west-1", path: "/proxy/%s/prod/widgets"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler()
			id := deployMock(t, h.BackendFor(tc.region), "from-"+tc.region)

			code, body := regionCall(t, h, mrHome, http.MethodGet, strings.Replace(tc.path, "%s", id, 1), "")
			assert.Equal(t, http.StatusOK, code)
			assert.Equal(t, "from-"+tc.region, body)

			code, _ = regionCall(t, h, mrHome, http.MethodGet, strings.Replace(tc.path, "%s", "nosuchapi", 1), "")
			assert.Equal(t, http.StatusForbidden, code)
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
			regionCall(t, src, mrHome, http.MethodPost, "/restapis", `{"name":"home"}`)

			if tc.remote {
				regionCall(t, src, "eu-west-1", http.MethodPost, "/restapis", `{"name":"eu"}`)
			}

			snap := src.Snapshot(context.Background())
			require.NotNil(t, snap)
			assert.Equal(t, tc.remote, strings.Contains(string(snap), `"regions"`))

			dst := newRegionHandler()
			require.NoError(t, dst.Restore(context.Background(), snap))
			assert.Equal(t, []string{"home"}, apiNames(t, dst, mrHome))
			assert.Equal(t, tc.remote, len(apiNames(t, dst, "eu-west-1")) == 1)

			old := apigateway.NewInMemoryBackend()
			require.NoError(t, old.Restore(context.Background(), snap))
		})
	}
}
