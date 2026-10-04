package lambda_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

const (
	mrHome    = "us-east-1"
	mrAccount = "000000000000"
	mrEU      = "eu-west-1"
)

func newRegionHandler(t *testing.T) *lambda.Handler {
	t.Helper()

	b := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), mrAccount, mrHome)
	h := lambda.NewHandler(b)
	h.DefaultRegion = mrHome
	h.AccountID = mrAccount
	h.EnableRegions()

	t.Cleanup(func() {
		h.CloseRegions(context.Background())
		b.Close(context.Background())
	})

	return h
}

func regionCtx(region string) context.Context {
	return awsmeta.Set(context.Background(), &awsmeta.Metadata{Region: region, Account: mrAccount})
}

func regionCall(t *testing.T, h *lambda.Handler, region, method, path string, body any) (int, []byte) {
	t.Helper()

	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}

	req := httptest.NewRequest(method, path, &buf).WithContext(regionCtx(region))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

	return rec.Code, rec.Body.Bytes()
}

func createRegionFunction(t *testing.T, h *lambda.Handler, region, name string) string {
	t.Helper()

	code, body := regionCall(t, h, region, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": name,
		"PackageType":  "Zip",
		"Runtime":      "nodejs20.x",
		"Handler":      "index.handler",
		"Role":         "arn:aws:iam::000000000000:role/r",
		"Code":         map[string]any{"ZipFile": []byte("PK")},
	})
	require.Equal(t, http.StatusCreated, code, string(body))

	var out struct {
		FunctionArn string `json:"FunctionArn"`
	}

	require.NoError(t, json.Unmarshal(body, &out))

	return out.FunctionArn
}

func regionFunctionNames(t *testing.T, h *lambda.Handler, region string) []string {
	t.Helper()

	code, body := regionCall(t, h, region, http.MethodGet, "/2015-03-31/functions", nil)
	require.Equal(t, http.StatusOK, code, string(body))

	var out struct {
		Functions []struct {
			FunctionName string `json:"FunctionName"`
			FunctionArn  string `json:"FunctionArn"`
		} `json:"Functions"`
	}

	require.NoError(t, json.Unmarshal(body, &out))

	names := make([]string, 0, len(out.Functions))

	for _, f := range out.Functions {
		assert.Contains(t, f.FunctionArn, ":"+region+":")
		names = append(names, f.FunctionName)
	}

	return names
}

func TestHandler_MultiRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
	}{
		{name: "two-regions", regions: []string{mrHome, mrEU}},
		{name: "three-regions", regions: []string{mrHome, mrEU, "ap-south-1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler(t)

			for _, r := range tc.regions {
				createRegionFunction(t, h, r, "shared")
				createRegionFunction(t, h, r, "only-"+r)
			}

			for _, r := range tc.regions {
				assert.ElementsMatch(t, []string{"shared", "only-" + r}, regionFunctionNames(t, h, r))
			}
		})
	}
}

func TestHandler_MultiRegionVersionsAliasesAndPermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
		other  string
	}{
		{name: "home-then-eu", region: mrHome, other: mrEU},
		{name: "eu-then-home", region: mrEU, other: mrHome},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler(t)
			createRegionFunction(t, h, tc.region, "fn")
			createRegionFunction(t, h, tc.other, "fn")

			code, body := regionCall(
				t, h, tc.region, http.MethodPost, "/2015-03-31/functions/fn/versions", map[string]any{})
			require.Equal(t, http.StatusCreated, code, string(body))

			code, body = regionCall(t, h, tc.region, http.MethodPost, "/2015-03-31/functions/fn/aliases",
				map[string]any{"Name": "live", "FunctionVersion": "1"})
			require.Equal(t, http.StatusCreated, code, string(body))

			code, body = regionCall(t, h, tc.region, http.MethodPost, "/2015-03-31/functions/fn/policy", map[string]any{
				"StatementId": "s1", "Action": "lambda:InvokeFunction", "Principal": "s3.amazonaws.com",
			})
			require.Equal(t, http.StatusCreated, code, string(body))

			code, _ = regionCall(t, h, tc.other, http.MethodGet, "/2015-03-31/functions/fn/aliases/live", nil)
			assert.Equal(t, http.StatusNotFound, code)

			code, _ = regionCall(t, h, tc.other, http.MethodGet, "/2015-03-31/functions/fn/policy", nil)
			assert.Equal(t, http.StatusNotFound, code)

			code, body = regionCall(t, h, tc.region, http.MethodGet, "/2015-03-31/functions/fn/aliases/live", nil)
			require.Equal(t, http.StatusOK, code, string(body))
			assert.Contains(t, string(body), ":"+tc.region+":")
		})
	}
}

func TestHandler_MultiRegionLayers(t *testing.T) {
	t.Parallel()

	h := newRegionHandler(t)

	for _, r := range []string{mrHome, mrEU} {
		code, body := regionCall(t, h, r, http.MethodPost, "/2018-10-31/layers/shared/versions", map[string]any{
			"Content": map[string]any{"ZipFile": []byte("PK")}, "CompatibleRuntimes": []string{"nodejs20.x"},
		})
		require.Equal(t, http.StatusCreated, code, string(body))
		assert.Contains(t, string(body), ":"+r+":"+mrAccount+":layer:shared:1")
	}

	code, body := regionCall(t, h, mrEU, http.MethodGet, "/2018-10-31/layers", nil)
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, string(body), ":"+mrEU+":")
	assert.NotContains(t, string(body), ":"+mrHome+":")
}

func TestHandler_MultiRegionBackendFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
		home   bool
	}{
		{name: "home", region: mrHome, home: true},
		{name: "empty-is-home", region: "", home: true},
		{name: "sibling", region: mrEU},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler(t)
			assert.Equal(t, tc.home, h.BackendFor(tc.region) == h.Backend)
			assert.Equal(t, tc.home, h.RegionHandler(tc.region) == h)
			assert.Len(t, h.RegionBackends(), map[bool]int{true: 1, false: 2}[tc.home])
		})
	}
}

func TestBackend_InvokeResolvesFunctionByARNRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		target  func(euARN, homeARN string) string
		ctxReg  string
		wantErr bool
	}{
		{name: "eu-arn", target: func(eu, _ string) string { return eu }},
		{name: "home-arn", target: func(_, home string) string { return home }},
		{name: "eu-name-eu-context", target: func(_, _ string) string { return "only-eu" }, ctxReg: mrEU},
		{
			name: "eu-name-home-context", target: func(_, _ string) string { return "only-eu" },
			ctxReg: mrHome, wantErr: true,
		},
		{name: "home-arn-for-eu-function", target: func(_, _ string) string {
			return "arn:aws:lambda:" + mrHome + ":" + mrAccount + ":function:only-eu"
		}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler(t)
			euARN := createRegionFunction(t, h, mrEU, "only-eu")
			homeARN := createRegionFunction(t, h, mrHome, "only-home")

			home, ok := h.Backend.(*lambda.InMemoryBackend)
			require.True(t, ok)

			_, status, err := home.InvokeFunction(
				regionCtx(tc.ctxReg), tc.target(euARN, homeARN), lambda.InvocationTypeDryRun, nil)

			if tc.wantErr {
				require.ErrorIs(t, err, lambda.ErrFunctionNotFound)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, http.StatusNoContent, status)
		})
	}
}

func TestHandler_MultiRegionTagsByARN(t *testing.T) {
	t.Parallel()

	h := newRegionHandler(t)
	euARN := createRegionFunction(t, h, mrEU, "fn")
	createRegionFunction(t, h, mrHome, "fn")

	require.NoError(t, h.TagFunctionByARN(context.Background(), euARN, map[string]string{"k": "v"}))

	eu := h.TaggedFunctions(regionCtx(mrEU))
	require.Len(t, eu, 1)
	assert.Equal(t, map[string]string{"k": "v"}, eu[0].Tags)

	home := h.TaggedFunctions(regionCtx(mrHome))
	require.Len(t, home, 1)
	assert.Empty(t, home[0].Tags)
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

			src := newRegionHandler(t)
			createRegionFunction(t, src, mrHome, "home")

			if tc.remote {
				createRegionFunction(t, src, mrEU, "eu")
			}

			snap := src.Snapshot(context.Background())
			require.NotNil(t, snap)

			var doc map[string]json.RawMessage

			require.NoError(t, json.Unmarshal(snap, &doc))

			_, hasRegions := doc["regions"]
			assert.Equal(t, tc.remote, hasRegions)

			dst := newRegionHandler(t)
			require.NoError(t, dst.Restore(context.Background(), snap))
			assert.Equal(t, []string{"home"}, regionFunctionNames(t, dst, mrHome))

			if tc.remote {
				assert.Equal(t, []string{"eu"}, regionFunctionNames(t, dst, mrEU))
			} else {
				assert.Empty(t, regionFunctionNames(t, dst, mrEU))
			}

			legacy, ok := newRegionHandler(t).Backend.(*lambda.InMemoryBackend)
			require.True(t, ok)
			require.NoError(t, legacy.Restore(context.Background(), snap))

			fns := legacy.ListFunctions("", 0)
			require.Len(t, fns.Data, 1)
			assert.Equal(t, "home", fns.Data[0].FunctionName)
		})
	}
}
