package appsync_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/appsync"
)

const mrHome = "us-east-1"

func newRegionHandler() *appsync.Handler {
	h := appsync.NewHandler(appsync.NewInMemoryBackend("000000000000", mrHome, "http://localhost:8000"))
	h.DefaultRegion = mrHome
	h.EnableRegions()

	return h
}

func regionDo(t *testing.T, h *appsync.Handler, region, method, path, body string) (int, map[string]any) {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

	out := map[string]any{}
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}

	return rec.Code, out
}

func regionCall(t *testing.T, h *appsync.Handler, region, method, body string) map[string]any {
	t.Helper()

	code, out := regionDo(t, h, region, method, "/v1/apis", body)
	require.Less(t, code, http.StatusMultipleChoices, out)

	return out
}

const apiBody = `{"name":"shared","authenticationType":"API_KEY"}`

func apisIn(t *testing.T, h *appsync.Handler, region string) []map[string]any {
	t.Helper()

	list, _ := regionCall(t, h, region, http.MethodGet, "")["graphqlApis"].([]any)
	out := make([]map[string]any, 0, len(list))

	for _, v := range list {
		m, _ := v.(map[string]any)
		out = append(out, m)
	}

	return out
}

func TestHandler_MultiRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
	}{
		{name: "two-regions", regions: []string{mrHome, "eu-west-1"}},
		{name: "three-regions", regions: []string{mrHome, "eu-west-1", "ap-south-1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler()

			for _, r := range tc.regions {
				regionCall(t, h, r, http.MethodPost, apiBody)
			}

			for _, r := range tc.regions {
				apis := apisIn(t, h, r)
				require.Len(t, apis, 1)

				arn, _ := apis[0]["arn"].(string)
				assert.Contains(t, arn, ":appsync:"+r+":")
			}

			assert.Len(t, h.RegionBackends(), len(tc.regions))
		})
	}
}

func TestHandler_MultiRegionGraphQLFindsOwner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createIn   string
		requestFrm string
	}{
		{name: "home-api-peer-request", createIn: mrHome, requestFrm: "eu-west-1"},
		{name: "peer-api-home-request", createIn: "eu-west-1", requestFrm: mrHome},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler()
			api, _ := regionCall(t, h, tc.createIn, http.MethodPost, apiBody)["graphqlApi"].(map[string]any)
			id, _ := api["apiId"].(string)

			wantCode, _ := regionDo(t, h, tc.createIn, http.MethodPost, "/v1/apis/"+id+"/graphql", `{"query":"{a}"}`)
			gotCode, _ := regionDo(t, h, tc.requestFrm, http.MethodPost, "/v1/apis/"+id+"/graphql", `{"query":"{a}"}`)

			assert.Equal(t, wantCode, gotCode)
			assert.NotEqual(t, http.StatusNotFound, gotCode)
		})
	}
}

func TestHandler_MultiRegionPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		extraRegion string
		wantRegions bool
	}{
		{name: "home-only-snapshot-unchanged"},
		{name: "with-sibling", extraRegion: "eu-west-1", wantRegions: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newRegionHandler()
			regionCall(t, src, mrHome, http.MethodPost, apiBody)

			if tc.extraRegion != "" {
				regionCall(t, src, tc.extraRegion, http.MethodPost, apiBody)
			}

			snap := src.Snapshot(t.Context())

			var doc map[string]json.RawMessage

			require.NoError(t, json.Unmarshal(snap, &doc))

			_, hasRegions := doc["regions"]
			assert.Equal(t, tc.wantRegions, hasRegions)

			dst := newRegionHandler()
			require.NoError(t, dst.Restore(t.Context(), snap))
			assert.Len(t, apisIn(t, dst, mrHome), 1)

			if tc.extraRegion != "" {
				apis := apisIn(t, dst, tc.extraRegion)
				require.Len(t, apis, 1)

				arn, _ := apis[0]["arn"].(string)
				assert.Contains(t, arn, ":appsync:"+tc.extraRegion+":")
			}
		})
	}
}

func TestHandler_MultiRegionLegacyRestore(t *testing.T) {
	t.Parallel()

	legacy := appsync.NewHandler(appsync.NewInMemoryBackend("000000000000", mrHome, "http://localhost:8000"))
	regionCall(t, legacy, mrHome, http.MethodPost, apiBody)

	dst := newRegionHandler()
	regionCall(t, dst, "eu-west-1", http.MethodPost, apiBody)

	require.NoError(t, dst.Restore(t.Context(), legacy.Snapshot(t.Context())))
	assert.Len(t, apisIn(t, dst, mrHome), 1)
	assert.Empty(t, apisIn(t, dst, "eu-west-1"))
}

type regionRecorder struct {
	regions []string
	mu      sync.Mutex
}

func (r *regionRecorder) record(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.regions = append(r.regions, awsmeta.Region(ctx))
}

func (r *regionRecorder) InvokeFunction(ctx context.Context, _, _ string, _ []byte) ([]byte, int, error) {
	r.record(ctx)

	return []byte(`"ok"`), http.StatusOK, nil
}

func (r *regionRecorder) GetItemRaw(ctx context.Context, _ string, _ map[string]any) (map[string]any, error) {
	r.record(ctx)

	return map[string]any{}, nil
}

func (r *regionRecorder) PutItemRaw(ctx context.Context, _ string, _ map[string]any) error {
	r.record(ctx)

	return nil
}

func TestBackend_ResolversCallOtherServicesInAPIRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		dsType     appsync.DataSourceType
		ddbRegion  string
		wantRegion string
	}{
		{name: "lambda-api-region", dsType: appsync.DataSourceTypeLambda, wantRegion: "eu-west-1"},
		{name: "dynamodb-api-region", dsType: appsync.DataSourceTypeDynamoDB, wantRegion: "eu-west-1"},
		{
			name: "dynamodb-config-region", dsType: appsync.DataSourceTypeDynamoDB,
			ddbRegion: "ap-south-1", wantRegion: "ap-south-1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := appsync.NewInMemoryBackend("000000000000", "eu-west-1", "")
			rec := &regionRecorder{}
			b.SetLambdaInvoker(rec)
			b.SetDynamoDBBackend(rec)

			api, err := b.CreateGraphqlAPI("api", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			key, err := b.CreateAPIKey(api.APIID, "", 0)
			require.NoError(t, err)

			_, err = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)
			require.NoError(t, err)

			ds := &appsync.DataSource{Name: "ds", Type: tc.dsType}
			ds.LambdaConfig = &appsync.LambdaDataSourceConfig{LambdaFunctionARN: "fn"}
			ds.DynamoDBConfig = &appsync.DynamoDBDataSourceConfig{TableName: "t", AWSRegion: tc.ddbRegion}

			_, err = b.CreateDataSource(api.APIID, ds)
			require.NoError(t, err)

			_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{FieldName: "hello", DataSourceName: "ds"})
			require.NoError(t, err)

			ctx := awsmeta.Set(t.Context(), &awsmeta.Metadata{Region: "us-east-1", Account: "000000000000"})
			_, err = b.ExecuteGraphQL(ctx, api.APIID, "query { hello }", "", nil, appsync.GraphQLAuth{APIKey: key.ID})
			require.NoError(t, err)

			rec.mu.Lock()
			defer rec.mu.Unlock()

			assert.Equal(t, []string{tc.wantRegion}, rec.regions)
		})
	}
}
