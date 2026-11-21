package app

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/appsync"
	astypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	sdkddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const asDLSchema = `
type Item { id: ID! title: String _version: Int }
type SyncOut { items: [Item] nextToken: String startedAt: Float }
type Query { syncItems(lastSync: Float, idx: String, base: String): SyncOut }
type Mutation {
  putItem(id: ID!, title: String, tenant: String, populate: Boolean): Item
  putBadLayout(id: ID!): Item
}`

type asDLFixture struct {
	fx    *sfnFixture
	ddb   *sdkddb.Client
	apiID string
	key   string
}

func newASDLFixture(t *testing.T) *asDLFixture {
	t.Helper()

	fx := newSFNFixture(t)
	ctx := t.Context()
	ddb := sdkddb.NewFromConfig(fx.cfg)
	as := appsync.NewFromConfig(fx.cfg)
	str := ddbsdktypes.ScalarAttributeTypeS

	hash := func(name string) ddbsdktypes.KeySchemaElement {
		return ddbsdktypes.KeySchemaElement{AttributeName: aws.String(name), KeyType: ddbsdktypes.KeyTypeHash}
	}
	rng := func(name string) ddbsdktypes.KeySchemaElement {
		return ddbsdktypes.KeySchemaElement{AttributeName: aws.String(name), KeyType: ddbsdktypes.KeyTypeRange}
	}
	def := func(name string) ddbsdktypes.AttributeDefinition {
		return ddbsdktypes.AttributeDefinition{AttributeName: aws.String(name), AttributeType: str}
	}

	_, err := ddb.CreateTable(ctx, &sdkddb.CreateTableInput{
		TableName:   aws.String("items"),
		BillingMode: ddbsdktypes.BillingModePayPerRequest,
		KeySchema: []ddbsdktypes.KeySchemaElement{
			hash("id"),
		},
		AttributeDefinitions: []ddbsdktypes.AttributeDefinition{def("id")},
	})
	require.NoError(t, err)

	_, err = ddb.CreateTable(ctx, &sdkddb.CreateTableInput{
		TableName: aws.String("items-delta"), BillingMode: ddbsdktypes.BillingModePayPerRequest,
		KeySchema: []ddbsdktypes.KeySchemaElement{hash("ds_pk"), rng("ds_sk")},
		AttributeDefinitions: []ddbsdktypes.AttributeDefinition{
			def("ds_pk"), def("ds_sk"), def("gsi_ds_pk"), def("gsi_ds_sk"),
		},
		GlobalSecondaryIndexes: []ddbsdktypes.GlobalSecondaryIndex{{
			IndexName:  aws.String("delta-index"),
			KeySchema:  []ddbsdktypes.KeySchemaElement{hash("gsi_ds_pk"), rng("gsi_ds_sk")},
			Projection: &ddbsdktypes.Projection{ProjectionType: ddbsdktypes.ProjectionTypeAll},
		}},
	})
	require.NoError(t, err)

	api, err := as.CreateGraphqlApi(ctx, &appsync.CreateGraphqlApiInput{
		Name: aws.String("dl"), AuthenticationType: astypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)

	key, err := as.CreateApiKey(ctx, &appsync.CreateApiKeyInput{ApiId: api.GraphqlApi.ApiId})
	require.NoError(t, err)

	_, err = as.StartSchemaCreation(ctx, &appsync.StartSchemaCreationInput{
		ApiId: api.GraphqlApi.ApiId, Definition: []byte(asDLSchema),
	})
	require.NoError(t, err)

	_, err = as.CreateDataSource(ctx, &appsync.CreateDataSourceInput{
		ApiId: api.GraphqlApi.ApiId, Name: aws.String("items"), Type: astypes.DataSourceTypeAmazonDynamodb,
		ServiceRoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
		DynamodbConfig: &astypes.DynamodbDataSourceConfig{
			TableName: aws.String("items"), AwsRegion: aws.String("us-east-1"), Versioned: true,
			DeltaSyncConfig: &astypes.DeltaSyncConfig{
				DeltaSyncTableName: aws.String("items-delta"), DeltaSyncTableTTL: 60,
			},
		},
	})
	require.NoError(t, err)

	pre := `{"version":"2018-05-29",`
	resolvers := []struct{ typeName, field, req string }{
		{
			"Mutation",
			"putItem",
			pre + `"operation":"PutItem","key":{"id":$util.dynamodb.toDynamoDBJson($ctx.args.id)},` +
				`"attributeValues":{"title":$util.dynamodb.toDynamoDBJson($ctx.args.title)},` +
				`"customPartitionKey":$util.toJson($ctx.args.tenant),"populateIndexFields":$util.toJson($ctx.args.populate)}`,
		},
		{
			"Mutation",
			"putBadLayout",
			pre + `"operation":"PutItem","key":{"id":$util.dynamodb.toDynamoDBJson($ctx.args.id)},` +
				`"customPartitionKey":42}`,
		},
		{"Query", "syncItems", pre + `"operation":"Sync","lastSync":$util.toJson($ctx.args.lastSync),` +
			`"deltaIndexName":$util.toJson($ctx.args.idx),"basePartitionKey":$util.toJson($ctx.args.base)}`},
	}

	for _, r := range resolvers {
		_, err = as.CreateResolver(ctx, &appsync.CreateResolverInput{
			ApiId: api.GraphqlApi.ApiId, TypeName: aws.String(r.typeName), FieldName: aws.String(r.field),
			DataSourceName: aws.String("items"), RequestMappingTemplate: aws.String(r.req),
		})
		require.NoError(t, err)
	}

	return &asDLFixture{fx: fx, ddb: ddb, apiID: aws.ToString(api.GraphqlApi.ApiId), key: aws.ToString(key.ApiKey.Id)}
}

func (f *asDLFixture) do(t *testing.T, query string) asGQLResponse {
	t.Helper()

	body, err := json.Marshal(map[string]any{"query": query}, jsonx.V1())
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/v1/apis/"+f.apiID+"/graphql", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", f.key)

	rec := httptest.NewRecorder()
	f.fx.handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out asGQLResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out, jsonx.V1()), rec.Body.String())

	return out
}

func (f *asDLFixture) syncIDs(t *testing.T, args string) []string {
	t.Helper()

	res := f.do(t, `query { syncItems(`+args+`) { items { id } } }`)
	require.Empty(t, res.Errors)

	out, _ := res.Data["syncItems"].(map[string]any)
	items, _ := out["items"].([]any)

	ids := make([]string, 0, len(items))

	for _, it := range items {
		m, _ := it.(map[string]any)
		ids = append(ids, m["id"].(string))
	}

	return ids
}

func TestAppSyncCustomPartitionKeyDeltaLayout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, f *asDLFixture, since int64)
		name  string
	}{
		{
			name: "custom key reshapes delta records and populates the index fields",
			check: func(t *testing.T, f *asDLFixture, _ int64) {
				t.Helper()

				out, err := f.ddb.Scan(t.Context(), &sdkddb.ScanInput{TableName: aws.String("items-delta")})
				require.NoError(t, err)
				require.Len(t, out.Items, 2)

				byID := map[string]map[string]ddbsdktypes.AttributeValue{}
				for _, it := range out.Items {
					byID[it["id"].(*ddbsdktypes.AttributeValueMemberS).Value] = it
				}

				day := time.Now().UTC().Format("2006-01-02")
				a := byID["a"]
				assert.Equal(t, "tenantA:"+day, a["ds_pk"].(*ddbsdktypes.AttributeValueMemberS).Value)
				assert.Contains(t, a["ds_sk"].(*ddbsdktypes.AttributeValueMemberS).Value, ":tenantA:")
				assert.Equal(t, "items:"+day, a["gsi_ds_pk"].(*ddbsdktypes.AttributeValueMemberS).Value)
				assert.NotContains(t, a["gsi_ds_sk"].(*ddbsdktypes.AttributeValueMemberS).Value, "tenantA")

				b := byID["b"]
				assert.Equal(t, "tenantB:"+day, b["ds_pk"].(*ddbsdktypes.AttributeValueMemberS).Value)
				assert.NotContains(t, b, "gsi_ds_pk")
			},
		},
		{
			name: "sync through the delta index reads across custom keys",
			check: func(t *testing.T, f *asDLFixture, since int64) {
				t.Helper()

				assert.Empty(t, f.syncIDs(t, `lastSync: `+itoa(since)))
				assert.Equal(t, []string{"a"}, f.syncIDs(t, `lastSync: `+itoa(since)+`, idx: "delta-index"`))
			},
		},
		{
			name: "base sync accepts basePartitionKey",
			check: func(t *testing.T, f *asDLFixture, _ int64) {
				t.Helper()

				assert.ElementsMatch(t, []string{"a", "b"}, f.syncIDs(t, `base: "id"`))
			},
		},
		{
			name: "non-string customPartitionKey is rejected",
			check: func(t *testing.T, f *asDLFixture, _ int64) {
				t.Helper()

				res := f.do(t, `mutation { putBadLayout(id: "c") { id } }`)
				require.NotEmpty(t, res.Errors)
				assert.Contains(t, res.Errors[0].Message, "customPartitionKey")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newASDLFixture(t)
			since := time.Now().UnixMilli() - 1000

			require.Empty(
				t,
				f.do(t, `mutation { putItem(id:"a", title:"x", tenant:"tenantA", populate:true) { id } }`).Errors,
			)
			require.Empty(
				t,
				f.do(t, `mutation { putItem(id:"b", title:"y", tenant:"tenantB", populate:false) { id } }`).Errors,
			)

			tt.check(t, f, since)
		})
	}
}

func itoa(n int64) string { return fmtInt(n) }

func fmtInt(n int64) string {
	b, _ := json.Marshal(n, jsonx.V1())

	return string(b)
}
