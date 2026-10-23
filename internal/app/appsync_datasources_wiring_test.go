package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/appsync"
	astypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	ebsdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	ossdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	asDSSchema = `type Query {
  eb: AWSJSON
  os(op: String, path: String, body: String): AWSJSON
  rds(q: String, n: String): AWSJSON
}`
	asDSOSReq = `{"version":"2018-05-29","operation":"$ctx.args.op","path":"$ctx.args.path",` +
		`"params":{"body":$util.toJson($ctx.args.body)}}`
	asDSRDSReq = `{"version":"2018-05-29","statements":[$util.toJson($ctx.args.q)],` +
		`"variableMap":{":n":$util.toJson($ctx.args.n)}}`
	asDSEBReq = `{"version":"2018-05-29","operation":"PutEvents","events":[` +
		`{"source":"appsync.test","detailType":"Hello","detail":{"msg":"hi"}}]}`
)

type asDSFixture struct {
	fx    *sfnFixture
	apiID string
	key   string
}

func (f *asDSFixture) addDS(t *testing.T, in *appsync.CreateDataSourceInput, field, req, resp string) {
	t.Helper()

	as := appsync.NewFromConfig(f.fx.cfg)
	in.ApiId = aws.String(f.apiID)
	in.ServiceRoleArn = aws.String("arn:aws:iam::000000000000:role/r")
	_, err := as.CreateDataSource(t.Context(), in)
	require.NoError(t, err)

	r := &appsync.CreateResolverInput{
		ApiId: aws.String(f.apiID), TypeName: aws.String("Query"), FieldName: aws.String(field),
		DataSourceName: in.Name, RequestMappingTemplate: aws.String(req),
	}
	if resp != "" {
		r.ResponseMappingTemplate = aws.String(resp)
	}

	_, err = as.CreateResolver(t.Context(), r)
	require.NoError(t, err)
}

func newASDSFixture(t *testing.T) *asDSFixture {
	t.Helper()

	fx := newSFNFixture(t)
	as := appsync.NewFromConfig(fx.cfg)

	api, err := as.CreateGraphqlApi(t.Context(), &appsync.CreateGraphqlApiInput{
		Name: aws.String("ds"), AuthenticationType: astypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)

	key, err := as.CreateApiKey(t.Context(), &appsync.CreateApiKeyInput{ApiId: api.GraphqlApi.ApiId})
	require.NoError(t, err)

	_, err = as.StartSchemaCreation(t.Context(), &appsync.StartSchemaCreationInput{
		ApiId: api.GraphqlApi.ApiId, Definition: []byte(asDSSchema),
	})
	require.NoError(t, err)

	return &asDSFixture{fx: fx, apiID: aws.ToString(api.GraphqlApi.ApiId), key: aws.ToString(key.ApiKey.Id)}
}

func (f *asDSFixture) query(t *testing.T, field string, vars map[string]any) any {
	t.Helper()

	query := "query { " + field + " }"
	if len(vars) > 0 {
		decls := make([]string, 0, len(vars))
		for k := range vars {
			decls = append(decls, "$"+k+": String")
		}

		query = "query(" + strings.Join(decls, ", ") + ") { " + field + " }"
	}

	body, err := json.Marshal(map[string]any{"query": query, "variables": vars})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/v1/apis/"+f.apiID+"/graphql", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", f.key)

	rec := httptest.NewRecorder()
	f.fx.handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out struct {
		Data   map[string]any `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out), rec.Body.String())
	require.Empty(t, out.Errors, rec.Body.String())

	name, _, _ := strings.Cut(field, "(")

	return out.Data[name]
}

func asJSONValue(t *testing.T, v any) any {
	t.Helper()

	s, ok := v.(string)
	if !ok {
		return v
	}

	var out any
	require.NoError(t, json.Unmarshal([]byte(s), &out), s)

	return out
}

func TestAppSyncEventBridgeDataSource(t *testing.T) {
	t.Parallel()

	f := newASDSFixture(t)
	ctx := t.Context()

	sq := sqssdk.NewFromConfig(f.fx.cfg)
	q, err := sq.CreateQueue(ctx, &sqssdk.CreateQueueInput{QueueName: aws.String("as-eb")})
	require.NoError(t, err)

	attrs, err := sq.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
		QueueUrl: q.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)

	ebc := ebsdk.NewFromConfig(f.fx.cfg)
	_, err = ebc.PutRule(ctx, &ebsdk.PutRuleInput{
		Name: aws.String("r"), EventPattern: aws.String(`{"source":["appsync.test"]}`),
	})
	require.NoError(t, err)

	_, err = ebc.PutTargets(ctx, &ebsdk.PutTargetsInput{
		Rule: aws.String("r"),
		Targets: []ebtypes.Target{{
			Id: aws.String("t"), Arn: aws.String(attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]),
		}},
	})
	require.NoError(t, err)

	f.addDS(t, &appsync.CreateDataSourceInput{
		Name: aws.String("EB"), Type: astypes.DataSourceTypeAmazonEventbridge,
		EventBridgeConfig: &astypes.EventBridgeDataSourceConfig{
			EventBusArn: aws.String("arn:aws:events:us-east-1:000000000000:event-bus/default"),
		},
	}, "eb", asDSEBReq, `$util.toJson($ctx.result)`)

	res, _ := asJSONValue(t, f.query(t, "eb", nil)).(map[string]any)
	assert.InDelta(t, 0, res["FailedEntryCount"], 0)
	entries, _ := res["Entries"].([]any)
	require.Len(t, entries, 1)

	var body string

	require.Eventually(t, func() bool {
		m, rerr := sq.ReceiveMessage(ctx, &sqssdk.ReceiveMessageInput{QueueUrl: q.QueueUrl})
		if rerr != nil || len(m.Messages) == 0 {
			return false
		}

		body = aws.ToString(m.Messages[0].Body)

		return true
	}, authzDeadline, authzTick)

	assert.Contains(t, body, `"source":"appsync.test"`)
	assert.Contains(t, body, `"msg":"hi"`)
}

func TestAppSyncOpenSearchDataSource(t *testing.T) {
	t.Parallel()

	f := newASDSFixture(t)

	osc := ossdk.NewFromConfig(f.fx.cfg)
	_, err := osc.CreateDomain(t.Context(), &ossdk.CreateDomainInput{DomainName: aws.String("books")})
	require.NoError(t, err)

	dom, err := osc.DescribeDomain(t.Context(), &ossdk.DescribeDomainInput{DomainName: aws.String("books")})
	require.NoError(t, err)

	f.addDS(t, &appsync.CreateDataSourceInput{
		Name: aws.String("OS"), Type: astypes.DataSourceTypeAmazonOpensearchService,
		OpenSearchServiceConfig: &astypes.OpenSearchServiceDataSourceConfig{
			Endpoint:  aws.String("https://" + aws.ToString(dom.DomainStatus.Endpoint)),
			AwsRegion: aws.String("us-east-1"),
		},
	}, "os", asDSOSReq, `$util.toJson($ctx.result)`)

	f.query(t, `os(op: $op, path: $path, body: $body)`, map[string]any{"op": "PUT", "path": "/idx", "body": ""})
	f.query(t, `os(op: $op, path: $path, body: $body)`, map[string]any{
		"op": "PUT", "path": "/idx/_doc/1", "body": `{"title":"gopher"}`,
	})

	res, _ := asJSONValue(t, f.query(t, `os(op: $op, path: $path, body: $body)`, map[string]any{
		"op": "POST", "path": "/idx/_search", "body": `{"query":{"match_all":{}}}`,
	})).(map[string]any)

	hits, _ := res["hits"].(map[string]any)
	list, _ := hits["hits"].([]any)
	require.Len(t, list, 1)

	first, _ := list[0].(map[string]any)
	assert.Equal(t, map[string]any{"title": "gopher"}, first["_source"])
}

func TestAppSyncRDSDataSource(t *testing.T) {
	t.Parallel()

	f := newASDSFixture(t)

	f.addDS(t, &appsync.CreateDataSourceInput{
		Name: aws.String("RDS"), Type: astypes.DataSourceTypeRelationalDatabase,
		RelationalDatabaseConfig: &astypes.RelationalDatabaseDataSourceConfig{
			RelationalDatabaseSourceType: astypes.RelationalDatabaseSourceTypeRdsHttpEndpoint,
			RdsHttpEndpointConfig: &astypes.RdsHttpEndpointConfig{
				AwsRegion:           aws.String("us-east-1"),
				DbClusterIdentifier: aws.String("arn:aws:rds:us-east-1:000000000000:cluster:c1"),
				AwsSecretStoreArn:   aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:s"),
				DatabaseName:        aws.String("db"),
			},
		},
	}, "rds", asDSRDSReq, `$util.toJson($util.rds.toJsonObject($ctx.result))`)

	run := func(q, n string) any {
		return asJSONValue(t, f.query(t, `rds(q: $q, n: $n)`, map[string]any{"q": q, "n": n}))
	}

	run("CREATE TABLE pets (id INTEGER PRIMARY KEY, name TEXT)", "")
	run("INSERT INTO pets (name) VALUES (:n)", "rex")

	rows, _ := run("SELECT id, name FROM pets WHERE name = :n", "rex").([]any)
	require.Len(t, rows, 1)

	recs, _ := rows[0].([]any)
	require.Len(t, recs, 1)
	assert.Equal(t, map[string]any{"id": float64(1), "name": "rex"}, recs[0])
}
