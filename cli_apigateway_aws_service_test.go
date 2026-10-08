package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	kintypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPIGatewayAWSServiceIntegration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verify func(t *testing.T, fx *sfnFixture)
		setup  func(t *testing.T, fx *sfnFixture)
		name   string
		uri    string
		body   string
	}{
		{
			name: "dynamodb_putitem",
			uri:  "arn:aws:apigateway:us-east-1:dynamodb:action/PutItem",
			body: `{"TableName":"agw","Item":{"id":{"S":"k1"},"v":{"S":"hello"}}}`,
			setup: func(t *testing.T, fx *sfnFixture) {
				t.Helper()

				_, err := dynamodb.NewFromConfig(fx.cfg).CreateTable(t.Context(), &dynamodb.CreateTableInput{
					TableName:   aws.String("agw"),
					BillingMode: ddbtypes.BillingModePayPerRequest,
					KeySchema: []ddbtypes.KeySchemaElement{
						{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
					},
					AttributeDefinitions: []ddbtypes.AttributeDefinition{
						{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
					},
				})
				require.NoError(t, err)
			},
			verify: func(t *testing.T, fx *sfnFixture) {
				t.Helper()

				out, err := dynamodb.NewFromConfig(fx.cfg).GetItem(t.Context(), &dynamodb.GetItemInput{
					TableName: aws.String("agw"),
					Key:       map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "k1"}},
				})
				require.NoError(t, err)
				assert.Equal(t, "hello", out.Item["v"].(*ddbtypes.AttributeValueMemberS).Value)
			},
		},
		{
			name: "kinesis_putrecord",
			uri:  "arn:aws:apigateway:us-east-1:kinesis:action/PutRecord",
			body: `{"StreamName":"agw-stream","PartitionKey":"p","Data":"aGk="}`,
			setup: func(t *testing.T, fx *sfnFixture) {
				t.Helper()

				_, err := kinesis.NewFromConfig(fx.cfg).CreateStream(t.Context(), &kinesis.CreateStreamInput{
					StreamName: aws.String("agw-stream"), ShardCount: aws.Int32(1),
				})
				require.NoError(t, err)

				require.Eventually(t, func() bool {
					out, descErr := kinesis.NewFromConfig(fx.cfg).DescribeStreamSummary(
						t.Context(), &kinesis.DescribeStreamSummaryInput{StreamName: aws.String("agw-stream")},
					)

					return descErr == nil && out.StreamDescriptionSummary.StreamStatus == kintypes.StreamStatusActive
				}, 10*time.Second, 20*time.Millisecond)
			},
			verify: func(t *testing.T, fx *sfnFixture) {
				t.Helper()

				kc := kinesis.NewFromConfig(fx.cfg)
				shards, err := kc.ListShards(
					t.Context(),
					&kinesis.ListShardsInput{StreamName: aws.String("agw-stream")},
				)
				require.NoError(t, err)
				require.NotEmpty(t, shards.Shards)

				it, err := kc.GetShardIterator(t.Context(), &kinesis.GetShardIteratorInput{
					StreamName: aws.String("agw-stream"), ShardId: shards.Shards[0].ShardId,
					ShardIteratorType: kintypes.ShardIteratorTypeTrimHorizon,
				})
				require.NoError(t, err)

				recs, err := kc.GetRecords(t.Context(), &kinesis.GetRecordsInput{ShardIterator: it.ShardIterator})
				require.NoError(t, err)
				require.Len(t, recs.Records, 1)
				assert.Equal(t, "hi", string(recs.Records[0].Data))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			tt.setup(t, fx)

			ag := apigateway.NewFromConfig(fx.cfg)
			api, err := ag.CreateRestApi(t.Context(), &apigateway.CreateRestApiInput{Name: aws.String("a")})
			require.NoError(t, err)

			res, err := ag.GetResources(t.Context(), &apigateway.GetResourcesInput{RestApiId: api.Id})
			require.NoError(t, err)

			_, err = ag.PutMethod(t.Context(), &apigateway.PutMethodInput{
				RestApiId: api.Id, ResourceId: res.Items[0].Id, HttpMethod: aws.String("POST"),
				AuthorizationType: aws.String("NONE"),
			})
			require.NoError(t, err)

			_, err = ag.PutIntegration(t.Context(), &apigateway.PutIntegrationInput{
				RestApiId: api.Id, ResourceId: res.Items[0].Id, HttpMethod: aws.String("POST"),
				Type: apigwtypes.IntegrationTypeAws, IntegrationHttpMethod: aws.String("POST"),
				Uri: aws.String(tt.uri),
			})
			require.NoError(t, err)

			_, err = ag.CreateDeployment(t.Context(), &apigateway.CreateDeploymentInput{
				RestApiId: api.Id, StageName: aws.String("prod"),
			})
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/proxy/"+*api.Id+"/prod/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			fx.handler.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			tt.verify(t, fx)
		})
	}
}
