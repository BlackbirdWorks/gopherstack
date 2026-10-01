package main

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sdks3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	dynamodbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const benchAuth = "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/%s/aws4_request, " +
	"SignedHeaders=host;x-amz-date, Signature=0000000000000000000000000000000000000000000000000000000000000000"

// discardWriter is a header-keeping ResponseWriter that drops the body.
type discardWriter struct {
	h    http.Header
	code int
}

func (w *discardWriter) Header() http.Header         { return w.h }
func (w *discardWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *discardWriter) WriteHeader(code int)        { w.code = code }

//nolint:gochecknoglobals // one-time fixtures shared across benchmark calibration runs
var serverFixtureOnce sync.Once

func serverFixtures(b *testing.B, byName map[string]service.Registerable) {
	b.Helper()

	serverFixtureOnce.Do(func() {
		ctx := context.Background()

		ddbH, ok := byName["DynamoDB"].(*dynamodbbackend.DynamoDBHandler)
		require.True(b, ok)

		_, err := ddbH.Backend.CreateTable(ctx, &sdkdynamodb.CreateTableInput{
			TableName: aws.String("bench-server-table"),
			KeySchema: []ddbtypes.KeySchemaElement{
				{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
			},
			AttributeDefinitions: []ddbtypes.AttributeDefinition{
				{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			},
		})
		require.NoError(b, err)

		_, err = ddbH.Backend.PutItem(ctx, &sdkdynamodb.PutItemInput{
			TableName: aws.String("bench-server-table"),
			Item:      map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "k"}},
		})
		require.NoError(b, err)

		s3H, ok := byName["S3"].(*s3backend.S3Handler)
		require.True(b, ok)

		_, err = s3H.Backend.CreateBucket(ctx, &sdks3.CreateBucketInput{Bucket: aws.String("bench-server-bucket")})
		require.NoError(b, err)

		_, err = s3H.Backend.PutObject(ctx, &sdks3.PutObjectInput{
			Bucket: aws.String("bench-server-bucket"),
			Key:    aws.String("obj.txt"),
			Body:   strings.NewReader("bench object body"),
		})
		require.NoError(b, err)
	})
}

func BenchmarkServerPath(b *testing.B) {
	e, byName := benchServer(b)
	serverFixtures(b, byName)

	const (
		json10 = "application/x-amz-json-1.0"
		form   = "application/x-www-form-urlencoded"
	)

	cases := []struct {
		name, method, path, signSvc, ctype, target, body string
	}{
		{"ddb_getitem", "POST", "/", "dynamodb", json10, "DynamoDB_20120810.GetItem",
			`{"TableName":"bench-server-table","Key":{"id":{"S":"k"}}}`},
		{"sts_query", "POST", "/", "sts", form, "", "Action=GetCallerIdentity&Version=2011-06-15"},
		{"sns_query", "POST", "/", "sns", form, "", "Action=ListTopics&Version=2010-03-31"},
		{"sqs_query", "POST", "/", "sqs", form, "", "Action=ListQueues&Version=2012-11-05"},
		{"s3_getobject", "GET", "/bench-server-bucket/obj.txt", "s3", "", "", ""},
		{"lambda_list", "GET", "/2015-03-31/functions/", "lambda", "", "", ""},
		{"apigw_restjson", "GET", "/restapis", "apigateway", "", "", ""},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()

			for range b.N {
				req, err := http.NewRequestWithContext(
					b.Context(), tc.method, benchEndpoint+tc.path, strings.NewReader(tc.body),
				)
				if err != nil {
					b.Fatal(err)
				}

				req.Header.Set("Authorization", strings.Replace(benchAuth, "%s", tc.signSvc, 1))
				req.Header.Set("X-Amz-Date", "20260101T000000Z")

				if tc.ctype != "" {
					req.Header.Set("Content-Type", tc.ctype)
				}

				if tc.target != "" {
					req.Header.Set("X-Amz-Target", tc.target)
				}

				w := &discardWriter{h: make(http.Header)}
				e.ServeHTTP(w, req)

				if w.code >= http.StatusInternalServerError {
					b.Fatalf("status %d", w.code)
				}
			}
		})
	}
}
