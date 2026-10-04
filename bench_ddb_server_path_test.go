package main

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	dynamodbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
)

const (
	ddbBenchSmall = "bench-ddb-small"
	ddbBenchBig   = "bench-ddb-big"
	ddbBenchBlob  = 380_000
)

//nolint:gochecknoglobals // one-time fixtures shared across benchmark calibration runs
var ddbBenchFixtureOnce sync.Once

func ddbBenchCreateTable(
	ctx context.Context,
	b *testing.B,
	db dynamodbbackend.StorageBackend,
	name string,
	rangeKey bool,
) {
	b.Helper()

	ks := []ddbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash}}
	ad := []ddbtypes.AttributeDefinition{
		{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
	}

	if rangeKey {
		ks = append(ks, ddbtypes.KeySchemaElement{AttributeName: aws.String("sk"), KeyType: ddbtypes.KeyTypeRange})
		ad = append(ad, ddbtypes.AttributeDefinition{
			AttributeName: aws.String("sk"), AttributeType: ddbtypes.ScalarAttributeTypeS,
		})
	}

	_, err := db.CreateTable(ctx, &sdkdynamodb.CreateTableInput{
		TableName: aws.String(name), KeySchema: ks, AttributeDefinitions: ad,
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(b, err)
}

func ddbBenchSeed(b *testing.B, byName map[string]service.Registerable) {
	b.Helper()

	ddbBenchFixtureOnce.Do(func() {
		ctx := context.Background()

		ddbH, ok := byName["DynamoDB"].(*dynamodbbackend.DynamoDBHandler)
		require.True(b, ok)

		ddbBenchCreateTable(ctx, b, ddbH.Backend, ddbBenchSmall, true)
		ddbBenchCreateTable(ctx, b, ddbH.Backend, ddbBenchBig, false)

		for i := range 1000 {
			_, err := ddbH.Backend.PutItem(ctx, &sdkdynamodb.PutItemInput{
				TableName: aws.String(ddbBenchSmall),
				Item: map[string]ddbtypes.AttributeValue{
					"pk":   &ddbtypes.AttributeValueMemberS{Value: "cust#" + strconv.Itoa(i%10)},
					"sk":   &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("order#%05d", i)},
					"val":  &ddbtypes.AttributeValueMemberN{Value: strconv.Itoa(i)},
					"name": &ddbtypes.AttributeValueMemberS{Value: "customer name " + strconv.Itoa(i)},
					"tags": &ddbtypes.AttributeValueMemberSS{Value: []string{"a", "b", "c"}},
					"meta": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{
						"ok": &ddbtypes.AttributeValueMemberBOOL{Value: true},
						"n":  &ddbtypes.AttributeValueMemberN{Value: "1.5"},
					}},
				},
			})
			require.NoError(b, err)
		}

		for i := range 4 {
			_, err := ddbH.Backend.PutItem(ctx, &sdkdynamodb.PutItemInput{
				TableName: aws.String(ddbBenchBig),
				Item:      ddbBenchBigItem("big#" + strconv.Itoa(i)),
			})
			require.NoError(b, err)
		}
	})
}

func ddbBenchBigItem(pk string) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		"pk":   &ddbtypes.AttributeValueMemberS{Value: pk},
		"blob": &ddbtypes.AttributeValueMemberS{Value: strings.Repeat("x", ddbBenchBlob)},
	}
}

func ddbBenchPutBatchBody(n int) string {
	var sb strings.Builder

	sb.WriteString(`{"RequestItems":{"` + ddbBenchSmall + `":[`)

	for i := range n {
		if i > 0 {
			sb.WriteByte(',')
		}

		fmt.Fprintf(&sb, `{"PutRequest":{"Item":{"pk":{"S":"batch#%d"},"sk":{"S":"s%d"},"val":{"N":"%d"}}}}`, i, i, i)
	}

	sb.WriteString(`]}}`)

	return sb.String()
}

func BenchmarkDynamoDBServerPath(b *testing.B) {
	e, byName := benchServer(b)
	ddbBenchSeed(b, byName)

	bigBody := strings.Repeat("y", ddbBenchBlob)
	pre := "DynamoDB_20120810."

	cases := []struct{ name, target, body string }{
		{"getitem", pre + "GetItem",
			`{"TableName":"` + ddbBenchSmall + `","Key":{"pk":{"S":"cust#1"},"sk":{"S":"order#00001"}}}`},
		{"getitem_400kb", pre + "GetItem",
			`{"TableName":"` + ddbBenchBig + `","Key":{"pk":{"S":"big#1"}}}`},
		{"putitem", pre + "PutItem",
			`{"TableName":"` + ddbBenchSmall + `","Item":{"pk":{"S":"put#1"},"sk":{"S":"x"},"val":{"N":"7"},` +
				`"name":{"S":"alice <&> bob"},"tags":{"SS":["a","b"]},"meta":{"M":{"ok":{"BOOL":true}}}}}`},
		{"putitem_400kb", pre + "PutItem",
			`{"TableName":"` + ddbBenchBig + `","Item":{"pk":{"S":"put#big"},"blob":{"S":"` + bigBody + `"}}}`},
		{"query_100", pre + "Query",
			`{"TableName":"` + ddbBenchSmall + `","KeyConditionExpression":"pk = :pk",` +
				`"ExpressionAttributeValues":{":pk":{"S":"cust#3"}}}`},
		{"query_filter", pre + "Query",
			`{"TableName":"` + ddbBenchSmall + `","KeyConditionExpression":"pk = :pk AND begins_with(sk, :p)",` +
				`"FilterExpression":"val > :v","ExpressionAttributeValues":{":pk":{"S":"cust#3"},` +
				`":p":{"S":"order#"},":v":{"N":"500"}}}`},
		{"scan_page_100", pre + "Scan", `{"TableName":"` + ddbBenchSmall + `","Limit":100}`},
		{"scan_filter", pre + "Scan",
			`{"TableName":"` + ddbBenchSmall + `","FilterExpression":"val BETWEEN :a AND :b",` +
				`"ExpressionAttributeValues":{":a":{"N":"100"},":b":{"N":"200"}}}`},
		{"scan_400kb", pre + "Scan", `{"TableName":"` + ddbBenchBig + `"}`},
		{"updateitem", pre + "UpdateItem",
			`{"TableName":"` + ddbBenchSmall + `","Key":{"pk":{"S":"cust#1"},"sk":{"S":"order#00001"}},` +
				`"UpdateExpression":"SET val = val + :inc ADD cnt :inc","ExpressionAttributeValues":{":inc":{"N":"1"}},` +
				`"ReturnValues":"ALL_NEW"}`},
		{"batchwrite_25", pre + "BatchWriteItem", ddbBenchPutBatchBody(25)},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()

			for range b.N {
				req, err := http.NewRequestWithContext(
					b.Context(), http.MethodPost, benchEndpoint+"/", strings.NewReader(tc.body),
				)
				if err != nil {
					b.Fatal(err)
				}

				req.Header.Set("Authorization", strings.Replace(benchAuth, "%s", "dynamodb", 1))
				req.Header.Set("X-Amz-Date", "20260101T000000Z")
				req.Header.Set("Content-Type", "application/x-amz-json-1.0")
				req.Header.Set("X-Amz-Target", tc.target)

				w := &discardWriter{h: make(http.Header)}
				e.ServeHTTP(w, req)

				if w.code >= http.StatusBadRequest {
					b.Fatalf("status %d", w.code)
				}
			}
		})
	}
}
