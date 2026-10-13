package dynamodb_test

import (
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

type metricRecorder struct {
	points []cwmetric.Point
	mu     sync.Mutex
}

func (r *metricRecorder) EmitMetric(p cwmetric.Point) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.points = append(r.points, p)

	return nil
}

func (r *metricRecorder) has(name string, dims map[string]string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, p := range r.points {
		if p.Name != name || len(p.Dimensions) != len(dims) {
			continue
		}

		match := true

		for _, d := range p.Dimensions {
			if dims[d.Name] != d.Value {
				match = false
			}
		}

		if match {
			return true
		}
	}

	return false
}

func TestMetrics_IndexAndTransactDimensions(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	db.SetDefaultRegion("us-east-1")

	rec := &metricRecorder{}
	db.SetMetricEmitter(rec)

	ctx := t.Context()
	_, err := db.CreateTable(ctx, &sdk.CreateTableInput{
		TableName: aws.String("m"),
		KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("email"), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
			IndexName:  aws.String("by-email"),
			KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("email"), KeyType: types.KeyTypeHash}},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	item := map[string]types.AttributeValue{
		"pk":    &types.AttributeValueMemberS{Value: "a"},
		"email": &types.AttributeValueMemberS{Value: "a@x"},
	}
	_, err = db.PutItem(ctx, &sdk.PutItemInput{TableName: aws.String("m"), Item: item})
	require.NoError(t, err)

	_, err = db.QueryWithContext(ctx, &sdk.QueryInput{
		TableName:                 aws.String("m"),
		IndexName:                 aws.String("by-email"),
		KeyConditionExpression:    aws.String("email = :e"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":e": &types.AttributeValueMemberS{Value: "a@x"}},
	})
	require.NoError(t, err)

	_, err = db.TransactWriteItems(ctx, &sdk.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{{
		Put: &types.Put{TableName: aws.String("m"), Item: item},
	}}})
	require.NoError(t, err)

	_, err = db.TransactGetItems(ctx, &sdk.TransactGetItemsInput{TransactItems: []types.TransactGetItem{{
		Get: &types.Get{TableName: aws.String("m"), Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "a"},
		}},
	}}})
	require.NoError(t, err)

	gsi := map[string]string{"TableName": "m", "GlobalSecondaryIndexName": "by-email"}

	assert.True(t, rec.has("ConsumedWriteCapacityUnits", gsi), "gsi write capacity")
	assert.True(t, rec.has("ConsumedReadCapacityUnits", gsi), "gsi read capacity")
	assert.True(t, rec.has("SuccessfulRequestLatency", map[string]string{
		"TableName": "m", "Operation": "TransactWriteItems",
	}), "transact write latency")
	assert.True(t, rec.has("SuccessfulRequestLatency", map[string]string{
		"TableName": "m", "Operation": "TransactGetItems",
	}), "transact get latency")
}
