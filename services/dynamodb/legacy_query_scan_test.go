package dynamodb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// newLegacyQueryScanTestTable creates a table with partition key "pk" and
// sort key "sk", loaded with three items sharing pk="k1" and sk 1/2/3 (n
// mirrors sk), plus one unrelated item under pk="k2".
func newLegacyQueryScanTestTable(t *testing.T) (*dynamodbsdk.Client, string) {
	t.Helper()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
	ctx := t.Context()

	tableName := "legacy-query-scan-table"
	_, err := client.CreateTable(ctx, &dynamodbsdk.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: dynamodbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: dynamodbtypes.ScalarAttributeTypeN},
		},
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	items := []struct {
		pk string
		sk string
		n  string
	}{
		{"k1", "1", "1"},
		{"k1", "2", "2"},
		{"k1", "3", "3"},
		{"k2", "1", "99"},
	}
	for _, it := range items {
		_, putErr := client.PutItem(ctx, &dynamodbsdk.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]dynamodbtypes.AttributeValue{
				"pk": &dynamodbtypes.AttributeValueMemberS{Value: it.pk},
				"sk": &dynamodbtypes.AttributeValueMemberN{Value: it.sk},
				"n":  &dynamodbtypes.AttributeValueMemberN{Value: it.n},
			},
		})
		require.NoError(t, putErr)
	}

	return client, tableName
}

// TestQuery_LegacyKeyConditions proves a legacy KeyConditions query actually
// restricts results to the matching partition (and sort-key range), not the
// silently-dropped "return everything" failure mode gopherstack-yvs8 was
// filed for.
func TestQuery_LegacyKeyConditions(t *testing.T) {
	t.Parallel()

	t.Run("pk equality only", func(t *testing.T) {
		t.Parallel()

		client, tableName := newLegacyQueryScanTestTable(t)

		out, err := client.Query(t.Context(), &dynamodbsdk.QueryInput{
			TableName: aws.String(tableName),
			KeyConditions: map[string]dynamodbtypes.Condition{
				"pk": {
					ComparisonOperator: dynamodbtypes.ComparisonOperatorEq,
					AttributeValueList: []dynamodbtypes.AttributeValue{
						&dynamodbtypes.AttributeValueMemberS{Value: "k1"},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.Items, 3, "must return only k1's 3 items, not all 4 in the table")
	})

	// The map is keyed with the sort key inserted before the partition key --
	// Go map literals have no inherent order, and the KeyConditions map form
	// gives the caller no way to control iteration order at all. This is the
	// case a naive (non-reordering) translation would get wrong by treating
	// the sort-key clause as the partition-key clause.
	t.Run("sort key listed first in the map, pk equality plus sk range", func(t *testing.T) {
		t.Parallel()

		client, tableName := newLegacyQueryScanTestTable(t)

		keyConditions := map[string]dynamodbtypes.Condition{
			"sk": {
				ComparisonOperator: dynamodbtypes.ComparisonOperatorGt,
				AttributeValueList: []dynamodbtypes.AttributeValue{
					&dynamodbtypes.AttributeValueMemberN{Value: "1"},
				},
			},
			"pk": {
				ComparisonOperator: dynamodbtypes.ComparisonOperatorEq,
				AttributeValueList: []dynamodbtypes.AttributeValue{
					&dynamodbtypes.AttributeValueMemberS{Value: "k1"},
				},
			},
		}

		out, err := client.Query(t.Context(), &dynamodbsdk.QueryInput{
			TableName:     aws.String(tableName),
			KeyConditions: keyConditions,
		})
		require.NoError(t, err)
		require.Len(t, out.Items, 2, "must return k1's sk=2 and sk=3 only")

		gotSKs := make([]string, len(out.Items))
		for i, item := range out.Items {
			skVal, ok := item["sk"].(*dynamodbtypes.AttributeValueMemberN)
			require.True(t, ok)
			gotSKs[i] = skVal.Value
		}
		require.ElementsMatch(t, []string{"2", "3"}, gotSKs)
	})

	t.Run("BETWEEN sort key range", func(t *testing.T) {
		t.Parallel()

		client, tableName := newLegacyQueryScanTestTable(t)

		out, err := client.Query(t.Context(), &dynamodbsdk.QueryInput{
			TableName: aws.String(tableName),
			KeyConditions: map[string]dynamodbtypes.Condition{
				"pk": {
					ComparisonOperator: dynamodbtypes.ComparisonOperatorEq,
					AttributeValueList: []dynamodbtypes.AttributeValue{
						&dynamodbtypes.AttributeValueMemberS{Value: "k1"},
					},
				},
				"sk": {
					ComparisonOperator: dynamodbtypes.ComparisonOperatorBetween,
					AttributeValueList: []dynamodbtypes.AttributeValue{
						&dynamodbtypes.AttributeValueMemberN{Value: "1"},
						&dynamodbtypes.AttributeValueMemberN{Value: "2"},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.Items, 2, "must return only sk 1 and 2")
	})

	t.Run("partition key must use EQ", func(t *testing.T) {
		t.Parallel()

		client, tableName := newLegacyQueryScanTestTable(t)

		_, err := client.Query(t.Context(), &dynamodbsdk.QueryInput{
			TableName: aws.String(tableName),
			KeyConditions: map[string]dynamodbtypes.Condition{
				"pk": {
					ComparisonOperator: dynamodbtypes.ComparisonOperatorGt,
					AttributeValueList: []dynamodbtypes.AttributeValue{
						&dynamodbtypes.AttributeValueMemberS{Value: "k1"},
					},
				},
			},
		})
		requireValidationException(t, err)
	})

	t.Run("sort key rejects an operator outside the allowed subset", func(t *testing.T) {
		t.Parallel()

		client, tableName := newLegacyQueryScanTestTable(t)

		_, err := client.Query(t.Context(), &dynamodbsdk.QueryInput{
			TableName: aws.String(tableName),
			KeyConditions: map[string]dynamodbtypes.Condition{
				"pk": {
					ComparisonOperator: dynamodbtypes.ComparisonOperatorEq,
					AttributeValueList: []dynamodbtypes.AttributeValue{
						&dynamodbtypes.AttributeValueMemberS{Value: "k1"},
					},
				},
				"sk": {
					ComparisonOperator: dynamodbtypes.ComparisonOperatorContains,
					AttributeValueList: []dynamodbtypes.AttributeValue{
						&dynamodbtypes.AttributeValueMemberN{Value: "1"},
					},
				},
			},
		})
		requireValidationException(t, err)
	})

	t.Run("missing partition key is rejected", func(t *testing.T) {
		t.Parallel()

		client, tableName := newLegacyQueryScanTestTable(t)

		_, err := client.Query(t.Context(), &dynamodbsdk.QueryInput{
			TableName: aws.String(tableName),
			KeyConditions: map[string]dynamodbtypes.Condition{
				"sk": {
					ComparisonOperator: dynamodbtypes.ComparisonOperatorEq,
					AttributeValueList: []dynamodbtypes.AttributeValue{
						&dynamodbtypes.AttributeValueMemberN{Value: "1"},
					},
				},
			},
		})
		requireValidationException(t, err)
	})
}

// TestQuery_LegacyQueryFilter proves QueryFilter actually filters the
// key-condition-matched items, not the silently-dropped "returns everything
// the key condition matched" failure mode.
func TestQuery_LegacyQueryFilter(t *testing.T) {
	t.Parallel()

	client, tableName := newLegacyQueryScanTestTable(t)

	unfiltered, err := client.Query(t.Context(), &dynamodbsdk.QueryInput{
		TableName: aws.String(tableName),
		KeyConditions: map[string]dynamodbtypes.Condition{
			"pk": {
				ComparisonOperator: dynamodbtypes.ComparisonOperatorEq,
				AttributeValueList: []dynamodbtypes.AttributeValue{
					&dynamodbtypes.AttributeValueMemberS{Value: "k1"},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, unfiltered.Items, 3)

	filtered, err := client.Query(t.Context(), &dynamodbsdk.QueryInput{
		TableName: aws.String(tableName),
		KeyConditions: map[string]dynamodbtypes.Condition{
			"pk": {
				ComparisonOperator: dynamodbtypes.ComparisonOperatorEq,
				AttributeValueList: []dynamodbtypes.AttributeValue{
					&dynamodbtypes.AttributeValueMemberS{Value: "k1"},
				},
			},
		},
		QueryFilter: map[string]dynamodbtypes.Condition{
			"n": {
				ComparisonOperator: dynamodbtypes.ComparisonOperatorGt,
				AttributeValueList: []dynamodbtypes.AttributeValue{
					&dynamodbtypes.AttributeValueMemberN{Value: "1"},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, filtered.Items, 2, "QueryFilter n>1 must exclude the sk=1/n=1 item")
}

// TestScan_LegacyScanFilter proves ScanFilter actually excludes items -- the
// exact failure mode named in gopherstack-yvs8: a caller relying on
// ScanFilter silently got every item in the table back.
func TestScan_LegacyScanFilter(t *testing.T) {
	t.Parallel()

	client, tableName := newLegacyQueryScanTestTable(t)

	unfiltered, err := client.Scan(t.Context(), &dynamodbsdk.ScanInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)
	require.Len(t, unfiltered.Items, 4)

	filtered, err := client.Scan(t.Context(), &dynamodbsdk.ScanInput{
		TableName: aws.String(tableName),
		ScanFilter: map[string]dynamodbtypes.Condition{
			"pk": {
				ComparisonOperator: dynamodbtypes.ComparisonOperatorEq,
				AttributeValueList: []dynamodbtypes.AttributeValue{
					&dynamodbtypes.AttributeValueMemberS{Value: "k1"},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, filtered.Items, 3, "ScanFilter pk=k1 must exclude k2's item")

	t.Run("ConditionalOperator OR", func(t *testing.T) {
		t.Parallel()

		out, orErr := client.Scan(t.Context(), &dynamodbsdk.ScanInput{
			TableName:           aws.String(tableName),
			ConditionalOperator: dynamodbtypes.ConditionalOperatorOr,
			ScanFilter: map[string]dynamodbtypes.Condition{
				"pk": {
					ComparisonOperator: dynamodbtypes.ComparisonOperatorEq,
					AttributeValueList: []dynamodbtypes.AttributeValue{
						&dynamodbtypes.AttributeValueMemberS{Value: "k2"},
					},
				},
				"sk": {
					ComparisonOperator: dynamodbtypes.ComparisonOperatorEq,
					AttributeValueList: []dynamodbtypes.AttributeValue{
						&dynamodbtypes.AttributeValueMemberN{Value: "3"},
					},
				},
			},
		})
		require.NoError(t, orErr)
		require.Len(t, out.Items, 2, "OR must match k2's item and k1's sk=3 item")
	})
}

// TestLegacyQueryScanParams_MutualExclusion proves KeyConditions/QueryFilter/
// ScanFilter cannot be mixed with their modern expression equivalents in the
// same request.
func TestLegacyQueryScanParams_MutualExclusion(t *testing.T) {
	t.Parallel()

	t.Run("KeyConditions plus KeyConditionExpression", func(t *testing.T) {
		t.Parallel()

		client, tableName := newLegacyQueryScanTestTable(t)

		_, err := client.Query(t.Context(), &dynamodbsdk.QueryInput{
			TableName:              aws.String(tableName),
			KeyConditionExpression: aws.String("pk = :pk"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":pk": &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
			},
			KeyConditions: map[string]dynamodbtypes.Condition{
				"pk": {
					ComparisonOperator: dynamodbtypes.ComparisonOperatorEq,
					AttributeValueList: []dynamodbtypes.AttributeValue{
						&dynamodbtypes.AttributeValueMemberS{Value: "k1"},
					},
				},
			},
		})
		requireValidationException(t, err)
	})

	t.Run("QueryFilter plus FilterExpression", func(t *testing.T) {
		t.Parallel()

		client, tableName := newLegacyQueryScanTestTable(t)

		_, err := client.Query(t.Context(), &dynamodbsdk.QueryInput{
			TableName: aws.String(tableName),
			KeyConditions: map[string]dynamodbtypes.Condition{
				"pk": {
					ComparisonOperator: dynamodbtypes.ComparisonOperatorEq,
					AttributeValueList: []dynamodbtypes.AttributeValue{
						&dynamodbtypes.AttributeValueMemberS{Value: "k1"},
					},
				},
			},
			FilterExpression: aws.String("n > :n"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":n": &dynamodbtypes.AttributeValueMemberN{Value: "1"},
			},
			QueryFilter: map[string]dynamodbtypes.Condition{
				"n": {
					ComparisonOperator: dynamodbtypes.ComparisonOperatorGt,
					AttributeValueList: []dynamodbtypes.AttributeValue{
						&dynamodbtypes.AttributeValueMemberN{Value: "1"},
					},
				},
			},
		})
		requireValidationException(t, err)
	})

	t.Run("ScanFilter plus FilterExpression", func(t *testing.T) {
		t.Parallel()

		client, tableName := newLegacyQueryScanTestTable(t)

		_, err := client.Scan(t.Context(), &dynamodbsdk.ScanInput{
			TableName:        aws.String(tableName),
			FilterExpression: aws.String("pk = :pk"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":pk": &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
			},
			ScanFilter: map[string]dynamodbtypes.Condition{
				"pk": {
					ComparisonOperator: dynamodbtypes.ComparisonOperatorEq,
					AttributeValueList: []dynamodbtypes.AttributeValue{
						&dynamodbtypes.AttributeValueMemberS{Value: "k1"},
					},
				},
			},
		})
		requireValidationException(t, err)
	})

	t.Run("ConditionalOperator without ScanFilter", func(t *testing.T) {
		t.Parallel()

		client, tableName := newLegacyQueryScanTestTable(t)

		_, err := client.Scan(t.Context(), &dynamodbsdk.ScanInput{
			TableName:           aws.String(tableName),
			ConditionalOperator: dynamodbtypes.ConditionalOperatorAnd,
		})
		requireValidationException(t, err)
	})
}
