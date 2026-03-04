package dynamodb_test

import (
	"errors"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/dynamodb"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newThrottledDB creates an InMemoryDB with throughput enforcement enabled and a
// table named "tbl" provisioned with the given rcu/wcu. The token bucket is
// initialised at full capacity, so the first request will always succeed.
func newThrottledDB(t *testing.T, rcu, wcu int64) *dynamodb.InMemoryDB {
	t.Helper()

	db := dynamodb.NewInMemoryDB()
	db.SetEnforceThroughput(true)

	_, err := db.CreateTable(t.Context(), &ddbsdk.CreateTableInput{
		TableName: aws.String("tbl"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(rcu),
			WriteCapacityUnits: aws.Int64(wcu),
		},
	})
	require.NoError(t, err)

	return db
}

func TestThrottler_WriteExceedsCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wcu     int64
		numPuts int
		wantErr bool
	}{
		{
			name:    "within_capacity",
			wcu:     5,
			numPuts: 5,
			wantErr: false,
		},
		{
			name:    "exceeds_on_second_put",
			wcu:     1,
			numPuts: 2,
			wantErr: true,
		},
		{
			name:    "exceeds_on_third_put",
			wcu:     2,
			numPuts: 3,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := newThrottledDB(t, 5, tt.wcu)

			var lastErr error

			for range tt.numPuts {
				_, putErr := db.PutItem(t.Context(), &ddbsdk.PutItemInput{
					TableName: aws.String("tbl"),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "k"},
					},
				})
				if putErr != nil {
					lastErr = putErr

					break
				}
			}

			if tt.wantErr {
				require.Error(t, lastErr)
				var ddbErr *dynamodb.Error
				require.ErrorAs(t, lastErr, &ddbErr)
				assert.Contains(t, ddbErr.Type, "ProvisionedThroughputExceededException")
			} else {
				require.NoError(t, lastErr)
			}
		})
	}
}

func TestThrottler_ReadExceedsCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rcu     int64
		numGets int
		wantErr bool
	}{
		{
			name:    "within_read_capacity",
			rcu:     3,
			numGets: 3,
			wantErr: false,
		},
		{
			name:    "exceeds_read_capacity",
			rcu:     1,
			numGets: 3,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := newThrottledDB(t, tt.rcu, 5)

			// Seed a single item.
			_, err := db.PutItem(t.Context(), &ddbsdk.PutItemInput{
				TableName: aws.String("tbl"),
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "k"},
				},
			})
			require.NoError(t, err)

			// Use a separate database so that WCU consumed by the PutItem above
			// does not affect the RCU bucket used by the read test below.
			db2 := newThrottledDB(t, tt.rcu, 10)

			var lastErr error

			for range tt.numGets {
				_, lastErr = db2.GetItem(t.Context(), &ddbsdk.GetItemInput{
					TableName: aws.String("tbl"),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "k"},
					},
				})
				if lastErr != nil {
					break
				}
			}

			if tt.wantErr {
				require.Error(t, lastErr)
				var ddbErr *dynamodb.Error
				require.ErrorAs(t, lastErr, &ddbErr)
				assert.Contains(t, ddbErr.Type, "ProvisionedThroughputExceededException")
			} else {
				require.NoError(t, lastErr)
			}
		})
	}
}

func TestThrottler_Disabled(t *testing.T) {
	t.Parallel()

	// When throttling is disabled, even very low provisioned capacity must not block requests.
	db := dynamodb.NewInMemoryDB()

	_, err := db.CreateTable(t.Context(), &ddbsdk.CreateTableInput{
		TableName: aws.String("tbl"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	})
	require.NoError(t, err)

	for range 20 {
		_, err = db.PutItem(t.Context(), &ddbsdk.PutItemInput{
			TableName: aws.String("tbl"),
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "k"},
			},
		})
		require.NoError(t, err)
	}
}

func TestThrottler_TokenBucketRefill(t *testing.T) {
	t.Parallel()

	// Provision 1 WCU. First write consumes it; after 1 second the bucket refills,
	// so the second write must succeed (once we advance mock time).
	// We test this indirectly through SetEnforceThroughput on a fresh DB.
	db := dynamodb.NewInMemoryDB()
	db.SetEnforceThroughput(true)

	_, err := db.CreateTable(t.Context(), &ddbsdk.CreateTableInput{
		TableName: aws.String("tbl"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	})
	require.NoError(t, err)

	// First put succeeds.
	_, err = db.PutItem(t.Context(), &ddbsdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "k1"},
		},
	})
	require.NoError(t, err)

	// Second put immediately should fail (bucket empty).
	_, err = db.PutItem(t.Context(), &ddbsdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "k2"},
		},
	})
	require.Error(t, err)

	// Eventually, after the bucket refills to >= 1 WCU, a third put should succeed.
	require.Eventually(t, func() bool {
		_, putErr := db.PutItem(t.Context(), &ddbsdk.PutItemInput{
			TableName: aws.String("tbl"),
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "k3"},
			},
		})

		return putErr == nil
	}, 2*time.Second, 10*time.Millisecond)
}

func TestThrottler_ScanExceedsCapacity(t *testing.T) {
	t.Parallel()

	// 1 RCU provisioned. Each scan of 3 items costs 1.5 RCU, exceeding the bucket.
	db := newThrottledDB(t, 1, 10)

	// Insert 3 items; each write is within the WCU budget (10 WCU provisioned).
	for i := range 3 {
		key := string(rune('a' + i))
		_, err := db.PutItem(t.Context(), &ddbsdk.PutItemInput{
			TableName: aws.String("tbl"),
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: key},
			},
		})
		require.NoError(t, err)
	}

	// First scan may succeed (full RCU bucket = 1.0, cost = 3 * 0.5 = 1.5).
	// It may also immediately fail if the first scan costs more than 1 RCU.
	// Either way, after the first scan the RCU bucket should be exhausted.
	// Do two scans; at least one must return ProvisionedThroughputExceededException.
	var throttled bool

	for range 2 {
		_, err := db.Scan(t.Context(), &ddbsdk.ScanInput{
			TableName: aws.String("tbl"),
		})
		if err != nil {
			var ddbErr *dynamodb.Error
			if errors.As(err, &ddbErr) && ddbErr.Type != "" {
				assert.Contains(t, ddbErr.Type, "ProvisionedThroughputExceededException")
				throttled = true

				break
			}
		}
	}

	assert.True(t, throttled, "expected at least one scan to be throttled")
}

func TestThrottler_UpdateTableCapacity(t *testing.T) {
	t.Parallel()

	// Start with 1 WCU; immediately a second write should fail.
	// After UpdateTable to 10 WCU, the bucket refills at 10 WCU/s — writes
	// should succeed as soon as at least 1 WCU has accumulated.
	db := newThrottledDB(t, 5, 1)

	_, err := db.PutItem(t.Context(), &ddbsdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item:      map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "k1"}},
	})
	require.NoError(t, err)

	_, err = db.PutItem(t.Context(), &ddbsdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item:      map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "k2"}},
	})
	require.Error(t, err) // Should be throttled.

	// Increase capacity — token count is preserved (not instantly refilled),
	// but the bucket now refills at 10 WCU/s.
	newRCU := int64(5)
	newWCU := int64(10)
	_, err = db.UpdateTable(t.Context(), &ddbsdk.UpdateTableInput{
		TableName: aws.String("tbl"),
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  &newRCU,
			WriteCapacityUnits: &newWCU,
		},
	})
	require.NoError(t, err)

	// Writes should succeed once the bucket has accumulated at least 1 WCU.
	require.Eventually(t, func() bool {
		_, putErr := db.PutItem(t.Context(), &ddbsdk.PutItemInput{
			TableName: aws.String("tbl"),
			Item:      map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "k3"}},
		})

		return putErr == nil
	}, 2*time.Second, 10*time.Millisecond)
}

func TestThrottler_DeleteItemExceedsCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wcu        int64
		numDeletes int
		wantErr    bool
	}{
		{
			name:       "within_capacity",
			wcu:        3,
			numDeletes: 3,
			wantErr:    false,
		},
		{
			name:       "exceeds_capacity",
			wcu:        1,
			numDeletes: 2,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := newThrottledDB(t, 5, tt.wcu)

			var lastErr error

			for i := range tt.numDeletes {
				key := string(rune('a' + i))
				_, lastErr = db.DeleteItem(t.Context(), &ddbsdk.DeleteItemInput{
					TableName: aws.String("tbl"),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: key},
					},
				})
				if lastErr != nil {
					break
				}
			}

			if tt.wantErr {
				require.Error(t, lastErr)
				var ddbErr *dynamodb.Error
				require.ErrorAs(t, lastErr, &ddbErr)
				assert.Contains(t, ddbErr.Type, "ProvisionedThroughputExceededException")
			} else {
				require.NoError(t, lastErr)
			}
		})
	}
}

func TestThrottler_DeleteTableCleansUpBucket(t *testing.T) {
	t.Parallel()

	db := newThrottledDB(t, 1, 1)

	// Exhaust the write bucket.
	_, err := db.PutItem(t.Context(), &ddbsdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item:      map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "k"}},
	})
	require.NoError(t, err)

	// Delete the table.
	_, err = db.DeleteTable(t.Context(), &ddbsdk.DeleteTableInput{
		TableName: aws.String("tbl"),
	})
	require.NoError(t, err)

	// Re-create the same table — the old exhausted bucket must be gone.
	_, err = db.CreateTable(t.Context(), &ddbsdk.CreateTableInput{
		TableName: aws.String("tbl"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	})
	require.NoError(t, err)

	// A fresh write should succeed because the bucket was recreated at full capacity.
	_, err = db.PutItem(t.Context(), &ddbsdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item:      map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "k2"}},
	})
	require.NoError(t, err)
}

// TestThrottler_QueryExceedsCapacity verifies that Query returns
// ProvisionedThroughputExceededException when the read bucket is exhausted.
func TestThrottler_QueryExceedsCapacity(t *testing.T) {
	t.Parallel()

	// 2 RCU provisioned. Query of 3 items costs 1.5 RCU — first query succeeds,
	// second query exhausts the remaining 0.5 RCU and should be throttled.
	db := dynamodb.NewInMemoryDB()
	db.SetEnforceThroughput(true)

	_, err := db.CreateTable(t.Context(), &ddbsdk.CreateTableInput{
		TableName: aws.String("tbl"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(2),
			WriteCapacityUnits: aws.Int64(10),
		},
	})
	require.NoError(t, err)

	// Insert 3 items under the same partition key.
	for i := range 3 {
		sk := string(rune('a' + i))
		_, putErr := db.PutItem(t.Context(), &ddbsdk.PutItemInput{
			TableName: aws.String("tbl"),
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "pk1"},
				"sk": &types.AttributeValueMemberS{Value: sk},
			},
		})
		require.NoError(t, putErr)
	}

	q := &ddbsdk.QueryInput{
		TableName:              aws.String("tbl"),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "pk1"},
		},
	}

	// First query (3 items × 0.5 RCU = 1.5 RCU) should succeed.
	_, err = db.Query(t.Context(), q)
	require.NoError(t, err)

	// Second query should be throttled (only 0.5 RCU remaining, needs 1.5).
	_, err = db.Query(t.Context(), q)
	require.Error(t, err)

	var ddbErr *dynamodb.Error
	require.ErrorAs(t, err, &ddbErr)
	assert.Contains(t, ddbErr.Type, "ProvisionedThroughputExceededException")
}

func TestThrottler_UpdateItemExceedsCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wcu        int64
		numUpdates int
		wantErr    bool
	}{
		{
			name:       "within_capacity",
			wcu:        3,
			numUpdates: 3,
			wantErr:    false,
		},
		{
			name:       "exceeds_capacity",
			wcu:        1,
			numUpdates: 2,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := newThrottledDB(t, 5, tt.wcu)

			var lastErr error

			for range tt.numUpdates {
				_, lastErr = db.UpdateItem(t.Context(), &ddbsdk.UpdateItemInput{
					TableName: aws.String("tbl"),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "k"},
					},
					UpdateExpression: aws.String("SET val = :v"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":v": &types.AttributeValueMemberS{Value: "hello"},
					},
				})
				if lastErr != nil {
					break
				}
			}

			if tt.wantErr {
				require.Error(t, lastErr)
				var ddbErr *dynamodb.Error
				require.ErrorAs(t, lastErr, &ddbErr)
				assert.Contains(t, ddbErr.Type, "ProvisionedThroughputExceededException")
			} else {
				require.NoError(t, lastErr)
			}
		})
	}
}
