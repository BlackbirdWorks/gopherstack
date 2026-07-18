package dynamodb_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestConsumedCapacityIndexes_PutItem(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()

	_, err := db.CreateTable(ctx, &dynamodb_sdk.CreateTableInput{
		TableName: aws.String("TestCC"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsi_pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("gsi1"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("gsi_pk"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(5),
					WriteCapacityUnits: aws.Int64(5),
				},
			},
		},
		BillingMode: types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	out, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
		TableName: aws.String("TestCC"),
		Item: map[string]types.AttributeValue{
			"pk":     &types.AttributeValueMemberS{Value: "k1"},
			"gsi_pk": &types.AttributeValueMemberS{Value: "g1"},
		},
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		t.Fatalf("PutItem: %v", err)
	}

	if out.ConsumedCapacity == nil {
		t.Fatal("expected ConsumedCapacity, got nil")
	}

	if aws.ToString(out.ConsumedCapacity.TableName) != "TestCC" {
		t.Errorf("unexpected table name: %s", aws.ToString(out.ConsumedCapacity.TableName))
	}

	if out.ConsumedCapacity.CapacityUnits == nil || *out.ConsumedCapacity.CapacityUnits <= 0 {
		t.Error("expected positive CapacityUnits")
	}
}

func TestConsumedCapacityIndexes_None(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "CCNone")

	out, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
		TableName: aws.String("CCNone"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "k1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
		ReturnConsumedCapacity: types.ReturnConsumedCapacityNone,
	})
	if err != nil {
		t.Fatalf("PutItem: %v", err)
	}

	if out.ConsumedCapacity != nil {
		t.Error("expected nil ConsumedCapacity for NONE")
	}
}

func TestConsistentRead_GetItem_DoesNotError(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "CRTable")

	putTestItem(t, db, "CRTable", map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "pk1"},
		"sk": &types.AttributeValueMemberS{Value: "sk1"},
	})

	out, err := db.GetItem(ctx, &dynamodb_sdk.GetItemInput{
		TableName:      aws.String("CRTable"),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "pk1"},
			"sk": &types.AttributeValueMemberS{Value: "sk1"},
		},
	})
	if err != nil {
		t.Fatalf("GetItem with ConsistentRead: %v", err)
	}

	if out.Item == nil {
		t.Error("expected item, got nil")
	}
}

func TestConsistentRead_Query_DoesNotError(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "CRQuery")

	putTestItem(t, db, "CRQuery", map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "p1"},
		"sk": &types.AttributeValueMemberS{Value: "s1"},
	})

	_, err := db.Query(ctx, &dynamodb_sdk.QueryInput{
		TableName:              aws.String("CRQuery"),
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
		},
	})
	if err != nil {
		t.Fatalf("Query with ConsistentRead: %v", err)
	}
}

// TestConsistentRead_Query_OnGSI_Rejected verifies that a strongly-consistent Query
// against a global secondary index is rejected with a ValidationException (AWS does not
// support consistent reads on a GSI), while the same query on the primary index and on a
// local secondary index is allowed.
func TestConsistentRead_Query_OnGSI_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		indexName string
		wantErr   bool
	}{
		{name: "primary_index_allowed", indexName: "", wantErr: false},
		{name: "lsi_allowed", indexName: "lsi1", wantErr: false},
		{name: "gsi_rejected", indexName: "gsi1", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := newInMemoryTestDB(t)
			ctx := context.Background()

			_, err := db.CreateTable(ctx, &dynamodb_sdk.CreateTableInput{
				TableName: aws.String("CRGSI"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
					{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
					{
						AttributeName: aws.String("gsi_pk"),
						AttributeType: types.ScalarAttributeTypeS,
					},
					{
						AttributeName: aws.String("lsi_sk"),
						AttributeType: types.ScalarAttributeTypeS,
					},
				},
				GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
					{
						IndexName: aws.String("gsi1"),
						KeySchema: []types.KeySchemaElement{
							{AttributeName: aws.String("gsi_pk"), KeyType: types.KeyTypeHash},
						},
						Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
					},
				},
				LocalSecondaryIndexes: []types.LocalSecondaryIndex{
					{
						IndexName: aws.String("lsi1"),
						KeySchema: []types.KeySchemaElement{
							{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
							{AttributeName: aws.String("lsi_sk"), KeyType: types.KeyTypeRange},
						},
						Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
					},
				},
				BillingMode: types.BillingModePayPerRequest,
			})
			if err != nil {
				t.Fatalf("CreateTable: %v", err)
			}

			input := &dynamodb_sdk.QueryInput{
				TableName:              aws.String("CRGSI"),
				ConsistentRead:         aws.Bool(true),
				KeyConditionExpression: aws.String("pk = :pk"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":pk": &types.AttributeValueMemberS{Value: "p1"},
				},
			}
			if tt.indexName != "" {
				input.IndexName = aws.String(tt.indexName)
				if tt.indexName == "gsi1" {
					input.KeyConditionExpression = aws.String("gsi_pk = :pk")
				}
			}

			_, err = db.Query(ctx, input)
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), "ValidationException") {
					t.Fatalf("expected ValidationException, got: %v", err)
				}

				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestConsistentRead_Scan_DoesNotError(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "CRScan")

	_, err := db.Scan(ctx, &dynamodb_sdk.ScanInput{
		TableName:      aws.String("CRScan"),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("Scan with ConsistentRead: %v", err)
	}
}

func TestBuildConsumedCapacityWithIndexes_Total(t *testing.T) {
	t.Parallel()
	cc := dynamodb.BuildConsumedCapacityWithIndexes(
		"myTable",
		types.ReturnConsumedCapacityTotal,
		1.0, 0,
		map[string]float64{"gsi1": 1.0}, map[string]float64{},
		nil, nil,
	)

	if cc == nil {
		t.Fatal("expected ConsumedCapacity")
	}

	if aws.ToString(cc.TableName) != "myTable" {
		t.Errorf("wrong table name: %s", aws.ToString(cc.TableName))
	}

	if aws.ToFloat64(cc.CapacityUnits) != 2.0 {
		t.Errorf("expected 2.0 total CU, got %v", aws.ToFloat64(cc.CapacityUnits))
	}

	// TOTAL should not include index breakdowns.
	if cc.GlobalSecondaryIndexes != nil {
		t.Error("TOTAL should not include index breakdowns")
	}
}

func TestBuildConsumedCapacityWithIndexes_Indexes(t *testing.T) {
	t.Parallel()
	cc := dynamodb.BuildConsumedCapacityWithIndexes(
		"myTable",
		types.ReturnConsumedCapacityIndexes,
		1.0, 0,
		map[string]float64{"gsi1": 0.5}, map[string]float64{},
		map[string]float64{"lsi1": 0.5}, map[string]float64{},
	)

	if cc == nil {
		t.Fatal("expected ConsumedCapacity")
	}

	if _, hasGSI := cc.GlobalSecondaryIndexes["gsi1"]; !hasGSI {
		t.Error("expected gsi1 in GlobalSecondaryIndexes")
	}

	if _, hasLSI := cc.LocalSecondaryIndexes["lsi1"]; !hasLSI {
		t.Error("expected lsi1 in LocalSecondaryIndexes")
	}

	if cc.Table == nil {
		t.Error("expected Table capacity breakdown")
	}
}

func TestBuildConsumedCapacityWithIndexes_None(t *testing.T) {
	t.Parallel()
	cc := dynamodb.BuildConsumedCapacityWithIndexes(
		"myTable",
		types.ReturnConsumedCapacityNone,
		1.0, 0, nil, nil, nil, nil,
	)

	if cc != nil {
		t.Error("expected nil for NONE")
	}
}

func TestConsistentRead_Scan_Parallel(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "CRScanP")

	// Populate some items.
	for i := range 10 {
		putTestItem(t, db, "CRScanP", map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("pk%d", i)},
			"sk": &types.AttributeValueMemberS{Value: "sk1"},
		})
	}

	out, err := db.Scan(ctx, &dynamodb_sdk.ScanInput{
		TableName:      aws.String("CRScanP"),
		ConsistentRead: aws.Bool(false),
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if out.Count != 10 {
		t.Errorf("expected 10 items, got %d", out.Count)
	}
}

func TestApplyConsistentReadMultiplier_False(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "CRMultFalse")

	putTestItem(t, db, "CRMultFalse", map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "pk1"},
		"sk": &types.AttributeValueMemberS{Value: "sk1"},
	})

	// ConsistentRead=false should not error.
	_, err := db.GetItem(ctx, &dynamodb_sdk.GetItemInput{
		TableName:      aws.String("CRMultFalse"),
		ConsistentRead: aws.Bool(false),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "pk1"},
			"sk": &types.AttributeValueMemberS{Value: "sk1"},
		},
	})
	if err != nil {
		t.Fatalf("GetItem with ConsistentRead=false: %v", err)
	}
}
