package dynamodb_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

func TestGSILimit_CreateTable_Exceeds20_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()

	const gsiCount21 = 21
	gsis := make([]types.GlobalSecondaryIndex, gsiCount21)
	ks := []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}}
	attrDefs := make([]types.AttributeDefinition, 0, 1+len(gsis))
	attrDefs = append(attrDefs, types.AttributeDefinition{
		AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS,
	})

	for i := range gsis {
		attrName := fmt.Sprintf("gsi_pk_%d", i)
		attrDefs = append(attrDefs, types.AttributeDefinition{
			AttributeName: aws.String(attrName),
			AttributeType: types.ScalarAttributeTypeS,
		})
		gsis[i] = types.GlobalSecondaryIndex{
			IndexName: aws.String(fmt.Sprintf("gsi-%d", i)),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrName), KeyType: types.KeyTypeHash},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
		}
	}

	_, err := db.CreateTable(ctx, &dynamodb_sdk.CreateTableInput{
		TableName:              aws.String("TooManyGSI"),
		KeySchema:              ks,
		AttributeDefinitions:   attrDefs,
		GlobalSecondaryIndexes: gsis,
		BillingMode:            types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	assertErrorCode(t, err, "LimitExceededException")
}

func TestGSILimit_CreateTable_Exactly20_Accepted(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()

	const gsiCount20 = 20
	gsis := make([]types.GlobalSecondaryIndex, gsiCount20)
	ks := []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}}
	attrDefs := make([]types.AttributeDefinition, 0, 1+len(gsis))
	attrDefs = append(attrDefs, types.AttributeDefinition{
		AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS,
	})

	for i := range gsis {
		attrName := fmt.Sprintf("gsi_pk_%d", i)
		attrDefs = append(attrDefs, types.AttributeDefinition{
			AttributeName: aws.String(attrName),
			AttributeType: types.ScalarAttributeTypeS,
		})
		gsis[i] = types.GlobalSecondaryIndex{
			IndexName: aws.String(fmt.Sprintf("gsi-%d", i)),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrName), KeyType: types.KeyTypeHash},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
		}
	}

	_, err := db.CreateTable(ctx, &dynamodb_sdk.CreateTableInput{
		TableName:              aws.String("Exactly20GSI"),
		KeySchema:              ks,
		AttributeDefinitions:   attrDefs,
		GlobalSecondaryIndexes: gsis,
		BillingMode:            types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	if err != nil {
		t.Fatalf("expected success for exactly 20 GSIs, got: %v", err)
	}
}

func TestLSILimit_CreateTable_Exceeds5_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()

	const lsiCount6 = 6
	lsis := make([]types.LocalSecondaryIndex, lsiCount6)
	ks := []types.KeySchemaElement{
		{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
	}
	attrDefs := make([]types.AttributeDefinition, 0, 2+len(lsis))
	attrDefs = append(
		attrDefs,
		types.AttributeDefinition{
			AttributeName: aws.String("pk"),
			AttributeType: types.ScalarAttributeTypeS,
		},
		types.AttributeDefinition{
			AttributeName: aws.String("sk"),
			AttributeType: types.ScalarAttributeTypeS,
		},
	)

	for i := range lsis {
		attrName := fmt.Sprintf("lsi_sk_%d", i)
		attrDefs = append(attrDefs, types.AttributeDefinition{
			AttributeName: aws.String(attrName),
			AttributeType: types.ScalarAttributeTypeS,
		})
		lsis[i] = types.LocalSecondaryIndex{
			IndexName: aws.String(fmt.Sprintf("lsi-%d", i)),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(attrName), KeyType: types.KeyTypeRange},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}
	}

	_, err := db.CreateTable(ctx, &dynamodb_sdk.CreateTableInput{
		TableName:             aws.String("TooManyLSI"),
		KeySchema:             ks,
		AttributeDefinitions:  attrDefs,
		LocalSecondaryIndexes: lsis,
		BillingMode:           types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	assertErrorCode(t, err, "LimitExceededException")
}

func TestIsOnDemandTable(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createOnDemandTestTable(t, db, "ODCheck")

	out, err := db.DescribeTable(ctx, &dynamodb_sdk.DescribeTableInput{
		TableName: aws.String("ODCheck"),
	})
	if err != nil {
		t.Fatalf("DescribeTable: %v", err)
	}

	if out.Table.BillingModeSummary.BillingMode != types.BillingModePayPerRequest {
		t.Errorf("want PAY_PER_REQUEST, got %s", out.Table.BillingModeSummary.BillingMode)
	}
}

func TestGSILimit_UpdateTable_Add21st_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()

	// Create table with 20 GSIs.
	const gsiCount = 20
	gsis := make([]types.GlobalSecondaryIndex, gsiCount)
	attrDefs := make([]types.AttributeDefinition, 0, 1+gsiCount)
	attrDefs = append(attrDefs, types.AttributeDefinition{
		AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS,
	})

	for i := range gsis {
		an := fmt.Sprintf("gk%d", i)
		attrDefs = append(attrDefs, types.AttributeDefinition{
			AttributeName: aws.String(an), AttributeType: types.ScalarAttributeTypeS,
		})
		gsis[i] = types.GlobalSecondaryIndex{
			IndexName: aws.String(fmt.Sprintf("gsi-%d", i)),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(an), KeyType: types.KeyTypeHash},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
		}
	}

	_, err := db.CreateTable(ctx, &dynamodb_sdk.CreateTableInput{
		TableName: aws.String("MaxGSITable"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions:   attrDefs,
		GlobalSecondaryIndexes: gsis,
		BillingMode:            types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	if err != nil {
		t.Fatalf("CreateTable with 20 GSIs: %v", err)
	}

	// Try adding a 21st GSI via UpdateTable.
	newAttrName := "extra_gk"
	_, err = db.UpdateTable(ctx, &dynamodb_sdk.UpdateTableInput{
		TableName: aws.String("MaxGSITable"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(newAttrName), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
			{Create: &types.CreateGlobalSecondaryIndexAction{
				IndexName: aws.String("gsi-21"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String(newAttrName), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(1),
					WriteCapacityUnits: aws.Int64(1),
				},
			}},
		},
	})
	assertErrorCode(t, err, "LimitExceededException")
}

func TestCreateTable_NoHashKey_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()

	_, err := db.CreateTable(ctx, &dynamodb_sdk.CreateTableInput{
		TableName: aws.String("tbl"),
		KeySchema: []types.KeySchemaElement{
			// Only a RANGE key — no HASH key
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestCreateTable_TwoHashKeys_Rejected(t *testing.T) {
	t.Parallel()

	keySchema := []models.KeySchemaElement{
		{AttributeName: "pk1", KeyType: models.KeyTypeHash},
		{AttributeName: "pk2", KeyType: models.KeyTypeHash},
	}
	err := dynamodb.ValidateCreateTableKeySchema(keySchema)
	assertErrorCode(t, err, "ValidationException")
}

func TestCreateTable_HashAndRange_Accepted(t *testing.T) {
	t.Parallel()

	keySchema := []models.KeySchemaElement{
		{AttributeName: "pk", KeyType: models.KeyTypeHash},
		{AttributeName: "sk", KeyType: models.KeyTypeRange},
	}
	if err := dynamodb.ValidateCreateTableKeySchema(keySchema); err != nil {
		t.Fatalf("valid hash+range schema should be accepted: %v", err)
	}
}

func TestCreateTable_HashOnly_Accepted(t *testing.T) {
	t.Parallel()

	keySchema := []models.KeySchemaElement{
		{AttributeName: "pk", KeyType: models.KeyTypeHash},
	}
	if err := dynamodb.ValidateCreateTableKeySchema(keySchema); err != nil {
		t.Fatalf("hash-only schema should be accepted: %v", err)
	}
}

func TestProvisionedThroughput_ZeroRead_Rejected(t *testing.T) {
	t.Parallel()

	zero := int64(0)
	one := int64(1)
	pt := &types.ProvisionedThroughput{
		ReadCapacityUnits:  &zero,
		WriteCapacityUnits: &one,
	}
	err := dynamodb.ValidateProvisionedThroughput(pt, types.BillingModeProvisioned)
	assertErrorCode(t, err, "ValidationException")
}

func TestProvisionedThroughput_ZeroWrite_Rejected(t *testing.T) {
	t.Parallel()

	one := int64(1)
	zero := int64(0)
	pt := &types.ProvisionedThroughput{
		ReadCapacityUnits:  &one,
		WriteCapacityUnits: &zero,
	}
	err := dynamodb.ValidateProvisionedThroughput(pt, types.BillingModeProvisioned)
	assertErrorCode(t, err, "ValidationException")
}

func TestProvisionedThroughput_PayPerRequest_ZeroAllowed(t *testing.T) {
	t.Parallel()

	// PAY_PER_REQUEST: zero-value throughput (not positive) is allowed.
	zero := int64(0)
	pt := &types.ProvisionedThroughput{ReadCapacityUnits: &zero, WriteCapacityUnits: &zero}
	if err := dynamodb.ValidateProvisionedThroughput(pt, types.BillingModePayPerRequest); err != nil {
		t.Fatalf("PAY_PER_REQUEST with zero throughput should be accepted: %v", err)
	}
}

func TestProvisionedThroughput_PayPerRequest_PositiveRejected(t *testing.T) {
	t.Parallel()

	// PAY_PER_REQUEST: positive throughput must be rejected.
	rcu := int64(5)
	wcu := int64(5)
	pt := &types.ProvisionedThroughput{ReadCapacityUnits: &rcu, WriteCapacityUnits: &wcu}
	if err := dynamodb.ValidateProvisionedThroughput(pt, types.BillingModePayPerRequest); err == nil {
		t.Fatal("PAY_PER_REQUEST with positive throughput must be rejected")
	}
}

func TestProvisionedThroughput_NilPT_ExplicitProvisioned_Rejected(t *testing.T) {
	t.Parallel()

	// Explicit PROVISIONED billing mode without throughput must be rejected.
	if err := dynamodb.ValidateProvisionedThroughput(nil, types.BillingModeProvisioned); err == nil {
		t.Fatal("explicit PROVISIONED with nil throughput must be rejected")
	}
}

func TestProvisionedThroughput_NilPT_DefaultBilling_Accepted(t *testing.T) {
	t.Parallel()

	// Default (unset) billing mode without throughput is allowed (uses defaults).
	if err := dynamodb.ValidateProvisionedThroughput(nil, ""); err != nil {
		t.Fatalf("default billing mode with nil throughput should use defaults: %v", err)
	}
}

func TestProvisionedThroughput_Positive_Accepted(t *testing.T) {
	t.Parallel()

	rcu := int64(5)
	wcu := int64(5)
	pt := &types.ProvisionedThroughput{ReadCapacityUnits: &rcu, WriteCapacityUnits: &wcu}
	if err := dynamodb.ValidateProvisionedThroughput(pt, types.BillingModeProvisioned); err != nil {
		t.Fatalf("positive throughput should be accepted: %v", err)
	}
}

func TestCreateTable_ExplicitZeroThroughput_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()

	_, err := db.CreateTable(ctx, &dynamodb_sdk.CreateTableInput{
		TableName: aws.String("tbl"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(0),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	assertErrorCode(t, err, "ValidationException")
}
