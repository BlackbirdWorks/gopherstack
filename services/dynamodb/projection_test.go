package dynamodb_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestProjection_BothSupplied_ReturnsError(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "ProjTable")

	putTestItem(t, db, "ProjTable", map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "pk1"},
		"sk": &types.AttributeValueMemberS{Value: "sk1"},
	})

	_, err := db.GetItem(ctx, &dynamodb_sdk.GetItemInput{
		TableName: aws.String("ProjTable"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "pk1"},
			"sk": &types.AttributeValueMemberS{Value: "sk1"},
		},
		ProjectionExpression: aws.String("pk"),
		AttributesToGet:      []string{"sk"},
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestProjection_AttributesToGetFallback(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "ProjFallback")

	putTestItem(t, db, "ProjFallback", map[string]types.AttributeValue{
		"pk":    &types.AttributeValueMemberS{Value: "pk1"},
		"sk":    &types.AttributeValueMemberS{Value: "sk1"},
		"extra": &types.AttributeValueMemberS{Value: "should_be_excluded"},
	})

	out, err := db.GetItem(ctx, &dynamodb_sdk.GetItemInput{
		TableName: aws.String("ProjFallback"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "pk1"},
			"sk": &types.AttributeValueMemberS{Value: "sk1"},
		},
		AttributesToGet: []string{"pk", "sk"},
	})
	if err != nil {
		t.Fatalf("GetItem with AttributesToGet: %v", err)
	}

	if _, hasExtra := out.Item["extra"]; hasExtra {
		t.Error("extra attribute should have been excluded by AttributesToGet")
	}

	if _, hasPK := out.Item["pk"]; !hasPK {
		t.Error("pk should be present")
	}
}

func TestProjection_NoBothEmpty_ReturnsFullItem(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "ProjEmpty")

	putTestItem(t, db, "ProjEmpty", map[string]types.AttributeValue{
		"pk":    &types.AttributeValueMemberS{Value: "pk1"},
		"sk":    &types.AttributeValueMemberS{Value: "sk1"},
		"extra": &types.AttributeValueMemberS{Value: "present"},
	})

	out, err := db.GetItem(ctx, &dynamodb_sdk.GetItemInput{
		TableName: aws.String("ProjEmpty"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "pk1"},
			"sk": &types.AttributeValueMemberS{Value: "sk1"},
		},
	})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}

	if _, ok := out.Item["extra"]; !ok {
		t.Error("expected 'extra' attribute when no projection specified")
	}
}
