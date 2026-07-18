package dynamodb_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluateExpression(t *testing.T) {
	t.Parallel()
	item := map[string]any{
		"pk":     map[string]any{"S": "item1"},
		"val":    map[string]any{"N": "100"},
		"status": map[string]any{"S": "active"},
		"tags":   map[string]any{"SS": []string{"tag1", "tag2"}},
		"meta": map[string]any{
			"M": map[string]any{
				"ver": map[string]any{"N": "1"},
			},
		},
	}

	tests := []struct {
		vals      map[string]any
		name      string
		expr      string
		wantMatch bool
		wantErr   bool
	}{
		{
			name:      "Simple Equality",
			expr:      "status = :s",
			vals:      map[string]any{":s": map[string]any{"S": "active"}},
			wantMatch: true,
		},
		{
			name:      "Numeric Comparison (>)",
			expr:      "val > :v",
			vals:      map[string]any{":v": map[string]any{"N": "50"}},
			wantMatch: true,
		},
		{
			name:      "Numeric Comparison (<) - False",
			expr:      "val < :v",
			vals:      map[string]any{":v": map[string]any{"N": "50"}},
			wantMatch: false,
		},
		{
			name: "Between Condition",
			expr: "val BETWEEN :min AND :max",
			vals: map[string]any{
				":min": map[string]any{"N": "50"},
				":max": map[string]any{"N": "150"},
			},
			wantMatch: true,
		},
		{
			name:      "Attribute Exists",
			expr:      "attribute_exists(pk)",
			wantMatch: true,
		},
		{
			name:      "Attribute Not Exists",
			expr:      "attribute_not_exists(missing)",
			wantMatch: true,
		},
		{
			name:      "Begins With",
			expr:      "begins_with(status, :prefix)",
			vals:      map[string]any{":prefix": map[string]any{"S": "act"}},
			wantMatch: true,
		},
		{
			name:      "Contains (String)",
			expr:      "contains(status, :sub)",
			vals:      map[string]any{":sub": map[string]any{"S": "tiv"}},
			wantMatch: true,
		},
		{
			name:      "Nested Attribute Path",
			expr:      "meta.ver = :v",
			vals:      map[string]any{":v": map[string]any{"N": "1"}},
			wantMatch: true,
		},
		{
			name: "OR Condition",
			expr: "val < :min OR val > :max",
			vals: map[string]any{
				":min": map[string]any{"N": "10"},
				":max": map[string]any{"N": "90"},
			},
			wantMatch: true,
		},
		{
			name: "AND Condition",
			expr: "status = :s AND val = :v",
			vals: map[string]any{
				":s": map[string]any{"S": "active"},
				":v": map[string]any{"N": "100"},
			},
			wantMatch: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			match, err := dynamodb.EvaluateExpression(tc.expr, item, tc.vals, nil)
			if tc.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantMatch, match)
		})
	}
}

func TestCompareValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		lhs  any
		rhs  any
		name string
		op   string
		want bool
	}{
		{
			name: "Number < True",
			lhs:  map[string]any{"N": "10"},
			op:   "<",
			rhs:  map[string]any{"N": "20"},
			want: true,
		},
		{
			name: "Number < False",
			lhs:  map[string]any{"N": "20"},
			op:   "<",
			rhs:  map[string]any{"N": "10"},
			want: false,
		},
		{
			name: "Number > True",
			lhs:  map[string]any{"N": "20"},
			op:   ">",
			rhs:  map[string]any{"N": "10"},
			want: true,
		},
		{
			name: "Number = True",
			lhs:  map[string]any{"N": "10"},
			op:   "=",
			rhs:  map[string]any{"N": "10"},
			want: true,
		},
		{
			name: "String < True",
			lhs:  map[string]any{"S": "a"},
			op:   "<",
			rhs:  map[string]any{"S": "b"},
			want: true,
		},
		{
			name: "String > True",
			lhs:  map[string]any{"S": "b"},
			op:   ">",
			rhs:  map[string]any{"S": "a"},
			want: true,
		},
		{
			name: "String = True",
			lhs:  map[string]any{"S": "a"},
			op:   "=",
			rhs:  map[string]any{"S": "a"},
			want: true,
		},
		{
			name: "String != True",
			lhs:  map[string]any{"S": "a"},
			op:   "<>",
			rhs:  map[string]any{"S": "b"},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := dynamodb.CompareValues(tc.lhs, tc.op, tc.rhs)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestUnwrapAttributeValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input any
		want  any
		name  string
	}{
		{
			name:  "String",
			input: map[string]any{"S": "test"},
			want:  "test",
		},
		{
			name:  "Number",
			input: map[string]any{"N": "123"},
			want:  "123",
		},
		{
			name:  "Boolean",
			input: map[string]any{"BOOL": true},
			want:  true,
		},
		{
			name:  "Null",
			input: map[string]any{"NULL": true},
			want:  nil,
		},
		{
			name:  "RawString",
			input: "raw",
			want:  "raw",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := dynamodb.UnwrapAttributeValue(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEAVTypes_InvalidTypeKey_Rejected(t *testing.T) {
	t.Parallel()
	// Use an invalid type key by manipulating the wire format directly.
	err := dynamodb.ValidateEAVTypes(map[string]any{
		":val": map[string]any{"INVALID": "something"},
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestEAVTypes_ValidTypes_Accepted(t *testing.T) {
	t.Parallel()
	err := dynamodb.ValidateEAVTypes(map[string]any{
		":s":    map[string]any{"S": "hello"},
		":n":    map[string]any{"N": "42"},
		":b":    map[string]any{"BOOL": true},
		":null": map[string]any{"NULL": true},
		":m":    map[string]any{"M": map[string]any{}},
		":l":    map[string]any{"L": []any{}},
	})
	if err != nil {
		t.Fatalf("expected valid EAV types to pass, got: %v", err)
	}
}

func TestEAVTypes_NonMapValue_Rejected(t *testing.T) {
	t.Parallel()
	err := dynamodb.ValidateEAVTypes(map[string]any{
		":bad": "not-a-map",
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestEAVTypes_MultipleTypeKeys_Rejected(t *testing.T) {
	t.Parallel()
	err := dynamodb.ValidateEAVTypes(map[string]any{
		":bad": map[string]any{"S": "a", "N": "1"},
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestValidateEAVTypes_Empty(t *testing.T) {
	t.Parallel()
	// Empty EAV map is valid.
	err := dynamodb.ValidateEAVTypes(map[string]any{})
	if err != nil {
		t.Fatalf("empty EAV should be valid: %v", err)
	}
}

func TestValidateEAVTypes_SS_Valid(t *testing.T) {
	t.Parallel()
	err := dynamodb.ValidateEAVTypes(map[string]any{
		":ss": map[string]any{"SS": []string{"a", "b"}},
	})
	if err != nil {
		t.Fatalf("SS type should be valid: %v", err)
	}
}

func TestEAN_KeyWithoutHash_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	_, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
		ConditionExpression:      aws.String("attribute_not_exists(nopk)"),
		ExpressionAttributeNames: map[string]string{"nopk": "pk"}, // missing #
	})
	assertErrorCode(t, err, "ValidationException")
	if err != nil && !strings.Contains(err.Error(), "ExpressionAttributeNames") {
		t.Fatalf("error should mention ExpressionAttributeNames, got: %v", err)
	}
}

func TestEAN_EmptyValue_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	_, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
		ConditionExpression:      aws.String("attribute_not_exists(#pk)"),
		ExpressionAttributeNames: map[string]string{"#pk": ""}, // empty value
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestEAN_ValidHash_Accepted(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	_, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
		ConditionExpression:      aws.String("attribute_not_exists(#pk)"),
		ExpressionAttributeNames: map[string]string{"#pk": "pk"},
	})
	if err != nil {
		t.Fatalf("valid EAN with # prefix should be accepted: %v", err)
	}
}

func TestUnusedEAN_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	_, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
		// #unused is in EAN but never referenced in ConditionExpression
		ExpressionAttributeNames: map[string]string{"#unused": "someAttr"},
	})
	assertErrorCode(t, err, "ValidationException")
	if err != nil && !strings.Contains(err.Error(), "unused in expressions") {
		t.Fatalf("expected 'unused in expressions' in error, got: %v", err)
	}
}

func TestUnusedEAV_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	_, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
		// :val is provided but not used in any expression
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":val": &types.AttributeValueMemberS{Value: "test"},
		},
	})
	assertErrorCode(t, err, "ValidationException")
	if err != nil && !strings.Contains(err.Error(), "unused in expressions") {
		t.Fatalf("expected 'unused in expressions' in error, got: %v", err)
	}
}

func TestUnusedEAV_DeleteItem_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	_, err := db.DeleteItem(ctx, &dynamodb_sdk.DeleteItemInput{
		TableName: aws.String("tbl"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
		// :x is provided but not used in ConditionExpression
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":x": &types.AttributeValueMemberS{Value: "v"},
		},
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestUsedEAV_PutItem_Accepted(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	_, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
		ConditionExpression: aws.String("v = :val"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":val": &types.AttributeValueMemberS{Value: "test"},
		},
	})
	// May succeed or fail on condition, but NOT with unused-EAV error
	if err != nil && strings.Contains(err.Error(), "unused in expressions") {
		t.Fatalf("used EAV should not trigger unused error: %v", err)
	}
}

func TestUpdateItem_CannotModifyPartitionKey(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	putTestItem(t, db, "tbl", map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "p1"},
		"sk": &types.AttributeValueMemberS{Value: "s1"},
		"v":  &types.AttributeValueMemberS{Value: "old"},
	})

	_, err := db.UpdateItem(ctx, &dynamodb_sdk.UpdateItemInput{
		TableName: aws.String("tbl"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
		UpdateExpression: aws.String("SET pk = :newpk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":newpk": &types.AttributeValueMemberS{Value: "p2"},
		},
	})
	assertErrorCode(t, err, "ValidationException")
	if err != nil && !strings.Contains(err.Error(), "part of the key") {
		t.Fatalf("expected 'part of the key' in error, got: %v", err)
	}
}

func TestUpdateItem_CannotModifySortKey(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	putTestItem(t, db, "tbl", map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "p1"},
		"sk": &types.AttributeValueMemberS{Value: "s1"},
		"v":  &types.AttributeValueMemberS{Value: "old"},
	})

	_, err := db.UpdateItem(ctx, &dynamodb_sdk.UpdateItemInput{
		TableName: aws.String("tbl"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
		UpdateExpression: aws.String("SET sk = :newsk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":newsk": &types.AttributeValueMemberS{Value: "s2"},
		},
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestUpdateItem_CannotModifyKeyViaAlias(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	_, err := db.UpdateItem(ctx, &dynamodb_sdk.UpdateItemInput{
		TableName: aws.String("tbl"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
		UpdateExpression: aws.String("SET #k = :newval"),
		ExpressionAttributeNames: map[string]string{
			"#k": "pk",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":newval": &types.AttributeValueMemberS{Value: "p2"},
		},
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestUpdateItem_NonKeyAttribute_Accepted(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	putTestItem(t, db, "tbl", map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "p1"},
		"sk": &types.AttributeValueMemberS{Value: "s1"},
		"v":  &types.AttributeValueMemberS{Value: "old"},
	})

	_, err := db.UpdateItem(ctx, &dynamodb_sdk.UpdateItemInput{
		TableName: aws.String("tbl"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
		UpdateExpression: aws.String("SET v = :newv"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":newv": &types.AttributeValueMemberS{Value: "new"},
		},
	})
	if err != nil {
		t.Fatalf("updating non-key attribute should succeed: %v", err)
	}
}
