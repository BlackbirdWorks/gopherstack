package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

const (
	num38Low  = "12345678901234567890123456789012345677"
	num38Mid  = "12345678901234567890123456789012345678"
	num38High = "12345678901234567890123456789012345679"
	num38Max  = "99999999999999999999999999999999999999"
)

func newNumberTable(t *testing.T) *dynamodbsdk.Client {
	t.Helper()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))

	_, err := client.CreateTable(t.Context(), &dynamodbsdk.CreateTableInput{
		TableName: aws.String("nums"),
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

	for _, sk := range []string{num38Mid, num38High, num38Low} {
		putNum(t, client, "p", sk, sk)
	}

	return client
}

func putNum(t *testing.T, c *dynamodbsdk.Client, pk, sk, n string) {
	t.Helper()

	_, err := c.PutItem(t.Context(), &dynamodbsdk.PutItemInput{
		TableName: aws.String("nums"),
		Item: map[string]dynamodbtypes.AttributeValue{
			"pk": &dynamodbtypes.AttributeValueMemberS{Value: pk},
			"sk": &dynamodbtypes.AttributeValueMemberN{Value: sk},
			"n":  &dynamodbtypes.AttributeValueMemberN{Value: n},
		},
	})
	require.NoError(t, err)
}

func numVals(vals ...string) map[string]dynamodbtypes.AttributeValue {
	out := map[string]dynamodbtypes.AttributeValue{":p": &dynamodbtypes.AttributeValueMemberS{Value: "p"}}
	for i, v := range vals {
		out[":"+string(rune('a'+i))] = &dynamodbtypes.AttributeValueMemberN{Value: v}
	}

	return out
}

func nVals(v string) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{":a": &dynamodbtypes.AttributeValueMemberN{Value: v}}
}

func skList(items []map[string]dynamodbtypes.AttributeValue) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it["sk"].(*dynamodbtypes.AttributeValueMemberN).Value)
	}

	return out
}

func TestNumberPrecision_KeyConditionAndOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		keyCond string
		vals    []string
		want    []string
		reverse bool
	}{
		{name: "eq", keyCond: "pk = :p AND sk = :a", vals: []string{num38Mid}, want: []string{num38Mid}},
		{name: "gt", keyCond: "pk = :p AND sk > :a", vals: []string{num38Mid}, want: []string{num38High}},
		{name: "ge", keyCond: "pk = :p AND sk >= :a", vals: []string{num38Mid}, want: []string{num38Mid, num38High}},
		{name: "lt", keyCond: "pk = :p AND sk < :a", vals: []string{num38Mid}, want: []string{num38Low}},
		{name: "le", keyCond: "pk = :p AND sk <= :a", vals: []string{num38Mid}, want: []string{num38Low, num38Mid}},
		{
			name: "between", keyCond: "pk = :p AND sk BETWEEN :a AND :b", vals: []string{num38Mid, num38High},
			want: []string{num38Mid, num38High},
		},
		{name: "all asc", keyCond: "pk = :p", want: []string{num38Low, num38Mid, num38High}},
		{
			name: "all desc", keyCond: "pk = :p", reverse: true,
			want: []string{num38High, num38Mid, num38Low},
		},
		{
			name:    "exponent form",
			keyCond: "pk = :p AND sk = :a",
			vals:    []string{"1.2345678901234567890123456789012345678E+37"},
			want:    []string{num38Mid},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newNumberTable(t)
			out, err := client.Query(t.Context(), &dynamodbsdk.QueryInput{
				TableName:                 aws.String("nums"),
				KeyConditionExpression:    aws.String(tt.keyCond),
				ExpressionAttributeValues: numVals(tt.vals...),
				ScanIndexForward:          aws.Bool(!tt.reverse),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, skList(out.Items))
		})
	}
}

func TestNumberPrecision_QueryPagination(t *testing.T) {
	t.Parallel()

	client := newNumberTable(t)

	var (
		got   []string
		start map[string]dynamodbtypes.AttributeValue
	)

	for range 5 {
		out, err := client.Query(t.Context(), &dynamodbsdk.QueryInput{
			TableName:                 aws.String("nums"),
			KeyConditionExpression:    aws.String("pk = :p"),
			ExpressionAttributeValues: numVals(),
			Limit:                     aws.Int32(1),
			ExclusiveStartKey:         start,
		})
		require.NoError(t, err)

		got = append(got, skList(out.Items)...)

		if start = out.LastEvaluatedKey; start == nil {
			break
		}
	}

	assert.Equal(t, []string{num38Low, num38Mid, num38High}, got)
}

func TestNumberPrecision_FilterAndCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter string
		vals   []string
		want   []string
	}{
		{name: "eq", filter: "n = :a", vals: []string{num38Mid}, want: []string{num38Mid}},
		{name: "ne", filter: "n <> :a", vals: []string{num38Mid}, want: []string{num38Low, num38High}},
		{name: "gt", filter: "n > :a", vals: []string{num38Mid}, want: []string{num38High}},
		{name: "lt", filter: "n < :a", vals: []string{num38Mid}, want: []string{num38Low}},
		{
			name:   "between",
			filter: "n BETWEEN :a AND :b",
			vals:   []string{num38Mid, num38High},
			want:   []string{num38Mid, num38High},
		},
		{name: "in", filter: "n IN (:a, :b)", vals: []string{num38Low, num38High}, want: []string{num38Low, num38High}},
		{
			name:   "exponent eq",
			filter: "n = :a",
			vals:   []string{"1.2345678901234567890123456789012345678E+37"},
			want:   []string{num38Mid},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newNumberTable(t)
			out, err := client.Scan(t.Context(), &dynamodbsdk.ScanInput{
				TableName:                 aws.String("nums"),
				FilterExpression:          aws.String(tt.filter),
				ExpressionAttributeValues: numVals(tt.vals...),
			})
			require.NoError(t, err)

			got := skList(out.Items)
			assert.ElementsMatch(t, tt.want, got)
		})
	}

	t.Run("condition", func(t *testing.T) {
		t.Parallel()

		client := newNumberTable(t)
		key := map[string]dynamodbtypes.AttributeValue{
			"pk": &dynamodbtypes.AttributeValueMemberS{Value: "p"},
			"sk": &dynamodbtypes.AttributeValueMemberN{Value: num38Mid},
		}
		_, err := client.DeleteItem(t.Context(), &dynamodbsdk.DeleteItemInput{
			TableName:                 aws.String("nums"),
			Key:                       key,
			ConditionExpression:       aws.String("n = :a"),
			ExpressionAttributeValues: nVals(num38High),
		})
		var ccf *dynamodbtypes.ConditionalCheckFailedException
		require.ErrorAs(t, err, &ccf)

		_, err = client.DeleteItem(t.Context(), &dynamodbsdk.DeleteItemInput{
			TableName:                 aws.String("nums"),
			Key:                       key,
			ConditionExpression:       aws.String("n = :a"),
			ExpressionAttributeValues: nVals(num38Mid),
		})
		require.NoError(t, err)
	})
}

func TestNumberPrecision_Normalisation(t *testing.T) {
	t.Parallel()

	tests := []struct{ in, want string }{
		{"1.0", "1"}, {"1.10", "1.1"}, {"1E+3", "1000"}, {"-0", "0"}, {"0.50", "0.5"},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()

			client := newNumberTable(t)
			putNum(t, client, "q", "1", tt.in)

			out, err := client.GetItem(t.Context(), &dynamodbsdk.GetItemInput{
				TableName: aws.String("nums"),
				Key: map[string]dynamodbtypes.AttributeValue{
					"pk": &dynamodbtypes.AttributeValueMemberS{Value: "q"},
					"sk": &dynamodbtypes.AttributeValueMemberN{Value: "1.0"},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, out.Item)
			assert.Equal(t, tt.want, out.Item["n"].(*dynamodbtypes.AttributeValueMemberN).Value)
		})
	}
}

func TestNumberPrecision_InvalidNumbers(t *testing.T) {
	t.Parallel()

	tests := []struct{ name, in, msg string }{
		{"too many digits", "123456789012345678901234567890123456789", "38 significant digits"},
		{"overflow", "1E+126", "overflow"},
		{"underflow", "1E-131", "underflow"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newNumberTable(t)
			_, err := client.PutItem(t.Context(), &dynamodbsdk.PutItemInput{
				TableName: aws.String("nums"),
				Item: map[string]dynamodbtypes.AttributeValue{
					"pk": &dynamodbtypes.AttributeValueMemberS{Value: "q"},
					"sk": &dynamodbtypes.AttributeValueMemberN{Value: "1"},
					"n":  &dynamodbtypes.AttributeValueMemberN{Value: tt.in},
				},
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "ValidationException")
			assert.Contains(t, err.Error(), tt.msg)
		})
	}
}

func TestNumberPrecision_UpdateArithmetic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		start   string
		update  string
		operand string
		want    string
		errMsg  string
	}{
		{name: "set add", start: num38Mid, update: "SET n = n + :a", operand: "1", want: num38High},
		{name: "set sub", start: num38Mid, update: "SET n = n - :a", operand: "1", want: num38Low},
		{name: "add action", start: num38Mid, update: "ADD n :a", operand: "1", want: num38High},
		{name: "decimal exact", start: "0.1", update: "SET n = n + :a", operand: "0.2", want: "0.3"},
		{name: "cancel to zero", start: "1.5", update: "SET n = n - :a", operand: "1.5", want: "0"},
		{name: "overflow", start: "5E+125", update: "ADD n :a", operand: "5E+125", errMsg: "overflow"},
		{name: "digit limit", start: num38Max, update: "ADD n :a", operand: "0.5", errMsg: "38 significant digits"},
		{name: "underflow", start: "1.5E-130", update: "SET n = n - :a", operand: "1E-130", errMsg: "underflow"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newNumberTable(t)
			putNum(t, client, "u", "1", tt.start)

			_, err := client.UpdateItem(t.Context(), &dynamodbsdk.UpdateItemInput{
				TableName: aws.String("nums"),
				Key: map[string]dynamodbtypes.AttributeValue{
					"pk": &dynamodbtypes.AttributeValueMemberS{Value: "u"},
					"sk": &dynamodbtypes.AttributeValueMemberN{Value: "1"},
				},
				UpdateExpression: aws.String(tt.update),
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
					":a": &dynamodbtypes.AttributeValueMemberN{Value: tt.operand},
				},
			})

			if tt.errMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)

				return
			}

			require.NoError(t, err)

			out, err := client.GetItem(t.Context(), &dynamodbsdk.GetItemInput{
				TableName: aws.String("nums"),
				Key: map[string]dynamodbtypes.AttributeValue{
					"pk": &dynamodbtypes.AttributeValueMemberS{Value: "u"},
					"sk": &dynamodbtypes.AttributeValueMemberN{Value: "1"},
				},
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, out.Item["n"].(*dynamodbtypes.AttributeValueMemberN).Value)
		})
	}
}

func TestNumberPrecision_NumberSets(t *testing.T) {
	t.Parallel()

	client := newNumberTable(t)
	key := map[string]dynamodbtypes.AttributeValue{
		"pk": &dynamodbtypes.AttributeValueMemberS{Value: "s"},
		"sk": &dynamodbtypes.AttributeValueMemberN{Value: "1"},
	}

	_, err := client.PutItem(t.Context(), &dynamodbsdk.PutItemInput{
		TableName: aws.String("nums"),
		Item: map[string]dynamodbtypes.AttributeValue{
			"pk": key["pk"], "sk": key["sk"],
			"ns": &dynamodbtypes.AttributeValueMemberNS{Value: []string{"1", "1.0"}},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicates")

	_, err = client.PutItem(t.Context(), &dynamodbsdk.PutItemInput{
		TableName: aws.String("nums"),
		Item: map[string]dynamodbtypes.AttributeValue{
			"pk": key["pk"], "sk": key["sk"],
			"ns": &dynamodbtypes.AttributeValueMemberNS{Value: []string{"1", "2"}},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateItem(t.Context(), &dynamodbsdk.UpdateItemInput{
		TableName: aws.String("nums"), Key: key,
		UpdateExpression: aws.String("ADD ns :a"),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":a": &dynamodbtypes.AttributeValueMemberNS{Value: []string{"1.0", "3"}},
		},
	})
	require.NoError(t, err)

	out, err := client.GetItem(t.Context(), &dynamodbsdk.GetItemInput{TableName: aws.String("nums"), Key: key})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"1", "2", "3"}, out.Item["ns"].(*dynamodbtypes.AttributeValueMemberNS).Value)

	_, err = client.UpdateItem(t.Context(), &dynamodbsdk.UpdateItemInput{
		TableName: aws.String("nums"), Key: key,
		UpdateExpression: aws.String("DELETE ns :a"),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":a": &dynamodbtypes.AttributeValueMemberNS{Value: []string{"2.00"}},
		},
	})
	require.NoError(t, err)

	out, err = client.GetItem(t.Context(), &dynamodbsdk.GetItemInput{TableName: aws.String("nums"), Key: key})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"1", "3"}, out.Item["ns"].(*dynamodbtypes.AttributeValueMemberNS).Value)
}
