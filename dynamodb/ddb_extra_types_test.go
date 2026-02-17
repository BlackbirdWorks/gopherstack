package dynamodb_test

import (
	"testing"

	"Gopherstack/dynamodb"
	"Gopherstack/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
)

func TestDynamoDB_ExtraTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		item map[string]any
		want map[string]any
		name string
	}{
		{
			name: "AllTypes",
			item: map[string]any{
				"pk":   map[string]any{"S": "item1"},
				"null": map[string]any{"NULL": true},
				"bool": map[string]any{"BOOL": false},
				"bin":  map[string]any{"B": "Ymlu"},
				"ss":   map[string]any{"SS": []any{"a", "b"}},
				"ns":   map[string]any{"NS": []any{"1", "2"}},
				"bs":   map[string]any{"BS": []any{"YjE="}},
			},
			want: map[string]any{
				"pk":   map[string]any{"S": "item1"},
				"null": map[string]any{"NULL": true},
				"bool": map[string]any{"BOOL": false},
				"bin":  map[string]any{"B": "Ymlu"},
				"ss":   map[string]any{"SS": []string{"a", "b"}},
				"ns":   map[string]any{"NS": []string{"1", "2"}},
				"bs":   map[string]any{"BS": []any{"YjE="}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			tableName := "ExtraTypesTable"
			createTableHelper(t, db, tableName, "pk")

			sdkInputItem, err := models.ToSDKItem(tt.item)
			require.NoError(t, err)

			_, err = db.PutItem(t.Context(), &sdk.PutItemInput{
				TableName: aws.String(tableName),
				Item:      sdkInputItem,
			})
			require.NoError(t, err)

			res, err := db.GetItem(t.Context(), &sdk.GetItemInput{
				TableName: aws.String(tableName),
				Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "item1"}},
			})
			require.NoError(t, err)
			require.NotNil(t, res.Item)

			got := models.FromSDKItem(res.Item)
			if diff := cmp.Diff(tt.want, got, cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
				t.Errorf("GetItem response mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDynamoDB_TTL_Operations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want  *sdk.DescribeTimeToLiveOutput
		input *sdk.UpdateTimeToLiveInput
		name  string
	}{
		{
			name: "EnableTTL",
			input: &sdk.UpdateTimeToLiveInput{
				TableName: aws.String("TTLTable"),
				TimeToLiveSpecification: &types.TimeToLiveSpecification{
					AttributeName: aws.String("ttl_attr"),
					Enabled:       aws.Bool(true),
				},
			},
			want: &sdk.DescribeTimeToLiveOutput{
				TimeToLiveDescription: &types.TimeToLiveDescription{
					AttributeName:    aws.String("ttl_attr"),
					TimeToLiveStatus: types.TimeToLiveStatusEnabled,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			createTableHelper(t, db, "TTLTable", "pk")

			_, err := db.UpdateTimeToLive(t.Context(), tt.input)
			require.NoError(t, err)

			res, err := db.DescribeTimeToLive(t.Context(), &sdk.DescribeTimeToLiveInput{
				TableName: aws.String("TTLTable"),
			})
			require.NoError(t, err)

			diff := cmp.Diff(tt.want.TimeToLiveDescription, res.TimeToLiveDescription,
				cmpopts.IgnoreUnexported(types.TimeToLiveDescription{}))
			if diff != "" {
				t.Errorf("DescribeTimeToLive mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDynamoDB_Transaction_Operations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		write     *sdk.TransactWriteItemsInput
		get       *sdk.TransactGetItemsInput
		wantItems []map[string]any
	}{
		{
			name: "WriteAndGet",
			write: &sdk.TransactWriteItemsInput{
				TransactItems: []types.TransactWriteItem{
					{
						Put: &types.Put{
							TableName: aws.String("TxTable"),
							Item: map[string]types.AttributeValue{
								"pk": &types.AttributeValueMemberS{Value: "tx1"},
								"v":  &types.AttributeValueMemberS{Value: "val1"},
							},
						},
					},
					{
						Put: &types.Put{
							TableName: aws.String("TxTable"),
							Item: map[string]types.AttributeValue{
								"pk": &types.AttributeValueMemberS{Value: "tx2"},
								"v":  &types.AttributeValueMemberS{Value: "val2"},
							},
						},
					},
				},
			},
			get: &sdk.TransactGetItemsInput{
				TransactItems: []types.TransactGetItem{
					{
						Get: &types.Get{
							TableName: aws.String("TxTable"),
							Key: map[string]types.AttributeValue{
								"pk": &types.AttributeValueMemberS{Value: "tx1"},
							},
						},
					},
					{
						Get: &types.Get{
							TableName: aws.String("TxTable"),
							Key: map[string]types.AttributeValue{
								"pk": &types.AttributeValueMemberS{Value: "tx2"},
							},
						},
					},
				},
			},
			wantItems: []map[string]any{
				{
					"pk": map[string]any{"S": "tx1"},
					"v":  map[string]any{"S": "val1"},
				},
				{
					"pk": map[string]any{"S": "tx2"},
					"v":  map[string]any{"S": "val2"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			createTableHelper(t, db, "TxTable", "pk")

			_, err := db.TransactWriteItems(t.Context(), tt.write)
			require.NoError(t, err)

			res, err := db.TransactGetItems(t.Context(), tt.get)
			require.NoError(t, err)
			require.Len(t, res.Responses, len(tt.wantItems))

			for i, resp := range res.Responses {
				got := models.FromSDKItem(resp.Item)
				if diff := cmp.Diff(tt.wantItems[i], got); diff != "" {
					t.Errorf("Transact item %d mismatch (-want +got):\n%s", i, diff)
				}
			}
		})
	}
}
