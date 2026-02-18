package dynamodb_test

import (
	"testing"

	"Gopherstack/dynamodb"
	"Gopherstack/dynamodb/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScan_Ordering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		tableName     string
		itemsToInsert []struct {
			pk string
			sk string
		}
		expectedOrder []struct {
			pk string
			sk string
		}
	}{
		{
			name:      "items sorted by PK then SK",
			tableName: "ScanOrderTable",
			itemsToInsert: []struct {
				pk string
				sk string
			}{
				{"PK1", "SK3"},
				{"PK1", "SK1"},
				{"PK2", "SK2"},
				{"PK1", "SK2"},
				{"PK2", "SK1"},
			},
			expectedOrder: []struct {
				pk string
				sk string
			}{
				{"PK1", "SK1"},
				{"PK1", "SK2"},
				{"PK1", "SK3"},
				{"PK2", "SK1"},
				{"PK2", "SK2"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()

			createTableJSON := `{
				"TableName": "` + tt.tableName + `",
				"KeySchema": [
					{"AttributeName": "pk", "KeyType": "HASH"},
					{"AttributeName": "sk", "KeyType": "RANGE"}
				],
				"AttributeDefinitions": [
					{"AttributeName": "pk", "AttributeType": "S"},
					{"AttributeName": "sk", "AttributeType": "S"}
				]
			}`

			ctInput := mustUnmarshal[models.CreateTableInput](t, createTableJSON)
			_, err := db.CreateTable(t.Context(), models.ToSDKCreateTableInput(&ctInput))
			require.NoError(t, err)

			// Insert items in NON-SORTED order
			for _, it := range tt.itemsToInsert {
				item := map[string]any{
					"pk": map[string]any{"S": it.pk},
					"sk": map[string]any{"S": it.sk},
				}
				putInput := models.PutItemInput{
					TableName: tt.tableName,
					Item:      item,
				}
				sdkPutInput, _ := models.ToSDKPutItemInput(&putInput)
				_, err := db.PutItem(t.Context(), sdkPutInput)
				require.NoError(t, err)
			}

			// Perform Scan
			scanInput := models.ScanInput{
				TableName: tt.tableName,
			}
			sdkScanInput, _ := models.ToSDKScanInput(&scanInput)
			res, err := db.Scan(t.Context(), sdkScanInput)
			require.NoError(t, err)

			// Verify order
			require.Len(t, res.Items, len(tt.expectedOrder))

			for i, exp := range tt.expectedOrder {
				item := models.FromSDKItem(res.Items[i])
				actualPK := item["pk"].(map[string]any)["S"].(string)
				actualSK := item["sk"].(map[string]any)["S"].(string)
				assert.Equal(t, exp.pk, actualPK, "PK mismatch at index %d", i)
				assert.Equal(t, exp.sk, actualSK, "SK mismatch at index %d", i)
			}
		})
	}
}
