package dynamodb_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// newFastDDBJanitor creates a Janitor with a short interval for deterministic tests.
func newFastDDBJanitor(db *dynamodb.InMemoryDB) *dynamodb.Janitor {
	return dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: 5 * time.Millisecond})
}

func TestDDBJanitor_DeleteTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		deleteTable  string
		wantGone     string
		wantPresent  string
		createTables []string
	}{
		{
			name:         "deleted_table_moves_to_deleting_queue",
			createTables: []string{"queued-table"},
			deleteTable:  "queued-table",
			wantGone:     "queued-table",
		},
		{
			name:         "active_table_unaffected_when_other_deleted",
			createTables: []string{"keep-table", "drop-table"},
			deleteTable:  "drop-table",
			wantGone:     "drop-table",
			wantPresent:  "keep-table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			for _, tbl := range tt.createTables {
				createTable(t, db, tbl)
			}

			_, err := db.DeleteTable(t.Context(), &dynamodb_sdk.DeleteTableInput{
				TableName: aws.String(tt.deleteTable),
			})
			require.NoError(t, err)

			_, err = db.DescribeTable(t.Context(), &dynamodb_sdk.DescribeTableInput{
				TableName: aws.String(tt.wantGone),
			})
			require.Error(t, err, "expected deleted table to be gone from active map")

			if tt.wantPresent != "" {
				out, descErr := db.DescribeTable(t.Context(), &dynamodb_sdk.DescribeTableInput{
					TableName: aws.String(tt.wantPresent),
				})
				require.NoError(t, descErr)
				assert.Equal(t, tt.wantPresent, aws.ToString(out.Table.TableName))
			}
		})
	}
}

func TestDDBJanitor_RemovesTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		createTable string
	}{
		{
			name:        "janitor_finally_removes_deleted_table",
			createTable: "delete-me",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			createTable(t, db, tt.createTable)

			_, err := db.DeleteTable(t.Context(), &dynamodb_sdk.DeleteTableInput{
				TableName: aws.String(tt.createTable),
			})
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			j := newFastDDBJanitor(db)
			go j.Run(ctx)

			require.Eventually(t, func() bool {
				listed, listErr := db.ListTables(t.Context(), &dynamodb_sdk.ListTablesInput{})

				return listErr == nil && len(listed.TableNames) == 0
			}, 500*time.Millisecond, 10*time.Millisecond)
		})
	}
}
