package dynamodb_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// newFastDDBJanitor creates a Janitor with a short interval for deterministic tests.
func newFastDDBJanitor(db *dynamodb.InMemoryDB) *dynamodb.Janitor {
	j := dynamodb.NewJanitor(db, logger.NewTestLogger())
	j.Interval = 5 * time.Millisecond

	return j
}

func TestDDBJanitor_DeleteTableMovesToDeletingQueue(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTable(t, db, "queued-table")

	_, err := db.DeleteTable(t.Context(), &dynamodb_sdk.DeleteTableInput{
		TableName: aws.String("queued-table"),
	})
	require.NoError(t, err)

	// Table should be invisible to DescribeTable immediately after DeleteTable.
	_, err = db.DescribeTable(t.Context(), &dynamodb_sdk.DescribeTableInput{
		TableName: aws.String("queued-table"),
	})
	require.Error(t, err, "expected table to be gone from active map")
}

func TestDDBJanitor_JanitorFinallyRemovesTable(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTable(t, db, "delete-me")

	_, err := db.DeleteTable(t.Context(), &dynamodb_sdk.DeleteTableInput{
		TableName: aws.String("delete-me"),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	j := newFastDDBJanitor(db)
	go j.Run(ctx)

	// After the janitor runs, the deleting map should drain.
	require.Eventually(t, func() bool {
		listed, listErr := db.ListTables(t.Context(), &dynamodb_sdk.ListTablesInput{})
		return listErr == nil && len(listed.TableNames) == 0
	}, 500*time.Millisecond, 10*time.Millisecond)
}

func TestDDBJanitor_ActiveTableUnaffected(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTable(t, db, "keep-table")
	createTable(t, db, "drop-table")

	_, err := db.DeleteTable(t.Context(), &dynamodb_sdk.DeleteTableInput{
		TableName: aws.String("drop-table"),
	})
	require.NoError(t, err)

	// "keep-table" must still be accessible.
	out, err := db.DescribeTable(t.Context(), &dynamodb_sdk.DescribeTableInput{
		TableName: aws.String("keep-table"),
	})
	require.NoError(t, err)
	assert.Equal(t, "keep-table", aws.ToString(out.Table.TableName))

	// "drop-table" is gone.
	_, err = db.DescribeTable(t.Context(), &dynamodb_sdk.DescribeTableInput{
		TableName: aws.String("drop-table"),
	})
	require.Error(t, err)
}
