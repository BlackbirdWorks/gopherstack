package dynamodb_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// TestListGlobalTables_DefaultLimitPagination creates more global tables
// than the documented default page size and calls ListGlobalTables without
// a Limit, driving the real SDK client through the full pagination loop.
// ListGlobalTablesInput.Limit's doc comment (api_op_ListGlobalTables.go:35)
// states DynamoDB defaults to 100 when the caller omits it; before the fix,
// applyGlobalTableLimit only capped the page when the caller supplied an
// explicit Limit, so an unspecified Limit returned every global table in
// one uncapped response with no LastEvaluatedGlobalTableName.
func TestListGlobalTables_DefaultLimitPagination(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	client := newTestDynamoDBClient(t, dynamodb.NewHandler(backend))
	ctx := t.Context()

	const total = 105

	want := make(map[string]bool, total)

	for i := range total {
		name := fmt.Sprintf("gt-%03d", i)
		_, err := client.CreateGlobalTable(ctx, &dynamodbsdk.CreateGlobalTableInput{
			GlobalTableName:  aws.String(name),
			ReplicationGroup: []dynamodbtypes.Replica{{RegionName: aws.String(ddbTagsRTRegion)}},
		})
		require.NoError(t, err)
		want[name] = true
	}

	got := make(map[string]bool, total)

	var startName *string
	for pages := 0; ; pages++ {
		require.Less(t, pages, total, "pagination loop did not terminate")

		out, err := client.ListGlobalTables(ctx, &dynamodbsdk.ListGlobalTablesInput{
			ExclusiveStartGlobalTableName: startName,
		})
		require.NoError(t, err)
		require.LessOrEqualf(t, len(out.GlobalTables), 100,
			"documented default Limit of 100 must actually cap the page; pre-fix an omitted "+
				"Limit returned everything uncapped")

		for _, gt := range out.GlobalTables {
			name := aws.ToString(gt.GlobalTableName)
			require.Falsef(t, got[name], "global table %q returned twice across pages", name)
			got[name] = true
		}

		if out.LastEvaluatedGlobalTableName == nil {
			break
		}

		startName = out.LastEvaluatedGlobalTableName
	}

	require.Equal(t, want, got)
}
