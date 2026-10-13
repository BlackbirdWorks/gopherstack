package athena_test

import (
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var uuidShape = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func TestStartQueryExecution_UnknownTableFails(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)

	start, err := client.StartQueryExecution(t.Context(), &athenasdk.StartQueryExecutionInput{
		QueryString:           aws.String("SELECT * FROM default.no_such_table"),
		QueryExecutionContext: &athenatypes.QueryExecutionContext{Database: aws.String("default")},
	})
	require.NoError(t, err)
	assert.Regexp(t, uuidShape, aws.ToString(start.QueryExecutionId))

	got, err := client.GetQueryExecution(t.Context(), &athenasdk.GetQueryExecutionInput{
		QueryExecutionId: start.QueryExecutionId,
	})
	require.NoError(t, err)
	assert.Equal(t, athenatypes.QueryExecutionStateFailed, got.QueryExecution.Status.State)
	assert.Contains(t, aws.ToString(got.QueryExecution.Status.StateChangeReason), "TABLE_NOT_FOUND")

	_, err = client.GetQueryResults(
		t.Context(),
		&athenasdk.GetQueryResultsInput{QueryExecutionId: start.QueryExecutionId},
	)
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidRequestException", apiErr.ErrorCode())
	assert.Equal(t, "Query did not finish successfully. Final query state: FAILED", apiErr.ErrorMessage())
}

func TestAPIErrors_NoCodePrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call    func(c *athenasdk.Client) error
		name    string
		wantMsg string
	}{
		{
			name: "workgroup_missing",
			call: func(c *athenasdk.Client) error {
				_, err := c.GetWorkGroup(t.Context(), &athenasdk.GetWorkGroupInput{WorkGroup: aws.String("nope")})

				return err
			},
			wantMsg: "WorkGroup nope is not found.",
		},
		{
			name: "workgroup_bad_name",
			call: func(c *athenasdk.Client) error {
				_, err := c.CreateWorkGroup(t.Context(), &athenasdk.CreateWorkGroupInput{Name: aws.String("bad name!")})

				return err
			},
			wantMsg: "Value 'bad name!' at 'name' failed to satisfy constraint",
		},
		{
			name: "query_missing",
			call: func(c *athenasdk.Client) error {
				_, err := c.GetQueryExecution(
					t.Context(),
					&athenasdk.GetQueryExecutionInput{QueryExecutionId: aws.String("nope")},
				)

				return err
			},
			wantMsg: "QueryExecution nope was not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call(newRealClient(t))
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "InvalidRequestException", apiErr.ErrorCode())
			assert.Contains(t, apiErr.ErrorMessage(), tt.wantMsg)
			assert.NotContains(t, apiErr.ErrorMessage(), "Exception:")
		})
	}
}
