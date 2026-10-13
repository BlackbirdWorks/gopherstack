package timestreamquery_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	tqsdk "github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuerySyntaxValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		query   string
		wantErr bool
	}{
		{"SELECT 1", false},
		{"  select * from db.t where x in (1, 2)", false},
		{"WITH a AS (SELECT 1) SELECT * FROM a", false},
		{"SELEC 1", true},
		{"DROP TABLE x", true},
		{"SELECT (1", true},
		{"SELECT 'abc", true},
		{"SELECT 1)", true},
	}

	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)

			_, err := client.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(tc.query)})
			_, prepErr := client.PrepareQuery(t.Context(), &tqsdk.PrepareQueryInput{QueryString: aws.String(tc.query)})

			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ValidationException")
				require.Error(t, prepErr)

				return
			}

			require.NoError(t, err)
			require.NoError(t, prepErr)
		})
	}
}

func TestScheduledQueryPagingAndNames(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.ListScheduledQueries(ctx, &tqsdk.ListScheduledQueriesInput{NextToken: aws.String("!!bad")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ValidationException")

	_, err = client.ListScheduledQueries(ctx, &tqsdk.ListScheduledQueriesInput{MaxResults: aws.Int32(1001)})
	require.Error(t, err)

	create := func(name, q string) error {
		_, cerr := client.CreateScheduledQuery(ctx, &tqsdk.CreateScheduledQueryInput{
			Name:        aws.String(name),
			QueryString: aws.String(q),
			ScheduleConfiguration: &types.ScheduleConfiguration{
				ScheduleExpression: aws.String("rate(1 hour)"),
			},
			ScheduledQueryExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
			NotificationConfiguration: &types.NotificationConfiguration{
				SnsConfiguration: &types.SnsConfiguration{TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:t")},
			},
			ErrorReportConfiguration: &types.ErrorReportConfiguration{
				S3Configuration: &types.S3Configuration{BucketName: aws.String("b")},
			},
		})

		return cerr
	}

	require.Error(t, create("bad name", "SELECT 1"))
	require.Error(t, create("ok-name", "SELEC 1"))
	require.NoError(t, create("sq-a", "SELECT 1"))
	require.NoError(t, create("sq-b", "SELECT 1"))

	first, err := client.ListScheduledQueries(ctx, &tqsdk.ListScheduledQueriesInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, first.ScheduledQueries, 1)
	require.NotNil(t, first.NextToken)
	assert.NotContains(t, aws.ToString(first.NextToken), "sq-b")

	second, err := client.ListScheduledQueries(ctx, &tqsdk.ListScheduledQueriesInput{NextToken: first.NextToken})
	require.NoError(t, err)
	require.Len(t, second.ScheduledQueries, 1)
	assert.Nil(t, second.NextToken)
}
