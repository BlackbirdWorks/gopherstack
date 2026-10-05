package redshiftdata_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftdatasdk "github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

// TestDescribeTableAndStatementResult_Paging checks DescribeTable paging and the one-page result tokens.
func TestDescribeTableAndStatementResult_Paging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want []int
		size int32
	}{
		{name: "two_two_one", size: 2, want: []int{2, 2, 1}},
		{name: "exact_division", size: 5, want: []int{5}},
		{name: "default_single_page", size: 0, want: []int{5}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestRedshiftDataSDKClient(t, redshiftdata.NewHandler(
				redshiftdata.NewInMemoryBackend(testAccountID, testRegion),
			))

			var (
				token *string
				got   []int
			)

			for range 5 {
				out, err := client.DescribeTable(t.Context(), &redshiftdatasdk.DescribeTableInput{
					Database: aws.String("dev"), Table: aws.String("users"), MaxResults: tt.size, NextToken: token,
				})
				require.NoError(t, err)

				got = append(got, len(out.ColumnList))
				if token = out.NextToken; token == nil {
					break
				}
			}

			assert.Equal(t, tt.want, got)

			_, err := client.DescribeTable(t.Context(), &redshiftdatasdk.DescribeTableInput{
				Database: aws.String("dev"), NextToken: aws.String("no-such-column"),
			})
			require.ErrorContains(t, err, "ValidationException")

			st, err := client.ExecuteStatement(t.Context(), &redshiftdatasdk.ExecuteStatementInput{
				Database: aws.String("dev"), Sql: aws.String("SELECT 1"), ClusterIdentifier: aws.String("c"),
			})
			require.NoError(t, err)

			_, err = client.GetStatementResult(t.Context(), &redshiftdatasdk.GetStatementResultInput{
				Id: st.Id, NextToken: aws.String("x"),
			})
			require.ErrorContains(t, err, "ValidationException")

			_, err = client.GetStatementResult(t.Context(), &redshiftdatasdk.GetStatementResultInput{Id: st.Id})
			require.NoError(t, err)

			csv, err := client.ExecuteStatement(t.Context(), &redshiftdatasdk.ExecuteStatementInput{
				Database: aws.String("dev"), Sql: aws.String("SELECT 1"), ClusterIdentifier: aws.String("c"),
				ResultFormat: "CSV",
			})
			require.NoError(t, err)

			_, err = client.GetStatementResultV2(t.Context(), &redshiftdatasdk.GetStatementResultV2Input{
				Id: csv.Id, NextToken: aws.String("x"),
			})
			require.ErrorContains(t, err, "ValidationException")
		})
	}
}
