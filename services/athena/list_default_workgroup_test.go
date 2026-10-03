package athena_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

// TestList_DefaultsToPrimaryWorkGroup pins api_op_ListNamedQueries.go:40 and
// api_op_ListQueryExecutions.go:41: no WorkGroup means primary only.
func TestList_DefaultsToPrimaryWorkGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		workGroup *string
		name      string
		want      int
	}{
		{name: "unset_is_primary", want: 1},
		{name: "explicit_primary", workGroup: aws.String("primary"), want: 1},
		{name: "other_workgroup", workGroup: aws.String("wg2"), want: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAthenaClient(t, athena.NewHandler(athena.NewInMemoryBackend("000000000000", "us-east-1")))
			ctx := t.Context()

			_, err := client.CreateWorkGroup(ctx, &athenasdk.CreateWorkGroupInput{Name: aws.String("wg2")})
			require.NoError(t, err)

			for _, wg := range []*string{nil, aws.String("wg2")} {
				_, err = client.CreateNamedQuery(ctx, &athenasdk.CreateNamedQueryInput{
					Name: aws.String(
						"nq",
					),
					Database:    aws.String("db"),
					QueryString: aws.String("SELECT 1"),
					WorkGroup:   wg,
				})
				require.NoError(t, err)

				_, err = client.StartQueryExecution(ctx, &athenasdk.StartQueryExecutionInput{
					QueryString: aws.String("SELECT 1"),
					WorkGroup:   wg,
					ResultConfiguration: &athenatypes.ResultConfiguration{
						OutputLocation: aws.String("s3://bucket/out/"),
					},
				})
				require.NoError(t, err)
			}

			named, err := client.ListNamedQueries(ctx, &athenasdk.ListNamedQueriesInput{WorkGroup: tc.workGroup})
			require.NoError(t, err)
			assert.Len(t, named.NamedQueryIds, tc.want)

			execs, err := client.ListQueryExecutions(ctx, &athenasdk.ListQueryExecutionsInput{WorkGroup: tc.workGroup})
			require.NoError(t, err)
			assert.Len(t, execs.QueryExecutionIds, tc.want)
		})
	}
}
