package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestGetPartitions_ExpressionTypes pins api_op_GetPartitions.go:58-122: numeric keys
// compare numerically, unquoted numbers parse, BETWEEN is inclusive.
func TestGetPartitions_ExpressionTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expr    string
		want    []string
		wantErr bool
	}{
		{name: "int_gt_numeric_not_lexical", expr: "n > 9", want: []string{"10", "100"}},
		{name: "int_gt_quoted_literal", expr: "n > '9'", want: []string{"10", "100"}},
		{name: "int_le", expr: "n <= 10", want: []string{"9", "10"}},
		{name: "int_eq_unquoted", expr: "n = 10", want: []string{"10"}},
		{name: "int_between_inclusive", expr: "n BETWEEN 9 AND 10", want: []string{"9", "10"}},
		{name: "int_not_between", expr: "n NOT BETWEEN 9 AND 10", want: []string{"100"}},
		{name: "int_between_and_boolean", expr: "n BETWEEN 9 AND 10 AND s = 'b'", want: []string{"10"}},
		{name: "int_in_unquoted", expr: "n IN (9, 100)", want: []string{"9", "100"}},
		{name: "int_not_in", expr: "n NOT IN (9, 100)", want: []string{"10"}},
		{name: "string_lexical_compare", expr: "s > 'a'", want: []string{"10", "100"}},
		{name: "string_between", expr: "s BETWEEN 'a' AND 'b'", want: []string{"9", "10"}},
		{name: "between_missing_and", expr: "n BETWEEN 9", wantErr: true},
		{name: "not_without_operator", expr: "n NOT 9", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestGlueClient(t, glue.NewHandler(glue.NewInMemoryBackend(testAccountID, testRegion)))
			ctx := t.Context()

			_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
				DatabaseInput: &types.DatabaseInput{Name: aws.String("db")},
			})
			require.NoError(t, err)

			_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
				DatabaseName: aws.String("db"),
				TableInput: &types.TableInput{
					Name: aws.String("t"),
					PartitionKeys: []types.Column{
						{Name: aws.String("n"), Type: aws.String("int")},
						{Name: aws.String("s"), Type: aws.String("string")},
					},
				},
			})
			require.NoError(t, err)

			for _, p := range [][]string{{"9", "a"}, {"10", "b"}, {"100", "c"}} {
				_, err = client.CreatePartition(ctx, &gluesdk.CreatePartitionInput{
					DatabaseName:   aws.String("db"),
					TableName:      aws.String("t"),
					PartitionInput: &types.PartitionInput{Values: p},
				})
				require.NoError(t, err)
			}

			out, err := client.GetPartitions(ctx, &gluesdk.GetPartitionsInput{
				DatabaseName: aws.String("db"),
				TableName:    aws.String("t"),
				Expression:   aws.String(tc.expr),
			})

			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			got := make([]string, 0, len(out.Partitions))
			for _, p := range out.Partitions {
				got = append(got, p.Values[0])
			}

			assert.ElementsMatch(t, tc.want, got)
		})
	}
}
