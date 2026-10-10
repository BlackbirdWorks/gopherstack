package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetUserDefinedFunctions_FunctionType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		functionType string
		want         []string
	}{
		{name: "no_filter", want: []string{"proc", "reg"}},
		{name: "regular", functionType: "REGULAR_FUNCTION", want: []string{"reg"}},
		{name: "stored_procedure", functionType: "STORED_PROCEDURE", want: []string{"proc"}},
		{name: "aggregate", functionType: "AGGREGATE_FUNCTION", want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
				DatabaseInput: &types.DatabaseInput{Name: aws.String("db")},
			})
			require.NoError(t, err)

			for name, ft := range map[string]types.FunctionType{
				"reg": types.FunctionTypeRegularFunction, "proc": types.FunctionTypeStoredProcedure,
			} {
				_, err = client.CreateUserDefinedFunction(ctx, &gluesdk.CreateUserDefinedFunctionInput{
					DatabaseName: aws.String("db"),
					FunctionInput: &types.UserDefinedFunctionInput{
						FunctionName: aws.String(name), ClassName: aws.String("c"), FunctionType: ft,
					},
				})
				require.NoError(t, err)
			}

			in := &gluesdk.GetUserDefinedFunctionsInput{Pattern: aws.String(".*"), DatabaseName: aws.String("db")}
			if tt.functionType != "" {
				in.FunctionType = types.FunctionType(tt.functionType)
			}

			out, err := client.GetUserDefinedFunctions(ctx, in)
			require.NoError(t, err)

			got := make([]string, 0, len(out.UserDefinedFunctions))
			for _, f := range out.UserDefinedFunctions {
				got = append(got, aws.ToString(f.FunctionName))
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
