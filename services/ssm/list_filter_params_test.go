package ssm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestDescribeParameters_PathFilter asserts ParameterStringFilter{Key:"Path"}
// (api_op_DescribeParameters.go / types.ParameterStringFilter: "valid for
// DescribeParameters" with Option Recursive|OneLevel) actually narrows the
// result set instead of matching every parameter. paramMatchesFilter's
// switch (parameters.go) had no "Path" case, so it fell through to the
// unknown-key default (return true, matching everything).
func TestDescribeParameters_PathFilter(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))

	for _, name := range []string{"/team/a", "/team/nested/b", "/other/c"} {
		_, err := client.PutParameter(t.Context(), &ssmsdk.PutParameterInput{
			Name:  aws.String(name),
			Value: aws.String("v"),
			Type:  ssmtypes.ParameterTypeString,
		})
		require.NoError(t, err)
	}

	t.Run("OneLevel", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeParameters(t.Context(), &ssmsdk.DescribeParametersInput{
			ParameterFilters: []ssmtypes.ParameterStringFilter{{
				Key:    aws.String("Path"),
				Option: aws.String("OneLevel"),
				Values: []string{"/team"},
			}},
		})
		require.NoError(t, err)

		names := make([]string, 0, len(out.Parameters))
		for _, p := range out.Parameters {
			names = append(names, aws.ToString(p.Name))
		}

		require.Equal(t, []string{"/team/a"}, names)
	})

	t.Run("Recursive", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeParameters(t.Context(), &ssmsdk.DescribeParametersInput{
			ParameterFilters: []ssmtypes.ParameterStringFilter{{
				Key:    aws.String("Path"),
				Option: aws.String("Recursive"),
				Values: []string{"/team"},
			}},
		})
		require.NoError(t, err)

		names := make([]string, 0, len(out.Parameters))
		for _, p := range out.Parameters {
			names = append(names, aws.ToString(p.Name))
		}

		require.ElementsMatch(t, []string{"/team/a", "/team/nested/b"}, names)
	})
}
