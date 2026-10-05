package workspaces_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wssdk "github.com/aws/aws-sdk-go-v2/service/workspaces"
	"github.com/aws/aws-sdk-go-v2/service/workspaces/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeWorkspacesPools_Filters checks PoolName filters, their operators and the validation errors.
func TestDescribeWorkspacesPools_Filters(t *testing.T) {
	t.Parallel()

	poolName := func(op types.DescribeWorkspacesPoolsFilterOperator, v ...string) []types.DescribeWorkspacesPoolsFilter {
		return []types.DescribeWorkspacesPoolsFilter{{
			Name: types.DescribeWorkspacesPoolsFilterNamePoolname, Operator: op, Values: v,
		}}
	}

	tests := []struct {
		name    string
		filters []types.DescribeWorkspacesPoolsFilter
		want    []string
		wantErr bool
	}{
		{name: "none", want: []string{"alpha", "alpine", "beta"}},
		{name: "equals", filters: poolName("EQUALS", "beta"), want: []string{"beta"}},
		{name: "equals_any", filters: poolName("EQUALS", "beta", "alpha"), want: []string{"alpha", "beta"}},
		{name: "notequals", filters: poolName("NOTEQUALS", "beta"), want: []string{"alpha", "alpine"}},
		{name: "contains", filters: poolName("CONTAINS", "alp"), want: []string{"alpha", "alpine"}},
		{name: "notcontains", filters: poolName("NOTCONTAINS", "alp"), want: []string{"beta"}},
		{name: "no_match", filters: poolName("EQUALS", "zzz")},
		{name: "bad_name", wantErr: true, filters: []types.DescribeWorkspacesPoolsFilter{{
			Name: "Nope", Operator: "EQUALS", Values: []string{"x"},
		}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)

			for _, n := range []string{"beta", "alpha", "alpine"} {
				_, err := client.CreateWorkspacesPool(t.Context(), &wssdk.CreateWorkspacesPoolInput{
					PoolName: aws.String(n), BundleId: aws.String("wsb-1"), DirectoryId: aws.String("d-1"),
					Description: aws.String("d"), Capacity: &types.Capacity{DesiredUserSessions: aws.Int32(1)},
				})
				require.NoError(t, err)
			}

			in := &wssdk.DescribeWorkspacesPoolsInput{Filters: tt.filters}

			out, err := client.DescribeWorkspacesPools(t.Context(), in)
			if tt.wantErr {
				require.ErrorContains(t, err, "InvalidParameterValuesException")

				return
			}

			require.NoError(t, err)

			var got []string
			for _, p := range out.WorkspacesPools {
				got = append(got, aws.ToString(p.PoolName))
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
