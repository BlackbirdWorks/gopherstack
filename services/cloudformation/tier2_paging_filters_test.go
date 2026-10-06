package cloudformation_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfnsdktypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

const pagedTemplate = `{"Resources":{"Topic":{"Type":"AWS::SNS::Topic"}}}`

// TestRealClient_DescribeStacksPages covers the NextToken member of DescribeStacks
// (query protocol, default page 100).
func TestRealClient_DescribeStacksPages(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	for i := range 101 {
		_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
			StackName: aws.String(fmt.Sprintf("paged-%03d", i)), TemplateBody: aws.String(pagedTemplate),
		})
		require.NoError(t, err)
	}

	first, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{})
	require.NoError(t, err)
	require.Len(t, first.Stacks, 100)
	require.NotNil(t, first.NextToken)

	second, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{NextToken: first.NextToken})
	require.NoError(t, err)
	require.Len(t, second.Stacks, 1)
	require.Nil(t, second.NextToken)
}

func TestRealClient_PagingValidationAndFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call    func(c *cfnsdk.Client) error
		name    string
		wantErr bool
	}{
		{name: "bad_token", wantErr: true, call: func(c *cfnsdk.Client) error {
			_, err := c.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{NextToken: aws.String("%%%")})

			return err
		}},
		{name: "account_limits_bad_token", wantErr: true, call: func(c *cfnsdk.Client) error {
			_, err := c.DescribeAccountLimits(
				t.Context(),
				&cfnsdk.DescribeAccountLimitsInput{NextToken: aws.String("%%%")},
			)

			return err
		}},
		{name: "max_results_out_of_range", wantErr: true, call: func(c *cfnsdk.Client) error {
			_, err := c.ListStackSets(t.Context(), &cfnsdk.ListStackSetsInput{MaxResults: aws.Int32(101)})

			return err
		}},
		{name: "drift_status_filter", call: func(c *cfnsdk.Client) error {
			_, err := c.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
				StackName: aws.String("drift-stack"), TemplateBody: aws.String(pagedTemplate),
			})
			if err != nil {
				return err
			}

			out, err := c.DescribeStackResourceDrifts(t.Context(), &cfnsdk.DescribeStackResourceDriftsInput{
				StackName: aws.String("drift-stack"),
				StackResourceDriftStatusFilters: []cfnsdktypes.StackResourceDriftStatus{
					cfnsdktypes.StackResourceDriftStatusDeleted,
				},
			})
			if err != nil {
				return err
			}

			require.Empty(t, out.StackResourceDrifts)

			all, err := c.DescribeStackResourceDrifts(t.Context(), &cfnsdk.DescribeStackResourceDriftsInput{
				StackName: aws.String("drift-stack"), MaxResults: aws.Int32(1),
			})
			require.NoError(t, err)
			require.Len(t, all.StackResourceDrifts, 1)

			return nil
		}},
		{name: "list_types_prefix", call: func(c *cfnsdk.Client) error {
			out, err := c.ListTypes(t.Context(), &cfnsdk.ListTypesInput{
				Filters: &cfnsdktypes.TypeFilters{TypeNamePrefix: aws.String("No::Such::")},
			})
			if err != nil {
				return err
			}

			require.Empty(t, out.TypeSummaries)

			return nil
		}},
		{name: "scan_filter_types", call: func(c *cfnsdk.Client) error {
			scan, err := c.StartResourceScan(t.Context(), &cfnsdk.StartResourceScanInput{
				ScanFilters: []cfnsdktypes.ScanFilter{{Types: []string{"AWS::Lambda::*"}}},
			})
			if err != nil {
				return err
			}

			out, err := c.ListResourceScanResources(t.Context(), &cfnsdk.ListResourceScanResourcesInput{
				ResourceScanId: scan.ResourceScanId,
			})
			if err != nil {
				return err
			}

			require.Empty(t, out.Resources)

			return nil
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call(newTestHandlerAndClient(t))
			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError

			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, "ValidationError", apiErr.ErrorCode())
		})
	}
}
