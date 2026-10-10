package xray_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	xraysdk "github.com/aws/aws-sdk-go-v2/service/xray"
	xraytypes "github.com/aws/aws-sdk-go-v2/service/xray/types"
	"github.com/stretchr/testify/require"
)

func TestList_InvalidNextToken(t *testing.T) {
	t.Parallel()

	now := time.Now()
	tests := []struct {
		call func(c *xraysdk.Client, arn string) error
		name string
	}{
		{name: "get_groups", call: func(c *xraysdk.Client, _ string) error {
			_, err := c.GetGroups(t.Context(), &xraysdk.GetGroupsInput{NextToken: aws.String("!!bad")})

			return err
		}},
		{name: "get_sampling_rules", call: func(c *xraysdk.Client, _ string) error {
			_, err := c.GetSamplingRules(t.Context(), &xraysdk.GetSamplingRulesInput{NextToken: aws.String("!!bad")})

			return err
		}},
		{name: "list_tags", call: func(c *xraysdk.Client, arn string) error {
			_, err := c.ListTagsForResource(t.Context(), &xraysdk.ListTagsForResourceInput{
				ResourceARN: aws.String(arn), NextToken: aws.String("!!bad"),
			})

			return err
		}},
		{name: "trace_summaries", call: func(c *xraysdk.Client, _ string) error {
			_, err := c.GetTraceSummaries(t.Context(), &xraysdk.GetTraceSummariesInput{
				StartTime: aws.Time(now.Add(-time.Hour)), EndTime: aws.Time(now), NextToken: aws.String("!!bad"),
			})

			return err
		}},
		{name: "service_graph", call: func(c *xraysdk.Client, _ string) error {
			_, err := c.GetServiceGraph(t.Context(), &xraysdk.GetServiceGraphInput{
				StartTime: aws.Time(now.Add(-time.Hour)), EndTime: aws.Time(now), NextToken: aws.String("!!bad"),
			})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestXRayClient(t)
			g, err := c.CreateGroup(t.Context(), &xraysdk.CreateGroupInput{GroupName: aws.String("grp")})
			require.NoError(t, err)

			var ire *xraytypes.InvalidRequestException
			require.ErrorAs(t, tt.call(c, aws.ToString(g.Group.GroupARN)), &ire)
		})
	}
}
