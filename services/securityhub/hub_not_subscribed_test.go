package securityhub_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	"github.com/aws/aws-sdk-go-v2/service/securityhub/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestV1Ops_UnsubscribedAccountIsInvalidAccess(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	tests := []struct {
		call func(c *securityhubsdk.Client) error
		name string
	}{
		{name: "DisableSecurityHub", call: func(c *securityhubsdk.Client) error {
			_, err := c.DisableSecurityHub(ctx, &securityhubsdk.DisableSecurityHubInput{})

			return err
		}},
		{name: "DescribeHub", call: func(c *securityhubsdk.Client) error {
			_, err := c.DescribeHub(ctx, &securityhubsdk.DescribeHubInput{})

			return err
		}},
		{name: "UpdateSecurityHubConfiguration", call: func(c *securityhubsdk.Client) error {
			_, err := c.UpdateSecurityHubConfiguration(ctx, &securityhubsdk.UpdateSecurityHubConfigurationInput{
				AutoEnableControls: aws.Bool(true),
			})

			return err
		}},
		{name: "UpdateFindings", call: func(c *securityhubsdk.Client) error {
			_, err := c.UpdateFindings(ctx, &securityhubsdk.UpdateFindingsInput{
				Filters: &types.AwsSecurityFindingFilters{},
				Note:    &types.NoteUpdate{Text: aws.String("n"), UpdatedBy: aws.String("u")},
			})

			return err
		}},
		{name: "CreateInsight", call: func(c *securityhubsdk.Client) error {
			_, err := c.CreateInsight(ctx, &securityhubsdk.CreateInsightInput{
				Name:             aws.String("i"),
				GroupByAttribute: aws.String("ResourceId"),
				Filters:          &types.AwsSecurityFindingFilters{},
			})

			return err
		}},
		{name: "GetInsights", call: func(c *securityhubsdk.Client) error {
			_, err := c.GetInsights(ctx, &securityhubsdk.GetInsightsInput{})

			return err
		}},
		{name: "EnableImportFindingsForProduct", call: func(c *securityhubsdk.Client) error {
			_, err := c.EnableImportFindingsForProduct(ctx, &securityhubsdk.EnableImportFindingsForProductInput{
				ProductArn: aws.String("arn:aws:securityhub:us-east-1::product/aws/guardduty"),
			})

			return err
		}},
		{name: "CreateActionTarget", call: func(c *securityhubsdk.Client) error {
			_, err := c.CreateActionTarget(ctx, &securityhubsdk.CreateActionTargetInput{
				Name:        aws.String("n"),
				Description: aws.String("d"),
				Id:          aws.String("id1"),
			})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSecurityHubClient(t, newTestHandler(t))

			err := tt.call(client)
			require.Error(t, err)

			var iae *types.InvalidAccessException
			require.ErrorAs(t, err, &iae)
			assert.Contains(t, aws.ToString(iae.Message), "not subscribed to AWS Security Hub")
		})
	}
}
