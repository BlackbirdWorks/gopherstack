package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	wafsdk "github.com/aws/aws-sdk-go-v2/service/waf"
	waftypes "github.com/aws/aws-sdk-go-v2/service/waf/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_WAF_GetSampledRequests drives GetChangeToken/CreateWebACL/
// GetSampledRequests through a real aws-sdk-go-v2 client. WebAclId existence
// is now validated against real backend state (this pass, gopherstack-smld):
// a known WebACL returns an empty (but well-formed) sample, and an unknown
// one returns WAFNonexistentItemException instead of silently succeeding.
func TestIntegration_WAF_GetSampledRequests(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createWAFClient(t)
	ctx := t.Context()

	tokenOut, err := client.GetChangeToken(ctx, &wafsdk.GetChangeTokenInput{})
	require.NoError(t, err)

	webACLName := "it-waf-acl"

	createOut, err := client.CreateWebACL(ctx, &wafsdk.CreateWebACLInput{
		ChangeToken: tokenOut.ChangeToken,
		Name:        aws.String(webACLName),
		MetricName:  aws.String("itWafAcl"),
		DefaultAction: &waftypes.WafAction{
			Type: waftypes.WafActionTypeAllow,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.WebACL)

	webACLID := aws.ToString(createOut.WebACL.WebACLId)
	require.NotEmpty(t, webACLID)

	now := time.Now()

	t.Run("known WebACL returns an empty sample", func(t *testing.T) {
		t.Parallel()

		out, sampleErr := client.GetSampledRequests(ctx, &wafsdk.GetSampledRequestsInput{
			WebAclId: aws.String(webACLID),
			RuleId:   aws.String("Default_Action"),
			MaxItems: aws.Int64(100),
			TimeWindow: &waftypes.TimeWindow{
				StartTime: aws.Time(now.Add(-time.Hour)),
				EndTime:   aws.Time(now),
			},
		})
		require.NoError(t, sampleErr)
		assert.Empty(t, out.SampledRequests)
		require.NotNil(t, out.TimeWindow)
	})

	t.Run("unknown WebACL returns WAFNonexistentItemException", func(t *testing.T) {
		t.Parallel()

		_, sampleErr := client.GetSampledRequests(ctx, &wafsdk.GetSampledRequestsInput{
			WebAclId: aws.String("nonexistent-web-acl"),
			RuleId:   aws.String("Default_Action"),
			MaxItems: aws.Int64(100),
			TimeWindow: &waftypes.TimeWindow{
				StartTime: aws.Time(now.Add(-time.Hour)),
				EndTime:   aws.Time(now),
			},
		})
		require.Error(t, sampleErr)

		var notFound *waftypes.WAFNonexistentItemException
		require.ErrorAs(t, sampleErr, &notFound)
	})
}
