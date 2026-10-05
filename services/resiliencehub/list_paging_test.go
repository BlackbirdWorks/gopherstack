package resiliencehub_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	resiliencehubsdk "github.com/aws/aws-sdk-go-v2/service/resiliencehub"
	"github.com/aws/aws-sdk-go-v2/service/resiliencehub/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// TestRealClient_ListOpsPageAndRejectBadPaging covers body-bound maxResults/nextToken
// (resiliencehub serializers) on ops that previously ignored them.
func TestRealClient_ListOpsPageAndRejectBadPaging(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	app, err := client.CreateApp(ctx, &resiliencehubsdk.CreateAppInput{Name: aws.String("paged-app")})
	require.NoError(t, err)

	for i := range 3 {
		_, err = client.CreateAppVersionAppComponent(ctx, &resiliencehubsdk.CreateAppVersionAppComponentInput{
			AppArn: app.App.AppArn, Name: aws.String(fmt.Sprintf("comp-%d", i)),
			Type: aws.String("AWS::ResilienceHub::AppComponent"),
		})
		require.NoError(t, err)
	}

	started, err := client.StartAppAssessment(ctx, &resiliencehubsdk.StartAppAssessmentInput{
		AppArn: app.App.AppArn, AppVersion: aws.String("draft"), AssessmentName: aws.String("paged-assessment"),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		desc, descErr := client.DescribeAppAssessment(ctx, &resiliencehubsdk.DescribeAppAssessmentInput{
			AssessmentArn: started.Assessment.AssessmentArn,
		})
		require.NoError(t, descErr)

		return desc.Assessment.AssessmentStatus == types.AssessmentStatusSuccess
	}, defaultAsyncWait, defaultAsyncPoll)

	arn := started.Assessment.AssessmentArn

	var token *string

	got, pages := 0, 0

	for {
		out, listErr := client.ListAppComponentCompliances(ctx, &resiliencehubsdk.ListAppComponentCompliancesInput{
			AssessmentArn: arn, MaxResults: aws.Int32(2), NextToken: token,
		})
		require.NoError(t, listErr)

		got += len(out.ComponentCompliances)
		pages++

		if out.NextToken == nil {
			break
		}

		token = out.NextToken
	}

	require.Equal(t, 3, got)
	require.Equal(t, 2, pages)

	tests := []struct {
		call func() error
		name string
	}{
		{name: "alarm_recommendations", call: func() error {
			_, e := client.ListAlarmRecommendations(ctx, &resiliencehubsdk.ListAlarmRecommendationsInput{
				AssessmentArn: arn, NextToken: aws.String("%%%"),
			})

			return e
		}},
		{name: "component_recommendations", call: func() error {
			_, e := client.ListAppComponentRecommendations(ctx, &resiliencehubsdk.ListAppComponentRecommendationsInput{
				AssessmentArn: arn, NextToken: aws.String("%%%"),
			})

			return e
		}},
		{name: "compliance_drifts", call: func() error {
			_, e := client.ListAppAssessmentComplianceDrifts(
				ctx,
				&resiliencehubsdk.ListAppAssessmentComplianceDriftsInput{
					AssessmentArn: arn, NextToken: aws.String("%%%"),
				},
			)

			return e
		}},
		{name: "metrics", call: func() error {
			_, e := client.ListMetrics(ctx, &resiliencehubsdk.ListMetricsInput{NextToken: aws.String("%%%")})

			return e
		}},
		{name: "grouping_recommendations", call: func() error {
			_, e := client.ListResourceGroupingRecommendations(
				ctx,
				&resiliencehubsdk.ListResourceGroupingRecommendationsInput{
					AppArn: app.App.AppArn, NextToken: aws.String("%%%"),
				},
			)

			return e
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var apiErr smithy.APIError

			require.ErrorAs(t, tt.call(), &apiErr)
			require.Equal(t, "ValidationException", apiErr.ErrorCode())
		})
	}
}
