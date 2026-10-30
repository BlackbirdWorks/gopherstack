package resiliencehub_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	resiliencehubsdk "github.com/aws/aws-sdk-go-v2/service/resiliencehub"
	"github.com/aws/aws-sdk-go-v2/service/resiliencehub/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransitionDelay_Assessment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		delay     time.Duration
		wantStays bool
	}{
		{name: "long delay stays pending", delay: time.Hour, wantStays: true},
		{name: "short delay settles", delay: 20 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				h, client := newInMemoryHandlerAndClient(t)
				h.Backend.SetTransitionDelay(tt.delay)
				ctx := t.Context()

				app, err := client.CreateApp(ctx, &resiliencehubsdk.CreateAppInput{Name: aws.String("app")})
				require.NoError(t, err)

				started, err := client.StartAppAssessment(ctx, &resiliencehubsdk.StartAppAssessmentInput{
					AppArn: app.App.AppArn, AppVersion: aws.String("draft"), AssessmentName: aws.String("a"),
				})
				require.NoError(t, err)

				status := func() types.AssessmentStatus {
					out, dErr := client.DescribeAppAssessment(ctx, &resiliencehubsdk.DescribeAppAssessmentInput{
						AssessmentArn: started.Assessment.AssessmentArn,
					})
					require.NoError(t, dErr)

					return out.Assessment.AssessmentStatus
				}

				if tt.wantStays {
					assert.Equal(t, types.AssessmentStatusPending, status())

					return
				}

				time.Sleep(10 * tt.delay)
				synctest.Wait()

				assert.Equal(t, types.AssessmentStatusSuccess, status())
			})
		})
	}
}

func TestReleaseVersionAlias(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	app, err := client.CreateApp(ctx, &resiliencehubsdk.CreateAppInput{Name: aws.String("app")})
	require.NoError(t, err)

	start := func() error {
		_, sErr := client.StartAppAssessment(ctx, &resiliencehubsdk.StartAppAssessmentInput{
			AppArn: app.App.AppArn, AppVersion: aws.String("release"), AssessmentName: aws.String("a"),
		})

		return sErr
	}

	var notFound *types.ResourceNotFoundException

	require.ErrorAs(t, start(), &notFound, "no release before the first publish")

	_, err = client.PublishAppVersion(ctx, &resiliencehubsdk.PublishAppVersionInput{AppArn: app.App.AppArn})
	require.NoError(t, err)
	require.NoError(t, start())
}

func TestListValidation(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	tests := []struct {
		call func() error
		name string
	}{
		{
			name: "apps bad token",
			call: func() error {
				_, err := client.ListApps(ctx, &resiliencehubsdk.ListAppsInput{NextToken: aws.String("garbage")})

				return err
			},
		},
		{
			name: "apps max results too large",
			call: func() error {
				_, err := client.ListApps(ctx, &resiliencehubsdk.ListAppsInput{MaxResults: aws.Int32(101)})

				return err
			},
		},
		{
			name: "policies bad token",
			call: func() error {
				_, err := client.ListResiliencyPolicies(
					ctx, &resiliencehubsdk.ListResiliencyPoliciesInput{NextToken: aws.String("garbage")},
				)

				return err
			},
		},
		{
			name: "assessments unknown status",
			call: func() error {
				_, err := client.ListAppAssessments(ctx, &resiliencehubsdk.ListAppAssessmentsInput{
					AssessmentStatus: []types.AssessmentStatus{"Bogus"},
				})

				return err
			},
		},
		{
			name: "app versions bad token",
			call: func() error {
				_, err := client.ListAppVersions(ctx, &resiliencehubsdk.ListAppVersionsInput{
					AppArn: aws.String(
						"arn:aws:resiliencehub:us-east-1:000000000000:app/x",
					),
					NextToken: aws.String("garbage"),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var apiErr smithy.APIError

			require.ErrorAs(t, tt.call(), &apiErr)
			assert.Equal(t, "ValidationException", apiErr.ErrorCode())
		})
	}
}
