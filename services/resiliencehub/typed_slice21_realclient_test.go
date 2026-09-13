package resiliencehub_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	resiliencehubsdk "github.com/aws/aws-sdk-go-v2/service/resiliencehub"
	"github.com/aws/aws-sdk-go-v2/service/resiliencehub/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypedSlice21RealClient drives resiliencehub's remaining
// typed-coverage-blind ops (gopherstack-n3zi slice 21) through the real
// aws-sdk-go-v2 client: DeleteAppInputSource, DescribeAppVersion,
// DescribeAppVersionAppComponent, DescribeAppVersionResource,
// ListAppComponentCompliances, ListAppInputSources,
// ListAppVersionAppComponents, ListAppVersionResourceMappings,
// RejectResourceGroupingRecommendations, RemoveDraftAppVersionResourceMappings,
// UpdateAppVersion.
func TestTypedSlice21RealClient(t *testing.T) {
	t.Parallel()

	t.Run("app version and app component", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClient(t)
		ctx := t.Context()

		appOut, err := client.CreateApp(
			ctx,
			&resiliencehubsdk.CreateAppInput{Name: aws.String("s21-app")},
		)
		require.NoError(t, err)
		appArn := appOut.App.AppArn

		descVerOut, err := client.DescribeAppVersion(ctx, &resiliencehubsdk.DescribeAppVersionInput{
			AppArn: appArn, AppVersion: aws.String("draft"),
		})
		require.NoError(t, err)
		assert.Equal(t, "draft", aws.ToString(descVerOut.AppVersion))
		assert.Equal(t, aws.ToString(appArn), aws.ToString(descVerOut.AppArn))

		updVerOut, err := client.UpdateAppVersion(ctx, &resiliencehubsdk.UpdateAppVersionInput{
			AppArn:         appArn,
			AdditionalInfo: map[string][]string{"note": {"s21"}},
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"s21"}, updVerOut.AdditionalInfo["note"])

		compOut, err := client.CreateAppVersionAppComponent(
			ctx, &resiliencehubsdk.CreateAppVersionAppComponentInput{
				AppArn: appArn, Name: aws.String("s21-comp"), Type: aws.String("AWS::ResilienceHub::AppComponent"),
			},
		)
		require.NoError(t, err)
		compID := compOut.AppComponent.Id

		descCompOut, err := client.DescribeAppVersionAppComponent(
			ctx, &resiliencehubsdk.DescribeAppVersionAppComponentInput{
				AppArn: appArn, AppVersion: aws.String("draft"), Id: compID,
			},
		)
		require.NoError(t, err)
		require.NotNil(t, descCompOut.AppComponent)
		assert.Equal(t, "s21-comp", aws.ToString(descCompOut.AppComponent.Name))

		listCompOut, err := client.ListAppVersionAppComponents(
			ctx, &resiliencehubsdk.ListAppVersionAppComponentsInput{
				AppArn: appArn, AppVersion: aws.String("draft"),
			},
		)
		require.NoError(t, err)
		require.Len(t, listCompOut.AppComponents, 1)
		assert.Equal(t, aws.ToString(compID), aws.ToString(listCompOut.AppComponents[0].Id))
	})

	t.Run("resource and mappings", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClient(t)
		ctx := t.Context()

		appOut, err := client.CreateApp(
			ctx,
			&resiliencehubsdk.CreateAppInput{Name: aws.String("s21-res-app")},
		)
		require.NoError(t, err)
		appArn := appOut.App.AppArn

		_, err = client.CreateAppVersionAppComponent(
			ctx,
			&resiliencehubsdk.CreateAppVersionAppComponentInput{
				AppArn: appArn, Name: aws.String("s21-res-comp"), Type: aws.String("AWS::ResilienceHub::AppComponent"),
			},
		)
		require.NoError(t, err)

		_, err = client.CreateAppVersionResource(
			ctx,
			&resiliencehubsdk.CreateAppVersionResourceInput{
				AppArn:        appArn,
				AppComponents: []string{"s21-res-comp"},
				PhysicalResourceId: aws.String(
					"arn:aws:lambda:us-east-1:000000000000:function:s21-fn",
				),
				ResourceType: aws.String("AWS::Lambda::Function"),
				ResourceName: aws.String("s21-fn-resource"),
				LogicalResourceId: &types.LogicalResourceId{
					Identifier: aws.String("s21-fn-resource"),
				},
			},
		)
		require.NoError(t, err)

		descResOut, err := client.DescribeAppVersionResource(
			ctx,
			&resiliencehubsdk.DescribeAppVersionResourceInput{
				AppArn: appArn, AppVersion: aws.String("draft"), ResourceName: aws.String("s21-fn-resource"),
			},
		)
		require.NoError(t, err)
		require.NotNil(t, descResOut.PhysicalResource)
		assert.Equal(
			t,
			"AWS::Lambda::Function",
			aws.ToString(descResOut.PhysicalResource.ResourceType),
		)

		_, err = client.AddDraftAppVersionResourceMappings(
			ctx, &resiliencehubsdk.AddDraftAppVersionResourceMappingsInput{
				AppArn: appArn,
				ResourceMappings: []types.ResourceMapping{
					{
						MappingType:  types.ResourceMappingTypeResource,
						ResourceName: aws.String("s21-mapped-resource"),
						PhysicalResourceId: &types.PhysicalResourceId{
							Identifier: aws.String(
								"arn:aws:sns:us-east-1:000000000000:topic:s21-topic",
							),
							Type: types.PhysicalIdentifierTypeArn,
						},
					},
				},
			},
		)
		require.NoError(t, err)

		listMapOut, err := client.ListAppVersionResourceMappings(
			ctx, &resiliencehubsdk.ListAppVersionResourceMappingsInput{
				AppArn: appArn, AppVersion: aws.String("draft"),
			},
		)
		require.NoError(t, err)
		require.Len(t, listMapOut.ResourceMappings, 1)
		assert.Equal(
			t,
			"s21-mapped-resource",
			aws.ToString(listMapOut.ResourceMappings[0].ResourceName),
		)

		_, err = client.RemoveDraftAppVersionResourceMappings(
			ctx, &resiliencehubsdk.RemoveDraftAppVersionResourceMappingsInput{
				AppArn:        appArn,
				ResourceNames: []string{"s21-mapped-resource"},
			},
		)
		require.NoError(t, err)

		listMapOut2, err := client.ListAppVersionResourceMappings(
			ctx, &resiliencehubsdk.ListAppVersionResourceMappingsInput{
				AppArn: appArn, AppVersion: aws.String("draft"),
			},
		)
		require.NoError(t, err)
		assert.Empty(t, listMapOut2.ResourceMappings)
	})

	t.Run("input sources", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClient(t)
		ctx := t.Context()

		appOut, err := client.CreateApp(
			ctx,
			&resiliencehubsdk.CreateAppInput{Name: aws.String("s21-input-app")},
		)
		require.NoError(t, err)
		appArn := appOut.App.AppArn

		_, err = client.ImportResourcesToDraftAppVersion(
			ctx, &resiliencehubsdk.ImportResourcesToDraftAppVersionInput{
				AppArn: appArn,
				SourceArns: []string{
					"arn:aws:cloudformation:us-east-1:000000000000:stack/s21-stack/abc",
				},
			},
		)
		require.NoError(t, err)

		listSrcOut, err := client.ListAppInputSources(
			ctx,
			&resiliencehubsdk.ListAppInputSourcesInput{
				AppArn: appArn, AppVersion: aws.String("draft"),
			},
		)
		require.NoError(t, err)
		require.Len(t, listSrcOut.AppInputSources, 1)
		sourceArn := listSrcOut.AppInputSources[0].SourceArn

		delOut, err := client.DeleteAppInputSource(ctx, &resiliencehubsdk.DeleteAppInputSourceInput{
			AppArn: appArn, SourceArn: sourceArn,
		})
		require.NoError(t, err)
		require.NotNil(t, delOut.AppInputSource)
		assert.Equal(t, aws.ToString(sourceArn), aws.ToString(delOut.AppInputSource.SourceArn))

		listSrcOut2, err := client.ListAppInputSources(
			ctx,
			&resiliencehubsdk.ListAppInputSourcesInput{
				AppArn: appArn, AppVersion: aws.String("draft"),
			},
		)
		require.NoError(t, err)
		assert.Empty(t, listSrcOut2.AppInputSources)
	})

	t.Run("component compliances", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClient(t)
		ctx := t.Context()

		appOut, err := client.CreateApp(
			ctx,
			&resiliencehubsdk.CreateAppInput{Name: aws.String("s21-compliance-app")},
		)
		require.NoError(t, err)
		appArn := appOut.App.AppArn

		_, err = client.CreateAppVersionAppComponent(
			ctx,
			&resiliencehubsdk.CreateAppVersionAppComponentInput{
				AppArn: appArn, Name: aws.String("s21-compliance-comp"),
				Type: aws.String("AWS::ResilienceHub::AppComponent"),
			},
		)
		require.NoError(t, err)

		started, err := client.StartAppAssessment(ctx, &resiliencehubsdk.StartAppAssessmentInput{
			AppArn: appArn, AppVersion: aws.String("draft"), AssessmentName: aws.String("s21-assessment"),
		})
		require.NoError(t, err)
		assessmentArn := started.Assessment.AssessmentArn

		require.Eventually(t, func() bool {
			desc, descErr := client.DescribeAppAssessment(
				ctx, &resiliencehubsdk.DescribeAppAssessmentInput{AssessmentArn: assessmentArn},
			)
			require.NoError(t, descErr)

			return desc.Assessment.AssessmentStatus == types.AssessmentStatusSuccess
		}, defaultAsyncWait, defaultAsyncPoll)

		compliOut, err := client.ListAppComponentCompliances(
			ctx, &resiliencehubsdk.ListAppComponentCompliancesInput{AssessmentArn: assessmentArn},
		)
		require.NoError(t, err)
		require.Len(t, compliOut.ComponentCompliances, 1)
		assert.Equal(
			t,
			"s21-compliance-comp",
			aws.ToString(compliOut.ComponentCompliances[0].AppComponentName),
		)
	})

	t.Run("reject resource grouping recommendations", func(t *testing.T) {
		t.Parallel()

		_, client := newTestHandlerAndClient(t)
		ctx := t.Context()

		appOut, err := client.CreateApp(
			ctx,
			&resiliencehubsdk.CreateAppInput{Name: aws.String("s21-grouping-app")},
		)
		require.NoError(t, err)
		appArn := appOut.App.AppArn

		// This backend has no resource-grouping ML output to reject against
		// (documented, honest gap -- RejectResourceGroupingRecommendations
		// always reports every requested id as a FailedEntry), so this
		// proves the real wire shape of that failure response rather than a
		// successful exclusion.
		rejectOut, err := client.RejectResourceGroupingRecommendations(
			ctx, &resiliencehubsdk.RejectResourceGroupingRecommendationsInput{
				AppArn: appArn,
				Entries: []types.RejectGroupingRecommendationEntry{
					{GroupingRecommendationId: aws.String("s21-grouping-rec-1")},
				},
			},
		)
		require.NoError(t, err)
		require.Len(t, rejectOut.FailedEntries, 1)
		assert.Equal(
			t,
			"s21-grouping-rec-1",
			aws.ToString(rejectOut.FailedEntries[0].GroupingRecommendationId),
		)
		assert.NotEmpty(t, aws.ToString(rejectOut.FailedEntries[0].ErrorMessage))
	})
}
