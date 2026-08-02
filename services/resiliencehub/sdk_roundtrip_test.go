package resiliencehub_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	resiliencehubsdk "github.com/aws/aws-sdk-go-v2/service/resiliencehub"
	"github.com/aws/aws-sdk-go-v2/service/resiliencehub/types"
	"github.com/stretchr/testify/require"
)

// TestRoundTrip_AppLifecycle drives the real SDK client through
// Create/Describe/Update/List/Delete for App -- the core CRUD surface every
// other operation family depends on.
func TestRoundTrip_AppLifecycle(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	created, err := client.CreateApp(ctx, &resiliencehubsdk.CreateAppInput{
		Name:        aws.String("my-app"),
		Description: aws.String("test app"),
	})
	require.NoError(t, err)
	require.NotNil(t, created.App)
	require.NotEmpty(t, aws.ToString(created.App.AppArn))
	require.Equal(t, "my-app", aws.ToString(created.App.Name))
	require.Equal(t, types.AppStatusTypeActive, created.App.Status)

	appArn := created.App.AppArn

	described, err := client.DescribeApp(ctx, &resiliencehubsdk.DescribeAppInput{AppArn: appArn})
	require.NoError(t, err)
	require.Equal(t, "test app", aws.ToString(described.App.Description))

	updated, err := client.UpdateApp(ctx, &resiliencehubsdk.UpdateAppInput{
		AppArn:      appArn,
		Description: aws.String("updated description"),
	})
	require.NoError(t, err)
	require.Equal(t, "updated description", aws.ToString(updated.App.Description))

	listed, err := client.ListApps(ctx, &resiliencehubsdk.ListAppsInput{})
	require.NoError(t, err)
	require.Len(t, listed.AppSummaries, 1)
	require.Equal(t, aws.ToString(appArn), aws.ToString(listed.AppSummaries[0].AppArn))

	_, err = client.DeleteApp(ctx, &resiliencehubsdk.DeleteAppInput{AppArn: appArn})
	require.NoError(t, err)

	_, err = client.DescribeApp(ctx, &resiliencehubsdk.DescribeAppInput{AppArn: appArn})
	require.Error(t, err)

	var nf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)
}

// TestRoundTrip_ResiliencyPolicyLifecycle drives Create/Describe/Update/List/
// Delete for ResiliencyPolicy, including CreateApp binding a policy by ARN.
func TestRoundTrip_ResiliencyPolicyLifecycle(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	policy := map[string]types.FailurePolicy{
		"Software": {RtoInSecs: 600, RpoInSecs: 600},
		"Hardware": {RtoInSecs: 600, RpoInSecs: 600},
		"AZ":       {RtoInSecs: 3600, RpoInSecs: 3600},
		"Region":   {RtoInSecs: 86400, RpoInSecs: 86400},
	}

	created, err := client.CreateResiliencyPolicy(ctx, &resiliencehubsdk.CreateResiliencyPolicyInput{
		PolicyName: aws.String("my-policy"),
		Tier:       types.ResiliencyPolicyTierCritical,
		Policy:     policy,
	})
	require.NoError(t, err)
	require.NotNil(t, created.Policy)
	policyArn := created.Policy.PolicyArn

	described, err := client.DescribeResiliencyPolicy(
		ctx,
		&resiliencehubsdk.DescribeResiliencyPolicyInput{PolicyArn: policyArn},
	)
	require.NoError(t, err)
	require.Equal(t, types.ResiliencyPolicyTierCritical, described.Policy.Tier)
	require.Len(t, described.Policy.Policy, 4)

	updated, err := client.UpdateResiliencyPolicy(ctx, &resiliencehubsdk.UpdateResiliencyPolicyInput{
		PolicyArn: policyArn,
		Tier:      types.ResiliencyPolicyTierMissionCritical,
	})
	require.NoError(t, err)
	require.Equal(t, types.ResiliencyPolicyTierMissionCritical, updated.Policy.Tier)

	appCreated, err := client.CreateApp(ctx, &resiliencehubsdk.CreateAppInput{
		Name:      aws.String("bound-app"),
		PolicyArn: policyArn,
	})
	require.NoError(t, err)
	require.Equal(t, aws.ToString(policyArn), aws.ToString(appCreated.App.PolicyArn))

	// Deleting a bound policy is rejected (ConflictException).
	_, err = client.DeleteResiliencyPolicy(ctx, &resiliencehubsdk.DeleteResiliencyPolicyInput{PolicyArn: policyArn})
	require.Error(t, err)

	var conflict *types.ConflictException
	require.ErrorAs(t, err, &conflict)

	// Clear the binding, then deletion succeeds.
	trueVal := true
	_, err = client.UpdateApp(ctx, &resiliencehubsdk.UpdateAppInput{
		AppArn:                   appCreated.App.AppArn,
		ClearResiliencyPolicyArn: &trueVal,
	})
	require.NoError(t, err)

	_, err = client.DeleteResiliencyPolicy(ctx, &resiliencehubsdk.DeleteResiliencyPolicyInput{PolicyArn: policyArn})
	require.NoError(t, err)

	listed, err := client.ListResiliencyPolicies(ctx, &resiliencehubsdk.ListResiliencyPoliciesInput{})
	require.NoError(t, err)
	require.Empty(t, listed.ResiliencyPolicies)

	suggested, err := client.ListSuggestedResiliencyPolicies(
		ctx,
		&resiliencehubsdk.ListSuggestedResiliencyPoliciesInput{},
	)
	require.NoError(t, err)
	require.NotEmpty(t, suggested.ResiliencyPolicies)
}

// TestRoundTrip_AppVersionWorkflow drives the draft-version workflow: app
// components, resources, resource mappings, resolution, publish, and
// template read/write.
func TestRoundTrip_AppVersionWorkflow(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	appOut, err := client.CreateApp(ctx, &resiliencehubsdk.CreateAppInput{Name: aws.String("versioned-app")})
	require.NoError(t, err)
	appArn := appOut.App.AppArn

	compOut, err := client.CreateAppVersionAppComponent(ctx, &resiliencehubsdk.CreateAppVersionAppComponentInput{
		AppArn: appArn, Name: aws.String("comp1"), Type: aws.String("AWS::ResilienceHub::AppComponent"),
	})
	require.NoError(t, err)
	require.Equal(t, "draft", aws.ToString(compOut.AppVersion))
	require.NotEmpty(t, aws.ToString(compOut.AppComponent.Id))

	resOut, err := client.CreateAppVersionResource(ctx, &resiliencehubsdk.CreateAppVersionResourceInput{
		AppArn:             appArn,
		AppComponents:      []string{"comp1"},
		PhysicalResourceId: aws.String("arn:aws:lambda:us-east-1:000000000000:function:my-fn"),
		ResourceType:       aws.String("AWS::Lambda::Function"),
		ResourceName:       aws.String("my-fn-resource"),
		LogicalResourceId:  &types.LogicalResourceId{Identifier: aws.String("my-fn-resource")},
	})
	require.NoError(t, err)
	require.Equal(t, "AWS::Lambda::Function", aws.ToString(resOut.PhysicalResource.ResourceType))

	listResources, err := client.ListAppVersionResources(ctx, &resiliencehubsdk.ListAppVersionResourcesInput{
		AppArn: appArn, AppVersion: aws.String("draft"),
	})
	require.NoError(t, err)
	require.Len(t, listResources.PhysicalResources, 1)

	// Add a resource mapping of type Resource, then resolve.
	_, err = client.AddDraftAppVersionResourceMappings(ctx, &resiliencehubsdk.AddDraftAppVersionResourceMappingsInput{
		AppArn: appArn,
		ResourceMappings: []types.ResourceMapping{
			{
				MappingType:  types.ResourceMappingTypeResource,
				ResourceName: aws.String("mapped-resource"),
				PhysicalResourceId: &types.PhysicalResourceId{
					Identifier: aws.String("arn:aws:sns:us-east-1:000000000000:topic:my-topic"),
					Type:       types.PhysicalIdentifierTypeArn,
				},
			},
		},
	})
	require.NoError(t, err)

	resolveOut, err := client.ResolveAppVersionResources(ctx, &resiliencehubsdk.ResolveAppVersionResourcesInput{
		AppArn: appArn, AppVersion: aws.String("draft"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(resolveOut.ResolutionId))

	require.Eventually(t, func() bool {
		status, statusErr := client.DescribeAppVersionResourcesResolutionStatus(
			ctx, &resiliencehubsdk.DescribeAppVersionResourcesResolutionStatusInput{
				AppArn: appArn, AppVersion: aws.String("draft"), ResolutionId: resolveOut.ResolutionId,
			},
		)
		require.NoError(t, statusErr)

		return status.Status == types.ResourceResolutionStatusTypeSuccess
	}, defaultAsyncWait, defaultAsyncPoll)

	// Publish the draft into version "1".
	published, err := client.PublishAppVersion(ctx, &resiliencehubsdk.PublishAppVersionInput{AppArn: appArn})
	require.NoError(t, err)
	require.Equal(t, "1", aws.ToString(published.AppVersion))
	require.Equal(t, int64(1), aws.ToInt64(published.Identifier))

	versions, err := client.ListAppVersions(ctx, &resiliencehubsdk.ListAppVersionsInput{AppArn: appArn})
	require.NoError(t, err)
	require.Len(t, versions.AppVersions, 2) // draft + published "1"

	// Template read/write.
	_, err = client.PutDraftAppVersionTemplate(ctx, &resiliencehubsdk.PutDraftAppVersionTemplateInput{
		AppArn: appArn, AppTemplateBody: aws.String(`{"Resources":{}}`),
	})
	require.NoError(t, err)

	tmpl, err := client.DescribeAppVersionTemplate(ctx, &resiliencehubsdk.DescribeAppVersionTemplateInput{
		AppArn: appArn, AppVersion: aws.String("draft"),
	})
	require.NoError(t, err)
	require.JSONEq(t, `{"Resources":{}}`, aws.ToString(tmpl.AppTemplateBody))
}

// TestRoundTrip_AssessmentLifecycle drives StartAppAssessment through its
// async Pending -> InProgress -> Success transition, then Describe/List/Delete.
func TestRoundTrip_AssessmentLifecycle(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	appOut, err := client.CreateApp(ctx, &resiliencehubsdk.CreateAppInput{Name: aws.String("assessed-app")})
	require.NoError(t, err)
	appArn := appOut.App.AppArn

	started, err := client.StartAppAssessment(ctx, &resiliencehubsdk.StartAppAssessmentInput{
		AppArn: appArn, AppVersion: aws.String("draft"), AssessmentName: aws.String("assessment-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, started.Assessment)
	assessmentArn := started.Assessment.AssessmentArn
	require.Nil(t, started.Assessment.Summary, "AssessmentSummary must never be fabricated")

	require.Eventually(t, func() bool {
		desc, descErr := client.DescribeAppAssessment(
			ctx,
			&resiliencehubsdk.DescribeAppAssessmentInput{AssessmentArn: assessmentArn},
		)
		require.NoError(t, descErr)

		return desc.Assessment.AssessmentStatus == types.AssessmentStatusSuccess
	}, defaultAsyncWait, defaultAsyncPoll)

	final, err := client.DescribeAppAssessment(
		ctx,
		&resiliencehubsdk.DescribeAppAssessmentInput{AssessmentArn: assessmentArn},
	)
	require.NoError(t, err)
	require.Nil(t, final.Assessment.Summary)
	require.Equal(t, types.ComplianceStatusMissingPolicy, final.Assessment.ComplianceStatus)

	listed, err := client.ListAppAssessments(ctx, &resiliencehubsdk.ListAppAssessmentsInput{AppArn: appArn})
	require.NoError(t, err)
	require.Len(t, listed.AssessmentSummaries, 1)

	_, err = client.DeleteAppAssessment(ctx, &resiliencehubsdk.DeleteAppAssessmentInput{AssessmentArn: assessmentArn})
	require.NoError(t, err)
}

// TestRoundTrip_RecommendationsAlwaysEmpty verifies every recommendation
// family returns a real, empty (never fabricated) list.
func TestRoundTrip_RecommendationsAlwaysEmpty(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	appOut, err := client.CreateApp(ctx, &resiliencehubsdk.CreateAppInput{Name: aws.String("rec-app")})
	require.NoError(t, err)

	started, err := client.StartAppAssessment(ctx, &resiliencehubsdk.StartAppAssessmentInput{
		AppArn: appOut.App.AppArn, AppVersion: aws.String("draft"), AssessmentName: aws.String("a1"),
	})
	require.NoError(t, err)

	assessmentArn := started.Assessment.AssessmentArn

	alarms, err := client.ListAlarmRecommendations(
		ctx,
		&resiliencehubsdk.ListAlarmRecommendationsInput{AssessmentArn: assessmentArn},
	)
	require.NoError(t, err)
	require.Empty(t, alarms.AlarmRecommendations)

	sops, err := client.ListSopRecommendations(
		ctx,
		&resiliencehubsdk.ListSopRecommendationsInput{AssessmentArn: assessmentArn},
	)
	require.NoError(t, err)
	require.Empty(t, sops.SopRecommendations)

	tests, err := client.ListTestRecommendations(
		ctx,
		&resiliencehubsdk.ListTestRecommendationsInput{AssessmentArn: assessmentArn},
	)
	require.NoError(t, err)
	require.Empty(t, tests.TestRecommendations)

	comps, err := client.ListAppComponentRecommendations(
		ctx, &resiliencehubsdk.ListAppComponentRecommendationsInput{AssessmentArn: assessmentArn},
	)
	require.NoError(t, err)
	require.Empty(t, comps.ComponentRecommendations)

	excluded := true
	batch, err := client.BatchUpdateRecommendationStatus(ctx, &resiliencehubsdk.BatchUpdateRecommendationStatusInput{
		AppArn: appOut.App.AppArn,
		RequestEntries: []types.UpdateRecommendationStatusRequestEntry{
			{EntryId: aws.String("e1"), ReferenceId: aws.String("ref1"), Excluded: &excluded},
		},
	})
	require.NoError(t, err)
	require.Empty(t, batch.SuccessfulEntries)
	require.Len(t, batch.FailedEntries, 1)
}

// TestRoundTrip_RecommendationTemplate drives Create/List/Delete for
// RecommendationTemplate.
func TestRoundTrip_RecommendationTemplate(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	appOut, err := client.CreateApp(ctx, &resiliencehubsdk.CreateAppInput{Name: aws.String("template-app")})
	require.NoError(t, err)

	started, err := client.StartAppAssessment(ctx, &resiliencehubsdk.StartAppAssessmentInput{
		AppArn: appOut.App.AppArn, AppVersion: aws.String("draft"), AssessmentName: aws.String("a1"),
	})
	require.NoError(t, err)

	created, err := client.CreateRecommendationTemplate(ctx, &resiliencehubsdk.CreateRecommendationTemplateInput{
		AssessmentArn: started.Assessment.AssessmentArn, Name: aws.String("my-template"),
	})
	require.NoError(t, err)
	require.Equal(t, types.RecommendationTemplateStatusSuccess, created.RecommendationTemplate.Status)

	listed, err := client.ListRecommendationTemplates(ctx, &resiliencehubsdk.ListRecommendationTemplatesInput{})
	require.NoError(t, err)
	require.Len(t, listed.RecommendationTemplates, 1)

	_, err = client.DeleteRecommendationTemplate(ctx, &resiliencehubsdk.DeleteRecommendationTemplateInput{
		RecommendationTemplateArn: created.RecommendationTemplate.RecommendationTemplateArn,
	})
	require.NoError(t, err)
}

// TestRoundTrip_ResourceGroupingTask drives the resource-grouping
// recommendation task family, confirming it always completes with zero
// generated recommendations.
func TestRoundTrip_ResourceGroupingTask(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	appOut, err := client.CreateApp(ctx, &resiliencehubsdk.CreateAppInput{Name: aws.String("grouping-app")})
	require.NoError(t, err)
	appArn := appOut.App.AppArn

	started, err := client.StartResourceGroupingRecommendationTask(
		ctx, &resiliencehubsdk.StartResourceGroupingRecommendationTaskInput{AppArn: appArn},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(started.GroupingId))

	require.Eventually(t, func() bool {
		desc, descErr := client.DescribeResourceGroupingRecommendationTask(
			ctx, &resiliencehubsdk.DescribeResourceGroupingRecommendationTaskInput{
				AppArn: appArn, GroupingId: started.GroupingId,
			},
		)
		require.NoError(t, descErr)

		return desc.Status == types.ResourcesGroupingRecGenStatusTypeSuccess
	}, defaultAsyncWait, defaultAsyncPoll)

	listed, err := client.ListResourceGroupingRecommendations(
		ctx, &resiliencehubsdk.ListResourceGroupingRecommendationsInput{AppArn: appArn},
	)
	require.NoError(t, err)
	require.Empty(t, listed.GroupingRecommendations)

	accepted, err := client.AcceptResourceGroupingRecommendations(
		ctx,
		&resiliencehubsdk.AcceptResourceGroupingRecommendationsInput{
			AppArn:  appArn,
			Entries: []types.AcceptGroupingRecommendationEntry{{GroupingRecommendationId: aws.String("nonexistent")}},
		},
	)
	require.NoError(t, err)
	require.Len(t, accepted.FailedEntries, 1)
}

// TestRoundTrip_MetricsExport drives StartMetricsExport through its async
// transition, then DescribeMetricsExport, plus ListMetrics's honestly-empty
// result.
func TestRoundTrip_MetricsExport(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	metrics, err := client.ListMetrics(ctx, &resiliencehubsdk.ListMetricsInput{})
	require.NoError(t, err)
	require.Empty(t, metrics.Rows)

	started, err := client.StartMetricsExport(
		ctx,
		&resiliencehubsdk.StartMetricsExportInput{BucketName: aws.String("my-bucket")},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(started.MetricsExportId))

	require.Eventually(t, func() bool {
		desc, descErr := client.DescribeMetricsExport(
			ctx, &resiliencehubsdk.DescribeMetricsExportInput{MetricsExportId: started.MetricsExportId},
		)
		require.NoError(t, descErr)

		return desc.Status == types.MetricsExportStatusTypeSuccess
	}, defaultAsyncWait, defaultAsyncPoll)
}

// TestRoundTrip_Tagging drives TagResource/ListTagsForResource/UntagResource
// against an App ARN through the real SDK client (exercising the ARN-in-path
// /tags/{resourceArn} route).
func TestRoundTrip_Tagging(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	appOut, err := client.CreateApp(ctx, &resiliencehubsdk.CreateAppInput{Name: aws.String("tagged-app")})
	require.NoError(t, err)
	appArn := appOut.App.AppArn

	_, err = client.TagResource(ctx, &resiliencehubsdk.TagResourceInput{
		ResourceArn: appArn, Tags: map[string]string{"env": "prod"},
	})
	require.NoError(t, err)

	listed, err := client.ListTagsForResource(ctx, &resiliencehubsdk.ListTagsForResourceInput{ResourceArn: appArn})
	require.NoError(t, err)
	require.Equal(t, "prod", listed.Tags["env"])

	// Cross-check against the backend's own resourcegroupstaggingapi
	// integration surface (see tagging.go's TaggedResources, cli.go's
	// wireTaggingResilienceHub).
	tagged := h.Backend.TaggedResources()
	require.Len(t, tagged, 1)
	require.Equal(t, aws.ToString(appArn), tagged[0].ARN)

	_, err = client.UntagResource(
		ctx,
		&resiliencehubsdk.UntagResourceInput{ResourceArn: appArn, TagKeys: []string{"env"}},
	)
	require.NoError(t, err)

	listed, err = client.ListTagsForResource(ctx, &resiliencehubsdk.ListTagsForResourceInput{ResourceArn: appArn})
	require.NoError(t, err)
	require.Empty(t, listed.Tags)
}

// TestRoundTrip_DeleteRecommendationTemplate_NoConflictException confirms
// DeleteRecommendationTemplate is the one op in this service that does NOT
// accept ConflictException (PARITY.md's sharpest per-op claim) -- deleting a
// nonexistent template must surface ResourceNotFoundException, never
// ConflictException.
func TestRoundTrip_DeleteRecommendationTemplate_NoConflictException(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.DeleteRecommendationTemplate(ctx, &resiliencehubsdk.DeleteRecommendationTemplateInput{
		RecommendationTemplateArn: aws.String(
			"arn:aws:resiliencehub:us-east-1:000000000000:recommendation-template/nonexistent",
		),
	})
	require.Error(t, err)

	var nf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)

	var conflict *types.ConflictException
	require.NotErrorAs(t, err, &conflict, "DeleteRecommendationTemplate must never surface ConflictException")
}
