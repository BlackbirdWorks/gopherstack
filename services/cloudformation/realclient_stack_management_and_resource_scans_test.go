package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_StackManagementAndResourceScans drives cloudformation's typed-coverage-blind
// ops (gopherstack-n3zi) through the real aws-sdk-go-v2 client.
func TestRealClient_StackManagementAndResourceScans(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "describe change set hooks", run: func(t *testing.T) {
			t.Helper()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			_, err := client.CreateStack(ctx, &cfnsdk.CreateStackInput{
				StackName:    aws.String("s11-hooks-stack"),
				TemplateBody: aws.String(simpleTemplate),
			})
			require.NoError(t, err)

			csOut, err := client.CreateChangeSet(ctx, &cfnsdk.CreateChangeSetInput{
				StackName:     aws.String("s11-hooks-stack"),
				ChangeSetName: aws.String("s11-hooks-cs"),
				TemplateBody:  aws.String(modifiedTemplate),
			})
			require.NoError(t, err)

			hooksOut, err := client.DescribeChangeSetHooks(ctx, &cfnsdk.DescribeChangeSetHooksInput{
				ChangeSetName: aws.String(aws.ToString(csOut.Id)),
			})
			require.NoError(t, err)
			assert.Empty(t, hooksOut.Hooks, "hook configurations aren't emulated; empty is the honest response")
		}},
		{name: "generated template describe and get", run: func(t *testing.T) {
			t.Helper()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			createOut, err := client.CreateGeneratedTemplate(ctx, &cfnsdk.CreateGeneratedTemplateInput{
				GeneratedTemplateName: aws.String("s11-generated-template"),
				Resources: []types.ResourceDefinition{
					{
						ResourceType:       aws.String("AWS::S3::Bucket"),
						LogicalResourceId:  aws.String("MyBucket"),
						ResourceIdentifier: map[string]string{"BucketName": "s11-generated-bucket"},
					},
				},
			})
			require.NoError(t, err)
			templateID := aws.ToString(createOut.GeneratedTemplateId)

			descOut, err := client.DescribeGeneratedTemplate(ctx, &cfnsdk.DescribeGeneratedTemplateInput{
				GeneratedTemplateName: aws.String(templateID),
			})
			require.NoError(t, err)
			assert.Equal(t, "s11-generated-template", aws.ToString(descOut.GeneratedTemplateName))
			assert.Equal(t, types.GeneratedTemplateStatusComplete, descOut.Status)

			getOut, err := client.GetGeneratedTemplate(ctx, &cfnsdk.GetGeneratedTemplateInput{
				GeneratedTemplateName: aws.String(templateID),
			})
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(getOut.TemplateBody), "MyBucket")
		}},
		{name: "organizations access", run: func(t *testing.T) {
			t.Helper()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			beforeOut, err := client.DescribeOrganizationsAccess(ctx, &cfnsdk.DescribeOrganizationsAccessInput{})
			require.NoError(t, err)
			assert.Equal(t, types.OrganizationStatusDisabled, beforeOut.Status)

			_, err = client.ActivateOrganizationsAccess(ctx, &cfnsdk.ActivateOrganizationsAccessInput{})
			require.NoError(t, err)

			afterOut, err := client.DescribeOrganizationsAccess(ctx, &cfnsdk.DescribeOrganizationsAccessInput{})
			require.NoError(t, err)
			assert.Equal(t, types.OrganizationStatusEnabled, afterOut.Status)
		}},
		{name: "resource scan describe and related resources", run: func(t *testing.T) {
			t.Helper()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			startOut, err := client.StartResourceScan(ctx, &cfnsdk.StartResourceScanInput{})
			require.NoError(t, err)
			scanID := aws.ToString(startOut.ResourceScanId)
			require.NotEmpty(t, scanID)

			descOut, err := client.DescribeResourceScan(ctx, &cfnsdk.DescribeResourceScanInput{
				ResourceScanId: aws.String(scanID),
			})
			require.NoError(t, err)
			assert.Equal(t, types.ResourceScanStatusComplete, descOut.Status)
			assert.InDelta(t, 100.0, aws.ToFloat64(descOut.PercentageCompleted), 0.001)

			relatedOut, err := client.ListResourceScanRelatedResources(
				ctx,
				&cfnsdk.ListResourceScanRelatedResourcesInput{
					ResourceScanId: aws.String(scanID),
					Resources: []types.ScannedResourceIdentifier{
						{
							ResourceType:       aws.String("AWS::S3::Bucket"),
							ResourceIdentifier: map[string]string{"Id": "example-bucket"},
						},
					},
				},
			)
			require.NoError(t, err)

			assert.Empty(t, relatedOut.RelatedResources)
		}},
		{name: "type registration and test type", run: func(t *testing.T) {
			t.Helper()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			regOut, err := client.RegisterType(ctx, &cfnsdk.RegisterTypeInput{
				TypeName:             aws.String("S11Company::S11Service::S11Type"),
				SchemaHandlerPackage: aws.String("s3://bucket/schema.zip"),
			})
			require.NoError(t, err)
			token := aws.ToString(regOut.RegistrationToken)
			require.NotEmpty(t, token)

			descOut, err := client.DescribeTypeRegistration(ctx, &cfnsdk.DescribeTypeRegistrationInput{
				RegistrationToken: aws.String(token),
			})
			require.NoError(t, err)
			assert.Equal(t, types.RegistrationStatusComplete, descOut.ProgressStatus)
			require.NotNil(t, descOut.TypeArn,
				"real AWS: TypeArn is populated once ProgressStatus reaches COMPLETE")
			assert.Contains(t, aws.ToString(descOut.TypeArn), "S11Company::S11Service::S11Type")

			testOut, err := client.TestType(ctx, &cfnsdk.TestTypeInput{
				TypeName: aws.String("S11Company::S11Service::S11Type"),
			})
			require.NoError(t, err)
			testToken := aws.ToString(testOut.TypeVersionArn)
			require.NotEmpty(t, testToken)
		}},
		{name: "signal resource", run: func(t *testing.T) {
			t.Helper()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			_, err := client.CreateStack(ctx, &cfnsdk.CreateStackInput{
				StackName:    aws.String("s11-signal-stack"),
				TemplateBody: aws.String(simpleTemplate),
			})
			require.NoError(t, err)

			_, err = client.SignalResource(ctx, &cfnsdk.SignalResourceInput{
				StackName:         aws.String("s11-signal-stack"),
				LogicalResourceId: aws.String("MyBucket"),
				UniqueId:          aws.String("s11-unique-id"),
				Status:            types.ResourceSignalStatusSuccess,
			})
			require.NoError(t, err)

			_, err = client.SignalResource(ctx, &cfnsdk.SignalResourceInput{
				StackName:         aws.String("s11-missing-stack"),
				LogicalResourceId: aws.String("MyBucket"),
				UniqueId:          aws.String("s11-unique-id"),
				Status:            types.ResourceSignalStatusSuccess,
			})
			assert.Error(t, err, "SignalResource against an unknown stack must fail")
		}},
		{name: "list hook results", run: func(t *testing.T) {
			t.Helper()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			out, err := client.ListHookResults(ctx, &cfnsdk.ListHookResultsInput{
				TargetType: types.ListHookResultsTargetTypeChangeSet,
				TargetId:   aws.String("s11-hook-target"),
			})
			require.NoError(t, err)
			assert.Empty(t, out.HookResults,
				"hook execution results aren't emulated; empty is the honest response")
		}},
		{name: "execute stack refactor", run: func(t *testing.T) {
			t.Helper()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			_, err := client.CreateStack(ctx, &cfnsdk.CreateStackInput{
				StackName:    aws.String("s11-refactor-src"),
				TemplateBody: aws.String(simpleTemplate),
			})
			require.NoError(t, err)

			refactorOut, err := client.CreateStackRefactor(ctx, &cfnsdk.CreateStackRefactorInput{
				Description: aws.String("s11 refactor"),
				StackDefinitions: []types.StackDefinition{
					{StackName: aws.String("s11-refactor-dst"), TemplateBody: aws.String(simpleTemplate)},
				},
				ResourceMappings: []types.ResourceMapping{
					{
						Source: &types.ResourceLocation{
							StackName:         aws.String("s11-refactor-src"),
							LogicalResourceId: aws.String("MyBucket"),
						},
						Destination: &types.ResourceLocation{
							StackName:         aws.String("s11-refactor-dst"),
							LogicalResourceId: aws.String("MyBucket"),
						},
					},
				},
				EnableStackCreation: aws.Bool(true),
			})
			require.NoError(t, err)
			refactorID := aws.ToString(refactorOut.StackRefactorId)
			require.NotEmpty(t, refactorID)

			_, err = client.ExecuteStackRefactor(ctx, &cfnsdk.ExecuteStackRefactorInput{
				StackRefactorId: aws.String(refactorID),
			})
			require.NoError(t, err)

			dstResources, err := client.DescribeStackResources(ctx, &cfnsdk.DescribeStackResourcesInput{
				StackName: aws.String("s11-refactor-dst"),
			})
			require.NoError(t, err)
			require.Len(t, dstResources.StackResources, 1)
			assert.Equal(t, "MyBucket", aws.ToString(dstResources.StackResources[0].LogicalResourceId))

			srcResources, err := client.DescribeStackResources(ctx, &cfnsdk.DescribeStackResourcesInput{
				StackName: aws.String("s11-refactor-src"),
			})
			require.NoError(t, err)
			assert.Empty(t, srcResources.StackResources, "the resource must be moved out of the source stack")
		}},
		{name: "update stack instances", run: func(t *testing.T) {
			t.Helper()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			stackSetName := "s11-update-instances-stackset"
			_, err := client.CreateStackSet(ctx, &cfnsdk.CreateStackSetInput{
				StackSetName: aws.String(stackSetName),
				TemplateBody: aws.String(simpleTemplate),
			})
			require.NoError(t, err)

			_, err = client.CreateStackInstances(ctx, &cfnsdk.CreateStackInstancesInput{
				StackSetName: aws.String(stackSetName),
				Accounts:     []string{"123456789012"},
				Regions:      []string{"us-east-1"},
			})
			require.NoError(t, err)

			updOut, err := client.UpdateStackInstances(ctx, &cfnsdk.UpdateStackInstancesInput{
				StackSetName: aws.String(stackSetName),
				Accounts:     []string{"123456789012"},
				Regions:      []string{"us-east-1"},
			})
			require.NoError(t, err)
			opID := aws.ToString(updOut.OperationId)
			require.NotEmpty(t, opID)

			opOut, err := client.DescribeStackSetOperation(ctx, &cfnsdk.DescribeStackSetOperationInput{
				StackSetName: aws.String(stackSetName),
				OperationId:  aws.String(opID),
			})
			require.NoError(t, err)

			assert.Equal(t, types.StackSetOperationActionUpdate, opOut.StackSetOperation.Action)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
