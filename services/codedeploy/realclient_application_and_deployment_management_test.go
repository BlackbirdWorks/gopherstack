package codedeploy_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/codedeploy"
	"github.com/aws/aws-sdk-go-v2/service/codedeploy/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_ApplicationAndDeploymentManagement drives every
// gopherstack-n3zi uncovered codedeploy op through the real aws-sdk-go-v2
// client.
func TestRealClient_ApplicationAndDeploymentManagement(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "applications", run: func(t *testing.T) {
			t.Helper()

			h := newTestHandler(t)
			client := newTestCodeDeployClient(t, h)
			ctx := t.Context()

			_, err := client.CreateApplication(
				ctx,
				&codedeploy.CreateApplicationInput{ApplicationName: aws.String("app-a")},
			)
			require.NoError(t, err)
			_, err = client.CreateApplication(
				ctx,
				&codedeploy.CreateApplicationInput{ApplicationName: aws.String("app-b")},
			)
			require.NoError(t, err)

			batchOut, err := client.BatchGetApplications(ctx, &codedeploy.BatchGetApplicationsInput{
				ApplicationNames: []string{"app-a", "app-b"},
			})
			require.NoError(t, err)
			require.Len(t, batchOut.ApplicationsInfo, 2)

			names := make([]string, 0, 2)
			for _, a := range batchOut.ApplicationsInfo {
				names = append(names, aws.ToString(a.ApplicationName))
			}
			assert.ElementsMatch(t, []string{"app-a", "app-b"}, names)

			_, err = client.UpdateApplication(ctx, &codedeploy.UpdateApplicationInput{
				ApplicationName:    aws.String("app-a"),
				NewApplicationName: aws.String("app-a-renamed"),
			})
			require.NoError(t, err)

			getOut, err := client.GetApplication(
				ctx,
				&codedeploy.GetApplicationInput{ApplicationName: aws.String("app-a-renamed")},
			)
			require.NoError(t, err)
			assert.Equal(t, "app-a-renamed", aws.ToString(getOut.Application.ApplicationName))
		}},
		{name: "deployment_groups", run: func(t *testing.T) {
			t.Helper()

			h := newTestHandler(t)
			client := newTestCodeDeployClient(t, h)
			ctx := t.Context()

			_, err := h.Backend.CreateApplication("app", "Server", nil)
			require.NoError(t, err)
			_, err = createDG(h.Backend, "app", "dg-1", "", "", nil)
			require.NoError(t, err)

			listOut, err := client.ListDeploymentGroups(ctx, &codedeploy.ListDeploymentGroupsInput{
				ApplicationName: aws.String("app"),
			})
			require.NoError(t, err)
			assert.Equal(t, []string{"dg-1"}, listOut.DeploymentGroups)

			updOut, err := client.UpdateDeploymentGroup(ctx, &codedeploy.UpdateDeploymentGroupInput{
				ApplicationName:            aws.String("app"),
				CurrentDeploymentGroupName: aws.String("dg-1"),
				NewDeploymentGroupName:     aws.String("dg-1-renamed"),
				ServiceRoleArn:             aws.String("arn:aws:iam::000000000000:role/svc"),
			})
			require.NoError(t, err)
			assert.Empty(t, updOut.HooksNotCleanedUp)

			dg, err := h.Backend.GetDeploymentGroup("app", "dg-1-renamed")
			require.NoError(t, err)
			assert.Equal(t, "arn:aws:iam::000000000000:role/svc", dg.ServiceRoleArn)

			_, err = client.DeleteDeploymentGroup(ctx, &codedeploy.DeleteDeploymentGroupInput{
				ApplicationName:     aws.String("app"),
				DeploymentGroupName: aws.String("dg-1-renamed"),
			})
			require.NoError(t, err)

			_, err = h.Backend.GetDeploymentGroup("app", "dg-1-renamed")
			require.Error(t, err)
		}},
		{name: "deployments", run: func(t *testing.T) {
			t.Helper()

			h := newTestHandler(t)
			client := newTestCodeDeployClient(t, h)
			ctx := t.Context()

			_, err := h.Backend.CreateApplication("app", "Server", nil)
			require.NoError(t, err)
			_, err = createDG(h.Backend, "app", "dg", "", "", nil)
			require.NoError(t, err)

			dep1, err := createDeploy(h.Backend, "app", "dg", "first", "tester")
			require.NoError(t, err)
			dep2, err := createDeploy(h.Backend, "app", "dg", "second", "tester")
			require.NoError(t, err)

			batchOut, err := client.BatchGetDeployments(ctx, &codedeploy.BatchGetDeploymentsInput{
				DeploymentIds: []string{dep1.DeploymentID, dep2.DeploymentID},
			})
			require.NoError(t, err)
			require.Len(t, batchOut.DeploymentsInfo, 2)

			instOut, err := client.BatchGetDeploymentInstances(ctx, &codedeploy.BatchGetDeploymentInstancesInput{
				DeploymentId: aws.String(dep1.DeploymentID),
				InstanceIds:  []string{"i-doesnotexist"},
			})
			require.NoError(t, err)
			assert.Empty(t, instOut.InstancesSummary)

			targetsOut, err := client.BatchGetDeploymentTargets(ctx, &codedeploy.BatchGetDeploymentTargetsInput{
				DeploymentId: aws.String(dep1.DeploymentID),
				TargetIds:    []string{"i-doesnotexist"},
			})
			require.NoError(t, err)
			assert.Empty(t, targetsOut.DeploymentTargets)

			listInstOut, err := client.ListDeploymentInstances(ctx, &codedeploy.ListDeploymentInstancesInput{
				DeploymentId: aws.String(dep1.DeploymentID),
			})
			require.NoError(t, err)
			assert.Empty(t, listInstOut.InstancesList)

			listTargetsOut, err := client.ListDeploymentTargets(ctx, &codedeploy.ListDeploymentTargetsInput{
				DeploymentId: aws.String(dep1.DeploymentID),
			})
			require.NoError(t, err)
			assert.Empty(t, listTargetsOut.TargetIds)

			_, err = client.SkipWaitTimeForInstanceTermination(ctx, &codedeploy.SkipWaitTimeForInstanceTerminationInput{
				DeploymentId: aws.String(dep1.DeploymentID),
			})
			require.NoError(t, err)

			_, err = client.ContinueDeployment(ctx, &codedeploy.ContinueDeploymentInput{
				DeploymentId:       aws.String(dep1.DeploymentID),
				DeploymentWaitType: types.DeploymentWaitTypeReadyWait,
			})
			require.Error(t, err, "a deployment that never reached Ready cannot be continued")
		}},
		{name: "deployment_configs", run: func(t *testing.T) {
			t.Helper()

			h := newTestHandler(t)
			client := newTestCodeDeployClient(t, h)
			ctx := t.Context()

			_, err := createCfg(h.Backend, "cfg-slice29", "Server")
			require.NoError(t, err)

			listOut, err := client.ListDeploymentConfigs(ctx, &codedeploy.ListDeploymentConfigsInput{})
			require.NoError(t, err)
			assert.Contains(t, listOut.DeploymentConfigsList, "cfg-slice29")

			_, err = client.DeleteDeploymentConfig(ctx, &codedeploy.DeleteDeploymentConfigInput{
				DeploymentConfigName: aws.String("cfg-slice29"),
			})
			require.NoError(t, err)

			listOut2, err := client.ListDeploymentConfigs(ctx, &codedeploy.ListDeploymentConfigsInput{})
			require.NoError(t, err)
			assert.NotContains(t, listOut2.DeploymentConfigsList, "cfg-slice29")
		}},
		{name: "application_revisions", run: func(t *testing.T) {
			t.Helper()

			h := newTestHandler(t)
			client := newTestCodeDeployClient(t, h)
			ctx := t.Context()

			_, err := h.Backend.CreateApplication("app", "Server", nil)
			require.NoError(t, err)

			rev := types.RevisionLocation{
				RevisionType: types.RevisionLocationTypeS3,
				S3Location: &types.S3Location{
					Bucket:     aws.String("my-bucket"),
					Key:        aws.String("my-key"),
					BundleType: types.BundleTypeZip,
				},
			}

			_, err = client.RegisterApplicationRevision(ctx, &codedeploy.RegisterApplicationRevisionInput{
				ApplicationName: aws.String("app"),
				Revision:        &rev,
				Description:     aws.String("slice29 revision"),
			})
			require.NoError(t, err)

			listOut, err := client.ListApplicationRevisions(ctx, &codedeploy.ListApplicationRevisionsInput{
				ApplicationName: aws.String("app"),
			})
			require.NoError(t, err)
			require.Len(t, listOut.Revisions, 1)
			assert.Equal(t, "my-bucket", aws.ToString(listOut.Revisions[0].S3Location.Bucket))
		}},
		{name: "on_premises_instances", run: func(t *testing.T) {
			t.Helper()

			h := newTestHandler(t)
			client := newTestCodeDeployClient(t, h)
			ctx := t.Context()

			_, err := client.RegisterOnPremisesInstance(ctx, &codedeploy.RegisterOnPremisesInstanceInput{
				InstanceName: aws.String("onprem-1"),
				IamUserArn:   aws.String("arn:aws:iam::000000000000:user/onprem"),
			})
			require.NoError(t, err)

			_, err = client.AddTagsToOnPremisesInstances(ctx, &codedeploy.AddTagsToOnPremisesInstancesInput{
				InstanceNames: []string{"onprem-1"},
				Tags:          []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
			})
			require.NoError(t, err)

			listOut, err := client.ListOnPremisesInstances(ctx, &codedeploy.ListOnPremisesInstancesInput{
				TagFilters: []types.TagFilter{
					{Key: aws.String("env"), Value: aws.String("test"), Type: types.TagFilterTypeKeyAndValue},
				},
			})
			require.NoError(t, err)
			assert.Equal(t, []string{"onprem-1"}, listOut.InstanceNames)

			_, err = client.RemoveTagsFromOnPremisesInstances(ctx, &codedeploy.RemoveTagsFromOnPremisesInstancesInput{
				InstanceNames: []string{"onprem-1"},
				Tags:          []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
			})
			require.NoError(t, err)

			listOut2, err := client.ListOnPremisesInstances(ctx, &codedeploy.ListOnPremisesInstancesInput{
				TagFilters: []types.TagFilter{
					{Key: aws.String("env"), Value: aws.String("test"), Type: types.TagFilterTypeKeyAndValue},
				},
			})
			require.NoError(t, err)
			assert.Empty(t, listOut2.InstanceNames, "tag was removed, filter should no longer match")

			_, err = client.DeregisterOnPremisesInstance(ctx, &codedeploy.DeregisterOnPremisesInstanceInput{
				InstanceName: aws.String("onprem-1"),
			})
			require.NoError(t, err)

			getOut, err := client.GetOnPremisesInstance(ctx, &codedeploy.GetOnPremisesInstanceInput{
				InstanceName: aws.String("onprem-1"),
			})
			require.NoError(t, err, "deregistering does not delete the instance record, only marks it deregistered")
			require.NotNil(t, getOut.InstanceInfo.DeregisterTime)
		}},
		{name: "github_tokens_and_external", run: func(t *testing.T) {
			t.Helper()

			h := newTestHandler(t)
			client := newTestCodeDeployClient(t, h)
			ctx := t.Context()

			h.Backend.AddGitHubAccountTokenInternal("gh-token-1")

			listOut, err := client.ListGitHubAccountTokenNames(ctx, &codedeploy.ListGitHubAccountTokenNamesInput{})
			require.NoError(t, err)
			assert.Equal(t, []string{"gh-token-1"}, listOut.TokenNameList)

			_, err = client.DeleteResourcesByExternalId(ctx, &codedeploy.DeleteResourcesByExternalIdInput{
				ExternalId: aws.String("stack-123"),
			})
			require.NoError(t, err)
		}},
		{name: "lifecycle_hooks", run: func(t *testing.T) {
			t.Helper()

			h := newTestHandler(t)
			client := newTestCodeDeployClient(t, h)
			ctx := t.Context()

			_, err := h.Backend.CreateApplication("app", "Server", nil)
			require.NoError(t, err)
			_, err = createDG(h.Backend, "app", "dg", "", "", nil)
			require.NoError(t, err)
			dep, err := createDeploy(h.Backend, "app", "dg", "hook-test", "tester")
			require.NoError(t, err)

			out, err := client.PutLifecycleEventHookExecutionStatus(
				ctx,
				&codedeploy.PutLifecycleEventHookExecutionStatusInput{
					DeploymentId:                  aws.String(dep.DeploymentID),
					LifecycleEventHookExecutionId: aws.String("hook-exec-1"),
					Status:                        types.LifecycleEventStatusSucceeded,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "hook-exec-1", aws.ToString(out.LifecycleEventHookExecutionId))
		}},
		{name: "tags", run: func(t *testing.T) {
			t.Helper()

			h := newTestHandler(t)
			client := newTestCodeDeployClient(t, h)
			ctx := t.Context()

			_, err := h.Backend.CreateApplication("app", "Server", nil)
			require.NoError(t, err)

			appARN := h.Backend.ApplicationARN("app")

			_, err = client.TagResource(ctx, &codedeploy.TagResourceInput{
				ResourceArn: aws.String(appARN),
				Tags:        []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
			})
			require.NoError(t, err)

			_, err = client.UntagResource(ctx, &codedeploy.UntagResourceInput{
				ResourceArn: aws.String(appARN),
				TagKeys:     []string{"k"},
			})
			require.NoError(t, err)

			tagsOut, err := client.ListTagsForResource(ctx, &codedeploy.ListTagsForResourceInput{
				ResourceArn: aws.String(appARN),
			})
			require.NoError(t, err)
			assert.Empty(t, tagsOut.Tags)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
