package codedeploy_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	codedeploysdk "github.com/aws/aws-sdk-go-v2/service/codedeploy"
	"github.com/aws/aws-sdk-go-v2/service/codedeploy/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

// TestListTagsForResource_RealClient_Tags proves ListTagsForResourceOutput's
// Tags survive a real client round trip. Before the fix, the handler emitted
// the response body under the key "tags"; the real SDK's response
// deserializer switches on the case-sensitive key "Tags" (awsjson1.1, no
// EqualFold -- confirmed at deserializers.go's
// awsAwsjson11_deserializeOpDocumentListTagsForResourceOutput), so a real
// client's Tags field was always empty regardless of what had been tagged.
// This is the one op family in this service whose wire shape uses PascalCase
// (ResourceArn/Tags/TagKeys/NextToken) instead of the rest of the service's
// camelCase convention -- the shared generic tagging shape.
func TestListTagsForResource_RealClient_Tags(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	_, err := client.CreateApplication(t.Context(), &codedeploysdk.CreateApplicationInput{
		ApplicationName: aws.String("wf-tags-app"),
	})
	require.NoError(t, err)

	appARN := backend.ApplicationARN("wf-tags-app")

	_, err = client.TagResource(t.Context(), &codedeploysdk.TagResourceInput{
		ResourceArn: aws.String(appARN),
		Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	out, err := client.ListTagsForResource(t.Context(), &codedeploysdk.ListTagsForResourceInput{
		ResourceArn: aws.String(appARN),
	})
	require.NoError(t, err)
	require.Len(t, out.Tags, 1, "Tags must round-trip through the real client's case-sensitive deserializer")
	assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
	assert.Equal(t, "prod", aws.ToString(out.Tags[0].Value))
}

// TestGetDeploymentGroup_RealClient_History proves
// LastAttemptedDeployment/LastSuccessfulDeployment/TargetRevision are
// derived from the group's real deployment history through a real client.
// Before the fix, DeploymentGroupInfo never carried any of the three despite
// the backend already tracking every deployment's CreateTime/Status/
// Revision per application/deployment-group pair (real
// DeploymentGroupInfo has all three, deserializers.go's
// awsAwsjson11_deserializeDocumentDeploymentGroupInfo).
func TestGetDeploymentGroup_RealClient_History(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	_, err := client.CreateApplication(t.Context(), &codedeploysdk.CreateApplicationInput{
		ApplicationName: aws.String("wf-history-app"),
	})
	require.NoError(t, err)

	_, err = client.CreateDeploymentGroup(t.Context(), &codedeploysdk.CreateDeploymentGroupInput{
		ApplicationName:     aws.String("wf-history-app"),
		DeploymentGroupName: aws.String("wf-history-dg"),
		ServiceRoleArn:      aws.String("arn:aws:iam::000000000000:role/role"),
	})
	require.NoError(t, err)

	before := time.Now().Add(-time.Minute)

	revision := &types.RevisionLocation{
		RevisionType: types.RevisionLocationTypeS3,
		S3Location: &types.S3Location{
			Bucket:     aws.String("wf-bucket"),
			Key:        aws.String("wf-key"),
			BundleType: types.BundleTypeZip,
		},
	}

	deployOut, err := client.CreateDeployment(t.Context(), &codedeploysdk.CreateDeploymentInput{
		ApplicationName:     aws.String("wf-history-app"),
		DeploymentGroupName: aws.String("wf-history-dg"),
		Revision:            revision,
	})
	require.NoError(t, err)

	dgOut, err := client.GetDeploymentGroup(t.Context(), &codedeploysdk.GetDeploymentGroupInput{
		ApplicationName:     aws.String("wf-history-app"),
		DeploymentGroupName: aws.String("wf-history-dg"),
	})
	require.NoError(t, err)

	dg := dgOut.DeploymentGroupInfo
	require.NotNil(t, dg.LastAttemptedDeployment)
	assert.Equal(t, aws.ToString(deployOut.DeploymentId), aws.ToString(dg.LastAttemptedDeployment.DeploymentId))
	assert.Equal(t, types.DeploymentStatusSucceeded, dg.LastAttemptedDeployment.Status)
	assertRecentTime(t, *dg.LastAttemptedDeployment.CreateTime, before, "LastAttemptedDeployment.CreateTime")

	require.NotNil(t, dg.LastSuccessfulDeployment)
	assert.Equal(t, aws.ToString(deployOut.DeploymentId), aws.ToString(dg.LastSuccessfulDeployment.DeploymentId))

	require.NotNil(t, dg.TargetRevision)
	require.NotNil(t, dg.TargetRevision.S3Location)
	assert.Equal(t, "wf-bucket", aws.ToString(dg.TargetRevision.S3Location.Bucket))
	assert.Equal(t, "wf-key", aws.ToString(dg.TargetRevision.S3Location.Key))

	// BatchGetDeploymentGroups shares the same converter path -- confirm the
	// enrichment applies there too, not just GetDeploymentGroup.
	batchOut, err := client.BatchGetDeploymentGroups(t.Context(), &codedeploysdk.BatchGetDeploymentGroupsInput{
		ApplicationName:      aws.String("wf-history-app"),
		DeploymentGroupNames: []string{"wf-history-dg"},
	})
	require.NoError(t, err)
	require.Len(t, batchOut.DeploymentGroupsInfo, 1)
	require.NotNil(t, batchOut.DeploymentGroupsInfo[0].LastAttemptedDeployment)
	assert.Equal(t,
		aws.ToString(deployOut.DeploymentId),
		aws.ToString(batchOut.DeploymentGroupsInfo[0].LastAttemptedDeployment.DeploymentId),
	)
}

// TestGetDeploymentGroup_RealClient_NoDeploymentsYet proves a deployment
// group with no deployments yet correctly omits (nil, not fabricated) all
// three history fields, rather than synthesizing empty placeholders.
func TestGetDeploymentGroup_RealClient_NoDeploymentsYet(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	_, err := client.CreateApplication(t.Context(), &codedeploysdk.CreateApplicationInput{
		ApplicationName: aws.String("wf-empty-app"),
	})
	require.NoError(t, err)

	_, err = client.CreateDeploymentGroup(t.Context(), &codedeploysdk.CreateDeploymentGroupInput{
		ApplicationName:     aws.String("wf-empty-app"),
		DeploymentGroupName: aws.String("wf-empty-dg"),
		ServiceRoleArn:      aws.String("arn:aws:iam::000000000000:role/role"),
	})
	require.NoError(t, err)

	dgOut, err := client.GetDeploymentGroup(t.Context(), &codedeploysdk.GetDeploymentGroupInput{
		ApplicationName:     aws.String("wf-empty-app"),
		DeploymentGroupName: aws.String("wf-empty-dg"),
	})
	require.NoError(t, err)

	assert.Nil(t, dgOut.DeploymentGroupInfo.LastAttemptedDeployment)
	assert.Nil(t, dgOut.DeploymentGroupInfo.LastSuccessfulDeployment)
	assert.Nil(t, dgOut.DeploymentGroupInfo.TargetRevision)
}

// TestOnPremisesInstance_RealClient_InstanceArn proves InstanceArn is
// populated through a real client, matching the same "instance:<name>"
// resource format already used for InstanceTarget.TargetArn elsewhere in
// this service (deployment_instances.go). Before the fix,
// OnPremisesInstanceInfo never carried InstanceArn at all despite the real
// type always having it (deserializers.go's
// awsAwsjson11_deserializeDocumentInstanceInfo).
func TestOnPremisesInstance_RealClient_InstanceArn(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	_, err := client.RegisterOnPremisesInstance(t.Context(), &codedeploysdk.RegisterOnPremisesInstanceInput{
		InstanceName: aws.String("wf-arn-instance"),
		IamUserArn:   aws.String("arn:aws:iam::000000000000:user/instance"),
	})
	require.NoError(t, err)

	wantARN := backend.OnPremisesInstanceARN("wf-arn-instance")

	getOut, err := client.GetOnPremisesInstance(t.Context(), &codedeploysdk.GetOnPremisesInstanceInput{
		InstanceName: aws.String("wf-arn-instance"),
	})
	require.NoError(t, err)
	assert.Equal(t, wantARN, aws.ToString(getOut.InstanceInfo.InstanceArn))

	batchOut, err := client.BatchGetOnPremisesInstances(t.Context(), &codedeploysdk.BatchGetOnPremisesInstancesInput{
		InstanceNames: []string{"wf-arn-instance"},
	})
	require.NoError(t, err)
	require.Len(t, batchOut.InstanceInfos, 1)
	assert.Equal(t, wantARN, aws.ToString(batchOut.InstanceInfos[0].InstanceArn))
}

// TestStopDeployment_RealClient_StatusMessage proves StopDeploymentOutput's
// StatusMessage is populated through a real client. Before the fix, the
// real StopStatus/StatusMessage pair (both present on the real Output type)
// only ever had Status set -- StatusMessage was never modeled at all.
func TestStopDeployment_RealClient_StatusMessage(t *testing.T) {
	t.Parallel()

	backend := codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)
	h := codedeploy.NewHandler(backend)
	client := newTestCodeDeployClient(t, h)

	_, err := client.CreateApplication(t.Context(), &codedeploysdk.CreateApplicationInput{
		ApplicationName: aws.String("wf-stopmsg-app"),
	})
	require.NoError(t, err)

	_, err = client.CreateDeploymentGroup(t.Context(), &codedeploysdk.CreateDeploymentGroupInput{
		ApplicationName:     aws.String("wf-stopmsg-app"),
		DeploymentGroupName: aws.String("wf-stopmsg-dg"),
		ServiceRoleArn:      aws.String("arn:aws:iam::000000000000:role/role"),
	})
	require.NoError(t, err)

	deployOut, err := client.CreateDeployment(t.Context(), &codedeploysdk.CreateDeploymentInput{
		ApplicationName:     aws.String("wf-stopmsg-app"),
		DeploymentGroupName: aws.String("wf-stopmsg-dg"),
	})
	require.NoError(t, err)

	stopOut, err := client.StopDeployment(t.Context(), &codedeploysdk.StopDeploymentInput{
		DeploymentId: deployOut.DeploymentId,
	})
	require.NoError(t, err)
	assert.Equal(t, "The stop operation was successful.", aws.ToString(stopOut.StatusMessage))
}
