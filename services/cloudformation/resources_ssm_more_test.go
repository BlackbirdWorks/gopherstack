package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestCreateStack_SSMMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testSSMMaintenanceWindowTarget, "maintenance_window_target"},
		{testSSMMaintenanceWindowTask, "maintenance_window_task"},
		{testSSMPatchBaseline, "patch_baseline"},
		{testSSMResourceDataSync, "resource_data_sync"},
		{testSSMResourcePolicy, "resource_policy"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testSSMMaintenanceWindowTarget(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Win": {
    "Type": "AWS::SSM::MaintenanceWindow",
    "Properties": {
      "Name": "my-window",
      "Schedule": "cron(0 2 ? * SUN *)",
      "Duration": 4,
      "Cutoff": 1,
      "AllowUnassociatedTargets": true
    }
  },
  "Target": {
    "Type": "AWS::SSM::MaintenanceWindowTarget",
    "Properties": {
      "WindowId": {"Ref": "Win"},
      "ResourceType": "INSTANCE",
      "Targets": [{"Key": "InstanceIds", "Values": ["i-1234567890abcdef0"]}]
    }
  }
},
"Outputs": {"WinRef": {"Value": {"Ref": "Win"}}, "TargetRef": {"Value": {"Ref": "Target"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "mwt-stack", tmpl)
	require.NotEmpty(t, outputs["TargetRef"])

	out, err := backends.SSM.Backend.DescribeMaintenanceWindowTargets(
		t.Context(), &ssmbackend.DescribeMaintenanceWindowTargetsInput{WindowID: outputs["WinRef"]},
	)
	require.NoError(t, err)
	require.Len(t, out.Targets, 1)
	assert.Equal(t, outputs["TargetRef"], out.Targets[0].WindowTargetID)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("mwt-stack")})
	require.NoError(t, err)

	out, err = backends.SSM.Backend.DescribeMaintenanceWindowTargets(
		t.Context(), &ssmbackend.DescribeMaintenanceWindowTargetsInput{WindowID: outputs["WinRef"]},
	)
	require.NoError(t, err)
	assert.Empty(t, out.Targets)
}

func testSSMMaintenanceWindowTask(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Win": {
    "Type": "AWS::SSM::MaintenanceWindow",
    "Properties": {
      "Name": "my-window2",
      "Schedule": "cron(0 2 ? * SUN *)",
      "Duration": 4,
      "Cutoff": 1,
      "AllowUnassociatedTargets": true
    }
  },
  "Task": {
    "Type": "AWS::SSM::MaintenanceWindowTask",
    "Properties": {
      "WindowId": {"Ref": "Win"},
      "TaskArn": "AWS-RunShellScript",
      "TaskType": "RUN_COMMAND",
      "Priority": 1,
      "MaxConcurrency": "1",
      "MaxErrors": "1",
      "Targets": [{"Key": "InstanceIds", "Values": ["i-1234567890abcdef0"]}]
    }
  }
},
"Outputs": {"WinRef": {"Value": {"Ref": "Win"}}, "TaskRef": {"Value": {"Ref": "Task"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "mwtask-stack", tmpl)
	require.NotEmpty(t, outputs["TaskRef"])

	out, err := backends.SSM.Backend.DescribeMaintenanceWindowTasks(
		t.Context(), &ssmbackend.DescribeMaintenanceWindowTasksInput{WindowID: outputs["WinRef"]},
	)
	require.NoError(t, err)
	require.Len(t, out.Tasks, 1)
	assert.Equal(t, outputs["TaskRef"], out.Tasks[0].WindowTaskID)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("mwtask-stack")})
	require.NoError(t, err)

	out, err = backends.SSM.Backend.DescribeMaintenanceWindowTasks(
		t.Context(), &ssmbackend.DescribeMaintenanceWindowTasksInput{WindowID: outputs["WinRef"]},
	)
	require.NoError(t, err)
	assert.Empty(t, out.Tasks)
}

func testSSMPatchBaseline(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "PB": {
    "Type": "AWS::SSM::PatchBaseline",
    "Properties": {
      "Name": "my-patch-baseline",
      "OperatingSystem": "AMAZON_LINUX_2",
      "ApprovedPatches": ["KB123456"]
    }
  }
},
"Outputs": {"PBRef": {"Value": {"Ref": "PB"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "pb-stack", tmpl)
	require.NotEmpty(t, outputs["PBRef"])

	out, err := backends.SSM.Backend.GetPatchBaseline(
		t.Context(), &ssmbackend.GetPatchBaselineInput{BaselineID: outputs["PBRef"]},
	)
	require.NoError(t, err)
	assert.Equal(t, "my-patch-baseline", out.PatchBaseline.Name)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("pb-stack")})
	require.NoError(t, err)

	_, err = backends.SSM.Backend.GetPatchBaseline(
		t.Context(), &ssmbackend.GetPatchBaselineInput{BaselineID: outputs["PBRef"]},
	)
	require.Error(t, err)
}

func testSSMResourceDataSync(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "RDS": {
    "Type": "AWS::SSM::ResourceDataSync",
    "Properties": {
      "SyncName": "my-resource-sync",
      "BucketName": "my-sync-bucket",
      "BucketRegion": "us-east-1",
      "SyncFormat": "JsonSerDe"
    }
  }
},
"Outputs": {"SyncRef": {"Value": {"Ref": "RDS"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "sync-stack", tmpl)
	assert.Equal(t, "my-resource-sync", outputs["SyncRef"])

	out, err := backends.SSM.Backend.ListResourceDataSync(t.Context(), &ssmbackend.ListResourceDataSyncInput{})
	require.NoError(t, err)
	require.Len(t, out.ResourceDataSyncItems, 1)
	assert.Equal(t, "my-resource-sync", out.ResourceDataSyncItems[0].SyncName)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sync-stack")})
	require.NoError(t, err)

	out, err = backends.SSM.Backend.ListResourceDataSync(t.Context(), &ssmbackend.ListResourceDataSyncInput{})
	require.NoError(t, err)
	assert.Empty(t, out.ResourceDataSyncItems)
}

func testSSMResourcePolicy(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	resourceARN := "arn:aws:ssm:us-east-1:000000000000:opsitemgroup/default"

	tmpl := `{
"Resources": {
  "Policy": {
    "Type": "AWS::SSM::ResourcePolicy",
    "Properties": {
      "ResourceArn": "` + resourceARN + `",
      "Policy": "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
},
"Outputs": {"PolicyRef": {"Value": {"Ref": "Policy"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "policy-stack", tmpl)
	require.NotEmpty(t, outputs["PolicyRef"])

	out, err := backends.SSM.Backend.GetResourcePolicies(
		t.Context(), &ssmbackend.GetResourcePoliciesInput{ResourceARN: resourceARN},
	)
	require.NoError(t, err)
	require.Len(t, out.Policies, 1)
	assert.Equal(t, outputs["PolicyRef"], out.Policies[0].PolicyID)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("policy-stack")})
	require.NoError(t, err)

	out, err = backends.SSM.Backend.GetResourcePolicies(
		t.Context(), &ssmbackend.GetResourcePoliciesInput{ResourceARN: resourceARN},
	)
	require.NoError(t, err)
	assert.Empty(t, out.Policies)
}
