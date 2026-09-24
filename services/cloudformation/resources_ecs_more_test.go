package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func newECSMoreTestClient(t *testing.T) (*cloudformation.ServiceBackends, *cfnsdk.Client) {
	t.Helper()

	backends := newAdditionalServiceBackends()
	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	client := newTestClientForBackend(t, backend)

	return backends, client
}

func TestCreateStack_ECSCapacityProviderAndClusterAssociation(t *testing.T) {
	t.Parallel()

	backends, client := newECSMoreTestClient(t)

	tmpl := `{
"Resources": {
  "Cluster": {"Type": "AWS::ECS::Cluster", "Properties": {"ClusterName": "cp-cluster"}},
  "CP": {
    "Type": "AWS::ECS::CapacityProvider",
    "Properties": {
      "Name": "my-capacity-provider",
      "AutoScalingGroupProvider": {
        "AutoScalingGroupArn": "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:x:asgName/my-asg",
        "ManagedTerminationProtection": "ENABLED"
      }
    }
  },
  "Assoc": {
    "Type": "AWS::ECS::ClusterCapacityProviderAssociations",
    "Properties": {
      "Cluster": {"Ref": "Cluster"},
      "CapacityProviders": [{"Ref": "CP"}],
      "DefaultCapacityProviderStrategy": [{"CapacityProvider": {"Ref": "CP"}, "Weight": 1}]
    }
  }
},
"Outputs": {
  "CPRef": {"Value": {"Ref": "CP"}},
  "AssocRef": {"Value": {"Ref": "Assoc"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ecs-cp-stack", tmpl)

	assert.Equal(t, "my-capacity-provider", outputs["CPRef"])
	assert.Equal(t, "cp-cluster", outputs["AssocRef"], "Ref must be the cluster name per the CFN docs")

	cluster, _, err := backends.ECS.Backend.DescribeClusters([]string{"cp-cluster"})
	require.NoError(t, err)
	require.Len(t, cluster, 1)
	assert.Equal(t, []string{"my-capacity-provider"}, cluster[0].CapacityProviders)
	require.Len(t, cluster[0].DefaultCapacityProviderStrategy, 1)
	assert.Equal(t, "my-capacity-provider", cluster[0].DefaultCapacityProviderStrategy[0].CapacityProvider)

	cps, _, err := backends.ECS.Backend.DescribeCapacityProviders([]string{"my-capacity-provider"}, "")
	require.NoError(t, err)
	require.Len(t, cps, 1)
	require.NotNil(t, cps[0].AutoScalingGroupProvider)
	assert.Equal(t, "ENABLED", cps[0].AutoScalingGroupProvider.ManagedTerminationProtection)

	// Deleting the stack tears down the whole cluster (Assoc's own delete just
	// clears the association; Cluster's delete removes the cluster entirely).
	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ecs-cp-stack")})
	require.NoError(t, err)

	after, failures, err := backends.ECS.Backend.DescribeClusters([]string{"cp-cluster"})
	require.NoError(t, err)
	assert.Empty(t, after)
	assert.Len(t, failures, 1)
}

func TestCreateStack_ECSTaskSetAndPrimaryTaskSet(t *testing.T) {
	t.Parallel()

	backends, client := newECSMoreTestClient(t)

	tmpl := `{
"Resources": {
  "Cluster": {"Type": "AWS::ECS::Cluster", "Properties": {"ClusterName": "ts-cluster"}},
  "TD": {
    "Type": "AWS::ECS::TaskDefinition",
    "Properties": {
      "Family": "ts-family",
      "NetworkMode": "bridge",
      "ContainerDefinitions": [{"Name": "app", "Image": "nginx:latest"}]
    }
  },
  "Svc": {
    "Type": "AWS::ECS::Service",
    "Properties": {
      "ServiceName": "ts-service",
      "Cluster": {"Ref": "Cluster"},
      "TaskDefinition": {"Ref": "TD"},
      "DesiredCount": 0,
      "DeploymentController": {"Type": "EXTERNAL"}
    }
  },
  "TaskSet": {
    "Type": "AWS::ECS::TaskSet",
    "Properties": {
      "Cluster": {"Ref": "Cluster"},
      "Service": {"Ref": "Svc"},
      "TaskDefinition": {"Ref": "TD"},
      "LaunchType": "FARGATE",
      "Scale": {"Unit": "PERCENT", "Value": 50}
    }
  },
  "Primary": {
    "Type": "AWS::ECS::PrimaryTaskSet",
    "Properties": {
      "Cluster": {"Ref": "Cluster"},
      "Service": {"Ref": "Svc"},
      "TaskSetId": {"Fn::GetAtt": ["TaskSet", "Id"]}
    }
  }
},
"Outputs": {
  "TaskSetArn": {"Value": {"Ref": "TaskSet"}},
  "TaskSetId": {"Value": {"Fn::GetAtt": ["TaskSet", "Id"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ecs-taskset-stack", tmpl)

	assert.Contains(t, outputs["TaskSetArn"], "task-set/ts-cluster/ts-service/")
	assert.Contains(t, outputs["TaskSetArn"], outputs["TaskSetId"])

	sets, _, err := backends.ECS.Backend.DescribeTaskSets("ts-cluster", "ts-service", nil)
	require.NoError(t, err)
	require.Len(t, sets, 1)
	assert.Equal(t, "PRIMARY", sets[0].Status)
	assert.InDelta(t, 50.0, sets[0].Scale.Value, 0)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ecs-taskset-stack")})
	require.NoError(t, err)

	_, failures, err := backends.ECS.Backend.DescribeClusters([]string{"ts-cluster"})
	require.NoError(t, err)
	assert.Len(t, failures, 1)
}
