package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_NeptuneMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testNeptuneDBSubnetGroup, "db_subnet_group"},
		{testNeptuneDBClusterParameterGroup, "db_cluster_parameter_group"},
		{testNeptuneDBParameterGroup, "db_parameter_group"},
		{testNeptuneGlobalCluster, "global_cluster"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testNeptuneDBSubnetGroup(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "SG": {
    "Type": "AWS::Neptune::DBSubnetGroup",
    "Properties": {
      "DBSubnetGroupName": "my-neptune-subnet-group",
      "DBSubnetGroupDescription": "test subnet group",
      "SubnetIds": ["subnet-1", "subnet-2"]
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "SG"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "np-sg-stack", tmpl)
	assert.Equal(t, "my-neptune-subnet-group", outputs["Ref"])

	groups, err := backends.Neptune.Backend.DescribeDBSubnetGroups(t.Context(), "my-neptune-subnet-group")
	require.NoError(t, err)
	require.Len(t, groups, 1)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("np-sg-stack")})
	require.NoError(t, err)

	_, err = backends.Neptune.Backend.DescribeDBSubnetGroups(t.Context(), "my-neptune-subnet-group")
	require.Error(t, err)
}

func testNeptuneDBClusterParameterGroup(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "CPG": {
    "Type": "AWS::Neptune::DBClusterParameterGroup",
    "Properties": {
      "Name": "my-neptune-cluster-pg",
      "Family": "neptune1.3",
      "Description": "test cluster parameter group"
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "CPG"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "np-cpg-stack", tmpl)
	assert.Equal(t, "my-neptune-cluster-pg", outputs["Ref"])

	groups, err := backends.Neptune.Backend.DescribeDBClusterParameterGroups(t.Context(), "my-neptune-cluster-pg")
	require.NoError(t, err)
	require.Len(t, groups, 1)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("np-cpg-stack")})
	require.NoError(t, err)

	_, err = backends.Neptune.Backend.DescribeDBClusterParameterGroups(t.Context(), "my-neptune-cluster-pg")
	require.Error(t, err)
}

func testNeptuneDBParameterGroup(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "PG": {
    "Type": "AWS::Neptune::DBParameterGroup",
    "Properties": {
      "Name": "my-neptune-pg",
      "Family": "neptune1.3",
      "Description": "test parameter group"
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "PG"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "np-pg-stack", tmpl)
	assert.Equal(t, "my-neptune-pg", outputs["Ref"])

	groups, err := backends.Neptune.Backend.DescribeDBParameterGroups(t.Context(), "my-neptune-pg")
	require.NoError(t, err)
	require.Len(t, groups, 1)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("np-pg-stack")})
	require.NoError(t, err)

	_, err = backends.Neptune.Backend.DescribeDBParameterGroups(t.Context(), "my-neptune-pg")
	require.Error(t, err)
}

func testNeptuneGlobalCluster(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "GC": {
    "Type": "AWS::Neptune::GlobalCluster",
    "Properties": {
      "GlobalClusterIdentifier": "my-neptune-global-cluster",
      "Engine": "neptune",
      "EngineVersion": "1.3.0.0"
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "GC"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "np-gc-stack", tmpl)
	assert.Equal(t, "my-neptune-global-cluster", outputs["Ref"])

	clusters := backends.Neptune.Backend.DescribeGlobalClusters(t.Context())
	found := false

	for _, c := range clusters {
		if c.GlobalClusterIdentifier == "my-neptune-global-cluster" {
			found = true
		}
	}

	assert.True(t, found)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("np-gc-stack")})
	require.NoError(t, err)

	clusters = backends.Neptune.Backend.DescribeGlobalClusters(t.Context())
	for _, c := range clusters {
		assert.NotEqual(t, "my-neptune-global-cluster", c.GlobalClusterIdentifier)
	}
}
