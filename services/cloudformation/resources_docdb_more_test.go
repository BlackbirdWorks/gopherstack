package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_DocDBMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testDocDBDBSubnetGroup, "db_subnet_group"},
		{testDocDBDBClusterParameterGroup, "db_cluster_parameter_group"},
		{testDocDBGlobalCluster, "global_cluster"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testDocDBDBSubnetGroup(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "SG": {
    "Type": "AWS::DocDB::DBSubnetGroup",
    "Properties": {
      "DBSubnetGroupName": "my-docdb-subnet-group",
      "DBSubnetGroupDescription": "test subnet group",
      "SubnetIds": ["subnet-1", "subnet-2"]
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "SG"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "dd-sg-stack", tmpl)
	assert.Equal(t, "my-docdb-subnet-group", outputs["Ref"])

	groups, err := backends.DocDB.Backend.DescribeDBSubnetGroups(t.Context(), "my-docdb-subnet-group")
	require.NoError(t, err)
	require.Len(t, groups, 1)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("dd-sg-stack")})
	require.NoError(t, err)

	_, err = backends.DocDB.Backend.DescribeDBSubnetGroups(t.Context(), "my-docdb-subnet-group")
	require.Error(t, err)
}

func testDocDBDBClusterParameterGroup(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "CPG": {
    "Type": "AWS::DocDB::DBClusterParameterGroup",
    "Properties": {
      "Name": "my-docdb-cluster-pg",
      "Family": "docdb5.0",
      "Description": "test cluster parameter group"
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "CPG"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "dd-cpg-stack", tmpl)
	assert.Equal(t, "my-docdb-cluster-pg", outputs["Ref"])

	groups, err := backends.DocDB.Backend.DescribeDBClusterParameterGroups(t.Context(), "my-docdb-cluster-pg")
	require.NoError(t, err)
	require.Len(t, groups, 1)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("dd-cpg-stack")})
	require.NoError(t, err)

	_, err = backends.DocDB.Backend.DescribeDBClusterParameterGroups(t.Context(), "my-docdb-cluster-pg")
	require.Error(t, err)
}

func testDocDBGlobalCluster(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "GC": {
    "Type": "AWS::DocDB::GlobalCluster",
    "Properties": {
      "GlobalClusterIdentifier": "my-docdb-global-cluster",
      "Engine": "docdb",
      "EngineVersion": "5.0.0"
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "GC"}},
  "Arn": {"Value": {"Fn::GetAtt": ["GC", "GlobalClusterArn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "dd-gc-stack", tmpl)
	assert.Equal(t, "my-docdb-global-cluster", outputs["Ref"])
	assert.NotEmpty(t, outputs["Arn"])

	clusters := backends.DocDB.Backend.DescribeGlobalClusters(t.Context(), "my-docdb-global-cluster")
	require.Len(t, clusters, 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("dd-gc-stack")})
	require.NoError(t, err)

	clusters = backends.DocDB.Backend.DescribeGlobalClusters(t.Context(), "my-docdb-global-cluster")
	assert.Empty(t, clusters)
}
