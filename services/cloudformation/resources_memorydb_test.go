package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	memorydbbackend "github.com/blackbirdworks/gopherstack/services/memorydb"
)

// newMemoryDBTestClient wires a real aws-sdk-go-v2 CloudFormation client
// against a backend with MemoryDB (among others) wired to a real in-memory
// service backend.
func newMemoryDBTestClient(t *testing.T) (*cloudformation.ServiceBackends, *cfnsdk.Client) {
	t.Helper()

	backends := newMoreTypesServiceBackends(t)
	backends.MemoryDB = memorydbbackend.NewHandler(memorydbbackend.NewInMemoryBackend("000000000000", "us-east-1"))
	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	client := newTestClientForBackend(t, backend)

	return backends, client
}

func TestCreateStack_MemoryDBTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testMemoryDBParameterGroup, "parameter_group"},
		{testMemoryDBSubnetGroup, "subnet_group"},
		{testMemoryDBACL, "acl"},
		{testMemoryDBUser, "user"},
		{testMemoryDBCluster, "cluster"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testMemoryDBParameterGroup(t *testing.T) {
	t.Helper()

	backends, client := newMemoryDBTestClient(t)

	tmpl := `{
"Resources": {"PG": {"Type": "AWS::MemoryDB::ParameterGroup", "Properties": {
  "ParameterGroupName": "pg-1",
  "Family": "memorydb_redis7",
  "Description": "test parameter group",
  "Parameters": {"maxmemory-policy": "allkeys-lru"}
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "PG"}},
  "Arn": {"Value": {"Fn::GetAtt": ["PG", "ARN"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "mdb-pg-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "parametergroup/pg-1")
	assert.Equal(t, outputs["Ref"], outputs["Arn"])

	groups, err := backends.MemoryDB.Backend.DescribeParameterGroups(t.Context(), "pg-1")
	require.NoError(t, err)
	require.Len(t, groups, 1)

	params, err := backends.MemoryDB.Backend.DescribeParameters(t.Context(), "pg-1")
	require.NoError(t, err)
	assert.Equal(t, "allkeys-lru", params["maxmemory-policy"])

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("mdb-pg-stack")})
	require.NoError(t, err)

	_, err = backends.MemoryDB.Backend.DescribeParameterGroups(t.Context(), "pg-1")
	require.Error(t, err)
}

func testMemoryDBSubnetGroup(t *testing.T) {
	t.Helper()

	backends, client := newMemoryDBTestClient(t)

	tmpl := `{
"Resources": {"SG": {"Type": "AWS::MemoryDB::SubnetGroup", "Properties": {
  "SubnetGroupName": "sg-1",
  "Description": "test subnet group",
  "SubnetIds": ["subnet-aaa", "subnet-bbb"]
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "SG"}},
  "Types": {"Value": {"Fn::GetAtt": ["SG", "SupportedNetworkTypes"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "mdb-sg-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "subnetgroup/sg-1")
	assert.Equal(t, "ipv4", outputs["Types"])

	_, err := backends.MemoryDB.Backend.DescribeSubnetGroups(t.Context(), "sg-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("mdb-sg-stack")})
	require.NoError(t, err)

	_, err = backends.MemoryDB.Backend.DescribeSubnetGroups(t.Context(), "sg-1")
	require.Error(t, err)
}

func testMemoryDBACL(t *testing.T) {
	t.Helper()

	backends, client := newMemoryDBTestClient(t)

	tmpl := `{
"Resources": {"A": {"Type": "AWS::MemoryDB::ACL", "Properties": {
  "ACLName": "acl-1"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "A"}},
  "Status": {"Value": {"Fn::GetAtt": ["A", "Status"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "mdb-acl-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "acl/acl-1")
	assert.Equal(t, "active", outputs["Status"])

	_, err := backends.MemoryDB.Backend.DescribeACLs(t.Context(), "acl-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("mdb-acl-stack")})
	require.NoError(t, err)

	_, err = backends.MemoryDB.Backend.DescribeACLs(t.Context(), "acl-1")
	require.Error(t, err)
}

func testMemoryDBUser(t *testing.T) {
	t.Helper()

	backends, client := newMemoryDBTestClient(t)

	tmpl := `{
"Resources": {"U": {"Type": "AWS::MemoryDB::User", "Properties": {
  "UserName": "user-1",
  "AccessString": "on ~* &* +@all",
  "AuthenticationMode": {"Type": "password", "Passwords": ["1234567890123456"]}
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "U"}},
  "Status": {"Value": {"Fn::GetAtt": ["U", "Status"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "mdb-user-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "user/user-1")
	assert.Equal(t, "active", outputs["Status"])

	_, err := backends.MemoryDB.Backend.DescribeUsers(t.Context(), "user-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("mdb-user-stack")})
	require.NoError(t, err)

	_, err = backends.MemoryDB.Backend.DescribeUsers(t.Context(), "user-1")
	require.Error(t, err)
}

func testMemoryDBCluster(t *testing.T) {
	t.Helper()

	backends, client := newMemoryDBTestClient(t)

	tmpl := `{
"Resources": {
  "SG": {"Type": "AWS::MemoryDB::SubnetGroup", "Properties": {
    "SubnetGroupName": "cl-sg-1", "SubnetIds": ["subnet-aaa"]
  }},
  "C": {"Type": "AWS::MemoryDB::Cluster", "DependsOn": "SG", "Properties": {
    "ClusterName": "cl-1",
    "NodeType": "db.t4g.small",
    "ACLName": "open-access",
    "SubnetGroupName": "cl-sg-1",
    "NumShards": 1,
    "NumReplicasPerShard": 0
  }}
},
"Outputs": {
  "Ref": {"Value": {"Ref": "C"}},
  "Status": {"Value": {"Fn::GetAtt": ["C", "Status"]}},
  "PGStatus": {"Value": {"Fn::GetAtt": ["C", "ParameterGroupStatus"]}},
  "Address": {"Value": {"Fn::GetAtt": ["C", "ClusterEndpoint.Address"]}},
  "Port": {"Value": {"Fn::GetAtt": ["C", "ClusterEndpoint.Port"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "mdb-cluster-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "cluster/cl-1")
	assert.Equal(t, "available", outputs["Status"])
	assert.Equal(t, "in-sync", outputs["PGStatus"])
	assert.Contains(t, outputs["Address"], "cl-1.memorydb.us-east-1.amazonaws.com")
	assert.NotEmpty(t, outputs["Port"])

	clusters, err := backends.MemoryDB.Backend.DescribeClusters(t.Context(), "cl-1")
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	assert.Equal(t, "cl-sg-1", clusters[0].SubnetGroupName)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("mdb-cluster-stack")})
	require.NoError(t, err)

	_, err = backends.MemoryDB.Backend.DescribeClusters(t.Context(), "cl-1")
	require.Error(t, err)
}
