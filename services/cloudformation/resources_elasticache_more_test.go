package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_ElastiCacheMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testElastiCacheParameterGroup, "parameter_group"},
		{testElastiCacheSecurityGroup, "security_group"},
		{testElastiCacheGlobalReplicationGroup, "global_replication_group"},
		{testElastiCacheUserGroup, "user_group"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testElastiCacheParameterGroup(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "PG": {
    "Type": "AWS::ElastiCache::ParameterGroup",
    "Properties": {
      "CacheParameterGroupFamily": "redis7",
      "Description": "test param group"
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "PG"}},
  "Name": {"Value": {"Fn::GetAtt": ["PG", "CacheParameterGroupName"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ec-pg-stack", tmpl)
	assert.NotEmpty(t, outputs["Ref"])
	assert.Equal(t, outputs["Ref"], outputs["Name"])

	groups, err := backends.ElastiCache.Backend.DescribeParameterGroups(t.Context(), outputs["Ref"], "", 0)
	require.NoError(t, err)
	require.Len(t, groups.Data, 1)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec-pg-stack")})
	require.NoError(t, err)

	groups, err = backends.ElastiCache.Backend.DescribeParameterGroups(t.Context(), outputs["Ref"], "", 0)
	require.Error(t, err)
	assert.Empty(t, groups.Data)
}

func testElastiCacheSecurityGroup(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "SG": {
    "Type": "AWS::ElastiCache::SecurityGroup",
    "Properties": {
      "Description": "test security group"
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "SG"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "ec-sg-stack", tmpl)
	assert.NotEmpty(t, outputs["Ref"])

	groups, err := backends.ElastiCache.Backend.DescribeCacheSecurityGroups(t.Context(), outputs["Ref"], "", 0)
	require.NoError(t, err)
	require.Len(t, groups.Data, 1)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec-sg-stack")})
	require.NoError(t, err)

	groups, err = backends.ElastiCache.Backend.DescribeCacheSecurityGroups(t.Context(), outputs["Ref"], "", 0)
	require.Error(t, err)
	assert.Empty(t, groups.Data)
}

func testElastiCacheGlobalReplicationGroup(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "GRG": {
    "Type": "AWS::ElastiCache::GlobalReplicationGroup",
    "Properties": {
      "GlobalReplicationGroupIdSuffix": "my-suffix",
      "GlobalReplicationGroupDescription": "test global replication group"
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "GRG"}},
  "Status": {"Value": {"Fn::GetAtt": ["GRG", "Status"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ec-grg-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "my-suffix")
	assert.NotEmpty(t, outputs["Status"])

	groups, err := backends.ElastiCache.Backend.DescribeGlobalReplicationGroups(t.Context(), outputs["Ref"], "", 0)
	require.NoError(t, err)
	require.Len(t, groups.Data, 1)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec-grg-stack")})
	require.NoError(t, err)

	groups, err = backends.ElastiCache.Backend.DescribeGlobalReplicationGroups(t.Context(), outputs["Ref"], "", 0)
	require.Error(t, err)
	assert.Empty(t, groups.Data)
}

func testElastiCacheUserGroup(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "UG": {
    "Type": "AWS::ElastiCache::UserGroup",
    "Properties": {
      "Engine": "redis",
      "UserGroupId": "my-user-group",
      "UserIds": ["default"]
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "UG"}},
  "Arn": {"Value": {"Fn::GetAtt": ["UG", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ec-ug-stack", tmpl)
	assert.Equal(t, "my-user-group", outputs["Ref"])
	assert.NotEmpty(t, outputs["Arn"])

	groups, err := backends.ElastiCache.Backend.DescribeUserGroups(t.Context(), "my-user-group", "", 0)
	require.NoError(t, err)
	require.Len(t, groups.Data, 1)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec-ug-stack")})
	require.NoError(t, err)

	groups, err = backends.ElastiCache.Backend.DescribeUserGroups(t.Context(), "my-user-group", "", 0)
	require.Error(t, err)
	assert.Empty(t, groups.Data)
}
