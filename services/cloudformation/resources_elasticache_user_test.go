package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_ElastiCacheUser(t *testing.T) {
	t.Parallel()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "User": {
    "Type": "AWS::ElastiCache::User",
    "Properties": {
      "UserId": "unit-ec-user",
      "UserName": "unit-user",
      "Engine": "redis",
      "AccessString": "on ~* +@all",
      "NoPasswordRequired": true
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "User"}},
  "Arn": {"Value": {"Fn::GetAtt": ["User", "Arn"]}},
  "Status": {"Value": {"Fn::GetAtt": ["User", "Status"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ec-user-stack", tmpl)
	assert.Equal(t, "unit-ec-user", outputs["Ref"])
	assert.NotEmpty(t, outputs["Arn"])
	assert.NotEmpty(t, outputs["Status"])

	u, err := backends.ElastiCache.Backend.DescribeUsers(t.Context(), "unit-ec-user", "", "", 0, nil)
	require.NoError(t, err)
	require.Len(t, u.Data, 1)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ec-user-stack")})
	require.NoError(t, err)

	_, err = backends.ElastiCache.Backend.DescribeUsers(t.Context(), "unit-ec-user", "", "", 0, nil)
	require.Error(t, err)
}
