package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_ECRMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testECRPullThroughCacheRule, "pull_through_cache_rule"},
		{testECRRegistryPolicy, "registry_policy"},
		{testECRRepositoryCreationTemplate, "repository_creation_template"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testECRPullThroughCacheRule(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "PTC": {
    "Type": "AWS::ECR::PullThroughCacheRule",
    "Properties": {
      "EcrRepositoryPrefix": "docker-hub",
      "UpstreamRegistryUrl": "registry-1.docker.io"
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "PTC"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "ptc-stack", tmpl)
	assert.Equal(t, "docker-hub", outputs["Ref"])

	rules, err := backends.ECR.Backend.DescribePullThroughCacheRules(t.Context(), nil, "")
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.Equal(t, "registry-1.docker.io", rules[0].UpstreamRegistryURL)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ptc-stack")})
	require.NoError(t, err)

	rules, err = backends.ECR.Backend.DescribePullThroughCacheRules(t.Context(), nil, "")
	require.NoError(t, err)
	assert.Empty(t, rules)
}

func testECRRegistryPolicy(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "RP": {
    "Type": "AWS::ECR::RegistryPolicy",
    "Properties": {
      "PolicyText": {
        "Version": "2012-10-17",
        "Statement": [{
          "Sid": "AllowReplication",
          "Effect": "Allow",
          "Principal": {"AWS": "arn:aws:iam::210987654321:root"},
          "Action": ["ecr:CreateRepository"],
          "Resource": "*"
        }]
      }
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "RP"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "rp-ecr-stack", tmpl)
	assert.Equal(t, "000000000000", outputs["Ref"])

	policy, err := backends.ECR.Backend.GetRegistryPolicy(t.Context())
	require.NoError(t, err)
	assert.Contains(t, policy.PolicyText, "AllowReplication")

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("rp-ecr-stack")})
	require.NoError(t, err)

	_, err = backends.ECR.Backend.GetRegistryPolicy(t.Context())
	require.Error(t, err)
}

func testECRRepositoryCreationTemplate(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "RCT": {
    "Type": "AWS::ECR::RepositoryCreationTemplate",
    "Properties": {
      "Prefix": "my-prefix",
      "Description": "a test template",
      "AppliedFor": ["PULL_THROUGH_CACHE"]
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "RCT"}},
  "CreatedAt": {"Value": {"Fn::GetAtt": ["RCT", "CreatedAt"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "rct-stack", tmpl)
	assert.Equal(t, "my-prefix", outputs["Ref"])
	assert.NotEmpty(t, outputs["CreatedAt"])

	tmpls, err := backends.ECR.Backend.DescribeRepositoryCreationTemplates(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, tmpls, 1)
	assert.Equal(t, "a test template", tmpls[0].Description)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("rct-stack")})
	require.NoError(t, err)

	tmpls, err = backends.ECR.Backend.DescribeRepositoryCreationTemplates(t.Context(), nil)
	require.NoError(t, err)
	assert.Empty(t, tmpls)
}
