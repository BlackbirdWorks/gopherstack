package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeleteStack_PropertyGetAtt is a regression suite for gopherstack-9e44r:
// DeleteStack rebuilt its physicalIDs map from {logicalID: PhysicalID} only,
// so a resource whose own Properties re-resolve a sibling Fn::GetAtt at
// delete time (e.g. to find its owning parent) fell back to the sibling's
// physical ID instead of the requested attribute -- deleting (or looking up)
// the wrong parent entirely. See Stack.ResourceAttrs / deleteResolveContext.
func TestDeleteStack_PropertyGetAtt(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testDeleteGetAttCodeArtifactRepository, "codeartifact_repository"},
		{testDeleteGetAttCodeArtifactPackageGroup, "codeartifact_package_group"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testDeleteGetAttCodeArtifactRepository(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Dom": {"Type": "AWS::CodeArtifact::Domain", "Properties": {"DomainName": "del-domain"}},
  "Repo": {"Type": "AWS::CodeArtifact::Repository", "Properties": {
    "DomainName": {"Fn::GetAtt": ["Dom", "Name"]},
    "RepositoryName": "del-repo"
  }}
},
"Outputs": {"RepoRef": {"Value": {"Ref": "Repo"}}}
}`

	createStackAndGetOutputs(t, client, "del-repo-stack", tmpl)

	_, err := backends.CodeArtifact.Backend.DescribeRepository(t.Context(), "del-domain", "del-repo")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("del-repo-stack")})
	require.NoError(t, err)

	_, err = backends.CodeArtifact.Backend.DescribeRepository(t.Context(), "del-domain", "del-repo")
	assert.Error(t, err, "delete must resolve Dom's real Name, not fall back to its ARN physical ID")
}

func testDeleteGetAttCodeArtifactPackageGroup(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Dom": {"Type": "AWS::CodeArtifact::Domain", "Properties": {"DomainName": "del-pg-domain"}},
  "PG": {"Type": "AWS::CodeArtifact::PackageGroup", "Properties": {
    "DomainName": {"Fn::GetAtt": ["Dom", "Name"]},
    "Pattern": "/npm/*"
  }}
},
"Outputs": {"PGRef": {"Value": {"Ref": "PG"}}}
}`

	createStackAndGetOutputs(t, client, "del-pg-stack", tmpl)

	_, err := backends.CodeArtifact.Backend.DescribePackageGroup(t.Context(), "del-pg-domain", "/npm/*")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("del-pg-stack")})
	require.NoError(t, err)

	_, err = backends.CodeArtifact.Backend.DescribePackageGroup(t.Context(), "del-pg-domain", "/npm/*")
	assert.Error(t, err, "delete must resolve Dom's real Name, not fall back to its ARN physical ID")
}
