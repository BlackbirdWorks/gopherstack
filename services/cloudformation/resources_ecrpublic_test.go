package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	ecrpublicbackend "github.com/blackbirdworks/gopherstack/services/ecrpublic"
)

// newECRPublicTestClient wires a real aws-sdk-go-v2 CloudFormation client
// against a backend with ECRPublic (among the other backends
// newMoreTypesServiceBackends already wires) set to a real in-memory service
// backend.
func newECRPublicTestClient(t *testing.T) (*cloudformation.ServiceBackends, *cfnsdk.Client) {
	t.Helper()

	backends := newMoreTypesServiceBackends(t)
	backends.ECRPublic = ecrpublicbackend.NewHandler(ecrpublicbackend.NewInMemoryBackend("000000000000", "us-east-1"))

	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	client := newTestClientForBackend(t, backend)

	return backends, client
}

func TestCreateStack_ECRPublicRepository(t *testing.T) {
	t.Parallel()

	backends, client := newECRPublicTestClient(t)

	tmpl := `{
"Resources": {"Repo": {"Type": "AWS::ECR::PublicRepository", "Properties": {
  "RepositoryName": "my-repo",
  "RepositoryCatalogData": {"AboutText": "about text", "RepositoryDescription": "short desc"}
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "Repo"}},
  "Arn": {"Value": {"Fn::GetAtt": ["Repo", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ecrpublic-stack", tmpl)
	assert.Equal(t, "my-repo", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "repository/my-repo")

	repos, err := backends.ECRPublic.Backend.DescribeRepositories("", []string{"my-repo"})
	require.NoError(t, err)
	require.Len(t, repos, 1)
	assert.Equal(t, "about text", repos[0].CatalogData.AboutText)
	assert.Equal(t, "short desc", repos[0].CatalogData.Description)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ecrpublic-stack")})
	require.NoError(t, err)

	_, err = backends.ECRPublic.Backend.DescribeRepositories("", []string{"my-repo"})
	require.Error(t, err)
}
