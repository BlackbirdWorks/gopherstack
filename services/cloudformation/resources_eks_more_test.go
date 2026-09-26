package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// newEKSMoreTestClient wires a real aws-sdk-go-v2 CloudFormation client against
// the phase-3 backends (which already include a real EKS backend).
func newEKSMoreTestClient(t *testing.T) (*cloudformation.ServiceBackends, *cfnsdk.Client) {
	t.Helper()

	backends := newDependentServiceBackends(t)
	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	client := newTestClientForBackend(t, backend)

	return backends, client
}

func eksClusterTemplateFragment(clusterName string) string {
	return `"Cluster": {
    "Type": "AWS::EKS::Cluster",
    "Properties": {"Name": "` + clusterName + `", "RoleArn": "arn:aws:iam::000000000000:role/EKSRole"}
  },`
}

func TestCreateStack_EKSFargateProfile(t *testing.T) {
	t.Parallel()

	backends, client := newEKSMoreTestClient(t)

	tmpl := `{
"Resources": {
  ` + eksClusterTemplateFragment("fp-cluster") + `
  "Profile": {
    "Type": "AWS::EKS::FargateProfile",
    "Properties": {
      "ClusterName": {"Ref": "Cluster"},
      "FargateProfileName": "my-profile",
      "PodExecutionRoleArn": "arn:aws:iam::000000000000:role/PodExecRole",
      "Selectors": [{"Namespace": "my-namespace"}]
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Profile"}},
  "Arn": {"Value": {"Fn::GetAtt": ["Profile", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "eks-fargate-stack", tmpl)

	assert.Equal(t, "fp-cluster/my-profile", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "arn:aws:eks:us-east-1:000000000000:fargateprofile/fp-cluster/my-profile/")

	profile, err := backends.EKS.Backend.DescribeFargateProfile("fp-cluster", "my-profile")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/PodExecRole", profile.PodExecutionRoleARN)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("eks-fargate-stack")})
	require.NoError(t, err)

	_, err = backends.EKS.Backend.DescribeFargateProfile("fp-cluster", "my-profile")
	require.Error(t, err)
}

func TestCreateStack_EKSAddon(t *testing.T) {
	t.Parallel()

	backends, client := newEKSMoreTestClient(t)

	tmpl := `{
"Resources": {
  ` + eksClusterTemplateFragment("addon-cluster") + `
  "Addon": {
    "Type": "AWS::EKS::Addon",
    "Properties": {
      "ClusterName": {"Ref": "Cluster"},
      "AddonName": "vpc-cni",
      "ResolveConflicts": "OVERWRITE"
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Addon"}},
  "Arn": {"Value": {"Fn::GetAtt": ["Addon", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "eks-addon-stack", tmpl)

	assert.Equal(t, "addon-cluster|vpc-cni", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "arn:aws:eks:us-east-1:000000000000:addon/")

	addon, err := backends.EKS.Backend.DescribeAddon("addon-cluster", "vpc-cni")
	require.NoError(t, err)
	assert.Equal(t, "OVERWRITE", addon.ResolveConflicts)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("eks-addon-stack")})
	require.NoError(t, err)

	_, err = backends.EKS.Backend.DescribeAddon("addon-cluster", "vpc-cni")
	require.Error(t, err)
}

func TestCreateStack_EKSAccessEntry(t *testing.T) {
	t.Parallel()

	backends, client := newEKSMoreTestClient(t)

	principalARN := "arn:aws:iam::000000000000:role/my-role"

	tmpl := `{
"Resources": {
  ` + eksClusterTemplateFragment("ae-cluster") + `
  "Entry": {
    "Type": "AWS::EKS::AccessEntry",
    "Properties": {
      "ClusterName": {"Ref": "Cluster"},
      "PrincipalArn": "` + principalARN + `",
      "Type": "STANDARD",
      "KubernetesGroups": ["group-a"]
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Entry"}},
  "Arn": {"Value": {"Fn::GetAtt": ["Entry", "AccessEntryArn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "eks-accessentry-stack", tmpl)

	assert.Equal(t, principalARN, outputs["Ref"], "Ref must be PrincipalArn per the CFN docs")
	assert.Contains(t, outputs["Arn"], "arn:aws:eks:us-east-1:000000000000:access-entry/")

	entry, err := backends.EKS.Backend.DescribeAccessEntry("ae-cluster", principalARN)
	require.NoError(t, err)
	assert.Equal(t, []string{"group-a"}, entry.KubernetesGroups)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("eks-accessentry-stack")})
	require.NoError(t, err)

	_, err = backends.EKS.Backend.DescribeAccessEntry("ae-cluster", principalARN)
	require.Error(t, err)
}

func TestCreateStack_EKSPodIdentityAssociation(t *testing.T) {
	t.Parallel()

	backends, client := newEKSMoreTestClient(t)

	tmpl := `{
"Resources": {
  ` + eksClusterTemplateFragment("pia-cluster") + `
  "Assoc": {
    "Type": "AWS::EKS::PodIdentityAssociation",
    "Properties": {
      "ClusterName": {"Ref": "Cluster"},
      "Namespace": "default",
      "ServiceAccount": "my-sa",
      "RoleArn": "arn:aws:iam::000000000000:role/PodRole"
    }
  }
},
"Outputs": {
  "Id": {"Value": {"Ref": "Assoc"}},
  "Arn": {"Value": {"Fn::GetAtt": ["Assoc", "AssociationArn"]}},
  "ExternalId": {"Value": {"Fn::GetAtt": ["Assoc", "ExternalId"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "eks-podidentity-stack", tmpl)

	assert.Contains(t, outputs["Arn"], "arn:aws:eks:us-east-1:000000000000:podidentityassociation/pia-cluster/")
	assert.NotEmpty(t, outputs["ExternalId"])

	assocID := outputs["Id"]

	assoc, err := backends.EKS.Backend.DescribePodIdentityAssociation("pia-cluster", assocID)
	require.NoError(t, err)
	assert.Equal(t, "my-sa", assoc.ServiceAccount)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("eks-podidentity-stack")})
	require.NoError(t, err)

	_, err = backends.EKS.Backend.DescribePodIdentityAssociation("pia-cluster", assocID)
	require.Error(t, err)
}

func TestCreateStack_EKSIdentityProviderConfig(t *testing.T) {
	t.Parallel()

	backends, client := newEKSMoreTestClient(t)

	tmpl := `{
"Resources": {
  ` + eksClusterTemplateFragment("idp-cluster") + `
  "Idp": {
    "Type": "AWS::EKS::IdentityProviderConfig",
    "Properties": {
      "ClusterName": {"Ref": "Cluster"},
      "IdentityProviderConfigName": "my-idp",
      "Type": "oidc",
      "Oidc": {"ClientId": "kubernetes", "IssuerUrl": "https://example.com"}
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Idp"}},
  "Arn": {"Value": {"Fn::GetAtt": ["Idp", "IdentityProviderConfigArn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "eks-idp-stack", tmpl)

	assert.Equal(t, "idp-cluster/oidc/my-idp", outputs["Ref"])
	assert.Contains(
		t, outputs["Arn"], "arn:aws:eks:us-east-1:000000000000:identityproviderconfig/idp-cluster/oidc/my-idp/",
	)

	cfg, err := backends.EKS.Backend.DescribeIdentityProviderConfig("idp-cluster", "my-idp")
	require.NoError(t, err)
	assert.Equal(t, "kubernetes", cfg.OIDC["ClientId"])

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("eks-idp-stack")})
	require.NoError(t, err)

	_, err = backends.EKS.Backend.DescribeIdentityProviderConfig("idp-cluster", "my-idp")
	require.Error(t, err)
}
