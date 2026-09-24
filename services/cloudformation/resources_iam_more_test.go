package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_IAMMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testIAMGroupPolicy, "group_policy"},
		{testIAMRolePolicy, "role_policy"},
		{testIAMUserPolicy, "user_policy"},
		{testIAMServerCertificate, "server_certificate"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testIAMGroupPolicy(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	_, err := backends.IAM.Backend.CreateGroup("my-group", "/")
	require.NoError(t, err)

	tmpl := `{
"Resources": {
  "GP": {
    "Type": "AWS::IAM::GroupPolicy",
    "Properties": {
      "GroupName": "my-group",
      "PolicyName": "my-group-policy",
      "PolicyDocument": {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]
      }
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "GP"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "gp-stack", tmpl, cfntypes.CapabilityCapabilityIam)
	assert.Equal(t, "my-group-policy", outputs["Ref"])

	doc, err := backends.IAM.Backend.GetGroupPolicy("my-group", "my-group-policy")
	require.NoError(t, err)
	assert.Contains(t, doc, "s3:GetObject")

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("gp-stack")})
	require.NoError(t, err)

	_, err = backends.IAM.Backend.GetGroupPolicy("my-group", "my-group-policy")
	require.Error(t, err)
}

func testIAMRolePolicy(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	_, err := backends.IAM.Backend.CreateRole("my-role", "/", "", "")
	require.NoError(t, err)

	tmpl := `{
"Resources": {
  "RP": {
    "Type": "AWS::IAM::RolePolicy",
    "Properties": {
      "RoleName": "my-role",
      "PolicyName": "my-role-policy",
      "PolicyDocument": {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]
      }
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "RP"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "rp-stack", tmpl, cfntypes.CapabilityCapabilityIam)
	assert.Equal(t, "my-role-policy", outputs["Ref"])

	doc, err := backends.IAM.Backend.GetRolePolicy("my-role", "my-role-policy")
	require.NoError(t, err)
	assert.Contains(t, doc, "s3:GetObject")

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("rp-stack")})
	require.NoError(t, err)

	_, err = backends.IAM.Backend.GetRolePolicy("my-role", "my-role-policy")
	require.Error(t, err)
}

func testIAMUserPolicy(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	_, err := backends.IAM.Backend.CreateUser("my-user", "/", "")
	require.NoError(t, err)

	tmpl := `{
"Resources": {
  "UP": {
    "Type": "AWS::IAM::UserPolicy",
    "Properties": {
      "UserName": "my-user",
      "PolicyName": "my-user-policy",
      "PolicyDocument": {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]
      }
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "UP"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "up-stack", tmpl, cfntypes.CapabilityCapabilityIam)
	assert.Equal(t, "my-user-policy", outputs["Ref"])

	doc, err := backends.IAM.Backend.GetUserPolicy("my-user", "my-user-policy")
	require.NoError(t, err)
	assert.Contains(t, doc, "s3:GetObject")

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("up-stack")})
	require.NoError(t, err)

	_, err = backends.IAM.Backend.GetUserPolicy("my-user", "my-user-policy")
	require.Error(t, err)
}

func testIAMServerCertificate(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "SC": {
    "Type": "AWS::IAM::ServerCertificate",
    "Properties": {
      "ServerCertificateName": "my-cert",
      "CertificateBody": "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----",
      "CertificateChain": "-----BEGIN CERTIFICATE-----\nMIIC\n-----END CERTIFICATE-----"
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "SC"}},
  "Arn": {"Value": {"Fn::GetAtt": ["SC", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "sc-stack", tmpl, cfntypes.CapabilityCapabilityIam)
	assert.Equal(t, "my-cert", outputs["Ref"])
	assert.NotEmpty(t, outputs["Arn"])

	cert, err := backends.IAM.Backend.GetServerCertificate("my-cert")
	require.NoError(t, err)
	assert.Equal(t, "my-cert", cert.ServerCertificateName)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sc-stack")})
	require.NoError(t, err)

	_, err = backends.IAM.Backend.GetServerCertificate("my-cert")
	require.Error(t, err)
}
