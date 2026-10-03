package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_IAMExtrasTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testIAMSAMLProvider, "saml_provider"},
		{testIAMVirtualMFADevice, "virtual_mfa_device"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testIAMSAMLProvider(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "Provider": {
    "Type": "AWS::IAM::SAMLProvider",
    "Properties": {"Name": "unit-saml-provider", "SamlMetadataDocument": "<EntityDescriptor></EntityDescriptor>"}
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Provider"}},
  "Arn": {"Value": {"Fn::GetAtt": ["Provider", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "iam-saml-stack", tmpl, cfntypes.CapabilityCapabilityIam)
	assert.Equal(t, outputs["Ref"], outputs["Arn"])
	assert.Contains(t, outputs["Ref"], "saml-provider/unit-saml-provider")

	p, err := backends.IAM.Backend.GetSAMLProvider(outputs["Ref"])
	require.NoError(t, err)
	assert.NotNil(t, p)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iam-saml-stack")})
	require.NoError(t, err)

	_, err = backends.IAM.Backend.GetSAMLProvider(outputs["Ref"])
	require.Error(t, err)
}

func testIAMVirtualMFADevice(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "Device": {
    "Type": "AWS::IAM::VirtualMFADevice",
    "Properties": {"VirtualMfaDeviceName": "unit-mfa-device", "Path": "/", "Users": ["unit-user"]}
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Device"}},
  "SerialNumber": {"Value": {"Fn::GetAtt": ["Device", "SerialNumber"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "iam-mfa-stack", tmpl, cfntypes.CapabilityCapabilityIam)
	assert.Equal(t, outputs["Ref"], outputs["SerialNumber"])
	assert.Contains(t, outputs["Ref"], "mfa/unit-mfa-device")

	_, _, err := backends.IAM.Backend.GetVirtualMFADevice(outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iam-mfa-stack")})
	require.NoError(t, err)

	_, _, err = backends.IAM.Backend.GetVirtualMFADevice(outputs["Ref"])
	require.Error(t, err)
}
