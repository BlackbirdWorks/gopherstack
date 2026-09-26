package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_CognitoMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testCognitoUserPoolResourceServer, "user_pool_resource_server"},
		{testCognitoUserPoolIdentityProvider, "user_pool_identity_provider"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testCognitoUserPoolResourceServer(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Pool": {"Type": "AWS::Cognito::UserPool", "Properties": {"UserPoolName": "rs-pool"}},
  "RS": {
    "Type": "AWS::Cognito::UserPoolResourceServer",
    "Properties": {
      "UserPoolId": {"Ref": "Pool"},
      "Identifier": "my-api",
      "Name": "My API",
      "Scopes": [{"ScopeName": "read", "ScopeDescription": "read access"}]
    }
  }
},
"Outputs": {
  "PoolRef": {"Value": {"Ref": "Pool"}},
  "RSRef": {"Value": {"Ref": "RS"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "rs-stack", tmpl)
	assert.Equal(t, "my-api", outputs["RSRef"])

	poolID := outputs["PoolRef"]

	rs, err := backends.CognitoIDP.Backend.DescribeResourceServer(poolID, "my-api")
	require.NoError(t, err)
	assert.Equal(t, "My API", rs.Name)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("rs-stack")})
	require.NoError(t, err)

	_, err = backends.CognitoIDP.Backend.DescribeResourceServer(poolID, "my-api")
	require.Error(t, err)
}

func testCognitoUserPoolIdentityProvider(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Pool": {"Type": "AWS::Cognito::UserPool", "Properties": {"UserPoolName": "idp-pool"}},
  "IDP": {
    "Type": "AWS::Cognito::UserPoolIdentityProvider",
    "Properties": {
      "UserPoolId": {"Ref": "Pool"},
      "ProviderName": "MySAMLProvider",
      "ProviderType": "SAML",
      "ProviderDetails": {"MetadataURL": "https://example.com/metadata"}
    }
  }
},
"Outputs": {
  "PoolRef": {"Value": {"Ref": "Pool"}},
  "IDPRef": {"Value": {"Ref": "IDP"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "idp-stack", tmpl)
	assert.Equal(t, "MySAMLProvider", outputs["IDPRef"])

	poolID := outputs["PoolRef"]

	idp, err := backends.CognitoIDP.Backend.DescribeIdentityProvider(poolID, "MySAMLProvider")
	require.NoError(t, err)
	assert.Equal(t, "SAML", idp.ProviderType)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("idp-stack")})
	require.NoError(t, err)

	_, err = backends.CognitoIDP.Backend.DescribeIdentityProvider(poolID, "MySAMLProvider")
	require.Error(t, err)
}
