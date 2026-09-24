package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testRSAPublicKeyPEM is a real 2048-bit RSA public key in PEM form, required
// by AWS::CloudFront::PublicKey's backend (validatePEMPublicKey parses and
// checks the key length).
const testRSAPublicKeyPEM = "-----BEGIN PUBLIC KEY-----\n" +
	"MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAp+pA7y4rv0Rn0tdXmPbT\n" +
	"nPW+8GW7IQvTEQs6n/9Gi7Tywawjagoh1HiEzFXW8QYSsODOD67kAFrf49RtGWTN\n" +
	"6egWtF8PmDKLlkRRorIpcPPkFSuQw59grKBfGIXfU58Dd5skZvavLA6rSvcqbWOO\n" +
	"UXPaoeE5f0qkaTovQCkv4Hup/9Nhok1xPC6Q0eq00lsw41mnhmHVg3qxkDV3G6oC\n" +
	"WycbD2pyjuI+ZrqdBHg7XlX3SgMqOzJfazrSlnuJT15YANOccOcS3lK9FqM0rQ6L\n" +
	"3SfX6q4LTXpgjb31fds5XNkJ/NAnAbM/jakpIjYgRCcoQqYZHn90U4kb0cf5N2mP\n" +
	"hQIDAQAB\n" +
	"-----END PUBLIC KEY-----\n"

func TestCreateStack_CloudFrontMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testCFOriginRequestPolicy, "origin_request_policy"},
		{testCFKeyGroup, "key_group"},
		{testCFPublicKey, "public_key"},
		{testCFOAI, "cloudfront_origin_access_identity"},
		{testCFRealtimeLogConfig, "realtime_log_config"},
		{testCFKeyValueStore, "key_value_store"},
		{testCFContinuousDeploymentPolicy, "continuous_deployment_policy"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testCFOriginRequestPolicy(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "ORP": {
    "Type": "AWS::CloudFront::OriginRequestPolicy",
    "Properties": {"OriginRequestPolicyConfig": {"Name": "my-orp", "Comment": "test"}}
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "ORP"}},
  "LastModifiedTime": {"Value": {"Fn::GetAtt": ["ORP", "LastModifiedTime"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "orp-stack", tmpl)
	require.NotEmpty(t, outputs["Ref"])
	assert.NotEmpty(t, outputs["LastModifiedTime"])

	p, err := backends.CloudFront.Backend.GetOriginRequestPolicy(outputs["Ref"])
	require.NoError(t, err)
	assert.Equal(t, "my-orp", p.Name)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("orp-stack")})
	require.NoError(t, err)

	_, err = backends.CloudFront.Backend.GetOriginRequestPolicy(outputs["Ref"])
	require.Error(t, err)
}

func testCFKeyGroup(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "PK": {
    "Type": "AWS::CloudFront::PublicKey",
    "Properties": {"PublicKeyConfig": {
      "Name": "kg-pk", "CallerReference": "kg-pk-ref", "EncodedKey": "` + jsonEscapePEM(testRSAPublicKeyPEM) + `"
    }}
  },
  "KG": {
    "Type": "AWS::CloudFront::KeyGroup",
    "Properties": {"KeyGroupConfig": {"Name": "my-key-group", "Items": [{"Ref": "PK"}]}}
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "KG"}},
  "LastModifiedTime": {"Value": {"Fn::GetAtt": ["KG", "LastModifiedTime"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "kg-stack", tmpl)
	require.NotEmpty(t, outputs["Ref"])
	assert.NotEmpty(t, outputs["LastModifiedTime"])

	kg, err := backends.CloudFront.Backend.GetKeyGroup(outputs["Ref"])
	require.NoError(t, err)
	assert.Equal(t, "my-key-group", kg.Name)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("kg-stack")})
	require.NoError(t, err)

	_, err = backends.CloudFront.Backend.GetKeyGroup(outputs["Ref"])
	require.Error(t, err)
}

func testCFPublicKey(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "PK": {
    "Type": "AWS::CloudFront::PublicKey",
    "Properties": {"PublicKeyConfig": {
      "Name": "my-public-key", "CallerReference": "pk-ref", "EncodedKey": "` +
		jsonEscapePEM(testRSAPublicKeyPEM) + `"
    }}
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "PK"}},
  "CreatedTime": {"Value": {"Fn::GetAtt": ["PK", "CreatedTime"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "pk-stack", tmpl)
	require.NotEmpty(t, outputs["Ref"])
	assert.NotEmpty(t, outputs["CreatedTime"])

	pk, err := backends.CloudFront.Backend.GetPublicKey(outputs["Ref"])
	require.NoError(t, err)
	assert.Equal(t, "my-public-key", pk.Name)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("pk-stack")})
	require.NoError(t, err)

	_, err = backends.CloudFront.Backend.GetPublicKey(outputs["Ref"])
	require.Error(t, err)
}

func testCFOAI(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "OAI": {
    "Type": "AWS::CloudFront::CloudFrontOriginAccessIdentity",
    "Properties": {"CloudFrontOriginAccessIdentityConfig": {"Comment": "my oai", "CallerReference": "oai-ref"}}
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "OAI"}},
  "S3CanonicalUserId": {"Value": {"Fn::GetAtt": ["OAI", "S3CanonicalUserId"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "oai-stack", tmpl)
	require.NotEmpty(t, outputs["Ref"])
	assert.NotEmpty(t, outputs["S3CanonicalUserId"])

	oai, err := backends.CloudFront.Backend.GetOAI(outputs["Ref"])
	require.NoError(t, err)
	assert.Equal(t, "my oai", oai.Comment)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("oai-stack")})
	require.NoError(t, err)

	_, err = backends.CloudFront.Backend.GetOAI(outputs["Ref"])
	require.Error(t, err)
}

func testCFRealtimeLogConfig(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "RLC": {
    "Type": "AWS::CloudFront::RealtimeLogConfig",
    "Properties": {
      "Name": "my-realtime-log-config",
      "SamplingRate": 50,
      "Fields": ["timestamp", "c-ip"],
      "EndPoints": [{
        "StreamType": "Kinesis",
        "KinesisStreamConfig": {
          "RoleArn": "arn:aws:iam::000000000000:role/rlc-role",
          "StreamArn": "arn:aws:kinesis:us-east-1:000000000000:stream/rlc-stream"
        }
      }]
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "RLC"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "rlc-stack", tmpl)
	require.NotEmpty(t, outputs["Ref"])

	cfg, err := backends.CloudFront.Backend.GetRealtimeLogConfig(outputs["Ref"])
	require.NoError(t, err)
	assert.Equal(t, "my-realtime-log-config", cfg.Name)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("rlc-stack")})
	require.NoError(t, err)

	_, err = backends.CloudFront.Backend.GetRealtimeLogConfig(outputs["Ref"])
	require.Error(t, err)
}

func testCFKeyValueStore(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "KVS": {
    "Type": "AWS::CloudFront::KeyValueStore",
    "Properties": {"Name": "my-kvs", "Comment": "test store"}
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "KVS"}},
  "Arn": {"Value": {"Fn::GetAtt": ["KVS", "Arn"]}},
  "Status": {"Value": {"Fn::GetAtt": ["KVS", "Status"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "kvs-stack", tmpl)
	assert.Equal(t, "my-kvs", outputs["Ref"])
	assert.NotEmpty(t, outputs["Arn"])
	assert.NotEmpty(t, outputs["Status"])

	kvs, err := backends.CloudFront.Backend.GetKeyValueStore("my-kvs")
	require.NoError(t, err)
	assert.Equal(t, "test store", kvs.Comment)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("kvs-stack")})
	require.NoError(t, err)

	_, err = backends.CloudFront.Backend.GetKeyValueStore("my-kvs")
	require.Error(t, err)
}

func testCFContinuousDeploymentPolicy(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "CDP": {
    "Type": "AWS::CloudFront::ContinuousDeploymentPolicy",
    "Properties": {
      "ContinuousDeploymentPolicyConfig": {
        "Enabled": true,
        "StagingDistributionDnsNames": ["d111111abcdef8.cloudfront.net"]
      }
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "CDP"}},
  "LastModifiedTime": {"Value": {"Fn::GetAtt": ["CDP", "LastModifiedTime"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "cdp-stack", tmpl)
	require.NotEmpty(t, outputs["Ref"])
	assert.NotEmpty(t, outputs["LastModifiedTime"])

	cdp, err := backends.CloudFront.Backend.GetContinuousDeploymentPolicy(outputs["Ref"])
	require.NoError(t, err)
	assert.True(t, cdp.Enabled)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("cdp-stack")})
	require.NoError(t, err)

	_, err = backends.CloudFront.Backend.GetContinuousDeploymentPolicy(outputs["Ref"])
	require.Error(t, err)
}

// jsonEscapePEM escapes a PEM block's embedded newlines for inclusion as a JSON string literal.
func jsonEscapePEM(pem string) string {
	out := make([]byte, 0, len(pem)+16)

	for _, c := range []byte(pem) {
		if c == '\n' {
			out = append(out, '\\', 'n')

			continue
		}

		out = append(out, c)
	}

	return string(out)
}
