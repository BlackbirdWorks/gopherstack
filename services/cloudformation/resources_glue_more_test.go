package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_GlueMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testGlueClassifier, "classifier"},
		{testGlueRegistry, "registry"},
		{testGlueSchema, "schema"},
		{testGlueSecurityConfiguration, "security_configuration"},
		{testGlueDevEndpoint, "dev_endpoint"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testGlueClassifier(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "CL": {
    "Type": "AWS::Glue::Classifier",
    "Properties": {
      "CsvClassifier": {
        "Name": "my-csv-classifier",
        "Delimiter": ",",
        "Header": ["col1", "col2"]
      }
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "CL"}},
  "Name": {"Value": {"Fn::GetAtt": ["CL", "Name"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "glue-cl-stack", tmpl)
	assert.Equal(t, "my-csv-classifier", outputs["Ref"])
	assert.Equal(t, "my-csv-classifier", outputs["Name"])

	c, err := backends.Glue.Backend.GetClassifier("my-csv-classifier")
	require.NoError(t, err)
	require.NotNil(t, c.CsvClassifier)
	assert.Equal(t, ",", c.CsvClassifier.Delimiter)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("glue-cl-stack")})
	require.NoError(t, err)

	_, err = backends.Glue.Backend.GetClassifier("my-csv-classifier")
	require.Error(t, err)
}

func testGlueRegistry(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "RG": {
    "Type": "AWS::Glue::Registry",
    "Properties": {
      "Name": "my-registry",
      "Description": "test registry"
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "RG"}},
  "Arn": {"Value": {"Fn::GetAtt": ["RG", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "glue-rg-stack", tmpl)
	assert.Equal(t, "my-registry", outputs["Ref"])
	assert.NotEmpty(t, outputs["Arn"])

	reg, err := backends.Glue.Backend.DescribeRegistry("my-registry")
	require.NoError(t, err)
	assert.Equal(t, "test registry", reg.Description)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("glue-rg-stack")})
	require.NoError(t, err)

	_, err = backends.Glue.Backend.DescribeRegistry("my-registry")
	require.Error(t, err)
}

func testGlueSchema(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	_, err := backends.Glue.Backend.CreateRegistry("schema-registry", "", nil)
	require.NoError(t, err)

	tmpl := `{
"Resources": {
  "SCH": {
    "Type": "AWS::Glue::Schema",
    "Properties": {
      "Name": "my-schema",
      "Registry": {"Name": "schema-registry"},
      "DataFormat": "JSON",
      "Compatibility": "NONE",
      "SchemaDefinition": "{\"type\": \"object\"}"
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "SCH"}},
  "Arn": {"Value": {"Fn::GetAtt": ["SCH", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "glue-sch-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["Arn"])
	assert.NotEmpty(t, outputs["Ref"])

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("glue-sch-stack")})
	require.NoError(t, err)

	_, err = backends.Glue.Backend.DeleteSchema("schema-registry", "my-schema")
	require.Error(t, err)
}

func testGlueSecurityConfiguration(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "SEC": {
    "Type": "AWS::Glue::SecurityConfiguration",
    "Properties": {
      "Name": "my-security-config",
      "EncryptionConfiguration": {
        "CloudWatchEncryption": {
          "CloudWatchEncryptionMode": "SSE-KMS",
          "KmsKeyArn": "arn:aws:kms:us-east-1:000000000000:key/test"
        }
      }
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "SEC"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "glue-sec-stack", tmpl)
	assert.Equal(t, "my-security-config", outputs["Ref"])

	sc, err := backends.Glue.Backend.GetSecurityConfiguration("my-security-config")
	require.NoError(t, err)
	require.NotNil(t, sc.EncryptionConfiguration.CloudWatchEncryption)
	assert.Equal(t, "SSE-KMS", sc.EncryptionConfiguration.CloudWatchEncryption.CloudWatchEncryptionMode)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("glue-sec-stack")})
	require.NoError(t, err)

	_, err = backends.Glue.Backend.GetSecurityConfiguration("my-security-config")
	require.Error(t, err)
}

func testGlueDevEndpoint(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "DE": {
    "Type": "AWS::Glue::DevEndpoint",
    "Properties": {
      "EndpointName": "my-dev-endpoint",
      "RoleArn": "arn:aws:iam::000000000000:role/GlueRole"
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "DE"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "glue-de-stack", tmpl)
	assert.Equal(t, "my-dev-endpoint", outputs["Ref"])

	dep, err := backends.Glue.Backend.GetDevEndpoint("my-dev-endpoint")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/GlueRole", dep.RoleArn)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("glue-de-stack")})
	require.NoError(t, err)

	_, err = backends.Glue.Backend.GetDevEndpoint("my-dev-endpoint")
	require.Error(t, err)
}
