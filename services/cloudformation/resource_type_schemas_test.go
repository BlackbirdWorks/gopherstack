package cloudformation_test

import (
	"encoding/json"
	"encoding/xml"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// describeTypeXMLResult mirrors the DescribeTypeResult XML shape returned by the
// handler, used here only to pull the raw Schema JSON string back out for
// unmarshalling and assertions.
type describeTypeXMLResult struct {
	XMLName          xml.Name `xml:"DescribeTypeResponse"`
	Arn              string   `xml:"DescribeTypeResult>Arn"`
	DefaultVersionID string   `xml:"DescribeTypeResult>DefaultVersionId"`
	Schema           string   `xml:"DescribeTypeResult>Schema"`
	Type             string   `xml:"DescribeTypeResult>Type"`
	TypeName         string   `xml:"DescribeTypeResult>TypeName"`
	Visibility       string   `xml:"DescribeTypeResult>Visibility"`
	ProvisioningType string   `xml:"DescribeTypeResult>ProvisioningType"`
}

type parsedProviderSchema struct {
	Properties           map[string]any `json:"properties"`
	TypeName             string         `json:"typeName"`
	Description          string         `json:"description"`
	PrimaryIdentifier    []string       `json:"primaryIdentifier"`
	ReadOnlyProperties   []string       `json:"readOnlyProperties"`
	CreateOnlyProperties []string       `json:"createOnlyProperties"`
}

// TestHandler_DescribeType_CatalogSchemas verifies that DescribeType returns a
// realistic resource-provider schema (real primary properties, correct JSON
// types, primaryIdentifier) for the modeled AWS::* resource type catalog.
func TestHandler_DescribeType_CatalogSchemas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantPropertyTypes map[string]string
		name              string
		typeName          string
		wantPrimaryID     string
		wantProperties    []string
		wantReadOnly      []string
	}{
		{
			name:           "S3 Bucket",
			typeName:       "AWS::S3::Bucket",
			wantPrimaryID:  "/properties/BucketName",
			wantProperties: []string{"BucketName", "Arn", "WebsiteURL"},
			wantReadOnly:   []string{"/properties/Arn", "/properties/WebsiteURL"},
			wantPropertyTypes: map[string]string{
				"BucketName": "string",
			},
		},
		{
			name:           "DynamoDB Table",
			typeName:       "AWS::DynamoDB::Table",
			wantPrimaryID:  "/properties/TableName",
			wantProperties: []string{"TableName", "AttributeDefinitions", "KeySchema", "Arn", "StreamArn"},
			wantReadOnly:   []string{"/properties/Arn", "/properties/StreamArn"},
			wantPropertyTypes: map[string]string{
				"TableName":            "string",
				"AttributeDefinitions": "array",
			},
		},
		{
			name:           "SQS Queue",
			typeName:       "AWS::SQS::Queue",
			wantPrimaryID:  "/properties/QueueUrl",
			wantProperties: []string{"QueueName", "FifoQueue", "Arn", "QueueUrl"},
			wantReadOnly:   []string{"/properties/Arn", "/properties/QueueUrl"},
			wantPropertyTypes: map[string]string{
				"FifoQueue": "boolean",
			},
		},
		{
			name:           "SNS Topic",
			typeName:       "AWS::SNS::Topic",
			wantPrimaryID:  "/properties/TopicArn",
			wantProperties: []string{"TopicName", "TopicArn", "FifoTopic"},
			wantReadOnly:   []string{"/properties/TopicArn"},
			wantPropertyTypes: map[string]string{
				"TopicName": "string",
			},
		},
		{
			name:           "Lambda Function",
			typeName:       "AWS::Lambda::Function",
			wantPrimaryID:  "/properties/FunctionName",
			wantProperties: []string{"FunctionName", "Runtime", "Handler", "Role", "Arn"},
			wantReadOnly:   []string{"/properties/Arn"},
			wantPropertyTypes: map[string]string{
				"MemorySize": "integer",
			},
		},
		{
			name:           "IAM Role",
			typeName:       "AWS::IAM::Role",
			wantPrimaryID:  "/properties/RoleName",
			wantProperties: []string{"RoleName", "AssumeRolePolicyDocument", "Arn", "RoleId"},
			wantReadOnly:   []string{"/properties/Arn", "/properties/RoleId"},
			wantPropertyTypes: map[string]string{
				"AssumeRolePolicyDocument": "object",
			},
		},
		{
			name:           "EC2 Instance",
			typeName:       "AWS::EC2::Instance",
			wantPrimaryID:  "/properties/InstanceId",
			wantProperties: []string{"ImageId", "InstanceType", "InstanceId", "PublicIp", "PrivateIp"},
			wantReadOnly:   []string{"/properties/InstanceId", "/properties/PublicIp", "/properties/PrivateIp"},
			wantPropertyTypes: map[string]string{
				"ImageId": "string",
			},
		},
		{
			name:           "EC2 SecurityGroup",
			typeName:       "AWS::EC2::SecurityGroup",
			wantPrimaryID:  "/properties/GroupId",
			wantProperties: []string{"GroupName", "GroupDescription", "VpcId", "GroupId"},
			wantReadOnly:   []string{"/properties/GroupId"},
			wantPropertyTypes: map[string]string{
				"SecurityGroupIngress": "array",
			},
		},
		{
			name:           "CloudWatch Alarm",
			typeName:       "AWS::CloudWatch::Alarm",
			wantPrimaryID:  "/properties/AlarmName",
			wantProperties: []string{"AlarmName", "MetricName", "Namespace", "Threshold", "Arn"},
			wantReadOnly:   []string{"/properties/Arn"},
			wantPropertyTypes: map[string]string{
				"Threshold":         "number",
				"EvaluationPeriods": "integer",
			},
		},
		{
			name:           "KMS Key",
			typeName:       "AWS::KMS::Key",
			wantPrimaryID:  "/properties/KeyId",
			wantProperties: []string{"KeyId", "Arn", "KeyPolicy", "Enabled"},
			wantReadOnly:   []string{"/properties/KeyId", "/properties/Arn"},
			wantPropertyTypes: map[string]string{
				"Enabled": "boolean",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHandler()
			resp := postFormValues(t, h, url.Values{
				"Action":   {"DescribeType"},
				"TypeName": {tc.typeName},
			})
			resp.mustOK(t)

			var parsed describeTypeXMLResult
			require.NoError(t, xml.Unmarshal([]byte(resp.Body), &parsed))
			assert.Equal(t, tc.typeName, parsed.TypeName)
			assert.Equal(t, "RESOURCE", parsed.Type)
			assert.Equal(t, "PUBLIC", parsed.Visibility)
			assert.Equal(t, "FULLY_MUTABLE", parsed.ProvisioningType)
			require.NotEmpty(t, parsed.Schema)

			var schema parsedProviderSchema
			require.NoError(t, json.Unmarshal([]byte(parsed.Schema), &schema))
			assert.Equal(t, tc.typeName, schema.TypeName)
			assert.NotEmpty(t, schema.Description)
			require.Contains(t, schema.PrimaryIdentifier, tc.wantPrimaryID)

			for _, prop := range tc.wantProperties {
				assert.Contains(t, schema.Properties, prop, "expected property %q in schema for %s", prop, tc.typeName)
			}

			for _, ro := range tc.wantReadOnly {
				assert.Contains(t, schema.ReadOnlyProperties, ro)
			}

			for prop, wantType := range tc.wantPropertyTypes {
				propDef, ok := schema.Properties[prop].(map[string]any)
				require.True(t, ok, "property %q should be an object definition", prop)
				assert.Equal(t, wantType, propDef["type"])
			}
		})
	}
}

// TestHandler_DescribeType_UnknownType_ValidFallback verifies that a type not
// present in the modeled catalog and not registered via RegisterType still
// returns a valid, non-crashing DescribeType response with a usable schema.
func TestHandler_DescribeType_UnknownType_ValidFallback(t *testing.T) {
	t.Parallel()

	h := newHandler()
	resp := postFormValues(t, h, url.Values{
		"Action":   {"DescribeType"},
		"TypeName": {"AWS::Totally::Unmodeled"},
	})
	resp.mustOK(t)

	var parsed describeTypeXMLResult
	require.NoError(t, xml.Unmarshal([]byte(resp.Body), &parsed))
	assert.Equal(t, "AWS::Totally::Unmodeled", parsed.TypeName)
	assert.Equal(t, "RESOURCE", parsed.Type)
	assert.Equal(t, "PUBLIC", parsed.Visibility)
	assert.Equal(t, "FULLY_MUTABLE", parsed.ProvisioningType)
	require.NotEmpty(t, parsed.Schema)

	var schema parsedProviderSchema
	require.NoError(t, json.Unmarshal([]byte(parsed.Schema), &schema))
	assert.Equal(t, "AWS::Totally::Unmodeled", schema.TypeName)
	require.NotEmpty(t, schema.PrimaryIdentifier)
	assert.NotEmpty(t, schema.Properties)
}
