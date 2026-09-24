package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_AWSConfigTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testConfigConfigRule, "config_rule"},
		{testConfigConfigurationRecorder, "configuration_recorder"},
		{testConfigDeliveryChannel, "delivery_channel"},
		{testConfigConfigurationAggregator, "configuration_aggregator"},
		{testConfigAggregationAuthorization, "aggregation_authorization"},
		{testConfigConformancePack, "conformance_pack"},
		{testConfigRemediationConfiguration, "remediation_configuration"},
		{testConfigStoredQuery, "stored_query"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testConfigConfigRule(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"CR": {"Type": "AWS::Config::ConfigRule", "Properties": {
  "ConfigRuleName": "cr-1", "Source": {"Owner": "AWS", "SourceIdentifier": "REQUIRED_TAGS"}
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "CR"}},
  "Arn": {"Value": {"Fn::GetAtt": ["CR", "Arn"]}},
  "Id": {"Value": {"Fn::GetAtt": ["CR", "ConfigRuleId"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "config-configrule-stack", tmpl)
	assert.Equal(t, "cr-1", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "config-rule/")
	assert.NotEmpty(t, outputs["Id"])

	rules, err := backends.AWSConfig.Backend.DescribeConfigRules([]string{"cr-1"})
	require.NoError(t, err)
	require.Len(t, rules, 1)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("config-configrule-stack")},
	)
	require.NoError(t, err)

	_, err = backends.AWSConfig.Backend.DescribeConfigRules([]string{"cr-1"})
	require.Error(t, err)
}

func testConfigConfigurationRecorder(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"Rec": {"Type": "AWS::Config::ConfigurationRecorder", "Properties": {
  "Name": "default", "RoleARN": "arn:aws:iam::000000000000:role/config-role",
  "RecordingGroup": {"AllSupported": true}
}}},
"Outputs": {"Ref": {"Value": {"Ref": "Rec"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "config-recorder-stack", tmpl)
	assert.Equal(t, "default", outputs["Ref"])

	recorders := backends.AWSConfig.Backend.DescribeConfigurationRecorders([]string{"default"})
	require.Len(t, recorders, 1)
	assert.True(t, recorders[0].RecordingGroup.AllSupported)

	_, err := client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("config-recorder-stack")},
	)
	require.NoError(t, err)

	assert.Empty(t, backends.AWSConfig.Backend.DescribeConfigurationRecorders([]string{"default"}))
}

func testConfigDeliveryChannel(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"DCh": {"Type": "AWS::Config::DeliveryChannel", "Properties": {
  "Name": "default", "S3BucketName": "my-config-bucket"
}}},
"Outputs": {"Ref": {"Value": {"Ref": "DCh"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "config-deliverychannel-stack", tmpl)
	assert.Equal(t, "default", outputs["Ref"])

	channels := backends.AWSConfig.Backend.DescribeDeliveryChannels([]string{"default"})
	require.Len(t, channels, 1)
	assert.Equal(t, "my-config-bucket", channels[0].S3Bucket)

	_, err := client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("config-deliverychannel-stack")},
	)
	require.NoError(t, err)

	assert.Empty(t, backends.AWSConfig.Backend.DescribeDeliveryChannels([]string{"default"}))
}

func testConfigConfigurationAggregator(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"Agg": {"Type": "AWS::Config::ConfigurationAggregator", "Properties": {
  "ConfigurationAggregatorName": "agg-1",
  "AccountAggregationSources": [{"AccountIds": ["111111111111"], "AllAwsRegions": true}]
}}},
"Outputs": {"Ref": {"Value": {"Ref": "Agg"}}, "Arn": {"Value": {"Fn::GetAtt": ["Agg", "ConfigurationAggregatorArn"]}}}
}`

	outputs := createStackAndGetOutputs(t, client, "config-aggregator-stack", tmpl)
	assert.Equal(t, "agg-1", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "config-aggregator/")

	_, ok := backends.AWSConfig.Backend.GetConfigurationAggregator("agg-1")
	require.True(t, ok)

	_, err := client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("config-aggregator-stack")},
	)
	require.NoError(t, err)

	_, ok = backends.AWSConfig.Backend.GetConfigurationAggregator("agg-1")
	assert.False(t, ok)
}

func testConfigAggregationAuthorization(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"AA": {"Type": "AWS::Config::AggregationAuthorization", "Properties": {
  "AuthorizedAccountId": "222222222222", "AuthorizedAwsRegion": "us-west-2"
}}},
"Outputs": {"Ref": {"Value": {"Ref": "AA"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "config-aggauth-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "aggregation-authorization/222222222222/us-west-2")

	found := false

	for _, a := range backends.AWSConfig.Backend.DescribeAggregationAuthorizations() {
		if a.AuthorizedAccountID == "222222222222" && a.AuthorizedAwsRegion == "us-west-2" {
			found = true
		}
	}

	require.True(t, found)

	_, err := client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("config-aggauth-stack")},
	)
	require.NoError(t, err)

	for _, a := range backends.AWSConfig.Backend.DescribeAggregationAuthorizations() {
		assert.False(t, a.AuthorizedAccountID == "222222222222" && a.AuthorizedAwsRegion == "us-west-2")
	}
}

func testConfigConformancePack(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"CP": {"Type": "AWS::Config::ConformancePack", "Properties": {
  "ConformancePackName": "cp-1", "TemplateBody": "{}"
}}},
"Outputs": {"Ref": {"Value": {"Ref": "CP"}}, "Arn": {"Value": {"Fn::GetAtt": ["CP", "ConformancePackArn"]}}}
}`

	outputs := createStackAndGetOutputs(t, client, "config-conformancepack-stack", tmpl)
	assert.Equal(t, "cp-1", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "conformance-pack/cp-1")

	packs := backends.AWSConfig.Backend.DescribeConformancePacks()
	found := false

	for _, p := range packs {
		if p.ConformancePackName == "cp-1" {
			found = true
		}
	}

	require.True(t, found)

	_, err := client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("config-conformancepack-stack")},
	)
	require.NoError(t, err)

	for _, p := range backends.AWSConfig.Backend.DescribeConformancePacks() {
		assert.NotEqual(t, "cp-1", p.ConformancePackName)
	}
}

func testConfigRemediationConfiguration(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "CR": {"Type": "AWS::Config::ConfigRule", "Properties": {
    "ConfigRuleName": "cr-remediate", "Source": {"Owner": "AWS", "SourceIdentifier": "REQUIRED_TAGS"}
  }},
  "RC": {"Type": "AWS::Config::RemediationConfiguration", "Properties": {
    "ConfigRuleName": {"Ref": "CR"},
    "TargetId": "AWS-StartEC2Instance",
    "TargetType": "SSM_DOCUMENT",
    "TargetVersion": "1",
    "Parameters": {"AutomationAssumeRole": {"StaticValue": {"Values": ["arn:aws:iam::000000000000:role/automation"]}}}
  }}
},
"Outputs": {"Ref": {"Value": {"Ref": "RC"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "config-remediation-stack", tmpl)
	assert.Equal(t, "cr-remediate", outputs["Ref"])

	configs := backends.AWSConfig.Backend.DescribeRemediationConfigurations([]string{"cr-remediate"})
	require.Len(t, configs, 1)
	assert.Equal(t, "AWS-StartEC2Instance", configs[0].TargetID)

	_, err := client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("config-remediation-stack")},
	)
	require.NoError(t, err)

	assert.Empty(t, backends.AWSConfig.Backend.DescribeRemediationConfigurations([]string{"cr-remediate"}))
}

func testConfigStoredQuery(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"SQ": {"Type": "AWS::Config::StoredQuery", "Properties": {
  "QueryName": "sq-1", "QueryExpression": "SELECT resourceId WHERE resourceType = 'AWS::S3::Bucket'"
}}},
"Outputs": {"Ref": {"Value": {"Ref": "SQ"}}, "Arn": {"Value": {"Fn::GetAtt": ["SQ", "QueryArn"]}}}
}`

	outputs := createStackAndGetOutputs(t, client, "config-storedquery-stack", tmpl)
	require.NotEmpty(t, outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "stored-query/sq-1")

	q := backends.AWSConfig.Backend.GetStoredQuery("sq-1")
	require.NotNil(t, q)
	assert.Equal(t, outputs["Ref"], q.QueryID)

	_, err := client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("config-storedquery-stack")},
	)
	require.NoError(t, err)

	assert.Nil(t, backends.AWSConfig.Backend.GetStoredQuery("sq-1"))
}
