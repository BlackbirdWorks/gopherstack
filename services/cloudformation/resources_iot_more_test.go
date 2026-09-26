package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_IoTMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testIoTThingType, "thing_type"},
		{testIoTThingGroup, "thing_group"},
		{testIoTPolicy, "policy"},
		{testIoTTopicRuleDestination, "topic_rule_destination"},
		{testIoTRoleAlias, "role_alias"},
		{testIoTCertificate, "certificate"},
		{testIoTProvisioningTemplate, "provisioning_template"},
		{testIoTAuthorizer, "authorizer"},
		{testIoTDomainConfiguration, "domain_configuration"},
		{testIoTJobTemplate, "job_template"},
		{testIoTDimension, "dimension"},
		{testIoTSecurityProfile, "security_profile"},
		{testIoTCustomMetric, "custom_metric"},
		{testIoTFleetMetric, "fleet_metric"},
		{testIoTBillingGroup, "billing_group"},
		{testIoTMitigationAction, "mitigation_action"},
		{testIoTScheduledAudit, "scheduled_audit"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testIoTThingType(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"TT": {"Type": "AWS::IoT::ThingType", "Properties": {"ThingTypeName": "tt-1"}}},
"Outputs": {"Ref": {"Value": {"Ref": "TT"}}, "Arn": {"Value": {"Fn::GetAtt": ["TT", "Arn"]}}}
}`

	outputs := createStackAndGetOutputs(t, client, "iot-thingtype-stack", tmpl)
	require.NotEmpty(t, outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "thingtype/tt-1")

	tt, err := backends.IoT.Backend.DescribeThingType("tt-1")
	require.NoError(t, err)
	assert.Equal(t, outputs["Ref"], tt.ThingTypeID)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iot-thingtype-stack")},
	)
	require.NoError(t, err)

	_, err = backends.IoT.Backend.DescribeThingType("tt-1")
	require.Error(t, err)
}

func testIoTThingGroup(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"TG": {"Type": "AWS::IoT::ThingGroup", "Properties": {"ThingGroupName": "tg-1"}}},
"Outputs": {"Ref": {"Value": {"Ref": "TG"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "iot-thinggroup-stack", tmpl)
	require.NotEmpty(t, outputs["Ref"])

	tg, err := backends.IoT.Backend.DescribeThingGroup("tg-1")
	require.NoError(t, err)
	assert.Equal(t, outputs["Ref"], tg.ThingGroupID)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iot-thinggroup-stack")},
	)
	require.NoError(t, err)

	_, err = backends.IoT.Backend.DescribeThingGroup("tg-1")
	require.Error(t, err)
}

func testIoTPolicy(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"Pol": {"Type": "AWS::IoT::Policy", "Properties": {
  "PolicyName": "pol-1",
  "PolicyDocument": {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": "iot:Connect", "Resource": "*"}]
  }
}}},
"Outputs": {"Ref": {"Value": {"Ref": "Pol"}}, "Arn": {"Value": {"Fn::GetAtt": ["Pol", "Arn"]}}}
}`

	outputs := createStackAndGetOutputs(t, client, "iot-policy-stack", tmpl)
	assert.Equal(t, "pol-1", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "policy/pol-1")

	_, err := backends.IoT.Backend.GetPolicy("pol-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iot-policy-stack")},
	)
	require.NoError(t, err)

	_, err = backends.IoT.Backend.GetPolicy("pol-1")
	require.Error(t, err)
}

func testIoTTopicRuleDestination(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"TRD": {"Type": "AWS::IoT::TopicRuleDestination", "Properties": {
  "HttpUrlProperties": {"ConfirmationUrl": "https://example.com/confirm"}
}}},
"Outputs": {"Ref": {"Value": {"Ref": "TRD"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "iot-trd-stack", tmpl)
	require.NotEmpty(t, outputs["Ref"])
	assert.Contains(t, outputs["Ref"], "arn:")

	_, err := backends.IoT.Backend.GetTopicRuleDestination(outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iot-trd-stack")},
	)
	require.NoError(t, err)

	_, err = backends.IoT.Backend.GetTopicRuleDestination(outputs["Ref"])
	require.Error(t, err)
}

func testIoTRoleAlias(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"RA": {"Type": "AWS::IoT::RoleAlias", "Properties": {
  "RoleAlias": "ra-1", "RoleArn": "arn:aws:iam::000000000000:role/test-role"
}}},
"Outputs": {"Ref": {"Value": {"Ref": "RA"}}, "Arn": {"Value": {"Fn::GetAtt": ["RA", "RoleAliasArn"]}}}
}`

	outputs := createStackAndGetOutputs(t, client, "iot-rolealias-stack", tmpl)
	assert.Equal(t, "ra-1", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "rolealias/ra-1")

	_, err := backends.IoT.Backend.DescribeRoleAlias("ra-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iot-rolealias-stack")},
	)
	require.NoError(t, err)

	_, err = backends.IoT.Backend.DescribeRoleAlias("ra-1")
	require.Error(t, err)
}

func testIoTCertificate(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"Cert": {"Type": "AWS::IoT::Certificate", "Properties": {
  "CertificateSigningRequest": "-----BEGIN CERTIFICATE REQUEST-----\nfake\n-----END CERTIFICATE REQUEST-----",
  "Status": "ACTIVE"
}}},
"Outputs": {"Ref": {"Value": {"Ref": "Cert"}}, "Arn": {"Value": {"Fn::GetAtt": ["Cert", "Arn"]}}}
}`

	outputs := createStackAndGetOutputs(t, client, "iot-cert-stack", tmpl)
	require.NotEmpty(t, outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "cert/"+outputs["Ref"])

	cert, err := backends.IoT.Backend.DescribeCertificate(outputs["Ref"])
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", cert.Status)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iot-cert-stack")},
	)
	require.NoError(t, err)

	_, err = backends.IoT.Backend.DescribeCertificate(outputs["Ref"])
	require.Error(t, err)
}

func testIoTProvisioningTemplate(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"PT": {"Type": "AWS::IoT::ProvisioningTemplate", "Properties": {
  "TemplateName": "pt-1", "TemplateBody": "{}",
  "ProvisioningRoleArn": "arn:aws:iam::000000000000:role/prov-role"
}}},
"Outputs": {"Ref": {"Value": {"Ref": "PT"}}, "Arn": {"Value": {"Fn::GetAtt": ["PT", "TemplateArn"]}}}
}`

	outputs := createStackAndGetOutputs(t, client, "iot-provtemplate-stack", tmpl)
	assert.Equal(t, "pt-1", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "provisioningtemplate/pt-1")

	_, err := backends.IoT.Backend.DescribeProvisioningTemplate("pt-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iot-provtemplate-stack")},
	)
	require.NoError(t, err)

	_, err = backends.IoT.Backend.DescribeProvisioningTemplate("pt-1")
	require.Error(t, err)
}

func testIoTAuthorizer(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"Auth": {"Type": "AWS::IoT::Authorizer", "Properties": {
  "AuthorizerName": "auth-1",
  "AuthorizerFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:auth-fn"
}}},
"Outputs": {"Ref": {"Value": {"Ref": "Auth"}}, "Arn": {"Value": {"Fn::GetAtt": ["Auth", "Arn"]}}}
}`

	outputs := createStackAndGetOutputs(t, client, "iot-authorizer-stack", tmpl)
	assert.Equal(t, "auth-1", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "authorizer/auth-1")

	_, err := backends.IoT.Backend.DescribeAuthorizer("auth-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iot-authorizer-stack")},
	)
	require.NoError(t, err)

	_, err = backends.IoT.Backend.DescribeAuthorizer("auth-1")
	require.Error(t, err)
}

func testIoTDomainConfiguration(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"DC": {"Type": "AWS::IoT::DomainConfiguration", "Properties": {"DomainConfigurationName": "dc-1"}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "DC"}},
  "Arn": {"Value": {"Fn::GetAtt": ["DC", "Arn"]}},
  "DomainType": {"Value": {"Fn::GetAtt": ["DC", "DomainType"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "iot-domainconfig-stack", tmpl)
	assert.Equal(t, "dc-1", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "domainconfiguration/dc-1")
	assert.Equal(t, "AWS_MANAGED", outputs["DomainType"])

	_, err := backends.IoT.Backend.DescribeDomainConfiguration("dc-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iot-domainconfig-stack")},
	)
	require.NoError(t, err)

	_, err = backends.IoT.Backend.DescribeDomainConfiguration("dc-1")
	require.Error(t, err)
}

func testIoTJobTemplate(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"JT": {"Type": "AWS::IoT::JobTemplate", "Properties": {
  "JobTemplateId": "jt-1", "Description": "test template", "Document": "{}"
}}},
"Outputs": {"Ref": {"Value": {"Ref": "JT"}}, "Arn": {"Value": {"Fn::GetAtt": ["JT", "Arn"]}}}
}`

	outputs := createStackAndGetOutputs(t, client, "iot-jobtemplate-stack", tmpl)
	assert.Equal(t, "jt-1", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "jobtemplate/jt-1")

	_, err := backends.IoT.Backend.DescribeJobTemplate("jt-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iot-jobtemplate-stack")},
	)
	require.NoError(t, err)

	_, err = backends.IoT.Backend.DescribeJobTemplate("jt-1")
	require.Error(t, err)
}

func testIoTDimension(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"Dim": {"Type": "AWS::IoT::Dimension", "Properties": {
  "Name": "dim-1", "Type": "TOPIC_FILTER", "StringValues": ["topic/#"]
}}},
"Outputs": {"Ref": {"Value": {"Ref": "Dim"}}, "Arn": {"Value": {"Fn::GetAtt": ["Dim", "Arn"]}}}
}`

	outputs := createStackAndGetOutputs(t, client, "iot-dimension-stack", tmpl)
	assert.Equal(t, "dim-1", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "dimension/dim-1")

	_, err := backends.IoT.Backend.DescribeDimension("dim-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iot-dimension-stack")},
	)
	require.NoError(t, err)

	_, err = backends.IoT.Backend.DescribeDimension("dim-1")
	require.Error(t, err)
}

func testIoTSecurityProfile(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"SP": {"Type": "AWS::IoT::SecurityProfile", "Properties": {"SecurityProfileName": "sp-1"}}},
"Outputs": {"Ref": {"Value": {"Ref": "SP"}}, "Arn": {"Value": {"Fn::GetAtt": ["SP", "SecurityProfileArn"]}}}
}`

	outputs := createStackAndGetOutputs(t, client, "iot-secprofile-stack", tmpl)
	assert.Equal(t, "sp-1", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "securityprofile/sp-1")

	_, err := backends.IoT.Backend.DescribeSecurityProfile("sp-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iot-secprofile-stack")},
	)
	require.NoError(t, err)

	_, err = backends.IoT.Backend.DescribeSecurityProfile("sp-1")
	require.Error(t, err)
}

func testIoTCustomMetric(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"CM": {"Type": "AWS::IoT::CustomMetric", "Properties": {"MetricName": "cm-1", "MetricType": "number"}}},
"Outputs": {"Ref": {"Value": {"Ref": "CM"}}, "Arn": {"Value": {"Fn::GetAtt": ["CM", "MetricArn"]}}}
}`

	outputs := createStackAndGetOutputs(t, client, "iot-custommetric-stack", tmpl)
	assert.Equal(t, "cm-1", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "custommetric/cm-1")

	_, err := backends.IoT.Backend.DescribeCustomMetric("cm-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iot-custommetric-stack")},
	)
	require.NoError(t, err)

	_, err = backends.IoT.Backend.DescribeCustomMetric("cm-1")
	require.Error(t, err)
}

func testIoTFleetMetric(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"FM": {"Type": "AWS::IoT::FleetMetric", "Properties": {
  "MetricName": "fm-1", "QueryString": "connectivity.connected:true",
  "AggregationField": "connectivity.connected",
  "AggregationType": {"Name": "Statistics", "Values": ["Average"]}
}}},
"Outputs": {"Ref": {"Value": {"Ref": "FM"}}, "Arn": {"Value": {"Fn::GetAtt": ["FM", "MetricArn"]}}}
}`

	outputs := createStackAndGetOutputs(t, client, "iot-fleetmetric-stack", tmpl)
	assert.Equal(t, "fm-1", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "fleetmetric/fm-1")

	_, err := backends.IoT.Backend.DescribeFleetMetric("fm-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iot-fleetmetric-stack")},
	)
	require.NoError(t, err)

	_, err = backends.IoT.Backend.DescribeFleetMetric("fm-1")
	require.Error(t, err)
}

func testIoTBillingGroup(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"BG": {"Type": "AWS::IoT::BillingGroup", "Properties": {"BillingGroupName": "bg-1"}}},
"Outputs": {"Ref": {"Value": {"Ref": "BG"}}, "Arn": {"Value": {"Fn::GetAtt": ["BG", "Arn"]}}}
}`

	outputs := createStackAndGetOutputs(t, client, "iot-billinggroup-stack", tmpl)
	require.NotEmpty(t, outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "billinggroup/bg-1")

	bg, err := backends.IoT.Backend.DescribeBillingGroup("bg-1")
	require.NoError(t, err)
	assert.Equal(t, outputs["Ref"], bg.BillingGroupID)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iot-billinggroup-stack")},
	)
	require.NoError(t, err)

	_, err = backends.IoT.Backend.DescribeBillingGroup("bg-1")
	require.Error(t, err)
}

func testIoTMitigationAction(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"MA": {"Type": "AWS::IoT::MitigationAction", "Properties": {
  "ActionName": "ma-1", "RoleArn": "arn:aws:iam::000000000000:role/mit-role",
  "ActionParams": {"UpdateDeviceCertificateParams": {"Action": "DEACTIVATE"}}
}}},
"Outputs": {"Ref": {"Value": {"Ref": "MA"}}, "Arn": {"Value": {"Fn::GetAtt": ["MA", "MitigationActionArn"]}}}
}`

	outputs := createStackAndGetOutputs(t, client, "iot-mitigationaction-stack", tmpl)
	assert.Equal(t, "ma-1", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "mitigationaction/ma-1")

	_, err := backends.IoT.Backend.DescribeMitigationAction("ma-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iot-mitigationaction-stack")},
	)
	require.NoError(t, err)

	_, err = backends.IoT.Backend.DescribeMitigationAction("ma-1")
	require.Error(t, err)
}

func testIoTScheduledAudit(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {"SA": {"Type": "AWS::IoT::ScheduledAudit", "Properties": {
  "ScheduledAuditName": "sa-1", "Frequency": "DAILY",
  "TargetCheckNames": ["DEVICE_CERTIFICATE_EXPIRING_CHECK"]
}}},
"Outputs": {"Ref": {"Value": {"Ref": "SA"}}, "Arn": {"Value": {"Fn::GetAtt": ["SA", "ScheduledAuditArn"]}}}
}`

	outputs := createStackAndGetOutputs(t, client, "iot-scheduledaudit-stack", tmpl)
	assert.Equal(t, "sa-1", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "scheduledaudit/sa-1")

	_, err := backends.IoT.Backend.DescribeScheduledAudit("sa-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(
		t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("iot-scheduledaudit-stack")},
	)
	require.NoError(t, err)

	_, err = backends.IoT.Backend.DescribeScheduledAudit("sa-1")
	require.Error(t, err)
}
