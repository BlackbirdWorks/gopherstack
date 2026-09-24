package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	codeartifactbackend "github.com/blackbirdworks/gopherstack/services/codeartifact"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

// newNewerTypesTestClient wires a real aws-sdk-go-v2 CloudFormation client
// against a backend with every service backend used by the resource types
// added in resources_lambda_csc.go/resources_events_endpoint.go/
// resources_scheduler_group.go/resources_appsync_more.go/
// resources_route53resolver_more.go/resources_cloudtrail_more.go/
// resources_logs_more.go/resources_codeartifact.go wired to a real in-memory
// service backend.
func newNewerTypesTestClient(t *testing.T) (*cloudformation.ServiceBackends, *cfnsdk.Client) {
	t.Helper()

	backends := newMoreTypesServiceBackends(t)
	backends.Lambda = lambdabackend.NewHandler(lambdabackend.NewInMemoryBackend(
		nil, nil, lambdabackend.DefaultSettings(), "000000000000", "us-east-1",
	))
	backends.CodeArtifact = codeartifactbackend.NewHandler(
		codeartifactbackend.NewInMemoryBackend("000000000000", "us-east-1"),
	)
	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	client := newTestClientForBackend(t, backend)

	return backends, client
}

func TestCreateStack_NewerTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testLambdaCodeSigningConfig, "lambda_code_signing_config"},
		{testEventsEndpoint, "events_endpoint"},
		{testSchedulerScheduleGroup, "scheduler_schedule_group"},
		{testAppSyncDomainName, "appsync_domain_name"},
		{testAppSyncGraphQLSchema, "appsync_graphql_schema"},
		{testAppSyncChannelNamespace, "appsync_channel_namespace"},
		{testR53RResolverRuleAssociation, "route53resolver_rule_association"},
		{testR53RFirewallDomainList, "route53resolver_firewall_domain_list"},
		{testR53RFirewallRuleGroup, "route53resolver_firewall_rule_group"},
		{testR53RFirewallRuleGroupAssociation, "route53resolver_firewall_rule_group_association"},
		{testR53RResolverQueryLogConfig, "route53resolver_query_log_config"},
		{testR53RResolverQueryLogConfigAssociation, "route53resolver_query_log_config_association"},
		{testR53ROutpostResolver, "route53resolver_outpost_resolver"},
		{testCloudTrailEventDataStore, "cloudtrail_event_data_store"},
		{testCloudTrailChannel, "cloudtrail_channel"},
		{testLogsDelivery, "logs_delivery"},
		{testLogsDeliveryDestination, "logs_delivery_destination"},
		{testLogsDeliverySource, "logs_delivery_source"},
		{testLogsIntegration, "logs_integration"},
		{testLogsAnomalyDetector, "logs_anomaly_detector"},
		{testLogsScheduledQuery, "logs_scheduled_query"},
		{testCodeArtifactDomain, "codeartifact_domain"},
		{testCodeArtifactRepository, "codeartifact_repository"},
		{testCodeArtifactPackageGroup, "codeartifact_package_group"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testLambdaCodeSigningConfig(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {"CSC": {"Type": "AWS::Lambda::CodeSigningConfig", "Properties": {
  "Description": "test csc",
  "AllowedPublishers": {"SigningProfileVersionArns": ["arn:aws:signer:us-east-1:000000000000:/signing-profiles/prof1"]},
  "CodeSigningPolicies": {"UntrustedArtifactOnDeployment": "Enforce"}
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "CSC"}},
  "Id": {"Value": {"Fn::GetAtt": ["CSC", "CodeSigningConfigId"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "csc-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "code-signing-config")
	assert.NotEmpty(t, outputs["Id"])

	imb, ok := backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	require.True(t, ok)

	_, err := imb.GetCodeSigningConfig(outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("csc-stack")})
	require.NoError(t, err)

	_, err = imb.GetCodeSigningConfig(outputs["Ref"])
	require.Error(t, err)
}

func testEventsEndpoint(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {"EP": {"Type": "AWS::Events::Endpoint", "Properties": {
  "Name": "ep-1",
  "EventBuses": [{"EventBusArn": "arn:aws:events:us-east-1:000000000000:event-bus/default"}],
  "RoutingConfig": {"FailoverConfig": {
    "Primary": {"HealthCheck": "arn:aws:route53:::healthcheck/0123456789abc"},
    "Secondary": {"Route": "us-west-2"}
  }}
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "EP"}},
  "Arn": {"Value": {"Fn::GetAtt": ["EP", "Arn"]}},
  "Url": {"Value": {"Fn::GetAtt": ["EP", "EndpointUrl"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ep-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "ep-1")
	assert.Contains(t, outputs["Arn"], "endpoint/ep-1")
	assert.Contains(t, outputs["Url"], "ep-1.endpoint.events")

	_, err := backends.EventBridge.Backend.DescribeEndpoint(t.Context(), "ep-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ep-stack")})
	require.NoError(t, err)

	_, err = backends.EventBridge.Backend.DescribeEndpoint(t.Context(), "ep-1")
	require.Error(t, err)
}

func testSchedulerScheduleGroup(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {"SG": {"Type": "AWS::Scheduler::ScheduleGroup", "Properties": {"Name": "sg-1"}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "SG"}},
  "Arn": {"Value": {"Fn::GetAtt": ["SG", "Arn"]}},
  "State": {"Value": {"Fn::GetAtt": ["SG", "State"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "sg-stack", tmpl)
	assert.Equal(t, "sg-1", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "schedule-group/sg-1")
	assert.Equal(t, "ACTIVE", outputs["State"])

	_, err := backends.Scheduler.Backend.GetScheduleGroup(t.Context(), "sg-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sg-stack")})
	require.NoError(t, err)

	_, err = backends.Scheduler.Backend.GetScheduleGroup(t.Context(), "sg-1")
	require.Error(t, err)
}

func testAppSyncDomainName(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {"DN": {"Type": "AWS::AppSync::DomainName", "Properties": {
  "DomainName": "api.example.com",
  "CertificateArn": "arn:aws:acm:us-east-1:000000000000:certificate/abc-123"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "DN"}},
  "Arn": {"Value": {"Fn::GetAtt": ["DN", "DomainNameArn"]}},
  "HostedZoneId": {"Value": {"Fn::GetAtt": ["DN", "HostedZoneId"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "dn-stack", tmpl)
	assert.Equal(t, "api.example.com", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "domainnames/api.example.com")
	assert.NotEmpty(t, outputs["HostedZoneId"])

	_, err := backends.AppSync.Backend.GetDomainName("api.example.com")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("dn-stack")})
	require.NoError(t, err)

	_, err = backends.AppSync.Backend.GetDomainName("api.example.com")
	require.Error(t, err)
}

func testAppSyncGraphQLSchema(t *testing.T) {
	t.Helper()

	_, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Api": {"Type": "AWS::AppSync::GraphQLApi", "Properties": {"Name": "schema-api", "AuthenticationType": "API_KEY"}},
  "Schema": {"Type": "AWS::AppSync::GraphQLSchema", "Properties": {
    "ApiId": {"Ref": "Api"},
    "Definition": "type Query { hello: String }"
  }}
},
"Outputs": {
  "ApiId": {"Value": {"Ref": "Api"}},
  "SchemaRef": {"Value": {"Ref": "Schema"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "schema-stack", tmpl)
	assert.Equal(t, outputs["ApiId"]+"GraphQLSchema", outputs["SchemaRef"])

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("schema-stack")})
	require.NoError(t, err)
}

func testAppSyncChannelNamespace(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	imb, ok := backends.AppSync.Backend.(*appsyncbackend.InMemoryBackend)
	require.True(t, ok)

	eventAPI, err := imb.CreateAPI("evt-api", "", nil, nil)
	require.NoError(t, err)

	tmpl := `{
"Parameters": {"ApiId": {"Type": "String"}},
"Resources": {"NS": {"Type": "AWS::AppSync::ChannelNamespace", "Properties": {
  "ApiId": {"Ref": "ApiId"},
  "Name": "ns-1"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "NS"}}
}
}`

	_, err = client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName:    aws.String("ns-stack"),
		TemplateBody: aws.String(tmpl),
		Parameters: []cfntypes.Parameter{
			{ParameterKey: aws.String("ApiId"), ParameterValue: aws.String(eventAPI.APIID)},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String("ns-stack")})
	require.NoError(t, err)
	require.Len(t, desc.Stacks, 1)
	assert.Equal(t, "CREATE_COMPLETE", string(desc.Stacks[0].StackStatus))

	ref := aws.ToString(desc.Stacks[0].Outputs[0].OutputValue)
	assert.Contains(t, ref, "channelNamespaces/ns-1")

	_, err = imb.GetChannelNamespace(eventAPI.APIID, "ns-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ns-stack")})
	require.NoError(t, err)

	_, err = imb.GetChannelNamespace(eventAPI.APIID, "ns-1")
	require.Error(t, err)
}

func testR53RResolverRuleAssociation(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Rule": {"Type": "AWS::Route53Resolver::ResolverRule", "Properties": {
    "DomainName": "example.com.", "RuleType": "RECURSIVE"
  }},
  "Assoc": {"Type": "AWS::Route53Resolver::ResolverRuleAssociation", "Properties": {
    "Name": "assoc-1",
    "ResolverRuleId": {"Ref": "Rule"},
    "VPCId": "vpc-assoc1"
  }}
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Assoc"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "rra-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "rslvr-rrassoc-")

	assocs := backends.Route53Resolver.Backend.ListResolverRuleAssociations(t.Context())
	require.Len(t, assocs, 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("rra-stack")})
	require.NoError(t, err)

	assocs = backends.Route53Resolver.Backend.ListResolverRuleAssociations(t.Context())
	assert.Empty(t, assocs)
}

func testR53RFirewallDomainList(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {"DL": {"Type": "AWS::Route53Resolver::FirewallDomainList", "Properties": {
  "Name": "dl-1",
  "Domains": ["example.com", "test.example.com"]
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "DL"}},
  "Arn": {"Value": {"Fn::GetAtt": ["DL", "Arn"]}},
  "DomainCount": {"Value": {"Fn::GetAtt": ["DL", "DomainCount"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "dl-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "rslvr-fdl-")
	assert.Contains(t, outputs["Arn"], "firewall-domain-list/")
	assert.Equal(t, "2", outputs["DomainCount"])

	_, err := backends.Route53Resolver.Backend.GetFirewallDomainList(t.Context(), outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("dl-stack")})
	require.NoError(t, err)

	_, err = backends.Route53Resolver.Backend.GetFirewallDomainList(t.Context(), outputs["Ref"])
	require.Error(t, err)
}

func testR53RFirewallRuleGroup(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {"RG": {"Type": "AWS::Route53Resolver::FirewallRuleGroup", "Properties": {"Name": "rg-1"}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "RG"}},
  "Arn": {"Value": {"Fn::GetAtt": ["RG", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "rg-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "rslvr-frg-")
	assert.Contains(t, outputs["Arn"], "firewall-rule-group/")

	_, err := backends.Route53Resolver.Backend.GetFirewallRuleGroup(t.Context(), outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("rg-stack")})
	require.NoError(t, err)

	_, err = backends.Route53Resolver.Backend.GetFirewallRuleGroup(t.Context(), outputs["Ref"])
	require.Error(t, err)
}

func testR53RFirewallRuleGroupAssociation(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {
  "RG": {"Type": "AWS::Route53Resolver::FirewallRuleGroup", "Properties": {"Name": "rg-assoc-1"}},
  "Assoc": {"Type": "AWS::Route53Resolver::FirewallRuleGroupAssociation", "Properties": {
    "Name": "frga-1",
    "FirewallRuleGroupId": {"Ref": "RG"},
    "VpcId": "vpc-frga1",
    "Priority": 101
  }}
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Assoc"}},
  "Arn": {"Value": {"Fn::GetAtt": ["Assoc", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "frga-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "rslvr-frgassoc-")
	assert.Contains(t, outputs["Arn"], "firewall-rule-group-association/")

	assocs := backends.Route53Resolver.Backend.ListFirewallRuleGroupAssociations(t.Context(), "", "")
	require.Len(t, assocs, 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("frga-stack")})
	require.NoError(t, err)

	assocs = backends.Route53Resolver.Backend.ListFirewallRuleGroupAssociations(t.Context(), "", "")
	assert.Empty(t, assocs)
}

func testR53RResolverQueryLogConfig(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {"QLC": {"Type": "AWS::Route53Resolver::ResolverQueryLoggingConfig", "Properties": {
  "Name": "qlc-1",
  "DestinationArn": "arn:aws:s3:::my-query-log-bucket"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "QLC"}},
  "Arn": {"Value": {"Fn::GetAtt": ["QLC", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "qlc-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "rqlc-")
	assert.Contains(t, outputs["Arn"], "resolver-query-log-config/")

	_, err := backends.Route53Resolver.Backend.GetResolverQueryLogConfig(t.Context(), outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("qlc-stack")})
	require.NoError(t, err)

	_, err = backends.Route53Resolver.Backend.GetResolverQueryLogConfig(t.Context(), outputs["Ref"])
	require.Error(t, err)
}

func testR53RResolverQueryLogConfigAssociation(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {
  "QLC": {"Type": "AWS::Route53Resolver::ResolverQueryLoggingConfig", "Properties": {
    "Name": "qlc-assoc-1", "DestinationArn": "arn:aws:s3:::my-query-log-bucket-2"
  }},
  "Assoc": {"Type": "AWS::Route53Resolver::ResolverQueryLoggingConfigAssociation", "Properties": {
    "ResolverQueryLogConfigId": {"Ref": "QLC"},
    "ResourceId": "vpc-qlog1"
  }}
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Assoc"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "qlca-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "rqlca-")

	assocs := backends.Route53Resolver.Backend.ListResolverQueryLogConfigAssociations(t.Context())
	require.Len(t, assocs, 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("qlca-stack")})
	require.NoError(t, err)

	assocs = backends.Route53Resolver.Backend.ListResolverQueryLogConfigAssociations(t.Context())
	assert.Empty(t, assocs)
}

func testR53ROutpostResolver(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {"OR": {"Type": "AWS::Route53Resolver::OutpostResolver", "Properties": {
  "Name": "or-1",
  "OutpostArn": "arn:aws:outposts:us-east-1:000000000000:outpost/op-12345678901234567",
  "PreferredInstanceType": "m5.large",
  "InstanceCount": 4
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "OR"}},
  "Arn": {"Value": {"Fn::GetAtt": ["OR", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "or-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "rslvr-op-")
	assert.Contains(t, outputs["Arn"], "outpost-resolver/")

	_, err := backends.Route53Resolver.Backend.GetOutpostResolver(t.Context(), outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("or-stack")})
	require.NoError(t, err)

	_, err = backends.Route53Resolver.Backend.GetOutpostResolver(t.Context(), outputs["Ref"])
	require.Error(t, err)
}

func testCloudTrailEventDataStore(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {"EDS": {"Type": "AWS::CloudTrail::EventDataStore", "Properties": {
  "Name": "eds-1",
  "MultiRegionEnabled": true,
  "RetentionPeriod": 90
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "EDS"}},
  "Status": {"Value": {"Fn::GetAtt": ["EDS", "Status"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "eds-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "eventdatastore/")
	assert.Equal(t, "ENABLED", outputs["Status"])

	_, err := backends.CloudTrail.Backend.GetEventDataStore(outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("eds-stack")})
	require.NoError(t, err)

	_, err = backends.CloudTrail.Backend.GetEventDataStore(outputs["Ref"])
	require.Error(t, err)
}

func testCloudTrailChannel(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {"CH": {"Type": "AWS::CloudTrail::Channel", "Properties": {
  "Name": "ch-1",
  "Source": "aws.partner/testpartner.example.com",
  "Destinations": [{
    "Type": "EVENT_DATA_STORE",
    "Location": "arn:aws:cloudtrail:us-east-1:000000000000:eventdatastore/EXAMPLE"
  }]
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "CH"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ch-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "channel/")

	_, err := backends.CloudTrail.Backend.GetChannel(outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ch-stack")})
	require.NoError(t, err)

	_, err = backends.CloudTrail.Backend.GetChannel(outputs["Ref"])
	require.Error(t, err)
}

func testLogsDelivery(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Src": {"Type": "AWS::Logs::DeliverySource", "Properties": {
    "Name": "src-1",
    "LogType": "APPLICATION_LOGS",
    "ResourceArn": "arn:aws:lambda:us-east-1:000000000000:function:my-func"
  }},
  "Dest": {"Type": "AWS::Logs::DeliveryDestination", "Properties": {
    "Name": "dest-1", "DestinationResourceArn": "arn:aws:s3:::my-delivery-bucket", "DeliveryDestinationType": "S3"
  }},
  "Del": {"Type": "AWS::Logs::Delivery", "DependsOn": "Dest", "Properties": {
    "DeliverySourceName": {"Ref": "Src"},
    "DeliveryDestinationArn": "arn:aws:logs:us-east-1:000000000000:delivery-destination:dest-1"
  }}
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Del"}},
  "Type": {"Value": {"Fn::GetAtt": ["Del", "DeliveryDestinationType"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "del-stack", tmpl)
	assert.NotEmpty(t, outputs["Ref"])
	assert.Equal(t, "S3", outputs["Type"])

	_, err := backends.CloudWatchLogs.Backend.GetDelivery(outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("del-stack")})
	require.NoError(t, err)

	_, err = backends.CloudWatchLogs.Backend.GetDelivery(outputs["Ref"])
	require.Error(t, err)
}

func testLogsDeliveryDestination(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {"D": {"Type": "AWS::Logs::DeliveryDestination", "Properties": {
  "Name": "dd-1",
  "DestinationResourceArn": "arn:aws:s3:::dd-bucket",
  "DeliveryDestinationType": "S3"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "D"}},
  "Arn": {"Value": {"Fn::GetAtt": ["D", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "dd-stack", tmpl)
	assert.Equal(t, "dd-1", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "delivery-destination:dd-1")

	imb, ok := backends.CloudWatchLogs.Backend.(*cwlogsbackend.InMemoryBackend)
	require.True(t, ok)

	_, err := imb.GetDeliveryDestination("dd-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("dd-stack")})
	require.NoError(t, err)

	_, err = imb.GetDeliveryDestination("dd-1")
	require.Error(t, err)
}

func testLogsDeliverySource(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {"S": {"Type": "AWS::Logs::DeliverySource", "Properties": {
  "Name": "ds-1",
  "LogType": "APPLICATION_LOGS",
  "ResourceArn": "arn:aws:lambda:us-east-1:000000000000:function:my-func2"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "S"}},
  "Arn": {"Value": {"Fn::GetAtt": ["S", "Arn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ds-stack", tmpl)
	assert.Equal(t, "ds-1", outputs["Ref"])
	assert.Contains(t, outputs["Arn"], "delivery-source:ds-1")

	imb, ok := backends.CloudWatchLogs.Backend.(*cwlogsbackend.InMemoryBackend)
	require.True(t, ok)

	_, err := imb.GetDeliverySource("ds-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ds-stack")})
	require.NoError(t, err)

	_, err = imb.GetDeliverySource("ds-1")
	require.Error(t, err)
}

func testLogsIntegration(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {"I": {"Type": "AWS::Logs::Integration", "Properties": {
  "IntegrationName": "int-1",
  "IntegrationType": "OPENSEARCH",
  "ResourceConfig": {"OpenSearchResourceConfig": {
    "DataSourceRoleArn": "arn:aws:iam::000000000000:role/osrole",
    "DashboardViewerPrincipals": ["arn:aws:iam::000000000000:role/viewer"],
    "RetentionDays": 30
  }}
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "I"}},
  "Status": {"Value": {"Fn::GetAtt": ["I", "IntegrationStatus"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "int-stack", tmpl)
	assert.Equal(t, "int-1", outputs["Ref"])
	assert.NotEmpty(t, outputs["Status"])

	imb, ok := backends.CloudWatchLogs.Backend.(*cwlogsbackend.InMemoryBackend)
	require.True(t, ok)

	_, err := imb.GetIntegration("int-1")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("int-stack")})
	require.NoError(t, err)

	_, err = imb.GetIntegration("int-1")
	require.Error(t, err)
}

func testLogsAnomalyDetector(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {"AD": {"Type": "AWS::Logs::LogAnomalyDetector", "Properties": {
  "DetectorName": "ad-1",
  "LogGroupArnList": ["arn:aws:logs:us-east-1:000000000000:log-group:/my/group:*"],
  "EvaluationFrequency": "TEN_MIN"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "AD"}},
  "Status": {"Value": {"Fn::GetAtt": ["AD", "AnomalyDetectorStatus"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "ad-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "log-anomaly-detector:")
	assert.Equal(t, "INITIALIZING", outputs["Status"])

	_, err := backends.CloudWatchLogs.Backend.GetLogAnomalyDetector(outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("ad-stack")})
	require.NoError(t, err)

	_, err = backends.CloudWatchLogs.Backend.GetLogAnomalyDetector(outputs["Ref"])
	require.Error(t, err)
}

func testLogsScheduledQuery(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {"SQ": {"Type": "AWS::Logs::ScheduledQuery", "Properties": {
  "Name": "sq-1",
  "QueryString": "fields @timestamp, @message | sort @timestamp desc",
  "QueryLanguage": "CWLI",
  "ScheduleExpression": "rate(1 hour)",
  "ExecutionRoleArn": "arn:aws:iam::000000000000:role/sqrole"
}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "SQ"}},
  "LastStatus": {"Value": {"Fn::GetAtt": ["SQ", "LastExecutionStatus"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "sq-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "scheduled-query:")
	assert.Equal(t, "Complete", outputs["LastStatus"])

	_, err := backends.CloudWatchLogs.Backend.GetScheduledQuery(outputs["Ref"])
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("sq-stack")})
	require.NoError(t, err)

	_, err = backends.CloudWatchLogs.Backend.GetScheduledQuery(outputs["Ref"])
	require.Error(t, err)
}

func testCodeArtifactDomain(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {"Dom": {"Type": "AWS::CodeArtifact::Domain", "Properties": {"DomainName": "my-domain"}}},
"Outputs": {
  "Ref": {"Value": {"Ref": "Dom"}},
  "Name": {"Value": {"Fn::GetAtt": ["Dom", "Name"]}},
  "Owner": {"Value": {"Fn::GetAtt": ["Dom", "Owner"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "domain-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "domain/my-domain")
	assert.Equal(t, "my-domain", outputs["Name"])
	assert.Equal(t, "000000000000", outputs["Owner"])

	_, err := backends.CodeArtifact.Backend.DescribeDomain(t.Context(), "my-domain")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("domain-stack")})
	require.NoError(t, err)

	d, err := backends.CodeArtifact.Backend.DescribeDomain(t.Context(), "my-domain")
	require.Error(t, err)
	assert.Nil(t, d)
}

func testCodeArtifactRepository(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Dom": {"Type": "AWS::CodeArtifact::Domain", "Properties": {"DomainName": "repo-domain"}},
  "Repo": {"Type": "AWS::CodeArtifact::Repository", "DependsOn": "Dom", "Properties": {
    "DomainName": "repo-domain",
    "RepositoryName": "my-repo",
    "Description": "test repo"
  }}
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Repo"}},
  "Name": {"Value": {"Fn::GetAtt": ["Repo", "Name"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "repo-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "repository/repo-domain/my-repo")
	assert.Equal(t, "my-repo", outputs["Name"])

	_, err := backends.CodeArtifact.Backend.DescribeRepository(t.Context(), "repo-domain", "my-repo")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("repo-stack")})
	require.NoError(t, err)

	_, err = backends.CodeArtifact.Backend.DescribeRepository(t.Context(), "repo-domain", "my-repo")
	require.Error(t, err)
}

func testCodeArtifactPackageGroup(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Dom": {"Type": "AWS::CodeArtifact::Domain", "Properties": {"DomainName": "pg-domain"}},
  "PG": {"Type": "AWS::CodeArtifact::PackageGroup", "DependsOn": "Dom", "Properties": {
    "DomainName": "pg-domain",
    "Pattern": "/npm/*"
  }}
},
"Outputs": {
  "Ref": {"Value": {"Ref": "PG"}},
  "Pattern": {"Value": {"Fn::GetAtt": ["PG", "Pattern"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "pg-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "package-group")
	assert.Equal(t, "/npm/*", outputs["Pattern"])

	_, err := backends.CodeArtifact.Backend.DescribePackageGroup(t.Context(), "pg-domain", "/npm/*")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("pg-stack")})
	require.NoError(t, err)

	_, err = backends.CodeArtifact.Backend.DescribePackageGroup(t.Context(), "pg-domain", "/npm/*")
	require.Error(t, err)
}
