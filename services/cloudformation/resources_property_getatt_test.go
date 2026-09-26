package cloudformation_test

import (
	"testing"

	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// TestCreateStack_PropertyTimeGetAtt is a regression suite for the
// property-time Fn::GetAtt bug: strProp/resolve (used to resolve a
// resource's own Properties at create time) called ResolveValue with a bare
// {params, physicalIDs} context and no resourceTypes map, so Fn::GetAtt used
// as an input property value for a sibling resource silently fell back to
// that resource's physical ID for any attribute that wasn't the physical ID
// itself, instead of the real stashed/derived value. See PARITY.md's
// 2026-09-24 property-time GetAtt fix note.
func TestCreateStack_PropertyTimeGetAtt(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testPropertyGetAttCodeArtifactDomainName, "codeartifact_repository_domain_name"},
		{testPropertyGetAttLogsDeliveryDestinationArn, "logs_delivery_destination_arn"},
		{testPropertyGetAttSQSQueueArnToSNSSubscription, "sqs_queue_arn_to_sns_subscription_endpoint"},
		{testPropertyGetAttFnSubResourceAttr, "fn_sub_resource_attr_in_property"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// testPropertyGetAttCodeArtifactDomainName is the reported repro:
// Repository.DomainName via Fn::GetAtt to Domain's Name attribute. Domain's
// Ref is its ARN (not its Name), so if DomainName fell back to the physical
// ID the repository would be created under the wrong (ARN-valued) domain
// and DescribeRepository below would 404.
func testPropertyGetAttCodeArtifactDomainName(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Dom": {"Type": "AWS::CodeArtifact::Domain", "Properties": {"DomainName": "gat-domain"}},
  "Repo": {"Type": "AWS::CodeArtifact::Repository", "Properties": {
    "DomainName": {"Fn::GetAtt": ["Dom", "Name"]},
    "RepositoryName": "gat-repo"
  }}
},
"Outputs": {
  "DomainRef": {"Value": {"Ref": "Dom"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "gat-domain-stack", tmpl)
	assert.NotEqual(t, "gat-domain", outputs["DomainRef"], "Dom's Ref is its ARN, not its Name")

	repo, err := backends.CodeArtifact.Backend.DescribeRepository(t.Context(), "gat-domain", "gat-repo")
	require.NoError(t, err)
	assert.Equal(t, "gat-domain", repo.DomainName)
}

// testPropertyGetAttLogsDeliveryDestinationArn is the reported repro:
// Delivery.DeliveryDestinationArn via Fn::GetAtt to DeliveryDestination's
// Arn attribute. Dest's Ref is its Name, not its Arn, so a fallback to the
// physical ID would stash the plain name instead of the real ARN.
func testPropertyGetAttLogsDeliveryDestinationArn(t *testing.T) {
	t.Helper()

	backends, client := newNewerTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Dest": {"Type": "AWS::Logs::DeliveryDestination", "Properties": {
    "Name": "gat-dest", "DestinationResourceArn": "arn:aws:s3:::gat-bucket", "DeliveryDestinationType": "S3"
  }},
  "Src": {"Type": "AWS::Logs::DeliverySource", "Properties": {
    "Name": "gat-src", "LogType": "APPLICATION_LOGS",
    "ResourceArn": "arn:aws:lambda:us-east-1:000000000000:function:gat-func"
  }},
  "Del": {"Type": "AWS::Logs::Delivery", "Properties": {
    "DeliverySourceName": {"Ref": "Src"},
    "DeliveryDestinationArn": {"Fn::GetAtt": ["Dest", "Arn"]}
  }}
},
"Outputs": {
  "DelRef": {"Value": {"Ref": "Del"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "gat-delivery-stack", tmpl)

	d, err := backends.CloudWatchLogs.Backend.GetDelivery(outputs["DelRef"])
	require.NoError(t, err)
	assert.NotEqual(t, "gat-dest", d.DeliveryDestinationArn, "must not fall back to Dest's Ref (its Name)")
	assert.Contains(t, d.DeliveryDestinationArn, "delivery-destination:gat-dest")
}

// testPropertyGetAttSQSQueueArnToSNSSubscription covers a derived attribute
// (not a stashed side-channel value): SQS Queue Arn is computed from the
// queue's physical ID (its QueueUrl) via arn.Build, and differs from that
// physical ID. A subscription's "sqs" protocol validates the endpoint looks
// like an SQS ARN, so a fallback to the queue's URL-shaped physical ID
// fails CreateStack outright rather than silently resolving wrong.
func testPropertyGetAttSQSQueueArnToSNSSubscription(t *testing.T) {
	t.Helper()

	backends, client := newPropertyGetAttSNSSQSTestClient(t)

	tmpl := `{
"Resources": {
  "Topic": {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": "gat-topic"}},
  "Queue": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "gat-queue"}},
  "Sub": {"Type": "AWS::SNS::Subscription", "Properties": {
    "TopicArn": {"Ref": "Topic"},
    "Protocol": "sqs",
    "Endpoint": {"Fn::GetAtt": ["Queue", "Arn"]}
  }}
},
"Outputs": {
  "TopicArn": {"Value": {"Ref": "Topic"}},
  "QueueRef": {"Value": {"Ref": "Queue"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "gat-sqs-sns-stack", tmpl)

	wantArn := "arn:aws:sqs:us-east-1:000000000000:gat-queue"
	assert.NotEqual(t, wantArn, outputs["QueueRef"], "Queue's Ref is its URL, not its ARN")

	subs, _, err := backends.SNS.Backend.ListSubscriptionsByTopic(outputs["TopicArn"], "")
	require.NoError(t, err)
	require.Len(t, subs, 1)
	assert.Equal(t, wantArn, subs[0].Endpoint)
}

// testPropertyGetAttFnSubResourceAttr covers Fn::Sub "${Res.Attr}" inside a
// property value, which resolves through the same resolveGetAtt path.
func testPropertyGetAttFnSubResourceAttr(t *testing.T) {
	t.Helper()

	backends, client := newPropertyGetAttSNSSQSTestClient(t)

	tmpl := `{
"Resources": {
  "Topic": {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": "gat-sub-topic"}},
  "Queue": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "gat-sub-queue"}},
  "Sub": {"Type": "AWS::SNS::Subscription", "Properties": {
    "TopicArn": {"Ref": "Topic"},
    "Protocol": "sqs",
    "Endpoint": {"Fn::Sub": "${Queue.Arn}"}
  }}
},
"Outputs": {
  "TopicArn": {"Value": {"Ref": "Topic"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "gat-sub-stack", tmpl)

	subs, _, err := backends.SNS.Backend.ListSubscriptionsByTopic(outputs["TopicArn"], "")
	require.NoError(t, err)
	require.Len(t, subs, 1)
	assert.Equal(t, "arn:aws:sqs:us-east-1:000000000000:gat-sub-queue", subs[0].Endpoint)
}

// newPropertyGetAttSNSSQSTestClient wires a real aws-sdk-go-v2 CloudFormation
// client against a backend with SQS and SNS (among the base set) wired to
// real in-memory service backends.
func newPropertyGetAttSNSSQSTestClient(t *testing.T) (*cloudformation.ServiceBackends, *cfnsdk.Client) {
	t.Helper()

	backends := newServiceBackends()
	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig(backends.AccountID, backends.Region, creator)
	client := newTestClientForBackend(t, backend)

	return backends, client
}
