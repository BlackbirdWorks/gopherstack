package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

func TestCreateStack_EC2TransitGatewayFamily(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "VPC": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}},
  "Subnet": {"Type": "AWS::EC2::Subnet", "Properties": {"VpcId": {"Ref": "VPC"}, "CidrBlock": "10.0.1.0/24"}},
  "TGW": {
    "Type": "AWS::EC2::TransitGateway",
    "Properties": {"Description": "test tgw", "AmazonSideAsn": 65000, "Tags": [{"Key": "Name", "Value": "tgw-tag"}]}
  },
  "Attachment": {
    "Type": "AWS::EC2::TransitGatewayAttachment",
    "Properties": {"TransitGatewayId": {"Ref": "TGW"}, "VpcId": {"Ref": "VPC"}, "SubnetIds": [{"Ref": "Subnet"}]}
  },
  "RouteTable": {
    "Type": "AWS::EC2::TransitGatewayRouteTable",
    "Properties": {"TransitGatewayId": {"Ref": "TGW"}}
  },
  "Route": {
    "Type": "AWS::EC2::TransitGatewayRoute",
    "Properties": {
      "TransitGatewayRouteTableId": {"Ref": "RouteTable"},
      "TransitGatewayAttachmentId": {"Ref": "Attachment"},
      "DestinationCidrBlock": "192.168.0.0/16"
    }
  }
},
"Outputs": {
  "TgwId": {"Value": {"Ref": "TGW"}},
  "AttachmentId": {"Value": {"Ref": "Attachment"}},
  "RouteTableId": {"Value": {"Ref": "RouteTable"}}
}
}`

	stackName := "tgw-stack"
	outputs := createStackAndGetOutputs(t, client, stackName, tmpl)

	tgwID := outputs["TgwId"]
	require.NotEmpty(t, tgwID)

	tgws := backends.EC2.Backend.DescribeTransitGateways([]string{tgwID})
	require.Len(t, tgws, 1)
	assert.Equal(t, "test tgw", tgws[0].Description)
	assert.Equal(t, int64(65000), tgws[0].Options.AmazonSideAsn)
	assert.Equal(t, "tgw-tag", backends.EC2.Backend.TagsForResource(tgwID)["Name"])

	attachmentID := outputs["AttachmentId"]
	require.NotEmpty(t, attachmentID)
	atts := backends.EC2.Backend.DescribeTransitGatewayVpcAttachments([]string{attachmentID})
	require.Len(t, atts, 1)
	assert.Equal(t, tgwID, atts[0].TransitGatewayID)

	routeTableID := outputs["RouteTableId"]
	require.NotEmpty(t, routeTableID)
	rts := backends.EC2.Backend.DescribeTransitGatewayRouteTables([]string{routeTableID})
	require.Len(t, rts, 1)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String(stackName)})
	require.NoError(t, err)

	// DescribeTransitGatewayVpcAttachments/RouteTables keep a tombstone so a
	// by-ID Describe still resolves post-delete (real AWS behavior); the
	// deletion itself is proven by the State transition.
	afterAtt := backends.EC2.Backend.DescribeTransitGatewayVpcAttachments([]string{attachmentID})
	require.Len(t, afterAtt, 1)
	assert.Equal(t, "deleted", afterAtt[0].State)

	afterRT := backends.EC2.Backend.DescribeTransitGatewayRouteTables([]string{routeTableID})
	require.Len(t, afterRT, 1)
	assert.Equal(t, "deleted", afterRT[0].State)
}

func TestCreateStack_IAMAccessKey(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "User": {"Type": "AWS::IAM::User", "Properties": {"UserName": "test-user"}},
  "Key": {"Type": "AWS::IAM::AccessKey", "Properties": {"UserName": {"Ref": "User"}}}
},
"Outputs": {
  "KeyId": {"Value": {"Ref": "Key"}},
  "Secret": {"Value": {"Fn::GetAtt": ["Key", "SecretAccessKey"]}}
}
}`

	stackName := "accesskey-stack"
	outputs := createStackAndGetOutputs(t, client, stackName, tmpl, cfntypes.CapabilityCapabilityIam)

	keyID := outputs["KeyId"]
	require.NotEmpty(t, keyID)
	require.NotEmpty(t, outputs["Secret"])

	keysPage, err := backends.IAM.Backend.ListAccessKeys("test-user", "", 10)
	require.NoError(t, err)
	require.Len(t, keysPage.Data, 1)
	assert.Equal(t, keyID, keysPage.Data[0].AccessKeyID)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String(stackName)})
	require.NoError(t, err)

	// User deletion tears down its own access keys too; just confirm the
	// stack deleted cleanly (DeleteStack asserted no error above).
}

func TestCreateStack_IAMServiceLinkedRole(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "SLR": {
    "Type": "AWS::IAM::ServiceLinkedRole",
    "Properties": {"AWSServiceName": "elasticbeanstalk.amazonaws.com", "Description": "test SLR"}
  }
},
"Outputs": {
  "RoleName": {"Value": {"Ref": "SLR"}},
  "RoleNameAttr": {"Value": {"Fn::GetAtt": ["SLR", "RoleName"]}}
}
}`

	stackName := "slr-stack"
	outputs := createStackAndGetOutputs(t, client, stackName, tmpl, cfntypes.CapabilityCapabilityIam)

	roleName := outputs["RoleName"]
	require.Equal(t, "AWSServiceRoleForElasticbeanstalk", roleName)
	assert.Equal(t, roleName, outputs["RoleNameAttr"])

	role, err := backends.IAM.Backend.GetRole(roleName)
	require.NoError(t, err)
	assert.Contains(t, role.AssumeRolePolicyDocument, "elasticbeanstalk.amazonaws.com")

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String(stackName)})
	require.NoError(t, err)

	_, err = backends.IAM.Backend.GetRole(roleName)
	require.Error(t, err)
}

func TestCreateStack_IAMUserToGroupAddition(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Group": {"Type": "AWS::IAM::Group", "Properties": {"GroupName": "test-group"}},
  "UserA": {"Type": "AWS::IAM::User", "Properties": {"UserName": "user-a"}},
  "UserB": {"Type": "AWS::IAM::User", "Properties": {"UserName": "user-b"}},
  "Addition": {
    "Type": "AWS::IAM::UserToGroupAddition",
    "Properties": {"GroupName": {"Ref": "Group"}, "Users": [{"Ref": "UserA"}, {"Ref": "UserB"}]}
  }
},
"Outputs": {"Id": {"Value": {"Ref": "Addition"}}}
}`

	stackName := "u2g-stack"
	outputs := createStackAndGetOutputs(t, client, stackName, tmpl, cfntypes.CapabilityCapabilityIam)
	require.NotEmpty(t, outputs["Id"])

	members, err := backends.IAM.Backend.GetGroupUsers("test-group")
	require.NoError(t, err)
	names := make([]string, 0, len(members))
	for _, m := range members {
		names = append(names, m.UserName)
	}
	assert.ElementsMatch(t, []string{"user-a", "user-b"}, names)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String(stackName)})
	require.NoError(t, err)
}

func TestCreateStack_LogsDestination(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Dest": {
    "Type": "AWS::Logs::Destination",
    "Properties": {
      "DestinationName": "test-destination",
      "RoleArn": "arn:aws:iam::000000000000:role/LogRole",
      "TargetArn": "arn:aws:kinesis:us-east-1:000000000000:stream/TestStream",
      "DestinationPolicy": "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
},
"Outputs": {
  "Name": {"Value": {"Ref": "Dest"}},
  "Arn": {"Value": {"Fn::GetAtt": ["Dest", "Arn"]}}
}
}`

	stackName := "logsdest-stack"
	outputs := createStackAndGetOutputs(t, client, stackName, tmpl)

	assert.Equal(t, "test-destination", outputs["Name"])
	assert.Contains(t, outputs["Arn"], "arn:aws:logs:us-east-1:000000000000:destination:test-destination")

	mem, ok := backends.CloudWatchLogs.Backend.(*cwlogsbackend.InMemoryBackend)
	require.True(t, ok)

	dests, _ := mem.DescribeDestinations("test-destination", 10, "")
	require.Len(t, dests, 1)
	assert.Equal(t, "arn:aws:kinesis:us-east-1:000000000000:stream/TestStream", dests[0].TargetArn)

	_, err := client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String(stackName)})
	require.NoError(t, err)

	dests, _ = mem.DescribeDestinations("test-destination", 10, "")
	assert.Empty(t, dests)
}

func TestCreateStack_Route53RecordSetGroup(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Zone": {"Type": "AWS::Route53::HostedZone", "Properties": {"Name": "example.com."}},
  "Group": {
    "Type": "AWS::Route53::RecordSetGroup",
    "Properties": {
      "HostedZoneId": {"Ref": "Zone"},
      "Comment": "mail records",
      "RecordSets": [
        {"Name": "mail.example.com.", "Type": "A", "TTL": "900", "ResourceRecords": ["192.0.2.44"]},
        {"Name": "mail.example.com.", "Type": "MX", "TTL": "900", "ResourceRecords": ["10 mail.example.com"]}
      ]
    }
  }
},
"Outputs": {"ZoneId": {"Value": {"Ref": "Zone"}}}
}`

	stackName := "rrsg-stack"
	outputs := createStackAndGetOutputs(t, client, stackName, tmpl)
	zoneID := outputs["ZoneId"]
	require.NotEmpty(t, zoneID)

	page, err := backends.Route53.Backend.ListResourceRecordSets(zoneID, "", "", "", 100)
	require.NoError(t, err)

	byType := map[string]bool{}
	for _, rrs := range page.Records {
		byType[rrs.Type] = true
	}
	assert.True(t, byType["A"])
	assert.True(t, byType["MX"])

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String(stackName)})
	require.NoError(t, err)
}

func TestCreateStack_SQSQueueInlinePolicy(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Queue": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "test-queue"}},
  "Policy": {
    "Type": "AWS::SQS::QueueInlinePolicy",
    "Properties": {
      "Queue": {"Ref": "Queue"},
      "PolicyDocument": {"Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "sqs:*", "Resource": "*"}]}
    }
  }
},
"Outputs": {"QueueUrl": {"Value": {"Ref": "Policy"}}}
}`

	stackName := "sqsinline-stack"
	outputs := createStackAndGetOutputs(t, client, stackName, tmpl)
	queueURL := outputs["QueueUrl"]
	require.NotEmpty(t, queueURL)

	attrsOut, err := backends.SQS.Backend.GetQueueAttributes(&sqsbackend.GetQueueAttributesInput{
		QueueURL: queueURL, Region: "us-east-1",
	})
	require.NoError(t, err)
	assert.Contains(t, attrsOut.Attributes["Policy"], "sqs:*")

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String(stackName)})
	require.NoError(t, err)
}

func TestCreateStack_SNSTopicInlinePolicy(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Topic": {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": "test-topic"}},
  "Policy": {
    "Type": "AWS::SNS::TopicInlinePolicy",
    "Properties": {
      "TopicArn": {"Ref": "Topic"},
      "PolicyDocument": {"Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "sns:Publish", "Resource": "*"}]}
    }
  }
},
"Outputs": {"TopicArn": {"Value": {"Ref": "Policy"}}}
}`

	stackName := "snsinline-stack"
	outputs := createStackAndGetOutputs(t, client, stackName, tmpl)
	topicArn := outputs["TopicArn"]
	require.NotEmpty(t, topicArn)

	attrs, err := backends.SNS.Backend.GetTopicAttributes(topicArn)
	require.NoError(t, err)
	assert.Contains(t, attrs["Policy"], "sns:Publish")

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String(stackName)})
	require.NoError(t, err)
}
