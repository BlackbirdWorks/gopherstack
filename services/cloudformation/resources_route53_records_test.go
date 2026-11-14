package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
)

func TestCreateStack_Route53AliasAndRoutingRecords(t *testing.T) {
	t.Parallel()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Zone": {"Type": "AWS::Route53::HostedZone", "Properties": {"Name": "example.com."}},
  "Alias": {
    "Type": "AWS::Route53::RecordSet",
    "Properties": {
      "HostedZoneName": "example.com.",
      "Name": "alias.example.com.",
      "Type": "A",
      "AliasTarget": {"DNSName": "d111.cloudfront.net", "HostedZoneId": "Z2FDTNDATAQYW2", "EvaluateTargetHealth": false}
    },
    "DependsOn": ["Zone"]
  },
  "Group": {
    "Type": "AWS::Route53::RecordSetGroup",
    "Properties": {
      "HostedZoneId": {"Ref": "Zone"},
      "RecordSets": [
        {"Name": "w.example.com.", "Type": "A", "SetIdentifier": "one", "Weight": 10, "TTL": "60",
         "ResourceRecords": ["192.0.2.1"]},
        {"Name": "w.example.com.", "Type": "A", "SetIdentifier": "two", "Weight": 20, "TTL": "60",
         "ResourceRecords": ["192.0.2.2"]}
      ]
    }
  }
},
"Outputs": {"ZoneId": {"Value": {"Ref": "Zone"}}}
}`

	stackName := "r53-records"
	zoneID := createStackAndGetOutputs(t, client, stackName, tmpl)["ZoneId"]
	require.NotEmpty(t, zoneID)

	page, err := backends.Route53.Backend.ListResourceRecordSets(zoneID, "", "", "", 100)
	require.NoError(t, err)

	var alias *route53backend.ResourceRecordSet
	weights := map[string]int64{}
	for i, rrs := range page.Records {
		switch rrs.SetIdentifier {
		case "":
			if rrs.Name == "alias.example.com." {
				alias = &page.Records[i]
			}
		default:
			require.NotNil(t, rrs.Weight)
			weights[rrs.SetIdentifier] = *rrs.Weight
		}
	}
	require.NotNil(t, alias, "alias record created from HostedZoneName")
	require.NotNil(t, alias.AliasTarget)
	assert.Equal(t, "d111.cloudfront.net", alias.AliasTarget.DNSName)
	assert.Equal(t, "Z2FDTNDATAQYW2", alias.AliasTarget.HostedZoneID)
	assert.Equal(t, map[string]int64{"one": 10, "two": 20}, weights)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String(stackName)})
	require.NoError(t, err)

	desc, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String(stackName)})
	if err == nil && len(desc.Stacks) == 1 {
		assert.Equal(
			t,
			"DELETE_COMPLETE",
			string(desc.Stacks[0].StackStatus),
			aws.ToString(desc.Stacks[0].StackStatusReason),
		)
	}
	_, err = backends.Route53.Backend.GetHostedZone(zoneID)
	require.Error(t, err, "hosted zone deleted once its records were removed")
}
