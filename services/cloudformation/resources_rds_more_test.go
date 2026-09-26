package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_RDSMoreTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testRDSOptionGroup, "option_group"},
		{testRDSEventSubscription, "event_subscription"},
		{testRDSGlobalCluster, "global_cluster"},
		{testRDSDBProxyEndpoint, "db_proxy_endpoint"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testRDSOptionGroup(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "OG": {
    "Type": "AWS::RDS::OptionGroup",
    "Properties": {
      "EngineName": "oracle-ee",
      "MajorEngineVersion": "19",
      "OptionGroupDescription": "test option group",
      "OptionGroupName": "my-option-group"
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "OG"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "og-stack", tmpl)
	assert.Equal(t, "my-option-group", outputs["Ref"])

	groups, err := backends.RDS.Backend.DescribeOptionGroups("my-option-group")
	require.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Equal(t, "oracle-ee", groups[0].EngineName)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("og-stack")})
	require.NoError(t, err)

	_, err = backends.RDS.Backend.DescribeOptionGroups("my-option-group")
	require.Error(t, err)
}

func testRDSEventSubscription(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "Sub": {
    "Type": "AWS::RDS::EventSubscription",
    "Properties": {
      "SubscriptionName": "my-event-sub",
      "SnsTopicArn": "arn:aws:sns:us-east-1:000000000000:example-topic",
      "SourceType": "db-instance",
      "SourceIds": ["db-instance-1"],
      "EventCategories": ["failure"]
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "Sub"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "es-stack", tmpl)
	assert.Equal(t, "my-event-sub", outputs["Ref"])

	subs, err := backends.RDS.Backend.DescribeEventSubscriptions("my-event-sub")
	require.NoError(t, err)
	require.Len(t, subs, 1)
	assert.Equal(t, "db-instance", subs[0].SourceType)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("es-stack")})
	require.NoError(t, err)

	_, err = backends.RDS.Backend.DescribeEventSubscriptions("my-event-sub")
	require.Error(t, err)
}

func testRDSGlobalCluster(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	tmpl := `{
"Resources": {
  "GC": {
    "Type": "AWS::RDS::GlobalCluster",
    "Properties": {
      "GlobalClusterIdentifier": "my-global-cluster",
      "Engine": "aurora-mysql",
      "EngineVersion": "8.0.mysql_aurora.3.04.0"
    }
  }
},
"Outputs": {"Ref": {"Value": {"Ref": "GC"}}}
}`

	outputs := createStackAndGetOutputs(t, client, "gc-stack", tmpl)
	assert.Equal(t, "my-global-cluster", outputs["Ref"])

	clusters, err := backends.RDS.Backend.DescribeGlobalClusters("my-global-cluster")
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	assert.Equal(t, "aurora-mysql", clusters[0].Engine)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("gc-stack")})
	require.NoError(t, err)

	_, err = backends.RDS.Backend.DescribeGlobalClusters("my-global-cluster")
	require.Error(t, err)
}

func testRDSDBProxyEndpoint(t *testing.T) {
	t.Helper()

	backends, client := newManagedTypesTestClient(t)

	_, err := backends.RDS.Backend.CreateDBProxy(
		"my-proxy", "MYSQL", "arn:aws:iam::000000000000:role/proxy-role",
		nil, []string{"subnet-1"}, nil, "", "", "",
	)
	require.NoError(t, err)

	tmpl := `{
"Resources": {
  "EP": {
    "Type": "AWS::RDS::DBProxyEndpoint",
    "Properties": {
      "DBProxyName": "my-proxy",
      "DBProxyEndpointName": "my-proxy-endpoint",
      "TargetRole": "READ_ONLY",
      "VpcSubnetIds": ["subnet-1"]
    }
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "EP"}},
  "Arn": {"Value": {"Fn::GetAtt": ["EP", "DBProxyEndpointArn"]}},
  "Endpoint": {"Value": {"Fn::GetAtt": ["EP", "Endpoint"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "epstack", tmpl)
	assert.Equal(t, "my-proxy-endpoint", outputs["Ref"])
	assert.NotEmpty(t, outputs["Arn"])
	assert.NotEmpty(t, outputs["Endpoint"])

	eps, err := backends.RDS.Backend.DescribeDBProxyEndpoints("my-proxy", "my-proxy-endpoint")
	require.NoError(t, err)
	require.Len(t, eps, 1)
	assert.Equal(t, "READ_ONLY", eps[0].TargetRole)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("epstack")})
	require.NoError(t, err)

	_, err = backends.RDS.Backend.DescribeDBProxyEndpoints("my-proxy", "my-proxy-endpoint")
	require.Error(t, err)
}
