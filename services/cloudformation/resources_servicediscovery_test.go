package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	sdbackend "github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

// newServiceDiscoveryTestClient wires a real aws-sdk-go-v2 CloudFormation
// client against a backend with ServiceDiscovery (Cloud Map) wired to a real
// in-memory backend, for tests that provision through the actual
// CreateStack/DescribeStackResources/DeleteStack path.
func newServiceDiscoveryTestClient(t *testing.T) (*cloudformation.ServiceBackends, *cfnsdk.Client) {
	t.Helper()

	backends := newDependentServiceBackends(t)
	backends.ServiceDiscovery = sdbackend.NewHandler(sdbackend.NewInMemoryBackend("000000000000", "us-east-1"))

	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)
	client := newTestClientForBackend(t, backend)

	return backends, client
}

func TestCreateStack_ServiceDiscoveryHttpNamespaceAndService(t *testing.T) {
	t.Parallel()

	backends, client := newServiceDiscoveryTestClient(t)

	tmpl := `{
"Resources": {
  "Namespace": {
    "Type": "AWS::ServiceDiscovery::HttpNamespace",
    "Properties": {"Name": "example-namespace", "Description": "test http ns"}
  },
  "Service": {
    "Type": "AWS::ServiceDiscovery::Service",
    "Properties": {
      "Name": "backend",
      "NamespaceId": {"Ref": "Namespace"},
      "Description": "test service"
    }
  }
},
"Outputs": {
  "NsId": {"Value": {"Ref": "Namespace"}},
  "NsArn": {"Value": {"Fn::GetAtt": ["Namespace", "Arn"]}},
  "SvcId": {"Value": {"Ref": "Service"}},
  "SvcArn": {"Value": {"Fn::GetAtt": ["Service", "Arn"]}},
  "SvcName": {"Value": {"Fn::GetAtt": ["Service", "Name"]}}
}
}`

	stackName := "sd-http-stack"
	outputs := createStackAndGetOutputs(t, client, stackName, tmpl)

	nsID := outputs["NsId"]
	require.NotEmpty(t, nsID)
	assert.Contains(t, outputs["NsArn"], "namespace/"+nsID)

	mem, ok := backends.ServiceDiscovery.Backend.(*sdbackend.InMemoryBackend)
	require.True(t, ok)

	ns, err := mem.GetNamespace(nsID)
	require.NoError(t, err)
	assert.Equal(t, "example-namespace", ns.Name)
	assert.Equal(t, "test http ns", ns.Description)

	svcID := outputs["SvcId"]
	require.NotEmpty(t, svcID)
	assert.Contains(t, outputs["SvcArn"], "service/"+svcID)
	assert.Equal(t, "backend", outputs["SvcName"])

	svc, err := mem.GetService(svcID)
	require.NoError(t, err)
	assert.Equal(t, nsID, svc.NamespaceID)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String(stackName)})
	require.NoError(t, err)

	_, err = mem.GetService(svcID)
	require.Error(t, err)
	_, err = mem.GetNamespace(nsID)
	require.Error(t, err)
}

func TestCreateStack_ServiceDiscoveryPrivateDnsNamespace(t *testing.T) {
	t.Parallel()

	backends, client := newServiceDiscoveryTestClient(t)

	tmpl := `{
"Resources": {
  "Namespace": {
    "Type": "AWS::ServiceDiscovery::PrivateDnsNamespace",
    "Properties": {"Name": "private.example.com", "Vpc": "vpc-12345678"}
  }
},
"Outputs": {
  "NsId": {"Value": {"Ref": "Namespace"}},
  "HostedZoneId": {"Value": {"Fn::GetAtt": ["Namespace", "HostedZoneId"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "sd-private-stack", tmpl)
	nsID := outputs["NsId"]
	require.NotEmpty(t, nsID)
	require.NotEmpty(t, outputs["HostedZoneId"])

	mem, ok := backends.ServiceDiscovery.Backend.(*sdbackend.InMemoryBackend)
	require.True(t, ok)

	ns, err := mem.GetNamespace(nsID)
	require.NoError(t, err)
	assert.Equal(t, "vpc-12345678", ns.VPC)
	require.NotNil(t, ns.Properties)
	require.NotNil(t, ns.Properties.DNSProperties)
	assert.Equal(t, outputs["HostedZoneId"], ns.Properties.DNSProperties.HostedZoneID)
}

func TestCreateStack_ServiceDiscoveryPublicDnsNamespaceAndInstance(t *testing.T) {
	t.Parallel()

	backends, client := newServiceDiscoveryTestClient(t)

	tmpl := `{
"Resources": {
  "Namespace": {"Type": "AWS::ServiceDiscovery::PublicDnsNamespace", "Properties": {"Name": "example.com"}},
  "Service": {
    "Type": "AWS::ServiceDiscovery::Service",
    "Properties": {
      "Name": "backend",
      "NamespaceId": {"Ref": "Namespace"},
      "DnsConfig": {"RoutingPolicy": "MULTIVALUE", "DnsRecords": [{"Type": "A", "TTL": 60}]}
    }
  },
  "Instance": {
    "Type": "AWS::ServiceDiscovery::Instance",
    "Properties": {
      "ServiceId": {"Ref": "Service"},
      "InstanceId": "i-abcd1234",
      "InstanceAttributes": {"AWS_INSTANCE_IPV4": "192.0.2.44"}
    }
  }
},
"Outputs": {
  "SvcId": {"Value": {"Ref": "Service"}},
  "InstanceId": {"Value": {"Ref": "Instance"}}
}
}`

	stackName := "sd-public-instance-stack"
	outputs := createStackAndGetOutputs(t, client, stackName, tmpl)
	assert.Equal(t, "i-abcd1234", outputs["InstanceId"])

	mem, ok := backends.ServiceDiscovery.Backend.(*sdbackend.InMemoryBackend)
	require.True(t, ok)

	inst, err := mem.GetInstance(outputs["SvcId"], "i-abcd1234")
	require.NoError(t, err)
	assert.Equal(t, "192.0.2.44", inst.Attributes["AWS_INSTANCE_IPV4"])

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String(stackName)})
	require.NoError(t, err)

	_, err = mem.GetInstance(outputs["SvcId"], "i-abcd1234")
	require.Error(t, err)
}
