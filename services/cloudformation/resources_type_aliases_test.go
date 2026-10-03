package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStack_TypeAliases(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testCertificateManagerCertificateAlias, "certificatemanager_certificate"},
		{testOpenSearchServiceDomainAlias, "opensearchservice_domain"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testCertificateManagerCertificateAlias(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "Cert": {
    "Type": "AWS::CertificateManager::Certificate",
    "Properties": {"DomainName": "unit-cm.example.com", "ValidationMethod": "DNS"}
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Cert"}},
  "CertificateArn": {"Value": {"Fn::GetAtt": ["Cert", "CertificateArn"]}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "cm-cert-stack", tmpl)
	assert.Equal(t, outputs["Ref"], outputs["CertificateArn"])
	assert.Contains(t, outputs["Ref"], "arn:aws:acm:")

	cert, err := backends.ACM.Backend.DescribeCertificate(t.Context(), outputs["Ref"])
	require.NoError(t, err)
	assert.NotNil(t, cert)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("cm-cert-stack")})
	require.NoError(t, err)

	_, err = backends.ACM.Backend.DescribeCertificate(t.Context(), outputs["Ref"])
	require.Error(t, err)
}

func testOpenSearchServiceDomainAlias(t *testing.T) {
	t.Helper()

	backends, client := newMoreResourcesTestClient(t)

	tmpl := `{
"Resources": {
  "Domain": {
    "Type": "AWS::OpenSearchService::Domain",
    "Properties": {"DomainName": "unit-os-domain", "EngineVersion": "OpenSearch_2.11"}
  }
},
"Outputs": {
  "Ref": {"Value": {"Ref": "Domain"}}
}
}`

	outputs := createStackAndGetOutputs(t, client, "os-domain-stack", tmpl)
	assert.Contains(t, outputs["Ref"], "unit-os-domain")

	_, err := backends.OpenSearch.Backend.DescribeDomain("unit-os-domain")
	require.NoError(t, err)

	_, err = client.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("os-domain-stack")})
	require.NoError(t, err)

	_, err = backends.OpenSearch.Backend.DescribeDomain("unit-os-domain")
	require.Error(t, err)
}
