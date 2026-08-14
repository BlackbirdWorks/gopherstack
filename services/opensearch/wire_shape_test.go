package opensearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// This file proves, through the real aws-sdk-go-v2 opensearch client
// (opensearch@v1.75.4), that two response families decode correctly. Before
// the fix:
//   - ListApplications/GetApplication/UpdateApplication emitted their item
//     fields ("Id", "Name", "Arn", "Status", "Endpoint", "AppConfigs",
//     "DataSources", "CreatedAt", "LastUpdatedAt") in PascalCase; the real
//     deserializers (awsRestjson1_deserializeDocumentApplicationSummary /
//     ...OpDocumentGetApplicationOutput / ...OpDocumentUpdateApplicationOutput)
//     switch on lowerCamel keys ("id", "name", "arn", "status", "endpoint",
//     "appConfigs", "dataSources", "createdAt", "lastUpdatedAt"). Every field
//     of every application was silently dropped by a real typed client, even
//     though the sibling op CreateApplication already used the correct
//     lowerCamel casing.
//   - ListVpcEndpoints wrapped its list under "VpcEndpoints"; the real
//     deserializer (awsRestjson1_deserializeOpDocumentListVpcEndpointsOutput)
//     switches on "VpcEndpointSummaryList", matching its sibling
//     ListVpcEndpointsForDomain. A real client's VpcEndpointSummaryList field
//     stayed nil for every call.

func TestSDKRoundTrip_Applications_LowerCamelWireFields(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)

	created, err := client.CreateApplication(t.Context(), &opensearchsdk.CreateApplicationInput{
		Name: aws.String("wire-shape-app"),
	})
	require.NoError(t, err)
	appID := aws.ToString(created.Id)

	listOut, err := client.ListApplications(t.Context(), &opensearchsdk.ListApplicationsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, listOut.ApplicationSummaries,
		"ListApplicationsOutput.ApplicationSummaries must decode a non-empty slice")

	var summary *types.ApplicationSummary

	for i := range listOut.ApplicationSummaries {
		if aws.ToString(listOut.ApplicationSummaries[i].Id) == appID {
			summary = &listOut.ApplicationSummaries[i]
		}
	}

	require.NotNil(t, summary, "created application must appear in ListApplications by Id")
	assert.Equal(t, "wire-shape-app", aws.ToString(summary.Name))
	assert.NotEmpty(t, aws.ToString(summary.Arn))
	assert.NotEmpty(t, aws.ToString(summary.Endpoint))
	assert.Equal(t, types.ApplicationStatusActive, summary.Status)

	getOut, err := client.GetApplication(t.Context(), &opensearchsdk.GetApplicationInput{Id: aws.String(appID)})
	require.NoError(t, err)
	assert.Equal(t, "wire-shape-app", aws.ToString(getOut.Name))
	assert.NotEmpty(t, aws.ToString(getOut.Arn))
	assert.NotEmpty(t, aws.ToString(getOut.Endpoint))
	assert.Equal(t, types.ApplicationStatusActive, getOut.Status)
	require.NotNil(t, getOut.CreatedAt, "GetApplicationOutput.CreatedAt must decode")
	require.NotNil(t, getOut.LastUpdatedAt, "GetApplicationOutput.LastUpdatedAt must decode")

	updateOut, err := client.UpdateApplication(t.Context(), &opensearchsdk.UpdateApplicationInput{
		Id: aws.String(appID),
		AppConfigs: []types.AppConfig{
			{Key: types.AppConfigTypeOpensearchDashboardAdminUsers, Value: aws.String("true")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "wire-shape-app", aws.ToString(updateOut.Name))
	assert.NotEmpty(t, aws.ToString(updateOut.Arn))
	require.NotEmpty(t, updateOut.AppConfigs,
		"UpdateApplicationOutput.AppConfigs must decode a non-empty slice")
	assert.Equal(t, "true", aws.ToString(updateOut.AppConfigs[0].Value))
}

func TestSDKRoundTrip_ListVpcEndpoints_VpcEndpointSummaryListWireKey(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)

	domOut, err := client.CreateDomain(t.Context(), &opensearchsdk.CreateDomainInput{
		DomainName: aws.String("vpc-wire-shape-domain"),
	})
	require.NoError(t, err)

	_, err = client.CreateVpcEndpoint(t.Context(), &opensearchsdk.CreateVpcEndpointInput{
		DomainArn: domOut.DomainStatus.ARN,
		VpcOptions: &types.VPCOptions{
			SubnetIds: []string{"subnet-0123456789abcdef0"},
		},
	})
	require.NoError(t, err)

	out, err := client.ListVpcEndpoints(t.Context(), &opensearchsdk.ListVpcEndpointsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.VpcEndpointSummaryList,
		"ListVpcEndpointsOutput.VpcEndpointSummaryList must decode a non-empty slice")
	assert.Equal(t, aws.ToString(domOut.DomainStatus.ARN), aws.ToString(out.VpcEndpointSummaryList[0].DomainArn))
}
