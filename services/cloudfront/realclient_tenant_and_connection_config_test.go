package cloudfront_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestRealClient_TenantAndConnectionConfig drives the 9 ops that a real aws-sdk-go-v2
// cloudfront client had never exercised before (gopherstack-n3zi):
// GetDistributionConfig, the distribution-tenant invalidation/customization/
// verify family, and the connection-group/connection-function update/test
// family.
func TestRealClient_TenantAndConnectionConfig(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "get_distribution_config", run: func(t *testing.T) {
			t.Helper()

			testGetDistributionConfigRealClient(t)
		}},
		{name: "tenant_invalidation_and_customization", run: func(t *testing.T) {
			t.Helper()

			testTenantInvalidationAndCustomizationRealClient(t)
		}},
		{name: "verify_dns_configuration", run: func(t *testing.T) {
			t.Helper()

			testVerifyDNSConfigurationRealClient(t)
		}},
		{name: "connection_group_and_function", run: func(t *testing.T) {
			t.Helper()

			testConnectionGroupAndFunctionRealClient(t)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func newRealClient(t *testing.T) *cfsdk.Client {
	t.Helper()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")

	return newTestCloudFrontClient(t, cloudfront.NewHandler(backend))
}

func testGetDistributionConfigRealClient(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	client := newRealClient(t)

	created, err := client.CreateDistribution(ctx, &cfsdk.CreateDistributionInput{
		DistributionConfig: &types.DistributionConfig{
			CallerReference: aws.String("ref-gdc-1"),
			Comment:         aws.String("gdc test"),
			Enabled:         aws.Bool(true),
			Origins: &types.Origins{
				Quantity: aws.Int32(1),
				Items: []types.Origin{
					{Id: aws.String("origin1"), DomainName: aws.String("example.com")},
				},
			},
			DefaultCacheBehavior: &types.DefaultCacheBehavior{
				TargetOriginId:       aws.String("origin1"),
				ViewerProtocolPolicy: types.ViewerProtocolPolicyAllowAll,
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.Distribution)
	distID := aws.ToString(created.Distribution.Id)

	out, err := client.GetDistributionConfig(ctx, &cfsdk.GetDistributionConfigInput{Id: aws.String(distID)})
	require.NoError(t, err)
	require.NotNil(t, out.DistributionConfig, "GetDistributionConfigOutput.DistributionConfig must decode")
	require.NotEmpty(t, aws.ToString(out.ETag), "ETag header must be populated")
	require.Equal(t, "ref-gdc-1", aws.ToString(out.DistributionConfig.CallerReference))
	require.Equal(t, "gdc test", aws.ToString(out.DistributionConfig.Comment))
	require.True(t, aws.ToBool(out.DistributionConfig.Enabled))
}

func testTenantInvalidationAndCustomizationRealClient(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	client := newRealClient(t)

	created, err := client.CreateDistribution(ctx, &cfsdk.CreateDistributionInput{
		DistributionConfig: &types.DistributionConfig{
			CallerReference: aws.String("ref-tenant-1"),
			Comment:         aws.String("multitenant"),
			Enabled:         aws.Bool(true),
			Origins: &types.Origins{
				Quantity: aws.Int32(1),
				Items: []types.Origin{
					{Id: aws.String("origin1"), DomainName: aws.String("example.com")},
				},
			},
			DefaultCacheBehavior: &types.DefaultCacheBehavior{
				TargetOriginId:       aws.String("origin1"),
				ViewerProtocolPolicy: types.ViewerProtocolPolicyAllowAll,
			},
		},
	})
	require.NoError(t, err)
	distID := aws.ToString(created.Distribution.Id)

	tenant, err := client.CreateDistributionTenant(ctx, &cfsdk.CreateDistributionTenantInput{
		DistributionId: aws.String(distID),
		Name:           aws.String("tenant-slice19"),
		Domains:        []types.DomainItem{{Domain: aws.String("tenant19.example.com")}},
	})
	require.NoError(t, err)
	require.NotNil(t, tenant.DistributionTenant)
	tenantID := aws.ToString(tenant.DistributionTenant.Id)
	tenantETag := aws.ToString(tenant.ETag)

	// UpdateDistributionTenant.
	updated, err := client.UpdateDistributionTenant(ctx, &cfsdk.UpdateDistributionTenantInput{
		Id:      aws.String(tenantID),
		IfMatch: aws.String(tenantETag),
		Domains: []types.DomainItem{
			{Domain: aws.String("tenant19.example.com")},
			{Domain: aws.String("tenant19b.example.com")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.DistributionTenant)
	require.NotEmpty(t, aws.ToString(updated.ETag))
	domains := make([]string, 0, len(updated.DistributionTenant.Domains))
	for _, d := range updated.DistributionTenant.Domains {
		domains = append(domains, aws.ToString(d.Domain))
	}
	require.Contains(t, domains, "tenant19b.example.com")

	// CreateInvalidationForDistributionTenant.
	createdInv, err := client.CreateInvalidationForDistributionTenant(
		ctx,
		&cfsdk.CreateInvalidationForDistributionTenantInput{
			Id: aws.String(tenantID),
			InvalidationBatch: &types.InvalidationBatch{
				CallerReference: aws.String("caller-ref-19"),
				Paths: &types.Paths{
					Quantity: aws.Int32(1),
					Items:    []string{"/*"},
				},
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, createdInv.Invalidation)
	invID := aws.ToString(createdInv.Invalidation.Id)
	require.NotNil(
		t,
		createdInv.Invalidation.InvalidationBatch,
		"CreateInvalidationForDistributionTenantOutput.Invalidation.InvalidationBatch must decode",
	)
	require.Equal(t, "caller-ref-19", aws.ToString(createdInv.Invalidation.InvalidationBatch.CallerReference))

	// GetInvalidationForDistributionTenant.
	gotInv, err := client.GetInvalidationForDistributionTenant(ctx, &cfsdk.GetInvalidationForDistributionTenantInput{
		DistributionTenantId: aws.String(tenantID),
		Id:                   aws.String(invID),
	})
	require.NoError(t, err)
	require.NotNil(t, gotInv.Invalidation)
	require.Equal(t, invID, aws.ToString(gotInv.Invalidation.Id))
	require.NotNil(
		t,
		gotInv.Invalidation.InvalidationBatch,
		"GetInvalidationForDistributionTenantOutput.Invalidation.InvalidationBatch is a required member and must decode",
	)
	require.Equal(t, "caller-ref-19", aws.ToString(gotInv.Invalidation.InvalidationBatch.CallerReference))
	require.NotNil(t, gotInv.Invalidation.InvalidationBatch.Paths)
	require.Contains(t, gotInv.Invalidation.InvalidationBatch.Paths.Items, "/*")

	// ListDistributionTenantsByCustomization (no filter -- every tenant qualifies).
	listed, err := client.ListDistributionTenantsByCustomization(
		ctx,
		&cfsdk.ListDistributionTenantsByCustomizationInput{},
	)
	require.NoError(t, err)
	found := false
	for _, ts := range listed.DistributionTenantList {
		if aws.ToString(ts.Id) == tenantID {
			found = true
		}
	}
	require.True(t, found, "ListDistributionTenantsByCustomization must include the created tenant")
}

func testVerifyDNSConfigurationRealClient(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	client := newRealClient(t)

	created, err := client.CreateDistribution(ctx, &cfsdk.CreateDistributionInput{
		DistributionConfig: &types.DistributionConfig{
			CallerReference: aws.String("ref-dns-1"),
			Comment:         aws.String("dns test"),
			Enabled:         aws.Bool(true),
			Origins: &types.Origins{
				Quantity: aws.Int32(1),
				Items: []types.Origin{
					{Id: aws.String("origin1"), DomainName: aws.String("example.com")},
				},
			},
			DefaultCacheBehavior: &types.DefaultCacheBehavior{
				TargetOriginId:       aws.String("origin1"),
				ViewerProtocolPolicy: types.ViewerProtocolPolicyAllowAll,
			},
		},
	})
	require.NoError(t, err)
	distID := aws.ToString(created.Distribution.Id)

	tenant, err := client.CreateDistributionTenant(ctx, &cfsdk.CreateDistributionTenantInput{
		DistributionId: aws.String(distID),
		Name:           aws.String("tenant-dns"),
		Domains:        []types.DomainItem{{Domain: aws.String("dns19.example.com")}},
	})
	require.NoError(t, err)
	tenantID := aws.ToString(tenant.DistributionTenant.Id)

	out, err := client.VerifyDnsConfiguration(ctx, &cfsdk.VerifyDnsConfigurationInput{
		Identifier: aws.String(tenantID),
	})
	require.NoError(t, err)
	require.Len(
		t,
		out.DnsConfigurationList,
		1,
		"DnsConfigurationList wrapper (<DnsConfiguration>, not <Item>) must decode",
	)
	require.Equal(t, "dns19.example.com", aws.ToString(out.DnsConfigurationList[0].Domain))
	require.Equal(
		t,
		types.DnsConfigurationStatusValid,
		out.DnsConfigurationList[0].Status,
		"Status must be a real DnsConfigurationStatus enum value, not a fabricated PASSED/FAILED string",
	)
}

func testConnectionGroupAndFunctionRealClient(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	client := newRealClient(t)

	cg, err := client.CreateConnectionGroup(ctx, &cfsdk.CreateConnectionGroupInput{
		Name: aws.String("cg-slice19"),
	})
	require.NoError(t, err)
	require.NotNil(t, cg.ConnectionGroup)
	cgID := aws.ToString(cg.ConnectionGroup.Id)
	cgETag := aws.ToString(cg.ETag)

	updatedCG, err := client.UpdateConnectionGroup(ctx, &cfsdk.UpdateConnectionGroupInput{
		Id:      aws.String(cgID),
		IfMatch: aws.String(cgETag),
		Enabled: aws.Bool(false),
	})
	require.NoError(t, err)
	require.NotNil(t, updatedCG.ConnectionGroup)
	require.False(t, aws.ToBool(updatedCG.ConnectionGroup.Enabled))
	require.NotEmpty(t, aws.ToString(updatedCG.ETag))

	cfn, err := client.CreateConnectionFunction(ctx, &cfsdk.CreateConnectionFunctionInput{
		Name:                   aws.String("cfn-slice19"),
		ConnectionFunctionCode: []byte("function handler(event) { return event; }"),
		ConnectionFunctionConfig: &types.FunctionConfig{
			Comment: aws.String("v1"),
			Runtime: types.FunctionRuntimeCloudfrontJs20,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, cfn.ConnectionFunctionSummary)
	cfnID := aws.ToString(cfn.ConnectionFunctionSummary.Id)
	cfnETag := aws.ToString(cfn.ETag)

	updatedCFN, err := client.UpdateConnectionFunction(ctx, &cfsdk.UpdateConnectionFunctionInput{
		Id:                     aws.String(cfnID),
		IfMatch:                aws.String(cfnETag),
		ConnectionFunctionCode: []byte("function handler(event) { return event; } // v2"),
		ConnectionFunctionConfig: &types.FunctionConfig{
			Comment: aws.String("v2"),
			Runtime: types.FunctionRuntimeCloudfrontJs20,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updatedCFN.ConnectionFunctionSummary)
	require.Equal(t, "v2", aws.ToString(updatedCFN.ConnectionFunctionSummary.ConnectionFunctionConfig.Comment))
	require.NotEmpty(
		t,
		aws.ToString(updatedCFN.ConnectionFunctionSummary.ConnectionFunctionArn),
		"UpdateConnectionFunctionOutput.ConnectionFunctionSummary.ConnectionFunctionArn must decode",
	)
	require.NotEmpty(t, aws.ToString(updatedCFN.ETag))

	testResult, err := client.TestConnectionFunction(ctx, &cfsdk.TestConnectionFunctionInput{
		Id:               aws.String(cfnID),
		IfMatch:          aws.String(aws.ToString(updatedCFN.ETag)),
		ConnectionObject: []byte(`{"foo":"bar"}`),
	})
	require.NoError(t, err)
	require.NotNil(
		t,
		testResult.ConnectionFunctionTestResult,
		"ConnectionFunctionTestResult must decode flat, no wrapper element",
	)
	require.NotNil(t, testResult.ConnectionFunctionTestResult.ConnectionFunctionSummary)
	require.Equal(t, cfnID, aws.ToString(testResult.ConnectionFunctionTestResult.ConnectionFunctionSummary.Id))
	require.NotEmpty(
		t,
		aws.ToString(testResult.ConnectionFunctionTestResult.ConnectionFunctionSummary.ConnectionFunctionArn),
		"TestConnectionFunction's nested summary must carry the full shape, not an abbreviated Id/Name/Stage-only one",
	)
	require.NotEmpty(t, testResult.ConnectionFunctionTestResult.ConnectionFunctionExecutionLogs)
}
