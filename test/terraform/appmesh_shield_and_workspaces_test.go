package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmsvc54 "github.com/aws/aws-sdk-go-v2/service/acm"
	acmtypes54 "github.com/aws/aws-sdk-go-v2/service/acm/types"
	apigwsvc54 "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes54 "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	appmeshsvc54 "github.com/aws/aws-sdk-go-v2/service/appmesh"
	cloudfrontsvc54 "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	cfkvssvc54 "github.com/aws/aws-sdk-go-v2/service/cloudfrontkeyvaluestore"
	codepipelinesvc54 "github.com/aws/aws-sdk-go-v2/service/codepipeline"
	codepipelinetypes54 "github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	cognitoidentitysvc54 "github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	cognitoidentitytypes54 "github.com/aws/aws-sdk-go-v2/service/cognitoidentity/types"
	directoryservicesvc54 "github.com/aws/aws-sdk-go-v2/service/directoryservice"
	directoryservicetypes54 "github.com/aws/aws-sdk-go-v2/service/directoryservice/types"
	ec2svc54 "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types54 "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	kinesisanalyticsv2svc54 "github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2"
	rdssvc54 "github.com/aws/aws-sdk-go-v2/service/rds"
	shieldsvc54 "github.com/aws/aws-sdk-go-v2/service/shield"
	shieldtypes54 "github.com/aws/aws-sdk-go-v2/service/shield/types"
	timestreamquerysvc54 "github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	workspacessvc54 "github.com/aws/aws-sdk-go-v2/service/workspaces"
	workspacestypes54 "github.com/aws/aws-sdk-go-v2/service/workspaces/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_AppmeshShieldAndWorkspaces provisions previously-uncovered terraform resource types end-to-end.
// aws_spot_fleet_request was dropped: its delete waiter never converges within a CI shard's time budget.
func TestTerraform_AppmeshShieldAndWorkspaces(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "appmesh-shield-and-workspaces",
			setup:   setupEndpoint,
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyAppmeshShieldAndWorkspacesACM(ctx, t)
				verifyAppmeshShieldAndWorkspacesAPIGateway(ctx, t)
				verifyAppmeshShieldAndWorkspacesAppMesh(ctx, t)
				verifyAppmeshShieldAndWorkspacesCloudFrontKVS(ctx, t)
				verifyAppmeshShieldAndWorkspacesCodePipeline(ctx, t)
				verifyAppmeshShieldAndWorkspacesCognitoIdentity(ctx, t)
				verifyAppmeshShieldAndWorkspacesEC2Misc(ctx, t)
				verifyAppmeshShieldAndWorkspacesShield(ctx, t)
				verifyAppmeshShieldAndWorkspacesTimestreamQuery(ctx, t)
				verifyAppmeshShieldAndWorkspacesWorkspaces(ctx, t)
				verifyAppmeshShieldAndWorkspacesRDS(ctx, t)
				verifyAppmeshShieldAndWorkspacesKinesisAnalyticsV2(ctx, t)
				verifyAppmeshShieldAndWorkspacesDirectoryService(ctx, t)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func verifyAppmeshShieldAndWorkspacesACM(ctx context.Context, t *testing.T) {
	t.Helper()
	client := acmsvc54.NewFromConfig(megaConfig(t), func(o *acmsvc54.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.ListCertificates(ctx, &acmsvc54.ListCertificatesInput{})
	require.NoError(t, err, "ListCertificates should succeed")
	cert := findBy(t, out.CertificateSummaryList, func(c acmtypes54.CertificateSummary) bool {
		return aws.ToString(c.DomainName) == "aswm.example.com"
	}, "certificate for aswm.example.com")

	descOut, err := client.DescribeCertificate(ctx, &acmsvc54.DescribeCertificateInput{
		CertificateArn: cert.CertificateArn,
	})
	require.NoError(t, err, "DescribeCertificate should succeed")
	require.NotNil(t, descOut.Certificate)
	assert.Equal(t, acmtypes54.CertificateStatusIssued, descOut.Certificate.Status,
		"aws_acm_certificate_validation should observe the certificate become ISSUED")
}

func verifyAppmeshShieldAndWorkspacesAPIGateway(ctx context.Context, t *testing.T) {
	t.Helper()
	client := apigwsvc54.NewFromConfig(megaConfig(t), func(o *apigwsvc54.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	domOut, err := client.GetDomainName(ctx, &apigwsvc54.GetDomainNameInput{
		DomainName: aws.String("aswm-api.example.com"),
	})
	require.NoError(t, err, "GetDomainName should succeed")
	require.NotNil(t, domOut.DomainNameArn)

	assocOut, err := client.GetDomainNameAccessAssociations(ctx, &apigwsvc54.GetDomainNameAccessAssociationsInput{})
	require.NoError(t, err, "GetDomainNameAccessAssociations should succeed")
	findBy(t, assocOut.Items, func(a apigwtypes54.DomainNameAccessAssociation) bool {
		return aws.ToString(a.DomainNameArn) == aws.ToString(domOut.DomainNameArn)
	}, "an access association for aswm-api.example.com's domain name")
}

func verifyAppmeshShieldAndWorkspacesAppMesh(ctx context.Context, t *testing.T) {
	t.Helper()
	client := appmeshsvc54.NewFromConfig(megaConfig(t), func(o *appmeshsvc54.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	vgOut, err := client.DescribeVirtualGateway(ctx, &appmeshsvc54.DescribeVirtualGatewayInput{
		MeshName:           aws.String("aswm-mesh"),
		VirtualGatewayName: aws.String("aswm-vgw"),
	})
	require.NoError(t, err, "DescribeVirtualGateway should succeed")
	require.NotNil(t, vgOut.VirtualGateway)

	grOut, err := client.DescribeGatewayRoute(ctx, &appmeshsvc54.DescribeGatewayRouteInput{
		MeshName:           aws.String("aswm-mesh"),
		VirtualGatewayName: aws.String("aswm-vgw"),
		GatewayRouteName:   aws.String("aswm-gw-route"),
	})
	require.NoError(t, err, "DescribeGatewayRoute should succeed")
	require.NotNil(t, grOut.GatewayRoute)
}

func verifyAppmeshShieldAndWorkspacesCloudFrontKVS(ctx context.Context, t *testing.T) {
	t.Helper()
	cfClient := cloudfrontsvc54.NewFromConfig(megaConfig(t), func(o *cloudfrontsvc54.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	kvsOut, err := cfClient.DescribeKeyValueStore(ctx, &cloudfrontsvc54.DescribeKeyValueStoreInput{
		Name: aws.String("aswm-kvs"),
	})
	require.NoError(t, err, "DescribeKeyValueStore should succeed")
	require.NotNil(t, kvsOut.KeyValueStore)
	kvsARN := aws.ToString(kvsOut.KeyValueStore.ARN)

	kvClient := cfkvssvc54.NewFromConfig(megaConfig(t), func(o *cfkvssvc54.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	keysOut, err := kvClient.ListKeys(ctx, &cfkvssvc54.ListKeysInput{KvsARN: aws.String(kvsARN)})
	require.NoError(t, err, "ListKeys should succeed")
	require.Len(t, keysOut.Items, 2, "keys_exclusive must make the store match exactly the configured pairs")
}

func verifyAppmeshShieldAndWorkspacesCodePipeline(ctx context.Context, t *testing.T) {
	t.Helper()
	client := codepipelinesvc54.NewFromConfig(megaConfig(t), func(o *codepipelinesvc54.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.ListWebhooks(ctx, &codepipelinesvc54.ListWebhooksInput{})
	require.NoError(t, err, "ListWebhooks should succeed")
	findBy(t, out.Webhooks, func(w codepipelinetypes54.ListWebhookItem) bool {
		return w.Definition != nil && aws.ToString(w.Definition.Name) == "aswm-webhook" &&
			w.Definition.AuthenticationConfiguration != nil
	}, "webhook aswm-webhook with a non-nil AuthenticationConfiguration")
}

func verifyAppmeshShieldAndWorkspacesCognitoIdentity(ctx context.Context, t *testing.T) {
	t.Helper()
	client := cognitoidentitysvc54.NewFromConfig(megaConfig(t), func(o *cognitoidentitysvc54.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	poolsOut, err := client.ListIdentityPools(
		ctx, &cognitoidentitysvc54.ListIdentityPoolsInput{MaxResults: aws.Int32(60)},
	)
	require.NoError(t, err, "ListIdentityPools should succeed")
	pool := findBy(t, poolsOut.IdentityPools, func(p cognitoidentitytypes54.IdentityPoolShortDescription) bool {
		return aws.ToString(p.IdentityPoolName) == "aswm_pool"
	}, "identity pool aswm_pool")

	tagOut, err := client.GetPrincipalTagAttributeMap(ctx, &cognitoidentitysvc54.GetPrincipalTagAttributeMapInput{
		IdentityPoolId:       pool.IdentityPoolId,
		IdentityProviderName: aws.String("graph.facebook.com"),
	})
	require.NoError(t, err, "GetPrincipalTagAttributeMap should succeed")
	assert.True(t, aws.ToBool(tagOut.UseDefaults), "use_defaults = true should round-trip")
}

func verifyAppmeshShieldAndWorkspacesEC2Misc(ctx context.Context, t *testing.T) {
	t.Helper()
	client := ec2svc54.NewFromConfig(megaConfig(t), func(o *ec2svc54.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	poolsOut, err := client.DescribeIpamPools(ctx, &ec2svc54.DescribeIpamPoolsInput{
		Filters: []ec2types54.Filter{{Name: aws.String("locale"), Values: []string{"us-east-1"}}},
	})
	require.NoError(t, err, "DescribeIpamPools should succeed")
	pool := findBy(t, poolsOut.IpamPools, func(p ec2types54.IpamPool) bool {
		return aws.ToString(p.Locale) == "us-east-1"
	}, "the aswm IPAM pool")

	allocOut, err := client.GetIpamPoolAllocations(ctx, &ec2svc54.GetIpamPoolAllocationsInput{
		IpamPoolId: pool.IpamPoolId,
	})
	require.NoError(t, err, "GetIpamPoolAllocations should succeed")
	assert.Empty(t, allocOut.IpamPoolAllocations,
		"a PreviewNextCidr=true allocation must not be recorded as a real allocation")

	npOut, err := client.DescribeAwsNetworkPerformanceMetricSubscriptions(
		ctx, &ec2svc54.DescribeAwsNetworkPerformanceMetricSubscriptionsInput{},
	)
	require.NoError(t, err, "DescribeAwsNetworkPerformanceMetricSubscriptions should succeed")
	findBy(t, npOut.Subscriptions, func(s ec2types54.Subscription) bool {
		return aws.ToString(s.Source) == "us-east-1" && aws.ToString(s.Destination) == "us-west-2"
	}, "a network performance metric subscription us-east-1 -> us-west-2")

	sgOut, err := client.DescribeSecurityGroupVpcAssociations(
		ctx, &ec2svc54.DescribeSecurityGroupVpcAssociationsInput{},
	)
	require.NoError(t, err, "DescribeSecurityGroupVpcAssociations should succeed")
	findBy(t, sgOut.SecurityGroupVpcAssociations, func(a ec2types54.SecurityGroupVpcAssociation) bool {
		return a.State == ec2types54.SecurityGroupVpcAssociationStateAssociated
	}, "an associated security group VPC association")
}

func verifyAppmeshShieldAndWorkspacesShield(ctx context.Context, t *testing.T) {
	t.Helper()
	client := shieldsvc54.NewFromConfig(megaConfig(t), func(o *shieldsvc54.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	listOut, err := client.ListProtections(ctx, &shieldsvc54.ListProtectionsInput{})
	require.NoError(t, err, "ListProtections should succeed")
	prot := findBy(t, listOut.Protections, func(p shieldtypes54.Protection) bool {
		return aws.ToString(p.Name) == "aswm-shield-protection"
	}, "shield protection aswm-shield-protection")

	descOut, err := client.DescribeProtection(ctx, &shieldsvc54.DescribeProtectionInput{ProtectionId: prot.Id})
	require.NoError(t, err, "DescribeProtection should succeed")
	require.NotNil(t, descOut.Protection)
	// Protection.HealthCheckIds holds the bare Route 53 health check ID, not
	// the ARN passed to AssociateHealthCheck.
	assert.Len(t, descOut.Protection.HealthCheckIds, 1)
}

func verifyAppmeshShieldAndWorkspacesTimestreamQuery(ctx context.Context, t *testing.T) {
	t.Helper()
	client := timestreamquerysvc54.NewFromConfig(megaConfig(t), func(o *timestreamquerysvc54.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.ListScheduledQueries(ctx, &timestreamquerysvc54.ListScheduledQueriesInput{})
	require.NoError(t, err, "ListScheduledQueries should succeed")
	found := false
	for _, q := range out.ScheduledQueries {
		if aws.ToString(q.Name) == "aswm-scheduled-query" {
			found = true
		}
	}
	assert.True(t, found, "scheduled query aswm-scheduled-query should be listed")
}

func verifyAppmeshShieldAndWorkspacesWorkspaces(ctx context.Context, t *testing.T) {
	t.Helper()
	client := workspacessvc54.NewFromConfig(megaConfig(t), func(o *workspacessvc54.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	dirOut, err := client.DescribeWorkspaceDirectories(ctx, &workspacessvc54.DescribeWorkspaceDirectoriesInput{})
	require.NoError(t, err, "DescribeWorkspaceDirectories should succeed")
	dir := findBy(t, dirOut.Directories, func(d workspacestypes54.WorkspaceDirectory) bool {
		return aws.ToString(d.Alias) != "" || d.DirectoryId != nil
	}, "a workspace directory")
	require.NotNil(t, dir.DirectoryId)

	wsOut, err := client.DescribeWorkspaces(ctx, &workspacessvc54.DescribeWorkspacesInput{
		DirectoryId: dir.DirectoryId,
	})
	require.NoError(t, err, "DescribeWorkspaces should succeed")
	findBy(t, wsOut.Workspaces, func(w workspacestypes54.Workspace) bool {
		return aws.ToString(w.UserName) == "Administrator"
	}, "a workspace for user Administrator")
}

func verifyAppmeshShieldAndWorkspacesRDS(ctx context.Context, t *testing.T) {
	t.Helper()
	client := rdssvc54.NewFromConfig(megaConfig(t), func(o *rdssvc54.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.DescribeDBEngineVersions(ctx, &rdssvc54.DescribeDBEngineVersionsInput{
		Engine:        aws.String("custom-oracle-ee"),
		EngineVersion: aws.String("19.aswm.1"),
	})
	require.NoError(t, err, "DescribeDBEngineVersions should succeed")
	require.Len(t, out.DBEngineVersions, 1)
	v := out.DBEngineVersions[0]
	assert.Equal(t, "available", aws.ToString(v.Status))
	require.NotNil(t, v.Image, "a completed custom DB engine version must have a non-nil Image "+
		"(terraform-provider-aws dereferences Image.ImageId unconditionally)")
	assert.NotEmpty(t, aws.ToString(v.Image.ImageId))
}

func verifyAppmeshShieldAndWorkspacesKinesisAnalyticsV2(ctx context.Context, t *testing.T) {
	t.Helper()
	client := kinesisanalyticsv2svc54.NewFromConfig(megaConfig(t), func(o *kinesisanalyticsv2svc54.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.ListApplicationSnapshots(ctx, &kinesisanalyticsv2svc54.ListApplicationSnapshotsInput{
		ApplicationName: aws.String("aswm-kda-app"),
	})
	require.NoError(t, err, "ListApplicationSnapshots should succeed")
	found := false
	for _, s := range out.SnapshotSummaries {
		if aws.ToString(s.SnapshotName) == "aswm-snapshot" {
			found = true
		}
	}
	assert.True(t, found, "snapshot aswm-snapshot should be listed")
}

func verifyAppmeshShieldAndWorkspacesDirectoryService(ctx context.Context, t *testing.T) {
	t.Helper()
	client := directoryservicesvc54.NewFromConfig(megaConfig(t), func(o *directoryservicesvc54.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	out, err := client.DescribeTrusts(ctx, &directoryservicesvc54.DescribeTrustsInput{})
	require.NoError(t, err, "DescribeTrusts should succeed")
	trust := findBy(t, out.Trusts, func(tr directoryservicetypes54.Trust) bool {
		return aws.ToString(tr.RemoteDomainName) == "remote-aswm.example.com"
	}, "trust for remote-aswm.example.com")
	assert.Equal(t, directoryservicetypes54.TrustStateVerified, trust.TrustState,
		"a One-Way: Outgoing trust must be auto-verified (there's nothing to verify for One-Way: Incoming only)")
}
