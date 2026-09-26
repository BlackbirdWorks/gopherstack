package terraform_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	configsvc "github.com/aws/aws-sdk-go-v2/service/configservice"
	identitystoresvc "github.com/aws/aws-sdk-go-v2/service/identitystore"
	lightsailsvc "github.com/aws/aws-sdk-go-v2/service/lightsail"
	ssoadminsvc "github.com/aws/aws-sdk-go-v2/service/ssoadmin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_SsoadminConfigAndLightsail provisions Lightsail (instance + public ports,
// disk + attachment, static IP + attachment, bucket + access key + resource
// access, container service + deployment version, distribution,
// certificate, domain + entry, database, load balancer + attachment +
// HTTPS redirection policy + stickiness policy + certificate + certificate
// attachment), SSO Admin (permission set + inline policy, managed/customer-
// managed policy attachments, permissions boundary attachment, account
// assignment, instance access control attributes, application + access
// scope + assignment configuration + assignment, trusted token issuer), and
// AWS Config (configuration recorder status, config rule, remediation
// configuration, retention configuration, configuration aggregator,
// aggregate authorization, conformance pack, organization custom/managed
// rule, organization conformance pack) resources via Terraform and verifies
// each through its own SDK client's Describe/Get/List path.
func TestTerraform_SsoadminConfigAndLightsail(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "ssoadmin-config-and-lightsail",
			setup: func(t *testing.T, dir string) map[string]any {
				t.Helper()

				functionZip := filepath.Join(dir, "ssol-config-custom-rule.zip")
				writeZipFixture(t, functionZip, "index.py",
					"def handler(event, context):\n    return {}\n")

				return map[string]any{
					"FunctionZip": functionZip,
				}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifySsoadminConfigAndLightsailLightsail(ctx, t)
				verifySsoadminConfigAndLightsailSSOAdmin(ctx, t)
				verifySsoadminConfigAndLightsailAWSConfig(ctx, t)
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

func createLightsailClient(t *testing.T) *lightsailsvc.Client {
	t.Helper()

	cfg := megaConfig(t)

	return lightsailsvc.NewFromConfig(cfg, func(o *lightsailsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func createSSOAdminClient(t *testing.T) *ssoadminsvc.Client {
	t.Helper()

	cfg := megaConfig(t)

	return ssoadminsvc.NewFromConfig(cfg, func(o *ssoadminsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func verifySsoadminConfigAndLightsailLightsail(ctx context.Context, t *testing.T) {
	t.Helper()

	client := createLightsailClient(t)

	instOut, err := client.GetInstance(ctx, &lightsailsvc.GetInstanceInput{
		InstanceName: aws.String("ssol-instance"),
	})
	require.NoError(t, err, "GetInstance should succeed")
	require.NotNil(t, instOut.Instance)
	assert.NotEmpty(t, instOut.Instance.Networking.Ports, "instance public ports should be set")

	diskOut, err := client.GetDisk(ctx, &lightsailsvc.GetDiskInput{DiskName: aws.String("ssol-disk")})
	require.NoError(t, err, "GetDisk should succeed")
	require.NotNil(t, diskOut.Disk)
	assert.True(t, aws.ToBool(diskOut.Disk.IsAttached), "disk should be attached")

	staticIPOut, err := client.GetStaticIp(ctx, &lightsailsvc.GetStaticIpInput{
		StaticIpName: aws.String("ssol-static-ip"),
	})
	require.NoError(t, err, "GetStaticIp should succeed")
	require.NotNil(t, staticIPOut.StaticIp)
	assert.True(t, aws.ToBool(staticIPOut.StaticIp.IsAttached), "static IP should be attached")

	bucketOut, err := client.GetBuckets(ctx, &lightsailsvc.GetBucketsInput{
		BucketName: aws.String("ssol-bucket"),
	})
	require.NoError(t, err, "GetBuckets should succeed")
	require.Len(t, bucketOut.Buckets, 1)
	require.Len(t, bucketOut.Buckets[0].ResourcesReceivingAccess, 1)
	assert.Equal(t, "ssol-container", aws.ToString(bucketOut.Buckets[0].ResourcesReceivingAccess[0].Name))

	keysOut, err := client.GetBucketAccessKeys(ctx, &lightsailsvc.GetBucketAccessKeysInput{
		BucketName: aws.String("ssol-bucket"),
	})
	require.NoError(t, err, "GetBucketAccessKeys should succeed")
	assert.Len(t, keysOut.AccessKeys, 1)

	csOut, err := client.GetContainerServices(ctx, &lightsailsvc.GetContainerServicesInput{
		ServiceName: aws.String("ssol-container"),
	})
	require.NoError(t, err, "GetContainerServices should succeed")
	require.Len(t, csOut.ContainerServices, 1)

	require.Eventually(t, func() bool {
		in := &lightsailsvc.GetContainerServiceDeploymentsInput{ServiceName: aws.String("ssol-container")}
		depsOut, depsErr := client.GetContainerServiceDeployments(ctx, in)

		return depsErr == nil && len(depsOut.Deployments) > 0
	}, 5*time.Second, 50*time.Millisecond, "a container service deployment should exist")

	distOut, err := client.GetDistributions(ctx, &lightsailsvc.GetDistributionsInput{
		DistributionName: aws.String("ssol-distribution"),
	})
	require.NoError(t, err, "GetDistributions should succeed")
	require.Len(t, distOut.Distributions, 1)
	assert.Equal(t, "ssol-instance", aws.ToString(distOut.Distributions[0].Origin.Name))

	certOut, err := client.GetCertificates(ctx, &lightsailsvc.GetCertificatesInput{
		CertificateName: aws.String("ssol-cert"),
	})
	require.NoError(t, err, "GetCertificates should succeed")
	require.Len(t, certOut.Certificates, 1)

	domOut, err := client.GetDomain(ctx, &lightsailsvc.GetDomainInput{
		DomainName: aws.String("ssol-domain.com"),
	})
	require.NoError(t, err, "GetDomain should succeed")
	require.NotNil(t, domOut.Domain)
	require.Len(t, domOut.Domain.DomainEntries, 1)
	assert.Equal(t, "www.ssol-domain.com", aws.ToString(domOut.Domain.DomainEntries[0].Name))

	dbOut, err := client.GetRelationalDatabase(ctx, &lightsailsvc.GetRelationalDatabaseInput{
		RelationalDatabaseName: aws.String("ssol-db"),
	})
	require.NoError(t, err, "GetRelationalDatabase should succeed")
	require.NotNil(t, dbOut.RelationalDatabase)
	assert.Equal(t, "ssol", aws.ToString(dbOut.RelationalDatabase.MasterDatabaseName))

	lbOut, err := client.GetLoadBalancer(ctx, &lightsailsvc.GetLoadBalancerInput{
		LoadBalancerName: aws.String("ssol-lb"),
	})
	require.NoError(t, err, "GetLoadBalancer should succeed")
	require.NotNil(t, lbOut.LoadBalancer)
	require.Len(t, lbOut.LoadBalancer.InstanceHealthSummary, 1)
	assert.Equal(t, "ssol-instance", aws.ToString(lbOut.LoadBalancer.InstanceHealthSummary[0].InstanceName))
	assert.True(t, aws.ToBool(lbOut.LoadBalancer.HttpsRedirectionEnabled), "HTTPS redirection should be enabled")

	lbCertOut, err := client.GetLoadBalancerTlsCertificates(ctx, &lightsailsvc.GetLoadBalancerTlsCertificatesInput{
		LoadBalancerName: aws.String("ssol-lb"),
	})
	require.NoError(t, err, "GetLoadBalancerTlsCertificates should succeed")
	require.Len(t, lbCertOut.TlsCertificates, 1)
	assert.True(t, aws.ToBool(lbCertOut.TlsCertificates[0].IsAttached), "LB TLS certificate should be attached")
}

func verifySsoadminConfigAndLightsailSSOAdmin(ctx context.Context, t *testing.T) {
	t.Helper()

	ssoClient := createSSOAdminClient(t)

	instancesOut, err := ssoClient.ListInstances(ctx, &ssoadminsvc.ListInstancesInput{})
	require.NoError(t, err, "ListInstances should succeed")
	require.NotEmpty(t, instancesOut.Instances)
	instanceArn := aws.ToString(instancesOut.Instances[0].InstanceArn)

	psOut, err := ssoClient.ListPermissionSets(ctx, &ssoadminsvc.ListPermissionSetsInput{
		InstanceArn: aws.String(instanceArn),
	})
	require.NoError(t, err, "ListPermissionSets should succeed")

	var permissionSetArn string

	for _, arn := range psOut.PermissionSets {
		desc, descErr := ssoClient.DescribePermissionSet(ctx, &ssoadminsvc.DescribePermissionSetInput{
			InstanceArn:      aws.String(instanceArn),
			PermissionSetArn: aws.String(arn),
		})
		if descErr == nil && aws.ToString(desc.PermissionSet.Name) == "ssol-permission-set" {
			permissionSetArn = arn
		}
	}

	require.NotEmpty(t, permissionSetArn, "ssol permission set should be listed")

	inlineOut, err := ssoClient.GetInlinePolicyForPermissionSet(ctx, &ssoadminsvc.GetInlinePolicyForPermissionSetInput{
		InstanceArn:      aws.String(instanceArn),
		PermissionSetArn: aws.String(permissionSetArn),
	})
	require.NoError(t, err, "GetInlinePolicyForPermissionSet should succeed")
	assert.Contains(t, aws.ToString(inlineOut.InlinePolicy), "s3:ListAllMyBuckets")

	mpOut, err := ssoClient.ListManagedPoliciesInPermissionSet(
		ctx,
		&ssoadminsvc.ListManagedPoliciesInPermissionSetInput{
			InstanceArn:      aws.String(instanceArn),
			PermissionSetArn: aws.String(permissionSetArn),
		},
	)
	require.NoError(t, err, "ListManagedPoliciesInPermissionSet should succeed")
	assert.Len(t, mpOut.AttachedManagedPolicies, 1)

	cmpOut, err := ssoClient.ListCustomerManagedPolicyReferencesInPermissionSet(ctx,
		&ssoadminsvc.ListCustomerManagedPolicyReferencesInPermissionSetInput{
			InstanceArn:      aws.String(instanceArn),
			PermissionSetArn: aws.String(permissionSetArn),
		})
	require.NoError(t, err, "ListCustomerManagedPolicyReferencesInPermissionSet should succeed")
	require.Len(t, cmpOut.CustomerManagedPolicyReferences, 1)
	assert.Equal(t, "ssol-cmp", aws.ToString(cmpOut.CustomerManagedPolicyReferences[0].Name))

	pbOut, err := ssoClient.GetPermissionsBoundaryForPermissionSet(ctx,
		&ssoadminsvc.GetPermissionsBoundaryForPermissionSetInput{
			InstanceArn:      aws.String(instanceArn),
			PermissionSetArn: aws.String(permissionSetArn),
		})
	require.NoError(t, err, "GetPermissionsBoundaryForPermissionSet should succeed")
	require.NotNil(t, pbOut.PermissionsBoundary)

	isClient := createIdentityStoreClient(t)

	instOut, err := ssoClient.ListInstances(ctx, &ssoadminsvc.ListInstancesInput{})
	require.NoError(t, err, "ListInstances should succeed")
	identityStoreID := aws.ToString(instOut.Instances[0].IdentityStoreId)

	groupsOut, err := isClient.ListGroups(ctx, &identitystoresvc.ListGroupsInput{
		IdentityStoreId: aws.String(identityStoreID),
	})
	require.NoError(t, err, "ListGroups should succeed")

	var groupID string

	for _, g := range groupsOut.Groups {
		if aws.ToString(g.DisplayName) == "ssol-group" {
			groupID = aws.ToString(g.GroupId)
		}
	}

	require.NotEmpty(t, groupID, "ssol group should be listed")

	assignmentsOut, err := ssoClient.ListAccountAssignments(ctx, &ssoadminsvc.ListAccountAssignmentsInput{
		InstanceArn:      aws.String(instanceArn),
		PermissionSetArn: aws.String(permissionSetArn),
		AccountId:        aws.String("000000000000"),
	})
	require.NoError(t, err, "ListAccountAssignments should succeed")

	var foundAssignment bool

	for _, a := range assignmentsOut.AccountAssignments {
		if aws.ToString(a.PrincipalId) == groupID {
			foundAssignment = true
		}
	}

	assert.True(t, foundAssignment, "account assignment for the group should be listed")

	acaOut, err := ssoClient.DescribeInstanceAccessControlAttributeConfiguration(ctx,
		&ssoadminsvc.DescribeInstanceAccessControlAttributeConfigurationInput{
			InstanceArn: aws.String(instanceArn),
		})
	require.NoError(t, err, "DescribeInstanceAccessControlAttributeConfiguration should succeed")
	require.NotNil(t, acaOut.InstanceAccessControlAttributeConfiguration)
	require.Len(t, acaOut.InstanceAccessControlAttributeConfiguration.AccessControlAttributes, 1)
	assert.Equal(t, "department",
		aws.ToString(acaOut.InstanceAccessControlAttributeConfiguration.AccessControlAttributes[0].Key))

	appsOut, err := ssoClient.ListApplications(ctx, &ssoadminsvc.ListApplicationsInput{
		InstanceArn: aws.String(instanceArn),
	})
	require.NoError(t, err, "ListApplications should succeed")

	var applicationArn string

	for _, a := range appsOut.Applications {
		if aws.ToString(a.Name) == "ssol-application" {
			applicationArn = aws.ToString(a.ApplicationArn)
		}
	}

	require.NotEmpty(t, applicationArn, "ssol application should be listed")

	scopesOut, err := ssoClient.ListApplicationAccessScopes(ctx, &ssoadminsvc.ListApplicationAccessScopesInput{
		ApplicationArn: aws.String(applicationArn),
	})
	require.NoError(t, err, "ListApplicationAccessScopes should succeed")
	require.Len(t, scopesOut.Scopes, 1)
	assert.Equal(t, "sso:account:access", aws.ToString(scopesOut.Scopes[0].Scope))

	assignCfgOut, err := ssoClient.GetApplicationAssignmentConfiguration(ctx,
		&ssoadminsvc.GetApplicationAssignmentConfigurationInput{ApplicationArn: aws.String(applicationArn)})
	require.NoError(t, err, "GetApplicationAssignmentConfiguration should succeed")
	assert.True(t, aws.ToBool(assignCfgOut.AssignmentRequired))

	appAssignOut, err := ssoClient.ListApplicationAssignments(ctx, &ssoadminsvc.ListApplicationAssignmentsInput{
		ApplicationArn: aws.String(applicationArn),
	})
	require.NoError(t, err, "ListApplicationAssignments should succeed")

	var foundAppAssignment bool

	for _, a := range appAssignOut.ApplicationAssignments {
		if aws.ToString(a.PrincipalId) == groupID {
			foundAppAssignment = true
		}
	}

	assert.True(t, foundAppAssignment, "application assignment for the group should be listed")

	ttiOut, err := ssoClient.ListTrustedTokenIssuers(ctx, &ssoadminsvc.ListTrustedTokenIssuersInput{
		InstanceArn: aws.String(instanceArn),
	})
	require.NoError(t, err, "ListTrustedTokenIssuers should succeed")

	var foundTTI bool

	for _, tti := range ttiOut.TrustedTokenIssuers {
		if aws.ToString(tti.Name) == "ssol-tti" {
			foundTTI = true
		}
	}

	assert.True(t, foundTTI, "trusted token issuer should be listed")
}

func verifySsoadminConfigAndLightsailAWSConfig(ctx context.Context, t *testing.T) {
	t.Helper()

	client := createAWSConfigClient(t)

	statusOut, err := client.DescribeConfigurationRecorderStatus(ctx,
		&configsvc.DescribeConfigurationRecorderStatusInput{
			ConfigurationRecorderNames: []string{"ssol-recorder"},
		})
	require.NoError(t, err, "DescribeConfigurationRecorderStatus should succeed")
	require.Len(t, statusOut.ConfigurationRecordersStatus, 1)
	assert.True(t, statusOut.ConfigurationRecordersStatus[0].Recording)

	ruleOut, err := client.DescribeConfigRules(ctx, &configsvc.DescribeConfigRulesInput{
		ConfigRuleNames: []string{"ssol-config-rule"},
	})
	require.NoError(t, err, "DescribeConfigRules should succeed")
	require.Len(t, ruleOut.ConfigRules, 1)

	remOut, err := client.DescribeRemediationConfigurations(ctx, &configsvc.DescribeRemediationConfigurationsInput{
		ConfigRuleNames: []string{"ssol-config-rule"},
	})
	require.NoError(t, err, "DescribeRemediationConfigurations should succeed")
	require.Len(t, remOut.RemediationConfigurations, 1)
	assert.Equal(t, "AWS-EnableS3BucketEncryption", aws.ToString(remOut.RemediationConfigurations[0].TargetId))

	retOut, err := client.DescribeRetentionConfigurations(ctx, &configsvc.DescribeRetentionConfigurationsInput{})
	require.NoError(t, err, "DescribeRetentionConfigurations should succeed")
	require.Len(t, retOut.RetentionConfigurations, 1)
	assert.Equal(t, int32(90), aws.ToInt32(retOut.RetentionConfigurations[0].RetentionPeriodInDays))

	aggOut, err := client.DescribeConfigurationAggregators(ctx, &configsvc.DescribeConfigurationAggregatorsInput{
		ConfigurationAggregatorNames: []string{"ssol-aggregator"},
	})
	require.NoError(t, err, "DescribeConfigurationAggregators should succeed")
	require.Len(t, aggOut.ConfigurationAggregators, 1)

	authOut, err := client.DescribeAggregationAuthorizations(ctx, &configsvc.DescribeAggregationAuthorizationsInput{})
	require.NoError(t, err, "DescribeAggregationAuthorizations should succeed")

	var foundAuth bool

	for _, a := range authOut.AggregationAuthorizations {
		if aws.ToString(a.AuthorizedAccountId) == "111111111111" {
			foundAuth = true
		}
	}

	assert.True(t, foundAuth, "aggregate authorization for 111111111111 should be listed")

	cpOut, err := client.DescribeConformancePacks(ctx, &configsvc.DescribeConformancePacksInput{
		ConformancePackNames: []string{"ssol-conformance-pack"},
	})
	require.NoError(t, err, "DescribeConformancePacks should succeed")
	require.Len(t, cpOut.ConformancePackDetails, 1)

	orgRulesOut, err := client.DescribeOrganizationConfigRules(ctx, &configsvc.DescribeOrganizationConfigRulesInput{
		OrganizationConfigRuleNames: []string{"ssol-org-custom-rule", "ssol-org-managed-rule"},
	})
	require.NoError(t, err, "DescribeOrganizationConfigRules should succeed")
	assert.Len(t, orgRulesOut.OrganizationConfigRules, 2)

	orgCPOut, err := client.DescribeOrganizationConformancePacks(ctx,
		&configsvc.DescribeOrganizationConformancePacksInput{
			OrganizationConformancePackNames: []string{"ssol-org-conformance-pack"},
		})
	require.NoError(t, err, "DescribeOrganizationConformancePacks should succeed")
	require.Len(t, orgCPOut.OrganizationConformancePacks, 1)
}
