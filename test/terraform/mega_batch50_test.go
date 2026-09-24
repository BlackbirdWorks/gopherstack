package terraform_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	detectivesvc50 "github.com/aws/aws-sdk-go-v2/service/detective"
	efssvc50 "github.com/aws/aws-sdk-go-v2/service/efs"
	elasticachesvc50 "github.com/aws/aws-sdk-go-v2/service/elasticache"
	iamsvc50 "github.com/aws/aws-sdk-go-v2/service/iam"
	iamsvc50types "github.com/aws/aws-sdk-go-v2/service/iam/types"
	s3svc50 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch50 provisions: IAM "*_exclusive" inline-policy and
// policy-attachment reconciliation resources (each with its own role/group/
// user), a legacy policy attachment, a server certificate, a signing
// certificate, a user/group membership, and STS preferences; a Detective
// graph with a member invitation, an organization admin account, and an
// organization configuration; an S3 bucket-to-bucket replication pair, an
// object-lock bucket, and an in-place object copy; an EFS file system
// replicated same-region; and an ElastiCache replication group
// promoted to a global replication group -- via Terraform, verifying each
// through its own SDK client.
func TestTerraform_MegaBatch50(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-50",
			setup:   setupEndpoint,
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyMegaBatch50IAM(ctx, t)
				verifyMegaBatch50Detective(ctx, t)
				verifyMegaBatch50S3(ctx, t)
				verifyMegaBatch50EFS(ctx, t)
				verifyMegaBatch50ElastiCache(ctx, t)
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

func verifyMegaBatch50IAM(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := iamsvc50.NewFromConfig(cfg, func(o *iamsvc50.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	rolePoliciesOut, err := client.ListRolePolicies(ctx, &iamsvc50.ListRolePoliciesInput{
		RoleName: aws.String("mega-batch-50-role-inline-excl"),
	})
	require.NoError(t, err, "ListRolePolicies should succeed")
	assert.Contains(t, rolePoliciesOut.PolicyNames, "mega-batch-50-role-inline-policy")

	attachedRolePoliciesOut, err := client.ListAttachedRolePolicies(ctx, &iamsvc50.ListAttachedRolePoliciesInput{
		RoleName: aws.String("mega-batch-50-role-attach-excl"),
	})
	require.NoError(t, err, "ListAttachedRolePolicies should succeed")
	require.Len(t, attachedRolePoliciesOut.AttachedPolicies, 1)
	assert.Equal(
		t,
		"mega-batch-50-role-attach-policy",
		aws.ToString(attachedRolePoliciesOut.AttachedPolicies[0].PolicyName),
	)

	groupPoliciesOut, err := client.ListGroupPolicies(ctx, &iamsvc50.ListGroupPoliciesInput{
		GroupName: aws.String("mega-batch-50-group-inline-excl"),
	})
	require.NoError(t, err, "ListGroupPolicies should succeed")
	assert.Contains(t, groupPoliciesOut.PolicyNames, "mega-batch-50-group-inline-policy")

	attachedGroupPoliciesOut, err := client.ListAttachedGroupPolicies(ctx, &iamsvc50.ListAttachedGroupPoliciesInput{
		GroupName: aws.String("mega-batch-50-group-attach-excl"),
	})
	require.NoError(t, err, "ListAttachedGroupPolicies should succeed")
	require.Len(t, attachedGroupPoliciesOut.AttachedPolicies, 1)
	assert.Equal(
		t,
		"mega-batch-50-group-attach-policy",
		aws.ToString(attachedGroupPoliciesOut.AttachedPolicies[0].PolicyName),
	)

	userPoliciesOut, err := client.ListUserPolicies(ctx, &iamsvc50.ListUserPoliciesInput{
		UserName: aws.String("mega-batch-50-user-inline-excl"),
	})
	require.NoError(t, err, "ListUserPolicies should succeed")
	assert.Contains(t, userPoliciesOut.PolicyNames, "mega-batch-50-user-inline-policy")

	attachedUserPoliciesOut, err := client.ListAttachedUserPolicies(ctx, &iamsvc50.ListAttachedUserPoliciesInput{
		UserName: aws.String("mega-batch-50-user-attach-excl"),
	})
	require.NoError(t, err, "ListAttachedUserPolicies should succeed")
	require.Len(t, attachedUserPoliciesOut.AttachedPolicies, 1)
	assert.Equal(
		t,
		"mega-batch-50-user-attach-policy",
		aws.ToString(attachedUserPoliciesOut.AttachedPolicies[0].PolicyName),
	)

	legacyAttachedOut, err := client.ListAttachedUserPolicies(ctx, &iamsvc50.ListAttachedUserPoliciesInput{
		UserName: aws.String("mega-batch-50-legacy-attach-user"),
	})
	require.NoError(t, err, "ListAttachedUserPolicies(legacy) should succeed")
	require.Len(t, legacyAttachedOut.AttachedPolicies, 1)
	assert.Equal(
		t,
		"mega-batch-50-legacy-attach-policy",
		aws.ToString(legacyAttachedOut.AttachedPolicies[0].PolicyName),
	)

	certOut, err := client.GetServerCertificate(ctx, &iamsvc50.GetServerCertificateInput{
		ServerCertificateName: aws.String("mega-batch-50-server-cert"),
	})
	require.NoError(t, err, "GetServerCertificate should succeed")
	require.NotNil(t, certOut.ServerCertificate)
	assert.Equal(
		t,
		"mega-batch-50-server-cert",
		aws.ToString(certOut.ServerCertificate.ServerCertificateMetadata.ServerCertificateName),
	)

	signingCertsOut, err := client.ListSigningCertificates(ctx, &iamsvc50.ListSigningCertificatesInput{
		UserName: aws.String("mega-batch-50-signing-user"),
	})
	require.NoError(t, err, "ListSigningCertificates should succeed")
	require.Len(t, signingCertsOut.Certificates, 1)
	assert.Equal(t, iamsvc50types.StatusTypeActive, signingCertsOut.Certificates[0].Status)

	groupsForUserOut, err := client.ListGroupsForUser(ctx, &iamsvc50.ListGroupsForUserInput{
		UserName: aws.String("mega-batch-50-group-membership-user"),
	})
	require.NoError(t, err, "ListGroupsForUser should succeed")
	require.Len(t, groupsForUserOut.Groups, 2)
}

func verifyMegaBatch50Detective(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := detectivesvc50.NewFromConfig(cfg, func(o *detectivesvc50.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	graphsOut, err := client.ListGraphs(ctx, &detectivesvc50.ListGraphsInput{})
	require.NoError(t, err, "ListGraphs should succeed")
	require.NotEmpty(t, graphsOut.GraphList, "at least one detective graph should be listed")

	var graphArn string
	for _, g := range graphsOut.GraphList {
		graphArn = aws.ToString(g.Arn)
	}
	require.NotEmpty(t, graphArn)

	membersOut, err := client.ListMembers(ctx, &detectivesvc50.ListMembersInput{GraphArn: aws.String(graphArn)})
	require.NoError(t, err, "ListMembers should succeed")

	var foundMember bool
	for _, m := range membersOut.MemberDetails {
		if aws.ToString(m.AccountId) == "444455556666" {
			foundMember = true
		}
	}
	assert.True(t, foundMember, "mega-batch-50 detective member should be listed")

	adminsOut, err := client.ListOrganizationAdminAccounts(ctx, &detectivesvc50.ListOrganizationAdminAccountsInput{})
	require.NoError(t, err, "ListOrganizationAdminAccounts should succeed")

	var foundAdmin bool
	for _, a := range adminsOut.Administrators {
		if aws.ToString(a.AccountId) == "444455556677" {
			foundAdmin = true
		}
	}
	assert.True(t, foundAdmin, "mega-batch-50 detective org admin account should be listed")
}

func verifyMegaBatch50S3(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := s3svc50.NewFromConfig(cfg, func(o *s3svc50.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	replOut, err := client.GetBucketReplication(ctx, &s3svc50.GetBucketReplicationInput{
		Bucket: aws.String("mega-batch-50-repl-src"),
	})
	require.NoError(t, err, "GetBucketReplication should succeed")
	require.NotNil(t, replOut.ReplicationConfiguration)
	require.Len(t, replOut.ReplicationConfiguration.Rules, 1)
	assert.Equal(
		t,
		"arn:aws:s3:::mega-batch-50-repl-dst",
		aws.ToString(replOut.ReplicationConfiguration.Rules[0].Destination.Bucket),
	)

	lockOut, err := client.GetObjectLockConfiguration(ctx, &s3svc50.GetObjectLockConfigurationInput{
		Bucket: aws.String("mega-batch-50-object-lock"),
	})
	require.NoError(t, err, "GetObjectLockConfiguration should succeed")
	require.NotNil(t, lockOut.ObjectLockConfiguration)
	require.NotNil(t, lockOut.ObjectLockConfiguration.Rule)
	require.NotNil(t, lockOut.ObjectLockConfiguration.Rule.DefaultRetention)
	assert.EqualValues(t, 5, aws.ToInt32(lockOut.ObjectLockConfiguration.Rule.DefaultRetention.Days))

	copyOut, err := client.GetObject(ctx, &s3svc50.GetObjectInput{
		Bucket: aws.String("mega-batch-50-copy-bucket"),
		Key:    aws.String("dest/copied.txt"),
	})
	require.NoError(t, err, "GetObject(copy) should succeed")
	defer copyOut.Body.Close()
}

func verifyMegaBatch50EFS(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := efssvc50.NewFromConfig(cfg, func(o *efssvc50.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	fsOut, err := client.DescribeFileSystems(ctx, &efssvc50.DescribeFileSystemsInput{
		CreationToken: aws.String("mega-batch-50-efs-repl-src"),
	})
	require.NoError(t, err, "DescribeFileSystems should succeed")
	require.Len(t, fsOut.FileSystems, 1)
	srcID := aws.ToString(fsOut.FileSystems[0].FileSystemId)

	replOut, err := client.DescribeReplicationConfigurations(ctx, &efssvc50.DescribeReplicationConfigurationsInput{
		FileSystemId: aws.String(srcID),
	})
	require.NoError(t, err, "DescribeReplicationConfigurations should succeed")
	require.Len(t, replOut.Replications, 1)
	require.Len(t, replOut.Replications[0].Destinations, 1)
}

func verifyMegaBatch50ElastiCache(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := elasticachesvc50.NewFromConfig(cfg, func(o *elasticachesvc50.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	globalOut, err := client.DescribeGlobalReplicationGroups(
		ctx,
		&elasticachesvc50.DescribeGlobalReplicationGroupsInput{},
	)
	require.NoError(t, err, "DescribeGlobalReplicationGroups should succeed")

	var found bool
	for _, g := range globalOut.GlobalReplicationGroups {
		if strings.HasSuffix(aws.ToString(g.GlobalReplicationGroupId), "mb50-global") {
			found = true
		}
	}
	assert.True(t, found, "mega-batch-50 global replication group should be listed")
}
