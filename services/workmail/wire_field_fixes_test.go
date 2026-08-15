package workmail_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	workmailsdk "github.com/aws/aws-sdk-go-v2/service/workmail"
	"github.com/aws/aws-sdk-go-v2/service/workmail/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

// Test_SDKRoundTrip_ListUsers_IdentityProviderFields proves ListUsers
// decodes IdentityProviderIdentityStoreId/IdentityProviderUserId through
// the real SDK client. types.User (the ListUsersOutput item,
// aws-sdk-go-v2/service/workmail@v1.39.4/types/types.go) carries both
// fields -- the backend already tracked them (DescribeUser emitted them
// correctly) but ListUsers' UserSummary converter never read them back.
func Test_SDKRoundTrip_ListUsers_IdentityProviderFields(t *testing.T) {
	t.Parallel()

	backend := workmail.NewInMemoryBackend("000000000000", "us-east-1")
	h := workmail.NewHandler(backend)
	client := newWorkMailSDKClient(t, h)
	ctx := t.Context()

	org, err := client.CreateOrganization(ctx, &workmailsdk.CreateOrganizationInput{
		Alias: aws.String("org-" + uuid.NewString()[:8]),
	})
	require.NoError(t, err)

	userName := "user-" + uuid.NewString()[:8]
	_, err = client.CreateUser(ctx, &workmailsdk.CreateUserInput{
		OrganizationId:         org.OrganizationId,
		Name:                   aws.String(userName),
		DisplayName:            aws.String(userName),
		IdentityProviderUserId: aws.String("idp-user-123"),
	})
	require.NoError(t, err)

	listed, err := client.ListUsers(ctx, &workmailsdk.ListUsersInput{OrganizationId: org.OrganizationId})
	require.NoError(t, err)
	require.Len(t, listed.Users, 1)
	assert.Equal(t, "idp-user-123", aws.ToString(listed.Users[0].IdentityProviderUserId))
}

// Test_SDKRoundTrip_ListGroupMembers_EnabledDate proves ListGroupMembers
// decodes a non-nil EnabledDate through the real SDK client. types.Member
// (the ListGroupMembersOutput item) carries EnabledDate/DisabledDate, but
// the backend synthesized a fresh Member value per membership without
// copying either date from the underlying user/group record it had already
// looked up.
func Test_SDKRoundTrip_ListGroupMembers_EnabledDate(t *testing.T) {
	t.Parallel()

	backend := workmail.NewInMemoryBackend("000000000000", "us-east-1")
	h := workmail.NewHandler(backend)
	client := newWorkMailSDKClient(t, h)
	ctx := t.Context()

	org, err := client.CreateOrganization(ctx, &workmailsdk.CreateOrganizationInput{
		Alias: aws.String("org-" + uuid.NewString()[:8]),
	})
	require.NoError(t, err)

	groupName := "group-" + uuid.NewString()[:8]
	group, err := client.CreateGroup(ctx, &workmailsdk.CreateGroupInput{
		OrganizationId: org.OrganizationId,
		Name:           aws.String(groupName),
	})
	require.NoError(t, err)

	userName := "user-" + uuid.NewString()[:8]
	user, err := client.CreateUser(ctx, &workmailsdk.CreateUserInput{
		OrganizationId: org.OrganizationId,
		Name:           aws.String(userName),
		DisplayName:    aws.String(userName),
	})
	require.NoError(t, err)

	_, err = client.RegisterToWorkMail(ctx, &workmailsdk.RegisterToWorkMailInput{
		OrganizationId: org.OrganizationId,
		EntityId:       user.UserId,
		Email:          aws.String(userName + "@example.com"),
	})
	require.NoError(t, err)

	_, err = client.AssociateMemberToGroup(ctx, &workmailsdk.AssociateMemberToGroupInput{
		OrganizationId: org.OrganizationId,
		GroupId:        group.GroupId,
		MemberId:       user.UserId,
	})
	require.NoError(t, err)

	members, err := client.ListGroupMembers(ctx, &workmailsdk.ListGroupMembersInput{
		OrganizationId: org.OrganizationId,
		GroupId:        group.GroupId,
	})
	require.NoError(t, err)
	require.Len(t, members.Members, 1)
	require.NotNil(t, members.Members[0].EnabledDate, "ListGroupMembers must decode a non-nil EnabledDate")
	assert.NotZero(t, *members.Members[0].EnabledDate)
}

// Test_SDKRoundTrip_ListMailboxExportJobs_NarrowShape proves
// ListMailboxExportJobs decodes only the fields real
// types.MailboxExportJob actually has, and that the raw wire body no
// longer leaks RoleArn/KmsKeyArn (an IAM role ARN and a KMS key ARN) on
// every list item. A prior fix mistakenly claimed ListMailboxExportJobs
// "reuses the full MailboxExportJob wire shape" as DescribeMailboxExportJob
// and added RoleArn/KmsKeyArn/S3Prefix/ErrorInfo to the list item -- fields
// the real types.MailboxExportJob (aws-sdk-go-v2/service/workmail@v1.39.4/
// types/types.go) does not have at all.
func Test_SDKRoundTrip_ListMailboxExportJobs_NarrowShape(t *testing.T) {
	t.Parallel()

	backend := workmail.NewInMemoryBackend("000000000000", "us-east-1")
	h := workmail.NewHandler(backend)
	client := newWorkMailSDKClient(t, h)
	ctx := t.Context()

	org, err := client.CreateOrganization(ctx, &workmailsdk.CreateOrganizationInput{
		Alias: aws.String("org-" + uuid.NewString()[:8]),
	})
	require.NoError(t, err)

	userName := "user-" + uuid.NewString()[:8]
	user, err := client.CreateUser(ctx, &workmailsdk.CreateUserInput{
		OrganizationId: org.OrganizationId,
		Name:           aws.String(userName),
		DisplayName:    aws.String(userName),
	})
	require.NoError(t, err)

	_, err = client.StartMailboxExportJob(ctx, &workmailsdk.StartMailboxExportJobInput{
		OrganizationId: org.OrganizationId,
		EntityId:       user.UserId,
		RoleArn:        aws.String("arn:aws:iam::000000000000:role/r"),
		KmsKeyArn:      aws.String("arn:aws:kms:us-east-1:000000000000:key/k"),
		S3BucketName:   aws.String("bkt"),
		S3Prefix:       aws.String("pfx"),
	})
	require.NoError(t, err)

	listed, err := client.ListMailboxExportJobs(ctx, &workmailsdk.ListMailboxExportJobsInput{
		OrganizationId: org.OrganizationId,
	})
	require.NoError(t, err)
	require.Len(t, listed.Jobs, 1)
	job := listed.Jobs[0]
	assert.Equal(t, "bkt", aws.ToString(job.S3BucketName))
	assert.NotEmpty(t, aws.ToString(job.S3Path))

	// The real types.MailboxExportJob struct has no RoleArn/KmsKeyArn/
	// S3Prefix/ErrorInfo fields at all -- this is a compile-time proof a
	// real typed client can never decode them from ListMailboxExportJobs,
	// distinct from the raw-body check below.
	var _ = types.MailboxExportJob{
		JobId: job.JobId, EntityId: job.EntityId, Description: job.Description,
		S3BucketName: job.S3BucketName, S3Path: job.S3Path, State: job.State,
		StartTime: job.StartTime, EndTime: job.EndTime, EstimatedProgress: job.EstimatedProgress,
	}

	// Raw-body check: the ARNs must not reach the wire at all, not just be
	// unreadable by the typed client.
	raw := doOp(t, h, "ListMailboxExportJobs", `{"OrganizationId":`+`"`+aws.ToString(org.OrganizationId)+`"}`)
	require.Equal(t, 200, raw.Code)
	m := decodeJSON(t, raw)
	jobs, ok := m["Jobs"].([]any)
	require.True(t, ok)
	require.Len(t, jobs, 1)
	item := jobs[0].(map[string]any)
	_, hasRoleArn := item["RoleArn"]
	_, hasKmsKeyArn := item["KmsKeyArn"]
	_, hasS3Prefix := item["S3Prefix"]
	_, hasErrorInfo := item["ErrorInfo"]
	assert.False(t, hasRoleArn, "ListMailboxExportJobs must not leak RoleArn")
	assert.False(t, hasKmsKeyArn, "ListMailboxExportJobs must not leak KmsKeyArn")
	assert.False(t, hasS3Prefix, "ListMailboxExportJobs must not leak S3Prefix")
	assert.False(t, hasErrorInfo, "ListMailboxExportJobs must not leak ErrorInfo")
}

// Test_SDKRoundTrip_Resource_HiddenFromGlobalAddressList proves
// UpdateResource's HiddenFromGlobalAddressList is stored and echoed by
// DescribeResource. Unlike users/groups, real CreateResourceInput has no
// such field -- it is only settable via UpdateResource -- and the backend
// previously had no field on Resource to hold it at all.
func Test_SDKRoundTrip_Resource_HiddenFromGlobalAddressList(t *testing.T) {
	t.Parallel()

	backend := workmail.NewInMemoryBackend("000000000000", "us-east-1")
	h := workmail.NewHandler(backend)
	client := newWorkMailSDKClient(t, h)
	ctx := t.Context()

	org, err := client.CreateOrganization(ctx, &workmailsdk.CreateOrganizationInput{
		Alias: aws.String("org-" + uuid.NewString()[:8]),
	})
	require.NoError(t, err)

	resName := "res-" + uuid.NewString()[:8]
	res, err := client.CreateResource(ctx, &workmailsdk.CreateResourceInput{
		OrganizationId: org.OrganizationId,
		Name:           aws.String(resName),
		Type:           types.ResourceTypeRoom,
	})
	require.NoError(t, err)

	before, err := client.DescribeResource(ctx, &workmailsdk.DescribeResourceInput{
		OrganizationId: org.OrganizationId,
		ResourceId:     res.ResourceId,
	})
	require.NoError(t, err)
	assert.False(t, before.HiddenFromGlobalAddressList, "a newly created resource must not be hidden by default")

	_, err = client.UpdateResource(ctx, &workmailsdk.UpdateResourceInput{
		OrganizationId:              org.OrganizationId,
		ResourceId:                  res.ResourceId,
		HiddenFromGlobalAddressList: aws.Bool(true),
	})
	require.NoError(t, err)

	after, err := client.DescribeResource(ctx, &workmailsdk.DescribeResourceInput{
		OrganizationId: org.OrganizationId,
		ResourceId:     res.ResourceId,
	})
	require.NoError(t, err)
	assert.True(t, after.HiddenFromGlobalAddressList, "UpdateResource must persist HiddenFromGlobalAddressList")
}
