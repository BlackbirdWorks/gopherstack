package cleanrooms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	crtypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReqFieldDiffSlice4_CreateCollaboration drives CreateCollaboration's
// CreatorPaymentConfiguration/IsMetricsEnabled/JobLogStatus (all restjson1
// body fields, confirmed against
// awsRestjson1_serializeOpDocumentCreateCollaborationInput -- none are
// httpQuery/Label/Header) through the real SDK client and asserts their
// observable effect via GetCollaboration and GetMembership on the
// collaboration's own auto-created membership.
func TestReqFieldDiffSlice4_CreateCollaboration(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	colOut, err := client.CreateCollaboration(ctx, &cleanroomssdk.CreateCollaborationInput{
		Name:                   aws.String("slice4-collab"),
		CreatorDisplayName:     aws.String("creator"),
		CreatorMemberAbilities: []crtypes.MemberAbility{crtypes.MemberAbilityCanQuery},
		Members:                []crtypes.MemberSpecification{},
		QueryLogStatus:         crtypes.CollaborationQueryLogStatusDisabled,
		JobLogStatus:           crtypes.CollaborationJobLogStatusEnabled,
		IsMetricsEnabled:       aws.Bool(true),
		CreatorPaymentConfiguration: &crtypes.PaymentConfiguration{
			QueryCompute: &crtypes.QueryComputePaymentConfig{IsResponsible: aws.Bool(false)},
		},
	})
	require.NoError(t, err)

	assert.Equal(t, crtypes.CollaborationJobLogStatusEnabled, colOut.Collaboration.JobLogStatus)
	assert.True(t, aws.ToBool(colOut.Collaboration.IsMetricsEnabled))

	getColOut, err := client.GetCollaboration(ctx, &cleanroomssdk.GetCollaborationInput{
		CollaborationIdentifier: colOut.Collaboration.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, crtypes.CollaborationJobLogStatusEnabled, getColOut.Collaboration.JobLogStatus)
	assert.True(t, aws.ToBool(getColOut.Collaboration.IsMetricsEnabled))

	memOut, err := client.GetMembership(ctx, &cleanroomssdk.GetMembershipInput{
		MembershipIdentifier: colOut.Collaboration.MembershipId,
	})
	require.NoError(t, err)
	require.NotNil(t, memOut.Membership.PaymentConfiguration)
	require.NotNil(t, memOut.Membership.PaymentConfiguration.QueryCompute)
	assert.False(t, aws.ToBool(memOut.Membership.PaymentConfiguration.QueryCompute.IsResponsible))
}

// TestReqFieldDiffSlice4_CreateMembership drives CreateMembership's
// DefaultJobResultConfiguration/IsMetricsEnabled/JobLogStatus through the
// real SDK client and asserts the round trip via GetMembership.
// DefaultJobResultConfiguration is a distinct wire field from the
// pre-existing DefaultResultConfiguration (both are separate members on
// CreateMembershipInput/types.Membership).
func TestReqFieldDiffSlice4_CreateMembership(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()
	collabID, _ := createCollaborationAndMembership(t, client)

	createOut, err := client.CreateMembership(ctx, &cleanroomssdk.CreateMembershipInput{
		CollaborationIdentifier: aws.String(collabID),
		QueryLogStatus:          crtypes.MembershipQueryLogStatusDisabled,
		JobLogStatus:            crtypes.MembershipJobLogStatusEnabled,
		IsMetricsEnabled:        aws.Bool(true),
		DefaultJobResultConfiguration: &crtypes.MembershipProtectedJobResultConfiguration{
			RoleArn: aws.String("arn:aws:iam::111122223333:role/job-result"),
			OutputConfiguration: &crtypes.MembershipProtectedJobOutputConfigurationMemberS3{
				Value: crtypes.ProtectedJobS3OutputConfigurationInput{
					Bucket:    aws.String("job-result-bucket"),
					KeyPrefix: aws.String("prefix/"),
				},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, crtypes.MembershipJobLogStatusEnabled, createOut.Membership.JobLogStatus)
	assert.True(t, aws.ToBool(createOut.Membership.IsMetricsEnabled))
	require.NotNil(t, createOut.Membership.DefaultJobResultConfiguration)
	assert.Equal(
		t,
		"arn:aws:iam::111122223333:role/job-result",
		aws.ToString(createOut.Membership.DefaultJobResultConfiguration.RoleArn),
	)

	memID := createOut.Membership.Id

	getOut, err := client.GetMembership(ctx, &cleanroomssdk.GetMembershipInput{MembershipIdentifier: memID})
	require.NoError(t, err)
	assert.Equal(t, crtypes.MembershipJobLogStatusEnabled, getOut.Membership.JobLogStatus)
	assert.True(t, aws.ToBool(getOut.Membership.IsMetricsEnabled))
	require.NotNil(t, getOut.Membership.DefaultJobResultConfiguration)
	config := getOut.Membership.DefaultJobResultConfiguration
	s3Out, ok := config.OutputConfiguration.(*crtypes.MembershipProtectedJobOutputConfigurationMemberS3)
	require.True(t, ok)
	assert.Equal(t, "job-result-bucket", aws.ToString(s3Out.Value.Bucket))
}

// TestReqFieldDiffSlice4_UpdateMembership drives UpdateMembership's
// DefaultJobResultConfiguration/JobLogStatus through the real SDK client and
// asserts the round trip via GetMembership, confirming the update actually
// mutates existing membership state rather than being silently dropped.
func TestReqFieldDiffSlice4_UpdateMembership(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()
	_, memID := createCollaborationAndMembership(t, client)

	updOut, err := client.UpdateMembership(ctx, &cleanroomssdk.UpdateMembershipInput{
		MembershipIdentifier: aws.String(memID),
		JobLogStatus:         crtypes.MembershipJobLogStatusEnabled,
		DefaultJobResultConfiguration: &crtypes.MembershipProtectedJobResultConfiguration{
			RoleArn: aws.String("arn:aws:iam::111122223333:role/updated"),
			OutputConfiguration: &crtypes.MembershipProtectedJobOutputConfigurationMemberS3{
				Value: crtypes.ProtectedJobS3OutputConfigurationInput{Bucket: aws.String("updated-bucket")},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, crtypes.MembershipJobLogStatusEnabled, updOut.Membership.JobLogStatus)
	require.NotNil(t, updOut.Membership.DefaultJobResultConfiguration)

	getOut, err := client.GetMembership(ctx, &cleanroomssdk.GetMembershipInput{MembershipIdentifier: aws.String(memID)})
	require.NoError(t, err)
	assert.Equal(t, crtypes.MembershipJobLogStatusEnabled, getOut.Membership.JobLogStatus)
	require.NotNil(t, getOut.Membership.DefaultJobResultConfiguration)
	config := getOut.Membership.DefaultJobResultConfiguration
	s3Out, ok := config.OutputConfiguration.(*crtypes.MembershipProtectedJobOutputConfigurationMemberS3)
	require.True(t, ok)
	assert.Equal(t, "updated-bucket", aws.ToString(s3Out.Value.Bucket))
}
