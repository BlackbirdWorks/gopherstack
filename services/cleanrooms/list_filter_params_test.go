package cleanrooms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	crtypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListCollaborations_MemberStatusFilter covers
// ListCollaborationsInput.MemberStatus (api_op_ListCollaborations.go): "The
// caller's status in a collaboration." Previously ignored -- the query
// parameter was parsed but discarded (passed as `_`) before reaching
// InMemoryBackend.ListCollaborations, so every collaboration was returned
// regardless of MemberStatus.
func TestListCollaborations_MemberStatusFilter(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()
	collabID, _ := createCollaborationAndMembership(t, client)

	active, err := client.ListCollaborations(ctx, &cleanroomssdk.ListCollaborationsInput{
		MemberStatus: crtypes.FilterableMemberStatusActive,
	})
	require.NoError(t, err)
	require.Len(t, active.CollaborationList, 1, "sanity: newly created collaboration is ACTIVE")
	assert.Equal(t, collabID, aws.ToString(active.CollaborationList[0].Id))

	invited, err := client.ListCollaborations(ctx, &cleanroomssdk.ListCollaborationsInput{
		MemberStatus: crtypes.FilterableMemberStatusInvited,
	})
	require.NoError(t, err)
	assert.Empty(t, invited.CollaborationList, "MemberStatus=INVITED must exclude the ACTIVE collaboration")
}

// TestListCollaborationChangeRequests_StatusFilter covers
// ListCollaborationChangeRequestsInput.Status
// (api_op_ListCollaborationChangeRequests.go): "A filter to only return
// change requests with the specified status." Previously ignored -- the
// handler never read the status query parameter at all, so every change
// request in the collaboration was returned regardless of Status.
func TestListCollaborationChangeRequests_StatusFilter(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()
	collabID, _ := createCollaborationAndMembership(t, client)

	createOut, err := client.CreateCollaborationChangeRequest(ctx, &cleanroomssdk.CreateCollaborationChangeRequestInput{
		CollaborationIdentifier: aws.String(collabID),
		Changes: []crtypes.ChangeInput{
			{
				SpecificationType: crtypes.ChangeSpecificationTypeMember,
				Specification: &crtypes.ChangeSpecificationMemberMember{
					Value: crtypes.MemberChangeSpecification{
						AccountId:       aws.String("111111111111"),
						MemberAbilities: []crtypes.MemberAbility{},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	changeRequestID := aws.ToString(createOut.CollaborationChangeRequest.Id)
	require.NotEmpty(t, changeRequestID)

	pending, err := client.ListCollaborationChangeRequests(ctx, &cleanroomssdk.ListCollaborationChangeRequestsInput{
		CollaborationIdentifier: aws.String(collabID),
		Status:                  crtypes.ChangeRequestStatusPending,
	})
	require.NoError(t, err)
	require.Len(t, pending.CollaborationChangeRequestSummaries, 1)
	assert.Equal(t, changeRequestID, aws.ToString(pending.CollaborationChangeRequestSummaries[0].Id))

	approved, err := client.ListCollaborationChangeRequests(ctx, &cleanroomssdk.ListCollaborationChangeRequestsInput{
		CollaborationIdentifier: aws.String(collabID),
		Status:                  crtypes.ChangeRequestStatusApproved,
	})
	require.NoError(t, err)
	assert.Empty(
		t, approved.CollaborationChangeRequestSummaries, "Status=APPROVED must exclude the PENDING change request",
	)
}
