package cleanrooms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	crtypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_MLMemberAbilities(t *testing.T) {
	t.Parallel()

	model := crtypes.CustomMLMemberAbilityCanReceiveModelOutput
	infer := crtypes.CustomMLMemberAbilityCanReceiveInferenceOutput

	tests := []struct {
		creator *crtypes.MLMemberAbilities
		member  *crtypes.MLMemberAbilities
		name    string
		wantErr bool
	}{
		{
			name:    "creator_and_member",
			creator: &crtypes.MLMemberAbilities{CustomMLMemberAbilities: []crtypes.CustomMLMemberAbility{model}},
			member:  &crtypes.MLMemberAbilities{CustomMLMemberAbilities: []crtypes.CustomMLMemberAbility{model, infer}},
		},
		{name: "none"},
		{
			name:    "bad_creator_ability",
			creator: &crtypes.MLMemberAbilities{CustomMLMemberAbilities: []crtypes.CustomMLMemberAbility{"BOGUS"}},
			wantErr: true,
		},
		{
			name:    "bad_member_ability",
			member:  &crtypes.MLMemberAbilities{CustomMLMemberAbilities: []crtypes.CustomMLMemberAbility{"BOGUS"}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRoundTripTestClient(t)
			ctx := t.Context()
			out, err := client.CreateCollaboration(ctx, &cleanroomssdk.CreateCollaborationInput{
				Name:                     aws.String("c"),
				Description:              aws.String("d"),
				CreatorDisplayName:       aws.String("creator"),
				CreatorMemberAbilities:   []crtypes.MemberAbility{crtypes.MemberAbilityCanQuery},
				CreatorMLMemberAbilities: tt.creator,
				Members: []crtypes.MemberSpecification{{
					AccountId:         aws.String("222222222222"),
					DisplayName:       aws.String("other"),
					MemberAbilities:   []crtypes.MemberAbility{crtypes.MemberAbilityCanQuery},
					MlMemberAbilities: tt.member,
				}},
				QueryLogStatus: crtypes.CollaborationQueryLogStatusDisabled,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)

			members, err := client.ListMembers(ctx, &cleanroomssdk.ListMembersInput{
				CollaborationIdentifier: out.Collaboration.Id,
			})
			require.NoError(t, err)
			require.Len(t, members.MemberSummaries, 2)
			assert.Equal(t, tt.creator, members.MemberSummaries[0].MlAbilities)
			assert.Equal(t, tt.member, members.MemberSummaries[1].MlAbilities)

			mem, err := client.GetMembership(ctx, &cleanroomssdk.GetMembershipInput{
				MembershipIdentifier: out.Collaboration.MembershipId,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.creator, mem.Membership.MlMemberAbilities)

			list, err := client.ListMemberships(ctx, &cleanroomssdk.ListMembershipsInput{})
			require.NoError(t, err)
			require.Len(t, list.MembershipSummaries, 1)
			assert.Equal(t, tt.creator, list.MembershipSummaries[0].MlMemberAbilities)
		})
	}
}

func TestRealClient_AddMemberChangeCarriesMLAbilities(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()
	collabID, _ := createCollaborationAndMembership(t, client)
	ml := &crtypes.MLMemberAbilities{
		CustomMLMemberAbilities: []crtypes.CustomMLMemberAbility{
			crtypes.CustomMLMemberAbilityCanReceiveInferenceOutput,
		},
	}

	cr, err := client.CreateCollaborationChangeRequest(ctx, &cleanroomssdk.CreateCollaborationChangeRequestInput{
		CollaborationIdentifier: aws.String(collabID),
		Changes: []crtypes.ChangeInput{{
			SpecificationType: crtypes.ChangeSpecificationTypeMember,
			Specification: &crtypes.ChangeSpecificationMemberMember{Value: crtypes.MemberChangeSpecification{
				AccountId:         aws.String("333333333333"),
				MemberAbilities:   []crtypes.MemberAbility{crtypes.MemberAbilityCanQuery},
				MlMemberAbilities: ml,
			}},
		}},
	})
	require.NoError(t, err)

	actions := []crtypes.ChangeRequestAction{
		crtypes.ChangeRequestActionApprove, crtypes.ChangeRequestActionCommit,
	}
	for _, action := range actions {
		_, err = client.UpdateCollaborationChangeRequest(ctx, &cleanroomssdk.UpdateCollaborationChangeRequestInput{
			CollaborationIdentifier: aws.String(collabID),
			ChangeRequestIdentifier: cr.CollaborationChangeRequest.Id,
			Action:                  action,
		})
		require.NoError(t, err)
	}

	members, err := client.ListMembers(ctx, &cleanroomssdk.ListMembersInput{
		CollaborationIdentifier: aws.String(collabID),
	})
	require.NoError(t, err)
	var got *crtypes.MLMemberAbilities
	for _, m := range members.MemberSummaries {
		if aws.ToString(m.AccountId) == "333333333333" {
			got = m.MlAbilities
		}
	}
	assert.Equal(t, ml, got)
}

func TestRealClient_ComputePayerAccountIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		payer string
	}{
		{name: "set", payer: "222222222222"},
		{name: "unset"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRoundTripTestClient(t)
			ctx := t.Context()
			_, memID := createCollaborationAndMembership(t, client)
			payer := aws.String(tt.payer)
			if tt.payer == "" {
				payer = nil
			}

			q, err := client.StartProtectedQuery(ctx, &cleanroomssdk.StartProtectedQueryInput{
				MembershipIdentifier:       aws.String(memID),
				Type:                       crtypes.ProtectedQueryTypeSql,
				SqlParameters:              &crtypes.ProtectedQuerySQLParameters{QueryString: aws.String("SELECT 1")},
				QueryComputePayerAccountId: payer,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.payer, aws.ToString(q.ProtectedQuery.QueryComputePayerAccountId))

			gq, err := client.GetProtectedQuery(ctx, &cleanroomssdk.GetProtectedQueryInput{
				MembershipIdentifier:     aws.String(memID),
				ProtectedQueryIdentifier: q.ProtectedQuery.Id,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.payer, aws.ToString(gq.ProtectedQuery.QueryComputePayerAccountId))

			lq, err := client.ListProtectedQueries(ctx, &cleanroomssdk.ListProtectedQueriesInput{
				MembershipIdentifier: aws.String(memID),
			})
			require.NoError(t, err)
			require.Len(t, lq.ProtectedQueries, 1)
			assert.Equal(t, tt.payer, aws.ToString(lq.ProtectedQueries[0].QueryComputePayerAccountId))

			j, err := client.StartProtectedJob(ctx, &cleanroomssdk.StartProtectedJobInput{
				MembershipIdentifier: aws.String(memID),
				Type:                 crtypes.ProtectedJobTypePyspark,
				JobParameters: &crtypes.ProtectedJobParameters{
					AnalysisTemplateArn: aws.String(
						"arn:aws:cleanrooms:us-east-1:123456789012:membership/" + memID + "/analysistemplate/t",
					),
				},
				JobComputePayerAccountId: payer,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.payer, aws.ToString(j.ProtectedJob.JobComputePayerAccountId))

			gj, err := client.GetProtectedJob(ctx, &cleanroomssdk.GetProtectedJobInput{
				MembershipIdentifier:   aws.String(memID),
				ProtectedJobIdentifier: j.ProtectedJob.Id,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.payer, aws.ToString(gj.ProtectedJob.JobComputePayerAccountId))

			lj, err := client.ListProtectedJobs(ctx, &cleanroomssdk.ListProtectedJobsInput{
				MembershipIdentifier: aws.String(memID),
			})
			require.NoError(t, err)
			require.Len(t, lj.ProtectedJobs, 1)
			assert.Equal(t, tt.payer, aws.ToString(lj.ProtectedJobs[0].JobComputePayerAccountId))
		})
	}
}
