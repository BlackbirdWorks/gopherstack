package cleanrooms_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	crtypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_ProtectedQuerySummaryReceiverConfigurations(t *testing.T) {
	t.Parallel()

	member := func(id string) crtypes.ProtectedQueryOutputConfiguration {
		return &crtypes.ProtectedQueryOutputConfigurationMemberMember{
			Value: crtypes.ProtectedQueryMemberOutputConfiguration{AccountId: aws.String(id)},
		}
	}
	s3 := &crtypes.ProtectedQueryOutputConfigurationMemberS3{
		Value: crtypes.ProtectedQueryS3OutputConfiguration{
			Bucket: aws.String("b"), ResultFormat: crtypes.ResultFormatCsv,
		},
	}

	tests := []struct {
		out  crtypes.ProtectedQueryOutputConfiguration
		name string
		want []string
	}{
		{name: "member", out: member("444455556666"), want: []string{"444455556666"}},
		{name: "s3", out: s3, want: []string{rtTestAccountID}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRoundTripTestClient(t)
			ctx := t.Context()
			_, memID := createCollaborationAndMembership(t, client)
			_, err := client.StartProtectedQuery(ctx, &cleanroomssdk.StartProtectedQueryInput{
				MembershipIdentifier: aws.String(memID),
				Type:                 crtypes.ProtectedQueryTypeSql,
				SqlParameters:        &crtypes.ProtectedQuerySQLParameters{QueryString: aws.String("SELECT 1")},
				ResultConfiguration: &crtypes.ProtectedQueryResultConfiguration{
					OutputConfiguration: tt.out,
				},
			})
			require.NoError(t, err)

			list, err := client.ListProtectedQueries(ctx, &cleanroomssdk.ListProtectedQueriesInput{
				MembershipIdentifier: aws.String(memID),
			})
			require.NoError(t, err)
			require.Len(t, list.ProtectedQueries, 1)
			rcs := list.ProtectedQueries[0].ReceiverConfigurations
			require.Len(t, rcs, 1)
			assert.Equal(t, crtypes.AnalysisTypeDirectAnalysis, rcs[0].AnalysisType)
			details, ok := rcs[0].ConfigurationDetails.(*crtypes.ConfigurationDetailsMemberDirectAnalysisConfigurationDetails)
			require.True(t, ok)
			assert.Equal(t, tt.want, details.Value.ReceiverAccountIds)
		})
	}
}

func TestProtectedJob_WireOmitsRequestOnlyType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
	}{
		{name: "start", op: "start"},
		{name: "get", op: "get"},
		{name: "list", op: "list"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)
			startRec := doRequest(t, e, http.MethodPost, "/memberships/"+mID+"/protectedJobs", map[string]any{
				"type": "PYSPARK",
				"resultConfiguration": map[string]any{
					"outputConfiguration": map[string]any{"member": map[string]any{"accountId": "444455556666"}},
				},
			})
			require.Equal(t, http.StatusOK, startRec.Code, startRec.Body.String())
			job := decodeBody(t, startRec.Body.Bytes())["protectedJob"].(map[string]any)

			switch tt.op {
			case "get":
				rec := doRequest(t, e, http.MethodGet, "/memberships/"+mID+"/protectedJobs/"+job["id"].(string), nil)
				require.Equal(t, http.StatusOK, rec.Code)
				job = decodeBody(t, rec.Body.Bytes())["protectedJob"].(map[string]any)
			case "list":
				rec := doRequest(t, e, http.MethodGet, "/memberships/"+mID+"/protectedJobs", nil)
				require.Equal(t, http.StatusOK, rec.Code)
				items := decodeBody(t, rec.Body.Bytes())["protectedJobs"].([]any)
				require.Len(t, items, 1)
				job = items[0].(map[string]any)
				assert.Contains(t, job, "receiverConfigurations")
			}
			assert.NotContains(t, job, "type")
		})
	}
}

func TestRealClient_ChangeRequestAbilityModification(t *testing.T) {
	t.Parallel()

	model := crtypes.CustomMLMemberAbilityCanReceiveModelOutput
	infer := crtypes.CustomMLMemberAbilityCanReceiveInferenceOutput

	tests := []struct {
		name      string
		next      []crtypes.CustomMLMemberAbility
		wantTypes []crtypes.ChangeType
		wantFinal []crtypes.CustomMLMemberAbility
	}{
		{
			name:      "swap",
			next:      []crtypes.CustomMLMemberAbility{infer},
			wantTypes: []crtypes.ChangeType{"REVOKE_CAN_RECEIVE_MODEL_OUTPUT", "GRANT_CAN_RECEIVE_INFERENCE_OUTPUT"},
			wantFinal: []crtypes.CustomMLMemberAbility{infer},
		},
		{
			name:      "revoke",
			next:      []crtypes.CustomMLMemberAbility{},
			wantTypes: []crtypes.ChangeType{"REVOKE_CAN_RECEIVE_MODEL_OUTPUT"},
			wantFinal: []crtypes.CustomMLMemberAbility{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRoundTripTestClient(t)
			ctx := t.Context()
			collabID, _ := createCollaborationAndMembership(t, client)
			change := func(ml []crtypes.CustomMLMemberAbility) crtypes.Change {
				spec := crtypes.MemberChangeSpecification{
					AccountId:         aws.String("333333333333"),
					MemberAbilities:   []crtypes.MemberAbility{},
					MlMemberAbilities: &crtypes.MLMemberAbilities{CustomMLMemberAbilities: ml},
				}
				in := &cleanroomssdk.CreateCollaborationChangeRequestInput{
					CollaborationIdentifier: aws.String(collabID),
					Changes: []crtypes.ChangeInput{{
						SpecificationType: crtypes.ChangeSpecificationTypeMember,
						Specification:     &crtypes.ChangeSpecificationMemberMember{Value: spec},
					}},
				}
				cr, err := client.CreateCollaborationChangeRequest(ctx, in)
				require.NoError(t, err)
				for _, action := range []crtypes.ChangeRequestAction{
					crtypes.ChangeRequestActionApprove, crtypes.ChangeRequestActionCommit,
				} {
					upd := &cleanroomssdk.UpdateCollaborationChangeRequestInput{
						CollaborationIdentifier: aws.String(collabID),
						ChangeRequestIdentifier: cr.CollaborationChangeRequest.Id,
						Action:                  action,
					}
					_, err = client.UpdateCollaborationChangeRequest(ctx, upd)
					require.NoError(t, err)
				}

				return cr.CollaborationChangeRequest.Changes[0]
			}

			change([]crtypes.CustomMLMemberAbility{model})
			got := change(tt.next)
			assert.ElementsMatch(t, tt.wantTypes, got.Types)

			members, err := client.ListMembers(ctx, &cleanroomssdk.ListMembersInput{
				CollaborationIdentifier: aws.String(collabID),
			})
			require.NoError(t, err)
			for _, m := range members.MemberSummaries {
				if aws.ToString(m.AccountId) == "333333333333" {
					require.NotNil(t, m.MlAbilities)
					assert.ElementsMatch(t, tt.wantFinal, m.MlAbilities.CustomMLMemberAbilities)
				}
			}
		})
	}
}
