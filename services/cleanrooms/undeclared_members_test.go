package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	"github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

func TestSDK_CollaborationRoundTripWithoutMembersKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		members int
	}{
		{name: "creator_only", members: 0},
		{name: "with_invitee", members: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := cleanrooms.NewHandler(cleanrooms.NewInMemoryBackend("000000000000", "us-east-1"))
			client := newTestCleanRoomsClient(t, h)
			ctx := t.Context()

			in := &cleanroomssdk.CreateCollaborationInput{
				Name:                   aws.String("c"),
				Description:            aws.String("d"),
				CreatorDisplayName:     aws.String("Owner"),
				CreatorMemberAbilities: []types.MemberAbility{types.MemberAbilityCanQuery},
				QueryLogStatus:         types.CollaborationQueryLogStatusDisabled,
				Members:                []types.MemberSpecification{},
			}
			if tc.members > 0 {
				in.Members = append(in.Members, types.MemberSpecification{
					AccountId:       aws.String("222222222222"),
					DisplayName:     aws.String("Invitee"),
					MemberAbilities: []types.MemberAbility{types.MemberAbilityCanReceiveResults},
				})
			}

			created, err := client.CreateCollaboration(ctx, in)
			require.NoError(t, err)

			got, err := client.GetCollaboration(ctx, &cleanroomssdk.GetCollaborationInput{
				CollaborationIdentifier: created.Collaboration.Id,
			})
			require.NoError(t, err)
			assert.Equal(t, "c", aws.ToString(got.Collaboration.Name))
			assert.Equal(t, "Owner", aws.ToString(got.Collaboration.CreatorDisplayName))
			assert.NotEmpty(t, aws.ToString(got.Collaboration.MembershipId))

			members, err := client.ListMembers(ctx, &cleanroomssdk.ListMembersInput{
				CollaborationIdentifier: created.Collaboration.Id,
			})
			require.NoError(t, err)
			assert.Len(t, members.MemberSummaries, 1+tc.members)
		})
	}
}

func TestHandler_CollaborationOmitsMembersKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
	}{
		{name: "create", op: "create"},
		{name: "get", op: "get"},
		{name: "update", op: "update"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newTestServer(t)
			rec := doRequest(t, e, http.MethodPost, "/collaborations", map[string]any{
				"name": "c", "creatorDisplayName": "Owner",
				"creatorMemberAbilities": []string{"CAN_QUERY"},
				"members":                []any{}, "queryLogStatus": "DISABLED",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			body := decodeBody(t, rec.Body.Bytes())
			id := body["collaboration"].(map[string]any)["id"].(string)

			switch tc.op {
			case "get":
				rec = doRequest(t, e, http.MethodGet, "/collaborations/"+id, nil)
			case "update":
				rec = doRequest(t, e, http.MethodPatch, "/collaborations/"+id, map[string]any{"description": "x"})
			}
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			collab := decodeBody(t, rec.Body.Bytes())["collaboration"]
			assert.NotContains(t, collab, "members")
		})
	}
}

func decodeBody(t *testing.T, b []byte) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))

	return m
}
