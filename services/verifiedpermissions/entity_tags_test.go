package verifiedpermissions_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	avpsdk "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"
	"github.com/aws/aws-sdk-go-v2/service/verifiedpermissions/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsAuthorized_EntityTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]types.CedarTagValue
		name string
		want types.Decision
	}{
		{name: "no tags", want: types.DecisionDeny},
		{
			name: "matching tag",
			tags: map[string]types.CedarTagValue{"role": &types.CedarTagValueMemberString{Value: "admin"}},
			want: types.DecisionAllow,
		},
		{
			name: "other value",
			tags: map[string]types.CedarTagValue{"role": &types.CedarTagValueMemberString{Value: "guest"}},
			want: types.DecisionDeny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ps, err := client.CreatePolicyStore(t.Context(), &avpsdk.CreatePolicyStoreInput{
				ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
			})
			require.NoError(t, err)

			_, err = client.CreatePolicy(t.Context(), &avpsdk.CreatePolicyInput{
				PolicyStoreId: ps.PolicyStoreId,
				Definition: &types.PolicyDefinitionMemberStatic{Value: types.StaticPolicyDefinition{
					Statement: aws.String(
						`permit(principal, action, resource) when { principal.hasTag("role") && principal.getTag("role") == "admin" };`,
					),
				}},
			})
			require.NoError(t, err)

			principal := &types.EntityIdentifier{EntityType: aws.String("User"), EntityId: aws.String("alice")}
			out, err := client.IsAuthorized(t.Context(), &avpsdk.IsAuthorizedInput{
				PolicyStoreId: ps.PolicyStoreId,
				Principal:     principal,
				Action:        &types.ActionIdentifier{ActionType: aws.String("Action"), ActionId: aws.String("view")},
				Resource:      &types.EntityIdentifier{EntityType: aws.String("Document"), EntityId: aws.String("d1")},
				Entities: &types.EntitiesDefinitionMemberEntityList{Value: []types.EntityItem{
					{Identifier: principal, Tags: tt.tags},
				}},
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, out.Decision)
		})
	}
}
