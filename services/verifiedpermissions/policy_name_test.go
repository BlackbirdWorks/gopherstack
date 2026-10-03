package verifiedpermissions_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	avpsdk "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"
	"github.com/aws/aws-sdk-go-v2/service/verifiedpermissions/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const permitAll = "permit(principal, action, resource);"

func staticDef(stmt string) *types.PolicyDefinitionMemberStatic {
	return &types.PolicyDefinitionMemberStatic{Value: types.StaticPolicyDefinition{Statement: aws.String(stmt)}}
}

func updateDef() *types.UpdatePolicyDefinitionMemberStatic {
	return &types.UpdatePolicyDefinitionMemberStatic{
		Value: types.UpdateStaticPolicyDefinition{Statement: aws.String(permitAll)},
	}
}

// TestPolicyName_RoundTrip checks Name is echoed, resolves via "name/", and follows the
// documented update semantics (nil keeps, "" removes).
func TestPolicyName_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		updateName *string
		wantName   string
	}{
		{name: "update_omitted_keeps", updateName: nil, wantName: "my-policy"},
		{name: "update_renames", updateName: aws.String("renamed"), wantName: "renamed"},
		{name: "update_empty_removes", updateName: aws.String(""), wantName: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			store, err := client.CreatePolicyStore(t.Context(), &avpsdk.CreatePolicyStoreInput{
				ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
			})
			require.NoError(t, err)

			created, err := client.CreatePolicy(t.Context(), &avpsdk.CreatePolicyInput{
				PolicyStoreId: store.PolicyStoreId,
				Definition:    staticDef(permitAll),
				Name:          aws.String("my-policy"),
			})
			require.NoError(t, err)

			got, err := client.GetPolicy(t.Context(), &avpsdk.GetPolicyInput{
				PolicyStoreId: store.PolicyStoreId,
				PolicyId:      aws.String("name/my-policy"),
			})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(created.PolicyId), aws.ToString(got.PolicyId))
			assert.Equal(t, "my-policy", aws.ToString(got.Name))

			batch, err := client.BatchGetPolicy(t.Context(), &avpsdk.BatchGetPolicyInput{
				Requests: []types.BatchGetPolicyInputItem{
					{PolicyStoreId: store.PolicyStoreId, PolicyId: aws.String("name/my-policy")},
				},
			})
			require.NoError(t, err)
			require.Len(t, batch.Results, 1)
			assert.Equal(t, "my-policy", aws.ToString(batch.Results[0].Name))

			_, err = client.UpdatePolicy(t.Context(), &avpsdk.UpdatePolicyInput{
				PolicyStoreId: store.PolicyStoreId,
				PolicyId:      aws.String("name/my-policy"),
				Definition:    updateDef(),
				Name:          tt.updateName,
			})
			require.NoError(t, err)

			listed, err := client.ListPolicies(
				t.Context(), &avpsdk.ListPoliciesInput{PolicyStoreId: store.PolicyStoreId},
			)
			require.NoError(t, err)
			require.Len(t, listed.Policies, 1)
			assert.Equal(t, tt.wantName, aws.ToString(listed.Policies[0].Name))
		})
	}
}

// TestPolicyName_ConflictAndDelete checks duplicate names conflict and DeletePolicy by
// "name/" frees the name.
func TestPolicyName_ConflictAndDelete(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	store, err := client.CreatePolicyStore(t.Context(), &avpsdk.CreatePolicyStoreInput{
		ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
	})
	require.NoError(t, err)

	create := func(name string) (*avpsdk.CreatePolicyOutput, error) {
		return client.CreatePolicy(t.Context(), &avpsdk.CreatePolicyInput{
			PolicyStoreId: store.PolicyStoreId,
			Definition:    staticDef(permitAll),
			Name:          aws.String(name),
		})
	}

	_, err = create("a")
	require.NoError(t, err)
	other, err := create("b")
	require.NoError(t, err)

	_, err = create("a")
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ConflictException", apiErr.ErrorCode())

	_, err = client.UpdatePolicy(t.Context(), &avpsdk.UpdatePolicyInput{
		PolicyStoreId: store.PolicyStoreId,
		PolicyId:      other.PolicyId,
		Definition:    updateDef(),
		Name:          aws.String("a"),
	})
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ConflictException", apiErr.ErrorCode())

	_, err = client.DeletePolicy(t.Context(), &avpsdk.DeletePolicyInput{
		PolicyStoreId: store.PolicyStoreId,
		PolicyId:      aws.String("name/a"),
	})
	require.NoError(t, err)

	_, err = create("a")
	require.NoError(t, err)
}
