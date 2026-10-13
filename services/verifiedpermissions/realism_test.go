package verifiedpermissions_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	avpsdk "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"
	"github.com/aws/aws-sdk-go-v2/service/verifiedpermissions/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const realismSchema = `{"NS":{"entityTypes":{"User":{},"Photo":{}},` +
	`"actions":{"view":{"appliesTo":{"principalTypes":["User"],"resourceTypes":["Photo"]}}}}}`

func realismPolicy(statement string) types.PolicyDefinition {
	return &types.PolicyDefinitionMemberStatic{Value: types.StaticPolicyDefinition{Statement: aws.String(statement)}}
}

func TestRealism_StrictValidationAgainstSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		statement string
		withSchem bool
		wantErr   bool
	}{
		{name: "no schema", statement: `permit(principal, action, resource);`, wantErr: true},
		{
			name: "valid", withSchem: true,
			statement: `permit(principal == NS::User::"a", action == NS::Action::"view", resource);`,
		},
		{
			name: "unknown principal type", withSchem: true, wantErr: true,
			statement: `permit(principal == NS::Nope::"a", action == NS::Action::"view", resource);`,
		},
		{
			name: "unknown action", withSchem: true, wantErr: true,
			statement: `permit(principal, action == NS::Action::"zzz", resource);`,
		},
		{
			name: "unknown resource type via is", withSchem: true, wantErr: true,
			statement: `permit(principal, action, resource is NS::Album);`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			store, err := client.CreatePolicyStore(ctx, &avpsdk.CreatePolicyStoreInput{
				ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeStrict},
			})
			require.NoError(t, err)

			if tc.withSchem {
				_, err = client.PutSchema(ctx, &avpsdk.PutSchemaInput{
					PolicyStoreId: store.PolicyStoreId,
					Definition:    &types.SchemaDefinitionMemberCedarJson{Value: realismSchema},
				})
				require.NoError(t, err)
			}

			_, err = client.CreatePolicy(ctx, &avpsdk.CreatePolicyInput{
				PolicyStoreId: store.PolicyStoreId, Definition: realismPolicy(tc.statement),
			})
			if !tc.wantErr {
				require.NoError(t, err)

				return
			}

			var ve *types.ValidationException
			require.ErrorAs(t, err, &ve)
			assert.NotContains(t, aws.ToString(ve.Message), "ValidationException")
		})
	}
}

func TestRealism_ListParamsAndMessages(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.ListPolicyStores(t.Context(), &avpsdk.ListPolicyStoresInput{NextToken: aws.String("%%%")})

	var ve *types.ValidationException
	require.ErrorAs(t, err, &ve)

	_, err = client.ListPolicyStores(t.Context(), &avpsdk.ListPolicyStoresInput{MaxResults: aws.Int32(51)})
	require.ErrorAs(t, err, &ve)

	_, err = client.GetPolicyStore(t.Context(), &avpsdk.GetPolicyStoreInput{PolicyStoreId: aws.String("missingstore")})

	var nf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)
	assert.NotContains(t, aws.ToString(nf.Message), "ResourceNotFoundException")
}
