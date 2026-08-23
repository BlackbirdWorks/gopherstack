package verifiedpermissions_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	avpsdk "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"
	"github.com/aws/aws-sdk-go-v2/service/verifiedpermissions/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSDKRoundTrip_IsAuthorized_ContextAffectsDecision proves IsAuthorized's
// "context" request member (IsAuthorizedInput.Context, real, previously never
// read by this backend at all -- gopherstack-c733's own gaps note disclosed
// this as "out of scope, not chased further") actually influences the
// evaluated Cedar decision, not just parsed-and-dropped. A policy referencing
// context.mfa can only ALLOW when a real client supplies that context.
func TestSDKRoundTrip_IsAuthorized_ContextAffectsDecision(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	ps, err := client.CreatePolicyStore(t.Context(), &avpsdk.CreatePolicyStoreInput{
		ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
	})
	require.NoError(t, err)

	_, err = client.CreatePolicy(t.Context(), &avpsdk.CreatePolicyInput{
		PolicyStoreId: ps.PolicyStoreId,
		Definition: &types.PolicyDefinitionMemberStatic{
			Value: types.StaticPolicyDefinition{
				Statement: aws.String("permit(principal, action, resource) when { context.mfa == true };"),
			},
		},
	})
	require.NoError(t, err)

	principal := &types.EntityIdentifier{EntityType: aws.String("User"), EntityId: aws.String("alice")}
	action := &types.ActionIdentifier{ActionType: aws.String("Action"), ActionId: aws.String("view")}
	resource := &types.EntityIdentifier{EntityType: aws.String("Document"), EntityId: aws.String("doc1")}

	withoutMFA, err := client.IsAuthorized(t.Context(), &avpsdk.IsAuthorizedInput{
		PolicyStoreId: ps.PolicyStoreId,
		Principal:     principal,
		Action:        action,
		Resource:      resource,
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		types.DecisionDeny,
		withoutMFA.Decision,
		"no context supplied -- context.mfa is unset, policy must not match",
	)

	withMFA, err := client.IsAuthorized(t.Context(), &avpsdk.IsAuthorizedInput{
		PolicyStoreId: ps.PolicyStoreId,
		Principal:     principal,
		Action:        action,
		Resource:      resource,
		Context: &types.ContextDefinitionMemberContextMap{
			Value: map[string]types.AttributeValue{
				"mfa": &types.AttributeValueMemberBoolean{Value: true},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, types.DecisionAllow, withMFA.Decision, "context.mfa=true supplied -- policy must match")
}

// TestSDKRoundTrip_IsAuthorized_EntitiesAffectDecision proves IsAuthorized's
// "entities" request member (IsAuthorizedInput.Entities, real, previously
// never read at all -- same disclosed gap as Context above) actually
// supplies attribute data to Cedar evaluation. A policy referencing
// resource.owner can only ALLOW when a real client supplies an entity list
// carrying that attribute.
func TestSDKRoundTrip_IsAuthorized_EntitiesAffectDecision(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	ps, err := client.CreatePolicyStore(t.Context(), &avpsdk.CreatePolicyStoreInput{
		ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
	})
	require.NoError(t, err)

	_, err = client.CreatePolicy(t.Context(), &avpsdk.CreatePolicyInput{
		PolicyStoreId: ps.PolicyStoreId,
		Definition: &types.PolicyDefinitionMemberStatic{
			Value: types.StaticPolicyDefinition{
				Statement: aws.String("permit(principal, action, resource) when { resource.owner == principal };"),
			},
		},
	})
	require.NoError(t, err)

	principal := &types.EntityIdentifier{EntityType: aws.String("User"), EntityId: aws.String("alice")}
	action := &types.ActionIdentifier{ActionType: aws.String("Action"), ActionId: aws.String("view")}
	resource := &types.EntityIdentifier{EntityType: aws.String("Document"), EntityId: aws.String("doc1")}

	withoutEntities, err := client.IsAuthorized(t.Context(), &avpsdk.IsAuthorizedInput{
		PolicyStoreId: ps.PolicyStoreId,
		Principal:     principal,
		Action:        action,
		Resource:      resource,
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		types.DecisionDeny,
		withoutEntities.Decision,
		"no entities supplied -- resource.owner is unknown, policy must not match",
	)

	withEntities, err := client.IsAuthorized(t.Context(), &avpsdk.IsAuthorizedInput{
		PolicyStoreId: ps.PolicyStoreId,
		Principal:     principal,
		Action:        action,
		Resource:      resource,
		Entities: &types.EntitiesDefinitionMemberEntityList{
			Value: []types.EntityItem{
				{
					Identifier: resource,
					Attributes: map[string]types.AttributeValue{
						"owner": &types.AttributeValueMemberEntityIdentifier{
							Value: *principal,
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		types.DecisionAllow,
		withEntities.Decision,
		"entities supplied resource.owner == principal -- policy must match",
	)
}
