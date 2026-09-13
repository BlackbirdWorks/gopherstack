package verifiedpermissions_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	avpsdk "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"
	"github.com/aws/aws-sdk-go-v2/service/verifiedpermissions/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_Slice29_RealClient drives every gopherstack-n3zi typed-slice-29
// uncovered verifiedpermissions op through the real aws-sdk-go-v2 client.
func Test_Slice29_RealClient(t *testing.T) {
	t.Parallel()

	t.Run("policy_store_lifecycle", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		createOut, err := client.CreatePolicyStore(ctx, &avpsdk.CreatePolicyStoreInput{
			ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
			Description:        aws.String("initial"),
		})
		require.NoError(t, err)

		listOut, err := client.ListPolicyStores(ctx, &avpsdk.ListPolicyStoresInput{})
		require.NoError(t, err)
		require.Len(t, listOut.PolicyStores, 1)
		assert.Equal(t, aws.ToString(createOut.PolicyStoreId), aws.ToString(listOut.PolicyStores[0].PolicyStoreId))

		updOut, err := client.UpdatePolicyStore(ctx, &avpsdk.UpdatePolicyStoreInput{
			PolicyStoreId:      createOut.PolicyStoreId,
			ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeStrict},
			Description:        aws.String("updated"),
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(createOut.PolicyStoreId), aws.ToString(updOut.PolicyStoreId))

		_, err = client.DeletePolicyStore(ctx, &avpsdk.DeletePolicyStoreInput{
			PolicyStoreId: createOut.PolicyStoreId,
		})
		require.NoError(t, err)

		listOut2, err := client.ListPolicyStores(ctx, &avpsdk.ListPolicyStoresInput{})
		require.NoError(t, err)
		assert.Empty(t, listOut2.PolicyStores)
	})

	t.Run("policy_store_alias_lifecycle", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		storeOut, err := client.CreatePolicyStore(ctx, &avpsdk.CreatePolicyStoreInput{
			ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
		})
		require.NoError(t, err)

		aliasOut, err := client.CreatePolicyStoreAlias(ctx, &avpsdk.CreatePolicyStoreAliasInput{
			AliasName:     aws.String("policy-store-alias/my-alias"),
			PolicyStoreId: storeOut.PolicyStoreId,
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(storeOut.PolicyStoreId), aws.ToString(aliasOut.PolicyStoreId))

		getOut, err := client.GetPolicyStoreAlias(ctx, &avpsdk.GetPolicyStoreAliasInput{
			AliasName: aws.String("policy-store-alias/my-alias"),
		})
		require.NoError(t, err)
		assert.Equal(t, types.AliasStateActive, getOut.State)

		listOut, err := client.ListPolicyStoreAliases(ctx, &avpsdk.ListPolicyStoreAliasesInput{
			Filter: &types.PolicyStoreAliasFilter{PolicyStoreId: storeOut.PolicyStoreId},
		})
		require.NoError(t, err)
		require.Len(t, listOut.PolicyStoreAliases, 1)
		assert.Equal(t, "policy-store-alias/my-alias", aws.ToString(listOut.PolicyStoreAliases[0].AliasName))

		_, err = client.DeletePolicyStoreAlias(ctx, &avpsdk.DeletePolicyStoreAliasInput{
			AliasName:    aws.String("policy-store-alias/my-alias"),
			DeletionMode: types.DeletionModeHardDelete,
		})
		require.NoError(t, err)

		listOut2, err := client.ListPolicyStoreAliases(ctx, &avpsdk.ListPolicyStoreAliasesInput{
			Filter: &types.PolicyStoreAliasFilter{PolicyStoreId: storeOut.PolicyStoreId},
		})
		require.NoError(t, err)
		assert.Empty(t, listOut2.PolicyStoreAliases)
	})

	t.Run("policy_lifecycle", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		storeOut, err := client.CreatePolicyStore(ctx, &avpsdk.CreatePolicyStoreInput{
			ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
		})
		require.NoError(t, err)

		createOut, err := client.CreatePolicy(ctx, &avpsdk.CreatePolicyInput{
			PolicyStoreId: storeOut.PolicyStoreId,
			Definition: &types.PolicyDefinitionMemberStatic{
				Value: types.StaticPolicyDefinition{Statement: aws.String(`permit(principal, action, resource);`)},
			},
		})
		require.NoError(t, err)

		getOut, err := client.GetPolicy(ctx, &avpsdk.GetPolicyInput{
			PolicyStoreId: storeOut.PolicyStoreId,
			PolicyId:      createOut.PolicyId,
		})
		require.NoError(t, err)
		require.NotNil(t, getOut.Definition)

		listOut, err := client.ListPolicies(ctx, &avpsdk.ListPoliciesInput{
			PolicyStoreId: storeOut.PolicyStoreId,
		})
		require.NoError(t, err)
		require.Len(t, listOut.Policies, 1)

		updOut, err := client.UpdatePolicy(ctx, &avpsdk.UpdatePolicyInput{
			PolicyStoreId: storeOut.PolicyStoreId,
			PolicyId:      createOut.PolicyId,
			Definition: &types.UpdatePolicyDefinitionMemberStatic{
				Value: types.UpdateStaticPolicyDefinition{
					Statement: aws.String(`forbid(principal, action, resource);`),
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(createOut.PolicyId), aws.ToString(updOut.PolicyId))

		_, err = client.DeletePolicy(ctx, &avpsdk.DeletePolicyInput{
			PolicyStoreId: storeOut.PolicyStoreId,
			PolicyId:      createOut.PolicyId,
		})
		require.NoError(t, err)

		listOut2, err := client.ListPolicies(ctx, &avpsdk.ListPoliciesInput{
			PolicyStoreId: storeOut.PolicyStoreId,
		})
		require.NoError(t, err)
		assert.Empty(t, listOut2.Policies)
	})

	t.Run("policy_template_delete", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		storeOut, err := client.CreatePolicyStore(ctx, &avpsdk.CreatePolicyStoreInput{
			ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
		})
		require.NoError(t, err)

		tmplOut, err := client.CreatePolicyTemplate(ctx, &avpsdk.CreatePolicyTemplateInput{
			PolicyStoreId: storeOut.PolicyStoreId,
			Statement:     aws.String(`permit(principal == ?principal, action, resource);`),
		})
		require.NoError(t, err)

		_, err = client.DeletePolicyTemplate(ctx, &avpsdk.DeletePolicyTemplateInput{
			PolicyStoreId:    storeOut.PolicyStoreId,
			PolicyTemplateId: tmplOut.PolicyTemplateId,
		})
		require.NoError(t, err)

		_, err = client.GetPolicyTemplate(ctx, &avpsdk.GetPolicyTemplateInput{
			PolicyStoreId:    storeOut.PolicyStoreId,
			PolicyTemplateId: tmplOut.PolicyTemplateId,
		})
		require.Error(t, err, "template was deleted")
	})

	t.Run("identity_source_lifecycle", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		storeOut, err := client.CreatePolicyStore(ctx, &avpsdk.CreatePolicyStoreInput{
			ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
		})
		require.NoError(t, err)

		createOut, err := client.CreateIdentitySource(ctx, &avpsdk.CreateIdentitySourceInput{
			PolicyStoreId:       storeOut.PolicyStoreId,
			PrincipalEntityType: aws.String("User"),
			Configuration: &types.ConfigurationMemberOpenIdConnectConfiguration{
				Value: types.OpenIdConnectConfiguration{
					Issuer: aws.String("https://example.com"),
					TokenSelection: &types.OpenIdConnectTokenSelectionMemberAccessTokenOnly{
						Value: types.OpenIdConnectAccessTokenConfiguration{
							PrincipalIdClaim: aws.String("sub"),
							Audiences:        []string{"myapp"},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		getOut, err := client.GetIdentitySource(ctx, &avpsdk.GetIdentitySourceInput{
			PolicyStoreId:    storeOut.PolicyStoreId,
			IdentitySourceId: createOut.IdentitySourceId,
		})
		require.NoError(t, err)
		require.NotNil(t, getOut.Configuration)
		detail, ok := getOut.Configuration.(*types.ConfigurationDetailMemberOpenIdConnectConfiguration)
		require.True(t, ok, "expected OIDC configuration detail, got %T", getOut.Configuration)
		assert.Equal(t, "https://example.com", aws.ToString(detail.Value.Issuer))

		listOut, err := client.ListIdentitySources(ctx, &avpsdk.ListIdentitySourcesInput{
			PolicyStoreId: storeOut.PolicyStoreId,
		})
		require.NoError(t, err)
		require.Len(t, listOut.IdentitySources, 1)

		updOut, err := client.UpdateIdentitySource(ctx, &avpsdk.UpdateIdentitySourceInput{
			PolicyStoreId:    storeOut.PolicyStoreId,
			IdentitySourceId: createOut.IdentitySourceId,
			UpdateConfiguration: &types.UpdateConfigurationMemberOpenIdConnectConfiguration{
				Value: types.UpdateOpenIdConnectConfiguration{
					Issuer: aws.String("https://updated.example.com"),
					TokenSelection: &types.UpdateOpenIdConnectTokenSelectionMemberAccessTokenOnly{
						Value: types.UpdateOpenIdConnectAccessTokenConfiguration{
							PrincipalIdClaim: aws.String("sub"),
							Audiences:        []string{"myapp"},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(createOut.IdentitySourceId), aws.ToString(updOut.IdentitySourceId))

		_, err = client.DeleteIdentitySource(ctx, &avpsdk.DeleteIdentitySourceInput{
			PolicyStoreId:    storeOut.PolicyStoreId,
			IdentitySourceId: createOut.IdentitySourceId,
		})
		require.NoError(t, err)

		listOut2, err := client.ListIdentitySources(ctx, &avpsdk.ListIdentitySourcesInput{
			PolicyStoreId: storeOut.PolicyStoreId,
		})
		require.NoError(t, err)
		assert.Empty(t, listOut2.IdentitySources)
	})

	t.Run("schema_lifecycle", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		storeOut, err := client.CreatePolicyStore(ctx, &avpsdk.CreatePolicyStoreInput{
			ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
		})
		require.NoError(t, err)

		schemaJSON := `{"MyNamespace":{"entityTypes":{},"actions":{}}}`

		_, err = client.PutSchema(ctx, &avpsdk.PutSchemaInput{
			PolicyStoreId: storeOut.PolicyStoreId,
			Definition: &types.SchemaDefinitionMemberCedarJson{
				Value: schemaJSON,
			},
		})
		require.NoError(t, err)

		getOut, err := client.GetSchema(ctx, &avpsdk.GetSchemaInput{
			PolicyStoreId: storeOut.PolicyStoreId,
		})
		require.NoError(t, err)
		assert.JSONEq(t, schemaJSON, aws.ToString(getOut.Schema))
		assert.Contains(t, getOut.Namespaces, "MyNamespace")
	})

	t.Run("batch_is_authorized", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		storeOut, err := client.CreatePolicyStore(ctx, &avpsdk.CreatePolicyStoreInput{
			ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
		})
		require.NoError(t, err)

		_, err = client.CreatePolicy(ctx, &avpsdk.CreatePolicyInput{
			PolicyStoreId: storeOut.PolicyStoreId,
			Definition: &types.PolicyDefinitionMemberStatic{
				Value: types.StaticPolicyDefinition{Statement: aws.String(`permit(principal, action, resource);`)},
			},
		})
		require.NoError(t, err)

		batchOut, err := client.BatchIsAuthorized(ctx, &avpsdk.BatchIsAuthorizedInput{
			PolicyStoreId: storeOut.PolicyStoreId,
			Requests: []types.BatchIsAuthorizedInputItem{
				{
					Principal: &types.EntityIdentifier{EntityType: aws.String("User"), EntityId: aws.String("alice")},
					Action:    &types.ActionIdentifier{ActionType: aws.String("Action"), ActionId: aws.String("view")},
					Resource:  &types.EntityIdentifier{EntityType: aws.String("Photo"), EntityId: aws.String("photo1")},
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, batchOut.Results, 1)
		assert.Equal(t, types.DecisionAllow, batchOut.Results[0].Decision)
	})

	t.Run("token_based_authorization", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		storeOut, err := client.CreatePolicyStore(ctx, &avpsdk.CreatePolicyStoreInput{
			ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
		})
		require.NoError(t, err)

		_, err = client.CreatePolicy(ctx, &avpsdk.CreatePolicyInput{
			PolicyStoreId: storeOut.PolicyStoreId,
			Definition: &types.PolicyDefinitionMemberStatic{
				Value: types.StaticPolicyDefinition{Statement: aws.String(`permit(principal, action, resource);`)},
			},
		})
		require.NoError(t, err)

		_, err = client.CreateIdentitySource(ctx, &avpsdk.CreateIdentitySourceInput{
			PolicyStoreId:       storeOut.PolicyStoreId,
			PrincipalEntityType: aws.String("User"),
			Configuration: &types.ConfigurationMemberOpenIdConnectConfiguration{
				Value: types.OpenIdConnectConfiguration{
					Issuer: aws.String("https://example.com"),
					TokenSelection: &types.OpenIdConnectTokenSelectionMemberAccessTokenOnly{
						Value: types.OpenIdConnectAccessTokenConfiguration{
							PrincipalIdClaim: aws.String("sub"),
							Audiences:        []string{"myapp"},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		token := makeTestJWT(map[string]any{"sub": "user-alice", "iss": "https://example.com", "aud": "myapp"})

		isOut, err := client.IsAuthorizedWithToken(ctx, &avpsdk.IsAuthorizedWithTokenInput{
			PolicyStoreId: storeOut.PolicyStoreId,
			AccessToken:   aws.String(token),
			Action:        &types.ActionIdentifier{ActionType: aws.String("Action"), ActionId: aws.String("view")},
			Resource:      &types.EntityIdentifier{EntityType: aws.String("Resource"), EntityId: aws.String("res1")},
		})
		require.NoError(t, err)
		assert.Equal(t, types.DecisionAllow, isOut.Decision)
		require.NotNil(t, isOut.Principal)
		assert.Equal(t, "user-alice", aws.ToString(isOut.Principal.EntityId))

		batchOut, err := client.BatchIsAuthorizedWithToken(ctx, &avpsdk.BatchIsAuthorizedWithTokenInput{
			PolicyStoreId: storeOut.PolicyStoreId,
			AccessToken:   aws.String(token),
			Requests: []types.BatchIsAuthorizedWithTokenInputItem{
				{
					Action:   &types.ActionIdentifier{ActionType: aws.String("Action"), ActionId: aws.String("view")},
					Resource: &types.EntityIdentifier{EntityType: aws.String("Resource"), EntityId: aws.String("res1")},
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, batchOut.Results, 1)
		assert.Equal(t, types.DecisionAllow, batchOut.Results[0].Decision)
	})
}
