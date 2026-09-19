package verifiedpermissions_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	avpsdk "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"
	"github.com/aws/aws-sdk-go-v2/service/verifiedpermissions/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListSummaryShapes proves this pass's over-wide-response audit
// (gopherstack, 2026-09-19) for verifiedpermissions's five flagged List
// ops. ListPolicyStoreAliases, ListPolicyStores and ListPolicyTemplates
// already matched their real *Item member sets exactly (verified via
// cmd/structfielddiff against verifiedpermissions@v1.36.4). ListIdentitySources
// was missing the deprecated-but-real "details" member entirely (backed by
// real state for Cognito-configured sources) -- fixed. ListPolicies is
// missing PolicyItem's optional "name" member, but it is unsourced: real
// CreatePolicy accepts an optional Name input that this backend's Policy
// model never stores at all -- recorded in items_still_open, not fabricated.
func TestListSummaryShapes(t *testing.T) {
	t.Parallel()

	//nolint:staticcheck // asserting the deprecated-but-real Details member this pass added
	t.Run("identity sources details was missing", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		storeOut, err := client.CreatePolicyStore(ctx, &avpsdk.CreatePolicyStoreInput{
			ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
		})
		require.NoError(t, err)

		_, err = client.CreateIdentitySource(ctx, &avpsdk.CreateIdentitySourceInput{
			PolicyStoreId:       storeOut.PolicyStoreId,
			PrincipalEntityType: aws.String("User"),
			Configuration: &types.ConfigurationMemberCognitoUserPoolConfiguration{
				Value: types.CognitoUserPoolConfiguration{
					UserPoolArn: aws.String("arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_abc123"),
					ClientIds:   []string{"client1"},
				},
			},
		})
		require.NoError(t, err)

		out, err := client.ListIdentitySources(ctx, &avpsdk.ListIdentitySourcesInput{
			PolicyStoreId: storeOut.PolicyStoreId,
		})
		require.NoError(t, err)
		require.Len(t, out.IdentitySources, 1)
		item := out.IdentitySources[0]
		require.NotNil(t, item.Details, "Details must round-trip for a Cognito-backed source")
		assert.Equal(t, types.OpenIdIssuerCognito, item.Details.OpenIdIssuer)
		assert.Contains(t, item.Details.ClientIds, "client1")
		assert.Equal(t,
			"arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_abc123",
			aws.ToString(item.Details.UserPoolArn),
		)
		assert.Contains(t, aws.ToString(item.Details.DiscoveryUrl), ".well-known/openid-configuration")
	})

	t.Run("policy store aliases exact", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		storeOut, err := client.CreatePolicyStore(ctx, &avpsdk.CreatePolicyStoreInput{
			ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
		})
		require.NoError(t, err)

		_, err = client.CreatePolicyStoreAlias(ctx, &avpsdk.CreatePolicyStoreAliasInput{
			AliasName:     aws.String("policy-store-alias/my-alias"),
			PolicyStoreId: storeOut.PolicyStoreId,
		})
		require.NoError(t, err)

		out, err := client.ListPolicyStoreAliases(ctx, &avpsdk.ListPolicyStoreAliasesInput{})
		require.NoError(t, err)
		require.Len(t, out.PolicyStoreAliases, 1)
		a := out.PolicyStoreAliases[0]
		assert.Equal(t, "policy-store-alias/my-alias", aws.ToString(a.AliasName))
		assert.NotEmpty(t, a.State)
		assert.NotNil(t, a.CreatedAt)
	})

	t.Run("policy stores exact", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		_, err := client.CreatePolicyStore(ctx, &avpsdk.CreatePolicyStoreInput{
			ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
			Description:        aws.String("d1"),
		})
		require.NoError(t, err)

		out, err := client.ListPolicyStores(ctx, &avpsdk.ListPolicyStoresInput{})
		require.NoError(t, err)
		require.Len(t, out.PolicyStores, 1)
		ps := out.PolicyStores[0]
		assert.Equal(t, "d1", aws.ToString(ps.Description))
		assert.NotNil(t, ps.CreatedDate)
	})

	t.Run("policy templates exact", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)
		ctx := t.Context()

		storeOut, err := client.CreatePolicyStore(ctx, &avpsdk.CreatePolicyStoreInput{
			ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
		})
		require.NoError(t, err)

		_, err = client.CreatePolicyTemplate(ctx, &avpsdk.CreatePolicyTemplateInput{
			PolicyStoreId: storeOut.PolicyStoreId,
			Statement:     aws.String(`permit(principal, action, resource);`),
			Description:   aws.String("tmpl desc"),
		})
		require.NoError(t, err)

		out, err := client.ListPolicyTemplates(ctx, &avpsdk.ListPolicyTemplatesInput{
			PolicyStoreId: storeOut.PolicyStoreId,
		})
		require.NoError(t, err)
		require.Len(t, out.PolicyTemplates, 1)
		tmpl := out.PolicyTemplates[0]
		assert.Equal(t, "tmpl desc", aws.ToString(tmpl.Description))
		assert.NotNil(t, tmpl.CreatedDate)
	})

	t.Run("policies name unsourced", func(t *testing.T) {
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
				Value: types.StaticPolicyDefinition{
					Statement: aws.String(`permit(principal, action, resource);`),
				},
			},
		})
		require.NoError(t, err)

		out, err := client.ListPolicies(ctx, &avpsdk.ListPoliciesInput{
			PolicyStoreId: storeOut.PolicyStoreId,
		})
		require.NoError(t, err)
		require.Len(t, out.Policies, 1)
		assert.NotEmpty(t, aws.ToString(out.Policies[0].PolicyId))
	})
}
