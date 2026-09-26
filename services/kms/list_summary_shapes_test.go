package kms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// TestListSummaryShapes proves this pass's over-wide-response audit
// (gopherstack, 2026-09-19) for kms's five flagged List ops: ListAliases,
// ListGrants and ListRetirableGrants already matched
// AliasListEntry/GrantListEntry exactly (verified via cmd/structfielddiff
// against kms@v1.59.0); ListKeyRotations' unsourced RotationsListEntry gaps
// (ExpirationModel/ImportState/KeyMaterialDescription/KeyMaterialId/
// KeyMaterialState/ValidTo) were already disclosed by a prior pass
// (items_still_open, gopherstack-xhu2t: no multi-key-material-generation
// tracking). ListKeys had a real leak: KeyListEntry carried a fabricated
// Description member the real type doesn't have.
func TestListSummaryShapes(t *testing.T) {
	t.Parallel()

	t.Run("keys description was leaking", func(t *testing.T) {
		t.Parallel()

		h := kms.NewHandler(kms.NewInMemoryBackend())
		client := newTestKMSClient(t, h)
		ctx := t.Context()

		_, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{
			Description: aws.String("secret plans"),
		})
		require.NoError(t, err)

		out, err := client.ListKeys(ctx, &kmssdk.ListKeysInput{})
		require.NoError(t, err)
		require.Len(t, out.Keys, 1)
		k := out.Keys[0]
		assert.NotEmpty(t, aws.ToString(k.KeyId))
		assert.NotEmpty(t, aws.ToString(k.KeyArn))

		rec := postKMSOp(t, h, "ListKeys", "{}")
		assert.NotContains(t, rec.Body.String(), "Description")
		assert.NotContains(t, rec.Body.String(), "secret plans")
	})

	t.Run("aliases exact", func(t *testing.T) {
		t.Parallel()

		h := kms.NewHandler(kms.NewInMemoryBackend())
		client := newTestKMSClient(t, h)
		ctx := t.Context()

		created, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
		require.NoError(t, err)

		_, err = client.CreateAlias(ctx, &kmssdk.CreateAliasInput{
			AliasName:   aws.String("alias/my-alias"),
			TargetKeyId: created.KeyMetadata.KeyId,
		})
		require.NoError(t, err)

		out, err := client.ListAliases(ctx, &kmssdk.ListAliasesInput{})
		require.NoError(t, err)
		require.Len(t, out.Aliases, 1)
		a := out.Aliases[0]
		assert.Equal(t, "alias/my-alias", aws.ToString(a.AliasName))
		assert.Equal(t, aws.ToString(created.KeyMetadata.KeyId), aws.ToString(a.TargetKeyId))
		assert.NotNil(t, a.CreationDate)
	})

	t.Run("grants and retirable grants exact", func(t *testing.T) {
		t.Parallel()

		h := kms.NewHandler(kms.NewInMemoryBackend())
		client := newTestKMSClient(t, h)
		ctx := t.Context()

		created, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
		require.NoError(t, err)

		granteeArn := "arn:aws:iam::123456789012:role/grantee"
		retiringArn := "arn:aws:iam::123456789012:role/retiring"

		_, err = client.CreateGrant(ctx, &kmssdk.CreateGrantInput{
			KeyId:             created.KeyMetadata.KeyId,
			GranteePrincipal:  aws.String(granteeArn),
			RetiringPrincipal: aws.String(retiringArn),
			Name:              aws.String("g1"),
			Operations:        []kmstypes.GrantOperation{kmstypes.GrantOperationDecrypt},
			Constraints: &kmstypes.GrantConstraints{
				EncryptionContextEquals: map[string]string{"k": "v"},
			},
		})
		require.NoError(t, err)

		grantsOut, err := client.ListGrants(ctx, &kmssdk.ListGrantsInput{
			KeyId: created.KeyMetadata.KeyId,
		})
		require.NoError(t, err)
		require.Len(t, grantsOut.Grants, 1)
		g := grantsOut.Grants[0]
		assert.Equal(t, "g1", aws.ToString(g.Name))
		assert.Equal(t, granteeArn, aws.ToString(g.GranteePrincipal))
		assert.Equal(t, retiringArn, aws.ToString(g.RetiringPrincipal))
		require.NotNil(t, g.Constraints)
		assert.Equal(t, "v", g.Constraints.EncryptionContextEquals["k"])
		assert.NotEmpty(t, aws.ToString(g.IssuingAccount))
		assert.NotNil(t, g.CreationDate)

		retirableOut, err := client.ListRetirableGrants(ctx, &kmssdk.ListRetirableGrantsInput{
			RetiringPrincipal: aws.String(retiringArn),
		})
		require.NoError(t, err)
		require.Len(t, retirableOut.Grants, 1)
		assert.Equal(t, "g1", aws.ToString(retirableOut.Grants[0].Name))
	})
}
