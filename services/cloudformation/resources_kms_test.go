package cloudformation_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
)

// TestResourceCreator_Extra_KMSAlias verifies an alias is created against a real key and that
// Fn::GetAtt Arn returns a real KMS ARN.
func TestResourceCreator_Extra_KMSAlias(t *testing.T) {
	t.Parallel()

	backends := newDependentServiceBackends(t)
	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()
	kmsb, ok := backends.KMS.Backend.(*kmsbackend.InMemoryBackend)
	require.True(t, ok)

	// Create a real key to point the alias at.
	keyPhys, err := rc.Create(ctx, "MyKey", "AWS::KMS::Key", map[string]any{}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, keyPhys)

	aliasPhys, err := rc.Create(ctx, "MyAlias", "AWS::KMS::Alias",
		map[string]any{"AliasName": "alias/phase5", "TargetKeyId": keyPhys}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "alias/phase5", aliasPhys)

	aliases, err := kmsb.ListAliases(context.Background(), &kmsbackend.ListAliasesInput{})
	require.NoError(t, err)
	found := false
	for _, a := range aliases.Aliases {
		if a.AliasName == "alias/phase5" {
			found = true
		}
	}
	assert.True(t, found, "alias should exist in KMS backend")

	got := cloudformation.GetResourceAttribute("AWS::KMS::Alias", aliasPhys, "Arn", "000000000000", "us-east-1")
	assert.Contains(t, got, "alias/phase5")
	assert.Contains(t, got, "arn:aws:kms")

	require.NoError(t, rc.Delete(ctx, "AWS::KMS::Alias", aliasPhys, nil))
}
