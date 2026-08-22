package verifiedpermissions

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func resourceTagsSize(b *InMemoryBackend) int {
	b.mu.RLock("test.resourceTagsSize")
	defer b.mu.RUnlock()

	return len(b.resourceTags)
}

func whiteboxSeedPolicyStore(t *testing.T, b *InMemoryBackend, desc string) *PolicyStore {
	t.Helper()

	ps, err := b.CreatePolicyStore(desc, nil, "OFF", "", "")
	require.NoError(t, err)

	return ps
}

// TestBackend_DeleteOps_NoOrphanedResourceTags locks in a leak fix: every
// Delete* operation (DeletePolicy, DeleteIdentitySource,
// DeletePolicyTemplate's cascade, DeletePolicyStore's cascade) must remove
// the deleted resource's entry from resourceTags along with its ARN index
// entry, so a tagged-then-deleted resource doesn't leave a ghost row behind
// forever.
func TestBackend_DeleteOps_NoOrphanedResourceTags(t *testing.T) {
	t.Parallel()

	t.Run("DeletePolicy", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend("123456789012", "us-east-1")
		ps := whiteboxSeedPolicyStore(t, b, "store")
		p, err := b.CreatePolicy(ps.PolicyStoreID, CreatePolicyParams{
			PolicyType: "STATIC", Statement: "permit(principal, action, resource);",
		})
		require.NoError(t, err)

		polARN := "arn:aws:verifiedpermissions::123456789012:policy/" + ps.PolicyStoreID + "/" + p.PolicyID
		require.NoError(t, b.TagResource(polARN, map[string]string{"k": "v"}))

		before := resourceTagsSize(b)
		require.NoError(t, b.DeletePolicy(ps.PolicyStoreID, p.PolicyID))
		assert.Less(t, resourceTagsSize(b), before)

		_, err = b.ListTagsForResource(polARN)
		require.Error(t, err)
	})

	t.Run("DeletePolicyTemplate cascades to linked policy tags", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend("123456789012", "us-east-1")
		ps := whiteboxSeedPolicyStore(t, b, "store")
		tmpl, err := b.CreatePolicyTemplate(
			ps.PolicyStoreID, "tmpl", `permit(principal == ?principal, action, resource);`, "", "",
		)
		require.NoError(t, err)

		linked, err := b.CreatePolicy(ps.PolicyStoreID, CreatePolicyParams{
			PolicyType: "TEMPLATE_LINKED", PolicyTemplateID: tmpl.PolicyTemplateID,
			PrincipalEntityType: "User", PrincipalEntityID: "alice",
		})
		require.NoError(t, err)

		polARN := "arn:aws:verifiedpermissions::123456789012:policy/" + ps.PolicyStoreID + "/" + linked.PolicyID
		tmplARN := "arn:aws:verifiedpermissions::123456789012:policy-template/" +
			ps.PolicyStoreID + "/" + tmpl.PolicyTemplateID
		require.NoError(t, b.TagResource(polARN, map[string]string{"k": "v"}))
		require.NoError(t, b.TagResource(tmplARN, map[string]string{"k": "v"}))

		before := resourceTagsSize(b)
		require.NoError(t, b.DeletePolicyTemplate(ps.PolicyStoreID, tmpl.PolicyTemplateID))
		assert.Equal(t, before-2, resourceTagsSize(b))
	})

	t.Run("DeletePolicyStore cascades to all child resource tags", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend("123456789012", "us-east-1")
		ps := whiteboxSeedPolicyStore(t, b, "store")
		p, err := b.CreatePolicy(ps.PolicyStoreID, CreatePolicyParams{
			PolicyType: "STATIC", Statement: "permit(principal, action, resource);",
		})
		require.NoError(t, err)

		polARN := "arn:aws:verifiedpermissions::123456789012:policy/" + ps.PolicyStoreID + "/" + p.PolicyID
		require.NoError(t, b.TagResource(ps.Arn, map[string]string{"k": "v"}))
		require.NoError(t, b.TagResource(polARN, map[string]string{"k": "v"}))

		before := resourceTagsSize(b)
		require.NoError(t, b.DeletePolicyStore(ps.PolicyStoreID))
		assert.Equal(t, before-2, resourceTagsSize(b))
	})
}
