package workspaces

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func directoryIPGroupIDs(b *InMemoryBackend, directoryID string) []string {
	b.mu.RLock("test.directoryIPGroupIDs")
	defer b.mu.RUnlock()

	groups := b.directoryIpGroups[directoryID]
	ids := make([]string, 0, len(groups))

	for id := range groups {
		ids = append(ids, id)
	}

	return ids
}

// dirCertAuthCA returns the stored CertAuth_CertificateAuthorityArn
// property for directoryID and whether it is present at all -- an absent
// key (deleted) differs from a key present with an empty value.
func dirCertAuthCA(b *InMemoryBackend, directoryID string) (string, bool) {
	b.mu.RLock("test.dirCertAuthCA")
	defer b.mu.RUnlock()

	ds, ok := b.dirSettings.Get(directoryID)
	if !ok {
		return "", false
	}

	v, ok := ds.Properties["CertAuth_CertificateAuthorityArn"]

	return v, ok
}

// TestInMemoryBackend_ModifyCertificateBasedAuthProperties_PropertiesToDelete
// documents that PropertiesToDelete removes a previously-set certificate
// auth property from backend state, rather than merely being accepted and
// discarded -- matching real ModifyCertificateBasedAuthPropertiesInput,
// which models PropertiesToDelete as the clear/reset mechanism alongside
// CertificateBasedAuthProperties.
func TestInMemoryBackend_ModifyCertificateBasedAuthProperties_PropertiesToDelete(t *testing.T) {
	t.Parallel()

	const wantARN = "arn:aws:acm-pca:us-east-1:111122223333:certificate-authority/abc"

	b := NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.RegisterWorkspaceDirectory("d-1234567890", nil, nil))

	require.NoError(t, b.ModifyCertificateBasedAuthProperties(
		"d-1234567890",
		map[string]string{"CertificateAuthorityArn": wantARN},
		nil,
	))

	arn, present := dirCertAuthCA(b, "d-1234567890")
	require.True(t, present)
	assert.Equal(t, wantARN, arn)

	require.NoError(t, b.ModifyCertificateBasedAuthProperties(
		"d-1234567890",
		nil,
		[]string{"CERTIFICATE_BASED_AUTH_PROPERTIES_CERTIFICATE_AUTHORITY_ARN"},
	))

	_, present = dirCertAuthCA(b, "d-1234567890")
	assert.False(t, present, "PropertiesToDelete should remove the key, not merely blank its value")
}

// TestInMemoryBackend_SnapshotRestore_DirectoryIpGroupsPersisted documents
// that directoryIpGroups (unlike imagePermissions, clientProperties, and
// appAssociations, which remain ephemeral) now survives a Snapshot -> Restore
// round trip -- fixed alongside the AssociateIpGroups/DisassociateIpGroups
// persistence gap (see PARITY.md gaps history; previously all four raw maps
// were ephemeral, matching pre-Phase-3.3 behavior).
func TestInMemoryBackend_SnapshotRestore_DirectoryIpGroupsPersisted(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1")
	ctx := t.Context()

	require.NoError(t, b.RegisterWorkspaceDirectory("d-1234567890", []string{"subnet-1"}, nil))

	_, err := b.CreateIpGroup("grp1", "desc", nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.AssociateIpGroups("d-1234567890", []string{"grp1"}))

	snap := b.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(ctx, snap))

	dirs, _, err := fresh.DescribeWorkspaceDirectories(ctx, nil, "")
	require.NoError(t, err)
	require.Len(t, dirs, 1)

	groups, _, err := fresh.DescribeIpGroups(nil, 0, "")
	require.NoError(t, err)
	require.Len(t, groups, 1)

	assert.Equal(t, []string{"grp1"}, directoryIPGroupIDs(fresh, "d-1234567890"))
}
