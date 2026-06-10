package opensearch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// pkgNames extracts and returns the package IDs from a ListPackagesForDomain result.
func pkgNames(pkgs []*opensearch.Package) []string {
	out := make([]string, 0, len(pkgs))
	for _, p := range pkgs {
		out = append(out, p.PackageID)
	}

	return out
}

// TestDomainPackagesReverseIndexConsistency exercises the domainPackages reverse
// index that backs ListPackagesForDomain and the DeleteDomain cascade. It verifies
// that the domain→packages view stays consistent with the package→domains view
// across associate, dissociate, and delete-domain operations.
func TestDomainPackagesReverseIndexConsistency(t *testing.T) {
	t.Parallel()

	t.Run("associate_and_dissociate", func(t *testing.T) {
		t.Parallel()

		b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
		b.AddDomainInternal("dom-a", "")
		b.AddDomainInternal("dom-b", "")
		b.AddPackageInternal("pkg-1", "p1", "TXT-DICTIONARY")
		b.AddPackageInternal("pkg-2", "p2", "TXT-DICTIONARY")

		// pkg-1 -> dom-a, dom-b ; pkg-2 -> dom-a
		_, err := b.AssociatePackage("pkg-1", "dom-a")
		require.NoError(t, err)
		_, err = b.AssociatePackage("pkg-1", "dom-b")
		require.NoError(t, err)
		_, err = b.AssociatePackage("pkg-2", "dom-a")
		require.NoError(t, err)

		// Reverse index (domain -> packages) must agree with the associations.
		assert.ElementsMatch(t, []string{"pkg-1", "pkg-2"}, pkgNames(b.ListPackagesForDomain("dom-a")))
		assert.ElementsMatch(t, []string{"pkg-1"}, pkgNames(b.ListPackagesForDomain("dom-b")))

		// Forward index (package -> domains) must agree too.
		assert.ElementsMatch(t, []string{"dom-a", "dom-b"}, b.ListDomainsForPackage("pkg-1"))
		assert.ElementsMatch(t, []string{"dom-a"}, b.ListDomainsForPackage("pkg-2"))

		// Dissociate pkg-1 from dom-a; both directions must update.
		_, err = b.DissociatePackage("pkg-1", "dom-a")
		require.NoError(t, err)

		assert.ElementsMatch(t, []string{"pkg-2"}, pkgNames(b.ListPackagesForDomain("dom-a")))
		assert.ElementsMatch(t, []string{"dom-b"}, b.ListDomainsForPackage("pkg-1"))
	})

	t.Run("delete_domain_clears_associations", func(t *testing.T) {
		t.Parallel()

		b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
		b.AddDomainInternal("dom-a", "")
		b.AddDomainInternal("dom-b", "")
		b.AddPackageInternal("pkg-1", "p1", "TXT-DICTIONARY")

		_, err := b.AssociatePackage("pkg-1", "dom-a")
		require.NoError(t, err)
		_, err = b.AssociatePackage("pkg-1", "dom-b")
		require.NoError(t, err)

		// Deleting dom-a must remove dom-a from the package's domain set and from
		// the reverse index, while leaving dom-b intact.
		_, err = b.DeleteDomain("dom-a")
		require.NoError(t, err)

		assert.Empty(t, b.ListPackagesForDomain("dom-a"))
		assert.ElementsMatch(t, []string{"dom-b"}, b.ListDomainsForPackage("pkg-1"))
		assert.ElementsMatch(t, []string{"pkg-1"}, pkgNames(b.ListPackagesForDomain("dom-b")))
	})
}
