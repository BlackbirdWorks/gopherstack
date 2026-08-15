package organizations

import (
	"slices"
	"strings"
)

// maxPathWalk bounds ancestor-chain traversal so a malformed (cyclic or
// dangling) ouParent chain can never loop forever. maxOUDepth (5) is the
// deepest a legal OU nesting can go, so this leaves margin for the full
// legal chain plus a couple of hops before giving up.
const maxPathWalk = maxOUDepth + 2

// ancestorOUChainLocked walks up from parentID (an OU or root ID) to the
// root, returning ancestor OU IDs from outermost (child-of-root) to
// innermost (parentID itself), the order AWS's path strings list them in.
// Returns (nil, true) when parentID is the root directly (no OU segments).
// ok is false if the chain doesn't reach the root within maxPathWalk hops --
// a cycle or a dangling/orphaned parent -- which callers must treat as an
// undeterminable path rather than fabricate one. Must be called with the
// lock held.
func (b *InMemoryBackend) ancestorOUChainLocked(parentID string) ([]string, bool) {
	if b.root != nil && parentID == b.root.ID {
		return nil, true
	}

	var chain []string

	current := parentID

	for range maxPathWalk {
		chain = append(chain, current)

		next, exists := b.ouParent[current]
		if !exists {
			return nil, false
		}

		if b.root != nil && next == b.root.ID {
			slices.Reverse(chain)

			return chain, true
		}

		current = next
	}

	return nil, false
}

// pathFixedSegments counts the org ID, root ID and the resource's own ID --
// the three buildPath segments that aren't ancestor OUs.
const pathFixedSegments = 3

// buildPath joins the org ID, root ID, ancestor OU IDs (root-to-leaf order)
// and the resource's own ID into AWS's documented path format. Verified
// against the AWS API Reference example responses for DescribeAccount
// ("o-exampleorgid/r-examplerootid111/555555555555/") and
// DescribeOrganizationalUnit
// ("o-exampleorgid/r-examplerootid111/ou-examplerootid111-exampleouid111/"),
// and against the Account.Paths / OrganizationalUnit.Path regex pattern
// published on those same pages:
// ^(o-[a-z0-9]{10,32}/r-[0-9a-z]{4,32}(/ou-[0-9a-z]{4,32}-[a-z0-9]{8,32})*(/\d{12})*)/.
func buildPath(orgID, rootID string, ancestorOUs []string, ownID string) string {
	segments := make([]string, 0, len(ancestorOUs)+pathFixedSegments)
	segments = append(segments, orgID, rootID)
	segments = append(segments, ancestorOUs...)
	segments = append(segments, ownID)

	return strings.Join(segments, "/") + "/"
}

// accountPathsLocked computes the Paths value for an account: AWS models an
// account as having exactly one parent (a strict tree, no multi-parenting --
// MoveAccount moves between exactly one source and one destination), so this
// always returns a single-element slice when the chain resolves. It returns
// nil for an account with no recorded parent, or one whose parent chain is
// detached or cyclic -- an honest "undeterminable" rather than a fabricated
// path. Must be called with the lock held.
func (b *InMemoryBackend) accountPathsLocked(acctID string) []string {
	if b.org == nil || b.root == nil {
		return nil
	}

	parentID, ok := b.accountParent[acctID]
	if !ok {
		return nil
	}

	ancestors, ok := b.ancestorOUChainLocked(parentID)
	if !ok {
		return nil
	}

	return []string{buildPath(b.org.ID, b.root.ID, ancestors, acctID)}
}

// ouPathLocked computes the Path value for an OU: its ancestor chain plus
// its own ID. Returns "" for the same detached/cyclic cases as
// accountPathsLocked. Must be called with the lock held.
func (b *InMemoryBackend) ouPathLocked(ou *OrganizationalUnit) string {
	if b.org == nil || b.root == nil {
		return ""
	}

	ancestors, ok := b.ancestorOUChainLocked(ou.ParentID)
	if !ok {
		return ""
	}

	return buildPath(b.org.ID, b.root.ID, ancestors, ou.ID)
}
