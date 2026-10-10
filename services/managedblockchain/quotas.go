package managedblockchain

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

// Default quotas from the AWS General Reference "Amazon Managed Blockchain endpoints and quotas".
const (
	maxEthereumNodesPerAccount = 50
	maxNetworksPerEdition      = 6

	maxMembersStarter  = 5
	maxMembersStandard = 14
	maxPeersStarter    = 2
	maxPeersStandard   = 3

	editionStarter  = "STARTER"
	editionStandard = "STANDARD"
)

// Per-edition limits from the AWS Managed Blockchain pricing page edition comparison.
func maxMembersPerNetwork(edition string) int {
	if edition == editionStandard {
		return maxMembersStandard
	}

	return maxMembersStarter
}

func maxPeerNodesPerMember(edition string) int {
	if edition == editionStandard {
		return maxPeersStandard
	}

	return maxPeersStarter
}

func fabricEditionOf(n *Network) string {
	if n == nil || n.FrameworkAttributes == nil || n.FrameworkAttributes.Fabric == nil {
		return ""
	}

	e := n.FrameworkAttributes.Fabric.Edition
	if e != editionStarter && e != editionStandard {
		return ""
	}

	return e
}

func (b *InMemoryBackend) memberCountLocked(networkID string) int {
	n := 0

	for _, m := range b.members.All() {
		if m.NetworkID == networkID {
			n++
		}
	}

	return n
}

func (b *InMemoryBackend) peerNodeCountLocked(networkID, memberID string) int {
	n := 0

	for _, node := range b.nodes.All() {
		if node.NetworkID == networkID && node.MemberID == memberID {
			n++
		}
	}

	return n
}

// ErrResourceLimitExceeded maps to ResourceLimitExceededException (HTTP 429).
var ErrResourceLimitExceeded = awserr.New(
	"ResourceLimitExceededException: resource limit exceeded",
	awserr.ErrInvalidParameter,
)

func (b *InMemoryBackend) ethereumNodeCountLocked() int {
	n := 0

	for _, node := range b.nodes.All() {
		if node.NetworkID == ethereumMainnetNetworkID {
			n++
		}
	}

	return n
}

// networksWithOwnedMemberLocked counts networks of the given Fabric edition where this account has a member.
func (b *InMemoryBackend) networksWithOwnedMemberLocked(edition string) int {
	seen := map[string]struct{}{}

	for _, m := range b.members.All() {
		if !m.IsOwned {
			continue
		}

		net, ok := b.networks.Get(m.NetworkID)
		if !ok || net.FrameworkAttributes == nil || net.FrameworkAttributes.Fabric == nil {
			continue
		}

		if net.FrameworkAttributes.Fabric.Edition == edition {
			seen[net.ID] = struct{}{}
		}
	}

	return len(seen)
}

func (b *InMemoryBackend) hasOwnedMemberLocked(networkID string) bool {
	for _, m := range b.members.All() {
		if m.IsOwned && m.NetworkID == networkID {
			return true
		}
	}

	return false
}
