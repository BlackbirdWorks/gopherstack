package azureeventhubs

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// SetNow replaces the backend clock for retention tests.
func SetNow(b *InMemoryBackend, fn func() time.Time) { b.now = fn }

// ClaimRights exposes SAS token validation.
func ClaimRights(b *InMemoryBackend, token string) ([]string, bool) {
	claim, ok := b.claimRights(token)

	return claim.Rights, ok
}

// ClaimAudience exposes the hub a SAS token's audience names.
func ClaimAudience(b *InMemoryBackend, token string) (string, bool) {
	claim, ok := b.claimRights(token)

	return claim.Audience, ok
}

// ReceiverCount reports registered receivers across all partitions.
func ReceiverCount(b *InMemoryBackend) int {
	b.mu.RLock("ReceiverCount")
	defer b.mu.RUnlock()

	n := 0
	for _, r := range b.receivers {
		n += len(r)
	}

	return n
}

// GroupHarness drives the consumer group coordinator with a fake clock.
type GroupHarness struct{ gc *groupCoordinator }

// NewGroupHarness returns a coordinator reading time from now.
func NewGroupHarness(now func() time.Time) *GroupHarness {
	return &GroupHarness{gc: newGroupCoordinator(now)}
}

// Sweep expires members.
func (g *GroupHarness) Sweep() { g.gc.sweep() }

// Join joins as member (empty for a new member) and returns the response.
func (g *GroupHarness) Join(ctx context.Context, group, member string, version int16) *kmsg.JoinGroupResponse {
	req := kmsg.NewPtrJoinGroupRequest()
	req.Version = version
	req.Group, req.MemberID, req.ProtocolType = group, member, "consumer"
	req.SessionTimeoutMillis, req.RebalanceTimeoutMillis = 10000, 10000
	req.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range", Metadata: []byte("m")}}

	return g.gc.join(ctx, groupKey("", group), req)
}

// Heartbeat sends a heartbeat and returns the error code.
func (g *GroupHarness) Heartbeat(group, member string, generation int32) int16 {
	return g.gc.heartbeat(groupKey("", group), member, generation)
}

// Members lists the member IDs of a group.
func (g *GroupHarness) Members(group string) []string { return g.gc.memberIDs(groupKey("", group)) }
