package medialive

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- InputSecurityGroup operations ---

// pruneDeletedInputSecurityGroupsLocked evicts InputSecurityGroups that have
// sat DELETED past medialiveDeletedTTL, so terraform-driven create/delete
// churn doesn't grow this table unbounded in a long-running emulator. Caller
// must hold the write lock.
func (b *InMemoryBackend) pruneDeletedInputSecurityGroupsLocked(now time.Time) {
	for _, g := range b.inputSecurityGroups.All() {
		if g.State == stateDeleted && !g.DeletedAt.IsZero() && now.Sub(g.DeletedAt) >= medialiveDeletedTTL {
			b.inputSecurityGroups.Delete(g.ID)
		}
	}
}

// CreateInputSecurityGroup creates a new input security group.
func (b *InMemoryBackend) CreateInputSecurityGroup(
	whitelistRules []WhitelistRule,
	tags map[string]string,
) (*InputSecurityGroup, error) {
	id := newID()
	rules := make([]WhitelistRule, len(whitelistRules))
	copy(rules, whitelistRules)

	g := &storedInputSecurityGroup{
		ARN:            b.inputSecurityGroupARN(id),
		ID:             id,
		State:          inputSecurityGroupActive,
		WhitelistRules: rules,
		Tags:           copyTags(tags),
	}

	b.mu.Lock("CreateInputSecurityGroup")
	defer b.mu.Unlock()

	b.pruneDeletedInputSecurityGroupsLocked(b.now())
	b.inputSecurityGroups.Put(g)

	return g.toGroup(), nil
}

// DescribeInputSecurityGroup returns an input security group by ID. Takes
// the write lock (not RLock) because it lazily prunes expired DELETED
// entries -- see pruneDeletedInputSecurityGroupsLocked.
func (b *InMemoryBackend) DescribeInputSecurityGroup(groupID string) (*InputSecurityGroup, error) {
	b.mu.Lock("DescribeInputSecurityGroup")
	defer b.mu.Unlock()

	b.pruneDeletedInputSecurityGroupsLocked(b.now())

	g, ok := b.inputSecurityGroups.Get(groupID)
	if !ok {
		return nil, fmt.Errorf("%w: inputSecurityGroup %s not found", ErrNotFound, groupID)
	}

	return g.toGroup(), nil
}

// UpdateInputSecurityGroup updates an input security group's whitelist rules.
func (b *InMemoryBackend) UpdateInputSecurityGroup(
	groupID string,
	whitelistRules []WhitelistRule,
) (*InputSecurityGroup, error) {
	b.mu.Lock("UpdateInputSecurityGroup")
	defer b.mu.Unlock()

	b.pruneDeletedInputSecurityGroupsLocked(b.now())

	g, ok := b.inputSecurityGroups.Get(groupID)
	if !ok {
		return nil, fmt.Errorf("%w: inputSecurityGroup %s not found", ErrNotFound, groupID)
	}

	rules := make([]WhitelistRule, len(whitelistRules))
	copy(rules, whitelistRules)

	g.WhitelistRules = rules

	return g.toGroup(), nil
}

// DeleteInputSecurityGroup marks an input security group DELETED rather
// than removing it outright: terraform-provider-aws's delete waiter
// (waitInputSecurityGroupDeleted, internal/service/medialive/input_security_group.go)
// polls DescribeInputSecurityGroup for State=="DELETED" with a non-empty
// Target, so a NotFound response here is treated as transient and retried
// rather than as the completion signal -- an outright removal left the
// waiter erroring "couldn't find resource" instead of completing.
func (b *InMemoryBackend) DeleteInputSecurityGroup(groupID string) error {
	b.mu.Lock("DeleteInputSecurityGroup")
	defer b.mu.Unlock()

	b.pruneDeletedInputSecurityGroupsLocked(b.now())

	g, ok := b.inputSecurityGroups.Get(groupID)
	if !ok {
		return fmt.Errorf("%w: inputSecurityGroup %s not found", ErrNotFound, groupID)
	}

	g.State = stateDeleted
	g.DeletedAt = b.now()

	return nil
}

// ListInputSecurityGroups returns a paginated list of input security groups.
// Takes the write lock (not RLock) because it lazily prunes expired DELETED
// entries -- see pruneDeletedInputSecurityGroupsLocked.
func (b *InMemoryBackend) ListInputSecurityGroups(
	maxResults int,
	nextToken string,
) ([]*InputSecurityGroupSummary, string, error) {
	b.mu.Lock("ListInputSecurityGroups")
	defer b.mu.Unlock()

	b.pruneDeletedInputSecurityGroupsLocked(b.now())

	all := b.inputSecurityGroups.All()

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*InputSecurityGroupSummary, 0, len(pg.Data))
	for _, g := range pg.Data {
		summaries = append(summaries, g.toSummary())
	}

	return summaries, pg.Next, nil
}
