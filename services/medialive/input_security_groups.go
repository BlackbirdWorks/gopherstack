package medialive

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- InputSecurityGroup operations ---

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

	b.inputSecurityGroups.Put(g)

	return g.toGroup(), nil
}

// DescribeInputSecurityGroup returns an input security group by ID.
func (b *InMemoryBackend) DescribeInputSecurityGroup(groupID string) (*InputSecurityGroup, error) {
	b.mu.RLock("DescribeInputSecurityGroup")
	defer b.mu.RUnlock()

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

	g, ok := b.inputSecurityGroups.Get(groupID)
	if !ok {
		return fmt.Errorf("%w: inputSecurityGroup %s not found", ErrNotFound, groupID)
	}

	g.State = stateDeleted

	return nil
}

// ListInputSecurityGroups returns a paginated list of input security groups.
func (b *InMemoryBackend) ListInputSecurityGroups(
	maxResults int,
	nextToken string,
) ([]*InputSecurityGroupSummary, string, error) {
	b.mu.RLock("ListInputSecurityGroups")
	defer b.mu.RUnlock()

	all := b.inputSecurityGroups.All()

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	summaries := make([]*InputSecurityGroupSummary, 0, len(pg.Data))
	for _, g := range pg.Data {
		summaries = append(summaries, g.toSummary())
	}

	return summaries, pg.Next, nil
}
