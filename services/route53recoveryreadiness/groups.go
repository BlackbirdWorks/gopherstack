package route53recoveryreadiness

import (
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// GroupView is the wire form of a recovery group.
type GroupView struct {
	Tags              map[string]string `json:"tags,omitempty"`
	RecoveryGroupArn  string            `json:"recoveryGroupArn"`
	RecoveryGroupName string            `json:"recoveryGroupName"`
	Cells             []string          `json:"cells"`
}

func groupView(g *RecoveryGroup) GroupView {
	return GroupView{
		RecoveryGroupArn: g.ARN, RecoveryGroupName: g.Name, Cells: slices.Clone(g.Cells), Tags: cloneTags(g.Tags),
	}
}

// CreateRecoveryGroup creates a group over top-level cells.
func (b *InMemoryBackend) CreateRecoveryGroup(name string, cells []string, tags map[string]string) (GroupView, error) {
	if err := validName("recovery group", name); err != nil {
		return GroupView{}, err
	}

	b.mu.Lock("CreateRecoveryGroup")
	defer b.mu.Unlock()

	if b.groups.Has(name) {
		return GroupView{}, restroute.Conflict("Recovery group %s already exists", name)
	}

	arn := b.arnFor("recovery-group", name)

	kids, err := b.adopt(arn, cells)
	if err != nil {
		return GroupView{}, err
	}

	g := &RecoveryGroup{ARN: arn, Name: name, Cells: kids, Tags: cloneTags(tags)}
	b.groups.Put(g)

	return groupView(g), nil
}

// GetRecoveryGroup returns a group.
func (b *InMemoryBackend) GetRecoveryGroup(name string) (GroupView, error) {
	b.mu.RLock("GetRecoveryGroup")
	defer b.mu.RUnlock()

	g, err := b.getGroup(name)
	if err != nil {
		return GroupView{}, err
	}

	return groupView(g), nil
}

// ListRecoveryGroups lists groups.
func (b *InMemoryBackend) ListRecoveryGroups(token string, maxResults int) ([]GroupView, string, error) {
	b.mu.RLock("ListRecoveryGroups")
	defer b.mu.RUnlock()

	pg, err := restroute.Paginate(sorted(b.groups.All(), func(g *RecoveryGroup) string { return g.Name }),
		token, maxResults, listDefault, listMax)
	if err != nil {
		return nil, "", err
	}

	out := make([]GroupView, 0, len(pg.Data))
	for _, g := range pg.Data {
		out = append(out, groupView(g))
	}

	return out, pg.Next, nil
}

// UpdateRecoveryGroup replaces a group's cell list.
func (b *InMemoryBackend) UpdateRecoveryGroup(name string, cells []string) (GroupView, error) {
	b.mu.Lock("UpdateRecoveryGroup")
	defer b.mu.Unlock()

	g, err := b.getGroup(name)
	if err != nil {
		return GroupView{}, err
	}

	if err = b.attachChildren(g.ARN, "", cells); err != nil {
		return GroupView{}, err
	}

	b.setParent(g.Cells, "")
	b.setParent(cells, g.ARN)
	g.Cells = slices.Clone(cells)

	if g.Cells == nil {
		g.Cells = []string{}
	}

	return groupView(g), nil
}

// DeleteRecoveryGroup deletes a group; its cells become top-level again.
func (b *InMemoryBackend) DeleteRecoveryGroup(name string) error {
	b.mu.Lock("DeleteRecoveryGroup")
	defer b.mu.Unlock()

	g, err := b.getGroup(name)
	if err != nil {
		return err
	}

	b.setParent(g.Cells, "")
	b.groups.Delete(name)

	return nil
}
