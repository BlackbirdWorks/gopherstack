package timestreaminfluxdb

import (
	"cmp"
	"context"
	"encoding/json"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const maxGroups = 50

// GroupInput holds CreateDbParameterGroup parameters.
type GroupInput struct {
	Tags        map[string]string
	Name        string
	Description string
	Parameters  json.RawMessage
}

// CreateDBParameterGroup creates a parameter group.
func (b *InMemoryBackend) CreateDBParameterGroup(ctx context.Context, in GroupInput) (wParameterGroup, error) {
	for _, err := range []error{
		validateName(in.Name), validateTags(in.Tags), validateParameters(in.Parameters),
	} {
		if err != nil {
			return wParameterGroup{}, err
		}
	}

	if len(in.Description) > maxDescription {
		return wParameterGroup{}, invalid("description must be at most %d characters", maxDescription)
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("CreateDbParameterGroup")
	defer b.mu.Unlock()

	taken, count := false, 0

	b.groups.Range(func(g *DBParameterGroup) bool {
		if sc.owns(g.AccountID, g.Region) {
			count++

			taken = taken || lowerEq(g.Name, in.Name)
		}

		return true
	})

	if taken {
		return wParameterGroup{}, conflict("DB parameter group %s already exists", in.Name)
	}

	if count >= maxGroups {
		return wParameterGroup{}, quota("DB parameter groups per account and region", maxGroups)
	}

	id := newID()
	g := &DBParameterGroup{
		ID: id, Name: in.Name, ARN: sc.arn("db-parameter-group/" + id), Description: in.Description,
		AccountID: sc.account, Region: sc.region, Parameters: slices.Clone(in.Parameters), Tags: cloneTags(in.Tags),
	}
	b.groups.Put(g)

	return wireGroup(g), nil
}

func (b *InMemoryBackend) groupLocked(sc scope, id string) (*DBParameterGroup, error) {
	g, ok := b.groups.Get(id)
	if !ok || !sc.owns(g.AccountID, g.Region) {
		return nil, notFound("DB parameter group", id)
	}

	return g, nil
}

// GetDBParameterGroup returns a parameter group.
func (b *InMemoryBackend) GetDBParameterGroup(ctx context.Context, id string) (wParameterGroup, error) {
	b.mu.RLock("GetDbParameterGroup")
	defer b.mu.RUnlock()

	g, err := b.groupLocked(b.scopeOf(ctx), id)
	if err != nil {
		return wParameterGroup{}, err
	}

	return wireGroup(g), nil
}

// ListDBParameterGroups lists parameter groups ordered by name.
func (b *InMemoryBackend) ListDBParameterGroups(
	ctx context.Context, token string, maxResults int32,
) (page.Page[wParameterGroupSummary], error) {
	if err := validateListArgs(maxResults); err != nil {
		return page.Page[wParameterGroupSummary]{}, err
	}

	sc := b.scopeOf(ctx)

	b.mu.RLock("ListDbParameterGroups")
	defer b.mu.RUnlock()

	var items []*DBParameterGroup

	b.groups.Range(func(g *DBParameterGroup) bool {
		if sc.owns(g.AccountID, g.Region) {
			items = append(items, g)
		}

		return true
	})

	slices.SortFunc(items, func(x, y *DBParameterGroup) int {
		return cmp.Or(cmp.Compare(x.Name, y.Name), cmp.Compare(x.ID, y.ID))
	})

	pg, err := paginate(items, token, maxResults)
	if err != nil {
		return page.Page[wParameterGroupSummary]{}, err
	}

	out := page.Page[wParameterGroupSummary]{Next: pg.Next, Data: make([]wParameterGroupSummary, 0, len(pg.Data))}
	for _, g := range pg.Data {
		out.Data = append(
			out.Data,
			wParameterGroupSummary{Arn: g.ARN, ID: g.ID, Name: g.Name, Description: g.Description},
		)
	}

	return out, nil
}
