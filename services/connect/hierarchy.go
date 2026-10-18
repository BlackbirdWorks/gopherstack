package connect

import (
	"context"
	"slices"

	"github.com/google/uuid"
)

const maxHierarchyDepth = 5

func levelNames() [maxHierarchyDepth]string {
	return [maxHierarchyDepth]string{
		"LevelOne",
		"LevelTwo",
		"LevelThree",
		"LevelFour",
		"LevelFive",
	}
}

// CreateUserHierarchyGroupInput is the CreateUserHierarchyGroup request.
type CreateUserHierarchyGroupInput struct {
	Tags          map[string]string
	Name          string
	ParentGroupID string `json:"ParentGroupId"`
	instPath
}

// UpdateHierarchyGroupNameInput is the UpdateUserHierarchyGroupName request.
type UpdateHierarchyGroupNameInput struct {
	Name    string
	GroupID string `json:"-" path:"HierarchyGroupId"`
	instPath
}

// HierarchyStructureUpdate renames hierarchy levels; an absent level is untouched.
type HierarchyStructureUpdate struct {
	LevelOne   *HierarchyLevelUpdate `json:"LevelOne,omitempty"`
	LevelTwo   *HierarchyLevelUpdate `json:"LevelTwo,omitempty"`
	LevelThree *HierarchyLevelUpdate `json:"LevelThree,omitempty"`
	LevelFour  *HierarchyLevelUpdate `json:"LevelFour,omitempty"`
	LevelFive  *HierarchyLevelUpdate `json:"LevelFive,omitempty"`
}

// HierarchyLevelUpdate carries a level's new name.
type HierarchyLevelUpdate struct {
	Name string `json:"Name"`
}

// UpdateHierarchyStructureInput is the UpdateUserHierarchyStructure request.
type UpdateHierarchyStructureInput struct {
	HierarchyStructure HierarchyStructureUpdate
	instPath
}

func levelSlots(s *HierarchyStructure) [maxHierarchyDepth]**HierarchyLevel {
	return [maxHierarchyDepth]**HierarchyLevel{&s.LevelOne, &s.LevelTwo, &s.LevelThree, &s.LevelFour, &s.LevelFive}
}

func pathSlots(p *HierarchyPath) [maxHierarchyDepth]**HierarchyGroupSummary {
	return [maxHierarchyDepth]**HierarchyGroupSummary{
		&p.LevelOne,
		&p.LevelTwo,
		&p.LevelThree,
		&p.LevelFour,
		&p.LevelFive,
	}
}

func (b *Backend) structureLocked(inst string) *hierarchyStructureRec {
	if s, ok := b.hierarchyLevels.Get(inst); ok {
		return s
	}

	s := &hierarchyStructureRec{InstanceID: inst}
	b.hierarchyLevels.Put(s)

	return s
}

func updateSlots(u *HierarchyStructureUpdate) [maxHierarchyDepth]*HierarchyLevelUpdate {
	return [maxHierarchyDepth]*HierarchyLevelUpdate{u.LevelOne, u.LevelTwo, u.LevelThree, u.LevelFour, u.LevelFive}
}

// UpdateUserHierarchyStructure defines or renames hierarchy levels; levels must be defined top-down.
func (b *Backend) UpdateUserHierarchyStructure(ctx context.Context, in *UpdateHierarchyStructureInput) (*empty, error) {
	updates := updateSlots(&in.HierarchyStructure)

	for _, u := range updates {
		if u != nil {
			if err := checkName("HierarchyStructure level Name", u.Name, maxNameLen); err != nil {
				return nil, err
			}
		}
	}

	b.mu.Lock("UpdateUserHierarchyStructure")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID
	st := b.structureLocked(inst)
	slots := levelSlots(&st.Structure)

	aboveDefined := true

	for i, u := range updates {
		if u != nil && !aboveDefined {
			return nil, invalidParamErr("%s cannot be defined before the level above it", levelName(i))
		}

		aboveDefined = u != nil || *slots[i] != nil
	}

	for i, u := range updates {
		if u == nil {
			continue
		}

		if *slots[i] == nil {
			id := uuid.NewString()
			*slots[i] = &HierarchyLevel{ID: id, Arn: b.buildARN(ctx, "instance/"+inst+"/agent-group-level/"+id)}
		}

		(*slots[i]).Name = u.Name
		(*slots[i]).Stamp = b.stamp(ctx)
	}

	return &empty{}, nil
}

// DescribeUserHierarchyStructure returns the defined hierarchy levels.
func (b *Backend) DescribeUserHierarchyStructure(inst string) (*HierarchyStructure, error) {
	b.mu.RLock("DescribeUserHierarchyStructure")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	out := HierarchyStructure{}

	if s, ok := b.hierarchyLevels.Get(r.Instance.ID); ok {
		out = s.Structure
	}

	return &out, nil
}

func (b *Backend) groupPathLocked(inst string, g *hierarchyGroupRec) *HierarchyPath {
	var chain []*hierarchyGroupRec

	for cur := g; cur != nil; {
		chain = append(chain, cur)

		if cur.ParentID == "" {
			break
		}

		cur, _ = b.hierarchyGroups.table.Get(inst + "/" + cur.ParentID)
	}

	slices.Reverse(chain)

	path := &HierarchyPath{}
	slots := pathSlots(path)

	for i, c := range chain {
		if i < maxHierarchyDepth {
			*slots[i] = &HierarchyGroupSummary{Arn: c.Group.Arn, ID: c.Group.ID, Name: c.Group.Name}
		}
	}

	return path
}

// CreateUserHierarchyGroup creates a group at the next free level under an optional parent.
func (b *Backend) CreateUserHierarchyGroup(
	ctx context.Context,
	in *CreateUserHierarchyGroupInput,
) (*hierarchyGroupOut, error) {
	if err := checkName("Name", in.Name, maxNameLen); err != nil {
		return nil, err
	}

	if err := checkTags(in.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateUserHierarchyGroup")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID
	st := b.structureLocked(inst)
	depth := 0

	if in.ParentGroupID != "" {
		parent, perr := b.hierarchyGroups.get(inst, in.ParentGroupID)
		if perr != nil {
			return nil, perr
		}

		for cur := parent; cur != nil; depth++ {
			cur, _ = b.hierarchyGroups.table.Get(inst + "/" + cur.ParentID)
		}
	}

	if depth >= maxHierarchyDepth {
		return nil, limitErr("the hierarchy can have at most %d levels", maxHierarchyDepth)
	}

	level := *levelSlots(&st.Structure)[depth]
	if level == nil {
		return nil, invalidRequestErr("%s of the hierarchy structure is not defined", levelName(depth))
	}

	for _, g := range b.hierarchyGroups.in(inst) {
		if g.ParentID == in.ParentGroupID && g.Group.Name == in.Name {
			return nil, duplicateErr("HierarchyGroup", in.Name)
		}
	}

	id := uuid.NewString()
	rec := &hierarchyGroupRec{InstanceID: inst, ParentID: in.ParentGroupID, Group: HierarchyGroup{
		Stamp: b.stamp(ctx), Tags: cloneTags(in.Tags), Arn: b.buildARN(ctx, "instance/"+inst+"/agent-group/"+id),
		ID: id, LevelID: level.ID, Name: in.Name,
	}}
	b.hierarchyGroups.put(rec)

	return &hierarchyGroupOut{HierarchyGroupArn: rec.Group.Arn, HierarchyGroupID: id}, nil
}

// DescribeUserHierarchyGroup returns a group with its path.
func (b *Backend) DescribeUserHierarchyGroup(inst, id string) (*HierarchyGroup, error) {
	b.mu.RLock("DescribeUserHierarchyGroup")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	g, err := b.hierarchyGroups.get(r.Instance.ID, id)
	if err != nil {
		return nil, err
	}

	out := g.Group
	out.Tags = cloneTags(out.Tags)
	out.HierarchyPath = b.groupPathLocked(r.Instance.ID, g)

	return &out, nil
}

// ListUserHierarchyGroups pages through hierarchy groups.
func (b *Backend) ListUserHierarchyGroups(inst, token string, maxResults int32) ([]Summary, string, error) {
	b.mu.RLock("ListUserHierarchyGroups")

	r, err := b.instanceLocked(inst)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []Summary
	for _, g := range b.hierarchyGroups.in(r.Instance.ID) {
		all = append(all, Summary{Stamp: g.Group.Stamp, Arn: g.Group.Arn, ID: g.Group.ID, Name: g.Group.Name})
	}

	b.mu.RUnlock()

	return paged(all, token, maxResults)
}

// UpdateUserHierarchyGroupName renames a group.
func (b *Backend) UpdateUserHierarchyGroupName(ctx context.Context, in *UpdateHierarchyGroupNameInput) (*empty, error) {
	if err := checkName("Name", in.Name, maxNameLen); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateUserHierarchyGroupName")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	g, err := b.hierarchyGroups.get(r.Instance.ID, in.GroupID)
	if err != nil {
		return nil, err
	}

	for _, o := range b.hierarchyGroups.in(r.Instance.ID) {
		if o.ParentID == g.ParentID && o.Group.Name == in.Name && o.Group.ID != g.Group.ID {
			return nil, duplicateErr("HierarchyGroup", in.Name)
		}
	}

	g.Group.Name = in.Name
	g.Group.Stamp = b.stamp(ctx)

	return &empty{}, nil
}

// DeleteUserHierarchyGroup removes a leaf group no user belongs to.
func (b *Backend) DeleteUserHierarchyGroup(inst, id string) (*empty, error) {
	b.mu.Lock("DeleteUserHierarchyGroup")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	i := r.Instance.ID

	if slices.ContainsFunc(b.hierarchyGroups.in(i), func(g *hierarchyGroupRec) bool { return g.ParentID == id }) {
		return nil, inUseErr("HierarchyGroup %q has child groups", id)
	}

	if slices.ContainsFunc(b.users.in(i), func(u *userRec) bool { return u.User.HierarchyGroupID == id }) {
		return nil, inUseErr("HierarchyGroup %q has users", id)
	}

	return nil, b.hierarchyGroups.del(i, id)
}

func levelName(i int) string {
	names := levelNames()
	if i < 0 || i >= len(names) {
		return ""
	}

	return names[i]
}
