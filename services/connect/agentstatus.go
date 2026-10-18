package connect

import (
	"context"
	"slices"

	"github.com/google/uuid"
)

const maxDisplayOrder = 50

// CreateAgentStatusInput is the CreateAgentStatus request.
type CreateAgentStatusInput struct {
	DisplayOrder *int32
	Tags         map[string]string
	Name         string
	Description  string
	State        string
	instPath
}

// UpdateAgentStatusInput is the UpdateAgentStatus request.
type UpdateAgentStatusInput struct {
	DisplayOrder  *int32
	Name          *string
	Description   *string
	State         *string
	AgentStatusID string `json:"-" path:"AgentStatusId"`
	instPath
	ResetOrderNumber bool
}

func (b *Backend) newAgentStatus(
	ctx context.Context,
	inst, name, desc, state, typ string,
	order int32,
	tags map[string]string,
) *agentStatusRec {
	id := uuid.NewString()

	return &agentStatusRec{InstanceID: inst, Status: AgentStatus{
		Stamp: b.stamp(ctx), DisplayOrder: &order, Tags: cloneTags(tags),
		AgentStatusARN: b.buildARN(ctx, "instance/"+inst+"/agent-state/"+id), AgentStatusID: id,
		Description: desc, Name: name, State: state, Type: typ,
	}}
}

// CreateAgentStatus creates a custom agent status.
func (b *Backend) CreateAgentStatus(ctx context.Context, in *CreateAgentStatusInput) (*agentStatusOut, error) {
	if err := checkName("Name", in.Name, maxNameLen); err != nil {
		return nil, err
	}

	if err := oneOf("State", in.State, "ENABLED", "DISABLED"); err != nil {
		return nil, err
	}

	if in.DisplayOrder != nil && (*in.DisplayOrder < 1 || *in.DisplayOrder > maxDisplayOrder) {
		return nil, invalidParamErr("DisplayOrder must be between 1 and %d", maxDisplayOrder)
	}

	if err := firstError(
		checkDescription(in.Description),
		checkTags(in.Tags),
	); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateAgentStatus")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID
	if nameExists(b.agentStatuses, inst, func(s *agentStatusRec) string { return s.Status.Name }, in.Name, "") {
		return nil, duplicateErr("AgentStatus", in.Name)
	}

	order := int32(1)

	for _, s := range b.agentStatuses.in(inst) {
		if s.Status.DisplayOrder != nil {
			order = max(order, *s.Status.DisplayOrder+1)
		}
	}

	if in.DisplayOrder != nil {
		order = *in.DisplayOrder
	}

	rec := b.newAgentStatus(ctx, inst, in.Name, in.Description, in.State, "CUSTOM", order, in.Tags)
	b.agentStatuses.put(rec)

	return &agentStatusOut{AgentStatusARN: rec.Status.AgentStatusARN, AgentStatusID: rec.Status.AgentStatusID}, nil
}

// DescribeAgentStatus returns an agent status.
func (b *Backend) DescribeAgentStatus(inst, id string) (*AgentStatus, error) {
	b.mu.RLock("DescribeAgentStatus")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	s, err := b.agentStatuses.get(r.Instance.ID, id)
	if err != nil {
		return nil, err
	}

	out := s.Status
	out.Tags = cloneTags(out.Tags)

	return &out, nil
}

// ListAgentStatuses pages through agent statuses, optionally restricted to the given types.
func (b *Backend) ListAgentStatuses(
	inst string,
	types []string,
	token string,
	maxResults int32,
) ([]AgentStatusSummary, string, error) {
	for _, t := range types {
		if err := oneOf("AgentStatusTypes", t, "ROUTABLE", "CUSTOM", "OFFLINE"); err != nil {
			return nil, "", err
		}
	}

	b.mu.RLock("ListAgentStatuses")

	r, err := b.instanceLocked(inst)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []AgentStatusSummary

	for _, s := range b.agentStatuses.in(r.Instance.ID) {
		if len(types) == 0 || slices.Contains(types, s.Status.Type) {
			all = append(
				all,
				AgentStatusSummary{
					Stamp: s.Status.Stamp,
					Arn:   s.Status.AgentStatusARN,
					ID:    s.Status.AgentStatusID,
					Name:  s.Status.Name,
					Type:  s.Status.Type,
				},
			)
		}
	}

	b.mu.RUnlock()

	return paged(all, token, maxResults)
}

// UpdateAgentStatus changes the supplied settings of an agent status.
func (b *Backend) UpdateAgentStatus(ctx context.Context, in *UpdateAgentStatusInput) (*empty, error) {
	if in.State != nil {
		if err := oneOf("State", *in.State, "ENABLED", "DISABLED"); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("UpdateAgentStatus")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	s, err := b.agentStatuses.get(r.Instance.ID, in.AgentStatusID)
	if err != nil {
		return nil, err
	}

	if in.Name != nil {
		if err = checkName("Name", *in.Name, maxNameLen); err != nil {
			return nil, err
		}

		if s.Status.Type != "CUSTOM" {
			return nil, invalidRequestErr("the name of a default agent status cannot be changed")
		}

		if nameExists(
			b.agentStatuses,
			s.InstanceID,
			func(x *agentStatusRec) string { return x.Status.Name },
			*in.Name,
			in.AgentStatusID,
		) {
			return nil, duplicateErr("AgentStatus", *in.Name)
		}

		s.Status.Name = *in.Name
	}

	if in.Description != nil {
		if err = checkDescription(*in.Description); err != nil {
			return nil, err
		}

		s.Status.Description = *in.Description
	}

	setIf(&s.Status.State, in.State)

	switch {
	case in.ResetOrderNumber:
		s.Status.DisplayOrder = nil
	case in.DisplayOrder != nil:
		if *in.DisplayOrder < 1 || *in.DisplayOrder > maxDisplayOrder {
			return nil, invalidParamErr("DisplayOrder must be between 1 and %d", maxDisplayOrder)
		}

		s.Status.DisplayOrder = in.DisplayOrder
	}

	s.Status.Stamp = b.stamp(ctx)

	return &empty{}, nil
}
