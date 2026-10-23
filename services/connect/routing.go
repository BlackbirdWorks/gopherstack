package connect

import (
	"context"
	"slices"

	"github.com/google/uuid"
)

func channels() []string {
	return []string{channelVoice, channelChat, channelTask, emailValue}
}

const maxQueueConfigs = 100

// CreateRoutingProfileInput is the CreateRoutingProfile request.
type CreateRoutingProfileInput struct {
	Tags                   map[string]string
	Name                   string
	Description            string
	DefaultOutboundQueueID string `json:"DefaultOutboundQueueId"`
	AgentAvailabilityTimer string
	instPath
	MediaConcurrencies     []MediaConcurrency
	QueueConfigs           []RoutingProfileQueueConfig
	ManualAssignmentQueues []RoutingProfileQueueConfig `json:"ManualAssignmentQueueConfigs"`
}

// UpdateRoutingProfileInput carries the routing profile update requests; each op reads only its own members.
type UpdateRoutingProfileInput struct {
	Name                   *string
	Description            *string
	DefaultOutboundQueueID *string `json:"DefaultOutboundQueueId"`
	AgentAvailabilityTimer *string
	ProfileID              string `json:"-"                               path:"RoutingProfileId"`
	instPath
	MediaConcurrencies     []MediaConcurrency
	QueueConfigs           []RoutingProfileQueueConfig
	ManualAssignmentQueues []RoutingProfileQueueConfig `json:"ManualAssignmentQueueConfigs"`
	QueueReferences        []QueueReference
	ManualReferences       []QueueReference `json:"ManualAssignmentQueueReferences"`
}

func checkConcurrencies(m []MediaConcurrency) error {
	for _, c := range m {
		if err := oneOf("MediaConcurrencies.Channel", c.Channel, channels()...); err != nil {
			return err
		}

		if c.Concurrency < 0 || c.Concurrency > maxQueueConfigs {
			return invalidParamErr("MediaConcurrencies.Concurrency must be between 0 and %d", maxQueueConfigs)
		}
	}

	return nil
}

func (b *Backend) checkQueueConfigs(inst string, cfgs []RoutingProfileQueueConfig) error {
	for _, c := range cfgs {
		if err := oneOf("QueueReference.Channel", c.QueueReference.Channel, channels()...); err != nil {
			return err
		}

		if !b.queues.has(inst, c.QueueReference.QueueID) {
			return notFoundErr("Queue", c.QueueReference.QueueID)
		}

		if c.Priority < 1 || c.Priority > maxQueueConfigs || c.Delay < 0 {
			return invalidParamErr("QueueConfigs need Priority 1-%d and a non-negative Delay", maxQueueConfigs)
		}
	}

	return nil
}

func mergeQueueConfigs(existing, add []RoutingProfileQueueConfig) []RoutingProfileQueueConfig {
	out := slices.Clone(existing)

	for _, a := range add {
		i := slices.IndexFunc(
			out,
			func(c RoutingProfileQueueConfig) bool { return c.QueueReference == a.QueueReference },
		)
		if i >= 0 {
			out[i] = a
		} else {
			out = append(out, a)
		}
	}

	return out
}

// CreateRoutingProfile creates a routing profile.
func (b *Backend) CreateRoutingProfile(ctx context.Context, in *CreateRoutingProfileInput) (*routingProfileOut, error) {
	if err := checkName("Name", in.Name, maxNameLen); err != nil {
		return nil, err
	}

	if in.Description == "" {
		return nil, invalidParamErr("Description is required")
	}

	if len(in.MediaConcurrencies) == 0 {
		return nil, invalidParamErr("MediaConcurrencies is required")
	}

	if in.AgentAvailabilityTimer != "" {
		if err := oneOf(
			"AgentAvailabilityTimer",
			in.AgentAvailabilityTimer,
			"TIME_SINCE_LAST_ACTIVITY",
			"TIME_SINCE_LAST_INBOUND",
		); err != nil {
			return nil, err
		}
	}

	if err := firstError(
		checkDescription(in.Description),
		checkTags(in.Tags),
		checkConcurrencies(in.MediaConcurrencies),
	); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateRoutingProfile")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID

	if !b.queues.has(inst, in.DefaultOutboundQueueID) {
		return nil, notFoundErr("Queue", in.DefaultOutboundQueueID)
	}

	if err = b.checkQueueConfigs(inst, in.QueueConfigs); err != nil {
		return nil, err
	}

	if err = b.checkQueueConfigs(inst, in.ManualAssignmentQueues); err != nil {
		return nil, err
	}

	if nameExists(b.routingProfiles, inst, func(p *routingProfileRec) string { return p.Profile.Name }, in.Name, "") {
		return nil, duplicateErr("RoutingProfile", in.Name)
	}

	id := uuid.NewString()
	rec := &routingProfileRec{
		InstanceID: inst,
		Queues:     in.QueueConfigs,
		Manual:     in.ManualAssignmentQueues,
		Profile: RoutingProfile{
			Stamp:                  b.stamp(ctx),
			Tags:                   cloneTags(in.Tags),
			AgentAvailabilityTimer: in.AgentAvailabilityTimer,
			DefaultOutboundQueueID: in.DefaultOutboundQueueID,
			Description:            in.Description,
			InstanceID:             inst,
			Name:                   in.Name,
			RoutingProfileArn:      b.buildARN(ctx, "instance/"+inst+"/routing-profile/"+id),
			RoutingProfileID:       id,
			MediaConcurrencies:     in.MediaConcurrencies,
		},
	}
	b.routingProfiles.put(rec)

	return &routingProfileOut{RoutingProfileArn: rec.Profile.RoutingProfileArn, RoutingProfileID: id}, nil
}

func queueIDsOf(cfgs []RoutingProfileQueueConfig) []string {
	var ids []string

	for _, c := range cfgs {
		if !slices.Contains(ids, c.QueueReference.QueueID) {
			ids = append(ids, c.QueueReference.QueueID)
		}
	}

	return ids
}

// DescribeRoutingProfile returns a routing profile with its derived counts.
func (b *Backend) DescribeRoutingProfile(inst, id string) (*RoutingProfile, error) {
	b.mu.RLock("DescribeRoutingProfile")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	p, err := b.routingProfiles.get(r.Instance.ID, id)
	if err != nil {
		return nil, err
	}

	out := p.Profile
	out.Tags = cloneTags(out.Tags)
	out.AssociatedQueueIDs = queueIDsOf(p.Queues)
	out.AssociatedManualAssignmentQueueIDs = queueIDsOf(p.Manual)
	out.NumberOfAssociatedQueues = int64(len(out.AssociatedQueueIDs))
	out.NumberOfAssociatedManualAssignmentQueues = int64(len(out.AssociatedManualAssignmentQueueIDs))

	for _, u := range b.users.in(r.Instance.ID) {
		if u.User.RoutingProfileID == id {
			out.NumberOfAssociatedUsers++
		}
	}

	return &out, nil
}

// ListRoutingProfiles pages through routing profiles.
func (b *Backend) ListRoutingProfiles(inst, token string, maxResults int32) ([]Summary, string, error) {
	b.mu.RLock("ListRoutingProfiles")

	r, err := b.instanceLocked(inst)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []Summary
	for _, p := range b.routingProfiles.in(r.Instance.ID) {
		all = append(
			all,
			Summary{
				Stamp: p.Profile.Stamp,
				Arn:   p.Profile.RoutingProfileArn,
				ID:    p.Profile.RoutingProfileID,
				Name:  p.Profile.Name,
			},
		)
	}

	b.mu.RUnlock()

	return paged(all, token, maxResults)
}

func (b *Backend) updateProfile(
	ctx context.Context,
	in *UpdateRoutingProfileInput,
	apply func(p *routingProfileRec) error,
) (*empty, error) {
	b.mu.Lock("UpdateRoutingProfile")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	p, err := b.routingProfiles.get(r.Instance.ID, in.ProfileID)
	if err != nil {
		return nil, err
	}

	if err = apply(p); err != nil {
		return nil, err
	}

	p.Profile.Stamp = b.stamp(ctx)

	return &empty{}, nil
}

// UpdateRoutingProfileName renames a routing profile.
func (b *Backend) UpdateRoutingProfileName(ctx context.Context, in *UpdateRoutingProfileInput) (*empty, error) {
	if in.Name == nil && in.Description == nil {
		return nil, invalidParamErr("Name or Description is required")
	}

	return b.updateProfile(ctx, in, func(p *routingProfileRec) error {
		return applyRename(b.routingProfiles, p, in.Name, in.Description,
			func(x *routingProfileRec) *string { return &x.Profile.Name },
			func(x *routingProfileRec) *string { return &x.Profile.Description })
	})
}

// UpdateRoutingProfileConcurrency replaces the per-channel concurrency.
func (b *Backend) UpdateRoutingProfileConcurrency(ctx context.Context, in *UpdateRoutingProfileInput) (*empty, error) {
	if len(in.MediaConcurrencies) == 0 {
		return nil, invalidParamErr("MediaConcurrencies is required")
	}

	if err := checkConcurrencies(in.MediaConcurrencies); err != nil {
		return nil, err
	}

	return b.updateProfile(ctx, in, func(p *routingProfileRec) error {
		p.Profile.MediaConcurrencies = in.MediaConcurrencies

		return nil
	})
}

// UpdateRoutingProfileDefaultOutboundQueue changes the default outbound queue.
func (b *Backend) UpdateRoutingProfileDefaultOutboundQueue(
	ctx context.Context,
	in *UpdateRoutingProfileInput,
) (*empty, error) {
	if in.DefaultOutboundQueueID == nil {
		return nil, invalidParamErr("DefaultOutboundQueueId is required")
	}

	return b.updateProfile(ctx, in, func(p *routingProfileRec) error {
		if !b.queues.has(p.InstanceID, *in.DefaultOutboundQueueID) {
			return notFoundErr("Queue", *in.DefaultOutboundQueueID)
		}

		p.Profile.DefaultOutboundQueueID = *in.DefaultOutboundQueueID

		return nil
	})
}

// UpdateRoutingProfileAgentAvailabilityTimer sets how agent availability is measured.
func (b *Backend) UpdateRoutingProfileAgentAvailabilityTimer(
	ctx context.Context,
	in *UpdateRoutingProfileInput,
) (*empty, error) {
	if in.AgentAvailabilityTimer == nil {
		return nil, invalidParamErr("AgentAvailabilityTimer is required")
	}

	if err := oneOf(
		"AgentAvailabilityTimer",
		*in.AgentAvailabilityTimer,
		"TIME_SINCE_LAST_ACTIVITY",
		"TIME_SINCE_LAST_INBOUND",
	); err != nil {
		return nil, err
	}

	return b.updateProfile(ctx, in, func(p *routingProfileRec) error {
		p.Profile.AgentAvailabilityTimer = *in.AgentAvailabilityTimer

		return nil
	})
}

// AssociateRoutingProfileQueues adds queue configs.
func (b *Backend) AssociateRoutingProfileQueues(ctx context.Context, in *UpdateRoutingProfileInput) (*empty, error) {
	return b.updateProfile(ctx, in, func(p *routingProfileRec) error {
		for _, cfgs := range [][]RoutingProfileQueueConfig{in.QueueConfigs, in.ManualAssignmentQueues} {
			if err := b.checkQueueConfigs(p.InstanceID, cfgs); err != nil {
				return err
			}
		}

		p.Queues = mergeQueueConfigs(p.Queues, in.QueueConfigs)
		p.Manual = mergeQueueConfigs(p.Manual, in.ManualAssignmentQueues)

		return nil
	})
}

// UpdateRoutingProfileQueues replaces the priority and delay of existing queue configs.
func (b *Backend) UpdateRoutingProfileQueues(ctx context.Context, in *UpdateRoutingProfileInput) (*empty, error) {
	return b.updateProfile(ctx, in, func(p *routingProfileRec) error {
		if err := b.checkQueueConfigs(p.InstanceID, in.QueueConfigs); err != nil {
			return err
		}

		for _, c := range in.QueueConfigs {
			i := slices.IndexFunc(
				p.Queues,
				func(e RoutingProfileQueueConfig) bool { return e.QueueReference == c.QueueReference },
			)
			if i < 0 {
				return notFoundErr("Queue association", c.QueueReference.QueueID)
			}

			p.Queues[i] = c
		}

		return nil
	})
}

// DisassociateRoutingProfileQueues removes queue configs.
func (b *Backend) DisassociateRoutingProfileQueues(ctx context.Context, in *UpdateRoutingProfileInput) (*empty, error) {
	return b.updateProfile(ctx, in, func(p *routingProfileRec) error {
		for _, ref := range in.QueueReferences {
			if p.Profile.DefaultOutboundQueueID == ref.QueueID {
				return invalidParamErr("the default outbound queue cannot be disassociated")
			}
		}

		drop := func(cfgs []RoutingProfileQueueConfig, refs []QueueReference) ([]RoutingProfileQueueConfig, error) {
			for _, ref := range refs {
				if !slices.ContainsFunc(
					cfgs,
					func(c RoutingProfileQueueConfig) bool { return c.QueueReference == ref },
				) {
					return nil, notFoundErr("Queue association", ref.QueueID)
				}
			}

			return slices.DeleteFunc(
				slices.Clone(cfgs),
				func(c RoutingProfileQueueConfig) bool { return slices.Contains(refs, c.QueueReference) },
			), nil
		}

		queues, err := drop(p.Queues, in.QueueReferences)
		if err != nil {
			return err
		}

		manual, err := drop(p.Manual, in.ManualReferences)
		if err != nil {
			return err
		}

		p.Queues, p.Manual = queues, manual

		return nil
	})
}

// ListRoutingProfileQueues pages through the queue configs of a profile.
func (b *Backend) ListRoutingProfileQueues(
	inst, id, token string,
	maxResults int32,
) ([]RoutingProfileQueueConfigSummary, string, error) {
	b.mu.RLock("ListRoutingProfileQueues")

	r, err := b.instanceLocked(inst)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	p, err := b.routingProfiles.get(r.Instance.ID, id)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []RoutingProfileQueueConfigSummary

	for _, c := range p.Queues {
		if q, ok := b.queues.table.Get(r.Instance.ID + "/" + c.QueueReference.QueueID); ok {
			all = append(all, RoutingProfileQueueConfigSummary{
				QueueArn: q.Queue.QueueArn, QueueID: q.Queue.QueueID, QueueName: q.Queue.Name,
				Channel: c.QueueReference.Channel, Delay: c.Delay, Priority: c.Priority,
			})
		}
	}

	b.mu.RUnlock()

	return paged(all, token, maxResults)
}

// DeleteRoutingProfile removes a routing profile that has no users.
func (b *Backend) DeleteRoutingProfile(inst, id string) (*empty, error) {
	b.mu.Lock("DeleteRoutingProfile")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	p, err := b.routingProfiles.get(r.Instance.ID, id)
	if err != nil {
		return nil, err
	}

	if p.Profile.IsDefault {
		return nil, invalidRequestErr("the default routing profile cannot be deleted")
	}

	if slices.ContainsFunc(b.users.in(r.Instance.ID), func(u *userRec) bool { return u.User.RoutingProfileID == id }) {
		return nil, inUseErr("RoutingProfile %q still has users", id)
	}

	return nil, b.routingProfiles.del(r.Instance.ID, id)
}
