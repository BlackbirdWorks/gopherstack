package connect

import (
	"context"
	"slices"

	"github.com/google/uuid"
)

const (
	queueEnabled  = "ENABLED"
	queueDisabled = "DISABLED"
	queueStandard = "STANDARD"
	queueAgent    = "AGENT"
	minMaxContact = 0
	maxQuickConns = 50
)

// CreateQueueInput is the CreateQueue request.
type CreateQueueInput struct {
	MaxContacts          *int32
	OutboundCallerConfig *OutboundCallerConfig
	OutboundEmailConfig  *OutboundEmailConfig
	Tags                 map[string]string
	Name                 string
	Description          string
	HoursOfOperationID   string `json:"HoursOfOperationId"`
	instPath
	QuickConnectIDs []string `json:"QuickConnectIds"`
}

// UpdateQueueInput carries the single-field queue update requests; each op sets only its own members.
type UpdateQueueInput struct {
	MaxContacts          *int32
	OutboundCallerConfig *OutboundCallerConfig
	OutboundEmailConfig  *OutboundEmailConfig
	Name                 *string
	Description          *string
	HoursOfOperationID   *string `json:"HoursOfOperationId"`
	Status               *string
	QueueID              string `json:"-"                  path:"QueueId"`
	instPath
}

// QuickConnectIDsInput is the (Dis)AssociateQueueQuickConnects request.
type QuickConnectIDsInput struct {
	QueueID string `json:"-"               path:"QueueId"`
	instPath
	QuickConnectIDs []string `json:"QuickConnectIds"`
}

func (b *Backend) checkQueueRefs(inst string, in *CreateQueueInput) error {
	if !b.hours.has(inst, in.HoursOfOperationID) {
		return notFoundErr("HoursOfOperation", in.HoursOfOperationID)
	}

	for _, qc := range in.QuickConnectIDs {
		if !b.quickConnects.has(inst, qc) {
			return notFoundErr("QuickConnect", qc)
		}
	}

	return b.checkCallerConfig(inst, in.OutboundCallerConfig)
}

func (b *Backend) checkCallerConfig(inst string, c *OutboundCallerConfig) error {
	if c == nil {
		return nil
	}

	if c.OutboundFlowID != "" && !b.flows.has(inst, c.OutboundFlowID) {
		return notFoundErr("ContactFlow", c.OutboundFlowID)
	}

	if c.OutboundCallerIDNumberID != "" {
		p, ok := b.phoneNumbers.Get(c.OutboundCallerIDNumberID)
		if !ok || p.InstanceID != inst {
			return notFoundErr("PhoneNumber", c.OutboundCallerIDNumberID)
		}
	}

	return nil
}

// CreateQueue creates a standard queue.
func (b *Backend) CreateQueue(ctx context.Context, in *CreateQueueInput) (*queueOut, error) {
	if err := checkName("Name", in.Name, maxNameLen); err != nil {
		return nil, err
	}

	if in.MaxContacts != nil && *in.MaxContacts < minMaxContact {
		return nil, invalidParamErr("MaxContacts must be at least %d", minMaxContact)
	}

	if len(in.QuickConnectIDs) > maxQuickConns {
		return nil, limitErr("a queue can have at most %d quick connects", maxQuickConns)
	}

	if err := firstError(
		checkDescription(in.Description),
		checkTags(in.Tags),
	); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateQueue")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID

	if err = b.checkQueueRefs(inst, in); err != nil {
		return nil, err
	}

	if nameExists(b.queues, inst, func(q *queueRec) string { return q.Queue.Name }, in.Name, "") {
		return nil, duplicateErr("Queue", in.Name)
	}

	id := uuid.NewString()
	rec := &queueRec{InstanceID: inst, QuickConnects: slices.Clone(in.QuickConnectIDs), Queue: Queue{
		Stamp:                b.stamp(ctx),
		OutboundCallerConfig: in.OutboundCallerConfig,
		OutboundEmailConfig:  in.OutboundEmailConfig,
		MaxContacts:          in.MaxContacts,
		Tags:                 cloneTags(in.Tags),
		Description:          in.Description,
		HoursOfOperationID:   in.HoursOfOperationID,
		Name:                 in.Name,
		QueueArn:             b.buildARN(ctx, "instance/"+inst+"/queue/"+id),
		QueueID:              id,
		Status:               queueEnabled,
	}}
	b.queues.put(rec)

	return &queueOut{QueueArn: rec.Queue.QueueArn, QueueID: id}, nil
}

// DescribeQueue returns a queue.
func (b *Backend) DescribeQueue(inst, id string) (*Queue, error) {
	b.mu.RLock("DescribeQueue")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	q, err := b.queues.get(r.Instance.ID, id)
	if err != nil {
		return nil, err
	}

	out := q.Queue
	out.Tags = cloneTags(out.Tags)

	return &out, nil
}

// ListQueues pages through queues, optionally restricted to the given types.
func (b *Backend) ListQueues(
	inst string,
	types []string,
	token string,
	maxResults int32,
) ([]QueueSummary, string, error) {
	for _, t := range types {
		if err := oneOf("queueTypes", t, queueStandard, queueAgent); err != nil {
			return nil, "", err
		}
	}

	b.mu.RLock("ListQueues")

	r, err := b.instanceLocked(inst)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []QueueSummary

	if len(types) == 0 || slices.Contains(types, queueStandard) {
		for _, q := range b.queues.in(r.Instance.ID) {
			all = append(
				all,
				QueueSummary{
					Stamp:     q.Queue.Stamp,
					Arn:       q.Queue.QueueArn,
					ID:        q.Queue.QueueID,
					Name:      q.Queue.Name,
					QueueType: queueStandard,
				},
			)
		}
	}

	b.mu.RUnlock()

	return paged(all, token, maxResults)
}

func (b *Backend) updateQueue(
	ctx context.Context,
	in *UpdateQueueInput,
	apply func(r *queueRec) error,
) (*empty, error) {
	b.mu.Lock("UpdateQueue")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	q, err := b.queues.get(r.Instance.ID, in.QueueID)
	if err != nil {
		return nil, err
	}

	if err = apply(q); err != nil {
		return nil, err
	}

	q.Queue.Stamp = b.stamp(ctx)

	return &empty{}, nil
}

// UpdateQueueName renames a queue.
func (b *Backend) UpdateQueueName(ctx context.Context, in *UpdateQueueInput) (*empty, error) {
	if in.Name == nil && in.Description == nil {
		return nil, invalidParamErr("Name or Description is required")
	}

	return b.updateQueue(ctx, in, func(q *queueRec) error {
		return applyRename(
			b.queues,
			q,
			in.Name,
			in.Description,
			func(x *queueRec) *string { return &x.Queue.Name },
			func(x *queueRec) *string { return &x.Queue.Description },
		)
	})
}

// UpdateQueueStatus enables or disables a queue.
func (b *Backend) UpdateQueueStatus(ctx context.Context, in *UpdateQueueInput) (*empty, error) {
	if in.Status == nil {
		return nil, invalidParamErr("Status is required")
	}

	if err := oneOf("Status", *in.Status, queueEnabled, queueDisabled); err != nil {
		return nil, err
	}

	return b.updateQueue(ctx, in, func(q *queueRec) error {
		q.Queue.Status = *in.Status

		return nil
	})
}

// UpdateQueueMaxContacts sets the queue capacity; nil removes the cap.
func (b *Backend) UpdateQueueMaxContacts(ctx context.Context, in *UpdateQueueInput) (*empty, error) {
	if in.MaxContacts != nil && *in.MaxContacts < minMaxContact {
		return nil, invalidParamErr("MaxContacts must be at least %d", minMaxContact)
	}

	return b.updateQueue(ctx, in, func(q *queueRec) error {
		q.Queue.MaxContacts = in.MaxContacts

		return nil
	})
}

// UpdateQueueHoursOfOperation points a queue at another schedule.
func (b *Backend) UpdateQueueHoursOfOperation(ctx context.Context, in *UpdateQueueInput) (*empty, error) {
	if in.HoursOfOperationID == nil {
		return nil, invalidParamErr("HoursOfOperationId is required")
	}

	return b.updateQueue(ctx, in, func(q *queueRec) error {
		if !b.hours.has(q.InstanceID, *in.HoursOfOperationID) {
			return notFoundErr("HoursOfOperation", *in.HoursOfOperationID)
		}

		q.Queue.HoursOfOperationID = *in.HoursOfOperationID

		return nil
	})
}

// UpdateQueueOutboundCallerConfig sets the caller ID of outbound calls from a queue.
func (b *Backend) UpdateQueueOutboundCallerConfig(ctx context.Context, in *UpdateQueueInput) (*empty, error) {
	if in.OutboundCallerConfig == nil {
		return nil, invalidParamErr("OutboundCallerConfig is required")
	}

	return b.updateQueue(ctx, in, func(q *queueRec) error {
		if err := b.checkCallerConfig(q.InstanceID, in.OutboundCallerConfig); err != nil {
			return err
		}

		q.Queue.OutboundCallerConfig = in.OutboundCallerConfig

		return nil
	})
}

// UpdateQueueOutboundEmailConfig sets the from address of outbound email from a queue.
func (b *Backend) UpdateQueueOutboundEmailConfig(ctx context.Context, in *UpdateQueueInput) (*empty, error) {
	if in.OutboundEmailConfig == nil {
		return nil, invalidParamErr("OutboundEmailConfig is required")
	}

	return b.updateQueue(ctx, in, func(q *queueRec) error {
		q.Queue.OutboundEmailConfig = in.OutboundEmailConfig

		return nil
	})
}

// DeleteQueue removes a queue that no routing profile or quick connect references.
func (b *Backend) DeleteQueue(inst, id string) (*empty, error) {
	b.mu.Lock("DeleteQueue")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	i := r.Instance.ID

	if _, err = b.queues.get(i, id); err != nil {
		return nil, err
	}

	for _, rp := range b.routingProfiles.in(i) {
		if rp.Profile.DefaultOutboundQueueID == id ||
			slices.ContainsFunc(
				rp.Queues,
				func(c RoutingProfileQueueConfig) bool { return c.QueueReference.QueueID == id },
			) {
			return nil, inUseErr("Queue %q is used by routing profile %q", id, rp.Profile.Name)
		}
	}

	for _, qc := range b.quickConnects.in(i) {
		if c := qc.Connect.QuickConnectConfig.QueueConfig; c != nil && c.QueueID == id {
			return nil, inUseErr("Queue %q is used by quick connect %q", id, qc.Connect.Name)
		}
	}

	return nil, b.queues.del(i, id)
}

func (b *Backend) queueRefs(inst, queueID string, ids []string, add bool) error {
	r, err := b.instanceLocked(inst)
	if err != nil {
		return err
	}

	q, err := b.queues.get(r.Instance.ID, queueID)
	if err != nil {
		return err
	}

	for _, id := range ids {
		if add && !b.quickConnects.has(r.Instance.ID, id) {
			return notFoundErr("QuickConnect", id)
		}

		if !add && !slices.Contains(q.QuickConnects, id) {
			return notFoundErr("QuickConnect association", id)
		}
	}

	if add {
		for _, id := range ids {
			if !slices.Contains(q.QuickConnects, id) {
				q.QuickConnects = append(q.QuickConnects, id)
			}
		}

		if len(q.QuickConnects) > maxQuickConns {
			return limitErr("a queue can have at most %d quick connects", maxQuickConns)
		}
	} else {
		q.QuickConnects = slices.DeleteFunc(q.QuickConnects, func(id string) bool { return slices.Contains(ids, id) })
	}

	return nil
}

// AssociateQueueQuickConnects adds quick connects to a queue.
func (b *Backend) AssociateQueueQuickConnects(in *QuickConnectIDsInput) (*empty, error) {
	b.mu.Lock("AssociateQueueQuickConnects")
	defer b.mu.Unlock()

	return nil, b.queueRefs(in.InstanceID, in.QueueID, in.QuickConnectIDs, true)
}

// DisassociateQueueQuickConnects removes quick connects from a queue.
func (b *Backend) DisassociateQueueQuickConnects(in *QuickConnectIDsInput) (*empty, error) {
	b.mu.Lock("DisassociateQueueQuickConnects")
	defer b.mu.Unlock()

	return nil, b.queueRefs(in.InstanceID, in.QueueID, in.QuickConnectIDs, false)
}

// ListQueueQuickConnects pages through the quick connects of a queue.
func (b *Backend) ListQueueQuickConnects(
	inst, queueID, token string,
	maxResults int32,
) ([]QuickConnectSummary, string, error) {
	b.mu.RLock("ListQueueQuickConnects")

	r, err := b.instanceLocked(inst)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	q, err := b.queues.get(r.Instance.ID, queueID)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []QuickConnectSummary

	for _, id := range q.QuickConnects {
		if qc, ok := b.quickConnects.table.Get(r.Instance.ID + "/" + id); ok {
			all = append(all, quickSummary(qc))
		}
	}

	b.mu.RUnlock()

	return paged(all, token, maxResults)
}
