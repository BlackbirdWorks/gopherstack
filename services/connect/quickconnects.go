package connect

import (
	"context"
	"regexp"
	"slices"

	"github.com/google/uuid"
)

var e164Pattern = regexp.MustCompile(`^\+[1-9]\d{1,14}$`)

// CreateQuickConnectInput is the CreateQuickConnect request.
type CreateQuickConnectInput struct {
	Tags        map[string]string
	Name        string
	Description string
	instPath
	QuickConnectConfig QuickConnectConfig
}

// UpdateQuickConnectInput is the UpdateQuickConnectName and UpdateQuickConnectConfig request.
type UpdateQuickConnectInput struct {
	Name               *string
	Description        *string
	QuickConnectConfig *QuickConnectConfig
	QuickConnectID     string `json:"-" path:"QuickConnectId"`
	instPath
}

func (b *Backend) checkQuickConnectConfig(inst string, c *QuickConnectConfig) error {
	switch c.QuickConnectType {
	case "USER":
		if c.UserConfig == nil || !b.users.has(inst, c.UserConfig.UserID) ||
			!b.flows.has(inst, c.UserConfig.ContactFlowID) {
			return invalidParamErr("UserConfig must reference an existing user and contact flow")
		}
	case "QUEUE":
		if c.QueueConfig == nil || !b.queues.has(inst, c.QueueConfig.QueueID) ||
			!b.flows.has(inst, c.QueueConfig.ContactFlowID) {
			return invalidParamErr("QueueConfig must reference an existing queue and contact flow")
		}
	case "PHONE_NUMBER":
		if c.PhoneConfig == nil || !e164Pattern.MatchString(c.PhoneConfig.PhoneNumber) {
			return invalidParamErr("PhoneConfig.PhoneNumber must be an E.164 number")
		}
	case "FLOW":
		if len(c.FlowConfig) == 0 {
			return invalidParamErr("FlowConfig is required")
		}
	default:
		return invalidParamErr("QuickConnectType must be one of USER, QUEUE, PHONE_NUMBER, FLOW")
	}

	return nil
}

// CreateQuickConnect creates a quick connect.
func (b *Backend) CreateQuickConnect(ctx context.Context, in *CreateQuickConnectInput) (*quickConnectOut, error) {
	if err := checkName("Name", in.Name, maxNameLen); err != nil {
		return nil, err
	}

	if err := firstError(
		checkDescription(in.Description),
		checkTags(in.Tags),
	); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateQuickConnect")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID

	if err = b.checkQuickConnectConfig(inst, &in.QuickConnectConfig); err != nil {
		return nil, err
	}

	if nameExists(b.quickConnects, inst, func(q *quickConnectRec) string { return q.Connect.Name }, in.Name, "") {
		return nil, duplicateErr("QuickConnect", in.Name)
	}

	id := uuid.NewString()
	rec := &quickConnectRec{InstanceID: inst, Connect: QuickConnect{
		Stamp: b.stamp(ctx), Tags: cloneTags(in.Tags), QuickConnectConfig: in.QuickConnectConfig,
		Description: in.Description, Name: in.Name,
		QuickConnectARN: b.buildARN(ctx, "instance/"+inst+"/transfer-destination/"+id), QuickConnectID: id,
	}}
	b.quickConnects.put(rec)

	return &quickConnectOut{QuickConnectARN: rec.Connect.QuickConnectARN, QuickConnectID: id}, nil
}

// DescribeQuickConnect returns a quick connect.
func (b *Backend) DescribeQuickConnect(inst, id string) (*QuickConnect, error) {
	b.mu.RLock("DescribeQuickConnect")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	q, err := b.quickConnects.get(r.Instance.ID, id)
	if err != nil {
		return nil, err
	}

	out := q.Connect
	out.Tags = cloneTags(out.Tags)

	return &out, nil
}

func quickSummary(q *quickConnectRec) QuickConnectSummary {
	return QuickConnectSummary{
		Stamp: q.Connect.Stamp, Arn: q.Connect.QuickConnectARN, ID: q.Connect.QuickConnectID, Name: q.Connect.Name,
		QuickConnectType: q.Connect.QuickConnectConfig.QuickConnectType,
	}
}

// ListQuickConnects pages through quick connects, optionally restricted to the given types.
func (b *Backend) ListQuickConnects(
	inst string,
	types []string,
	token string,
	maxResults int32,
) ([]QuickConnectSummary, string, error) {
	for _, t := range types {
		if err := oneOf("QuickConnectTypes", t, "USER", "QUEUE", "PHONE_NUMBER", "FLOW"); err != nil {
			return nil, "", err
		}
	}

	b.mu.RLock("ListQuickConnects")

	r, err := b.instanceLocked(inst)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []QuickConnectSummary

	for _, q := range b.quickConnects.in(r.Instance.ID) {
		if len(types) == 0 || containsStr(types, q.Connect.QuickConnectConfig.QuickConnectType) {
			all = append(all, quickSummary(q))
		}
	}

	b.mu.RUnlock()

	return paged(all, token, maxResults)
}

func containsStr(s []string, v string) bool {
	return slices.Contains(s, v)
}

func (b *Backend) updateQuickConnect(
	ctx context.Context,
	in *UpdateQuickConnectInput,
	apply func(q *quickConnectRec) error,
) (*empty, error) {
	b.mu.Lock("UpdateQuickConnect")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	q, err := b.quickConnects.get(r.Instance.ID, in.QuickConnectID)
	if err != nil {
		return nil, err
	}

	if err = apply(q); err != nil {
		return nil, err
	}

	q.Connect.Stamp = b.stamp(ctx)

	return &empty{}, nil
}

// UpdateQuickConnectName renames a quick connect.
func (b *Backend) UpdateQuickConnectName(ctx context.Context, in *UpdateQuickConnectInput) (*empty, error) {
	if in.Name == nil && in.Description == nil {
		return nil, invalidParamErr("Name or Description is required")
	}

	return b.updateQuickConnect(ctx, in, func(q *quickConnectRec) error {
		return applyRename(b.quickConnects, q, in.Name, in.Description,
			func(x *quickConnectRec) *string { return &x.Connect.Name },
			func(x *quickConnectRec) *string { return &x.Connect.Description })
	})
}

// UpdateQuickConnectConfig replaces the destination of a quick connect.
func (b *Backend) UpdateQuickConnectConfig(ctx context.Context, in *UpdateQuickConnectInput) (*empty, error) {
	if in.QuickConnectConfig == nil {
		return nil, invalidParamErr("QuickConnectConfig is required")
	}

	return b.updateQuickConnect(ctx, in, func(q *quickConnectRec) error {
		if err := b.checkQuickConnectConfig(q.InstanceID, in.QuickConnectConfig); err != nil {
			return err
		}

		q.Connect.QuickConnectConfig = *in.QuickConnectConfig

		return nil
	})
}

// DeleteQuickConnect removes a quick connect that no queue uses.
func (b *Backend) DeleteQuickConnect(inst, id string) (*empty, error) {
	b.mu.Lock("DeleteQuickConnect")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	for _, q := range b.queues.in(r.Instance.ID) {
		if containsStr(q.QuickConnects, id) {
			return nil, inUseErr("QuickConnect %q is used by queue %q", id, q.Queue.Name)
		}
	}

	return nil, b.quickConnects.del(r.Instance.ID, id)
}
