package connect

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"

	"github.com/google/uuid"
)

const (
	maxFlowContentBytes = 256000
	flowPublished       = "PUBLISHED"
	flowSaved           = "SAVED"
	flowActive          = "ACTIVE"
	flowArchived        = "ARCHIVED"
)

var (
	flowTypes = []string{ //nolint:gochecknoglobals // immutable lookup table
		"CONTACT_FLOW", "CUSTOMER_QUEUE", "CUSTOMER_HOLD", "CUSTOMER_WHISPER", "AGENT_HOLD", "AGENT_WHISPER",
		"OUTBOUND_WHISPER", "AGENT_TRANSFER", "QUEUE_TRANSFER", "CAMPAIGN",
	}
	actionTypePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*$`)
)

type flowDoc struct {
	StartAction string       `json:"StartAction"`
	Version     string       `json:"Version"`
	Actions     []flowAction `json:"Actions"`
}

type flowAction struct {
	Transitions *flowTransitions `json:"Transitions"`
	Identifier  string           `json:"Identifier"`
	Type        string           `json:"Type"`
}

type flowTransitions struct {
	NextAction *string    `json:"NextAction"`
	Errors     []flowNext `json:"Errors"`
	Conditions []flowNext `json:"Conditions"`
}

type flowNext struct {
	NextAction string `json:"NextAction"`
}

// validateFlowContent checks the structure of contact flow language: ids, types and transition targets.
func validateFlowContent(code, content string) error {
	if len(content) > maxFlowContentBytes {
		return invalidFlowErr(code, fmt.Sprintf("content exceeds %d bytes", maxFlowContentBytes))
	}

	var doc flowDoc
	if err := json.Unmarshal([]byte(content), &doc); err != nil {
		return invalidFlowErr(code, "content is not valid flow language JSON: "+err.Error())
	}

	var problems []string

	if doc.Version == "" {
		problems = append(problems, "Version is required")
	}

	ids := make(map[string]bool, len(doc.Actions))

	for _, a := range doc.Actions {
		switch {
		case a.Identifier == "":
			problems = append(problems, "every action needs an Identifier")
		case ids[a.Identifier]:
			problems = append(problems, "duplicate action Identifier "+a.Identifier)
		default:
			ids[a.Identifier] = true
		}

		if !actionTypePattern.MatchString(a.Type) {
			problems = append(problems, fmt.Sprintf("action %q has an invalid Type", a.Identifier))
		}
	}

	switch {
	case len(doc.Actions) == 0:
		problems = append(problems, "Actions must not be empty")
	case !ids[doc.StartAction]:
		problems = append(problems, fmt.Sprintf("StartAction %q does not reference an action", doc.StartAction))
	}

	problems = append(problems, checkTransitions(doc.Actions, ids)...)

	if len(problems) > 0 {
		return invalidFlowErr(code, problems...)
	}

	return nil
}

func checkTransitions(actions []flowAction, ids map[string]bool) []string {
	var problems []string

	for _, a := range actions {
		if a.Transitions == nil {
			continue
		}

		targets := []string{}
		if a.Transitions.NextAction != nil {
			targets = append(targets, *a.Transitions.NextAction)
		}

		for _, n := range slices.Concat(a.Transitions.Errors, a.Transitions.Conditions) {
			targets = append(targets, n.NextAction)
		}

		for _, t := range targets {
			if !ids[t] {
				problems = append(problems, fmt.Sprintf("action %q transitions to unknown action %q", a.Identifier, t))
			}
		}
	}

	return problems
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))

	return hex.EncodeToString(sum[:])
}

// CreateContactFlowInput is the CreateContactFlow request.
type CreateContactFlowInput struct {
	Tags        map[string]string
	Content     string
	Name        string
	Description string
	Type        string
	Status      string
	instPath
}

// CreateContactFlow creates a contact flow after validating its content.
func (b *Backend) CreateContactFlow(ctx context.Context, in *CreateContactFlowInput) (*contactFlowOut, error) {
	if err := checkName("Name", in.Name, maxNameLen); err != nil {
		return nil, err
	}

	if err := oneOf("Type", in.Type, flowTypes...); err != nil {
		return nil, err
	}

	if in.Status == "" {
		in.Status = flowPublished
	}

	if err := oneOf("Status", in.Status, flowPublished, flowSaved); err != nil {
		return nil, err
	}

	if err := firstError(
		checkDescription(in.Description),
		checkTags(in.Tags),
		validateFlowContent("InvalidContactFlowException", in.Content),
	); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateContactFlow")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID
	if nameExists(b.flows, inst, func(f *flowRec) string { return f.Flow.Name }, in.Name, "") {
		return nil, duplicateErr("ContactFlow", in.Name)
	}

	id := uuid.NewString()
	version := int64(1)
	rec := &flowRec{InstanceID: inst, Flow: ContactFlow{
		Stamp:       b.stamp(ctx),
		Version:     &version,
		Tags:        cloneTags(in.Tags),
		Arn:         b.buildARN(ctx, "instance/"+inst+"/contact-flow/"+id),
		Content:     in.Content,
		Description: in.Description,
		FlowContentSha256: sha256Hex(
			in.Content,
		),
		ID:     id,
		Name:   in.Name,
		State:  flowActive,
		Status: in.Status,
		Type:   in.Type,
	}}
	b.flows.put(rec)
	b.snapshotInitial(ctx, inst, kindFlow, id, in.Status)

	return &contactFlowOut{
		ContactFlowArn:    rec.Flow.Arn,
		ContactFlowID:     id,
		FlowContentSha256: rec.Flow.FlowContentSha256,
	}, nil
}

// DescribeContactFlow returns a contact flow.
func (b *Backend) DescribeContactFlow(inst, id string) (*ContactFlow, error) {
	b.mu.RLock("DescribeContactFlow")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	f, err := b.flows.get(r.Instance.ID, id)
	if err != nil {
		return nil, err
	}

	out := f.Flow
	out.Tags = cloneTags(out.Tags)

	return &out, nil
}

// ListContactFlows pages through contact flows, optionally restricted to the given types.
func (b *Backend) ListContactFlows(
	inst string,
	types []string,
	token string,
	maxResults int32,
) ([]ContactFlowSummary, string, error) {
	for _, t := range types {
		if err := oneOf("contactFlowTypes", t, flowTypes...); err != nil {
			return nil, "", err
		}
	}

	b.mu.RLock("ListContactFlows")

	r, err := b.instanceLocked(inst)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []ContactFlowSummary

	for _, f := range b.flows.in(r.Instance.ID) {
		if len(types) == 0 || slices.Contains(types, f.Flow.Type) {
			all = append(all, ContactFlowSummary{
				Arn: f.Flow.Arn, ContactFlowState: f.Flow.State, ContactFlowStatus: f.Flow.Status,
				ContactFlowType: f.Flow.Type, ID: f.Flow.ID, Name: f.Flow.Name,
			})
		}
	}

	b.mu.RUnlock()

	return paged(all, token, maxResults)
}

// UpdateContactFlowInput carries the contact flow update requests; each op reads only its own members.
type UpdateContactFlowInput struct {
	Content          *string
	Name             *string
	Description      *string
	ContactFlowState *string
	FlowID           string `json:"-" path:"ContactFlowId"`
	instPath
}

func (b *Backend) updateFlow(
	ctx context.Context,
	in *UpdateContactFlowInput,
	apply func(f *flowRec) error,
) (*empty, error) {
	b.mu.Lock("UpdateContactFlow")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	f, err := b.flows.get(r.Instance.ID, in.FlowID)
	if err != nil {
		return nil, err
	}

	if err = apply(f); err != nil {
		return nil, err
	}

	f.Flow.Stamp = b.stamp(ctx)

	return &empty{}, nil
}

// UpdateContactFlowContent replaces the content of a flow after validating it.
func (b *Backend) UpdateContactFlowContent(ctx context.Context, in *UpdateContactFlowInput) (*empty, error) {
	if in.Content == nil {
		return nil, invalidParamErr("Content is required")
	}

	if err := validateFlowContent("InvalidContactFlowException", *in.Content); err != nil {
		return nil, err
	}

	return b.updateFlow(ctx, in, func(f *flowRec) error {
		f.Flow.Content = *in.Content
		f.Flow.FlowContentSha256 = sha256Hex(*in.Content)

		return nil
	})
}

// UpdateContactFlowName renames a flow.
func (b *Backend) UpdateContactFlowName(ctx context.Context, in *UpdateContactFlowInput) (*empty, error) {
	if in.Name == nil && in.Description == nil {
		return nil, invalidParamErr("Name or Description is required")
	}

	return b.updateFlow(ctx, in, func(f *flowRec) error { return b.renameFlow(f, in) })
}

func (b *Backend) renameFlow(f *flowRec, in *UpdateContactFlowInput) error {
	return applyRename(b.flows, f, in.Name, in.Description,
		func(x *flowRec) *string { return &x.Flow.Name }, func(x *flowRec) *string { return &x.Flow.Description })
}

// UpdateContactFlowMetadata changes name, description and state of a flow.
func (b *Backend) UpdateContactFlowMetadata(ctx context.Context, in *UpdateContactFlowInput) (*empty, error) {
	if in.ContactFlowState != nil {
		if err := oneOf("ContactFlowState", *in.ContactFlowState, flowActive, flowArchived); err != nil {
			return nil, err
		}
	}

	return b.updateFlow(ctx, in, func(f *flowRec) error {
		if err := b.renameFlow(f, in); err != nil {
			return err
		}

		setIf(&f.Flow.State, in.ContactFlowState)

		return nil
	})
}

// DeleteContactFlow removes a flow no phone number, queue or quick connect uses.
func (b *Backend) DeleteContactFlow(inst, id string) (*empty, error) {
	b.mu.Lock("DeleteContactFlow")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	i := r.Instance.ID

	for _, p := range b.phoneNumbers.All() {
		if p.InstanceID == i && p.FlowID == id {
			return nil, inUseErr("ContactFlow %q is associated with phone number %s", id, p.Phone.PhoneNumber)
		}
	}

	for _, q := range b.queues.in(i) {
		if c := q.Queue.OutboundCallerConfig; c != nil && c.OutboundFlowID == id {
			return nil, inUseErr("ContactFlow %q is the outbound flow of queue %q", id, q.Queue.Name)
		}
	}

	for _, a := range b.flowAssocs.in(i) {
		if a.FlowID == id {
			return nil, inUseErr("ContactFlow %q is associated with %s %s", id, a.ResourceType, a.ResourceID)
		}
	}

	b.dropVersioned(i, kindFlow, id)

	return nil, b.flows.del(i, id)
}

// CreateContactFlowModuleInput is the CreateContactFlowModule request.
type CreateContactFlowModuleInput struct {
	Tags        map[string]string
	Content     string
	Name        string
	Description string
	Settings    string
	ClientToken string
	instPath
	ExternalInvocationConfiguration json.RawMessage
}

func validateModule(content, settings string) error {
	if err := validateFlowContent("InvalidContactFlowModuleException", content); err != nil {
		return err
	}

	if settings != "" && !json.Valid([]byte(settings)) {
		return invalidFlowErr("InvalidContactFlowModuleException", "Settings is not valid JSON")
	}

	return nil
}

// CreateContactFlowModule creates a flow module after validating its content.
func (b *Backend) CreateContactFlowModule(ctx context.Context, in *CreateContactFlowModuleInput) (*moduleOut, error) {
	if err := checkName("Name", in.Name, maxNameLen); err != nil {
		return nil, err
	}

	if err := firstError(
		checkDescription(in.Description),
		checkTags(in.Tags),
		validateModule(in.Content, in.Settings),
	); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateContactFlowModule")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID
	if nameExists(b.modules, inst, func(m *moduleRec) string { return m.Module.Name }, in.Name, "") {
		return nil, duplicateErr("ContactFlowModule", in.Name)
	}

	id := uuid.NewString()
	version := int64(1)
	rec := &moduleRec{InstanceID: inst, Module: ContactFlowModule{
		Version:                         &version,
		Tags:                            cloneTags(in.Tags),
		ExternalInvocationConfiguration: in.ExternalInvocationConfiguration,
		Arn:                             b.buildARN(ctx, "instance/"+inst+"/flow-module/"+id),
		Content:                         in.Content,
		Description:                     in.Description,
		FlowModuleContentSha256:         sha256Hex(in.Content),
		ID:                              id,
		Name:                            in.Name,
		Settings:                        in.Settings,
		State:                           flowActive,
		Status:                          flowPublished,
	}}
	b.modules.put(rec)
	b.snapshotInitial(ctx, inst, kindModule, id, flowPublished)

	return &moduleOut{Arn: rec.Module.Arn, ID: id}, nil
}

// DescribeContactFlowModule returns a flow module.
func (b *Backend) DescribeContactFlowModule(inst, id string) (*ContactFlowModule, error) {
	b.mu.RLock("DescribeContactFlowModule")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	m, err := b.modules.get(r.Instance.ID, id)
	if err != nil {
		return nil, err
	}

	out := m.Module
	out.Tags = cloneTags(out.Tags)

	return &out, nil
}

// ListContactFlowModules pages through flow modules, optionally restricted to one state.
func (b *Backend) ListContactFlowModules(
	inst, state, token string,
	maxResults int32,
) ([]ContactFlowModuleSummary, string, error) {
	if state != "" {
		if err := oneOf("state", state, flowActive, flowArchived); err != nil {
			return nil, "", err
		}
	}

	b.mu.RLock("ListContactFlowModules")

	r, err := b.instanceLocked(inst)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []ContactFlowModuleSummary

	for _, m := range b.modules.in(r.Instance.ID) {
		if state == "" || m.Module.State == state {
			all = append(
				all,
				ContactFlowModuleSummary{
					Arn:   m.Module.Arn,
					ID:    m.Module.ID,
					Name:  m.Module.Name,
					State: m.Module.State,
				},
			)
		}
	}

	b.mu.RUnlock()

	return paged(all, token, maxResults)
}

// UpdateContactFlowModuleInput carries the flow module update requests; each op reads only its own members.
type UpdateContactFlowModuleInput struct {
	Content     *string
	Settings    *string
	Name        *string
	Description *string
	State       *string
	ModuleID    string `json:"-" path:"ContactFlowModuleId"`
	instPath
}

func (b *Backend) updateModule(in *UpdateContactFlowModuleInput, apply func(m *moduleRec) error) (*empty, error) {
	b.mu.Lock("UpdateContactFlowModule")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	m, err := b.modules.get(r.Instance.ID, in.ModuleID)
	if err != nil {
		return nil, err
	}

	return nil, apply(m)
}

// UpdateContactFlowModuleContent replaces the content and settings of a module.
func (b *Backend) UpdateContactFlowModuleContent(_ context.Context, in *UpdateContactFlowModuleInput) (*empty, error) {
	if in.Content == nil && in.Settings == nil {
		return nil, invalidParamErr("Content or Settings is required")
	}

	return b.updateModule(in, func(m *moduleRec) error {
		content, settings := m.Module.Content, m.Module.Settings
		setIf(&content, in.Content)
		setIf(&settings, in.Settings)

		if err := validateModule(content, settings); err != nil {
			return err
		}

		m.Module.Content, m.Module.Settings = content, settings
		m.Module.FlowModuleContentSha256 = sha256Hex(content)

		return nil
	})
}

// UpdateContactFlowModuleMetadata changes name, description and state of a module.
func (b *Backend) UpdateContactFlowModuleMetadata(_ context.Context, in *UpdateContactFlowModuleInput) (*empty, error) {
	if in.State != nil {
		if err := oneOf("State", *in.State, flowActive, flowArchived); err != nil {
			return nil, err
		}
	}

	return b.updateModule(in, func(m *moduleRec) error {
		if in.Name != nil {
			if err := checkName("Name", *in.Name, maxNameLen); err != nil {
				return err
			}

			if nameExists(
				b.modules,
				m.InstanceID,
				func(x *moduleRec) string { return x.Module.Name },
				*in.Name,
				m.Module.ID,
			) {
				return duplicateErr("ContactFlowModule", *in.Name)
			}

			m.Module.Name = *in.Name
		}

		if in.Description != nil {
			if err := checkDescription(*in.Description); err != nil {
				return err
			}

			m.Module.Description = *in.Description
		}

		setIf(&m.Module.State, in.State)

		return nil
	})
}

// DeleteContactFlowModule removes a flow module.
func (b *Backend) DeleteContactFlowModule(inst, id string) (*empty, error) {
	b.mu.Lock("DeleteContactFlowModule")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	b.dropVersioned(r.Instance.ID, kindModule, id)

	return nil, b.modules.del(r.Instance.ID, id)
}
