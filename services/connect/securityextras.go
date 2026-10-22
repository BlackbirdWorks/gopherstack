package connect

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
)

const maxSecurityExtras = 100

// securityExtras are the Applications, AllowedFlowModules and AllowedAIAgents of a security profile.
type securityExtras struct {
	AIAgents     json.RawMessage `json:"aiAgents,omitempty"`
	Applications json.RawMessage `json:"applications,omitempty"`
	FlowModules  json.RawMessage `json:"flowModules,omitempty"`
}

type entityProfilesRec struct {
	InstanceID string   `json:"instanceId"`
	EntityARN  string   `json:"entityArn"`
	Profiles   []string `json:"profiles"`
}

// checkSecurityExtras validates the three lists and returns them ready to store; nil inputs stay nil.
func (b *Backend) checkSecurityExtras(inst string, aiAgents, apps, modules json.RawMessage) (securityExtras, error) {
	var out securityExtras

	if present(aiAgents) {
		if err := checkAIAgents(aiAgents); err != nil {
			return out, err
		}

		out.AIAgents = aiAgents
	}

	if present(apps) {
		if err := checkApplications(apps); err != nil {
			return out, err
		}

		out.Applications = apps
	}

	if present(modules) {
		if err := b.checkFlowModules(inst, modules); err != nil {
			return out, err
		}

		out.FlowModules = modules
	}

	return out, nil
}

func present(raw json.RawMessage) bool { return len(raw) > 0 && string(raw) != jsonNull }

func checkAIAgents(raw json.RawMessage) error {
	var items []struct {
		Arn  string `json:"Arn"`
		Type string `json:"Type"`
	}

	if err := decodeBounded(raw, &items, "AllowedAIAgents"); err != nil {
		return err
	}

	for _, i := range items {
		if err := firstError(
			oneOf("AllowedAIAgents.Type", i.Type, "THIRD_PARTY"),
			checkARN("AllowedAIAgents.Arn", i.Arn),
		); err != nil {
			return err
		}
	}

	return nil
}

func checkApplications(raw json.RawMessage) error {
	var items []struct {
		Namespace              string   `json:"Namespace"`
		Type                   string   `json:"Type"`
		ApplicationPermissions []string `json:"ApplicationPermissions"`
	}

	if err := decodeBounded(raw, &items, "Applications"); err != nil {
		return err
	}

	for _, i := range items {
		if err := firstError(
			checkName("Applications.Namespace", i.Namespace, maxNameLen),
			oneOf("Applications.Type", i.Type, "MCP", "THIRD_PARTY_APPLICATION"),
		); err != nil {
			return err
		}
	}

	return nil
}

func (b *Backend) checkFlowModules(inst string, raw json.RawMessage) error {
	var items []struct {
		FlowModuleID string `json:"FlowModuleId"`
		Type         string `json:"Type"`
	}

	if err := decodeBounded(raw, &items, "AllowedFlowModules"); err != nil {
		return err
	}

	for _, i := range items {
		if err := oneOf("AllowedFlowModules.Type", i.Type, "MCP"); err != nil {
			return err
		}

		if !b.modules.has(inst, i.FlowModuleID) {
			return notFoundErr(labelModule, i.FlowModuleID)
		}
	}

	return nil
}

func decodeBounded(raw json.RawMessage, dst any, field string) error {
	if err := json.Unmarshal(raw, dst); err != nil {
		return invalidParamErr("%s is not valid", field)
	}

	var list []json.RawMessage
	if json.Unmarshal(raw, &list) == nil && len(list) > maxSecurityExtras {
		return invalidParamErr("%s can hold at most %d entries", field, maxSecurityExtras)
	}

	return nil
}

func checkARN(field, v string) error {
	if !strings.HasPrefix(v, "arn:") || len(v) > maxARNLen {
		return invalidParamErr("%s must be an ARN", field)
	}

	return nil
}

const maxARNLen = 2048

func (e *securityExtras) merge(in securityExtras) {
	if in.AIAgents != nil {
		e.AIAgents = in.AIAgents
	}

	if in.Applications != nil {
		e.Applications = in.Applications
	}

	if in.FlowModules != nil {
		e.FlowModules = in.FlowModules
	}
}

// SecurityListInput is the shared request of the ListSecurityProfile* extras.
type SecurityListInput struct {
	ProfileID string `json:"-" path:"SecurityProfileId"`
	NextToken string `json:"-"                          query:"nextToken"`
	instPath
	MaxResults int32 `json:"-"                          query:"maxResults"`
}

// SecurityListOut is the response of the ListSecurityProfile* extras; the list key varies.
type SecurityListOut map[string]any

func (b *Backend) listSecurityExtra(
	op, key string, in *SecurityListInput, pick func(*securityExtras) json.RawMessage,
) (*SecurityListOut, error) {
	var (
		items []json.RawMessage
		stamp Stamp
	)

	if _, err := readInstance(b, op, in.InstanceID, func(inst string) (*empty, error) {
		p, err := b.securityProfiles.get(inst, in.ProfileID)
		if err != nil {
			return nil, err
		}

		stamp = p.Profile.Stamp

		if raw := pick(&p.Extras); raw != nil {
			err = json.Unmarshal(raw, &items)
		}

		return &empty{}, err
	}); err != nil {
		return nil, err
	}

	page, next, err := paged(items, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	out := SecurityListOut{key: orEmpty(page), "LastModifiedRegion": stamp.LastModifiedRegion}
	if stamp.LastModifiedTime != nil {
		out["LastModifiedTime"] = stamp.LastModifiedTime
	}

	if next != "" {
		out["NextToken"] = next
	}

	return &out, nil
}

// ListSecurityProfileApplications lists the applications a security profile grants.
func (b *Backend) ListSecurityProfileApplications(in *SecurityListInput) (*SecurityListOut, error) {
	return b.listSecurityExtra("ListSecurityProfileApplications", "Applications", in,
		func(e *securityExtras) json.RawMessage { return e.Applications })
}

// ListSecurityProfileFlowModules lists the flow modules a security profile allows.
func (b *Backend) ListSecurityProfileFlowModules(in *SecurityListInput) (*SecurityListOut, error) {
	return b.listSecurityExtra("ListSecurityProfileFlowModules", "AllowedFlowModules", in,
		func(e *securityExtras) json.RawMessage { return e.FlowModules })
}

// ListSecurityProfileAIAgents lists the AI agents a security profile allows.
func (b *Backend) ListSecurityProfileAIAgents(in *SecurityListInput) (*SecurityListOut, error) {
	return b.listSecurityExtra("ListSecurityProfileAIAgents", "AllowedAIAgents", in,
		func(e *securityExtras) json.RawMessage { return e.AIAgents })
}

// EntityProfilesInput is the Associate/Disassociate/ListEntitySecurityProfiles request.
type EntityProfilesInput struct {
	EntityType string
	EntityArn  string
	NextToken  string
	instPath
	SecurityProfiles []struct {
		ID string `json:"Id"`
	}
	MaxResults int32
}

// SecurityProfileItem is one security profile reference.
type SecurityProfileItem struct {
	ID string `json:"Id"`
}

// EntityProfilesOut is the ListEntitySecurityProfiles response.
type EntityProfilesOut struct {
	NextToken        string                `json:"NextToken,omitempty"`
	SecurityProfiles []SecurityProfileItem `json:"SecurityProfiles"`
}

func (in *EntityProfilesInput) ids() []string {
	out := make([]string, 0, len(in.SecurityProfiles))
	for _, p := range in.SecurityProfiles {
		out = append(out, p.ID)
	}

	return out
}

// entityProfiles returns the live profile list of an entity and a setter that stores a new one.
func (b *Backend) entityProfiles(inst string, in *EntityProfilesInput) ([]string, func([]string), error) {
	if err := oneOf("EntityType", in.EntityType, entityUser, "AI_AGENT"); err != nil {
		return nil, nil, err
	}

	if err := checkARN("EntityArn", in.EntityArn); err != nil {
		return nil, nil, err
	}

	if in.EntityType == entityUser {
		for _, u := range b.users.in(inst) {
			if u.User.Arn == in.EntityArn {
				return u.User.SecurityProfileIDs, func(ids []string) { u.User.SecurityProfileIDs = ids }, nil
			}
		}

		return nil, nil, notFoundErr("User", in.EntityArn)
	}

	rec, ok := b.entityProfileRecs.table.Get(inst + "/" + in.EntityArn)
	if !ok {
		rec = &entityProfilesRec{InstanceID: inst, EntityARN: in.EntityArn}
	}

	return rec.Profiles, func(ids []string) {
		rec.Profiles = ids
		b.entityProfileRecs.put(rec)
	}, nil
}

func (b *Backend) changeEntityProfiles(
	ctx context.Context, op string, in *EntityProfilesInput, add bool,
) (*empty, error) {
	ids := in.ids()
	if len(ids) == 0 {
		return nil, invalidParamErr("SecurityProfiles is required")
	}

	return writeInstance(b, op, in.InstanceID, func(inst string) (*empty, error) {
		cur, set, err := b.entityProfiles(inst, in)
		if err != nil {
			return nil, err
		}

		next, err := b.mergeProfileIDs(inst, cur, ids, add)
		if err != nil {
			return nil, err
		}

		set(next)

		if in.EntityType == entityUser {
			b.touchUser(ctx, inst, in.EntityArn)
		}

		return &empty{}, nil
	})
}

func (b *Backend) touchUser(ctx context.Context, inst, userARN string) {
	for _, u := range b.users.in(inst) {
		if u.User.Arn == userARN {
			u.User.Stamp = b.stamp(ctx)
		}
	}
}

func (b *Backend) mergeProfileIDs(inst string, cur, ids []string, add bool) ([]string, error) {
	next := slices.Clone(cur)

	for _, id := range ids {
		switch {
		case add && !b.securityProfiles.has(inst, id):
			return nil, notFoundErr("SecurityProfile", id)
		case add && !slices.Contains(next, id):
			next = append(next, id)
		case !add && !slices.Contains(next, id):
			return nil, notFoundErr("SecurityProfile", id)
		case !add:
			next = slices.DeleteFunc(next, func(x string) bool { return x == id })
		}
	}

	if len(next) > maxUserSecProfile {
		return nil, invalidParamErr("an entity can have at most %d security profiles", maxUserSecProfile)
	}

	return next, nil
}

// AssociateSecurityProfiles adds security profiles to a user or AI agent.
func (b *Backend) AssociateSecurityProfiles(ctx context.Context, in *EntityProfilesInput) (*empty, error) {
	return b.changeEntityProfiles(ctx, "AssociateSecurityProfiles", in, true)
}

// DisassociateSecurityProfiles removes security profiles from a user or AI agent.
func (b *Backend) DisassociateSecurityProfiles(ctx context.Context, in *EntityProfilesInput) (*empty, error) {
	return b.changeEntityProfiles(ctx, "DisassociateSecurityProfiles", in, false)
}

// ListEntitySecurityProfiles pages through the security profiles of a user or AI agent.
func (b *Backend) ListEntitySecurityProfiles(in *EntityProfilesInput) (*EntityProfilesOut, error) {
	all, err := readInstance(
		b,
		"ListEntitySecurityProfiles",
		in.InstanceID,
		func(inst string) ([]SecurityProfileItem, error) {
			cur, _, perr := b.entityProfiles(inst, in)
			if perr != nil {
				return nil, perr
			}

			out := make([]SecurityProfileItem, 0, len(cur))
			for _, id := range cur {
				out = append(out, SecurityProfileItem{ID: id})
			}

			return out, nil
		},
	)
	if err != nil {
		return nil, err
	}

	page, next, err := paged(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &EntityProfilesOut{NextToken: next, SecurityProfiles: orEmpty(page)}, nil
}
