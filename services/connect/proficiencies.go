package connect

import (
	"context"
	"encoding/json"
	"math"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	maxProficiencies = 100
	minProficiency   = 1.0
	maxProficiency   = 5.0
	proficiencyStep  = 0.5
)

// UserProficiency is a skill of an agent.
type UserProficiency struct {
	AttributeName  string  `json:"AttributeName"`
	AttributeValue string  `json:"AttributeValue"`
	Level          float32 `json:"Level"`
}

// UserProficiencyDisassociate names a proficiency to remove.
type UserProficiencyDisassociate struct {
	AttributeName  string `json:"AttributeName"`
	AttributeValue string `json:"AttributeValue"`
}

// ProficienciesInput is the Associate/Update/List/Disassociate proficiencies request.
type ProficienciesInput struct {
	UserID    string `json:"-" path:"UserId"`
	NextToken string `json:"-"               query:"nextToken"`
	instPath
	UserProficiencies []UserProficiency
	MaxResults        int32 `json:"-"               query:"maxResults"`
}

// DisassociateProficienciesInput is the DisassociateUserProficiencies request.
type DisassociateProficienciesInput struct {
	UserID string `json:"-" path:"UserId"`
	instPath
	UserProficiencies []UserProficiencyDisassociate
}

// ProficienciesOut is the ListUserProficiencies response.
type ProficienciesOut struct {
	LastModifiedTime    *awstime.EpochTime `json:"LastModifiedTime,omitempty"`
	NextToken           string             `json:"NextToken,omitempty"`
	LastModifiedRegion  string             `json:"LastModifiedRegion,omitempty"`
	UserProficiencyList []UserProficiency  `json:"UserProficiencyList"`
}

func userProficiencies(u *userRec) []proficiency {
	out := make([]proficiency, 0, len(u.Proficiencies))
	for _, p := range u.Proficiencies {
		out = append(out, proficiency{name: p.AttributeName, value: p.AttributeValue, level: float64(p.Level)})
	}

	return out
}

func (b *Backend) checkProficiency(inst string, p UserProficiency) error {
	lvl := float64(p.Level)
	if lvl < minProficiency || lvl > maxProficiency || math.Mod(lvl, proficiencyStep) != 0 {
		return invalidParamErr("Level must be between 1.0 and 5.0 in steps of 0.5")
	}

	attr, err := b.predefined.get(inst, p.AttributeName)
	if err != nil {
		return err
	}

	cfg := attr.Attribute.AttributeConfiguration
	if cfg != nil && cfg.EnableValueValidationOnAssociation &&
		(attr.Attribute.Values == nil || !slices.Contains(attr.Attribute.Values.StringList, p.AttributeValue)) {
		return invalidParamErr("%q is not a value of predefined attribute %q", p.AttributeValue, p.AttributeName)
	}

	return checkName("AttributeValue", p.AttributeValue, maxPredefinedValueLen)
}

func findProficiency(list []UserProficiency, name, value string) int {
	return slices.IndexFunc(list, func(p UserProficiency) bool {
		return p.AttributeName == name && p.AttributeValue == value
	})
}

func (b *Backend) changeProficiencies(
	ctx context.Context, op string, in *ProficienciesInput, create bool,
) (*empty, error) {
	if len(in.UserProficiencies) == 0 {
		return nil, invalidParamErr("UserProficiencies is required")
	}

	return writeInstance(b, op, in.InstanceID, func(inst string) (*empty, error) {
		u, err := b.users.get(inst, in.UserID)
		if err != nil {
			return nil, err
		}

		next := slices.Clone(u.Proficiencies)

		for _, p := range in.UserProficiencies {
			if err = b.checkProficiency(inst, p); err != nil {
				return nil, err
			}

			i := findProficiency(next, p.AttributeName, p.AttributeValue)

			switch {
			case i >= 0:
				next[i].Level = p.Level
			case create:
				next = append(next, p)
			default:
				return nil, notFoundErr("UserProficiency", p.AttributeName+"="+p.AttributeValue)
			}
		}

		if len(next) > maxProficiencies {
			return nil, limitErr("a user can have at most %d proficiencies", maxProficiencies)
		}

		u.Proficiencies = next
		u.User.Stamp = b.stamp(ctx)

		return &empty{}, nil
	})
}

// AssociateUserProficiencies adds or re-levels proficiencies of a user.
func (b *Backend) AssociateUserProficiencies(ctx context.Context, in *ProficienciesInput) (*empty, error) {
	return b.changeProficiencies(ctx, "AssociateUserProficiencies", in, true)
}

// UpdateUserProficiencies changes the level of proficiencies a user already has.
func (b *Backend) UpdateUserProficiencies(ctx context.Context, in *ProficienciesInput) (*empty, error) {
	return b.changeProficiencies(ctx, "UpdateUserProficiencies", in, false)
}

// DisassociateUserProficiencies removes proficiencies from a user.
func (b *Backend) DisassociateUserProficiencies(
	ctx context.Context, in *DisassociateProficienciesInput,
) (*empty, error) {
	if len(in.UserProficiencies) == 0 {
		return nil, invalidParamErr("UserProficiencies is required")
	}

	return writeInstance(b, "DisassociateUserProficiencies", in.InstanceID, func(inst string) (*empty, error) {
		u, err := b.users.get(inst, in.UserID)
		if err != nil {
			return nil, err
		}

		next := slices.Clone(u.Proficiencies)

		for _, d := range in.UserProficiencies {
			i := findProficiency(next, d.AttributeName, d.AttributeValue)
			if i < 0 {
				return nil, notFoundErr("UserProficiency", d.AttributeName+"="+d.AttributeValue)
			}

			next = slices.Delete(next, i, i+1)
		}

		u.Proficiencies = next
		u.User.Stamp = b.stamp(ctx)

		return &empty{}, nil
	})
}

// ListUserProficiencies pages through the proficiencies of a user.
func (b *Backend) ListUserProficiencies(in *ProficienciesInput) (*ProficienciesOut, error) {
	var stamp Stamp

	all, err := readInstance(b, "ListUserProficiencies", in.InstanceID, func(inst string) ([]UserProficiency, error) {
		u, uerr := b.users.get(inst, in.UserID)
		if uerr != nil {
			return nil, uerr
		}

		stamp = u.User.Stamp

		return slices.Clone(u.Proficiencies), nil
	})
	if err != nil {
		return nil, err
	}

	page, next, err := paged(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	out := &ProficienciesOut{
		UserProficiencyList: orEmpty(page),
		NextToken:           next,
		LastModifiedRegion:  stamp.LastModifiedRegion,
	}
	out.LastModifiedTime = stamp.LastModifiedTime

	return out, nil
}

// UpdateUserConfigInput is the UpdateUserConfig request.
type UpdateUserConfigInput struct {
	UserID string `json:"-" path:"UserId"`
	instPath
	AfterContactWorkConfigs     json.RawMessage
	AutoAcceptConfigs           json.RawMessage
	PersistentConnectionConfigs json.RawMessage
	PhoneNumberConfigs          json.RawMessage
	VoiceEnhancementConfigs     json.RawMessage
}

var userChannels = []string{"VOICE", "CHAT", "TASK", emailValue} //nolint:gochecknoglobals // immutable lookup table

// checkChannelConfigs validates that every entry names a known channel and returns the normalised list.
func checkChannelConfigs(field string, raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == jsonNull {
		return nil, nil
	}

	var items []struct {
		Channel string `json:"Channel"`
	}

	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, invalidParamErr("%s is not valid", field)
	}

	seen := map[string]bool{}

	for _, it := range items {
		if err := oneOf(field+".Channel", it.Channel, userChannels...); err != nil {
			return nil, err
		}

		if seen[it.Channel] {
			return nil, invalidParamErr("%s lists channel %s twice", field, it.Channel)
		}

		seen[it.Channel] = true
	}

	return raw, nil
}

type userConfigField struct {
	dst  func(*User) *json.RawMessage
	name string
	raw  json.RawMessage
}

func userConfigFields(in *UpdateUserConfigInput) []userConfigField {
	return []userConfigField{
		{name: "AfterContactWorkConfigs", raw: in.AfterContactWorkConfigs,
			dst: func(u *User) *json.RawMessage { return &u.AfterContactWorkConfigs }},
		{name: "AutoAcceptConfigs", raw: in.AutoAcceptConfigs,
			dst: func(u *User) *json.RawMessage { return &u.AutoAcceptConfigs }},
		{name: "PersistentConnectionConfigs", raw: in.PersistentConnectionConfigs,
			dst: func(u *User) *json.RawMessage { return &u.PersistentConnectionConfigs }},
		{name: "PhoneNumberConfigs", raw: in.PhoneNumberConfigs,
			dst: func(u *User) *json.RawMessage { return &u.PhoneNumberConfigs }},
		{name: "VoiceEnhancementConfigs", raw: in.VoiceEnhancementConfigs,
			dst: func(u *User) *json.RawMessage { return &u.VoiceEnhancementConfigs }},
	}
}

// UpdateUserConfig replaces the supplied per-channel settings of a user.
func (b *Backend) UpdateUserConfig(ctx context.Context, in *UpdateUserConfigInput) (*empty, error) {
	fields := userConfigFields(in)
	checked := make([]json.RawMessage, len(fields))

	for i, f := range fields {
		raw, err := checkChannelConfigs(f.name, f.raw)
		if err != nil {
			return nil, err
		}

		checked[i] = raw
	}

	return writeInstance(b, "UpdateUserConfig", in.InstanceID, func(inst string) (*empty, error) {
		u, err := b.users.get(inst, in.UserID)
		if err != nil {
			return nil, err
		}

		for i, f := range fields {
			if checked[i] != nil {
				*f.dst(&u.User) = checked[i]
			}
		}

		u.User.Stamp = b.stamp(ctx)

		return &empty{}, nil
	})
}
