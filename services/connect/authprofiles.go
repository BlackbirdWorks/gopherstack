package connect

import (
	"context"
	"net/netip"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	labelAuthProfile             = "AuthenticationProfile"
	defaultAuthProfileName       = "Default Authentication Profile"
	defaultAuthProfileDesc       = "Default authentication profile"
	maxAuthProfileDesc           = 250
	defaultProfileSessionMinutes = 600
	maxAuthProfileIPEntries      = 100
)

// AuthenticationProfile is a session and IP access policy for the agents of an instance.
type AuthenticationProfile struct {
	CreatedTime                      *awstime.EpochTime `json:"CreatedTime,omitempty"`
	LastModifiedTime                 *awstime.EpochTime `json:"LastModifiedTime,omitempty"`
	MaxSessionDuration               *int32             `json:"MaxSessionDuration,omitempty"`
	PeriodicSessionDuration          *int32             `json:"PeriodicSessionDuration,omitempty"`
	SessionInactivityDuration        *int32             `json:"SessionInactivityDuration,omitempty"`
	SessionInactivityHandlingEnabled *bool              `json:"SessionInactivityHandlingEnabled,omitempty"`
	Description                      string             `json:"Description,omitempty"`
	Arn                              string             `json:"Arn"`
	ID                               string             `json:"Id"`
	LastModifiedRegion               string             `json:"LastModifiedRegion,omitempty"`
	Name                             string             `json:"Name"`
	BlockedIps                       []string           `json:"BlockedIps,omitempty"`
	AllowedIps                       []string           `json:"AllowedIps,omitempty"`
	IsDefault                        bool               `json:"IsDefault"`
}

// AuthenticationProfileSummary is the list shape of an authentication profile.
type AuthenticationProfileSummary struct {
	Stamp
	Arn       string `json:"Arn"`
	ID        string `json:"Id"`
	Name      string `json:"Name"`
	IsDefault bool   `json:"IsDefault"`
}

type authProfileRec struct {
	InstanceID string                `json:"instanceId"`
	Profile    AuthenticationProfile `json:"profile"`
}

// UpdateAuthenticationProfileInput is the UpdateAuthenticationProfile request.
type UpdateAuthenticationProfileInput struct {
	Description                      *string
	Name                             *string
	PeriodicSessionDuration          *int32
	SessionInactivityDuration        *int32
	SessionInactivityHandlingEnabled *bool
	AuthenticationProfileID          string `json:"-" path:"AuthenticationProfileId"`
	instPath
	AllowedIps []string
	BlockedIps []string
}

func checkIPList(field string, ips []string) error {
	if len(ips) > maxAuthProfileIPEntries {
		return invalidParamErr("%s can hold at most %d ranges", field, maxAuthProfileIPEntries)
	}

	for _, ip := range ips {
		if _, err := netip.ParsePrefix(ip); err != nil {
			return invalidParamErr("%s entry %q is not a CIDR range", field, ip)
		}
	}

	return nil
}

func (b *Backend) ensureDefaultAuthProfile(ctx context.Context, inst string) {
	for _, r := range b.authProfiles.in(inst) {
		if r.Profile.IsDefault {
			return
		}
	}

	id := newID()
	created := awstime.NewEpochTime(b.now())
	maxSession := int32(defaultProfileSessionMinutes)
	st := b.stamp(ctx)
	b.authProfiles.put(&authProfileRec{InstanceID: inst, Profile: AuthenticationProfile{
		Arn: b.buildARN(ctx, "instance/"+inst+"/authentication-profile/"+id), ID: id,
		Name: defaultAuthProfileName, Description: defaultAuthProfileDesc, IsDefault: true,
		CreatedTime: &created, LastModifiedTime: st.LastModifiedTime, LastModifiedRegion: st.LastModifiedRegion,
		MaxSessionDuration: &maxSession,
	}})
}

func (r *authProfileRec) view() AuthenticationProfile {
	out := r.Profile
	out.AllowedIps = slices.Clone(out.AllowedIps)
	out.BlockedIps = slices.Clone(out.BlockedIps)

	return out
}

// DescribeAuthenticationProfile returns an authentication profile; the default one exists from the start.
func (b *Backend) DescribeAuthenticationProfile(
	ctx context.Context, inst, id string,
) (*AuthenticationProfile, error) {
	return writeInstance(b, "DescribeAuthenticationProfile", inst, func(inst string) (*AuthenticationProfile, error) {
		b.ensureDefaultAuthProfile(ctx, inst)

		r, err := b.authProfiles.get(inst, id)
		if err != nil {
			return nil, err
		}

		out := r.view()

		return &out, nil
	})
}

// ListAuthenticationProfiles pages through the authentication profiles, sorted by name.
func (b *Backend) ListAuthenticationProfiles(
	ctx context.Context, inst, token string, maxResults int32,
) ([]AuthenticationProfileSummary, string, error) {
	all, err := writeInstance(b, "ListAuthenticationProfiles", inst,
		func(inst string) ([]AuthenticationProfileSummary, error) {
			b.ensureDefaultAuthProfile(ctx, inst)

			var out []AuthenticationProfileSummary

			for _, r := range b.authProfiles.in(inst) {
				p := r.Profile
				sum := AuthenticationProfileSummary{Arn: p.Arn, ID: p.ID, Name: p.Name, IsDefault: p.IsDefault}
				sum.LastModifiedTime, sum.LastModifiedRegion = p.LastModifiedTime, p.LastModifiedRegion
				out = append(out, sum)
			}

			return out, nil
		})
	if err != nil {
		return nil, "", err
	}

	slices.SortFunc(all, func(a, c AuthenticationProfileSummary) int {
		return strings.Compare(strings.ToLower(a.Name), strings.ToLower(c.Name))
	})

	return paged(all, token, maxResults)
}

func checkAuthProfileUpdate(in *UpdateAuthenticationProfileInput) error {
	if in.Name != nil {
		if err := checkName("Name", *in.Name, maxNameLen); err != nil {
			return err
		}
	}

	if in.Description != nil && len(*in.Description) > maxAuthProfileDesc {
		return invalidParamErr("Description must be at most %d characters", maxAuthProfileDesc)
	}

	for _, d := range []*int32{in.PeriodicSessionDuration, in.SessionInactivityDuration} {
		if d != nil && *d < 1 {
			return invalidParamErr("session durations must be positive minutes")
		}
	}

	return firstError(checkIPList("AllowedIps", in.AllowedIps), checkIPList("BlockedIps", in.BlockedIps))
}

// UpdateAuthenticationProfile changes the supplied settings of an authentication profile.
func (b *Backend) UpdateAuthenticationProfile(
	ctx context.Context, in *UpdateAuthenticationProfileInput,
) (*empty, error) {
	if err := checkAuthProfileUpdate(in); err != nil {
		return nil, err
	}

	return writeInstance(b, "UpdateAuthenticationProfile", in.InstanceID, func(inst string) (*empty, error) {
		b.ensureDefaultAuthProfile(ctx, inst)

		r, err := b.authProfiles.get(inst, in.AuthenticationProfileID)
		if err != nil {
			return nil, err
		}

		p := &r.Profile

		if in.Name != nil {
			if nameExists(b.authProfiles, inst, func(x *authProfileRec) string { return x.Profile.Name },
				*in.Name, p.ID) {
				return nil, duplicateErr(labelAuthProfile, *in.Name)
			}

			p.Name = *in.Name
		}

		if in.Description != nil {
			p.Description = *in.Description
		}

		if in.AllowedIps != nil {
			p.AllowedIps = slices.Clone(in.AllowedIps)
		}

		if in.BlockedIps != nil {
			p.BlockedIps = slices.Clone(in.BlockedIps)
		}

		if in.PeriodicSessionDuration != nil {
			p.PeriodicSessionDuration = in.PeriodicSessionDuration
		}

		if in.SessionInactivityDuration != nil {
			p.SessionInactivityDuration = in.SessionInactivityDuration
		}

		if in.SessionInactivityHandlingEnabled != nil {
			p.SessionInactivityHandlingEnabled = in.SessionInactivityHandlingEnabled
		}

		st := b.stamp(ctx)
		p.LastModifiedTime, p.LastModifiedRegion = st.LastModifiedTime, st.LastModifiedRegion

		return &empty{}, nil
	})
}
