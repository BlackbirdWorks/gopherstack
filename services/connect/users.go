package connect

import (
	"context"
	"encoding/json"
	"slices"
	"unicode"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	maxUsernameLen    = 100
	minPasswordLen    = 8
	maxPasswordLen    = 64
	maxUserSecProfile = 10
)

// CreateUserInput is the CreateUser request.
type CreateUserInput struct {
	PhoneConfig  *UserPhoneConfig
	Tags         map[string]string
	IdentityInfo *UserIdentityInfo
	Password     string
	instPath
	RoutingProfileID            string `json:"RoutingProfileId"`
	HierarchyGroupID            string `json:"HierarchyGroupId"`
	DirectoryUserID             string `json:"DirectoryUserId"`
	Username                    string
	AfterContactWorkConfigs     json.RawMessage
	VoiceEnhancementConfigs     json.RawMessage
	PhoneNumberConfigs          json.RawMessage
	PersistentConnectionConfigs json.RawMessage
	SecurityProfileIDs          []string `json:"SecurityProfileIds"`
	AutoAcceptConfigs           json.RawMessage
}

// UpdateUserInput carries the user update requests; each op reads only its own members.
type UpdateUserInput struct {
	IdentityInfo     *UserIdentityInfo
	PhoneConfig      *UserPhoneConfig
	HierarchyGroupID string `json:"HierarchyGroupId"`
	RoutingProfileID string `json:"RoutingProfileId"`
	AgentStatusID    string `json:"AgentStatusId"`
	UserID           string `json:"-"                  path:"UserId"`
	instPath
	SecurityProfileIDs []string `json:"SecurityProfileIds"`
}

func validPassword(p string) bool {
	var upper, lower, digit bool

	for _, r := range p {
		switch {
		case unicode.IsUpper(r):
			upper = true
		case unicode.IsLower(r):
			lower = true
		case unicode.IsDigit(r):
			digit = true
		}
	}

	return len(p) >= minPasswordLen && len(p) <= maxPasswordLen && upper && lower && digit
}

func checkPhoneConfig(c *UserPhoneConfig) error {
	if c == nil {
		return invalidParamErr("PhoneConfig is required")
	}

	if err := oneOf("PhoneConfig.PhoneType", c.PhoneType, "SOFT_PHONE", "DESK_PHONE"); err != nil {
		return err
	}

	if c.PhoneType == "DESK_PHONE" && !e164Pattern.MatchString(c.DeskPhoneNumber) {
		return invalidParamErr("PhoneConfig.DeskPhoneNumber must be an E.164 number for DESK_PHONE")
	}

	if c.AfterContactWorkTimeLimit < 0 {
		return invalidParamErr("PhoneConfig.AfterContactWorkTimeLimit must not be negative")
	}

	return nil
}

func (b *Backend) checkSecurityProfiles(inst string, ids []string) error {
	if len(ids) == 0 || len(ids) > maxUserSecProfile {
		return invalidParamErr("SecurityProfileIds must hold 1-%d IDs", maxUserSecProfile)
	}

	for _, id := range ids {
		if !b.securityProfiles.has(inst, id) {
			return notFoundErr("SecurityProfile", id)
		}
	}

	return nil
}

func validateUserBasics(in *CreateUserInput, idType string) error {
	if err := checkName("Username", in.Username, maxUsernameLen); err != nil {
		return err
	}

	switch {
	case idType == dirConnectManaged && in.Password == "":
		return invalidParamErr("Password is required when Amazon Connect manages identities")
	case idType != dirConnectManaged && in.Password != "":
		return invalidParamErr("Password must not be set unless Amazon Connect manages identities")
	case in.Password != "" && !validPassword(in.Password):
		return invalidParamErr(
			"Password must be %d-%d characters with upper case, lower case and a digit",
			minPasswordLen,
			maxPasswordLen,
		)
	}

	if err := checkPhoneConfig(in.PhoneConfig); err != nil {
		return err
	}

	return checkTags(in.Tags)
}

// CreateUser creates a user.
func (b *Backend) CreateUser(ctx context.Context, in *CreateUserInput) (*userOut, error) {
	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID

	if err = validateUserBasics(in, r.Instance.IdentityManagementType); err != nil {
		return nil, err
	}

	if !b.routingProfiles.has(inst, in.RoutingProfileID) {
		return nil, notFoundErr("RoutingProfile", in.RoutingProfileID)
	}

	if err = b.checkSecurityProfiles(inst, in.SecurityProfileIDs); err != nil {
		return nil, err
	}

	if err = b.checkHierarchyRef(inst, in.HierarchyGroupID); err != nil {
		return nil, err
	}

	if nameExists(b.users, inst, func(u *userRec) string { return u.User.Username }, in.Username, "") {
		return nil, duplicateErr("User", in.Username)
	}

	id := uuid.NewString()
	rec := &userRec{InstanceID: inst, User: User{
		Stamp: b.stamp(ctx), IdentityInfo: in.IdentityInfo, PhoneConfig: in.PhoneConfig, Tags: cloneTags(in.Tags),
		Arn: b.buildARN(ctx, "instance/"+inst+"/agent/"+id), DirectoryUserID: in.DirectoryUserID,
		HierarchyGroupID: in.HierarchyGroupID, ID: id, RoutingProfileID: in.RoutingProfileID, Username: in.Username,
		SecurityProfileIDs:      slices.Clone(in.SecurityProfileIDs),
		AfterContactWorkConfigs: in.AfterContactWorkConfigs, AutoAcceptConfigs: in.AutoAcceptConfigs,
		PersistentConnectionConfigs: in.PersistentConnectionConfigs, PhoneNumberConfigs: in.PhoneNumberConfigs,
		VoiceEnhancementConfigs: in.VoiceEnhancementConfigs,
	}}
	b.users.put(rec)

	return &userOut{UserArn: rec.User.Arn, UserID: id}, nil
}

// DescribeUser returns a user.
func (b *Backend) DescribeUser(inst, id string) (*User, error) {
	b.mu.RLock("DescribeUser")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	u, err := b.users.get(r.Instance.ID, id)
	if err != nil {
		return nil, err
	}

	out := u.User
	out.Tags = cloneTags(out.Tags)
	out.SecurityProfileIDs = slices.Clone(out.SecurityProfileIDs)

	return &out, nil
}

// ListUsers pages through users.
func (b *Backend) ListUsers(inst, token string, maxResults int32) ([]UserSummary, string, error) {
	b.mu.RLock("ListUsers")

	r, err := b.instanceLocked(inst)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []UserSummary
	for _, u := range b.users.in(r.Instance.ID) {
		all = append(all, UserSummary{Stamp: u.User.Stamp, Arn: u.User.Arn, ID: u.User.ID, Username: u.User.Username})
	}

	b.mu.RUnlock()

	return paged(all, token, maxResults)
}

func (b *Backend) updateUser(ctx context.Context, in *UpdateUserInput, apply func(u *userRec) error) (*empty, error) {
	b.mu.Lock("UpdateUser")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	u, err := b.users.get(r.Instance.ID, in.UserID)
	if err != nil {
		return nil, apiErr(statusNotFound, "ResourceNotFoundException", "User %q was not found", in.UserID)
	}

	if err = apply(u); err != nil {
		return nil, err
	}

	u.User.Stamp = b.stamp(ctx)

	return &empty{}, nil
}

// UpdateUserIdentityInfo replaces a user's identity info.
func (b *Backend) UpdateUserIdentityInfo(ctx context.Context, in *UpdateUserInput) (*empty, error) {
	if in.IdentityInfo == nil {
		return nil, invalidParamErr("IdentityInfo is required")
	}

	return b.updateUser(ctx, in, func(u *userRec) error {
		u.User.IdentityInfo = in.IdentityInfo

		return nil
	})
}

// UpdateUserPhoneConfig replaces a user's phone config.
func (b *Backend) UpdateUserPhoneConfig(ctx context.Context, in *UpdateUserInput) (*empty, error) {
	if err := checkPhoneConfig(in.PhoneConfig); err != nil {
		return nil, err
	}

	return b.updateUser(ctx, in, func(u *userRec) error {
		u.User.PhoneConfig = in.PhoneConfig

		return nil
	})
}

// UpdateUserRoutingProfile moves a user to another routing profile.
func (b *Backend) UpdateUserRoutingProfile(ctx context.Context, in *UpdateUserInput) (*empty, error) {
	return b.updateUser(ctx, in, func(u *userRec) error {
		if !b.routingProfiles.has(u.InstanceID, in.RoutingProfileID) {
			return notFoundErr("RoutingProfile", in.RoutingProfileID)
		}

		u.User.RoutingProfileID = in.RoutingProfileID

		return nil
	})
}

// UpdateUserSecurityProfiles replaces a user's security profiles.
func (b *Backend) UpdateUserSecurityProfiles(ctx context.Context, in *UpdateUserInput) (*empty, error) {
	return b.updateUser(ctx, in, func(u *userRec) error {
		if err := b.checkSecurityProfiles(u.InstanceID, in.SecurityProfileIDs); err != nil {
			return err
		}

		u.User.SecurityProfileIDs = slices.Clone(in.SecurityProfileIDs)

		return nil
	})
}

// UpdateUserHierarchy sets or clears a user's hierarchy group.
func (b *Backend) UpdateUserHierarchy(ctx context.Context, in *UpdateUserInput) (*empty, error) {
	return b.updateUser(ctx, in, func(u *userRec) error {
		if err := b.checkHierarchyRef(u.InstanceID, in.HierarchyGroupID); err != nil {
			return err
		}

		u.User.HierarchyGroupID = in.HierarchyGroupID

		return nil
	})
}

// PutUserStatus sets the agent status of a user.
func (b *Backend) PutUserStatus(ctx context.Context, in *UpdateUserInput) (*empty, error) {
	return b.updateUser(ctx, in, func(u *userRec) error {
		s, err := b.agentStatuses.get(u.InstanceID, in.AgentStatusID)
		if err != nil {
			return err
		}

		if s.Status.State != "ENABLED" {
			return invalidRequestErr("AgentStatus %q is disabled", in.AgentStatusID)
		}

		now := awstime.NewEpochTime(b.now())
		u.AgentStatusID, u.StatusStart = in.AgentStatusID, &now

		return nil
	})
}

// DeleteUser removes a user that is not handling a contact.
func (b *Backend) DeleteUser(inst, id string) (*empty, error) {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	if slices.ContainsFunc(b.contacts.in(r.Instance.ID), func(c *contactRec) bool {
		return c.Contact.DisconnectTimestamp == nil && c.Contact.AgentInfo != nil && c.Contact.AgentInfo.ID == id
	}) {
		return nil, inUseErr("User %q is handling a contact", id)
	}

	if err = b.users.del(r.Instance.ID, id); err != nil {
		return nil, err
	}

	return &empty{}, nil
}
