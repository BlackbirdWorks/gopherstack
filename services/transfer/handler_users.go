package transfer

import (
	"context"
	"fmt"
	"time"
)

type posixProfileInput struct {
	SecondaryGids []int64 `json:"SecondaryGids,omitempty"`
	UID           int64   `json:"Uid"`
	GID           int64   `json:"Gid"`
}

type homeDirectoryMapEntryInput struct {
	Entry  string `json:"Entry"`
	Target string `json:"Target"`
	Type   string `json:"Type,omitempty"`
}

func toPosixProfile(in *posixProfileInput) *PosixProfile {
	if in == nil {
		return nil
	}

	return &PosixProfile{
		UID:           in.UID,
		GID:           in.GID,
		SecondaryGids: in.SecondaryGids,
	}
}

func toHomeDirectoryMappings(in []homeDirectoryMapEntryInput) []HomeDirectoryMapEntry {
	if in == nil {
		return nil
	}

	out := make([]HomeDirectoryMapEntry, len(in))
	for i, e := range in {
		out[i] = HomeDirectoryMapEntry(e)
	}

	return out
}

type createUserInput struct {
	PosixProfile          *posixProfileInput           `json:"PosixProfile,omitempty"`
	ServerID              string                       `json:"ServerId"`
	UserName              string                       `json:"UserName"`
	HomeDir               string                       `json:"HomeDirectory"`
	Role                  string                       `json:"Role"`
	HomeDirectoryType     string                       `json:"HomeDirectoryType,omitempty"`
	Policy                string                       `json:"Policy,omitempty"`
	HomeDirectoryMappings []homeDirectoryMapEntryInput `json:"HomeDirectoryMappings,omitempty"`
	Tags                  []map[string]string          `json:"Tags"`
}

type createUserOutput struct {
	ServerID string `json:"ServerId"`
	UserName string `json:"UserName"`
}

func (h *Handler) handleCreateUser(
	_ context.Context,
	in *createUserInput,
) (*createUserOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)

	u, err := h.Backend.CreateUserFull(&CreateUserInput{
		ServerID:              in.ServerID,
		UserName:              in.UserName,
		HomeDir:               in.HomeDir,
		Role:                  in.Role,
		HomeDirectoryType:     in.HomeDirectoryType,
		HomeDirectoryMappings: toHomeDirectoryMappings(in.HomeDirectoryMappings),
		Policy:                in.Policy,
		PosixProfile:          toPosixProfile(in.PosixProfile),
		Tags:                  tags,
	})
	if err != nil {
		return nil, err
	}

	return &createUserOutput{ServerID: u.ServerID, UserName: u.UserName}, nil
}

type describeUserInput struct {
	ServerID string `json:"ServerId"`
	UserName string `json:"UserName"`
}

type sshKeyView struct {
	DateImported     string `json:"DateImported"`
	SSHPublicKeyID   string `json:"SshPublicKeyId"`
	SSHPublicKeyBody string `json:"SshPublicKeyBody"`
	KeyType          string `json:"KeyType,omitempty"`
}

type posixProfileView struct {
	SecondaryGids []int64 `json:"SecondaryGids,omitempty"`
	UID           int64   `json:"Uid"`
	GID           int64   `json:"Gid"`
}

type homeDirectoryMapEntryView struct {
	Entry  string `json:"Entry"`
	Target string `json:"Target"`
	Type   string `json:"Type,omitempty"`
}

type userView struct {
	PosixProfile          *posixProfileView           `json:"PosixProfile,omitempty"`
	Arn                   string                      `json:"Arn"`
	UserName              string                      `json:"UserName"`
	HomeDir               string                      `json:"HomeDirectory"`
	Role                  string                      `json:"Role"`
	HomeDirectoryType     string                      `json:"HomeDirectoryType,omitempty"`
	Policy                string                      `json:"Policy,omitempty"`
	SSHPublicKeys         []sshKeyView                `json:"SshPublicKeys,omitempty"`
	HomeDirectoryMappings []homeDirectoryMapEntryView `json:"HomeDirectoryMappings,omitempty"`
	Tags                  []map[string]string         `json:"Tags"`
}

type describeUserOutput struct {
	ServerID string   `json:"ServerId"`
	User     userView `json:"User"`
}

func (h *Handler) handleDescribeUser(
	_ context.Context,
	in *describeUserInput,
) (*describeUserOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	u, err := h.Backend.DescribeUser(in.ServerID, in.UserName)
	if err != nil {
		return nil, err
	}

	// Include SSH public keys.
	sshKeys := h.Backend.ListSSHPublicKeys(in.ServerID, in.UserName)
	keyViews := make([]sshKeyView, len(sshKeys))
	for i, k := range sshKeys {
		keyViews[i] = sshKeyView{
			SSHPublicKeyID:   k.SSHPublicKeyID,
			SSHPublicKeyBody: k.SSHPublicKeyBody,
			DateImported:     k.DateImported.Format(time.RFC3339),
			KeyType:          k.KeyType,
		}
	}

	return &describeUserOutput{
		ServerID: u.ServerID,
		User:     toUserView(u, userARN(u.AccountID, u.Region, u.ServerID, u.UserName), keyViews),
	}, nil
}

type listUsersInput struct {
	ServerID   string `json:"ServerId"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type userListItem struct {
	Arn               string `json:"Arn"`
	UserName          string `json:"UserName"`
	HomeDir           string `json:"HomeDirectory"`
	Role              string `json:"Role"`
	HomeDirectoryType string `json:"HomeDirectoryType,omitempty"`
	SSHPublicKeyCount int    `json:"SshPublicKeyCount"`
}

type listUsersOutput struct {
	ServerID  string         `json:"ServerId"`
	NextToken string         `json:"NextToken,omitempty"`
	Users     []userListItem `json:"Users"`
}

func (h *Handler) handleListUsers(_ context.Context, in *listUsersInput) (*listUsersOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	users, err := h.Backend.ListUsers(in.ServerID)
	if err != nil {
		return nil, err
	}

	items := make([]userListItem, 0, len(users))

	for i := range users {
		u := &users[i]
		items = append(items, userListItem{
			Arn:               userARN(u.AccountID, u.Region, u.ServerID, u.UserName),
			UserName:          u.UserName,
			HomeDir:           u.HomeDir,
			Role:              u.Role,
			HomeDirectoryType: u.HomeDirectoryType,
			SSHPublicKeyCount: h.Backend.CountUserSSHPublicKeys(in.ServerID, u.UserName),
		})
	}

	items, nextToken := applyNextTokenItems(items, in.NextToken, in.MaxResults)

	return &listUsersOutput{ServerID: in.ServerID, Users: items, NextToken: nextToken}, nil
}

func (h *Handler) handleDeleteUser(_ context.Context, in *describeUserInput) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteUser(in.ServerID, in.UserName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type updateUserInput struct {
	PosixProfile          *posixProfileInput           `json:"PosixProfile,omitempty"`
	ServerID              string                       `json:"ServerId"`
	UserName              string                       `json:"UserName"`
	HomeDir               string                       `json:"HomeDirectory"`
	Role                  string                       `json:"Role"`
	HomeDirectoryType     string                       `json:"HomeDirectoryType,omitempty"`
	Policy                string                       `json:"Policy,omitempty"`
	HomeDirectoryMappings []homeDirectoryMapEntryInput `json:"HomeDirectoryMappings,omitempty"`
}

type updateUserOutput struct {
	ServerID string `json:"ServerId"`
	UserName string `json:"UserName"`
}

//nolint:dupl // handleUpdateUser and handleUpdateAccess are structurally similar but serve different entity types
func (h *Handler) handleUpdateUser(
	_ context.Context,
	in *updateUserInput,
) (*updateUserOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	u, err := h.Backend.UpdateUserFull(&UpdateUserInput{
		ServerID:                 in.ServerID,
		UserName:                 in.UserName,
		HomeDir:                  in.HomeDir,
		Role:                     in.Role,
		HomeDirectoryType:        in.HomeDirectoryType,
		SetHomeDirectoryType:     in.HomeDirectoryType != "",
		Policy:                   in.Policy,
		SetPolicy:                in.Policy != "",
		PosixProfile:             toPosixProfile(in.PosixProfile),
		SetPosixProfile:          in.PosixProfile != nil,
		HomeDirectoryMappings:    toHomeDirectoryMappings(in.HomeDirectoryMappings),
		SetHomeDirectoryMappings: in.HomeDirectoryMappings != nil,
	})
	if err != nil {
		return nil, err
	}

	return &updateUserOutput{ServerID: u.ServerID, UserName: u.UserName}, nil
}

func toUserView(u *User, arnStr string, sshKeys []sshKeyView) userView {
	v := userView{
		Arn:               arnStr,
		UserName:          u.UserName,
		HomeDir:           u.HomeDir,
		Role:              u.Role,
		Tags:              tagsToList(u.Tags),
		HomeDirectoryType: u.HomeDirectoryType,
		Policy:            u.Policy,
		SSHPublicKeys:     sshKeys,
	}

	if u.PosixProfile != nil {
		v.PosixProfile = &posixProfileView{
			UID:           u.PosixProfile.UID,
			GID:           u.PosixProfile.GID,
			SecondaryGids: u.PosixProfile.SecondaryGids,
		}
	}

	if u.HomeDirectoryMappings != nil {
		v.HomeDirectoryMappings = make([]homeDirectoryMapEntryView, len(u.HomeDirectoryMappings))
		for i, m := range u.HomeDirectoryMappings {
			v.HomeDirectoryMappings[i] = homeDirectoryMapEntryView(m)
		}
	}

	return v
}
