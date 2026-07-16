package transfer

import (
	"context"
	"fmt"
)

type createAccessInput struct {
	PosixProfile          *posixProfileInput           `json:"PosixProfile,omitempty"`
	ServerID              string                       `json:"ServerId"`
	ExternalID            string                       `json:"ExternalId"`
	Role                  string                       `json:"Role"`
	HomeDir               string                       `json:"HomeDirectory"`
	HomeDirectoryType     string                       `json:"HomeDirectoryType,omitempty"`
	Policy                string                       `json:"Policy,omitempty"`
	HomeDirectoryMappings []homeDirectoryMapEntryInput `json:"HomeDirectoryMappings,omitempty"`
	Tags                  []map[string]string          `json:"Tags"`
}

type createAccessOutput struct {
	ServerID   string `json:"ServerId"`
	ExternalID string `json:"ExternalId"`
}

func (h *Handler) handleCreateAccess(
	_ context.Context,
	in *createAccessInput,
) (*createAccessOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.ExternalID == "" {
		return nil, fmt.Errorf("%w: ExternalId is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)

	a, err := h.Backend.CreateAccessFull(&CreateAccessInput{
		ServerID:              in.ServerID,
		ExternalID:            in.ExternalID,
		Role:                  in.Role,
		HomeDir:               in.HomeDir,
		HomeDirectoryType:     in.HomeDirectoryType,
		HomeDirectoryMappings: toHomeDirectoryMappings(in.HomeDirectoryMappings),
		Policy:                in.Policy,
		PosixProfile:          toPosixProfile(in.PosixProfile),
		Tags:                  tags,
	})
	if err != nil {
		return nil, err
	}

	return &createAccessOutput{ServerID: a.ServerID, ExternalID: a.ExternalID}, nil
}

type deleteAccessInput struct {
	ServerID   string `json:"ServerId"`
	ExternalID string `json:"ExternalId"`
}

func (h *Handler) handleDeleteAccess(_ context.Context, in *deleteAccessInput) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.ExternalID == "" {
		return nil, fmt.Errorf("%w: ExternalId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteAccess(in.ServerID, in.ExternalID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeAccessInput struct {
	ServerID   string `json:"ServerId"`
	ExternalID string `json:"ExternalId"`
}

type describeAccessOutput struct {
	Access   map[string]any `json:"Access"`
	ServerID string         `json:"ServerId"`
}

func (h *Handler) handleDescribeAccess(
	_ context.Context,
	in *describeAccessInput,
) (*describeAccessOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.ExternalID == "" {
		return nil, fmt.Errorf("%w: ExternalId is required", errInvalidRequest)
	}

	a, err := h.Backend.DescribeAccess(in.ServerID, in.ExternalID)
	if err != nil {
		return nil, err
	}

	accessMap := map[string]any{
		"ExternalId":        a.ExternalID,
		"ServerId":          a.ServerID,
		keyRole:             a.Role,
		"HomeDirectory":     a.HomeDir,
		"HomeDirectoryType": a.HomeDirectoryType,
		"Policy":            a.Policy,
	}

	if a.PosixProfile != nil {
		accessMap["PosixProfile"] = map[string]any{
			"Uid":           a.PosixProfile.UID,
			"Gid":           a.PosixProfile.GID,
			"SecondaryGids": a.PosixProfile.SecondaryGids,
		}
	}

	if a.HomeDirectoryMappings != nil {
		mappings := make([]map[string]any, len(a.HomeDirectoryMappings))
		for i, m := range a.HomeDirectoryMappings {
			mappings[i] = map[string]any{"Entry": m.Entry, "Target": m.Target, keyStepType: m.Type}
		}
		accessMap["HomeDirectoryMappings"] = mappings
	}

	if a.Tags != nil {
		accessMap[keyTags] = tagsToList(a.Tags)
	}

	return &describeAccessOutput{
		ServerID: a.ServerID,
		Access:   accessMap,
	}, nil
}

type listAccessesInput struct {
	ServerID   string `json:"ServerId"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listAccessesOutput struct {
	NextToken string           `json:"NextToken,omitempty"`
	ServerID  string           `json:"ServerId"`
	Accesses  []map[string]any `json:"Accesses"`
}

func (h *Handler) handleListAccesses(
	_ context.Context,
	in *listAccessesInput,
) (*listAccessesOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	items, err := h.Backend.ListAccesses(in.ServerID)
	if err != nil {
		return nil, err
	}

	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, a := range page {
		out[i] = map[string]any{
			"ExternalId":        a.ExternalID,
			"HomeDirectory":     a.HomeDir,
			"HomeDirectoryType": a.HomeDirectoryType,
			keyRole:             a.Role,
		}
	}

	return &listAccessesOutput{Accesses: out, NextToken: next, ServerID: in.ServerID}, nil
}

type updateAccessInput struct {
	PosixProfile          *posixProfileInput           `json:"PosixProfile,omitempty"`
	ServerID              string                       `json:"ServerId"`
	ExternalID            string                       `json:"ExternalId"`
	Role                  string                       `json:"Role"`
	HomeDir               string                       `json:"HomeDirectory"`
	HomeDirectoryType     string                       `json:"HomeDirectoryType,omitempty"`
	Policy                string                       `json:"Policy,omitempty"`
	HomeDirectoryMappings []homeDirectoryMapEntryInput `json:"HomeDirectoryMappings,omitempty"`
}

type updateAccessOutput struct {
	ServerID   string `json:"ServerId"`
	ExternalID string `json:"ExternalId"`
}

//nolint:dupl // handleUpdateAccess and handleUpdateUser are structurally similar but serve different entity types
func (h *Handler) handleUpdateAccess(
	_ context.Context,
	in *updateAccessInput,
) (*updateAccessOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.ExternalID == "" {
		return nil, fmt.Errorf("%w: ExternalId is required", errInvalidRequest)
	}

	a, err := h.Backend.UpdateAccessFull(&UpdateAccessInput{
		ServerID:                 in.ServerID,
		ExternalID:               in.ExternalID,
		Role:                     in.Role,
		HomeDir:                  in.HomeDir,
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

	return &updateAccessOutput{ServerID: a.ServerID, ExternalID: a.ExternalID}, nil
}
