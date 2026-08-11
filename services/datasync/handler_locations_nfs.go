package datasync

import (
	"context"
	"fmt"
)

// --- NFS location ---

type mountOptionsInput struct {
	Version string `json:"Version,omitempty"`
}

type nfsOnPremConfigInput struct {
	AgentArns []string `json:"AgentArns"`
}

type createLocationNfsInput struct {
	MountOptions   *mountOptionsInput    `json:"MountOptions"`
	OnPremConfig   *nfsOnPremConfigInput `json:"OnPremConfig"`
	ServerHostname string                `json:"ServerHostname"`
	Subdirectory   string                `json:"Subdirectory,omitempty"`
	Tags           []tagInput            `json:"Tags"`
}

type createLocationNfsOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationNfs(
	_ context.Context,
	in *createLocationNfsInput,
) (*createLocationNfsOutput, error) {
	if in.ServerHostname == "" {
		return nil, fmt.Errorf("%w: ServerHostname is required", errInvalidRequest)
	}

	if in.Subdirectory == "" {
		return nil, fmt.Errorf("%w: Subdirectory is required", errInvalidRequest)
	}

	if in.OnPremConfig == nil || len(in.OnPremConfig.AgentArns) == 0 {
		return nil, fmt.Errorf("%w: OnPremConfig.AgentArns is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	var mo *MountOptions
	if in.MountOptions != nil {
		mo = &MountOptions{Version: in.MountOptions.Version}
	}

	var agentArns []string
	if in.OnPremConfig != nil {
		agentArns = in.OnPremConfig.AgentArns
	}

	l, err := h.Backend.CreateLocationNfs(in.ServerHostname, in.Subdirectory, mo, agentArns, tags)
	if err != nil {
		return nil, err
	}

	return &createLocationNfsOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationNfsInput struct {
	LocationArn string `json:"LocationArn"`
}

type mountOptionsOutput struct {
	Version string `json:"Version,omitempty"`
}

type nfsOnPremConfigOutput struct {
	AgentArns []string `json:"AgentArns,omitempty"`
}

// describeLocationNfsOutput intentionally has no ServerHostname or
// Subdirectory field: the real DescribeLocationNfsOutput has neither --
// confirmed against aws-sdk-go-v2 v1.61.4: CreationTime, LocationArn,
// LocationUri, MountOptions, OnPremConfig only (host and path are folded
// into LocationUri).
type describeLocationNfsOutput struct {
	MountOptions *mountOptionsOutput    `json:"MountOptions,omitempty"`
	OnPremConfig *nfsOnPremConfigOutput `json:"OnPremConfig,omitempty"`
	LocationArn  string                 `json:"LocationArn"`
	LocationURI  string                 `json:"LocationUri"`
	CreationTime int64                  `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationNfs(
	_ context.Context,
	in *describeLocationNfsInput,
) (*describeLocationNfsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationNfs(in.LocationArn)
	if err != nil {
		return nil, err
	}

	out := &describeLocationNfsOutput{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		CreationTime: l.CreationTime.Unix(),
	}

	if l.MountOptions != nil {
		out.MountOptions = &mountOptionsOutput{Version: l.MountOptions.Version}
	}

	if len(l.AgentArns) > 0 {
		out.OnPremConfig = &nfsOnPremConfigOutput{AgentArns: l.AgentArns}
	}

	return out, nil
}

type updateLocationNfsInput struct {
	MountOptions *mountOptionsInput    `json:"MountOptions"`
	OnPremConfig *nfsOnPremConfigInput `json:"OnPremConfig"`
	LocationArn  string                `json:"LocationArn"`
	Subdirectory string                `json:"Subdirectory,omitempty"`
}

type updateLocationNfsOutput struct{}

func (h *Handler) handleUpdateLocationNfs(
	_ context.Context,
	in *updateLocationNfsInput,
) (*updateLocationNfsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	var mo *MountOptions
	if in.MountOptions != nil {
		mo = &MountOptions{Version: in.MountOptions.Version}
	}

	var agentArns []string
	if in.OnPremConfig != nil {
		agentArns = in.OnPremConfig.AgentArns
	}

	if err := h.Backend.UpdateLocationNfs(in.LocationArn, in.Subdirectory, mo, agentArns); err != nil {
		return nil, err
	}

	return &updateLocationNfsOutput{}, nil
}
