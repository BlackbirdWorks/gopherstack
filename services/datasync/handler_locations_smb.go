package datasync

import (
	"context"
	"fmt"
)

// --- SMB location ---

type createLocationSmbInput struct {
	MountOptions   *mountOptionsInput `json:"MountOptions"`
	ServerHostname string             `json:"ServerHostname"`
	Subdirectory   string             `json:"Subdirectory,omitempty"`
	Domain         string             `json:"Domain,omitempty"`
	User           string             `json:"User"`
	Password       string             `json:"Password"`
	AgentArns      []string           `json:"AgentArns"`
	Tags           []tagInput         `json:"Tags"`
}

type createLocationSmbOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationSmb(
	_ context.Context,
	in *createLocationSmbInput,
) (*createLocationSmbOutput, error) {
	if in.ServerHostname == "" {
		return nil, fmt.Errorf("%w: ServerHostname is required", errInvalidRequest)
	}

	if in.Subdirectory == "" {
		return nil, fmt.Errorf("%w: Subdirectory is required", errInvalidRequest)
	}

	if len(in.AgentArns) == 0 {
		return nil, fmt.Errorf("%w: AgentArns is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	var mo *MountOptions
	if in.MountOptions != nil {
		mo = &MountOptions{Version: in.MountOptions.Version}
	}

	l, err := h.Backend.CreateLocationSmb(
		in.ServerHostname, in.Subdirectory, in.Domain, in.User, in.Password,
		mo, in.AgentArns, tags,
	)
	if err != nil {
		return nil, err
	}

	return &createLocationSmbOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationSmbInput struct {
	LocationArn string `json:"LocationArn"`
}

type describeLocationSmbOutput struct {
	MountOptions   *mountOptionsOutput `json:"MountOptions,omitempty"`
	LocationArn    string              `json:"LocationArn"`
	LocationURI    string              `json:"LocationUri"`
	ServerHostname string              `json:"ServerHostname,omitempty"`
	Subdirectory   string              `json:"Subdirectory,omitempty"`
	Domain         string              `json:"Domain,omitempty"`
	User           string              `json:"User,omitempty"`
	AgentArns      []string            `json:"AgentArns,omitempty"`
	CreationTime   int64               `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationSmb(
	_ context.Context,
	in *describeLocationSmbInput,
) (*describeLocationSmbOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationSmb(in.LocationArn)
	if err != nil {
		return nil, err
	}

	out := &describeLocationSmbOutput{
		LocationArn:    l.LocationArn,
		LocationURI:    l.LocationURI,
		ServerHostname: l.ServerHostname,
		Subdirectory:   l.Subdirectory,
		Domain:         l.Domain,
		User:           l.User,
		AgentArns:      l.AgentArns,
		CreationTime:   l.CreationTime.Unix(),
	}

	if l.MountOptions != nil {
		out.MountOptions = &mountOptionsOutput{Version: l.MountOptions.Version}
	}

	return out, nil
}

type updateLocationSmbInput struct {
	MountOptions *mountOptionsInput `json:"MountOptions"`
	LocationArn  string             `json:"LocationArn"`
	Subdirectory string             `json:"Subdirectory,omitempty"`
	Domain       string             `json:"Domain,omitempty"`
	User         string             `json:"User,omitempty"`
	Password     string             `json:"Password,omitempty"`
	AgentArns    []string           `json:"AgentArns"`
}

type updateLocationSmbOutput struct{}

func (h *Handler) handleUpdateLocationSmb(
	_ context.Context,
	in *updateLocationSmbInput,
) (*updateLocationSmbOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	var mo *MountOptions
	if in.MountOptions != nil {
		mo = &MountOptions{Version: in.MountOptions.Version}
	}

	if err := h.Backend.UpdateLocationSmb(
		in.LocationArn, in.Subdirectory, in.Domain, in.User, in.Password,
		mo, in.AgentArns,
	); err != nil {
		return nil, err
	}

	return &updateLocationSmbOutput{}, nil
}
