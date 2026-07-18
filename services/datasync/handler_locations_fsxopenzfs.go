package datasync

import (
	"context"
	"fmt"
)

// --- FSx OpenZFS location ---

type createLocationFsxOpenZfsInput struct {
	Protocol          *fsxProtocolInput `json:"Protocol"`
	FsxFilesystemArn  string            `json:"FsxFilesystemArn"`
	Subdirectory      string            `json:"Subdirectory,omitempty"`
	SecurityGroupArns []string          `json:"SecurityGroupArns"`
	Tags              []tagInput        `json:"Tags"`
}

type createLocationFsxOpenZfsOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationFsxOpenZfs(
	_ context.Context,
	in *createLocationFsxOpenZfsInput,
) (*createLocationFsxOpenZfsOutput, error) {
	if in.FsxFilesystemArn == "" {
		return nil, fmt.Errorf("%w: FsxFilesystemArn is required", errInvalidRequest)
	}

	if in.Protocol == nil {
		return nil, fmt.Errorf("%w: Protocol is required", errInvalidRequest)
	}

	if len(in.SecurityGroupArns) == 0 {
		return nil, fmt.Errorf("%w: SecurityGroupArns is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	l, err := h.Backend.CreateLocationFsxOpenZfs(
		in.FsxFilesystemArn, in.Subdirectory,
		fsxProtocolFromInput(in.Protocol), in.SecurityGroupArns, tags,
	)
	if err != nil {
		return nil, err
	}

	return &createLocationFsxOpenZfsOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationFsxOpenZfsInput struct {
	LocationArn string `json:"LocationArn"`
}

type describeLocationFsxOpenZfsOutput struct {
	Protocol          *fsxProtocolOutput `json:"Protocol,omitempty"`
	LocationArn       string             `json:"LocationArn"`
	LocationURI       string             `json:"LocationUri"`
	FsxFilesystemArn  string             `json:"FsxFilesystemArn,omitempty"`
	Subdirectory      string             `json:"Subdirectory,omitempty"`
	SecurityGroupArns []string           `json:"SecurityGroupArns,omitempty"`
	CreationTime      int64              `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationFsxOpenZfs(
	_ context.Context,
	in *describeLocationFsxOpenZfsInput,
) (*describeLocationFsxOpenZfsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationFsxOpenZfs(in.LocationArn)
	if err != nil {
		return nil, err
	}

	return &describeLocationFsxOpenZfsOutput{
		LocationArn:       l.LocationArn,
		LocationURI:       l.LocationURI,
		FsxFilesystemArn:  l.FsxFilesystemArn,
		Subdirectory:      l.Subdirectory,
		SecurityGroupArns: l.SecurityGroupArns,
		Protocol:          fsxProtocolToOutput(l.Protocol),
		CreationTime:      l.CreationTime.Unix(),
	}, nil
}

type updateLocationFsxOpenZfsInput struct {
	Protocol     *fsxProtocolInput `json:"Protocol"`
	LocationArn  string            `json:"LocationArn"`
	Subdirectory string            `json:"Subdirectory,omitempty"`
}

type updateLocationFsxOpenZfsOutput struct{}

func (h *Handler) handleUpdateLocationFsxOpenZfs(
	_ context.Context,
	in *updateLocationFsxOpenZfsInput,
) (*updateLocationFsxOpenZfsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateLocationFsxOpenZfs(
		in.LocationArn, in.Subdirectory, fsxProtocolFromInput(in.Protocol),
	); err != nil {
		return nil, err
	}

	return &updateLocationFsxOpenZfsOutput{}, nil
}
