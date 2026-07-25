package datasync

import (
	"context"
	"fmt"
)

// --- FSx Lustre location ---

type createLocationFsxLustreInput struct {
	FsxFilesystemArn  string     `json:"FsxFilesystemArn"`
	Subdirectory      string     `json:"Subdirectory,omitempty"`
	SecurityGroupArns []string   `json:"SecurityGroupArns"`
	Tags              []tagInput `json:"Tags"`
}

type createLocationFsxLustreOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationFsxLustre(
	_ context.Context,
	in *createLocationFsxLustreInput,
) (*createLocationFsxLustreOutput, error) {
	if in.FsxFilesystemArn == "" {
		return nil, fmt.Errorf("%w: FsxFilesystemArn is required", errInvalidRequest)
	}

	if len(in.SecurityGroupArns) == 0 {
		return nil, fmt.Errorf("%w: SecurityGroupArns is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	l, err := h.Backend.CreateLocationFsxLustre(in.FsxFilesystemArn, in.Subdirectory, in.SecurityGroupArns, tags)
	if err != nil {
		return nil, err
	}

	return &createLocationFsxLustreOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationFsxLustreInput struct {
	LocationArn string `json:"LocationArn"`
}

// describeLocationFsxLustreOutput intentionally has no FsxFilesystemArn or
// Subdirectory field: the real DescribeLocationFsxLustreOutput has neither --
// confirmed against aws-sdk-go-v2 v1.59.2: CreationTime, LocationArn,
// LocationUri, SecurityGroupArns only.
type describeLocationFsxLustreOutput struct {
	LocationArn       string   `json:"LocationArn"`
	LocationURI       string   `json:"LocationUri"`
	SecurityGroupArns []string `json:"SecurityGroupArns,omitempty"`
	CreationTime      int64    `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationFsxLustre(
	_ context.Context,
	in *describeLocationFsxLustreInput,
) (*describeLocationFsxLustreOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationFsxLustre(in.LocationArn)
	if err != nil {
		return nil, err
	}

	return &describeLocationFsxLustreOutput{
		LocationArn:       l.LocationArn,
		LocationURI:       l.LocationURI,
		SecurityGroupArns: l.SecurityGroupArns,
		CreationTime:      l.CreationTime.Unix(),
	}, nil
}

type updateLocationFsxLustreInput struct {
	LocationArn  string `json:"LocationArn"`
	Subdirectory string `json:"Subdirectory,omitempty"`
}

type updateLocationFsxLustreOutput struct{}

func (h *Handler) handleUpdateLocationFsxLustre(
	_ context.Context,
	in *updateLocationFsxLustreInput,
) (*updateLocationFsxLustreOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateLocationFsxLustre(in.LocationArn, in.Subdirectory); err != nil {
		return nil, err
	}

	return &updateLocationFsxLustreOutput{}, nil
}
