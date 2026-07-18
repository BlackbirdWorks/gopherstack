package datasync

import (
	"context"
	"fmt"
)

// --- FSx Windows location ---

type createLocationFsxWindowsInput struct {
	FsxFilesystemArn  string     `json:"FsxFilesystemArn"`
	Subdirectory      string     `json:"Subdirectory,omitempty"`
	Domain            string     `json:"Domain,omitempty"`
	User              string     `json:"User"`
	Password          string     `json:"Password"`
	SecurityGroupArns []string   `json:"SecurityGroupArns"`
	Tags              []tagInput `json:"Tags"`
}

type createLocationFsxWindowsOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationFsxWindows(
	_ context.Context,
	in *createLocationFsxWindowsInput,
) (*createLocationFsxWindowsOutput, error) {
	if in.FsxFilesystemArn == "" {
		return nil, fmt.Errorf("%w: FsxFilesystemArn is required", errInvalidRequest)
	}

	if in.User == "" {
		return nil, fmt.Errorf("%w: User is required", errInvalidRequest)
	}

	if len(in.SecurityGroupArns) == 0 {
		return nil, fmt.Errorf("%w: SecurityGroupArns is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	l, err := h.Backend.CreateLocationFsxWindows(
		in.FsxFilesystemArn, in.Subdirectory, in.Domain, in.User, in.Password,
		in.SecurityGroupArns, tags,
	)
	if err != nil {
		return nil, err
	}

	return &createLocationFsxWindowsOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationFsxWindowsInput struct {
	LocationArn string `json:"LocationArn"`
}

type describeLocationFsxWindowsOutput struct {
	LocationArn       string   `json:"LocationArn"`
	LocationURI       string   `json:"LocationUri"`
	FsxFilesystemArn  string   `json:"FsxFilesystemArn,omitempty"`
	Subdirectory      string   `json:"Subdirectory,omitempty"`
	Domain            string   `json:"Domain,omitempty"`
	User              string   `json:"User,omitempty"`
	SecurityGroupArns []string `json:"SecurityGroupArns,omitempty"`
	CreationTime      int64    `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationFsxWindows(
	_ context.Context,
	in *describeLocationFsxWindowsInput,
) (*describeLocationFsxWindowsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationFsxWindows(in.LocationArn)
	if err != nil {
		return nil, err
	}

	return &describeLocationFsxWindowsOutput{
		LocationArn:       l.LocationArn,
		LocationURI:       l.LocationURI,
		FsxFilesystemArn:  l.FsxFilesystemArn,
		Subdirectory:      l.Subdirectory,
		Domain:            l.Domain,
		User:              l.User,
		SecurityGroupArns: l.SecurityGroupArns,
		CreationTime:      l.CreationTime.Unix(),
	}, nil
}

type updateLocationFsxWindowsInput struct {
	LocationArn  string `json:"LocationArn"`
	Subdirectory string `json:"Subdirectory,omitempty"`
	Domain       string `json:"Domain,omitempty"`
	User         string `json:"User,omitempty"`
	Password     string `json:"Password,omitempty"`
}

type updateLocationFsxWindowsOutput struct{}

func (h *Handler) handleUpdateLocationFsxWindows(
	_ context.Context,
	in *updateLocationFsxWindowsInput,
) (*updateLocationFsxWindowsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateLocationFsxWindows(
		in.LocationArn, in.Subdirectory, in.Domain, in.User, in.Password,
	); err != nil {
		return nil, err
	}

	return &updateLocationFsxWindowsOutput{}, nil
}
