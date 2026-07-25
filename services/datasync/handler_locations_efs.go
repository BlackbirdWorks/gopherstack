package datasync

import (
	"context"
	"fmt"
)

// --- EFS location ---

type ec2ConfigInput struct {
	SubnetArn         string   `json:"SubnetArn"`
	SecurityGroupArns []string `json:"SecurityGroupArns"`
}

type createLocationEfsInput struct {
	Ec2Config               *ec2ConfigInput `json:"Ec2Config"`
	EfsFilesystemArn        string          `json:"EfsFilesystemArn"`
	Subdirectory            string          `json:"Subdirectory,omitempty"`
	AccessPointArn          string          `json:"AccessPointArn,omitempty"`
	FileSystemAccessRoleArn string          `json:"FileSystemAccessRoleArn,omitempty"`
	InTransitEncryption     string          `json:"InTransitEncryption,omitempty"`
	Tags                    []tagInput      `json:"Tags"`
}

type createLocationEfsOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationEfs(
	_ context.Context,
	in *createLocationEfsInput,
) (*createLocationEfsOutput, error) {
	if in.EfsFilesystemArn == "" {
		return nil, fmt.Errorf("%w: EfsFilesystemArn is required", errInvalidRequest)
	}

	if in.Ec2Config == nil {
		return nil, fmt.Errorf("%w: Ec2Config is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	var ec2Cfg *Ec2Config
	if in.Ec2Config != nil {
		ec2Cfg = &Ec2Config{
			SubnetArn:         in.Ec2Config.SubnetArn,
			SecurityGroupArns: in.Ec2Config.SecurityGroupArns,
		}
	}

	l, err := h.Backend.CreateLocationEfs(
		in.EfsFilesystemArn, in.Subdirectory,
		in.AccessPointArn, in.FileSystemAccessRoleArn, in.InTransitEncryption,
		ec2Cfg, tags,
	)
	if err != nil {
		return nil, err
	}

	return &createLocationEfsOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationEfsInput struct {
	LocationArn string `json:"LocationArn"`
}

type ec2ConfigOutput struct {
	SubnetArn         string   `json:"SubnetArn"`
	SecurityGroupArns []string `json:"SecurityGroupArns,omitempty"`
}

// describeLocationEfsOutput intentionally has no EfsFilesystemArn or
// Subdirectory field: the real DescribeLocationEfsOutput has neither -- the
// file system and path are folded into LocationUri only (confirmed against
// aws-sdk-go-v2 v1.59.2: AccessPointArn, CreationTime, Ec2Config,
// FileSystemAccessRoleArn, InTransitEncryption, LocationArn, LocationUri).
type describeLocationEfsOutput struct {
	Ec2Config               *ec2ConfigOutput `json:"Ec2Config,omitempty"`
	LocationArn             string           `json:"LocationArn"`
	LocationURI             string           `json:"LocationUri"`
	AccessPointArn          string           `json:"AccessPointArn,omitempty"`
	FileSystemAccessRoleArn string           `json:"FileSystemAccessRoleArn,omitempty"`
	InTransitEncryption     string           `json:"InTransitEncryption,omitempty"`
	CreationTime            int64            `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationEfs(
	_ context.Context,
	in *describeLocationEfsInput,
) (*describeLocationEfsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationEfs(in.LocationArn)
	if err != nil {
		return nil, err
	}

	out := &describeLocationEfsOutput{
		LocationArn:             l.LocationArn,
		LocationURI:             l.LocationURI,
		AccessPointArn:          l.AccessPointArn,
		FileSystemAccessRoleArn: l.FileSystemAccessRoleArn,
		InTransitEncryption:     l.InTransitEncryption,
		CreationTime:            l.CreationTime.Unix(),
	}

	if l.Ec2Config != nil {
		out.Ec2Config = &ec2ConfigOutput{
			SubnetArn:         l.Ec2Config.SubnetArn,
			SecurityGroupArns: l.Ec2Config.SecurityGroupArns,
		}
	}

	return out, nil
}

type updateLocationEfsInput struct {
	Ec2Config               *ec2ConfigInput `json:"Ec2Config"`
	LocationArn             string          `json:"LocationArn"`
	Subdirectory            string          `json:"Subdirectory,omitempty"`
	AccessPointArn          string          `json:"AccessPointArn,omitempty"`
	FileSystemAccessRoleArn string          `json:"FileSystemAccessRoleArn,omitempty"`
	InTransitEncryption     string          `json:"InTransitEncryption,omitempty"`
}

type updateLocationEfsOutput struct{}

func (h *Handler) handleUpdateLocationEfs(
	_ context.Context,
	in *updateLocationEfsInput,
) (*updateLocationEfsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	var ec2Cfg *Ec2Config
	if in.Ec2Config != nil {
		ec2Cfg = &Ec2Config{
			SubnetArn:         in.Ec2Config.SubnetArn,
			SecurityGroupArns: in.Ec2Config.SecurityGroupArns,
		}
	}

	if err := h.Backend.UpdateLocationEfs(
		in.LocationArn, in.Subdirectory,
		in.AccessPointArn, in.FileSystemAccessRoleArn, in.InTransitEncryption,
		ec2Cfg,
	); err != nil {
		return nil, err
	}

	return &updateLocationEfsOutput{}, nil
}
