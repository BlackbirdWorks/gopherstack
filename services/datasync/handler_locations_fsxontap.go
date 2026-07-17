package datasync

import (
	"context"
	"fmt"
)

// --- FSx ONTAP location ---

type fsxMountOptionsInput struct {
	Version string `json:"Version,omitempty"`
}

type fsxNfsProtocolInput struct {
	MountOptions *fsxMountOptionsInput `json:"MountOptions"`
}

type fsxSmbProtocolInput struct {
	MountOptions *fsxMountOptionsInput `json:"MountOptions"`
	Domain       string                `json:"Domain,omitempty"`
	Password     string                `json:"Password,omitempty"`
	User         string                `json:"User,omitempty"`
}

type fsxProtocolInput struct {
	NFS *fsxNfsProtocolInput `json:"NFS"`
	SMB *fsxSmbProtocolInput `json:"SMB"`
}

func fsxProtocolFromInput(p *fsxProtocolInput) *FsxProtocol {
	if p == nil {
		return nil
	}

	out := &FsxProtocol{}

	if p.NFS != nil {
		out.NFS = &FsxNfsProtocol{}
		if p.NFS.MountOptions != nil {
			out.NFS.MountOptions = &MountOptions{Version: p.NFS.MountOptions.Version}
		}
	}

	if p.SMB != nil {
		out.SMB = &FsxSmbProtocol{
			Domain:   p.SMB.Domain,
			Password: p.SMB.Password,
			User:     p.SMB.User,
		}
		if p.SMB.MountOptions != nil {
			out.SMB.MountOptions = &MountOptions{Version: p.SMB.MountOptions.Version}
		}
	}

	return out
}

type fsxMountOptionsOutput struct {
	Version string `json:"Version,omitempty"`
}

type fsxNfsProtocolOutput struct {
	MountOptions *fsxMountOptionsOutput `json:"MountOptions,omitempty"`
}

type fsxSmbProtocolOutput struct {
	MountOptions *fsxMountOptionsOutput `json:"MountOptions,omitempty"`
	Domain       string                 `json:"Domain,omitempty"`
	User         string                 `json:"User,omitempty"`
}

type fsxProtocolOutput struct {
	NFS *fsxNfsProtocolOutput `json:"NFS,omitempty"`
	SMB *fsxSmbProtocolOutput `json:"SMB,omitempty"`
}

func fsxProtocolToOutput(p *FsxProtocol) *fsxProtocolOutput {
	if p == nil {
		return nil
	}

	out := &fsxProtocolOutput{}

	if p.NFS != nil {
		out.NFS = &fsxNfsProtocolOutput{}
		if p.NFS.MountOptions != nil {
			out.NFS.MountOptions = &fsxMountOptionsOutput{Version: p.NFS.MountOptions.Version}
		}
	}

	if p.SMB != nil {
		out.SMB = &fsxSmbProtocolOutput{
			Domain: p.SMB.Domain,
			User:   p.SMB.User,
		}
		if p.SMB.MountOptions != nil {
			out.SMB.MountOptions = &fsxMountOptionsOutput{Version: p.SMB.MountOptions.Version}
		}
	}

	return out
}

type createLocationFsxOntapInput struct {
	Protocol                 *fsxProtocolInput `json:"Protocol"`
	StorageVirtualMachineArn string            `json:"StorageVirtualMachineArn"`
	Subdirectory             string            `json:"Subdirectory,omitempty"`
	SecurityGroupArns        []string          `json:"SecurityGroupArns"`
	Tags                     []tagInput        `json:"Tags"`
}

type createLocationFsxOntapOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationFsxOntap(
	_ context.Context,
	in *createLocationFsxOntapInput,
) (*createLocationFsxOntapOutput, error) {
	if in.StorageVirtualMachineArn == "" {
		return nil, fmt.Errorf("%w: StorageVirtualMachineArn is required", errInvalidRequest)
	}

	if in.Protocol == nil {
		return nil, fmt.Errorf("%w: Protocol is required", errInvalidRequest)
	}

	if len(in.SecurityGroupArns) == 0 {
		return nil, fmt.Errorf("%w: SecurityGroupArns is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	l, err := h.Backend.CreateLocationFsxOntap(
		in.StorageVirtualMachineArn, in.Subdirectory,
		fsxProtocolFromInput(in.Protocol), in.SecurityGroupArns, tags,
	)
	if err != nil {
		return nil, err
	}

	return &createLocationFsxOntapOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationFsxOntapInput struct {
	LocationArn string `json:"LocationArn"`
}

type describeLocationFsxOntapOutput struct {
	Protocol                 *fsxProtocolOutput `json:"Protocol,omitempty"`
	LocationArn              string             `json:"LocationArn"`
	LocationURI              string             `json:"LocationUri"`
	StorageVirtualMachineArn string             `json:"StorageVirtualMachineArn,omitempty"`
	Subdirectory             string             `json:"Subdirectory,omitempty"`
	SecurityGroupArns        []string           `json:"SecurityGroupArns,omitempty"`
	CreationTime             int64              `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationFsxOntap(
	_ context.Context,
	in *describeLocationFsxOntapInput,
) (*describeLocationFsxOntapOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationFsxOntap(in.LocationArn)
	if err != nil {
		return nil, err
	}

	return &describeLocationFsxOntapOutput{
		LocationArn:              l.LocationArn,
		LocationURI:              l.LocationURI,
		StorageVirtualMachineArn: l.StorageVirtualMachineArn,
		Subdirectory:             l.Subdirectory,
		SecurityGroupArns:        l.SecurityGroupArns,
		Protocol:                 fsxProtocolToOutput(l.Protocol),
		CreationTime:             l.CreationTime.Unix(),
	}, nil
}

type updateLocationFsxOntapInput struct {
	Protocol     *fsxProtocolInput `json:"Protocol"`
	LocationArn  string            `json:"LocationArn"`
	Subdirectory string            `json:"Subdirectory,omitempty"`
}

type updateLocationFsxOntapOutput struct{}

func (h *Handler) handleUpdateLocationFsxOntap(
	_ context.Context,
	in *updateLocationFsxOntapInput,
) (*updateLocationFsxOntapOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateLocationFsxOntap(
		in.LocationArn, in.Subdirectory, fsxProtocolFromInput(in.Protocol),
	); err != nil {
		return nil, err
	}

	return &updateLocationFsxOntapOutput{}, nil
}
