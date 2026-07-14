package dms

import (
	"context"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type createInstanceProfileInput struct {
	InstanceProfileName   *string    `json:"InstanceProfileName"`
	AvailabilityZone      *string    `json:"AvailabilityZone"`
	KmsKeyArn             *string    `json:"KmsKeyArn"`
	NetworkType           *string    `json:"NetworkType"`
	Description           *string    `json:"Description"`
	SubnetGroupIdentifier *string    `json:"SubnetGroupIdentifier"`
	PubliclyAccessible    *bool      `json:"PubliclyAccessible"`
	Tags                  []tagEntry `json:"Tags"`
}

type instanceProfileJSON struct {
	InstanceProfileName   string `json:"InstanceProfileName"`
	InstanceProfileArn    string `json:"InstanceProfileArn"`
	AvailabilityZone      string `json:"AvailabilityZone,omitempty"`
	KmsKeyArn             string `json:"KmsKeyArn,omitempty"`
	NetworkType           string `json:"NetworkType,omitempty"`
	Description           string `json:"Description,omitempty"`
	SubnetGroupIdentifier string `json:"SubnetGroupIdentifier,omitempty"`
	PubliclyAccessible    bool   `json:"PubliclyAccessible"`
}

type createInstanceProfileOutput struct {
	InstanceProfile instanceProfileJSON `json:"InstanceProfile"`
}

func (h *Handler) handleCreateInstanceProfile(
	ctx context.Context, in *createInstanceProfileInput,
) (*createInstanceProfileOutput, error) {
	kv := tagsToMap(in.Tags)
	ip, err := h.Backend.CreateInstanceProfile(
		ctx,
		ptrconv.String(in.InstanceProfileName),
		ptrconv.String(in.AvailabilityZone),
		ptrconv.String(in.KmsKeyArn),
		ptrconv.String(in.NetworkType),
		ptrconv.String(in.Description),
		ptrconv.String(in.SubnetGroupIdentifier),
		ptrconv.Bool(in.PubliclyAccessible),
		kv,
	)
	if err != nil {
		return nil, err
	}

	return &createInstanceProfileOutput{InstanceProfile: ipToJSON(ip)}, nil
}

func ipToJSON(ip *InstanceProfile) instanceProfileJSON {
	return instanceProfileJSON{
		InstanceProfileName:   ip.InstanceProfileName,
		InstanceProfileArn:    ip.InstanceProfileArn,
		AvailabilityZone:      ip.AvailabilityZone,
		KmsKeyArn:             ip.KmsKeyArn,
		NetworkType:           ip.NetworkType,
		Description:           ip.Description,
		SubnetGroupIdentifier: ip.SubnetGroupIdentifier,
		PubliclyAccessible:    ip.PubliclyAccessible,
	}
}

type deleteInstanceProfileInput struct {
	InstanceProfileArn *string `json:"InstanceProfileArn"`
}

type deleteInstanceProfileOutput struct {
	InstanceProfile instanceProfileJSON `json:"InstanceProfile"`
}

func (h *Handler) handleDeleteInstanceProfile(
	ctx context.Context, in *deleteInstanceProfileInput,
) (*deleteInstanceProfileOutput, error) {
	// We need to get the profile before deleting it for the response.
	arnOrName := ptrconv.String(in.InstanceProfileArn)

	profiles, _ := h.Backend.DescribeInstanceProfiles(ctx)
	var found *InstanceProfile
	for _, p := range profiles {
		if p.InstanceProfileArn == arnOrName || p.InstanceProfileName == arnOrName {
			found = p

			break
		}
	}

	if err := h.Backend.DeleteInstanceProfile(ctx, arnOrName); err != nil {
		return nil, err
	}

	if found == nil {
		return &deleteInstanceProfileOutput{}, nil
	}

	return &deleteInstanceProfileOutput{InstanceProfile: ipToJSON(found)}, nil
}

type describeInstanceProfilesInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeInstanceProfilesOutput struct {
	Marker           *string               `json:"Marker,omitempty"`
	InstanceProfiles []instanceProfileJSON `json:"InstanceProfiles"`
}

func (h *Handler) handleDescribeInstanceProfiles(
	ctx context.Context, in *describeInstanceProfilesInput,
) (*describeInstanceProfilesOutput, error) {
	list, err := h.Backend.DescribeInstanceProfiles(ctx)
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].InstanceProfileName < list[j].InstanceProfileName
	})

	all := make([]instanceProfileJSON, 0, len(list))
	for _, ip := range list {
		all = append(all, ipToJSON(ip))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeInstanceProfilesOutput{InstanceProfiles: data, Marker: nextMarker}, nil
}

type modifyInstanceProfileInput struct {
	InstanceProfileArn *string `json:"InstanceProfileArn"`
	AvailabilityZone   *string `json:"AvailabilityZone"`
	Description        *string `json:"Description"`
	NetworkType        *string `json:"NetworkType"`
}

type modifyInstanceProfileOutput struct {
	InstanceProfile instanceProfileJSON `json:"InstanceProfile"`
}

func (h *Handler) handleModifyInstanceProfile(
	ctx context.Context, in *modifyInstanceProfileInput,
) (*modifyInstanceProfileOutput, error) {
	ip, err := h.Backend.ModifyInstanceProfile(
		ctx,
		ptrconv.String(in.InstanceProfileArn),
		ptrconv.String(in.AvailabilityZone),
		ptrconv.String(in.Description),
		ptrconv.String(in.NetworkType),
	)
	if err != nil {
		return nil, err
	}

	return &modifyInstanceProfileOutput{InstanceProfile: ipToJSON(ip)}, nil
}

// opsInstanceProfiles returns the dispatch-table entries for the instance_profiles operation family.
func (h *Handler) opsInstanceProfiles() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateInstanceProfile: service.WrapOp(
			h.handleCreateInstanceProfile,
		),
		opDeleteInstanceProfile: service.WrapOp(
			h.handleDeleteInstanceProfile,
		),
		opDescribeInstanceProfiles: service.WrapOp(
			h.handleDescribeInstanceProfiles,
		),
		opModifyInstanceProfile: service.WrapOp(
			h.handleModifyInstanceProfile,
		),
	}
}
