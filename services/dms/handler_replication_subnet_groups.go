package dms

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// replicationSubnetGroupJSON is a minimal representation of a DMS replication
// subnet group. The Terraform AWS provider accesses ReplicationSubnetGroup.ReplicationSubnetGroupIdentifier
// unconditionally in its Read function, so we must always return a non-null object.
type replicationSubnetGroupJSON struct {
	ReplicationSubnetGroupIdentifier *string `json:"ReplicationSubnetGroupIdentifier"`
}

type createReplicationSubnetGroupInput struct {
	ReplicationSubnetGroupIdentifier  *string    `json:"ReplicationSubnetGroupIdentifier"`
	ReplicationSubnetGroupDescription *string    `json:"ReplicationSubnetGroupDescription"`
	SubnetIDs                         []string   `json:"SubnetIds"`
	Tags                              []tagEntry `json:"Tags"`
}

// replicationSubnetGroupFullJSON is the wire shape of types.ReplicationSubnetGroup.
// The real type has NO Arn field at all -- DMS subnet groups are referenced by
// identifier on the wire; a client that wants to tag one (AddTagsToResource
// requires a ResourceArn) must build the ARN itself from the deterministic
// arn:aws:dms:<region>:<account>:subgrp:<identifier> format. Don't add an Arn
// field back here without re-verifying against the SDK.
type replicationSubnetGroupFullJSON struct {
	ReplicationSubnetGroupIdentifier  string `json:"ReplicationSubnetGroupIdentifier"`
	ReplicationSubnetGroupDescription string `json:"ReplicationSubnetGroupDescription"`
	VpcID                             string `json:"VpcId"`
}

type createReplicationSubnetGroupOutput struct {
	ReplicationSubnetGroup replicationSubnetGroupFullJSON `json:"ReplicationSubnetGroup"`
}

func rsgToJSON(sg *ReplicationSubnetGroup) replicationSubnetGroupFullJSON {
	return replicationSubnetGroupFullJSON{
		ReplicationSubnetGroupIdentifier:  sg.ReplicationSubnetGroupIdentifier,
		ReplicationSubnetGroupDescription: sg.ReplicationSubnetGroupDescription,
		VpcID:                             sg.VpcID,
	}
}

func (h *Handler) handleCreateReplicationSubnetGroup(
	ctx context.Context, in *createReplicationSubnetGroupInput,
) (*createReplicationSubnetGroupOutput, error) {
	identifier := ptrconv.String(in.ReplicationSubnetGroupIdentifier)
	if identifier == "" {
		return nil, fmt.Errorf("%w: ReplicationSubnetGroupIdentifier is required", ErrValidation)
	}

	description := ptrconv.String(in.ReplicationSubnetGroupDescription)
	if description == "" {
		return nil, fmt.Errorf("%w: ReplicationSubnetGroupDescription is required", ErrValidation)
	}

	if len(in.SubnetIDs) == 0 {
		return nil, fmt.Errorf("%w: SubnetIds is required", ErrValidation)
	}

	kv := tagsToMap(in.Tags)
	sg, err := h.Backend.CreateReplicationSubnetGroup(
		ctx,
		identifier,
		ptrconv.String(in.ReplicationSubnetGroupDescription),
		"",
		kv,
	)
	if err != nil {
		return nil, err
	}

	return &createReplicationSubnetGroupOutput{ReplicationSubnetGroup: rsgToJSON(sg)}, nil
}

type deleteReplicationSubnetGroupInput struct {
	ReplicationSubnetGroupIdentifier *string `json:"ReplicationSubnetGroupIdentifier"`
}

type deleteReplicationSubnetGroupOutput struct{}

func (h *Handler) handleDeleteReplicationSubnetGroup(
	ctx context.Context, in *deleteReplicationSubnetGroupInput,
) (*deleteReplicationSubnetGroupOutput, error) {
	id := ptrconv.String(in.ReplicationSubnetGroupIdentifier)
	if err := h.Backend.DeleteReplicationSubnetGroup(ctx, id); err != nil {
		return nil, err
	}

	return &deleteReplicationSubnetGroupOutput{}, nil
}

type describeReplicationSubnetGroupsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeReplicationSubnetGroupsOutput struct {
	Marker                  *string                          `json:"Marker,omitempty"`
	ReplicationSubnetGroups []replicationSubnetGroupFullJSON `json:"ReplicationSubnetGroups"`
}

func (h *Handler) handleDescribeReplicationSubnetGroups(
	ctx context.Context, in *describeReplicationSubnetGroupsInput,
) (*describeReplicationSubnetGroupsOutput, error) {
	list, err := h.Backend.DescribeReplicationSubnetGroups(ctx)
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].ReplicationSubnetGroupIdentifier < list[j].ReplicationSubnetGroupIdentifier
	})

	idFilter := extractFilterValue(in.Filters, "replication-subnet-group-id")

	all := make([]replicationSubnetGroupFullJSON, 0, len(list))
	for _, sg := range list {
		if idFilter != "" && sg.ReplicationSubnetGroupIdentifier != idFilter {
			continue
		}

		all = append(all, rsgToJSON(sg))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeReplicationSubnetGroupsOutput{
		ReplicationSubnetGroups: data,
		Marker:                  nextMarker,
	}, nil
}

type modifyReplicationSubnetGroupInput struct {
	ReplicationSubnetGroupIdentifier  *string  `json:"ReplicationSubnetGroupIdentifier"`
	ReplicationSubnetGroupDescription *string  `json:"ReplicationSubnetGroupDescription"`
	SubnetIDs                         []string `json:"SubnetIds"`
}

type modifyReplicationSubnetGroupOutput struct {
	ReplicationSubnetGroup replicationSubnetGroupFullJSON `json:"ReplicationSubnetGroup"`
}

func (h *Handler) handleModifyReplicationSubnetGroup(
	ctx context.Context, in *modifyReplicationSubnetGroupInput,
) (*modifyReplicationSubnetGroupOutput, error) {
	identifier := ptrconv.String(in.ReplicationSubnetGroupIdentifier)
	if identifier == "" {
		return nil, fmt.Errorf("%w: ReplicationSubnetGroupIdentifier is required", ErrValidation)
	}

	if len(in.SubnetIDs) == 0 {
		return nil, fmt.Errorf("%w: SubnetIds is required", ErrValidation)
	}

	sg, err := h.Backend.ModifyReplicationSubnetGroup(
		ctx,
		identifier,
		ptrconv.String(in.ReplicationSubnetGroupDescription),
	)
	if err != nil {
		return nil, err
	}

	return &modifyReplicationSubnetGroupOutput{ReplicationSubnetGroup: rsgToJSON(sg)}, nil
}

// opsReplicationSubnetGroups returns the dispatch-table entries for the replication_subnet_groups operation family.
func (h *Handler) opsReplicationSubnetGroups() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateReplicationSubnetGroup: service.WrapOp(
			h.handleCreateReplicationSubnetGroup,
		),
		opDeleteReplicationSubnetGroup: service.WrapOp(
			h.handleDeleteReplicationSubnetGroup,
		),
		opDescribeReplicationSubnetGroups: service.WrapOp(
			h.handleDescribeReplicationSubnetGroups,
		),
		opModifyReplicationSubnetGroup: service.WrapOp(
			h.handleModifyReplicationSubnetGroup,
		),
	}
}
