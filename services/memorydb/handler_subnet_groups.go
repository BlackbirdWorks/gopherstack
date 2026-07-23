package memorydb

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

func (h *Handler) handleCreateSubnetGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req createSubnetGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SubnetGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SubnetGroupName is required")
	}

	if err := validateTagEntries(req.Tags); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	sg, err := h.Backend.CreateSubnetGroup(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createSubnetGroupResponse{SubnetGroup: toSubnetGroupObject(sg)})
}

func (h *Handler) handleDescribeSubnetGroups(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeSubnetGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	sgs, err := h.Backend.DescribeSubnetGroups(ctx, req.SubnetGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	sgs, nextToken := paginateItems(sgs, req.NextToken, req.MaxResults, func(sg *SubnetGroup) string { return sg.Name })

	objs := make([]subnetGroupObject, 0, len(sgs))

	for _, sg := range sgs {
		objs = append(objs, toSubnetGroupObject(sg))
	}

	return c.JSON(http.StatusOK, describeSubnetGroupResponse{SubnetGroups: objs, NextToken: nextToken})
}

func (h *Handler) handleDeleteSubnetGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req deleteSubnetGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SubnetGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SubnetGroupName is required")
	}

	sg, err := h.Backend.DeleteSubnetGroup(ctx, req.SubnetGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteSubnetGroupResponse{SubnetGroup: toSubnetGroupObject(sg)})
}

func (h *Handler) handleUpdateSubnetGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req updateSubnetGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SubnetGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SubnetGroupName is required")
	}

	sg, err := h.Backend.UpdateSubnetGroup(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateSubnetGroupResponse{SubnetGroup: toSubnetGroupObject(sg)})
}

// -- User handlers ---------------------------------------------------------------

// defaultSupportedNetworkTypes is the network-type list surfaced for both
// SubnetGroup and each nested Subnet -- this mock only ever creates IPv4
// subnets, so a single-element "ipv4" list matches actual behavior (real AWS
// returns "ipv6"/"dual-stack" too when applicable, per
// types.SubnetGroup.SupportedNetworkTypes).
var defaultSupportedNetworkTypes = []string{networkTypeIPv4} //nolint:gochecknoglobals // read-only default

// regionFromARN extracts the region segment (arn:partition:service:region:...)
// from an ARN, or "" if the ARN is malformed.
func regionFromARN(resourceARN string) string {
	parts := strings.SplitN(resourceARN, ":", splitARNParts)
	if len(parts) < splitARNParts {
		return ""
	}

	return parts[3]
}

// toSubnetGroupObject converts a SubnetGroup to its JSON representation.
// subnetGroupObject/subnetEntry are field-diffed against the real SDK's
// types.SubnetGroup/types.Subnet (deserializers.go's
// awsAwsjson11_deserializeDocumentSubnetGroup/...Subnet): both carry a
// SupportedNetworkTypes list, and each Subnet also carries its
// AvailabilityZone -- a prior pass omitted all three.
func toSubnetGroupObject(sg *SubnetGroup) subnetGroupObject {
	region := regionFromARN(sg.ARN)
	if region == "" {
		region = config.DefaultRegion
	}

	zones := []string{region + "a", region + "b", region + "c"}

	subnets := make([]subnetEntry, 0, len(sg.SubnetIDs))

	for i, id := range sg.SubnetIDs {
		subnets = append(subnets, subnetEntry{
			Identifier:            id,
			AvailabilityZone:      &availabilityZoneObject{Name: zones[i%len(zones)]},
			SupportedNetworkTypes: defaultSupportedNetworkTypes,
		})
	}

	return subnetGroupObject{
		Name:                  sg.Name,
		ARN:                   sg.ARN,
		Description:           sg.Description,
		VPCID:                 sg.VPCID,
		Subnets:               subnets,
		SupportedNetworkTypes: defaultSupportedNetworkTypes,
	}
}

// splitARNParts is the number of ":"-delimited segments in a well-formed ARN
// (arn:partition:service:region:account-id:resource).
const splitARNParts = 6
