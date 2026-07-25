package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

// ---- IPAM handlers ----

// parseIpamOperatingRegions extracts OperatingRegion.N.RegionName parameters.
func parseIpamOperatingRegions(vals url.Values) []string {
	var regions []string

	for i := 1; ; i++ {
		r := vals.Get(fmt.Sprintf("OperatingRegion.%d.RegionName", i))
		if r == "" {
			return regions
		}

		regions = append(regions, r)
	}
}

func (h *Handler) handleCreateIpam(vals url.Values, reqID string) (any, error) {
	ipam, err := h.Backend.CreateIpam(IpamOptions{
		Description:      vals.Get("Description"),
		OperatingRegions: parseIpamOperatingRegions(vals),
		Tier:             vals.Get("Tier"),
	})
	if err != nil {
		return nil, err
	}

	return &createIpamResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Ipam:      toIpamItem(ipam),
	}, nil
}

func (h *Handler) handleDescribeIpams(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "IpamId")
	ipams := h.Backend.DescribeIpams(ids)

	resp := &describeIpamsResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, ipam := range ipams {
		resp.IpamSet.Items = append(resp.IpamSet.Items, toIpamItem(ipam))
	}

	return resp, nil
}

func (h *Handler) handleModifyIpam(vals url.Values, reqID string) (any, error) {
	ipam, err := h.Backend.ModifyIpam(vals.Get("IpamId"), IpamOptions{
		Description:      vals.Get("Description"),
		OperatingRegions: parseIpamOperatingRegions(vals),
		Tier:             vals.Get("Tier"),
	})
	if err != nil {
		return nil, err
	}

	return &modifyIpamResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Ipam:      toIpamItem(ipam),
	}, nil
}

func (h *Handler) handleDeleteIpam(vals url.Values, reqID string) (any, error) {
	id := vals.Get("IpamId")

	ipams := h.Backend.DescribeIpams([]string{id})
	if len(ipams) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, id)
	}

	if err := h.Backend.DeleteIpam(id); err != nil {
		return nil, err
	}

	item := toIpamItem(ipams[0])
	item.State = ipamStateDeleteComplete

	return &deleteIpamResponse{Xmlns: ec2XMLNS, RequestID: reqID, Ipam: item}, nil
}

func (h *Handler) handleCreateIpamScope(vals url.Values, reqID string) (any, error) {
	scope, err := h.Backend.CreateIpamScope(vals.Get("IpamId"), vals.Get("Description"))
	if err != nil {
		return nil, err
	}

	return &createIpamScopeResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		IpamScope: toIpamScopeItem(scope),
	}, nil
}

func (h *Handler) handleDescribeIpamScopes(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "IpamScopeId")
	scopes := h.Backend.DescribeIpamScopes(ids)

	resp := &describeIpamScopesResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, scope := range scopes {
		resp.IpamScopeSet.Items = append(resp.IpamScopeSet.Items, toIpamScopeItem(scope))
	}

	return resp, nil
}

func (h *Handler) handleModifyIpamScope(vals url.Values, reqID string) (any, error) {
	scope, err := h.Backend.ModifyIpamScope(vals.Get("IpamScopeId"), vals.Get("Description"))
	if err != nil {
		return nil, err
	}

	return &modifyIpamScopeResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		IpamScope: toIpamScopeItem(scope),
	}, nil
}

func (h *Handler) handleDeleteIpamScope(vals url.Values, reqID string) (any, error) {
	id := vals.Get("IpamScopeId")

	scopes := h.Backend.DescribeIpamScopes([]string{id})
	if len(scopes) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrIpamScopeNotFound, id)
	}

	if err := h.Backend.DeleteIpamScope(id); err != nil {
		return nil, err
	}

	item := toIpamScopeItem(scopes[0])
	item.State = ipamStateDeleteComplete

	return &deleteIpamScopeResponse{Xmlns: ec2XMLNS, RequestID: reqID, IpamScope: item}, nil
}

// maxNetmaskLength is the widest possible IP prefix length (IPv6's /128 also covers IPv4's
// /32). Allocation netmask lengths from the wire are bounds-checked against it before their
// int32 conversion, since strconv.Atoi returns a platform-width int that could otherwise
// silently truncate.
const maxNetmaskLength = 128

// parseNetmaskLength parses an AllocationMinNetmaskLength/AllocationMaxNetmaskLength/
// AllocationDefaultNetmaskLength query value, bounds-checking it to [0, maxNetmaskLength]
// before the narrowing conversion to int32. An empty raw value yields (0, nil), matching the
// "field not provided" behavior the callers previously got from a discarded Atoi error.
func parseNetmaskLength(raw string) (int32, error) {
	if raw == "" {
		return 0, nil
	}

	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid netmask length %q", ErrInvalidParameter, raw)
	}

	if v < 0 || v > maxNetmaskLength {
		return 0, fmt.Errorf(
			"%w: netmask length must be between 0 and %d, got %d",
			ErrInvalidParameter, maxNetmaskLength, v,
		)
	}

	return int32(v), nil //nolint:gosec // G109: bounds-checked to [0, maxNetmaskLength] above
}

func (h *Handler) handleCreateIpamPool(vals url.Values, reqID string) (any, error) {
	// Accept either IpamId directly or fall back to the scope's parent IPAM.
	// For simplicity, prefer IpamId; if not present, use IpamScopeId as-is.
	ipamID := vals.Get("IpamId")
	if ipamID == "" {
		ipamID = vals.Get("IpamScopeId")
	}

	minNetmask, err := parseNetmaskLength(vals.Get("AllocationMinNetmaskLength"))
	if err != nil {
		return nil, err
	}

	maxNetmask, err := parseNetmaskLength(vals.Get("AllocationMaxNetmaskLength"))
	if err != nil {
		return nil, err
	}

	defaultNetmask, err := parseNetmaskLength(vals.Get("AllocationDefaultNetmaskLength"))
	if err != nil {
		return nil, err
	}

	pool, err := h.Backend.CreateIpamPool(
		ipamID,
		vals.Get("AddressFamily"),
		vals.Get("Locale"),
		vals.Get("ProvisionedCidrs.item.1.Cidr"),
		IpamPoolOptions{
			IpamScopeID:                    vals.Get("IpamScopeId"),
			Description:                    vals.Get("Description"),
			AutoImport:                     vals.Get("AutoImport") == ec2BooleanTrue,
			PubliclyAdvertisable:           vals.Get("PubliclyAdvertisable") == ec2BooleanTrue,
			AllocationMinNetmaskLength:     minNetmask,
			AllocationMaxNetmaskLength:     maxNetmask,
			AllocationDefaultNetmaskLength: defaultNetmask,
		},
	)
	if err != nil {
		return nil, err
	}

	return &createIpamPoolResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		IpamPool:  toIpamPoolItem(pool),
	}, nil
}

func (h *Handler) handleDescribeIpamPools(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "IpamPoolId")
	pools := h.Backend.DescribeIpamPools(ids)

	resp := &describeIpamPoolsResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, pool := range pools {
		resp.IpamPoolSet.Items = append(resp.IpamPoolSet.Items, toIpamPoolItem(pool))
	}

	return resp, nil
}

func (h *Handler) handleModifyIpamPool(vals url.Values, reqID string) (any, error) {
	minNetmask, err := parseNetmaskLength(vals.Get("AllocationMinNetmaskLength"))
	if err != nil {
		return nil, err
	}

	maxNetmask, err := parseNetmaskLength(vals.Get("AllocationMaxNetmaskLength"))
	if err != nil {
		return nil, err
	}

	defaultNetmask, err := parseNetmaskLength(vals.Get("AllocationDefaultNetmaskLength"))
	if err != nil {
		return nil, err
	}

	pool, err := h.Backend.ModifyIpamPool(vals.Get("IpamPoolId"), IpamPoolOptions{
		Description:                    vals.Get("Description"),
		AutoImport:                     vals.Get("AutoImport") == ec2BooleanTrue,
		AllocationMinNetmaskLength:     minNetmask,
		AllocationMaxNetmaskLength:     maxNetmask,
		AllocationDefaultNetmaskLength: defaultNetmask,
	})
	if err != nil {
		return nil, err
	}

	return &modifyIpamPoolResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		IpamPool:  toIpamPoolItem(pool),
	}, nil
}

func (h *Handler) handleDeleteIpamPool(vals url.Values, reqID string) (any, error) {
	id := vals.Get("IpamPoolId")

	pools := h.Backend.DescribeIpamPools([]string{id})
	if len(pools) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrIpamPoolNotFound, id)
	}

	if err := h.Backend.DeleteIpamPool(id); err != nil {
		return nil, err
	}

	item := toIpamPoolItem(pools[0])
	item.State = ipamStateDeleteComplete

	return &deleteIpamPoolResponse{Xmlns: ec2XMLNS, RequestID: reqID, IpamPool: item}, nil
}

func (h *Handler) handleProvisionIpamPoolCidr(vals url.Values, reqID string) (any, error) {
	cidr, err := h.Backend.ProvisionIpamPoolCidr(vals.Get("IpamPoolId"), vals.Get("Cidr"))
	if err != nil {
		return nil, err
	}

	return &provisionIpamPoolCidrResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		IpamPoolCidr: toIpamPoolCidrItem(cidr),
	}, nil
}

func (h *Handler) handleDeprovisionIpamPoolCidr(vals url.Values, reqID string) (any, error) {
	cidr, err := h.Backend.DeprovisionIpamPoolCidr(vals.Get("IpamPoolId"), vals.Get("Cidr"))
	if err != nil {
		return nil, err
	}

	return &deprovisionIpamPoolCidrResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		IpamPoolCidr: toIpamPoolCidrItem(cidr),
	}, nil
}

func (h *Handler) handleGetIpamPoolCidrs(vals url.Values, reqID string) (any, error) {
	poolID := vals.Get("IpamPoolId")
	cidrs := h.Backend.GetIpamPoolCidrs(poolID)

	resp := &getIpamPoolCidrsResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, c := range cidrs {
		resp.IpamPoolCidrSet.Items = append(resp.IpamPoolCidrSet.Items, toIpamPoolCidrItem(c))
	}

	return resp, nil
}

func (h *Handler) handleAllocateIpamPoolCidr(vals url.Values, reqID string) (any, error) {
	netmaskLenStr := vals.Get("NetmaskLength")
	netmaskLen, _ := strconv.Atoi(netmaskLenStr)

	alloc, err := h.Backend.AllocateIpamPoolCidr(
		vals.Get("IpamPoolId"),
		vals.Get("Cidr"),
		netmaskLen,
		IpamAllocationOptions{Description: vals.Get("Description")},
	)
	if err != nil {
		return nil, err
	}

	return &allocateIpamPoolCidrResponse{
		Xmlns:              ec2XMLNS,
		RequestID:          reqID,
		IpamPoolAllocation: toIpamPoolAllocationItem(alloc),
	}, nil
}

func (h *Handler) handleGetIpamPoolAllocations(vals url.Values, reqID string) (any, error) {
	poolID := vals.Get("IpamPoolId")
	allocationID := vals.Get("IpamPoolAllocationId")

	allocs, err := h.Backend.GetIpamPoolAllocations(poolID, allocationID)
	if err != nil {
		return nil, err
	}

	resp := &getIpamPoolAllocationsResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, alloc := range allocs {
		resp.IpamPoolAllocationSet.Items = append(
			resp.IpamPoolAllocationSet.Items,
			toIpamPoolAllocationItem(alloc),
		)
	}

	return resp, nil
}

func (h *Handler) handleReleaseIpamPoolAllocation(vals url.Values, reqID string) (any, error) {
	poolID := vals.Get("IpamPoolId")
	allocID := vals.Get("IpamPoolAllocationId")

	if err := h.Backend.ReleaseIpamPoolAllocation(poolID, allocID); err != nil {
		return nil, err
	}

	return &releaseIpamPoolAllocationResponse{RequestID: reqID, Return: true}, nil
}

type describeIpamPoolAllocationsResponse struct {
	XMLName               xml.Name `xml:"DescribeIpamPoolAllocationsResponse"`
	Xmlns                 string   `xml:"xmlns,attr"`
	RequestID             string   `xml:"requestId"`
	NextToken             string   `xml:"nextToken,omitempty"`
	IpamPoolAllocationSet struct {
		Items []ipamPoolAllocationItem `xml:"item"`
	} `xml:"ipamPoolAllocationSet"`
}

func (h *Handler) handleDescribeIpamPoolAllocations(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "IpamPoolAllocationId")

	allocs := h.Backend.DescribeIpamPoolAllocations(ids)

	resp := &describeIpamPoolAllocationsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, alloc := range allocs {
		resp.IpamPoolAllocationSet.Items = append(resp.IpamPoolAllocationSet.Items, toIpamPoolAllocationItem(alloc))
	}

	return resp, nil
}

type modifyIpamPoolAllocationResponse struct {
	XMLName            xml.Name               `xml:"ModifyIpamPoolAllocationResponse"`
	Xmlns              string                 `xml:"xmlns,attr"`
	RequestID          string                 `xml:"requestId"`
	IpamPoolAllocation ipamPoolAllocationItem `xml:"ipamPoolAllocation"`
}

func (h *Handler) handleModifyIpamPoolAllocation(vals url.Values, reqID string) (any, error) {
	alloc, err := h.Backend.ModifyIpamPoolAllocation(
		vals.Get("IpamPoolAllocationId"),
		vals.Get("Description"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyIpamPoolAllocationResponse{
		Xmlns:              ec2XMLNS,
		RequestID:          reqID,
		IpamPoolAllocation: toIpamPoolAllocationItem(alloc),
	}, nil
}

func (h *Handler) handleDescribeIpamResourceDiscoveries(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "IpamResourceDiscoveryId")
	discoveries := h.Backend.DescribeIpamResourceDiscoveries(ids)

	resp := &describeIpamResourceDiscoveriesResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, d := range discoveries {
		resp.IpamResourceDiscoverySet.Items = append(
			resp.IpamResourceDiscoverySet.Items, toIpamResourceDiscoveryItem(d),
		)
	}

	return resp, nil
}

func (h *Handler) handleDescribeIpamResourceDiscoveryAssociations(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "IpamResourceDiscoveryAssociationId")
	assocs := h.Backend.DescribeIpamResourceDiscoveryAssociations(ids)

	resp := &describeIpamResourceDiscoveryAssociationsResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, a := range assocs {
		resp.IpamResourceDiscoveryAssociationSet.Items = append(
			resp.IpamResourceDiscoveryAssociationSet.Items, toIpamResourceDiscoveryAssociationItem(a),
		)
	}

	return resp, nil
}

// handleGetIpamAddressHistory always returns an empty (but correctly shaped) history record
// set: modeling real IPAM address-usage history requires a live discovery pipeline this mock
// does not implement.
func (h *Handler) handleGetIpamAddressHistory(_ url.Values, reqID string) (any, error) {
	return &getIpamAddressHistoryResponse{Xmlns: ec2XMLNS, RequestID: reqID}, nil
}

// handleGetIpamDiscoveredAccounts always returns an empty (but correctly shaped) account set.
func (h *Handler) handleGetIpamDiscoveredAccounts(_ url.Values, reqID string) (any, error) {
	return &getIpamDiscoveredAccountsResponse{Xmlns: ec2XMLNS, RequestID: reqID}, nil
}

// handleGetIpamDiscoveredResourceCidrs always returns an empty (but correctly shaped) CIDR set.
func (h *Handler) handleGetIpamDiscoveredResourceCidrs(_ url.Values, reqID string) (any, error) {
	return &getIpamDiscoveredResourceCidrsResponse{Xmlns: ec2XMLNS, RequestID: reqID}, nil
}

// handleGetIpamDiscoveredPublicAddresses always returns an empty (but correctly shaped) address set.
func (h *Handler) handleGetIpamDiscoveredPublicAddresses(_ url.Values, reqID string) (any, error) {
	return &getIpamDiscoveredPublicAddressesResponse{Xmlns: ec2XMLNS, RequestID: reqID}, nil
}
