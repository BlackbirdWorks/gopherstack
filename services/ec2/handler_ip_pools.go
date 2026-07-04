package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

// handler_ip_pools.go implements the HTTP handlers for the COIP pool and public IPv4/IPv6
// address pool families, backed by backend_ip_pools.go.

// ---- Registration ----

func registerIPPoolOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateCoipPool"] = h.handleCreateCoipPool
	ops["DeleteCoipPool"] = h.handleDeleteCoipPool
	ops["DescribeCoipPools"] = h.handleDescribeCoipPools
	ops["CreateCoipCidr"] = h.handleCreateCoipCidr
	ops["DeleteCoipCidr"] = h.handleDeleteCoipCidr
	ops["GetCoipPoolUsage"] = h.handleGetCoipPoolUsage

	ops["CreatePublicIpv4Pool"] = h.handleCreatePublicIpv4Pool
	ops["DeletePublicIpv4Pool"] = h.handleDeletePublicIpv4Pool
	ops["DescribePublicIpv4Pools"] = h.handleDescribePublicIpv4Pools
	ops["ProvisionPublicIpv4PoolCidr"] = h.handleProvisionPublicIpv4PoolCidr
	ops["DeprovisionPublicIpv4PoolCidr"] = h.handleDeprovisionPublicIpv4PoolCidr
	ops["DescribeIpv6Pools"] = h.handleDescribeIpv6Pools
	ops["GetAssociatedIpv6PoolCidrs"] = h.handleGetAssociatedIpv6PoolCidrs
}

// ipPoolSupportedOperations lists the operation names registered by registerIPPoolOps, for
// GetSupportedOperations().
func ipPoolSupportedOperations() []string {
	return []string{
		"CreateCoipPool",
		"DeleteCoipPool",
		"DescribeCoipPools",
		"CreateCoipCidr",
		"DeleteCoipCidr",
		"GetCoipPoolUsage",
		"CreatePublicIpv4Pool",
		"DeletePublicIpv4Pool",
		"DescribePublicIpv4Pools",
		"ProvisionPublicIpv4PoolCidr",
		"DeprovisionPublicIpv4PoolCidr",
		"DescribeIpv6Pools",
		"GetAssociatedIpv6PoolCidrs",
	}
}

// ---- Response shapes: COIP ----

type coipPoolItem struct {
	LocalGatewayRouteTableID string `xml:"localGatewayRouteTableId,omitempty"`
	PoolArn                  string `xml:"poolArn,omitempty"`
	PoolID                   string `xml:"poolId,omitempty"`
	PoolCidrSet              struct {
		Items []string `xml:"item"`
	} `xml:"poolCidrSet"`
	TagSet struct {
		Items []instanceTagItem `xml:"item"`
	} `xml:"tagSet"`
}

func toCoipPoolItem(p *CoipPool) coipPoolItem {
	item := coipPoolItem{
		LocalGatewayRouteTableID: p.LocalGatewayRouteTableID,
		PoolArn:                  p.PoolArn,
		PoolID:                   p.PoolID,
	}
	item.PoolCidrSet.Items = append(item.PoolCidrSet.Items, p.PoolCidrs...)

	for k, v := range p.Tags {
		item.TagSet.Items = append(item.TagSet.Items, instanceTagItem{Key: k, Value: v})
	}

	return item
}

type createCoipPoolResponse struct {
	XMLName   xml.Name     `xml:"CreateCoipPoolResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	RequestID string       `xml:"requestId"`
	CoipPool  coipPoolItem `xml:"coipPool"`
}

type deleteCoipPoolResponse struct {
	XMLName   xml.Name     `xml:"DeleteCoipPoolResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	RequestID string       `xml:"requestId"`
	CoipPool  coipPoolItem `xml:"coipPool"`
}

type describeCoipPoolsResponse struct {
	XMLName     xml.Name `xml:"DescribeCoipPoolsResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	RequestID   string   `xml:"requestId"`
	CoipPoolSet struct {
		Items []coipPoolItem `xml:"item"`
	} `xml:"coipPoolSet"`
}

type coipCidrItem struct {
	Cidr                     string `xml:"cidr,omitempty"`
	CoipPoolID               string `xml:"coipPoolId,omitempty"`
	LocalGatewayRouteTableID string `xml:"localGatewayRouteTableId,omitempty"`
}

func toCoipCidrItem(c *CoipCidr) coipCidrItem {
	return coipCidrItem{
		Cidr:                     c.Cidr,
		CoipPoolID:               c.CoipPoolID,
		LocalGatewayRouteTableID: c.LocalGatewayRouteTableID,
	}
}

type createCoipCidrResponse struct {
	XMLName   xml.Name     `xml:"CreateCoipCidrResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	RequestID string       `xml:"requestId"`
	CoipCidr  coipCidrItem `xml:"coipCidr"`
}

type deleteCoipCidrResponse struct {
	XMLName   xml.Name     `xml:"DeleteCoipCidrResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	RequestID string       `xml:"requestId"`
	CoipCidr  coipCidrItem `xml:"coipCidr"`
}

type getCoipPoolUsageResponse struct {
	XMLName                  xml.Name `xml:"GetCoipPoolUsageResponse"`
	Xmlns                    string   `xml:"xmlns,attr"`
	RequestID                string   `xml:"requestId"`
	CoipPoolID               string   `xml:"coipPoolId,omitempty"`
	LocalGatewayRouteTableID string   `xml:"localGatewayRouteTableId,omitempty"`
	CoipAddressUsageSet      struct {
		Items []struct{} `xml:"item"`
	} `xml:"coipAddressUsageSet"`
}

// ---- Response shapes: Public IPv4 pools ----

type ipv4PoolRangeItem struct {
	FirstAddress          string `xml:"firstAddress,omitempty"`
	LastAddress           string `xml:"lastAddress,omitempty"`
	AddressCount          int32  `xml:"addressCount,omitempty"`
	AvailableAddressCount int32  `xml:"availableAddressCount,omitempty"`
}

func toIpv4PoolRangeItem(r Ipv4PoolRange) ipv4PoolRangeItem {
	return ipv4PoolRangeItem(r)
}

type publicIpv4PoolItem struct {
	Description         string `xml:"description,omitempty"`
	NetworkBorderGroup  string `xml:"networkBorderGroup,omitempty"`
	PoolID              string `xml:"poolId,omitempty"`
	PoolAddressRangeSet struct {
		Items []ipv4PoolRangeItem `xml:"item"`
	} `xml:"poolAddressRangeSet"`
	TagSet struct {
		Items []instanceTagItem `xml:"item"`
	} `xml:"tagSet"`
	TotalAddressCount          int32 `xml:"totalAddressCount,omitempty"`
	TotalAvailableAddressCount int32 `xml:"totalAvailableAddressCount,omitempty"`
}

func toPublicIpv4PoolItem(p *Ipv4Pool) publicIpv4PoolItem {
	item := publicIpv4PoolItem{
		Description:        p.Description,
		NetworkBorderGroup: p.NetworkBorderGroup,
		PoolID:             p.PoolID,
	}

	for _, r := range p.PoolAddressRanges {
		item.PoolAddressRangeSet.Items = append(item.PoolAddressRangeSet.Items, toIpv4PoolRangeItem(r))
		item.TotalAddressCount += r.AddressCount
		item.TotalAvailableAddressCount += r.AvailableAddressCount
	}

	for k, v := range p.Tags {
		item.TagSet.Items = append(item.TagSet.Items, instanceTagItem{Key: k, Value: v})
	}

	return item
}

type createPublicIpv4PoolResponse struct {
	XMLName   xml.Name `xml:"CreatePublicIpv4PoolResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	PoolID    string   `xml:"poolId,omitempty"`
}

type deletePublicIpv4PoolResponse struct {
	XMLName     xml.Name `xml:"DeletePublicIpv4PoolResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	RequestID   string   `xml:"requestId"`
	ReturnValue bool     `xml:"returnValue"`
}

type describePublicIpv4PoolsResponse struct {
	XMLName           xml.Name `xml:"DescribePublicIpv4PoolsResponse"`
	Xmlns             string   `xml:"xmlns,attr"`
	RequestID         string   `xml:"requestId"`
	PublicIpv4PoolSet struct {
		Items []publicIpv4PoolItem `xml:"item"`
	} `xml:"publicIpv4PoolSet"`
}

type provisionPublicIpv4PoolCidrResponse struct {
	XMLName          xml.Name          `xml:"ProvisionPublicIpv4PoolCidrResponse"`
	Xmlns            string            `xml:"xmlns,attr"`
	RequestID        string            `xml:"requestId"`
	PoolID           string            `xml:"poolId,omitempty"`
	PoolAddressRange ipv4PoolRangeItem `xml:"poolAddressRange"`
}

type deprovisionPublicIpv4PoolCidrResponse struct {
	XMLName                 xml.Name `xml:"DeprovisionPublicIpv4PoolCidrResponse"`
	Xmlns                   string   `xml:"xmlns,attr"`
	RequestID               string   `xml:"requestId"`
	PoolID                  string   `xml:"poolId,omitempty"`
	DeprovisionedAddressSet struct {
		Items []string `xml:"item"`
	} `xml:"deprovisionedAddressSet"`
}

// ---- Response shapes: IPv6 pools ----

type ipv6PoolItem struct {
	Description      string `xml:"description,omitempty"`
	PoolID           string `xml:"poolId,omitempty"`
	PoolCidrBlockSet struct {
		Items []struct {
			PoolCidrBlock string `xml:"poolCidrBlock,omitempty"`
		} `xml:"item"`
	} `xml:"poolCidrBlockSet"`
	TagSet struct {
		Items []instanceTagItem `xml:"item"`
	} `xml:"tagSet"`
}

func toIpv6PoolItem(p *Ipv6Pool) ipv6PoolItem {
	item := ipv6PoolItem{Description: p.Description, PoolID: p.PoolID}

	for _, cidr := range p.PoolCidrBlocks {
		item.PoolCidrBlockSet.Items = append(item.PoolCidrBlockSet.Items, struct {
			PoolCidrBlock string `xml:"poolCidrBlock,omitempty"`
		}{PoolCidrBlock: cidr})
	}

	for k, v := range p.Tags {
		item.TagSet.Items = append(item.TagSet.Items, instanceTagItem{Key: k, Value: v})
	}

	return item
}

type describeIpv6PoolsResponse struct {
	XMLName     xml.Name `xml:"DescribeIpv6PoolsResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	RequestID   string   `xml:"requestId"`
	Ipv6PoolSet struct {
		Items []ipv6PoolItem `xml:"item"`
	} `xml:"ipv6PoolSet"`
}

type ipv6CidrAssociationItem struct {
	Ipv6Cidr           string `xml:"ipv6Cidr,omitempty"`
	AssociatedResource string `xml:"associatedResource,omitempty"`
}

type getAssociatedIpv6PoolCidrsResponse struct {
	XMLName                xml.Name `xml:"GetAssociatedIpv6PoolCidrsResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	RequestID              string   `xml:"requestId"`
	Ipv6CidrAssociationSet struct {
		Items []ipv6CidrAssociationItem `xml:"item"`
	} `xml:"ipv6CidrAssociationSet"`
}

// ---- Handlers: COIP ----

func (h *Handler) handleCreateCoipPool(vals url.Values, reqID string) (any, error) {
	pool, err := h.Backend.CreateCoipPool(
		vals.Get("LocalGatewayRouteTableId"), parseTagSpecification(vals, "coip-pool"),
	)
	if err != nil {
		return nil, err
	}

	return &createCoipPoolResponse{Xmlns: ec2XMLNS, RequestID: reqID, CoipPool: toCoipPoolItem(pool)}, nil
}

func (h *Handler) handleDeleteCoipPool(vals url.Values, reqID string) (any, error) {
	pool, err := h.Backend.DeleteCoipPool(vals.Get("CoipPoolId"))
	if err != nil {
		return nil, err
	}

	return &deleteCoipPoolResponse{Xmlns: ec2XMLNS, RequestID: reqID, CoipPool: toCoipPoolItem(pool)}, nil
}

func (h *Handler) handleDescribeCoipPools(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "PoolId")
	pools := h.Backend.DescribeCoipPools(ids)

	resp := &describeCoipPoolsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, p := range pools {
		resp.CoipPoolSet.Items = append(resp.CoipPoolSet.Items, toCoipPoolItem(p))
	}

	return resp, nil
}

func (h *Handler) handleCreateCoipCidr(vals url.Values, reqID string) (any, error) {
	c, err := h.Backend.CreateCoipCidr(vals.Get("CoipPoolId"), vals.Get("Cidr"))
	if err != nil {
		return nil, err
	}

	return &createCoipCidrResponse{Xmlns: ec2XMLNS, RequestID: reqID, CoipCidr: toCoipCidrItem(c)}, nil
}

func (h *Handler) handleDeleteCoipCidr(vals url.Values, reqID string) (any, error) {
	c, err := h.Backend.DeleteCoipCidr(vals.Get("CoipPoolId"), vals.Get("Cidr"))
	if err != nil {
		return nil, err
	}

	return &deleteCoipCidrResponse{Xmlns: ec2XMLNS, RequestID: reqID, CoipCidr: toCoipCidrItem(c)}, nil
}

func (h *Handler) handleGetCoipPoolUsage(vals url.Values, reqID string) (any, error) {
	pool, err := h.Backend.GetCoipPoolUsage(vals.Get("PoolId"))
	if err != nil {
		return nil, err
	}

	return &getCoipPoolUsageResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		CoipPoolID: pool.PoolID, LocalGatewayRouteTableID: pool.LocalGatewayRouteTableID,
	}, nil
}

// ---- Handlers: Public IPv4 pools ----

func (h *Handler) handleCreatePublicIpv4Pool(vals url.Values, reqID string) (any, error) {
	pool := h.Backend.CreatePublicIpv4Pool(vals.Get("NetworkBorderGroup"), parseTagSpecification(vals, "ipv4pool-ec2"))

	return &createPublicIpv4PoolResponse{Xmlns: ec2XMLNS, RequestID: reqID, PoolID: pool.PoolID}, nil
}

func (h *Handler) handleDeletePublicIpv4Pool(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.DeletePublicIpv4Pool(vals.Get("PoolId")); err != nil {
		return nil, err
	}

	return &deletePublicIpv4PoolResponse{Xmlns: ec2XMLNS, RequestID: reqID, ReturnValue: true}, nil
}

func (h *Handler) handleDescribePublicIpv4Pools(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "PoolId")
	pools := h.Backend.DescribePublicIpv4Pools(ids)

	resp := &describePublicIpv4PoolsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, p := range pools {
		resp.PublicIpv4PoolSet.Items = append(resp.PublicIpv4PoolSet.Items, toPublicIpv4PoolItem(p))
	}

	return resp, nil
}

func (h *Handler) handleProvisionPublicIpv4PoolCidr(vals url.Values, reqID string) (any, error) {
	netmaskLength, _ := strconv.ParseInt(vals.Get("NetmaskLength"), 10, 32)

	poolID := vals.Get("PoolId")

	r, err := h.Backend.ProvisionPublicIpv4PoolCidr(poolID, int32(netmaskLength))
	if err != nil {
		return nil, err
	}

	return &provisionPublicIpv4PoolCidrResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, PoolID: poolID, PoolAddressRange: toIpv4PoolRangeItem(*r),
	}, nil
}

func (h *Handler) handleDeprovisionPublicIpv4PoolCidr(vals url.Values, reqID string) (any, error) {
	poolID := vals.Get("PoolId")
	cidr := vals.Get("Cidr")

	if err := h.Backend.DeprovisionPublicIpv4PoolCidr(poolID, cidr); err != nil {
		return nil, err
	}

	resp := &deprovisionPublicIpv4PoolCidrResponse{Xmlns: ec2XMLNS, RequestID: reqID, PoolID: poolID}
	resp.DeprovisionedAddressSet.Items = []string{cidr}

	return resp, nil
}

// ---- Handlers: IPv6 pools ----

func (h *Handler) handleDescribeIpv6Pools(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "PoolId")
	pools := h.Backend.DescribeIpv6Pools(ids)

	resp := &describeIpv6PoolsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, p := range pools {
		resp.Ipv6PoolSet.Items = append(resp.Ipv6PoolSet.Items, toIpv6PoolItem(p))
	}

	return resp, nil
}

func (h *Handler) handleGetAssociatedIpv6PoolCidrs(vals url.Values, reqID string) (any, error) {
	assocs, err := h.Backend.GetAssociatedIpv6PoolCidrs(vals.Get("PoolId"))
	if err != nil {
		return nil, err
	}

	resp := &getAssociatedIpv6PoolCidrsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, a := range assocs {
		resp.Ipv6CidrAssociationSet.Items = append(resp.Ipv6CidrAssociationSet.Items, ipv6CidrAssociationItem(a))
	}

	return resp, nil
}
