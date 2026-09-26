package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

// ---- IPAM routing policy registration handlers ----
//
// Wire element casing verified against ec2@v1.329.0 deserializers.go's
// awsEc2query_deserializeDocumentIpamRoutingPolicyRegistration(Delta) and the per-op
// awsEc2query_deserializeOpDocument<Op>Output functions. Request field names (Asn.N flat list,
// no ".member." wrapper) verified against the matching serializers.go functions.

type ipamRoutingPolicyRegistrationDeltaItem struct {
	DeltaID      string `xml:"deltaId"`
	DeltaJSON    string `xml:"deltaJson,omitempty"`
	State        string `xml:"state"`
	StateMessage string `xml:"stateMessage,omitempty"`
}

func toIpamRoutingPolicyRegistrationDeltaItem(
	d *IpamRoutingPolicyRegistrationDelta,
) ipamRoutingPolicyRegistrationDeltaItem {
	return ipamRoutingPolicyRegistrationDeltaItem{
		DeltaID: d.DeltaID, DeltaJSON: d.DeltaJSON, State: d.State, StateMessage: d.StateMessage,
	}
}

type ipamRoutingPolicyRegistrationItem struct {
	Cidr          string `xml:"cidr"`
	Description   string `xml:"description,omitempty"`
	LatestDeltaID string `xml:"latestDeltaId,omitempty"`
	State         string `xml:"state"`
	AsnSet        struct {
		Items []string `xml:"item"`
	} `xml:"asnSet"`
	MaxLength                       int32 `xml:"maxLength,omitempty"`
	PermitMoreSpecificAnnouncements bool  `xml:"permitMoreSpecificAnnouncements,omitempty"`
}

func toIpamRoutingPolicyRegistrationItem(r *IpamRoutingPolicyRegistration) ipamRoutingPolicyRegistrationItem {
	item := ipamRoutingPolicyRegistrationItem{
		Cidr: r.Cidr, Description: r.Description, LatestDeltaID: r.LatestDeltaID,
		MaxLength: r.MaxLength, PermitMoreSpecificAnnouncements: r.PermitMoreSpecificAnnouncements, State: r.State,
	}
	item.AsnSet.Items = append([]string(nil), r.Asns...)

	return item
}

// parseAsnList extracts Asn.1, Asn.2, ... (FlatKey, no ".member." wrapper --
// serializers.go's awsEc2query_serializeOpDocumentCreateIpamRoutingPolicyRegistrationInput).
func parseAsnList(vals url.Values) []string {
	return parseMemberList(vals, "Asn")
}

func parseInt32Field(vals url.Values, key string) (int32, error) {
	raw := vals.Get(key)
	if raw == "" {
		return 0, nil
	}

	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid %s %q", ErrInvalidParameter, key, raw)
	}

	return int32(n), nil
}

type createIpamRoutingPolicyRegistrationResponse struct {
	XMLName   xml.Name `xml:"CreateIpamRoutingPolicyRegistrationResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`

	IpamRoutingPolicyRegistrationDelta ipamRoutingPolicyRegistrationDeltaItem `xml:"ipamRoutingPolicyRegistrationDelta"`
}

func (h *Handler) handleCreateIpamRoutingPolicyRegistration(vals url.Values, reqID string) (any, error) {
	maxLength, err := parseInt32Field(vals, "MaxLength")
	if err != nil {
		return nil, err
	}

	permit, _ := strconv.ParseBool(vals.Get("PermitMoreSpecificAnnouncements"))
	force, _ := strconv.ParseBool(vals.Get("Force"))

	delta, err := h.Backend.CreateIpamRoutingPolicyRegistration(
		vals.Get("IpamInternetRegistryAssociationId"), vals.Get("Cidr"), parseAsnList(vals),
		vals.Get("Description"), maxLength, permit, force,
	)
	if err != nil {
		return nil, err
	}

	return &createIpamRoutingPolicyRegistrationResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		IpamRoutingPolicyRegistrationDelta: toIpamRoutingPolicyRegistrationDeltaItem(delta),
	}, nil
}

type modifyIpamRoutingPolicyRegistrationResponse struct {
	XMLName   xml.Name `xml:"ModifyIpamRoutingPolicyRegistrationResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`

	IpamRoutingPolicyRegistrationDelta ipamRoutingPolicyRegistrationDeltaItem `xml:"ipamRoutingPolicyRegistrationDelta"`
}

func (h *Handler) handleModifyIpamRoutingPolicyRegistration(vals url.Values, reqID string) (any, error) {
	maxLength, err := parseInt32Field(vals, "MaxLength")
	if err != nil {
		return nil, err
	}

	permit, _ := strconv.ParseBool(vals.Get("PermitMoreSpecificAnnouncements"))
	force, _ := strconv.ParseBool(vals.Get("Force"))

	delta, err := h.Backend.ModifyIpamRoutingPolicyRegistration(
		vals.Get("IpamInternetRegistryAssociationId"), vals.Get("Cidr"), parseAsnList(vals),
		vals.Get("Description"), maxLength, permit, force,
	)
	if err != nil {
		return nil, err
	}

	return &modifyIpamRoutingPolicyRegistrationResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		IpamRoutingPolicyRegistrationDelta: toIpamRoutingPolicyRegistrationDeltaItem(delta),
	}, nil
}

type deleteIpamRoutingPolicyRegistrationResponse struct {
	XMLName   xml.Name `xml:"DeleteIpamRoutingPolicyRegistrationResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`

	IpamRoutingPolicyRegistrationDelta ipamRoutingPolicyRegistrationDeltaItem `xml:"ipamRoutingPolicyRegistrationDelta"`
}

func (h *Handler) handleDeleteIpamRoutingPolicyRegistration(vals url.Values, reqID string) (any, error) {
	force, _ := strconv.ParseBool(vals.Get("Force"))

	delta, err := h.Backend.DeleteIpamRoutingPolicyRegistration(
		vals.Get("IpamInternetRegistryAssociationId"), vals.Get("Cidr"), force,
	)
	if err != nil {
		return nil, err
	}

	return &deleteIpamRoutingPolicyRegistrationResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		IpamRoutingPolicyRegistrationDelta: toIpamRoutingPolicyRegistrationDeltaItem(delta),
	}, nil
}

type batchModifyIpamRoutingPolicyRegistrationsResponse struct {
	XMLName   xml.Name `xml:"BatchModifyIpamRoutingPolicyRegistrationsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`

	IpamRoutingPolicyRegistrationDelta ipamRoutingPolicyRegistrationDeltaItem `xml:"ipamRoutingPolicyRegistrationDelta"`
}

func (h *Handler) handleBatchModifyIpamRoutingPolicyRegistrations(vals url.Values, reqID string) (any, error) {
	force, _ := strconv.ParseBool(vals.Get("Force"))

	delta, err := h.Backend.BatchModifyIpamRoutingPolicyRegistrations(
		vals.Get("IpamInternetRegistryAssociationId"), vals.Get("DeltaJson"), force,
	)
	if err != nil {
		return nil, err
	}

	return &batchModifyIpamRoutingPolicyRegistrationsResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		IpamRoutingPolicyRegistrationDelta: toIpamRoutingPolicyRegistrationDeltaItem(delta),
	}, nil
}

type getIpamRoutingPolicyRegistrationsResponse struct {
	XMLName                          xml.Name `xml:"GetIpamRoutingPolicyRegistrationsResponse"`
	Xmlns                            string   `xml:"xmlns,attr"`
	RequestID                        string   `xml:"requestId"`
	NextToken                        string   `xml:"nextToken,omitempty"`
	IpamRoutingPolicyRegistrationSet struct {
		Items []ipamRoutingPolicyRegistrationItem `xml:"item"`
	} `xml:"ipamRoutingPolicyRegistrationSet"`
}

func (h *Handler) handleGetIpamRoutingPolicyRegistrations(vals url.Values, reqID string) (any, error) {
	regs, err := h.Backend.GetIpamRoutingPolicyRegistrations(
		vals.Get("IpamInternetRegistryAssociationId"), vals.Get("Cidr"),
	)
	if err != nil {
		return nil, err
	}

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	page, nextToken := pageSlice(regs, offset, maxResults)

	resp := &getIpamRoutingPolicyRegistrationsResponse{Xmlns: ec2XMLNS, RequestID: reqID, NextToken: nextToken}
	for _, r := range page {
		resp.IpamRoutingPolicyRegistrationSet.Items = append(
			resp.IpamRoutingPolicyRegistrationSet.Items, toIpamRoutingPolicyRegistrationItem(r),
		)
	}

	return resp, nil
}

// parseEC2Timestamp parses a StartTime/EndTime value serialized via smithytime.FormatDateTime
// (RFC3339 / ISO8601), used by GetIpamRoutingPolicyRegistrationDeltas.
func parseEC2Timestamp(raw string) (*time.Time, error) {
	if raw == "" {
		return nil, nil //nolint:nilnil // absent optional timestamp is a valid "no filter" result
	}

	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid timestamp %q", ErrInvalidParameter, raw)
	}

	return &t, nil
}

type getIpamRoutingPolicyRegistrationDeltasResponse struct {
	XMLName                               xml.Name `xml:"GetIpamRoutingPolicyRegistrationDeltasResponse"`
	Xmlns                                 string   `xml:"xmlns,attr"`
	RequestID                             string   `xml:"requestId"`
	NextToken                             string   `xml:"nextToken,omitempty"`
	IpamRoutingPolicyRegistrationDeltaSet struct {
		Items []ipamRoutingPolicyRegistrationDeltaItem `xml:"item"`
	} `xml:"ipamRoutingPolicyRegistrationDeltaSet"`
}

func (h *Handler) handleGetIpamRoutingPolicyRegistrationDeltas(vals url.Values, reqID string) (any, error) {
	startTime, err := parseEC2Timestamp(vals.Get("StartTime"))
	if err != nil {
		return nil, err
	}

	endTime, err := parseEC2Timestamp(vals.Get("EndTime"))
	if err != nil {
		return nil, err
	}

	deltas, err := h.Backend.GetIpamRoutingPolicyRegistrationDeltas(
		vals.Get("IpamInternetRegistryAssociationId"), vals.Get("DeltaId"),
		vals.Get("ChronologicalOrder"), startTime, endTime,
	)
	if err != nil {
		return nil, err
	}

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	page, nextToken := pageSlice(deltas, offset, maxResults)

	resp := &getIpamRoutingPolicyRegistrationDeltasResponse{Xmlns: ec2XMLNS, RequestID: reqID, NextToken: nextToken}
	for _, d := range page {
		resp.IpamRoutingPolicyRegistrationDeltaSet.Items = append(
			resp.IpamRoutingPolicyRegistrationDeltaSet.Items, toIpamRoutingPolicyRegistrationDeltaItem(d),
		)
	}

	return resp, nil
}

type ipamRouteOriginAuthorizationItem struct {
	Asn       string `xml:"asn"`
	Cidr      string `xml:"cidr"`
	MaxLength int32  `xml:"maxLength,omitempty"`
}

type getIpamRouteOriginAuthorizationsResponse struct {
	XMLName                         xml.Name `xml:"GetIpamRouteOriginAuthorizationsResponse"`
	Xmlns                           string   `xml:"xmlns,attr"`
	RequestID                       string   `xml:"requestId"`
	NextToken                       string   `xml:"nextToken,omitempty"`
	IpamRouteOriginAuthorizationSet struct {
		Items []ipamRouteOriginAuthorizationItem `xml:"item"`
	} `xml:"ipamRouteOriginAuthorizationSet"`
}

func (h *Handler) handleGetIpamRouteOriginAuthorizations(vals url.Values, reqID string) (any, error) {
	roas, err := h.Backend.GetIpamRouteOriginAuthorizations(
		vals.Get("IpamInternetRegistryAssociationId"), vals.Get("Cidr"),
	)
	if err != nil {
		return nil, err
	}

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	page, nextToken := pageSlice(roas, offset, maxResults)

	resp := &getIpamRouteOriginAuthorizationsResponse{Xmlns: ec2XMLNS, RequestID: reqID, NextToken: nextToken}
	for _, r := range page {
		resp.IpamRouteOriginAuthorizationSet.Items = append(
			resp.IpamRouteOriginAuthorizationSet.Items,
			ipamRouteOriginAuthorizationItem{Asn: r.Asn, Cidr: r.Cidr, MaxLength: r.MaxLength},
		)
	}

	return resp, nil
}

// getIpamDiscoveredRoutesResponse always returns an empty (but correctly shaped) route set:
// this backend has no BGP route discovery pipeline (PARITY.md gap). Mirrors
// getIpamAddressHistoryResponse's precedent in handler_advanced_networking.go.
type getIpamDiscoveredRoutesResponse struct {
	XMLName                xml.Name `xml:"GetIpamDiscoveredRoutesResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	RequestID              string   `xml:"requestId"`
	NextToken              string   `xml:"nextToken,omitempty"`
	IpamDiscoveredRouteSet struct {
		Items []struct{} `xml:"item"`
	} `xml:"ipamDiscoveredRouteSet"`
}

func (h *Handler) handleGetIpamDiscoveredRoutes(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.GetIpamDiscoveredRoutes(vals.Get("IpamResourceDiscoveryId")); err != nil {
		return nil, err
	}

	return &getIpamDiscoveredRoutesResponse{Xmlns: ec2XMLNS, RequestID: reqID}, nil
}

// getIpamRouteProtectionFindingsResponse always returns an empty (but correctly shaped)
// findings set: this backend has no RPKI route validation pipeline (PARITY.md gap).
type getIpamRouteProtectionFindingsResponse struct {
	XMLName                   xml.Name `xml:"GetIpamRouteProtectionFindingsResponse"`
	Xmlns                     string   `xml:"xmlns,attr"`
	RequestID                 string   `xml:"requestId"`
	IpamID                    string   `xml:"ipamId,omitempty"`
	NextToken                 string   `xml:"nextToken,omitempty"`
	RouteProtectionFindingSet struct {
		Items []struct{} `xml:"item"`
	} `xml:"routeProtectionFindingSet"`
}

func (h *Handler) handleGetIpamRouteProtectionFindings(vals url.Values, reqID string) (any, error) {
	ipamID := vals.Get("IpamId")
	if err := h.Backend.GetIpamRouteProtectionFindings(ipamID); err != nil {
		return nil, err
	}

	return &getIpamRouteProtectionFindingsResponse{Xmlns: ec2XMLNS, RequestID: reqID, IpamID: ipamID}, nil
}
