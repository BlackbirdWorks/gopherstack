package redshift

import (
	"encoding/xml"
	"net/url"
)

// ---- AuthorizeEndpointAccess ----

type xmlEndpointAuthorization struct {
	Grantor           string   `xml:"Grantor"`
	Grantee           string   `xml:"Grantee"`
	ClusterIdentifier string   `xml:"ClusterIdentifier"`
	ClusterStatus     string   `xml:"ClusterStatus,omitempty"`
	Status            string   `xml:"Status"`
	AllowedVPCs       []string `xml:"AllowedVPCs>VpcIdentifier,omitempty"`
	EndpointCount     int      `xml:"EndpointCount"`
	AllowedAllVPCs    bool     `xml:"AllowedAllVPCs"`
}

type authorizeEndpointAccessResponse struct {
	XMLName xml.Name                 `xml:"AuthorizeEndpointAccessResponse"`
	Xmlns   string                   `xml:"xmlns,attr"`
	Result  xmlEndpointAuthorization `xml:"AuthorizeEndpointAccessResult"`
}

func endpointAuthToXML(ea *EndpointAuthorization) xmlEndpointAuthorization {
	vpcs := make([]string, len(ea.AllowedVPCs))
	copy(vpcs, ea.AllowedVPCs)

	return xmlEndpointAuthorization{
		Grantor:           ea.Grantor,
		Grantee:           ea.Grantee,
		ClusterIdentifier: ea.ClusterIdentifier,
		ClusterStatus:     ea.ClusterStatus,
		Status:            ea.Status,
		AllowedAllVPCs:    ea.AllowedAllVPCs,
		AllowedVPCs:       vpcs,
		EndpointCount:     ea.EndpointCount,
	}
}

func (h *Handler) handleAuthorizeEndpointAccess(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	account := vals.Get("Account")
	vpcIDs := parseStringList(vals, "VpcIds.VpcIdentifier.")

	auth, err := h.Backend.AuthorizeEndpointAccess(clusterID, account, vpcIDs)
	if err != nil {
		return nil, err
	}

	return &authorizeEndpointAccessResponse{
		Xmlns:  redshiftXMLNS,
		Result: endpointAuthToXML(auth),
	}, nil
}

// ---- DescribeEndpointAuthorization ----

type xmlEndpointAuthorizationList struct {
	Members []xmlEndpointAuthorization `xml:"EndpointAuthorization"`
}

type describeEndpointAuthorizationResponse struct {
	XMLName        xml.Name                     `xml:"DescribeEndpointAuthorizationResponse"`
	Xmlns          string                       `xml:"xmlns,attr"`
	Authorizations xmlEndpointAuthorizationList `xml:"DescribeEndpointAuthorizationResult>EndpointAuthorizationList"`
}

func (h *Handler) handleDescribeEndpointAuthorization(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	account := vals.Get("Account")
	grantee := vals.Get("Grantee") == paramValueTrue

	auths, err := h.Backend.DescribeEndpointAuthorization(clusterID, account, grantee)
	if err != nil {
		return nil, err
	}

	members := make([]xmlEndpointAuthorization, 0, len(auths))

	for _, a := range auths {
		ap := a
		members = append(members, endpointAuthToXML(&ap))
	}

	return &describeEndpointAuthorizationResponse{
		Xmlns:          redshiftXMLNS,
		Authorizations: xmlEndpointAuthorizationList{Members: members},
	}, nil
}

// ---- RevokeEndpointAccess ----

type revokeEndpointAccessResponse struct {
	XMLName xml.Name                 `xml:"RevokeEndpointAccessResponse"`
	Xmlns   string                   `xml:"xmlns,attr"`
	Result  xmlEndpointAuthorization `xml:"RevokeEndpointAccessResult"`
}

func (h *Handler) handleRevokeEndpointAccess(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	account := vals.Get("Account")
	vpcIDs := parseStringList(vals, "VpcIds.VpcIdentifier.")
	force := vals.Get("Force") == paramValueTrue

	ea, err := h.Backend.RevokeEndpointAccess(clusterID, account, vpcIDs, force)
	if err != nil {
		return nil, err
	}

	return &revokeEndpointAccessResponse{
		Xmlns:  redshiftXMLNS,
		Result: endpointAuthToXML(ea),
	}, nil
}
