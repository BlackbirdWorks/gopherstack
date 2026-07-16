package redshift

import (
	"encoding/xml"
	"net/url"
)

// ---- GetResourcePolicy ----

type xmlResourcePolicy struct {
	ResourceArn string `xml:"ResourceArn"`
	Policy      string `xml:"Policy,omitempty"`
}

type getResourcePolicyResponse struct {
	XMLName xml.Name          `xml:"GetResourcePolicyResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Policy  xmlResourcePolicy `xml:"GetResourcePolicyResult>ResourcePolicy"`
}

func (h *Handler) handleGetResourcePolicy(vals url.Values) (any, error) {
	resourceArn := vals.Get("ResourceArn")

	rp, err := h.Backend.GetResourcePolicy(resourceArn)
	if err != nil {
		return nil, err
	}

	return &getResourcePolicyResponse{
		Xmlns: redshiftXMLNS,
		Policy: xmlResourcePolicy{
			ResourceArn: rp.ResourceArn,
			Policy:      rp.Policy,
		},
	}, nil
}

// ---- PutResourcePolicy ----

type putResourcePolicyResponse struct {
	XMLName xml.Name          `xml:"PutResourcePolicyResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Policy  xmlResourcePolicy `xml:"PutResourcePolicyResult>ResourcePolicy"`
}

func (h *Handler) handlePutResourcePolicy(vals url.Values) (any, error) {
	resourceArn := vals.Get("ResourceArn")
	policy := vals.Get("Policy")

	rp, err := h.Backend.PutResourcePolicy(resourceArn, policy)
	if err != nil {
		return nil, err
	}

	return &putResourcePolicyResponse{
		Xmlns: redshiftXMLNS,
		Policy: xmlResourcePolicy{
			ResourceArn: rp.ResourceArn,
			Policy:      rp.Policy,
		},
	}, nil
}

// ---- DeleteResourcePolicy ----

type deleteResourcePolicyResponse struct {
	XMLName   xml.Name `xml:"DeleteResourcePolicyResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

func (h *Handler) handleDeleteResourcePolicy(vals url.Values) (any, error) {
	resourceArn := vals.Get("ResourceArn")

	if err := h.Backend.DeleteResourcePolicy(resourceArn); err != nil {
		return nil, err
	}

	return &deleteResourcePolicyResponse{Xmlns: redshiftXMLNS}, nil
}
