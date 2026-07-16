package redshift

import (
	"encoding/xml"
	"net/url"
)

// ----- HSM -----

type hsmClientCertificateXML struct {
	HsmClientCertificateIdentifier string `xml:"HsmClientCertificateIdentifier"`
	HsmClientCertificatePublicKey  string `xml:"HsmClientCertificatePublicKey"`
}

type createHsmClientCertificateResponse struct {
	XMLName xml.Name                `xml:"CreateHsmClientCertificateResponse"`
	Xmlns   string                  `xml:"xmlns,attr"`
	Result  hsmClientCertificateXML `xml:"CreateHsmClientCertificateResult"`
}

func (h *Handler) handleCreateHsmClientCertificate(vals url.Values) (any, error) {
	id := vals.Get("HsmClientCertificateIdentifier")
	cert, err := h.Backend.CreateHsmClientCertificate(id, nil)
	if err != nil {
		return nil, err
	}

	return &createHsmClientCertificateResponse{
		Xmlns: redshiftXMLNS,
		Result: hsmClientCertificateXML{
			HsmClientCertificateIdentifier: cert.HsmClientCertificateIdentifier,
			HsmClientCertificatePublicKey:  cert.HsmClientCertificatePublicKey,
		},
	}, nil
}

type deleteHsmClientCertificateResponse struct {
	XMLName xml.Name `xml:"DeleteHsmClientCertificateResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleDeleteHsmClientCertificate(vals url.Values) (any, error) {
	id := vals.Get("HsmClientCertificateIdentifier")
	if err := h.Backend.DeleteHsmClientCertificate(id); err != nil {
		return nil, err
	}

	return &deleteHsmClientCertificateResponse{Xmlns: redshiftXMLNS}, nil
}

type describeHsmClientCertificatesResponse struct {
	XMLName xml.Name `xml:"DescribeHsmClientCertificatesResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		HsmClientCertificates []hsmClientCertificateXML `xml:"HsmClientCertificates>HsmClientCertificate"`
	} `xml:"DescribeHsmClientCertificatesResult"`
}

func (h *Handler) handleDescribeHsmClientCertificates(vals url.Values) (any, error) {
	id := vals.Get("HsmClientCertificateIdentifier")
	certs, err := h.Backend.DescribeHsmClientCertificates(id)
	if err != nil {
		return nil, err
	}

	members := make([]hsmClientCertificateXML, 0, len(certs))

	for _, c := range certs {
		members = append(members, hsmClientCertificateXML{
			HsmClientCertificateIdentifier: c.HsmClientCertificateIdentifier,
			HsmClientCertificatePublicKey:  c.HsmClientCertificatePublicKey,
		})
	}

	resp := &describeHsmClientCertificatesResponse{Xmlns: redshiftXMLNS}
	resp.Result.HsmClientCertificates = members

	return resp, nil
}

type hsmConfigurationXML struct {
	HsmConfigurationIdentifier string `xml:"HsmConfigurationIdentifier"`
	Description                string `xml:"Description"`
	HsmIPAddress               string `xml:"HsmIPAddress"`
	HsmPartitionName           string `xml:"HsmPartitionName"`
}

type createHsmConfigurationResponse struct {
	XMLName xml.Name            `xml:"CreateHsmConfigurationResponse"`
	Xmlns   string              `xml:"xmlns,attr"`
	Result  hsmConfigurationXML `xml:"CreateHsmConfigurationResult"`
}

func (h *Handler) handleCreateHsmConfiguration(vals url.Values) (any, error) {
	id := vals.Get("HsmConfigurationIdentifier")
	cfg, err := h.Backend.CreateHsmConfiguration(
		id,
		vals.Get("Description"),
		vals.Get("HsmIPAddress"),
		vals.Get("HsmPartitionName"),
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &createHsmConfigurationResponse{
		Xmlns: redshiftXMLNS,
		Result: hsmConfigurationXML{
			HsmConfigurationIdentifier: cfg.HsmConfigurationIdentifier,
			Description:                cfg.Description,
			HsmIPAddress:               cfg.HsmIPAddress,
			HsmPartitionName:           cfg.HsmPartitionName,
		},
	}, nil
}

type deleteHsmConfigurationResponse struct {
	XMLName xml.Name `xml:"DeleteHsmConfigurationResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleDeleteHsmConfiguration(vals url.Values) (any, error) {
	id := vals.Get("HsmConfigurationIdentifier")
	if err := h.Backend.DeleteHsmConfiguration(id); err != nil {
		return nil, err
	}

	return &deleteHsmConfigurationResponse{Xmlns: redshiftXMLNS}, nil
}

type describeHsmConfigurationsResponse struct {
	XMLName xml.Name `xml:"DescribeHsmConfigurationsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		HsmConfigurations []hsmConfigurationXML `xml:"HsmConfigurations>HsmConfiguration"`
	} `xml:"DescribeHsmConfigurationsResult"`
}

func (h *Handler) handleDescribeHsmConfigurations(vals url.Values) (any, error) {
	id := vals.Get("HsmConfigurationIdentifier")
	cfgs, err := h.Backend.DescribeHsmConfigurations(id)
	if err != nil {
		return nil, err
	}

	members := make([]hsmConfigurationXML, 0, len(cfgs))

	for _, c := range cfgs {
		members = append(members, hsmConfigurationXML{
			HsmConfigurationIdentifier: c.HsmConfigurationIdentifier,
			Description:                c.Description,
			HsmIPAddress:               c.HsmIPAddress,
			HsmPartitionName:           c.HsmPartitionName,
		})
	}

	resp := &describeHsmConfigurationsResponse{Xmlns: redshiftXMLNS}
	resp.Result.HsmConfigurations = members

	return resp, nil
}
