package redshift

import (
	"encoding/xml"
	"net/url"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// ----- HSM -----

type hsmClientCertificateXML struct {
	HsmClientCertificateIdentifier string       `xml:"HsmClientCertificateIdentifier"`
	HsmClientCertificatePublicKey  string       `xml:"HsmClientCertificatePublicKey"`
	Tags                           []svcTags.KV `xml:"Tags>Tag,omitempty"`
}

type createHsmClientCertificateResponse struct {
	XMLName xml.Name                `xml:"CreateHsmClientCertificateResponse"`
	Xmlns   string                  `xml:"xmlns,attr"`
	Result  hsmClientCertificateXML `xml:"CreateHsmClientCertificateResult"`
}

func (h *Handler) handleCreateHsmClientCertificate(vals url.Values) (any, error) {
	id := vals.Get("HsmClientCertificateIdentifier")
	cert, err := h.Backend.CreateHsmClientCertificate(id, parseRedshiftTags(vals))
	if err != nil {
		return nil, err
	}

	return &createHsmClientCertificateResponse{
		Xmlns: redshiftXMLNS,
		Result: hsmClientCertificateXML{
			HsmClientCertificateIdentifier: cert.HsmClientCertificateIdentifier,
			HsmClientCertificatePublicKey:  cert.HsmClientCertificatePublicKey,
			Tags:                           tagMapToKVList(cert.Tags),
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
			Tags:                           tagMapToKVList(c.Tags),
		})
	}

	resp := &describeHsmClientCertificatesResponse{Xmlns: redshiftXMLNS}
	resp.Result.HsmClientCertificates = members

	return resp, nil
}

type hsmConfigurationXML struct {
	HsmConfigurationIdentifier string       `xml:"HsmConfigurationIdentifier"`
	Description                string       `xml:"Description"`
	HsmIPAddress               string       `xml:"HsmIPAddress"`
	HsmPartitionName           string       `xml:"HsmPartitionName"`
	Tags                       []svcTags.KV `xml:"Tags>Tag,omitempty"`
}

type createHsmConfigurationResponse struct {
	XMLName xml.Name            `xml:"CreateHsmConfigurationResponse"`
	Xmlns   string              `xml:"xmlns,attr"`
	Result  hsmConfigurationXML `xml:"CreateHsmConfigurationResult"`
}

// handleCreateHsmConfiguration implements CreateHsmConfiguration. Real
// CreateHsmConfigurationInput serializes the IP address param as "HsmIpAddress"
// (confirmed against awsAwsquery_serializeOpDocumentCreateHsmConfigurationInput in
// aws-sdk-go-v2/service/redshift@v1.65.4/serializers.go) -- a real SDK client never
// sends "HsmIPAddress", so the previous vals.Get("HsmIPAddress") silently dropped it.
func (h *Handler) handleCreateHsmConfiguration(vals url.Values) (any, error) {
	id := vals.Get("HsmConfigurationIdentifier")
	cfg, err := h.Backend.CreateHsmConfiguration(
		id,
		vals.Get("Description"),
		vals.Get("HsmIpAddress"),
		vals.Get("HsmPartitionName"),
		parseRedshiftTags(vals),
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
			Tags:                       tagMapToKVList(cfg.Tags),
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
			Tags:                       tagMapToKVList(c.Tags),
		})
	}

	resp := &describeHsmConfigurationsResponse{Xmlns: redshiftXMLNS}
	resp.Result.HsmConfigurations = members

	return resp, nil
}
