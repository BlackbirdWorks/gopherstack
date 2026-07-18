package elbv2

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

// parseCertArns extracts certificate ARNs from indexed form parameters.
func parseCertArns(vals url.Values) []string {
	arns := make([]string, 0)
	for i := 1; ; i++ {
		c := vals.Get(fmt.Sprintf("Certificates.member.%d.CertificateArn", i))
		if c == "" {
			break
		}

		arns = append(arns, c)
	}

	return arns
}

func (h *Handler) handleAddListenerCertificates(vals url.Values) (any, error) {
	listenerArn := vals.Get("ListenerArn")
	if listenerArn == "" {
		return nil, fmt.Errorf("%w: ListenerArn is required", ErrInvalidParameter)
	}

	certArns := parseCertArns(vals)
	if len(certArns) == 0 {
		return nil, fmt.Errorf("%w: at least one certificate ARN is required", ErrInvalidParameter)
	}

	certs := parseCerts(vals)
	if err := h.Backend.AddListenerCertificates(listenerArn, certs); err != nil {
		return nil, err
	}

	members := make([]xmlListenerCertificate, 0, len(certs))
	for _, c := range certs {
		members = append(members, xmlListenerCertificate(c))
	}

	return &addListenerCertificatesResponse{
		Xmlns: elbv2XMLNS,
		Result: addListenerCertificatesResult{
			Certificates: xmlListenerCertificateList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-add-listener-certs"},
	}, nil
}

func (h *Handler) handleDescribeListenerCertificates(vals url.Values) (any, error) {
	listenerArn := vals.Get("ListenerArn")
	if listenerArn == "" {
		return nil, fmt.Errorf("%w: ListenerArn is required", ErrInvalidParameter)
	}

	certs, err := h.Backend.DescribeListenerCertificates(listenerArn)
	if err != nil {
		return nil, err
	}

	members := make([]xmlListenerCertificate, 0, len(certs))
	for _, c := range certs {
		members = append(members, xmlListenerCertificate(c))
	}

	return &describeListenerCertificatesResponse{
		Xmlns: elbv2XMLNS,
		Result: describeListenerCertificatesResult{
			Certificates: xmlListenerCertificateList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-listener-certs"},
	}, nil
}

func (h *Handler) handleRemoveListenerCertificates(vals url.Values) (any, error) {
	listenerArn := vals.Get("ListenerArn")
	if listenerArn == "" {
		return nil, fmt.Errorf("%w: ListenerArn is required", ErrInvalidParameter)
	}

	certArns := parseCertArns(vals)
	if len(certArns) == 0 {
		return nil, fmt.Errorf("%w: at least one certificate ARN is required", ErrInvalidParameter)
	}

	if err := h.Backend.RemoveListenerCertificates(listenerArn, certArns); err != nil {
		return nil, err
	}

	return &removeListenerCertificatesResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-remove-listener-certs"},
	}, nil
}

type xmlListenerCertificate struct {
	CertificateArn string `xml:"CertificateArn"`
	IsDefault      bool   `xml:"IsDefault,omitempty"`
}

type xmlListenerCertificateList struct {
	Members []xmlListenerCertificate `xml:"member"`
}

type addListenerCertificatesResult struct {
	Certificates xmlListenerCertificateList `xml:"Certificates"`
}

type addListenerCertificatesResponse struct {
	XMLName          xml.Name                      `xml:"AddListenerCertificatesResponse"`
	Xmlns            string                        `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata           `xml:"ResponseMetadata"`
	Result           addListenerCertificatesResult `xml:"AddListenerCertificatesResult"`
}

type describeListenerCertificatesResult struct {
	NextMarker   string                     `xml:"NextMarker,omitempty"`
	Certificates xmlListenerCertificateList `xml:"Certificates"`
}

type describeListenerCertificatesResponse struct {
	XMLName          xml.Name                           `xml:"DescribeListenerCertificatesResponse"`
	Xmlns            string                             `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                `xml:"ResponseMetadata"`
	Result           describeListenerCertificatesResult `xml:"DescribeListenerCertificatesResult"`
}

type removeListenerCertificatesResponse struct {
	XMLName          xml.Name            `xml:"RemoveListenerCertificatesResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}
