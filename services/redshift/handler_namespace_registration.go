package redshift

import (
	"encoding/xml"
	"net/url"
)

// ----- Namespace Registration -----

type deregisterNamespaceResponse struct {
	XMLName xml.Name `xml:"DeregisterNamespaceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleDeregisterNamespace(_ url.Values) (any, error) {
	return &deregisterNamespaceResponse{Xmlns: redshiftXMLNS}, nil
}

type registerNamespaceResponse struct {
	XMLName xml.Name `xml:"RegisterNamespaceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleRegisterNamespace(_ url.Values) (any, error) {
	return &registerNamespaceResponse{Xmlns: redshiftXMLNS}, nil
}
