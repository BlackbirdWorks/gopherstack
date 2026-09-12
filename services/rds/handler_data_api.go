package rds

import (
	"encoding/xml"
	"net/url"
)

type enableHTTPEndpointResponse struct {
	XMLName     xml.Name `xml:"EnableHttpEndpointResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	HTTPEnabled bool     `xml:"EnableHttpEndpointResult>HttpEndpointEnabled"`
	ResourceArn string   `xml:"EnableHttpEndpointResult>ResourceArn,omitempty"`
}

type disableHTTPEndpointResponse struct {
	XMLName     xml.Name `xml:"DisableHttpEndpointResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	HTTPEnabled bool     `xml:"DisableHttpEndpointResult>HttpEndpointEnabled"`
	ResourceArn string   `xml:"DisableHttpEndpointResult>ResourceArn,omitempty"`
}

func (h *Handler) handleEnableHTTPEndpoint(vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceArn")
	if err := h.Backend.EnableHTTPEndpoint(resourceARN); err != nil {
		return nil, err
	}

	return &enableHTTPEndpointResponse{
		Xmlns:       rdsXMLNS,
		HTTPEnabled: true,
		ResourceArn: resourceARN,
	}, nil
}

func (h *Handler) handleDisableHTTPEndpoint(vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceArn")
	if err := h.Backend.DisableHTTPEndpoint(resourceARN); err != nil {
		return nil, err
	}

	return &disableHTTPEndpointResponse{
		Xmlns:       rdsXMLNS,
		HTTPEnabled: false,
		ResourceArn: resourceARN,
	}, nil
}
