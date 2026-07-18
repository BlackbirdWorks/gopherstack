package cloudformation

import (
	"encoding/xml"
	"net/url"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

func (h *Handler) handleListExports(form url.Values, c *echo.Context) error {
	nextToken := form.Get("NextToken")

	p, err := h.Backend.ListExports(nextToken)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type exportXML struct {
		ExportingStackID string `xml:"ExportingStackId"`
		Name             string `xml:"Name"`
		Value            string `xml:"Value"`
	}
	members := make([]exportXML, 0, len(p.Data))
	for _, exp := range p.Data {
		members = append(members, exportXML(exp))
	}

	type listResult struct {
		NextToken string      `xml:"NextToken,omitempty"`
		Exports   []exportXML `xml:"Exports>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListExportsResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListExportsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    listResult{Exports: members, NextToken: p.Next},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleListImports(form url.Values, c *echo.Context) error {
	exportName := form.Get("ExportName")
	nextToken := form.Get("NextToken")

	if exportName == "" {
		return h.xmlError(c, "ValidationError", "ExportName is required")
	}

	p, err := h.Backend.ListImports(exportName, nextToken)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type listResult struct {
		NextToken string   `xml:"NextToken,omitempty"`
		Imports   []string `xml:"Imports>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListImportsResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListImportsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    listResult{Imports: p.Data, NextToken: p.Next},
		RequestID: uuid.New().String(),
	})
}
