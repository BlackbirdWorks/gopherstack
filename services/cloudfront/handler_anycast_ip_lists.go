package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

type anycastIPListRequestXML struct {
	// Root element is CreateAnycastIpListRequest, not AnycastIPListRequest --
	// verified against cloudfront@v1.67.4 serializers.go
	// (awsRestxml_serializeOpCreateAnycastIpList's StartElement.Local).
	XMLName xml.Name `xml:"CreateAnycastIpListRequest"`
	Name    string   `xml:"Name"`
	Tags    []tagXML `xml:"Tags>Items>Tag"`
	// Wire element is IpCount, not IPCount (cloudfront@v1.67.4 serializers.go
	// awsRestxml_serializeOpDocumentCreateAnycastIpListInput) -- encoding/xml
	// unmarshal matches element names case-sensitively on the request path.
	IPCount int32 `xml:"IpCount"`
}

func (h *Handler) handleCreateAnycastIPList(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req anycastIPListRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid AnycastIPListRequest XML"),
			)
		}
	}

	tags := make(map[string]string, len(req.Tags))
	for _, tag := range req.Tags {
		tags[tag.Key] = tag.Value
	}

	list, createErr := h.Backend.CreateAnycastIPList(req.Name, req.IPCount, tags)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	var ips strings.Builder
	for _, ip := range list.AnycastIPs {
		fmt.Fprintf(&ips, `<IpAddress>%s</IpAddress>`, ip)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<AnycastIPList xmlns="%s">`+
		`<Id>%s</Id>`+
		`<ARN>%s</ARN>`+
		`<Name>%s</Name>`+
		`<Status>%s</Status>`+
		`<IPCount>%d</IPCount>`+
		`<AnycastIps>%s</AnycastIps>`+
		`</AnycastIPList>`,
		cfNS, list.ID, list.ARN, list.Name, list.Status, list.IPCount, ips.String())

	c.Response().Header().Set("Location", cfPathPrefix+"anycast-ip-list/"+list.ID)
	c.Response().Header().Set("ETag", list.ETag)

	return xmlResp(c, http.StatusCreated, resp)
}

func anycastIPListXML(ns string, list *AnycastIPList) string {
	var ips strings.Builder
	for _, ip := range list.AnycastIPs {
		fmt.Fprintf(&ips, `<IpAddress>%s</IpAddress>`, ip)
	}

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<AnycastIpList xmlns="%s">`+
		`<Id>%s</Id><ARN>%s</ARN><Name>%s</Name><Status>%s</Status><IpCount>%d</IpCount>`+
		`<AnycastIps>%s</AnycastIps>`+
		`</AnycastIpList>`,
		ns, list.ID, list.ARN, list.Name, list.Status, list.IPCount, ips.String())
}

// anycastIPListPreconditionFailedXML is the shared If-Match error body for anycast IP list
// update/delete operations.
func anycastIPListPreconditionFailedXML() string {
	return cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current anycast IP list ETag")
}

func (h *Handler) handleGetAnycastIPList(c *echo.Context, id string) error {
	list, err := h.Backend.GetAnycastIPList(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", list.ETag)

	return xmlResp(c, http.StatusOK, anycastIPListXML(cfNS, list))
}

func (h *Handler) handleListAnycastIPLists(c *echo.Context) error {
	items := h.Backend.ListAnycastIPLists()

	type ailSummary struct {
		XMLName xml.Name `xml:"AnycastIpListSummary"`
		ID      string   `xml:"Id"`
		Name    string   `xml:"Name"`
		Status  string   `xml:"Status"`
	}
	type ailList struct {
		XMLName  xml.Name     `xml:"AnycastIpLists"`
		XMLNS    string       `xml:"xmlns,attr"`
		Items    []ailSummary `xml:"Items>AnycastIpListSummary"`
		Quantity int          `xml:"Quantity"`
	}
	summaries := make([]ailSummary, 0, len(items))
	for _, ail := range items {
		summaries = append(summaries, ailSummary{ID: ail.ID, Name: ail.Name, Status: ail.Status})
	}
	list := ailList{XMLNS: cfNS, Quantity: len(summaries), Items: summaries}
	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleUpdateAnycastIPList(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetAnycastIPList(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if ifMatch := c.Request().Header.Get("If-Match"); ifMatch != "" && ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed, anycastIPListPreconditionFailedXML())
	}

	var req struct {
		XMLName xml.Name `xml:"AnycastIpListConfig"`
		IPCount int32    `xml:"IpCount"`
	}
	body, _ := readBody(c)

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}
	list, updateErr := h.Backend.UpdateAnycastIPList(id, req.IPCount)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", list.ETag)

	return xmlResp(c, http.StatusOK, anycastIPListXML(cfNS, list))
}

func (h *Handler) handleDeleteAnycastIPList(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetAnycastIPList(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if ifMatch := c.Request().Header.Get("If-Match"); ifMatch != "" && ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed, anycastIPListPreconditionFailedXML())
	}

	if err := h.Backend.DeleteAnycastIPList(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// ListDistributionsBy* handlers
// ---------------------------------------------------------------------------
