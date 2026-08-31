package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

// ipamCidrConfigXML mirrors types.IpamCidrConfig's wire shape (cloudfront@v1.67.4
// serializers.go:17332-17376, deserializers.go:46696-46752): a flat IpamCidrConfig element,
// no Items/Quantity wrapper.
type ipamCidrConfigXML struct {
	AnycastIP   string `xml:"AnycastIp"`
	Cidr        string `xml:"Cidr"`
	IpamPoolARN string `xml:"IpamPoolArn"`
	Status      string `xml:"Status"`
}

// ailIpamConfigXML mirrors the AnycastIpListSummary output's optional IpamConfig element
// (cloudfront@v1.67.4 types/types.go:3757-3771, deserializers.go:46852-46895): IpamCidrConfigs
// is a flat list directly under IpamCidrConfigs, no Items wrapper, matching
// anycastIPListIpamConfigXML's shape for the singular GetAnycastIpList response.
type ailIpamConfigXML struct {
	IpamCidrConfigs []ipamCidrConfigXML `xml:"IpamCidrConfigs>IpamCidrConfig"`
	Quantity        int                 `xml:"Quantity"`
}

func toIpamCidrConfigs(xs []ipamCidrConfigXML) []IpamCidrConfig {
	if xs == nil {
		return nil
	}

	out := make([]IpamCidrConfig, 0, len(xs))
	for _, x := range xs {
		out = append(out, IpamCidrConfig{
			Cidr: x.Cidr, IpamPoolARN: x.IpamPoolARN, AnycastIP: x.AnycastIP, Status: x.Status,
		})
	}

	return out
}

// anycastIPListIpamConfigXML renders the AnycastIpList output's optional IpamConfig element
// (cloudfront@v1.67.4 types/types.go:3757-3771, deserializers.go:46852-46895): omitted entirely
// when there are no configs, since IpamConfig is an optional pointer on the real type.
func anycastIPListIpamConfigXML(configs []IpamCidrConfig) string {
	if len(configs) == 0 {
		return ""
	}

	var items strings.Builder
	for _, cfg := range configs {
		fmt.Fprintf(&items,
			`<IpamCidrConfig><AnycastIp>%s</AnycastIp><Cidr>%s</Cidr>`+
				`<IpamPoolArn>%s</IpamPoolArn><Status>%s</Status></IpamCidrConfig>`,
			cfg.AnycastIP, cfg.Cidr, cfg.IpamPoolARN, cfg.Status)
	}

	return fmt.Sprintf(`<IpamConfig><IpamCidrConfigs>%s</IpamCidrConfigs><Quantity>%d</Quantity></IpamConfig>`,
		items.String(), len(configs))
}

type anycastIPListRequestXML struct {
	// Root element is CreateAnycastIpListRequest, not AnycastIPListRequest --
	// verified against cloudfront@v1.67.4 serializers.go
	// (awsRestxml_serializeOpCreateAnycastIpList's StartElement.Local).
	XMLName         xml.Name            `xml:"CreateAnycastIpListRequest"`
	Name            string              `xml:"Name"`
	Tags            []tagXML            `xml:"Tags>Items>Tag"`
	IpamCidrConfigs []ipamCidrConfigXML `xml:"IpamCidrConfigs>IpamCidrConfig"`
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

	list, createErr := h.Backend.CreateAnycastIPList(
		req.Name, req.IPCount, toIpamCidrConfigs(req.IpamCidrConfigs), tags,
	)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	var ips strings.Builder
	for _, ip := range list.AnycastIPs {
		// Wire element is AnycastIp, not IpAddress (cloudfront@v1.67.4 deserializers.go:34541,
		// awsRestxml_deserializeDocumentAnycastIps).
		fmt.Fprintf(&ips, `<AnycastIp>%s</AnycastIp>`, ip)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<AnycastIPList xmlns="%s">`+
		`<Id>%s</Id>`+
		`<ARN>%s</ARN>`+
		`<Name>%s</Name>`+
		`<Status>%s</Status>`+
		`<IPCount>%d</IPCount>`+
		`<LastModifiedTime>%s</LastModifiedTime>`+
		`%s`+
		`<AnycastIps>%s</AnycastIps>`+
		`</AnycastIPList>`,
		cfNS, list.ID, list.ARN, list.Name, list.Status, list.IPCount, list.LastModifiedTime,
		anycastIPListIpamConfigXML(list.IpamCidrConfigs), ips.String())

	c.Response().Header().Set("Location", cfPathPrefix+"anycast-ip-list/"+list.ID)
	c.Response().Header().Set("ETag", list.ETag)

	return xmlResp(c, http.StatusCreated, resp)
}

func anycastIPListXML(ns string, list *AnycastIPList) string {
	var ips strings.Builder
	for _, ip := range list.AnycastIPs {
		fmt.Fprintf(&ips, `<AnycastIp>%s</AnycastIp>`, ip)
	}

	var ipAddressType string
	if list.IPAddressType != "" {
		ipAddressType = fmt.Sprintf(`<IpAddressType>%s</IpAddressType>`, list.IPAddressType)
	}

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<AnycastIpList xmlns="%s">`+
		`<Id>%s</Id><ARN>%s</ARN><Name>%s</Name><Status>%s</Status><IpCount>%d</IpCount>%s`+
		`<LastModifiedTime>%s</LastModifiedTime>%s`+
		`<AnycastIps>%s</AnycastIps>`+
		`</AnycastIpList>`,
		ns, list.ID, list.ARN, list.Name, list.Status, list.IPCount, ipAddressType,
		list.LastModifiedTime, anycastIPListIpamConfigXML(list.IpamCidrConfigs), ips.String())
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

// handleListAnycastIPLists paginates the same way handleListDistributions does: Marker is the
// ID of the last item on the previous page, MaxItems caps the page (default/max maxItems), and
// IsTruncated/NextMarker are derived from what's left after the cut (cloudfront@v1.67.4
// types/types.go:214-249, AnycastIpListCollection).
func (h *Handler) handleListAnycastIPLists(c *echo.Context) error {
	items := h.Backend.ListAnycastIPLists()

	marker := c.QueryParam("Marker")
	pageSize := maxItems
	if s := c.QueryParam("MaxItems"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 && n < maxItems {
			pageSize = n
		}
	}

	if marker != "" {
		cut := 0
		for cut < len(items) && items[cut].ID <= marker {
			cut++
		}
		items = items[cut:]
	}

	isTruncated := len(items) > pageSize
	if isTruncated {
		items = items[:pageSize]
	}

	nextMarker := ""
	if isTruncated && len(items) > 0 {
		nextMarker = items[len(items)-1].ID
	}

	type ailSummary struct {
		IpamConfig       *ailIpamConfigXML `xml:"IpamConfig,omitempty"`
		XMLName          xml.Name          `xml:"AnycastIpListSummary"`
		ID               string            `xml:"Id"`
		ARN              string            `xml:"Arn"`
		Name             string            `xml:"Name"`
		Status           string            `xml:"Status"`
		ETag             string            `xml:"ETag,omitempty"`
		IPAddressType    string            `xml:"IpAddressType,omitempty"`
		LastModifiedTime string            `xml:"LastModifiedTime"`
		IPCount          int32             `xml:"IpCount"`
	}
	type ailList struct {
		XMLName     xml.Name     `xml:"AnycastIpLists"`
		XMLNS       string       `xml:"xmlns,attr"`
		NextMarker  string       `xml:"NextMarker,omitempty"`
		Items       []ailSummary `xml:"Items>AnycastIpListSummary"`
		MaxItems    int          `xml:"MaxItems"`
		Quantity    int          `xml:"Quantity"`
		IsTruncated bool         `xml:"IsTruncated"`
	}
	summaries := make([]ailSummary, 0, len(items))
	for _, ail := range items {
		s := ailSummary{
			ID: ail.ID, ARN: ail.ARN, Name: ail.Name, Status: ail.Status,
			ETag: ail.ETag, IPAddressType: ail.IPAddressType,
			LastModifiedTime: ail.LastModifiedTime, IPCount: ail.IPCount,
		}
		if len(ail.IpamCidrConfigs) > 0 {
			cidrs := make([]ipamCidrConfigXML, 0, len(ail.IpamCidrConfigs))
			for _, cfg := range ail.IpamCidrConfigs {
				cidrs = append(cidrs, ipamCidrConfigXML{
					AnycastIP: cfg.AnycastIP, Cidr: cfg.Cidr, IpamPoolARN: cfg.IpamPoolARN, Status: cfg.Status,
				})
			}
			s.IpamConfig = &ailIpamConfigXML{IpamCidrConfigs: cidrs, Quantity: len(cidrs)}
		}
		summaries = append(summaries, s)
	}
	list := ailList{
		XMLNS: cfNS, MaxItems: pageSize, Quantity: len(summaries), Items: summaries,
		IsTruncated: isTruncated, NextMarker: nextMarker,
	}
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

	// Root element is UpdateAnycastIpListRequest; settable body fields are IpAddressType and
	// IpamCidrConfigs -- cloudfront@v1.67.4 serializers.go:12012-12082 (there is no IpCount
	// member).
	var req struct {
		XMLName         xml.Name            `xml:"UpdateAnycastIpListRequest"`
		IPAddressType   string              `xml:"IpAddressType"`
		IpamCidrConfigs []ipamCidrConfigXML `xml:"IpamCidrConfigs>IpamCidrConfig"`
	}
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid UpdateAnycastIpListRequest XML"),
			)
		}
	}
	list, updateErr := h.Backend.UpdateAnycastIPList(id, req.IPAddressType, toIpamCidrConfigs(req.IpamCidrConfigs))
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
