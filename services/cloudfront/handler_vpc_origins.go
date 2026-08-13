package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

// vpcOriginResponseXML matches the real VpcOrigin shape (cloudfront@v1.67.4
// types.VpcOrigin): Id/Arn are siblings of VpcOriginEndpointConfig, not
// nested inside it -- a real client reading Arn from inside
// VpcOriginEndpointConfig (as this previously did) always got nil.
//
// VpcOriginEndpointConfig.Arn (types.go:6989-6992) is the ARN of the VPC
// interface endpoint or load balancer this origin routes to -- distinct from
// the top-level Arn, which is the VPC origin resource's own ARN.
func vpcOriginResponseXML(origin *VpcOrigin) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<VpcOrigin xmlns="%s">`+
		`<Id>%s</Id>`+
		`<Arn>%s</Arn>`+
		`<VpcOriginEndpointConfig>`+
		`<Name>%s</Name>`+
		`<Arn>%s</Arn>`+
		`<HTTPPort>%d</HTTPPort>`+
		`<HTTPSPort>%d</HTTPSPort>`+
		`<OriginProtocolPolicy>%s</OriginProtocolPolicy>`+
		`</VpcOriginEndpointConfig>`+
		`</VpcOrigin>`,
		cfNS, origin.ID, origin.ARN,
		origin.Name, origin.EndpointArn, origin.HTTPPort, origin.HTTPSPort, origin.OriginProtocolPolicy)
}

// vpcOriginRequestFields is shared by Create and Update: both carry the same
// VpcOriginEndpointConfig field set, but at different XML depths, because their
// real root elements differ in shape (cloudfront@v1.67.4 serializers.go). Create's
// root is CreateVpcOriginRequest, a wrapper with VpcOriginEndpointConfig and Tags as
// children (awsRestxml_serializeOpDocumentCreateVpcOriginInput). Update's root IS
// VpcOriginEndpointConfig itself -- Id/IfMatch travel as URI/header, and the payload
// has no wrapping element at all (awsRestxml_serializeOpUpdateVpcOrigin's payloadRoot
// is "VpcOriginEndpointConfig", not "UpdateVpcOriginRequest"). The previous Update
// struct used a root of "UpdateVpcOriginRequest", which never matched a real client's
// root element, so xml.Unmarshal errored on the whole body and the update silently
// no-opped (masked by the existing tests hand-crafting bodies with that same wrong
// root).
//
// Arn, HTTPPort, HTTPSPort, and OriginProtocolPolicy are also required members
// of VpcOriginEndpointConfig (types.go:6987-7018) that a prior version of this
// struct dropped entirely, so Arn -- the ARN of the VPC endpoint or ALB the
// origin actually routes to -- was silently discarded on every real request.
type vpcOriginRequestFields struct {
	Name                 string `xml:"Name"`
	Arn                  string `xml:"Arn"`
	OriginProtocolPolicy string `xml:"OriginProtocolPolicy"`
	HTTPPort             int32  `xml:"HTTPPort"`
	HTTPSPort            int32  `xml:"HTTPSPort"`
}

// endpointConfig converts the parsed request fields into a VpcOriginEndpointConfig.
func (f vpcOriginRequestFields) endpointConfig() VpcOriginEndpointConfig {
	return VpcOriginEndpointConfig(f)
}

type createVpcOriginRequestXML struct {
	XMLName                 xml.Name               `xml:"CreateVpcOriginRequest"`
	VpcOriginEndpointConfig vpcOriginRequestFields `xml:"VpcOriginEndpointConfig"`
	Tags                    tagsXML                `xml:"Tags"`
}

// updateVpcOriginRequestXML matches UpdateVpcOriginInput (cloudfront@v1.67.4
// serializers.go: awsRestxml_serializeOpUpdateVpcOrigin) -- the root element itself
// is VpcOriginEndpointConfig, with Name/Arn/OriginProtocolPolicy/HTTPPort/HTTPSPort
// as its direct children, not nested one level deeper under another element of the
// same name.
type updateVpcOriginRequestXML struct {
	XMLName xml.Name `xml:"VpcOriginEndpointConfig"`
	vpcOriginRequestFields
}

func (h *Handler) handleCreateVpcOrigin(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req createVpcOriginRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid CreateVpcOriginRequest XML"))
		}
	}

	if req.VpcOriginEndpointConfig.Name == "" {
		req.VpcOriginEndpointConfig.Name = generateID()
	}

	origin, createErr := h.Backend.CreateVpcOrigin(req.VpcOriginEndpointConfig.endpointConfig(), tagsXMLToMap(req.Tags))
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", origin.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"vpc-origin/"+origin.ID)

	return xmlResp(c, http.StatusCreated, vpcOriginResponseXML(origin))
}

func (h *Handler) handleGetVpcOrigin(c *echo.Context, id string) error {
	origin, err := h.Backend.GetVpcOrigin(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", origin.ETag)

	return xmlResp(c, http.StatusOK, vpcOriginResponseXML(origin))
}

//nolint:dupl // list handlers for different CloudFront resource types share XML list structure
func (h *Handler) handleListVpcOrigins(c *echo.Context) error {
	items := h.Backend.ListVpcOrigins()

	type vpcSummaryXML struct {
		XMLName xml.Name `xml:"VpcOriginSummary"`
		ID      string   `xml:"Id"`
		ARN     string   `xml:"ARN"`
		Name    string   `xml:"Name"`
	}

	type vpcListXML struct {
		XMLName     xml.Name        `xml:"VpcOriginList"`
		XMLNS       string          `xml:"xmlns,attr"`
		Items       []vpcSummaryXML `xml:"Items>VpcOriginSummary"`
		MaxItems    int             `xml:"MaxItems"`
		Quantity    int             `xml:"Quantity"`
		IsTruncated bool            `xml:"IsTruncated"`
	}

	summaries := make([]vpcSummaryXML, 0, len(items))
	for _, origin := range items {
		summaries = append(summaries, vpcSummaryXML{ID: origin.ID, ARN: origin.ARN, Name: origin.Name})
	}

	list := vpcListXML{XMLNS: cfNS, MaxItems: maxItems, Quantity: len(summaries), Items: summaries}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleUpdateVpcOrigin(c *echo.Context, id string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req updateVpcOriginRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid VpcOriginEndpointConfig XML"))
		}
	}

	current, getErr := h.Backend.GetVpcOrigin(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	cfg := req.endpointConfig()
	if cfg.Name == "" {
		cfg.Name = current.Name
	}
	if cfg.Arn == "" {
		cfg.Arn = current.EndpointArn
	}
	if cfg.OriginProtocolPolicy == "" {
		cfg.OriginProtocolPolicy = current.OriginProtocolPolicy
	}
	if cfg.HTTPPort == 0 {
		cfg.HTTPPort = current.HTTPPort
	}
	if cfg.HTTPSPort == 0 {
		cfg.HTTPSPort = current.HTTPSPort
	}

	origin, updateErr := h.Backend.UpdateVpcOrigin(id, cfg)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", origin.ETag)

	return xmlResp(c, http.StatusOK, vpcOriginResponseXML(origin))
}

func (h *Handler) handleDeleteVpcOrigin(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetVpcOrigin(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current VpcOrigin ETag"))
	}

	if err := h.Backend.DeleteVpcOrigin(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
