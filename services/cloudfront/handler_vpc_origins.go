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
func vpcOriginResponseXML(origin *VpcOrigin) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<VpcOrigin xmlns="%s">`+
		`<Id>%s</Id>`+
		`<Arn>%s</Arn>`+
		`<VpcOriginEndpointConfig>`+
		`<Name>%s</Name>`+
		`</VpcOriginEndpointConfig>`+
		`</VpcOrigin>`,
		cfNS, origin.ID, origin.ARN, origin.Name)
}

// vpcOriginRequestFields is shared by Create and Update, whose real request
// shapes carry identical fields but different root element names
// (CreateVpcOriginRequest vs UpdateVpcOriginRequest; cloudfront@v1.67.4
// serializers.go). A prior single struct fixed the root to
// "VpcOriginRequest", which matched neither real root name, so
// xml.Unmarshal silently failed on every field for any real client.
type vpcOriginRequestFields struct {
	Name string  `xml:"VpcOriginEndpointConfig>Name"`
	Tags tagsXML `xml:"Tags"`
}

type createVpcOriginRequestXML struct {
	XMLName xml.Name `xml:"CreateVpcOriginRequest"`
	vpcOriginRequestFields
}

type updateVpcOriginRequestXML struct {
	XMLName xml.Name `xml:"UpdateVpcOriginRequest"`
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
		_ = xml.Unmarshal(body, &req)
	}

	if req.Name == "" {
		req.Name = generateID()
	}

	origin, createErr := h.Backend.CreateVpcOrigin(req.Name, tagsXMLToMap(req.Tags))
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
		_ = xml.Unmarshal(body, &req)
	}

	current, getErr := h.Backend.GetVpcOrigin(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	name := req.Name
	if name == "" {
		name = current.Name
	}

	origin, updateErr := h.Backend.UpdateVpcOrigin(id, name)
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
