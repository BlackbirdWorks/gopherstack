package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

type oaiConfigXML struct {
	XMLName         xml.Name `xml:"CloudFrontOriginAccessIdentityConfig"`
	CallerReference string   `xml:"CallerReference"`
	Comment         string   `xml:"Comment"`
}

type oaiResponseXML struct {
	XMLName           xml.Name     `xml:"CloudFrontOriginAccessIdentity"`
	XMLNS             string       `xml:"xmlns,attr"`
	ID                string       `xml:"Id"`
	S3CanonicalUserID string       `xml:"S3CanonicalUserId"`
	Config            oaiConfigXML `xml:"CloudFrontOriginAccessIdentityConfig"`
}

type oaiSummary struct {
	XMLName           xml.Name `xml:"CloudFrontOriginAccessIdentitySummary"`
	ID                string   `xml:"Id"`
	S3CanonicalUserID string   `xml:"S3CanonicalUserId"`
	Comment           string   `xml:"Comment"`
}

type oaiList struct {
	XMLName     xml.Name     `xml:"CloudFrontOriginAccessIdentityList"`
	XMLNS       string       `xml:"xmlns,attr"`
	Items       []oaiSummary `xml:"Items>CloudFrontOriginAccessIdentitySummary"`
	MaxItems    int          `xml:"MaxItems"`
	Quantity    int          `xml:"Quantity"`
	IsTruncated bool         `xml:"IsTruncated"`
}

func (h *Handler) handleCreateOAI(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var cfg oaiConfigXML
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		return xmlResp(
			c,
			http.StatusBadRequest,
			cfErrorXML("MalformedXML", "invalid OAI config XML"),
		)
	}

	oai, createErr := h.Backend.CreateOAI(cfg.CallerReference, cfg.Comment)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("Location", cfPathPrefix+"origin-access-identity/cloudfront/"+oai.ID)
	c.Response().Header().Set("ETag", oai.ETag)

	resp := oaiResponseXML{
		XMLNS:             cfNS,
		ID:                oai.ID,
		S3CanonicalUserID: oai.S3CanonicalUserID,
		Config: oaiConfigXML{
			CallerReference: oai.CallerReference,
			Comment:         oai.Comment,
		},
	}

	out, xmlErr := xml.Marshal(resp)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusCreated, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleGetOAI(c *echo.Context, id string) error {
	oai, err := h.Backend.GetOAI(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", oai.ETag)

	resp := oaiResponseXML{
		XMLNS:             cfNS,
		ID:                oai.ID,
		S3CanonicalUserID: oai.S3CanonicalUserID,
		Config: oaiConfigXML{
			CallerReference: oai.CallerReference,
			Comment:         oai.Comment,
		},
	}

	out, xmlErr := xml.Marshal(resp)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleListOAIs(c *echo.Context) error {
	oais := h.Backend.ListOAIs()

	summaries := make([]oaiSummary, 0, len(oais))
	for _, oai := range oais {
		summaries = append(summaries, oaiSummary{
			ID:                oai.ID,
			S3CanonicalUserID: oai.S3CanonicalUserID,
			Comment:           oai.Comment,
		})
	}

	list := oaiList{
		XMLNS:    cfNS,
		MaxItems: maxItems,
		Quantity: len(summaries),
		Items:    summaries,
	}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleDeleteOAI(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetOAI(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current OAI ETag"))
	}

	if err := h.Backend.DeleteOAI(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetOAIConfig(c *echo.Context, id string) error {
	oai, err := h.Backend.GetOAI(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", oai.ETag)

	resp := oaiConfigXML{
		CallerReference: oai.CallerReference,
		Comment:         oai.Comment,
	}

	out, xmlErr := xml.Marshal(resp)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleUpdateOAI(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetOAI(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current OAI ETag"))
	}

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req oaiConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid CloudFrontOriginAccessIdentityConfig XML"),
			)
		}
	}

	oai, updateErr := h.Backend.UpdateOAI(id, req.Comment)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", oai.ETag)

	resp := oaiResponseXML{
		XMLNS:             cfNS,
		ID:                oai.ID,
		S3CanonicalUserID: oai.S3CanonicalUserID,
		Config: oaiConfigXML{
			CallerReference: oai.CallerReference,
			Comment:         oai.Comment,
		},
	}

	out, xmlErr := xml.Marshal(resp)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

// --- Tagging handlers ---

type oacConfigXML struct {
	XMLName         xml.Name `xml:"OriginAccessControlConfig"`
	Name            string   `xml:"Name"`
	Description     string   `xml:"Description"`
	OriginType      string   `xml:"OriginAccessControlOriginType"`
	SigningBehavior string   `xml:"SigningBehavior"`
	SigningProtocol string   `xml:"SigningProtocol"`
}

func (h *Handler) handleCreateOriginAccessControl(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req oacConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			const oacMsg = "invalid OriginAccessControlConfig XML"

			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", oacMsg))
		}
	}

	oac, createErr := h.Backend.CreateOriginAccessControl(
		req.Name, req.Description, req.OriginType, req.SigningBehavior, req.SigningProtocol,
	)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", oac.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"origin-access-control/"+oac.ID)

	return xmlResp(c, http.StatusCreated, oacResponseXML(oac))
}

func (h *Handler) handleGetOriginAccessControl(c *echo.Context, id string) error {
	oac, err := h.Backend.GetOriginAccessControl(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", oac.ETag)

	return xmlResp(c, http.StatusOK, oacResponseXML(oac))
}

func (h *Handler) handleGetOriginAccessControlConfig(c *echo.Context, id string) error {
	oac, err := h.Backend.GetOriginAccessControl(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", oac.ETag)

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<OriginAccessControlConfig xmlns="%s">`+
		`<Name>%s</Name>`+
		`<Description>%s</Description>`+
		`<OriginAccessControlOriginType>%s</OriginAccessControlOriginType>`+
		`<SigningBehavior>%s</SigningBehavior>`+
		`<SigningProtocol>%s</SigningProtocol>`+
		`</OriginAccessControlConfig>`,
		cfNS, oac.Name, oac.Description, oac.OriginType, oac.SigningBehavior, oac.SigningProtocol)

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handleListOriginAccessControls(c *echo.Context) error {
	oacs := h.Backend.ListOriginAccessControls()

	var sb strings.Builder

	for _, oac := range oacs {
		fmt.Fprintf(
			&sb,
			`<OriginAccessControlSummary>`+
				`<Id>%s</Id>`+
				`<Name>%s</Name>`+
				`<Description>%s</Description>`+
				`<OriginAccessControlOriginType>%s</OriginAccessControlOriginType>`+
				`<SigningBehavior>%s</SigningBehavior>`+
				`<SigningProtocol>%s</SigningProtocol>`+
				`</OriginAccessControlSummary>`,
			oac.ID,
			oac.Name,
			oac.Description,
			oac.OriginType,
			oac.SigningBehavior,
			oac.SigningProtocol,
		)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<OriginAccessControlList xmlns="%s">`+
		`<MaxItems>%d</MaxItems>`+
		`<Quantity>%d</Quantity>`+
		`<Items>%s</Items>`+
		`</OriginAccessControlList>`,
		cfNS, maxItems, len(oacs), sb.String())

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handleUpdateOriginAccessControl(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetOriginAccessControl(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current OAC ETag"))
	}

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req oacConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			const oacMsg = "invalid OriginAccessControlConfig XML"

			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", oacMsg))
		}
	}

	oac, updateErr := h.Backend.UpdateOriginAccessControl(
		id, req.Name, req.Description, req.OriginType, req.SigningBehavior, req.SigningProtocol,
	)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", oac.ETag)

	return xmlResp(c, http.StatusOK, oacResponseXML(oac))
}

func (h *Handler) handleDeleteOriginAccessControl(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetOriginAccessControl(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current OAC ETag"))
	}

	if err := h.Backend.DeleteOriginAccessControl(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func oacResponseXML(oac *OriginAccessControl) string {
	return fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<OriginAccessControl xmlns="%s">`+
			`<Id>%s</Id>`+
			`<OriginAccessControlConfig>`+
			`<Name>%s</Name>`+
			`<Description>%s</Description>`+
			`<OriginAccessControlOriginType>%s</OriginAccessControlOriginType>`+
			`<SigningBehavior>%s</SigningBehavior>`+
			`<SigningProtocol>%s</SigningProtocol>`+
			`</OriginAccessControlConfig>`+
			`</OriginAccessControl>`,
		cfNS,
		oac.ID,
		oac.Name,
		oac.Description,
		oac.OriginType,
		oac.SigningBehavior,
		oac.SigningProtocol,
	)
}

// --- Response Headers Policy handlers ---
