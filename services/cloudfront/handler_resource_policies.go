package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

// getResourcePolicyRequestXML matches GetResourcePolicyInput (api_op_GetResourcePolicy.go):
// ResourceArn travels in the body under root GetResourcePolicyRequest, not a query string --
// the op is a POST to /2020-05-31/get-resource-policy despite its "Get" name.
type getResourcePolicyRequestXML struct {
	XMLName     xml.Name `xml:"GetResourcePolicyRequest"`
	ResourceARN string   `xml:"ResourceArn"`
}

// putResourcePolicyRequestXML matches PutResourcePolicyInput (api_op_PutResourcePolicy.go:27-41).
// The root element is PutResourcePolicyRequest and the policy field is PolicyDocument, not
// Policy -- the previous xml:"Policy" tag never matched a real client's element name, and the
// previous xml:"ResourcePolicy" root name never matched either, so xml.Unmarshal errored on
// every real request and the body was silently discarded (err ignored).
type putResourcePolicyRequestXML struct {
	XMLName        xml.Name `xml:"PutResourcePolicyRequest"`
	PolicyDocument string   `xml:"PolicyDocument"`
	ResourceARN    string   `xml:"ResourceArn"`
}

// deleteResourcePolicyRequestXML matches DeleteResourcePolicyInput (api_op_DeleteResourcePolicy.go).
type deleteResourcePolicyRequestXML struct {
	XMLName     xml.Name `xml:"DeleteResourcePolicyRequest"`
	ResourceARN string   `xml:"ResourceArn"`
}

func (h *Handler) handleGetResourcePolicy(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req getResourcePolicyRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid GetResourcePolicyRequest XML"))
		}
	}

	policy, getErr := h.Backend.GetResourcePolicy(req.ResourceARN)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	return xmlResp(c, http.StatusOK, fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<GetResourcePolicyResult xmlns="%s">`+
			`<PolicyDocument>%s</PolicyDocument><ResourceArn>%s</ResourceArn>`+
			`</GetResourcePolicyResult>`,
		cfNS, xmlEscape(policy), xmlEscape(req.ResourceARN),
	))
}

func (h *Handler) handlePutResourcePolicy(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req putResourcePolicyRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid PutResourcePolicyRequest XML"))
		}
	}

	if putErr := h.Backend.PutResourcePolicy(req.ResourceARN, req.PolicyDocument); putErr != nil {
		return h.handleError(c, putErr)
	}

	return xmlResp(c, http.StatusOK, fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<PutResourcePolicyResult xmlns="%s"><ResourceArn>%s</ResourceArn></PutResourcePolicyResult>`,
		cfNS, xmlEscape(req.ResourceARN),
	))
}

func (h *Handler) handleDeleteResourcePolicy(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req deleteResourcePolicyRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid DeleteResourcePolicyRequest XML"),
			)
		}
	}

	if delErr := h.Backend.DeleteResourcePolicy(req.ResourceARN); delErr != nil {
		return h.handleError(c, delErr)
	}

	return c.NoContent(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// ConnectionGroup extra handlers (Get/List/Update/Delete)
// ---------------------------------------------------------------------------
