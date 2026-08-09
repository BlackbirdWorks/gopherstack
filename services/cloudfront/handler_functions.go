package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

type functionConfigXML struct {
	XMLName      xml.Name `xml:"FunctionConfig"`
	Comment      string   `xml:"Comment"`
	Runtime      string   `xml:"Runtime"`
	FunctionCode string   `xml:"FunctionCode"`
}

// functionRequestFields is shared by Create and Update, whose real request
// shapes carry identical fields but different root element names
// (CreateFunctionRequest vs UpdateFunctionRequest; cloudfront@v1.67.4
// serializers.go). A prior single struct fixed the root to
// "CreateFunctionRequest", so every real UpdateFunction call (root
// UpdateFunctionRequest) failed decode and was rejected as MalformedXML.
type functionRequestFields struct {
	Name           string            `xml:"Name"`
	FunctionCode   string            `xml:"FunctionCode"`
	FunctionConfig functionConfigXML `xml:"FunctionConfig"`
	Tags           tagsXML           `xml:"Tags"`
}

type createFunctionRequestXML struct {
	XMLName xml.Name `xml:"CreateFunctionRequest"`
	functionRequestFields
}

type updateFunctionRequestXML struct {
	XMLName xml.Name `xml:"UpdateFunctionRequest"`
	functionRequestFields
}

func (h *Handler) handleCreateFunction(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req createFunctionRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid CreateFunctionRequest XML"),
			)
		}
	}

	code := req.FunctionCode
	if code == "" {
		code = req.FunctionConfig.FunctionCode
	}

	fn, createErr := h.Backend.CreateFunction(
		req.Name,
		req.FunctionConfig.Comment,
		req.FunctionConfig.Runtime,
		code,
		tagsXMLToMap(req.Tags),
	)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", fn.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"function/"+fn.Name)

	return xmlResp(c, http.StatusCreated, functionResponseXML(fn))
}

func (h *Handler) handleGetFunction(c *echo.Context, name string) error {
	fn, err := h.Backend.GetFunction(name)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", fn.ETag)

	return xmlResp(c, http.StatusOK, functionResponseXML(fn))
}

func (h *Handler) handleDescribeFunction(c *echo.Context, name string) error {
	fn, err := h.Backend.GetFunction(name)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", fn.ETag)

	return xmlResp(c, http.StatusOK, functionResponseXML(fn))
}

func (h *Handler) handleListFunctions(c *echo.Context) error {
	fns := h.Backend.ListFunctions()

	var sb strings.Builder

	for _, fn := range fns {
		fmt.Fprintf(&sb,
			`<FunctionSummary>`+
				`<Name>%s</Name>`+
				`<Status>%s</Status>`+
				`<FunctionConfig>`+
				`<Comment>%s</Comment>`+
				`<Runtime>%s</Runtime>`+
				`</FunctionConfig>`+
				`<FunctionMetadata>`+
				`<FunctionARN>%s</FunctionARN>`+
				`<Stage>%s</Stage>`+
				`<CreatedTime>%s</CreatedTime>`+
				`<LastModifiedTime>%s</LastModifiedTime>`+
				`</FunctionMetadata>`+
				`</FunctionSummary>`,
			fn.Name, fn.Status, fn.Comment, fn.Runtime,
			fn.ARN, fn.Status, fn.CreatedTime, fn.LastModifiedTime)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<FunctionList xmlns="%s">`+
		`<MaxItems>%d</MaxItems>`+
		`<Quantity>%d</Quantity>`+
		`<Items>%s</Items>`+
		`</FunctionList>`,
		cfNS, maxItems, len(fns), sb.String())

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handlePublishFunction(c *echo.Context, name string) error {
	current, getErr := h.Backend.GetFunction(name)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current function ETag",
			),
		)
	}

	fn, err := h.Backend.PublishFunction(name)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", fn.ETag)

	return xmlResp(c, http.StatusCreated, functionResponseXML(fn))
}

func (h *Handler) handleUpdateFunction(c *echo.Context, name string) error {
	current, getErr := h.Backend.GetFunction(name)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current function ETag",
			),
		)
	}

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req updateFunctionRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid UpdateFunctionRequest XML"),
			)
		}
	}

	code := req.FunctionCode
	if code == "" {
		code = req.FunctionConfig.FunctionCode
	}

	fn, updateErr := h.Backend.UpdateFunction(
		name,
		req.FunctionConfig.Comment,
		req.FunctionConfig.Runtime,
		code,
	)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", fn.ETag)

	return xmlResp(c, http.StatusOK, functionResponseXML(fn))
}

func (h *Handler) handleDeleteFunction(c *echo.Context, name string) error {
	current, getErr := h.Backend.GetFunction(name)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current function ETag",
			),
		)
	}

	if err := h.Backend.DeleteFunction(name); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleTestFunction(c *echo.Context, name string) error {
	// TestFunction validates function logic; in-memory mock simply confirms it exists.
	_, err := h.Backend.GetFunction(name)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<TestResult xmlns="%s">`+
		`<FunctionSummary>`+
		`<Name>%s</Name>`+
		`</FunctionSummary>`+
		`<FunctionExecutionLogs></FunctionExecutionLogs>`+
		`<FunctionErrorMessage></FunctionErrorMessage>`+
		`<FunctionOutput></FunctionOutput>`+
		`</TestResult>`,
		cfNS, name)

	return xmlResp(c, http.StatusOK, resp)
}

func functionResponseXML(fn *Function) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<FunctionSummary xmlns="%s">`+
		`<Name>%s</Name>`+
		`<Status>%s</Status>`+
		`<FunctionConfig>`+
		`<Comment>%s</Comment>`+
		`<Runtime>%s</Runtime>`+
		`</FunctionConfig>`+
		`<FunctionMetadata>`+
		`<FunctionARN>%s</FunctionARN>`+
		`<Stage>%s</Stage>`+
		`<CreatedTime>%s</CreatedTime>`+
		`<LastModifiedTime>%s</LastModifiedTime>`+
		`</FunctionMetadata>`+
		`</FunctionSummary>`,
		cfNS, fn.Name, fn.Status, fn.Comment, fn.Runtime,
		fn.ARN, fn.Status, fn.CreatedTime, fn.LastModifiedTime)
}

// --- Origin Request Policy handlers ---
