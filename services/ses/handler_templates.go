package ses

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

func (h *Handler) handleCreateTemplate(vals url.Values, reqID string) (any, error) {
	tmpl := EmailTemplate{
		TemplateName: vals.Get("Template.TemplateName"),
		SubjectPart:  vals.Get("Template.SubjectPart"),
		TextPart:     vals.Get("Template.TextPart"),
		HTMLPart:     vals.Get("Template.HTMLPart"),
	}

	if err := h.Backend.CreateTemplate(tmpl); err != nil {
		return nil, err
	}

	return &createTemplateResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleUpdateTemplate(vals url.Values, reqID string) (any, error) {
	tmpl := EmailTemplate{
		TemplateName: vals.Get("Template.TemplateName"),
		SubjectPart:  vals.Get("Template.SubjectPart"),
		TextPart:     vals.Get("Template.TextPart"),
		HTMLPart:     vals.Get("Template.HTMLPart"),
	}

	if err := h.Backend.UpdateTemplate(tmpl); err != nil {
		return nil, err
	}

	return &updateTemplateResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleGetTemplate(vals url.Values, reqID string) (any, error) {
	name := vals.Get("TemplateName")

	tmpl, err := h.Backend.GetTemplate(name)
	if err != nil {
		return nil, err
	}

	return &getTemplateResponse{
		Xmlns: sesXMLNS,
		Result: getTemplateResult{
			Template: xmlTemplate(tmpl),
		},
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleListTemplates(vals url.Values, reqID string) any {
	nextToken := vals.Get("NextToken")
	maxItems := 0

	if s := vals.Get("MaxItems"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			maxItems = n
		}
	}

	p := h.Backend.ListTemplates(nextToken, maxItems)
	members := make([]xmlMember, 0, len(p.Data))

	for _, name := range p.Data {
		members = append(members, xmlMember{Value: name})
	}

	return &listTemplatesResponse{
		Xmlns: sesXMLNS,
		Result: listTemplatesResult{
			TemplatesMetadata: xmlMemberList{Members: members},
			NextToken:         p.Next,
		},
		RequestID: reqID,
	}
}

func (h *Handler) handleDeleteTemplate(vals url.Values, reqID string) any {
	name := vals.Get("TemplateName")

	h.Backend.DeleteTemplate(name)

	return &deleteTemplateResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}
}

type xmlTemplate struct {
	TemplateName string `xml:"TemplateName"`
	SubjectPart  string `xml:"SubjectPart,omitempty"`
	TextPart     string `xml:"TextPart,omitempty"`
	HTMLPart     string `xml:"HTMLPart,omitempty"`
}

type createTemplateResponse struct {
	XMLName   xml.Name `xml:"CreateTemplateResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type updateTemplateResponse struct {
	XMLName   xml.Name `xml:"UpdateTemplateResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type getTemplateResult struct {
	Template xmlTemplate `xml:"Template"`
}

type getTemplateResponse struct {
	XMLName   xml.Name          `xml:"GetTemplateResponse"`
	Xmlns     string            `xml:"xmlns,attr"`
	Result    getTemplateResult `xml:"GetTemplateResult"`
	RequestID string            `xml:"ResponseMetadata>RequestId"`
}

type listTemplatesResult struct {
	NextToken         string        `xml:"NextToken,omitempty"`
	TemplatesMetadata xmlMemberList `xml:"TemplatesMetadata"`
}

type listTemplatesResponse struct {
	XMLName   xml.Name            `xml:"ListTemplatesResponse"`
	Xmlns     string              `xml:"xmlns,attr"`
	RequestID string              `xml:"ResponseMetadata>RequestId"`
	Result    listTemplatesResult `xml:"ListTemplatesResult"`
}

type deleteTemplateResponse struct {
	XMLName   xml.Name `xml:"DeleteTemplateResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

func (h *Handler) handleTestRenderTemplate(vals url.Values, reqID string) (any, error) {
	rendered, err := h.Backend.TestRenderTemplate(vals.Get("TemplateName"), vals.Get("TemplateData"))
	if err != nil {
		return nil, err
	}

	return &testRenderTemplateResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result:    testRenderTemplateResult{RenderedTemplate: rendered},
	}, nil
}

type testRenderTemplateResult struct {
	RenderedTemplate string `xml:"RenderedTemplate"`
}

type testRenderTemplateResponse struct {
	XMLName   xml.Name                 `xml:"TestRenderTemplateResponse"`
	Xmlns     string                   `xml:"xmlns,attr"`
	RequestID string                   `xml:"ResponseMetadata>RequestId"`
	Result    testRenderTemplateResult `xml:"TestRenderTemplateResult"`
}
