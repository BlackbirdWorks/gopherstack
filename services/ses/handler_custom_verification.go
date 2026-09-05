package ses

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

func (h *Handler) handleCreateCustomVerificationEmailTemplate(vals url.Values, reqID string) (any, error) {
	tmpl := CustomVerificationEmailTemplate{
		TemplateName:          vals.Get("TemplateName"),
		FromEmailAddress:      vals.Get("FromEmailAddress"),
		TemplateSubject:       vals.Get("TemplateSubject"),
		TemplateContent:       vals.Get("TemplateContent"),
		SuccessRedirectionURL: vals.Get("SuccessRedirectionURL"),
		FailureRedirectionURL: vals.Get("FailureRedirectionURL"),
	}

	if err := h.Backend.CreateCustomVerificationEmailTemplate(tmpl); err != nil {
		return nil, err
	}

	return &createCustomVerificationEmailTemplateResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleDeleteCustomVerificationEmailTemplate(vals url.Values, reqID string) (any, error) {
	templateName := vals.Get("TemplateName")

	if err := h.Backend.DeleteCustomVerificationEmailTemplate(templateName); err != nil {
		return nil, err
	}

	return &deleteCustomVerificationEmailTemplateResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

type createCustomVerificationEmailTemplateResponse struct {
	XMLName   xml.Name `xml:"CreateCustomVerificationEmailTemplateResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type deleteCustomVerificationEmailTemplateResponse struct {
	XMLName   xml.Name `xml:"DeleteCustomVerificationEmailTemplateResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

func (h *Handler) handleGetCustomVerificationEmailTemplate(vals url.Values, reqID string) (any, error) {
	name := vals.Get("TemplateName")
	tmpl, err := h.Backend.GetCustomVerificationEmailTemplate(name)
	if err != nil {
		return nil, err
	}

	return &getCustomVerificationEmailTemplateResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result: getCustomVerificationEmailTemplateResult{
			Template: xmlCustomVerifTemplate(tmpl),
		},
	}, nil
}

func (h *Handler) handleListCustomVerificationEmailTemplates(vals url.Values, reqID string) any {
	maxResults := 0
	if s := vals.Get("MaxResults"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			maxResults = n
		}
	}

	p := h.Backend.ListCustomVerificationEmailTemplates(vals.Get("NextToken"), maxResults)
	members := make([]xmlCustomVerifTemplate, 0, len(p.Data))
	for _, t := range p.Data {
		members = append(members, xmlCustomVerifTemplate(t))
	}

	return &listCustomVerificationEmailTemplatesResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result: listCustomVerificationEmailTemplatesResult{
			CustomVerificationEmailTemplates: xmlCustomVerifTemplateList{Members: members},
			NextToken:                        p.Next,
		},
	}
}

type xmlCustomVerifTemplate struct {
	TemplateName          string `xml:"TemplateName"`
	FromEmailAddress      string `xml:"FromEmailAddress"`
	TemplateSubject       string `xml:"TemplateSubject"`
	TemplateContent       string `xml:"TemplateContent,omitempty"`
	SuccessRedirectionURL string `xml:"SuccessRedirectionURL,omitempty"`
	FailureRedirectionURL string `xml:"FailureRedirectionURL,omitempty"`
}

type getCustomVerificationEmailTemplateResult struct {
	Template xmlCustomVerifTemplate `xml:"CustomVerificationEmailTemplate"`
}

type getCustomVerificationEmailTemplateResponse struct {
	XMLName   xml.Name                                 `xml:"GetCustomVerificationEmailTemplateResponse"`
	Xmlns     string                                   `xml:"xmlns,attr"`
	RequestID string                                   `xml:"ResponseMetadata>RequestId"`
	Result    getCustomVerificationEmailTemplateResult `xml:"GetCustomVerificationEmailTemplateResult"`
}

type xmlCustomVerifTemplateList struct {
	Members []xmlCustomVerifTemplate `xml:"member"`
}

type listCustomVerificationEmailTemplatesResult struct {
	NextToken                        string                     `xml:"NextToken,omitempty"`
	CustomVerificationEmailTemplates xmlCustomVerifTemplateList `xml:"CustomVerificationEmailTemplates"`
}

type listCustomVerificationEmailTemplatesResponse struct {
	XMLName   xml.Name                                   `xml:"ListCustomVerificationEmailTemplatesResponse"`
	Xmlns     string                                     `xml:"xmlns,attr"`
	RequestID string                                     `xml:"ResponseMetadata>RequestId"`
	Result    listCustomVerificationEmailTemplatesResult `xml:"ListCustomVerificationEmailTemplatesResult"`
}

func (h *Handler) handleSendCustomVerificationEmail(vals url.Values, reqID string) (any, error) {
	msgID, err := h.Backend.SendCustomVerificationEmail(
		vals.Get("EmailAddress"),
		vals.Get("TemplateName"),
		vals.Get("ConfigurationSetName"),
	)
	if err != nil {
		return nil, err
	}

	return &sendCustomVerificationEmailResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result:    sendEmailResult{MessageID: msgID},
	}, nil
}

func (h *Handler) handleUpdateCustomVerificationEmailTemplate(vals url.Values, reqID string) (any, error) {
	tmpl := CustomVerificationEmailTemplate{
		TemplateName:          vals.Get("TemplateName"),
		FromEmailAddress:      vals.Get("FromEmailAddress"),
		TemplateSubject:       vals.Get("TemplateSubject"),
		TemplateContent:       vals.Get("TemplateContent"),
		SuccessRedirectionURL: vals.Get("SuccessRedirectionURL"),
		FailureRedirectionURL: vals.Get("FailureRedirectionURL"),
	}

	if err := h.Backend.UpdateCustomVerificationEmailTemplate(tmpl); err != nil {
		return nil, err
	}

	return newEmptyResponse("UpdateCustomVerificationEmailTemplate", reqID), nil
}

type sendCustomVerificationEmailResponse struct {
	XMLName   xml.Name        `xml:"SendCustomVerificationEmailResponse"`
	Xmlns     string          `xml:"xmlns,attr"`
	RequestID string          `xml:"ResponseMetadata>RequestId"`
	Result    sendEmailResult `xml:"SendCustomVerificationEmailResult"`
}
