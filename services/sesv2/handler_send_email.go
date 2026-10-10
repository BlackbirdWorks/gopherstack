package sesv2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"
)

type sendEmailInput struct {
	ListManagementOptions          *ListManagementOptions `json:"ListManagementOptions"`
	Content                        emailContent           `json:"Content"`
	FromEmailAddress               string                 `json:"FromEmailAddress"`
	ConfigurationSetName           string                 `json:"ConfigurationSetName"`
	TenantName                     string                 `json:"TenantName"`
	FeedbackForwardingEmailAddress string                 `json:"FeedbackForwardingEmailAddress"`
	EndpointID                     string                 `json:"EndpointId"`
	FromEmailAddressIdentityArn    string                 `json:"FromEmailAddressIdentityArn"`
	FeedbackForwardingIdentityArn  string                 `json:"FeedbackForwardingEmailAddressIdentityArn"`
	ReplyTo                        []string               `json:"ReplyToAddresses"`
	EmailTags                      []messageTag           `json:"EmailTags"`
	Destination                    emailDestination       `json:"Destination"`
}

func (in *sendEmailInput) options() SendOptions {
	return SendOptions{
		ConfigurationSetName: in.ConfigurationSetName, TenantName: in.TenantName,
		FeedbackForwardingEmailAddress: in.FeedbackForwardingEmailAddress, ListManagement: in.ListManagementOptions,
		EndpointID: in.EndpointID, FromEmailAddressIdentityArn: in.FromEmailAddressIdentityArn,
		FeedbackForwardingEmailAddressIdentityArn: in.FeedbackForwardingIdentityArn,
	}
}

type emailDestination struct {
	ToAddresses  []string `json:"ToAddresses"`
	CcAddresses  []string `json:"CcAddresses"`
	BccAddresses []string `json:"BccAddresses"`
}

type emailContent struct {
	Simple   *simpleEmailContent `json:"Simple"`
	Raw      *rawEmailContent    `json:"Raw"`
	Template *bulkEmailTemplate  `json:"Template"`
}

type simpleEmailContent struct {
	Body    emailBody `json:"Body"`
	Subject emailData `json:"Subject"`
}

type emailBody struct {
	Text *emailData `json:"Text"`
	HTML *emailData `json:"Html"`
}

type emailData struct {
	Data    string `json:"Data"`
	Charset string `json:"Charset"`
}

type rawEmailContent struct {
	Data []byte `json:"Data"`
}

type sendEmailOutput struct {
	MessageID string `json:"MessageId"`
}

func (h *Handler) handleSendEmail(c *echo.Context) (any, error) {
	var in sendEmailInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidParameter, err.Error())
	}

	dest := in.Destination
	if len(dest.ToAddresses) == 0 && len(dest.CcAddresses) == 0 && len(dest.BccAddresses) == 0 {
		return nil, fmt.Errorf(
			"%w: Destination must contain at least one ToAddress, CcAddress, or BccAddress",
			ErrInvalidParameter,
		)
	}

	for _, list := range [][]string{dest.ToAddresses, dest.CcAddresses, dest.BccAddresses} {
		for _, addr := range list {
			if err := validateSendAddress(addr); err != nil {
				return nil, err
			}
		}
	}

	var subject, bodyHTML, bodyText string

	if in.Content.Simple != nil {
		subject = in.Content.Simple.Subject.Data
		if in.Content.Simple.Body.HTML != nil {
			bodyHTML = in.Content.Simple.Body.HTML.Data
		}

		if in.Content.Simple.Body.Text != nil {
			bodyText = in.Content.Simple.Body.Text.Data
		}
	}

	out := OutboundEmail{
		From: in.FromEmailAddress, To: dest.ToAddresses, Cc: dest.CcAddresses, Bcc: dest.BccAddresses,
		ReplyTo: in.ReplyTo, Subject: subject, BodyHTML: bodyHTML, BodyText: bodyText, Template: in.Content.Template,
		Tags: toMessageTags(in.EmailTags), Options: in.options(),
	}
	if in.Content.Raw != nil {
		out.Raw = in.Content.Raw.Data
	}

	msgID, err := h.Backend.SendMessage(out)
	if err != nil {
		return nil, err
	}

	return &sendEmailOutput{MessageID: msgID}, nil
}

// bulk email handler

type sendBulkEmailInput struct {
	DefaultContent                 *bulkEmailContent `json:"DefaultContent"`
	FromEmailAddress               string            `json:"FromEmailAddress"`
	ConfigurationSetName           string            `json:"ConfigurationSetName"`
	TenantName                     string            `json:"TenantName"`
	FeedbackForwardingEmailAddress string            `json:"FeedbackForwardingEmailAddress"`
	EndpointID                     string            `json:"EndpointId"`
	FromEmailAddressIdentityArn    string            `json:"FromEmailAddressIdentityArn"`
	FeedbackForwardingIdentityArn  string            `json:"FeedbackForwardingEmailAddressIdentityArn"`
	BulkEmailEntries               []bulkEmailEntry  `json:"BulkEmailEntries"`
	DefaultEmailTags               []messageTag      `json:"DefaultEmailTags"`
}

func (h *Handler) handleSendBulkEmail(c *echo.Context) (any, error) {
	var in sendBulkEmailInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	results, err := h.Backend.SendBulkEmail(
		in.FromEmailAddress,
		in.DefaultContent,
		in.BulkEmailEntries,
		in.DefaultEmailTags,
		SendOptions{
			ConfigurationSetName: in.ConfigurationSetName, TenantName: in.TenantName,
			FeedbackForwardingEmailAddress: in.FeedbackForwardingEmailAddress,
			EndpointID:                     in.EndpointID, FromEmailAddressIdentityArn: in.FromEmailAddressIdentityArn,
			FeedbackForwardingEmailAddressIdentityArn: in.FeedbackForwardingIdentityArn,
		},
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{"BulkEmailEntryResults": results}, nil
}

type sendCustomVerificationEmailInput struct {
	EmailAddress string `json:"EmailAddress"`
	TemplateName string `json:"TemplateName"`
}

func (h *Handler) handleSendCustomVerificationEmail(c *echo.Context) (any, error) {
	var in sendCustomVerificationEmailInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	msgID, err := h.Backend.SendCustomVerificationEmail(in.EmailAddress, in.TemplateName)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyMessageID: msgID}, nil
}
