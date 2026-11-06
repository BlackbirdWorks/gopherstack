package connect

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	maxEmailAttachments = 50
	deliveryDelivered   = "DELIVERED"
	deliveryQueued      = "QUEUED"
	deliveryFailed      = "FAILED"
	trafficCampaign     = "CAMPAIGN"
	endpointEmail       = "EMAIL_ADDRESS"
)

// EmailAddressInfo is an email address with an optional display name.
type EmailAddressInfo struct {
	EmailAddress string `json:"EmailAddress"`
	DisplayName  string `json:"DisplayName,omitempty"`
}

// EmailAttachment names an S3 object attached to an email.
type EmailAttachment struct {
	FileName string `json:"FileName"`
	S3URL    string `json:"S3Url"`
}

// RawEmail is the raw body of an email message.
type RawEmail struct {
	Headers     map[string]string
	Body        string
	ContentType string
	Subject     string
}

// EmailContent is the message of an email request.
type EmailContent struct {
	RawMessage             *RawEmail
	TemplatedMessageConfig json.RawMessage
	MessageSourceType      string
}

// EmailMessage is a message handed to the Mailer.
type EmailMessage struct {
	From    string
	Subject string
	HTML    string
	Text    string
	To      []string
	Cc      []string
}

// Mailer delivers outbound Connect emails, wired to SES at startup.
type Mailer interface {
	SendEmail(ctx context.Context, m EmailMessage) error
}

// SetMailer wires outbound email delivery.
func (b *Backend) SetMailer(m Mailer) {
	b.mu.Lock("SetMailer")
	defer b.mu.Unlock()

	b.mailer = m
}

// InboundRecipients holds the extra recipients of an inbound email.
type InboundRecipients struct {
	CcAddresses []EmailAddressInfo
	ToAddresses []EmailAddressInfo
}

// OutboundRecipients holds the Cc recipients of an outbound email.
type OutboundRecipients struct {
	CcEmailAddresses []EmailAddressInfo
}

// StartEmailContactInput is the StartEmailContact request.
type StartEmailContactInput struct {
	FromEmailAddress     *EmailAddressInfo
	EmailMessage         *EmailContent
	AdditionalRecipients *InboundRecipients
	Attributes           map[string]string
	References           json.RawMessage
	SegmentAttributes    json.RawMessage
	Attachments          []EmailAttachment
	DestinationEmailAddress string
	InstanceID           string `json:"InstanceId"`
	ClientToken          string
	ContactFlowID        string `json:"ContactFlowId"`
	Description          string
	Name                 string
	RelatedContactID     string `json:"RelatedContactId"`
}

func checkRawEmail(c *EmailContent, allowTemplate bool) error {
	if c == nil {
		return invalidRequestErr("EmailMessage is required")
	}

	if c.MessageSourceType == "TEMPLATE" && allowTemplate {
		return invalidRequestErr("message templates are not available in this emulator; send a RAW message")
	}

	if err := oneOf("EmailMessage.MessageSourceType", c.MessageSourceType, "RAW"); err != nil {
		return err
	}

	m := c.RawMessage
	if m == nil || m.Body == "" || m.Subject == "" {
		return invalidRequestErr("EmailMessage.RawMessage needs a Subject and a Body")
	}

	return oneOf("EmailMessage.RawMessage.ContentType", m.ContentType, "text/plain", "text/html")
}

func checkInfo(field string, a *EmailAddressInfo) error {
	if a == nil {
		return invalidParamErr("%s is required", field)
	}

	return checkEmailAddress(a.EmailAddress)
}

func checkInfos(field string, list []EmailAddressInfo) error {
	for i := range list {
		if err := checkInfo(field, &list[i]); err != nil {
			return err
		}
	}

	return nil
}

func (b *Backend) instanceEmailLocked(inst, address string) bool {
	for _, e := range b.emails.in(inst) {
		if strs.Equal(e.Address, address) {
			return true
		}
	}

	return false
}

func recipients(list []EmailAddressInfo) []EmailRecipient {
	out := make([]EmailRecipient, len(list))
	for i, a := range list {
		out[i] = EmailRecipient{Address: a.EmailAddress, DisplayName: a.DisplayName}
	}

	return out
}

func validateStartEmail(in *StartEmailContactInput) error {
	if err := firstError(
		checkEmailAddress(in.DestinationEmailAddress),
		checkInfo("FromEmailAddress", in.FromEmailAddress),
		checkRawEmail(in.EmailMessage, false),
		checkContactText(in.Name, in.Description),
		validAttributeKeys(in.Attributes),
	); err != nil {
		return err
	}

	if len(in.Attachments) > maxEmailAttachments {
		return invalidRequestErr("at most %d attachments are allowed", maxEmailAttachments)
	}

	for _, a := range in.Attachments {
		if a.FileName == "" || a.S3URL == "" {
			return invalidParamErr("each attachment needs a FileName and an S3Url")
		}
	}

	if r := in.AdditionalRecipients; r != nil {
		return firstError(checkInfos("AdditionalRecipients", r.CcAddresses), checkInfos("AdditionalRecipients", r.ToAddresses))
	}

	return nil
}

// StartEmailContact creates an inbound email contact addressed to one of the instance's email addresses.
func (b *Backend) StartEmailContact(ctx context.Context, in *StartEmailContactInput) (*contactIDOut, error) {
	if err := validateStartEmail(in); err != nil {
		return nil, err
	}

	b.mu.Lock("StartEmailContact")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID

	if !b.instanceEmailLocked(inst, in.DestinationEmailAddress) {
		return nil, notFoundErr("EmailAddress", in.DestinationEmailAddress)
	}

	if in.ContactFlowID != "" {
		if err = b.requireFlow(inst, in.ContactFlowID); err != nil {
			return nil, err
		}
	}

	c := b.newContact(ctx, inst, channelEmail, "INBOUND")
	c.FlowID, c.References, c.Contact.SegmentAttributes = in.ContactFlowID, in.References, in.SegmentAttributes
	c.Contact.Name, c.Contact.Description = in.Name, in.Description
	c.Contact.Attributes = cloneTags(in.Attributes)

	if in.RelatedContactID != "" {
		rel, ok := b.contacts.table.Get(inst + "/" + in.RelatedContactID)
		if !ok {
			return nil, notFoundErr("Contact", in.RelatedContactID)
		}

		merged := cloneTags(rel.Contact.Attributes)
		if merged == nil {
			merged = map[string]string{}
		}

		for k, v := range in.Attributes {
			merged[k] = v
		}

		c.Contact.Attributes, c.Contact.RelatedContactID = merged, in.RelatedContactID
	}

	from, to := *in.FromEmailAddress, EmailAddressInfo{EmailAddress: in.DestinationEmailAddress}
	c.Contact.CustomerEndpoint = &EndpointInfo{Address: from.EmailAddress, Type: endpointEmail}
	c.Contact.SystemEndpoint = &EndpointInfo{Address: to.EmailAddress, Type: endpointEmail}

	if ar := in.AdditionalRecipients; ar != nil && (len(ar.CcAddresses)+len(ar.ToAddresses)) > 0 {
		c.Contact.AdditionalEmailRecipients = &AdditionalEmailRecipients{
			CcList: recipients(ar.CcAddresses), ToList: recipients(ar.ToAddresses),
		}
	}

	m := in.EmailMessage.RawMessage
	c.Emails = append(c.Emails, emailMessageRec{
		Sent: *c.Contact.InitiationTimestamp, Direction: "INBOUND", From: from, To: to, Subject: m.Subject,
		ContentType: m.ContentType, Body: m.Body, Headers: cloneTags(m.Headers), Attachments: in.Attachments,
	})
	b.contacts.put(c)

	return &contactIDOut{ContactID: c.Contact.ID}, nil
}

// StartOutboundEmailContactInput is the StartOutboundEmailContact request.
type StartOutboundEmailContactInput struct {
	DestinationEmailAddress *EmailAddressInfo
	FromEmailAddress        *EmailAddressInfo
	EmailMessage            *EmailContent
	AdditionalRecipients    *OutboundRecipients
	ContactID               string `json:"ContactId"`
	InstanceID              string `json:"InstanceId"`
	ClientToken             string
}

func (b *Backend) deliverLocked(ctx context.Context, rec *emailMessageRec) {
	rec.Delivery = deliveryQueued
	if b.mailer == nil {
		return
	}

	msg := EmailMessage{From: rec.From.EmailAddress, To: []string{rec.To.EmailAddress}, Subject: rec.Subject}
	for _, cc := range rec.Cc {
		msg.Cc = append(msg.Cc, cc.EmailAddress)
	}

	if rec.ContentType == "text/html" {
		msg.HTML = rec.Body
	} else {
		msg.Text = rec.Body
	}

	if err := b.mailer.SendEmail(ctx, msg); err != nil {
		logger.Load(ctx).WarnContext(ctx, "connect: outbound email delivery failed", "error", err)
		rec.Delivery = deliveryFailed

		return
	}

	rec.Delivery = deliveryDelivered
}

// StartOutboundEmailContact sends the email of an outbound or agent-reply email contact.
func (b *Backend) StartOutboundEmailContact(
	ctx context.Context,
	in *StartOutboundEmailContactInput,
) (*contactIDOut, error) {
	if err := firstError(
		checkInfo("DestinationEmailAddress", in.DestinationEmailAddress),
		checkRawEmail(in.EmailMessage, true),
	); err != nil {
		return nil, err
	}

	if in.FromEmailAddress != nil {
		if err := checkEmailAddress(in.FromEmailAddress.EmailAddress); err != nil {
			return nil, err
		}
	}

	if r := in.AdditionalRecipients; r != nil {
		if err := checkInfos("AdditionalRecipients", r.CcEmailAddresses); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("StartOutboundEmailContact")
	defer b.mu.Unlock()

	c, err := b.activeContactLocked(in.InstanceID, in.ContactID)
	if err != nil {
		return nil, err
	}

	method := c.Contact.InitiationMethod
	if c.Contact.Channel != channelEmail || (method != "OUTBOUND" && method != "AGENT_REPLY") {
		return nil, invalidRequestErr("contact %q is not an outbound or agent-reply email contact", in.ContactID)
	}

	if len(c.Emails) > 0 {
		return nil, invalidRequestErr("contact %q has already sent its email", in.ContactID)
	}

	from, err := b.outboundFromLocked(c, in.FromEmailAddress)
	if err != nil {
		return nil, err
	}

	to := *in.DestinationEmailAddress
	m := in.EmailMessage.RawMessage
	rec := emailMessageRec{
		Sent: awstime.NewEpochTime(b.now()), Direction: "OUTBOUND", From: from, To: to,
		Subject: m.Subject, ContentType: m.ContentType, Body: m.Body,
	}

	if r := in.AdditionalRecipients; r != nil {
		rec.Cc = r.CcEmailAddresses
		c.Contact.AdditionalEmailRecipients = &AdditionalEmailRecipients{CcList: recipients(r.CcEmailAddresses)}
	}

	b.deliverLocked(ctx, &rec)
	c.Emails = append(c.Emails, rec)
	c.Contact.CustomerEndpoint = &EndpointInfo{Address: to.EmailAddress, Type: endpointEmail}
	c.Contact.SystemEndpoint = &EndpointInfo{Address: from.EmailAddress, Type: endpointEmail}
	b.touch(c)

	return &contactIDOut{ContactID: c.Contact.ID}, nil
}

func (b *Backend) outboundFromLocked(c *contactRec, given *EmailAddressInfo) (EmailAddressInfo, error) {
	var from EmailAddressInfo

	switch {
	case given != nil:
		from = *given
	case c.Contact.SystemEndpoint != nil:
		from = EmailAddressInfo{EmailAddress: c.Contact.SystemEndpoint.Address}
	default:
		return from, invalidRequestErr("FromEmailAddress is required")
	}

	if !b.instanceEmailLocked(c.InstanceID, from.EmailAddress) {
		return from, notFoundErr("EmailAddress", from.EmailAddress)
	}

	return from, nil
}

// SendOutboundEmailInput is the SendOutboundEmail request.
type SendOutboundEmailInput struct {
	DestinationEmailAddress *EmailAddressInfo
	FromEmailAddress        *EmailAddressInfo
	EmailMessage            *EmailContent
	AdditionalRecipients    *OutboundRecipients
	SourceCampaign          *struct {
		CampaignID        string `json:"CampaignId"`
		OutboundRequestID string `json:"OutboundRequestId"`
	}
	InstanceID  string `json:"-" path:"InstanceId"`
	TrafficType string
	ClientToken string
}

// SendOutboundEmail sends a campaign email from one of the instance's email addresses.
func (b *Backend) SendOutboundEmail(ctx context.Context, in *SendOutboundEmailInput) (*empty, error) {
	if err := oneOf("TrafficType", in.TrafficType, "GENERAL", trafficCampaign); err != nil {
		return nil, err
	}

	if in.TrafficType != trafficCampaign {
		return nil, invalidRequestErr("only the CAMPAIGN traffic type is supported")
	}

	if in.SourceCampaign == nil || strings.TrimSpace(in.SourceCampaign.CampaignID) == "" {
		return nil, invalidRequestErr("SourceCampaign.CampaignId is required for CAMPAIGN traffic")
	}

	if err := firstError(
		checkInfo("DestinationEmailAddress", in.DestinationEmailAddress),
		checkInfo("FromEmailAddress", in.FromEmailAddress),
		checkRawEmail(in.EmailMessage, true),
	); err != nil {
		return nil, err
	}

	if r := in.AdditionalRecipients; r != nil {
		if err := checkInfos("AdditionalRecipients", r.CcEmailAddresses); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("SendOutboundEmail")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	rec := b.emailByAddressLocked(r.Instance.ID, in.FromEmailAddress.EmailAddress)
	if rec == nil {
		return nil, notFoundErr("EmailAddress", in.FromEmailAddress.EmailAddress)
	}

	m := in.EmailMessage.RawMessage
	msg := emailMessageRec{
		Sent: awstime.NewEpochTime(b.now()), Direction: "OUTBOUND", From: *in.FromEmailAddress,
		To: *in.DestinationEmailAddress, Subject: m.Subject, ContentType: m.ContentType, Body: m.Body,
	}

	if ar := in.AdditionalRecipients; ar != nil {
		msg.Cc = ar.CcEmailAddresses
	}

	b.deliverLocked(ctx, &msg)
	rec.Sent = append(rec.Sent, msg)

	return &empty{}, nil
}

func (b *Backend) emailByAddressLocked(inst, address string) *emailAddressRec {
	for _, e := range b.emails.in(inst) {
		if strs.Equal(e.Address, address) {
			return e
		}
	}

	return nil
}
