package ses

import (
	"encoding/xml"
	"fmt"
	"net/mail"
	"net/url"
	"strconv"
	"strings"
)

func (h *Handler) handleSendEmail(vals url.Values, reqID string) (any, error) {
	msgID, err := h.Backend.SendEmail(SendEmailInput{
		From:                 vals.Get("Source"),
		To:                   parseSESMemberList(vals, "Destination.ToAddresses"),
		Cc:                   parseSESMemberList(vals, "Destination.CcAddresses"),
		Bcc:                  parseSESMemberList(vals, "Destination.BccAddresses"),
		ReplyTo:              parseSESMemberList(vals, "ReplyToAddresses"),
		Subject:              vals.Get("Message.Subject.Data"),
		BodyHTML:             vals.Get("Message.Body.Html.Data"),
		BodyText:             vals.Get("Message.Body.Text.Data"),
		ConfigurationSetName: vals.Get("ConfigurationSetName"),
		Tags:                 parseSESTags(vals, "Tags"),
		ReturnPath:           vals.Get("ReturnPath"),
		ReturnPathArn:        vals.Get("ReturnPathArn"),
		SourceArn:            vals.Get("SourceArn"),
	})
	if err != nil {
		return nil, err
	}

	return &sendEmailResponse{
		Xmlns:     sesXMLNS,
		Result:    sendEmailResult{MessageID: msgID},
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleSendRawEmail(vals url.Values, reqID string) (any, error) {
	rawData := vals.Get("RawMessage.Data")
	source := vals.Get("Source")
	returnPath := vals.Get("ReturnPath")
	returnPathArn := vals.Get("ReturnPathArn")
	sourceArn := vals.Get("SourceArn")
	configSetName := vals.Get("ConfigurationSetName")
	tags := parseSESTags(vals, "Tags")
	destinations := parseSESMemberList(vals, "Destinations")

	// Parse RFC 2822 headers to extract From, Subject, and the visible
	// To/Cc recipients when not supplied explicitly.
	var headerTo, headerCc []string
	subject := "raw"

	msg, err := mail.ReadMessage(strings.NewReader(rawData))
	if err == nil {
		if from := msg.Header.Get("From"); source == "" && from != "" {
			source = from
		}

		subject = msg.Header.Get("Subject")
		headerTo = parseSESAddressHeader(msg.Header.Get("To"))
		headerCc = parseSESAddressHeader(msg.Header.Get("Cc"))
	}

	to, cc, bcc := headerTo, headerCc, []string(nil)
	// Destinations, when supplied, is the actual SMTP envelope AWS SES
	// delivers to: it takes precedence over the To/Cc/Bcc headers in the raw
	// message and is the documented mechanism for reaching recipients
	// deliberately absent from those headers (e.g. Bcc, which most MIME
	// builders strip before the message ever reaches SES). Classify each
	// Destinations address by whether it's visible in a To/Cc header so it's
	// recorded under the right bucket; anything not visible is, by
	// definition, a Bcc recipient.
	if len(destinations) > 0 {
		to, cc, bcc = classifySESDestinations(destinations, headerTo, headerCc)
	}

	msgID, sendErr := h.Backend.SendEmail(SendEmailInput{
		From:                 source,
		To:                   to,
		Cc:                   cc,
		Bcc:                  bcc,
		Subject:              subject,
		BodyText:             rawData,
		ConfigurationSetName: configSetName,
		Tags:                 tags,
		ReturnPath:           returnPath,
		ReturnPathArn:        returnPathArn,
		SourceArn:            sourceArn,
	})
	if sendErr != nil {
		return nil, sendErr
	}

	return &sendRawEmailResponse{
		Xmlns:     sesXMLNS,
		Result:    sendEmailResult{MessageID: msgID},
		RequestID: reqID,
	}, nil
}

// parseSESAddressHeader parses an RFC 2822 address header value (e.g. the
// "To" or "Cc" header) into a flat list of email addresses. An empty or
// unparseable header yields nil.
func parseSESAddressHeader(header string) []string {
	if header == "" {
		return nil
	}

	addrs, err := mail.ParseAddressList(header)
	if err != nil {
		return nil
	}

	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.Address)
	}

	return out
}

// classifySESDestinations splits SendRawEmail's Destinations envelope list
// into To/Cc/Bcc buckets by matching against the raw message's visible
// To/Cc headers.
func classifySESDestinations(destinations, headerTo, headerCc []string) ([]string, []string, []string) {
	inTo := make(map[string]bool, len(headerTo))
	for _, a := range headerTo {
		inTo[strings.ToLower(a)] = true
	}

	inCc := make(map[string]bool, len(headerCc))
	for _, a := range headerCc {
		inCc[strings.ToLower(a)] = true
	}

	var to, cc, bcc []string

	for _, d := range destinations {
		switch key := strings.ToLower(d); {
		case inTo[key]:
			to = append(to, d)
		case inCc[key]:
			cc = append(cc, d)
		default:
			bcc = append(bcc, d)
		}
	}

	return to, cc, bcc
}

func (h *Handler) handleSendTemplatedEmail(vals url.Values, reqID string) (any, error) {
	msgID, err := h.Backend.SendTemplatedEmail(SendTemplatedEmailInput{
		From:                 vals.Get("Source"),
		To:                   parseSESMemberList(vals, "Destination.ToAddresses"),
		Cc:                   parseSESMemberList(vals, "Destination.CcAddresses"),
		Bcc:                  parseSESMemberList(vals, "Destination.BccAddresses"),
		ReplyTo:              parseSESMemberList(vals, "ReplyToAddresses"),
		TemplateName:         vals.Get("Template"),
		TemplateData:         vals.Get("TemplateData"),
		ConfigurationSetName: vals.Get("ConfigurationSetName"),
		Tags:                 parseSESTags(vals, "Tags"),
		ReturnPath:           vals.Get("ReturnPath"),
		ReturnPathArn:        vals.Get("ReturnPathArn"),
		SourceArn:            vals.Get("SourceArn"),
	})
	if err != nil {
		return nil, err
	}

	return &sendTemplatedEmailResponse{
		Xmlns:     sesXMLNS,
		Result:    sendEmailResult{MessageID: msgID},
		RequestID: reqID,
	}, nil
}

type sendEmailResult struct {
	MessageID string `xml:"MessageId"`
}

type sendEmailResponse struct {
	XMLName   xml.Name        `xml:"SendEmailResponse"`
	Xmlns     string          `xml:"xmlns,attr"`
	Result    sendEmailResult `xml:"SendEmailResult"`
	RequestID string          `xml:"ResponseMetadata>RequestId"`
}

type sendRawEmailResponse struct {
	XMLName   xml.Name        `xml:"SendRawEmailResponse"`
	Xmlns     string          `xml:"xmlns,attr"`
	Result    sendEmailResult `xml:"SendRawEmailResult"`
	RequestID string          `xml:"ResponseMetadata>RequestId"`
}

type sendTemplatedEmailResponse struct {
	XMLName   xml.Name        `xml:"SendTemplatedEmailResponse"`
	Xmlns     string          `xml:"xmlns,attr"`
	Result    sendEmailResult `xml:"SendTemplatedEmailResult"`
	RequestID string          `xml:"ResponseMetadata>RequestId"`
}

func (h *Handler) handleSendBounce(vals url.Values, reqID string) (any, error) {
	recipients := parseBouncedRecipients(vals, "BouncedRecipientInfoList")

	msgID, err := h.Backend.SendBounce(vals.Get("OriginalMessageId"), vals.Get("BounceSender"), recipients)
	if err != nil {
		return nil, err
	}

	return &sendBounceResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result:    sendEmailResult{MessageID: msgID},
	}, nil
}

// parseBouncedRecipients parses "<prefix>.member.N.Recipient" form values into
// a flat list of bounced recipient email addresses.
func parseBouncedRecipients(vals url.Values, prefix string) []string {
	var recipients []string
	base := prefix + ".member."

	for i := 1; ; i++ {
		v := vals.Get(base + strconv.Itoa(i) + ".Recipient")
		if v == "" {
			return recipients
		}

		recipients = append(recipients, v)
	}
}

func (h *Handler) handleSendBulkTemplatedEmail(vals url.Values, reqID string) (any, error) {
	in := SendBulkTemplatedEmailInput{
		Source:               vals.Get("Source"),
		TemplateName:         vals.Get("Template"),
		DefaultTemplateData:  vals.Get("DefaultTemplateData"),
		ConfigurationSetName: vals.Get("ConfigurationSetName"),
		ReturnPath:           vals.Get("ReturnPath"),
		ReturnPathArn:        vals.Get("ReturnPathArn"),
		SourceArn:            vals.Get("SourceArn"),
		ReplyTo:              parseSESMemberList(vals, "ReplyToAddresses"),
		DefaultTags:          parseSESTags(vals, "DefaultTags"),
		Destinations:         parseBulkEmailDestinations(vals),
	}

	// AWS SES rejects SendBulkTemplatedEmail with more than 50 destinations.
	const maxBulkDestinations = 50
	if len(in.Destinations) > maxBulkDestinations {
		return nil, fmt.Errorf("%w: too many destinations: %d (max %d)",
			ErrInvalidParameter, len(in.Destinations), maxBulkDestinations)
	}

	msgIDs, err := h.Backend.SendBulkTemplatedEmail(in)
	if err != nil {
		return nil, err
	}

	statuses := make([]xmlBulkEmailDestStatus, 0, len(msgIDs))
	for _, id := range msgIDs {
		statuses = append(statuses, xmlBulkEmailDestStatus{MessageID: id, Status: identityStatusSuccess})
	}

	return &sendBulkTemplatedEmailResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result:    sendBulkTemplatedEmailResult{Status: xmlBulkStatusList{Members: statuses}},
	}, nil
}

// parseBulkEmailDestinations parses the "Destinations.member.N.*" form values
// into a slice of BulkEmailDestination, including each destination's
// ReplacementTags.member.M.{Name,Value} tag overrides.
func parseBulkEmailDestinations(vals url.Values) []BulkEmailDestination {
	var destinations []BulkEmailDestination

	for i := 1; ; i++ {
		prefix := "Destinations.member." + strconv.Itoa(i)
		to := parseSESMemberList(vals, prefix+".Destination.ToAddresses")
		cc := parseSESMemberList(vals, prefix+".Destination.CcAddresses")
		bcc := parseSESMemberList(vals, prefix+".Destination.BccAddresses")

		if len(to) == 0 && len(cc) == 0 && len(bcc) == 0 {
			break
		}

		destinations = append(destinations, BulkEmailDestination{
			To:                      to,
			Cc:                      cc,
			Bcc:                     bcc,
			ReplacementTemplateData: vals.Get(prefix + ".ReplacementTemplateData"),
			ReplacementTags:         parseSESTags(vals, prefix+".ReplacementTags"),
		})
	}

	return destinations
}

type sendBounceResponse struct {
	XMLName   xml.Name        `xml:"SendBounceResponse"`
	Xmlns     string          `xml:"xmlns,attr"`
	RequestID string          `xml:"ResponseMetadata>RequestId"`
	Result    sendEmailResult `xml:"SendBounceResult"`
}

type xmlBulkEmailDestStatus struct {
	MessageID string `xml:"MessageId"`
	Status    string `xml:"Status"`
}

type xmlBulkStatusList struct {
	Members []xmlBulkEmailDestStatus `xml:"member"`
}

type sendBulkTemplatedEmailResult struct {
	Status xmlBulkStatusList `xml:"Status"`
}

type sendBulkTemplatedEmailResponse struct {
	XMLName   xml.Name                     `xml:"SendBulkTemplatedEmailResponse"`
	Xmlns     string                       `xml:"xmlns,attr"`
	RequestID string                       `xml:"ResponseMetadata>RequestId"`
	Result    sendBulkTemplatedEmailResult `xml:"SendBulkTemplatedEmailResult"`
}
