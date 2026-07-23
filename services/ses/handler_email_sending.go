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

	// Parse RFC 2822 headers to extract From, To, and Subject when not supplied explicitly.
	var toAddrs []string
	subject := "raw"

	msg, err := mail.ReadMessage(strings.NewReader(rawData))
	if err == nil {
		if from := msg.Header.Get("From"); source == "" && from != "" {
			source = from
		}

		subject = msg.Header.Get("Subject")

		if toHeader := msg.Header.Get("To"); toHeader != "" {
			if addrs, parseErr := mail.ParseAddressList(toHeader); parseErr == nil {
				for _, a := range addrs {
					toAddrs = append(toAddrs, a.Address)
				}
			}
		}
	}

	msgID, sendErr := h.Backend.SendEmail(SendEmailInput{
		From:                 source,
		To:                   toAddrs,
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
