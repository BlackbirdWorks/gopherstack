package sesv2

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

// maxRetainedEmails is the maximum number of sent emails retained in memory.
// When the cap is exceeded the oldest entries are dropped FIFO so a long-running
// instance cannot leak memory through repeated SendEmail calls.
const maxRetainedEmails = 10000

// emailCompactionHighWater is the slice length that triggers compaction.
// Compacting only when the slice has grown to twice the cap keeps
// trimming amortized O(1) per SendEmail.
const emailCompactionHighWater = maxRetainedEmails + maxRetainedEmails

// Email captures a sent email for local inspection.
type Email struct {
	Timestamp time.Time    `json:"timestamp"`
	Options   SendOptions  `json:"options,omitzero"`
	From      string       `json:"from"`
	Subject   string       `json:"subject"`
	BodyHTML  string       `json:"bodyHTML"`
	BodyText  string       `json:"bodyText"`
	MessageID string       `json:"messageID"`
	To        []string     `json:"to"`
	Tags      []MessageTag `json:"emailTags,omitempty"`
}

// MessageTag is a SendEmail/SendBulkEmail EmailTags entry (types.MessageTag).
type MessageTag struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// OutboundEmail is everything one SendEmail call carries; Cc/Bcc/ReplyTo/Raw only feed SMTP relay.
type OutboundEmail struct {
	Options  SendOptions
	Template *bulkEmailTemplate
	From     string
	Subject  string
	BodyHTML string
	BodyText string
	Raw      []byte
	To       []string
	Cc       []string
	Bcc      []string
	ReplyTo  []string
	Tags     []MessageTag
}

// SendEmail captures an outbound email and returns a message ID.
func (b *InMemoryBackend) SendEmail(
	from string,
	to []string,
	subject, bodyHTML, bodyText string,
	template *bulkEmailTemplate,
) (string, error) {
	return b.SendMessage(OutboundEmail{
		From: from, To: to, Subject: subject, BodyHTML: bodyHTML, BodyText: bodyText, Template: template,
	})
}

// SendMessage captures an outbound email, relays it over SMTP when configured, and returns the message ID.
func (b *InMemoryBackend) SendMessage(out OutboundEmail) (string, error) {
	if out.From == "" {
		return "", fmt.Errorf("%w: FromEmailAddress is required", ErrInvalidInput)
	}

	if err := b.validateSendOptions(out.From, storedTemplateName(out.Template), out.Options); err != nil {
		return "", err
	}

	if out.Template != nil {
		tmplSubject, tmplHTML, tmplText, vars, err := b.resolveBulkTemplate(out.Template)
		if err != nil {
			return "", err
		}

		out.Subject = renderTemplateVars(tmplSubject, vars)
		out.BodyHTML = renderTemplateVars(tmplHTML, vars)
		out.BodyText = renderTemplateVars(tmplText, vars)
	}

	msgID := "sesv2-" + uuid.New().String()

	email := Email{
		MessageID: msgID,
		From:      out.From,
		To:        out.To,
		Subject:   out.Subject,
		BodyHTML:  out.BodyHTML,
		BodyText:  out.BodyText,
		Tags:      out.Tags,
		Options:   out.Options,
		Timestamp: time.Now(),
	}

	if err := b.storeEmail(email); err != nil {
		return "", err
	}

	b.relayEmail(email, out)

	return msgID, nil
}

func (b *InMemoryBackend) storeEmail(email Email) error {
	b.mu.Lock("SendEmail")
	defer b.mu.Unlock()

	if err := b.checkFromIdentityLocked(email.From); err != nil {
		return err
	}
	b.emails = append(b.emails, email)
	// Compact only at twice the cap so trimming stays amortized O(1) per send.
	if len(b.emails) >= emailCompactionHighWater {
		trimmed := make([]Email, maxRetainedEmails, emailCompactionHighWater)
		copy(trimmed, b.emails[len(b.emails)-maxRetainedEmails:])
		b.emails = trimmed
	}

	return nil
}

// checkFromIdentity verifies the from address against registered identities,
// acquiring b.mu itself. Used by callers that check identity once up front
// rather than per SendEmail call, e.g. SendBulkEmail.
func (b *InMemoryBackend) checkFromIdentity(from string) error {
	b.mu.RLock("checkFromIdentity")
	defer b.mu.RUnlock()

	return b.checkFromIdentityLocked(from)
}

// checkFromIdentityLocked verifies the from address against registered identities.
// It checks exact email match first, then the domain portion as a fallback.
// Must be called with b.mu held for writing or reading.
func (b *InMemoryBackend) checkFromIdentityLocked(from string) error {
	if id, ok := b.identities.Get(from); ok && id.VerifiedForSending {
		return nil
	}
	if _, domain, found := strings.CutLast(from, "@"); found {
		if id, ok := b.identities.Get(domain); ok && id.VerifiedForSending {
			return nil
		}
	}

	return fmt.Errorf("%w: identity not verified for sending: %s", ErrMailFromDomainNotVerified, from)
}

// ListEmails returns a copy of all captured emails.
func (b *InMemoryBackend) ListEmails() []Email {
	b.mu.RLock("ListEmails")
	defer b.mu.RUnlock()

	out := make([]Email, len(b.emails))
	copy(out, b.emails)

	return out
}

// ---- bulk email ----
//
// Field-diffed against aws-sdk-go-v2/service/sesv2/types' BulkEmailEntry/
// Destination/ReplacementEmailContent/ReplacementTemplate/MessageHeader/
// MessageTag. bulkEmailDestination/messageHeader/messageTag/
// replacementTemplate/replacementEmailContent/bulkEmailEntry replace what
// was previously a []map[string]any parsed with ad-hoc type assertions --
// functionally equivalent but with compile-time field-name safety.

// bulkEmailDestination mirrors types.Destination.
type bulkEmailDestination struct {
	ToAddresses  []string `json:"ToAddresses"`
	CcAddresses  []string `json:"CcAddresses"`
	BccAddresses []string `json:"BccAddresses"`
}

// messageHeader mirrors types.MessageHeader.
type messageHeader struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

// messageTag mirrors types.MessageTag.
type messageTag struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

// replacementTemplate mirrors types.ReplacementTemplate.
type replacementTemplate struct {
	ReplacementTemplateData string `json:"ReplacementTemplateData"`
}

// replacementEmailContent mirrors types.ReplacementEmailContent.
type replacementEmailContent struct {
	ReplacementTemplate *replacementTemplate `json:"ReplacementTemplate"`
}

// bulkEmailEntry mirrors types.BulkEmailEntry.
type bulkEmailEntry struct {
	Destination             bulkEmailDestination     `json:"Destination"`
	ReplacementEmailContent *replacementEmailContent `json:"ReplacementEmailContent"`
	ReplacementHeaders      []messageHeader          `json:"ReplacementHeaders"`
	ReplacementTags         []messageTag             `json:"ReplacementTags"`
}

// bulkEmailTemplate mirrors the subset of types.Template this emulator
// supports: inline content or a reference to a stored EmailTemplate, plus
// the default substitution data for {{var}} placeholders. Shared by
// SendEmail's EmailContent.Template and SendBulkEmail's DefaultContent.Template.
type bulkEmailTemplate struct {
	TemplateContent *EmailTemplateContent `json:"TemplateContent"`
	TemplateData    string                `json:"TemplateData"`
	TemplateName    string                `json:"TemplateName"`
}

// bulkEmailContent mirrors types.BulkEmailContent.
type bulkEmailContent struct {
	Template *bulkEmailTemplate `json:"Template"`
}

// SendBulkEmail sends bulk emails — records sent emails with actual recipients
// and content rendered from DefaultContent, with each entry's
// ReplacementEmailContent overriding substitution variables.
func (b *InMemoryBackend) SendBulkEmail(
	fromEmailAddress string,
	defaultContent *bulkEmailContent,
	bulkEmailEntries []bulkEmailEntry,
	defaultTags []messageTag,
	opts SendOptions,
) ([]bulkEmailEntryResultOutput, error) {
	if defaultContent == nil || defaultContent.Template == nil {
		return nil, fmt.Errorf("%w: DefaultContent.Template is required", ErrInvalidInput)
	}

	if err := b.validateSendOptions(fromEmailAddress, storedTemplateName(defaultContent.Template), opts); err != nil {
		return nil, err
	}

	baseSubject, baseHTML, baseText, defaultVars, err := b.resolveBulkTemplate(defaultContent.Template)
	if err != nil {
		return nil, err
	}

	if b.checkFromIdentity(fromEmailAddress) != nil {
		results := make([]bulkEmailEntryResultOutput, len(bulkEmailEntries))
		for i := range bulkEmailEntries {
			results[i] = bulkEmailEntryResultOutput{Status: keyStatusMailFromDomainNotVerified}
		}

		return results, nil
	}

	results := make([]bulkEmailEntryResultOutput, 0, len(bulkEmailEntries))

	for _, entry := range bulkEmailEntries {
		vars := defaultVars
		if entry.ReplacementEmailContent != nil && entry.ReplacementEmailContent.ReplacementTemplate != nil {
			replacementData := entry.ReplacementEmailContent.ReplacementTemplate.ReplacementTemplateData
			overrides, parseErr := parseTemplateVars(replacementData)
			if parseErr != nil {
				return nil, parseErr
			}
			vars = mergeTemplateVars(defaultVars, overrides)
		}

		subject := renderTemplateVars(baseSubject, vars)
		html := renderTemplateVars(baseHTML, vars)
		text := renderTemplateVars(baseText, vars)

		msgID, _ := b.SendMessage(OutboundEmail{
			From: fromEmailAddress, To: entry.Destination.ToAddresses, Cc: entry.Destination.CcAddresses,
			Bcc: entry.Destination.BccAddresses, Subject: subject, BodyHTML: html, BodyText: text,
			Tags: mergeMessageTags(defaultTags, entry.ReplacementTags), Options: opts,
		})
		if msgID == "" {
			msgID = "sesv2-bulk-" + uuid.New().String()
		}

		results = append(results, bulkEmailEntryResultOutput{
			MessageID: msgID,
			Status:    keyStatusSuccess,
		})
	}

	return results, nil
}

// resolveBulkTemplate resolves a bulkEmailTemplate to its base
// subject/HTML/text and default substitution vars. Inline TemplateContent
// takes precedence over a stored TemplateName lookup, matching the SDK doc
// for types.Template ("you will refer to this name ... unless you also
// provide the full template content in the request").
func (b *InMemoryBackend) resolveBulkTemplate(
	tmpl *bulkEmailTemplate,
) (string, string, string, map[string]string, error) {
	vars, err := parseTemplateVars(tmpl.TemplateData)
	if err != nil {
		return "", "", "", nil, err
	}

	content := tmpl.TemplateContent
	if content == nil {
		if tmpl.TemplateName == "" {
			return "", "", "", nil, fmt.Errorf(
				"%w: Template must specify TemplateContent or TemplateName", ErrInvalidInput,
			)
		}

		stored, lookupErr := b.GetEmailTemplate(tmpl.TemplateName)
		if lookupErr != nil {
			return "", "", "", nil, lookupErr
		}

		content = stored.TemplateContent
	}

	if content == nil {
		return "", "", "", vars, nil
	}

	return content.Subject, content.HTML, content.Text, vars, nil
}

// DeleteEmail removes the captured email with the given MessageID and reports whether it existed.
func (b *InMemoryBackend) DeleteEmail(messageID string) bool {
	b.mu.Lock("DeleteEmail")
	defer b.mu.Unlock()

	for i := range b.emails {
		if b.emails[i].MessageID == messageID {
			b.emails = append(b.emails[:i], b.emails[i+1:]...)

			return true
		}
	}

	return false
}

// ClearEmails drops every captured email.
func (b *InMemoryBackend) ClearEmails() {
	b.mu.Lock("ClearEmails")
	defer b.mu.Unlock()

	b.emails = nil
}

// mergeMessageTags applies replacement tags over defaults, replacing a default with the same Name.
func mergeMessageTags(defaults, replacements []messageTag) []MessageTag {
	out := make([]MessageTag, 0, len(defaults)+len(replacements))

	for _, d := range defaults {
		if !slices.ContainsFunc(replacements, func(r messageTag) bool { return r.Name == d.Name }) {
			out = append(out, MessageTag(d))
		}
	}

	for _, r := range replacements {
		out = append(out, MessageTag(r))
	}

	return out
}

func toMessageTags(in []messageTag) []MessageTag {
	return mergeMessageTags(nil, in)
}
