package sesv2

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CustomVerificationEmailTemplate represents a custom verification email template.
type CustomVerificationEmailTemplate struct {
	TemplateName          string `json:"templateName"`
	FromEmailAddress      string `json:"fromEmailAddress"`
	TemplateSubject       string `json:"templateSubject"`
	TemplateContent       string `json:"templateContent"`
	SuccessRedirectionURL string `json:"successRedirectionURL"`
	FailureRedirectionURL string `json:"failureRedirectionURL"`
}

// CreateCustomVerificationEmailTemplate creates a custom verification email template.
func (b *InMemoryBackend) CreateCustomVerificationEmailTemplate(
	in *CustomVerificationEmailTemplate,
) (*CustomVerificationEmailTemplate, error) {
	b.mu.Lock("CreateCustomVerificationEmailTemplate")
	defer b.mu.Unlock()

	if b.customVerificationTemplates.Has(in.TemplateName) {
		return nil, fmt.Errorf(
			"%w: custom verification template %s already exists",
			ErrAlreadyExists, in.TemplateName,
		)
	}

	cp := *in
	b.customVerificationTemplates.Put(&cp)

	out := *in

	return &out, nil
}

// ---- custom verification templates ----

// GetCustomVerificationEmailTemplate retrieves a custom verification template.
func (b *InMemoryBackend) GetCustomVerificationEmailTemplate(
	name string,
) (*CustomVerificationEmailTemplate, error) {
	b.mu.RLock("GetCustomVerificationEmailTemplate")
	defer b.mu.RUnlock()

	t, ok := b.customVerificationTemplates.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: custom verification template %s not found", ErrNotFound, name)
	}

	cp := *t

	return &cp, nil
}

// DeleteCustomVerificationEmailTemplate removes a custom verification template.
func (b *InMemoryBackend) DeleteCustomVerificationEmailTemplate(name string) error {
	b.mu.Lock("DeleteCustomVerificationEmailTemplate")
	defer b.mu.Unlock()

	if !b.customVerificationTemplates.Has(name) {
		return fmt.Errorf("%w: custom verification template %s not found", ErrNotFound, name)
	}

	b.customVerificationTemplates.Delete(name)

	return nil
}

// UpdateCustomVerificationEmailTemplate updates a custom verification template.
func (b *InMemoryBackend) UpdateCustomVerificationEmailTemplate(
	in *CustomVerificationEmailTemplate,
) error {
	b.mu.Lock("UpdateCustomVerificationEmailTemplate")
	defer b.mu.Unlock()

	if !b.customVerificationTemplates.Has(in.TemplateName) {
		return fmt.Errorf(
			"%w: custom verification template %s not found",
			ErrNotFound,
			in.TemplateName,
		)
	}

	cp := *in
	b.customVerificationTemplates.Put(&cp)

	return nil
}

// ListCustomVerificationEmailTemplates returns all custom verification templates.
func (b *InMemoryBackend) ListCustomVerificationEmailTemplates(
	nextToken string,
	pageSize int,
) page.Page[*CustomVerificationEmailTemplate] {
	b.mu.RLock("ListCustomVerificationEmailTemplates")
	defer b.mu.RUnlock()

	snap := b.customVerificationTemplates.Snapshot()

	items := make([]*CustomVerificationEmailTemplate, 0, len(snap))
	for _, t := range snap {
		cp := *t
		items = append(items, &cp)
	}

	return page.New(items, nextToken, pageSize, sesv2DefaultMaxItems)
}

// SendCustomVerificationEmail sends a custom verification email (stub).
func (b *InMemoryBackend) SendCustomVerificationEmail(
	emailAddress, templateName string,
) (string, error) {
	b.mu.RLock("SendCustomVerificationEmail")

	ok := b.customVerificationTemplates.Has(templateName)
	b.mu.RUnlock()

	if !ok {
		return "", fmt.Errorf(
			"%w: custom verification template %s not found",
			ErrNotFound,
			templateName,
		)
	}

	msgID := "sesv2-cvr-" + uuid.New().String()

	email := Email{
		MessageID: msgID,
		From:      "noreply@example.com",
		To:        []string{emailAddress},
		Subject:   "Verify your email",
		Timestamp: time.Now(),
	}

	b.mu.Lock("SendCustomVerificationEmail-record")
	b.emails = append(b.emails, email)
	if len(b.emails) >= emailCompactionHighWater {
		trimmed := make([]Email, maxRetainedEmails, emailCompactionHighWater)
		copy(trimmed, b.emails[len(b.emails)-maxRetainedEmails:])
		b.emails = trimmed
	}
	b.mu.Unlock()

	return msgID, nil
}
