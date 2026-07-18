package sesv2

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// EmailTemplateContent holds the content for an email template.
type EmailTemplateContent struct {
	Subject string `json:"subject"`
	Text    string `json:"text"`
	HTML    string `json:"html"`
}

// EmailTemplate represents a SES v2 email template.
type EmailTemplate struct {
	CreatedAt       time.Time             `json:"createdAt"`
	TemplateContent *EmailTemplateContent `json:"templateContent"`
	TemplateName    string                `json:"templateName"`
}

// CreateEmailTemplate creates a new email template.
func (b *InMemoryBackend) CreateEmailTemplate(
	templateName string,
	content *EmailTemplateContent,
) (*EmailTemplate, error) {
	if strings.TrimSpace(templateName) == "" {
		return nil, fmt.Errorf("%w: TemplateName is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateEmailTemplate")
	defer b.mu.Unlock()

	if b.emailTemplates.Has(templateName) {
		return nil, fmt.Errorf(
			"%w: email template %s already exists",
			ErrAlreadyExists,
			templateName,
		)
	}

	var contentCopy *EmailTemplateContent
	if content != nil {
		c := *content
		contentCopy = &c
	}

	t := &EmailTemplate{
		TemplateName:    templateName,
		TemplateContent: contentCopy,
		CreatedAt:       time.Now(),
	}
	b.emailTemplates.Put(t)

	cp := *t

	return &cp, nil
}

// ---- email templates ----

// GetEmailTemplate retrieves a template.
func (b *InMemoryBackend) GetEmailTemplate(name string) (*EmailTemplate, error) {
	b.mu.RLock("GetEmailTemplate")
	defer b.mu.RUnlock()

	t, ok := b.emailTemplates.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: email template %s not found", ErrNotFound, name)
	}

	cp := *t

	return &cp, nil
}

// DeleteEmailTemplate removes a template.
func (b *InMemoryBackend) DeleteEmailTemplate(name string) error {
	b.mu.Lock("DeleteEmailTemplate")
	defer b.mu.Unlock()

	if !b.emailTemplates.Has(name) {
		return fmt.Errorf("%w: email template %s not found", ErrNotFound, name)
	}

	b.emailTemplates.Delete(name)

	return nil
}

// UpdateEmailTemplate updates a template.
func (b *InMemoryBackend) UpdateEmailTemplate(name string, content *EmailTemplateContent) error {
	b.mu.Lock("UpdateEmailTemplate")
	defer b.mu.Unlock()

	t, ok := b.emailTemplates.Get(name)
	if !ok {
		return fmt.Errorf("%w: email template %s not found", ErrNotFound, name)
	}

	if content != nil {
		cp := *content
		t.TemplateContent = &cp
	}

	return nil
}

// ListEmailTemplates returns all email templates.
func (b *InMemoryBackend) ListEmailTemplates(
	nextToken string,
	pageSize int,
) page.Page[*EmailTemplate] {
	b.mu.RLock("ListEmailTemplates")
	defer b.mu.RUnlock()

	snap := b.emailTemplates.Snapshot()

	items := make([]*EmailTemplate, 0, len(snap))
	for _, t := range snap {
		cp := *t
		items = append(items, &cp)
	}

	return page.New(items, nextToken, pageSize, sesv2DefaultMaxItems)
}

// TestRenderEmailTemplate renders a template with merge data.
func (b *InMemoryBackend) TestRenderEmailTemplate(name, templateData string) (string, error) {
	b.mu.RLock("TestRenderEmailTemplate")
	defer b.mu.RUnlock()

	t, ok := b.emailTemplates.Get(name)
	if !ok {
		return "", fmt.Errorf("%w: email template %s not found", ErrNotFound, name)
	}

	vars := map[string]string{}
	if strings.TrimSpace(templateData) != "" {
		raw := map[string]any{}
		if err := json.Unmarshal([]byte(templateData), &raw); err != nil {
			return "", fmt.Errorf("%w: TemplateData must be valid JSON", ErrInvalidInput)
		}
		for k, v := range raw {
			vars[k] = fmt.Sprintf("%v", v)
		}
	}

	renderVars := func(s string) string {
		for k, v := range vars {
			s = strings.ReplaceAll(s, "{{"+k+"}}", v)
		}

		return s
	}

	subject, html, text := "", "", ""
	if t.TemplateContent != nil {
		subject = renderVars(t.TemplateContent.Subject)
		html = renderVars(t.TemplateContent.HTML)
		text = renderVars(t.TemplateContent.Text)
	}

	return strings.Join([]string{subject, html, text}, "\n---\n"), nil
}
