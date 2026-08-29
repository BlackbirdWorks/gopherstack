package ses

import (
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// customVerifTemplateMaxResults is the real op's hard cap: MaxResults must
// be 1-50, and an absent or out-of-range value falls back to 50 (own doc
// comment, api_op_ListCustomVerificationEmailTemplates.go).
const customVerifTemplateMaxResults = 50

// CreateCustomVerificationEmailTemplate creates a custom verification email template.
func (b *InMemoryBackend) CreateCustomVerificationEmailTemplate(tmpl CustomVerificationEmailTemplate) error {
	if strings.TrimSpace(tmpl.TemplateName) == "" {
		return fmt.Errorf("%w: TemplateName is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(tmpl.FromEmailAddress) == "" {
		return fmt.Errorf("%w: FromEmailAddress is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(tmpl.TemplateSubject) == "" {
		return fmt.Errorf("%w: TemplateSubject is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(tmpl.TemplateContent) == "" {
		return fmt.Errorf("%w: TemplateContent is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(tmpl.SuccessRedirectionURL) == "" {
		return fmt.Errorf("%w: SuccessRedirectionURL is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(tmpl.FailureRedirectionURL) == "" {
		return fmt.Errorf("%w: FailureRedirectionURL is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateCustomVerificationEmailTemplate")
	defer b.mu.Unlock()

	if b.customVerifTemplates.Has(tmpl.TemplateName) {
		return fmt.Errorf(
			"%w: custom verification email template %s already exists",
			ErrCustomVerifTemplateExists,
			tmpl.TemplateName,
		)
	}

	t := tmpl
	b.customVerifTemplates.Put(&t)

	return nil
}

// DeleteCustomVerificationEmailTemplate removes a custom verification email template.
func (b *InMemoryBackend) DeleteCustomVerificationEmailTemplate(templateName string) error {
	if strings.TrimSpace(templateName) == "" {
		return fmt.Errorf("%w: TemplateName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteCustomVerificationEmailTemplate")
	defer b.mu.Unlock()

	if !b.customVerifTemplates.Has(templateName) {
		return fmt.Errorf("%w: %s", ErrCustomVerifTemplateNotFound, templateName)
	}

	b.customVerifTemplates.Delete(templateName)

	return nil
}

// GetCustomVerificationEmailTemplate returns the named custom verification email template.
func (b *InMemoryBackend) GetCustomVerificationEmailTemplate(
	templateName string,
) (CustomVerificationEmailTemplate, error) {
	if strings.TrimSpace(templateName) == "" {
		return CustomVerificationEmailTemplate{}, fmt.Errorf("%w: TemplateName is required", ErrInvalidParameter)
	}
	b.mu.RLock("GetCustomVerificationEmailTemplate")
	defer b.mu.RUnlock()
	tmpl, exists := b.customVerifTemplates.Get(templateName)
	if !exists {
		return CustomVerificationEmailTemplate{}, fmt.Errorf("%w: %s", ErrCustomVerifTemplateNotFound, templateName)
	}

	return *tmpl, nil
}

// ListCustomVerificationEmailTemplates returns a page of custom
// verification email templates sorted by name.
func (b *InMemoryBackend) ListCustomVerificationEmailTemplates(
	nextToken string, maxResults int,
) page.Page[CustomVerificationEmailTemplate] {
	b.mu.RLock("ListCustomVerificationEmailTemplates")
	defer b.mu.RUnlock()
	out := make([]CustomVerificationEmailTemplate, 0, b.customVerifTemplates.Len())
	for _, t := range b.customVerifTemplates.All() {
		out = append(out, *t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TemplateName < out[j].TemplateName })

	if maxResults < 1 || maxResults > customVerifTemplateMaxResults {
		maxResults = customVerifTemplateMaxResults
	}

	return page.New(out, nextToken, maxResults, customVerifTemplateMaxResults)
}

// SendCustomVerificationEmail adds email to the account's identity list and
// attempts to verify it (matching this backend's instant-verification
// convention shared by VerifyEmailIdentity/VerifyEmailAddress) and sends a
// verification email using the named custom template. templateName must
// reference an existing custom verification email template
// (CreateCustomVerificationEmailTemplate), and configurationSetName, if
// supplied, must reference an existing configuration set — both required
// preconditions on the real AWS SES SendCustomVerificationEmailInput.
func (b *InMemoryBackend) SendCustomVerificationEmail(
	email, templateName, configurationSetName string,
) (string, error) {
	if strings.TrimSpace(email) == "" {
		return "", fmt.Errorf("%w: EmailAddress is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(templateName) == "" {
		return "", fmt.Errorf("%w: TemplateName is required", ErrInvalidParameter)
	}

	if _, err := b.GetCustomVerificationEmailTemplate(templateName); err != nil {
		return "", err
	}

	if configurationSetName != "" {
		b.mu.RLock("SendCustomVerificationEmail")
		exists := b.configSets.Has(configurationSetName)
		b.mu.RUnlock()

		if !exists {
			return "", fmt.Errorf("%w: %s", ErrConfigSetNotFound, configurationSetName)
		}
	}

	b.mu.Lock("SendCustomVerificationEmail")
	if rec, ok := b.identities.Get(email); ok {
		rec.Verified = true
	} else {
		b.identities.Put(&IdentityRecord{Identity: email, Verified: true, ForwardingEnabled: true})
	}
	b.mu.Unlock()

	return "ses-verif-" + uuid.New().String(), nil
}

// UpdateCustomVerificationEmailTemplate updates an existing custom verification email template.
func (b *InMemoryBackend) UpdateCustomVerificationEmailTemplate(tmpl CustomVerificationEmailTemplate) error {
	if strings.TrimSpace(tmpl.TemplateName) == "" {
		return fmt.Errorf("%w: TemplateName is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateCustomVerificationEmailTemplate")
	defer b.mu.Unlock()

	if !b.customVerifTemplates.Has(tmpl.TemplateName) {
		return fmt.Errorf("%w: %s", ErrCustomVerifTemplateNotFound, tmpl.TemplateName)
	}

	t := tmpl
	b.customVerifTemplates.Put(&t)

	return nil
}
