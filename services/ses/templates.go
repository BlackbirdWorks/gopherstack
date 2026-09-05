package ses

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateTemplate stores a new email template. Returns ErrTemplateExists if the name is taken.
func (b *InMemoryBackend) CreateTemplate(tmpl EmailTemplate) error {
	if strings.TrimSpace(tmpl.TemplateName) == "" {
		return fmt.Errorf("%w: TemplateName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateTemplate")
	defer b.mu.Unlock()

	if b.templates.Has(tmpl.TemplateName) {
		return fmt.Errorf("%w: template %s already exists", ErrTemplateExists, tmpl.TemplateName)
	}

	b.templates.Put(&tmpl)

	return nil
}

// UpdateTemplate overwrites an existing template. Returns ErrTemplateNotFound if it does not exist.
func (b *InMemoryBackend) UpdateTemplate(tmpl EmailTemplate) error {
	if strings.TrimSpace(tmpl.TemplateName) == "" {
		return fmt.Errorf("%w: TemplateName is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateTemplate")
	defer b.mu.Unlock()

	if !b.templates.Has(tmpl.TemplateName) {
		return fmt.Errorf("%w: %s", ErrTemplateNotFound, tmpl.TemplateName)
	}

	b.templates.Put(&tmpl)

	return nil
}

// GetTemplate returns the named template or ErrTemplateNotFound.
func (b *InMemoryBackend) GetTemplate(name string) (EmailTemplate, error) {
	b.mu.RLock("GetTemplate")
	defer b.mu.RUnlock()

	tmpl, ok := b.templates.Get(name)
	if !ok {
		return EmailTemplate{}, fmt.Errorf("%w: %s", ErrTemplateNotFound, name)
	}

	return *tmpl, nil
}

// DeleteTemplate removes the named template. Idempotent — missing template returns success.
func (b *InMemoryBackend) DeleteTemplate(name string) {
	b.mu.Lock("DeleteTemplate")
	defer b.mu.Unlock()

	b.templates.Delete(name)
}

// listTemplatesDefaultMaxItems/listTemplatesMaxItemsCap are this op's own
// documented default and ceiling (api_op_ListTemplates.go: "must be at least
// 1 and less than or equal to 100... If more than 100 items are requested,
// the page size will automatically set to 100. If you do not specify a
// value, 10 is the default page size") -- distinct from sesDefaultMaxItems
// (100), which every other List* op here defaults to.
const (
	listTemplatesDefaultMaxItems = 10
	listTemplatesMaxItemsCap     = 100
)

// ListTemplates returns template names sorted alphabetically, with pagination.
func (b *InMemoryBackend) ListTemplates(nextToken string, maxItems int) page.Page[string] {
	b.mu.RLock("ListTemplates")
	defer b.mu.RUnlock()

	snap := b.templates.Snapshot()
	names := make([]string, len(snap))

	for i, tmpl := range snap {
		names[i] = tmpl.TemplateName
	}

	if maxItems > listTemplatesMaxItemsCap {
		maxItems = listTemplatesMaxItemsCap
	}

	return page.New(names, nextToken, maxItems, listTemplatesDefaultMaxItems)
}

// parseTemplateData parses the JSON template-data document into a flat
// string-keyed variable map for {{key}} substitution. AWS SES requires
// TemplateData to be a valid JSON object; an empty/blank document yields no
// variables, while malformed JSON is rejected by the caller via the returned
// error so callers can surface InvalidParameterValue, matching real SES.
func parseTemplateData(templateData string) (map[string]string, error) {
	vars := map[string]string{}

	if strings.TrimSpace(templateData) == "" {
		return vars, nil
	}

	raw := map[string]any{}
	if err := json.Unmarshal([]byte(templateData), &raw); err != nil {
		return nil, fmt.Errorf("%w: TemplateData must be valid JSON", ErrInvalidParameter)
	}

	for k, v := range raw {
		vars[k] = fmt.Sprintf("%v", v)
	}

	return vars, nil
}

// mergeTemplateData layers replacement template data on top of default template
// data, matching SendBulkTemplatedEmail semantics where per-destination
// ReplacementTemplateData overrides the request-level DefaultTemplateData.
func mergeTemplateData(defaultData, replacementData string) (map[string]string, error) {
	vars, err := parseTemplateData(defaultData)
	if err != nil {
		return nil, err
	}

	repl, err := parseTemplateData(replacementData)
	if err != nil {
		return nil, err
	}

	maps.Copy(vars, repl)

	return vars, nil
}

// TestRenderTemplate renders the named template with the given JSON template data.
// Variable substitution uses {{key}} syntax matching AWS SES Handlebars-style templating.
func (b *InMemoryBackend) TestRenderTemplate(templateName, templateData string) (string, error) {
	tmpl, err := b.GetTemplate(templateName)
	if err != nil {
		return "", err
	}

	vars, err := parseTemplateData(templateData)
	if err != nil {
		return "", err
	}

	parts := []string{tmpl.SubjectPart, tmpl.TextPart, tmpl.HTMLPart}
	rendered := make([]string, len(parts))

	for i, p := range parts {
		rendered[i] = renderTemplateVars(p, vars)
	}

	return strings.Join(rendered, "\n---\n"), nil
}

// renderTemplateVars replaces {{key}} placeholders with values from vars.
func renderTemplateVars(s string, vars map[string]string) string {
	for k, v := range vars {
		s = strings.ReplaceAll(s, "{{"+k+"}}", v)
	}

	return s
}
