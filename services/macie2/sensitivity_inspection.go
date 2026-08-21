package macie2

import (
	"sort"

	"github.com/google/uuid"
)

const defaultTemplateName = "Default sensitivity inspection template"

func (b *InMemoryBackend) ensureDefaultTemplate() {
	if b.sensitivityTemplates.Len() > 0 {
		return
	}

	id := uuid.New().String()
	b.sensitivityTemplates.Put(&SensitivityInspectionTemplate{
		ID:   id,
		Name: defaultTemplateName,
	})
}

// GetSensitivityInspectionTemplate returns a sensitivity inspection template by ID.
func (b *InMemoryBackend) GetSensitivityInspectionTemplate(templateID string) (*SensitivityInspectionTemplate, error) {
	b.mu.Lock("GetSensitivityInspectionTemplate")
	defer b.mu.Unlock()

	b.ensureDefaultTemplate()

	tmpl, ok := b.sensitivityTemplates.Get(templateID)
	if !ok {
		return nil, ErrSensitivityTemplateNotFound
	}

	cp := *tmpl

	return &cp, nil
}

// ListSensitivityInspectionTemplates returns all templates.
func (b *InMemoryBackend) ListSensitivityInspectionTemplates() ([]*SensitivityInspectionTemplateSummary, error) {
	b.mu.Lock("ListSensitivityInspectionTemplates")
	defer b.mu.Unlock()

	b.ensureDefaultTemplate()

	templates := b.sensitivityTemplates.All()
	result := make([]*SensitivityInspectionTemplateSummary, 0, len(templates))

	for _, t := range templates {
		result = append(result, &SensitivityInspectionTemplateSummary{ID: t.ID, Name: t.Name})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })

	return result, nil
}

// UpdateSensitivityInspectionTemplate updates a template. The real input
// (types.UpdateSensitivityInspectionTemplateInput) carries Description,
// Excludes, and Includes as independently-optional pointers -- a client
// changing only one must not wipe the other two, so each is merged only
// when actually provided (gopherstack-c8ge).
func (b *InMemoryBackend) UpdateSensitivityInspectionTemplate(
	templateID, name string,
	description *string,
	excludes, includes map[string]any,
) error {
	b.mu.Lock("UpdateSensitivityInspectionTemplate")
	defer b.mu.Unlock()

	tmpl, ok := b.sensitivityTemplates.Get(templateID)
	if !ok {
		return ErrSensitivityTemplateNotFound
	}

	if name != "" {
		tmpl.Name = name
	}
	if description != nil {
		tmpl.Description = *description
	}
	if excludes != nil {
		tmpl.Excludes = excludes
	}
	if includes != nil {
		tmpl.Includes = includes
	}

	return nil
}
