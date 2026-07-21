package cleanrooms

import (
	"fmt"
	"maps"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) privacyBudgetTemplateARN(membershipID, id string) string {
	return arn.Build(
		"cleanrooms",
		b.region,
		b.accountID,
		fmt.Sprintf("membership/%s/privacybudgettemplate/%s", membershipID, id),
	)
}

func toPrivacyBudgetTemplateSummary(t *PrivacyBudgetTemplate) *PrivacyBudgetTemplateSummary {
	return &PrivacyBudgetTemplateSummary{
		PrivacyBudgetTemplateIdentifier: t.PrivacyBudgetTemplateIdentifier,
		Arn:                             t.Arn,
		CollaborationArn:                t.CollaborationArn,
		CollaborationIdentifier:         t.CollaborationIdentifier,
		MembershipArn:                   t.MembershipArn,
		MembershipIdentifier:            t.MembershipIdentifier,
		PrivacyBudgetType:               t.PrivacyBudgetType,
		CreateTime:                      t.CreateTime,
		UpdateTime:                      t.UpdateTime,
		ID:                              t.PrivacyBudgetTemplateIdentifier,
		MembershipID:                    t.MembershipIdentifier,
		CollaborationID:                 t.CollaborationIdentifier,
	}
}

func (b *InMemoryBackend) CreatePrivacyBudgetTemplate(
	membershipID, privacyBudgetType, autoRefresh string,
	parameters map[string]any,
	tags map[string]string,
) (*PrivacyBudgetTemplate, error) {
	b.mu.Lock("CreatePrivacyBudgetTemplate")
	defer b.mu.Unlock()
	mem, ok := b.memberships.Get(membershipID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	collab, _ := b.collaborations.Get(mem.CollaborationIdentifier)
	var collabArn string
	if collab != nil {
		collabArn = collab.Arn
	}
	tmpl := &PrivacyBudgetTemplate{
		PrivacyBudgetTemplateIdentifier: id,
		Arn:                             b.privacyBudgetTemplateARN(membershipID, id),
		CollaborationArn:                collabArn,
		CollaborationIdentifier:         mem.CollaborationIdentifier,
		MembershipArn:                   mem.Arn,
		MembershipIdentifier:            membershipID,
		PrivacyBudgetType:               privacyBudgetType,
		AutoRefresh:                     autoRefresh,
		Parameters:                      parameters,
		CreateTime:                      ts,
		UpdateTime:                      ts,
		Tags:                            tags,
		ID:                              id,
		MembershipID:                    membershipID,
		CollaborationID:                 mem.CollaborationIdentifier,
	}
	b.privacyBudgetTemplates.Put(tmpl)
	if len(tags) > 0 {
		b.tagsByArn[tmpl.Arn] = maps.Clone(tags)
	}

	return tmpl, nil
}

func (b *InMemoryBackend) GetPrivacyBudgetTemplate(
	membershipID, templateID string,
) (*PrivacyBudgetTemplate, error) {
	b.mu.RLock("GetPrivacyBudgetTemplate")
	defer b.mu.RUnlock()
	tmpl, ok := b.privacyBudgetTemplates.Get(membershipKey(membershipID, templateID))
	if !ok {
		return nil, ErrNotFound
	}

	return tmpl, nil
}

func (b *InMemoryBackend) ListPrivacyBudgetTemplates(
	membershipID, privacyBudgetType, maxResults, nextToken string,
) ([]*PrivacyBudgetTemplateSummary, string, error) {
	b.mu.RLock("ListPrivacyBudgetTemplates")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listItems(
		b.privacyBudgetTemplatesByMembership.Get(membershipID),
		func(t *PrivacyBudgetTemplate) bool {
			return privacyBudgetType == "" || t.PrivacyBudgetType == privacyBudgetType
		},
		toPrivacyBudgetTemplateSummary,
		func(a, c *PrivacyBudgetTemplateSummary) bool {
			return a.PrivacyBudgetTemplateIdentifier < c.PrivacyBudgetTemplateIdentifier
		},
		maxResults, nextToken,
	)

	return page, next, nil
}

func (b *InMemoryBackend) UpdatePrivacyBudgetTemplate(
	membershipID, templateID, autoRefresh string,
	parameters map[string]any,
) (*PrivacyBudgetTemplate, error) {
	b.mu.Lock("UpdatePrivacyBudgetTemplate")
	defer b.mu.Unlock()
	tmpl, ok := b.privacyBudgetTemplates.Get(membershipKey(membershipID, templateID))
	if !ok {
		return nil, ErrNotFound
	}
	if autoRefresh != "" {
		tmpl.AutoRefresh = autoRefresh
	}
	if parameters != nil {
		tmpl.Parameters = parameters
	}
	tmpl.UpdateTime = b.now()

	return tmpl, nil
}

func (b *InMemoryBackend) DeletePrivacyBudgetTemplate(membershipID, templateID string) error {
	b.mu.Lock("DeletePrivacyBudgetTemplate")
	defer b.mu.Unlock()
	key := membershipKey(membershipID, templateID)
	tmpl, ok := b.privacyBudgetTemplates.Get(key)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, tmpl.Arn)
	b.privacyBudgetTemplates.Delete(key)

	return nil
}

func (b *InMemoryBackend) ListPrivacyBudgets(
	membershipID, _, _, _ string,
) ([]*PrivacyBudget, string, error) {
	b.mu.RLock("ListPrivacyBudgets")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}

	return []*PrivacyBudget{}, "", nil
}

func (b *InMemoryBackend) ListCollaborationPrivacyBudgets(
	collaborationID, _, _, _ string,
) ([]*PrivacyBudget, string, error) {
	b.mu.RLock("ListCollaborationPrivacyBudgets")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, "", ErrNotFound
	}

	return []*PrivacyBudget{}, "", nil
}

func (b *InMemoryBackend) GetCollaborationPrivacyBudgetTemplate(
	collaborationID, templateID string,
) (*PrivacyBudgetTemplate, error) {
	b.mu.RLock("GetCollaborationPrivacyBudgetTemplate")
	defer b.mu.RUnlock()
	var found *PrivacyBudgetTemplate
	b.privacyBudgetTemplates.Range(func(t *PrivacyBudgetTemplate) bool {
		if t.CollaborationIdentifier == collaborationID && t.PrivacyBudgetTemplateIdentifier == templateID {
			found = t

			return false
		}

		return true
	})
	if found == nil {
		return nil, ErrNotFound
	}

	return found, nil
}

func (b *InMemoryBackend) ListCollaborationPrivacyBudgetTemplates(
	collaborationID, maxResults, nextToken string,
) ([]*PrivacyBudgetTemplateSummary, string, error) {
	b.mu.RLock("ListCollaborationPrivacyBudgetTemplates")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listNestedItems(
		b.privacyBudgetTemplates.All(),
		func(t *PrivacyBudgetTemplate) bool { return t.CollaborationIdentifier == collaborationID },
		toPrivacyBudgetTemplateSummary,
		func(a, c *PrivacyBudgetTemplateSummary) bool {
			return a.PrivacyBudgetTemplateIdentifier < c.PrivacyBudgetTemplateIdentifier
		},
		maxResults, nextToken,
	)

	return page, next, nil
}

func (b *InMemoryBackend) PreviewPrivacyImpact(
	membershipID string,
	_ map[string]any,
) (map[string]any, error) {
	b.mu.RLock("PreviewPrivacyImpact")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, ErrNotFound
	}

	return map[string]any{"privacyImpact": map[string]any{"aggregationCount": []any{}}}, nil
}
