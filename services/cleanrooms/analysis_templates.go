package cleanrooms

import (
	"fmt"
	"maps"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) analysisTemplateARN(membershipID, id string) string {
	return arn.Build(
		"cleanrooms",
		b.region,
		b.accountID,
		fmt.Sprintf("membership/%s/analysistemplate/%s", membershipID, id),
	)
}

func toAnalysisTemplateSummary(t *AnalysisTemplate) *AnalysisTemplateSummary {
	return &AnalysisTemplateSummary{
		AnalysisTemplateIdentifier: t.AnalysisTemplateIdentifier,
		Arn:                        t.Arn,
		CollaborationArn:           t.CollaborationArn,
		CollaborationIdentifier:    t.CollaborationIdentifier,
		MembershipIdentifier:       t.MembershipIdentifier,
		MembershipArn:              t.MembershipArn,
		Name:                       t.Name,
		Description:                t.Description,
		CreateTime:                 t.CreateTime,
		UpdateTime:                 t.UpdateTime,
		ID:                         t.ID,
		MembershipID:               t.MembershipID,
		CollaborationID:            t.CollaborationID,
	}
}

// toCollaborationAnalysisTemplateSummary builds the collaboration-scoped
// shape, which carries creatorAccountId in place of the membership-scoped
// membershipArn/membershipId (see CollaborationAnalysisTemplateSummary).
func toCollaborationAnalysisTemplateSummary(
	t *AnalysisTemplate, creatorAccountID string,
) *CollaborationAnalysisTemplateSummary {
	return &CollaborationAnalysisTemplateSummary{
		Arn:              t.Arn,
		CollaborationArn: t.CollaborationArn,
		CollaborationID:  t.CollaborationID,
		CreatorAccountID: creatorAccountID,
		ID:               t.ID,
		Name:             t.Name,
		CreateTime:       t.CreateTime,
		UpdateTime:       t.UpdateTime,
	}
}

func (b *InMemoryBackend) CreateAnalysisTemplate(
	membershipID, name, description, format string,
	source map[string]any,
	analysisParameters []map[string]any,
	tags map[string]string,
) (*AnalysisTemplate, error) {
	b.mu.Lock("CreateAnalysisTemplate")
	defer b.mu.Unlock()
	mem, ok := b.memberships.Get(membershipID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	collab, _ := b.collaborations.Get(mem.CollaborationID)
	var collabArn string
	if collab != nil {
		collabArn = collab.Arn
	}
	tmpl := &AnalysisTemplate{
		AnalysisTemplateIdentifier: id,
		Arn:                        b.analysisTemplateARN(membershipID, id),
		CollaborationArn:           collabArn,
		CollaborationIdentifier:    mem.CollaborationID,
		MembershipIdentifier:       membershipID,
		MembershipArn:              mem.Arn,
		Name:                       name,
		Description:                description,
		Format:                     format,
		Source:                     source,
		AnalysisParameters:         analysisParameters,
		CreateTime:                 ts,
		UpdateTime:                 ts,
		Tags:                       tags,
		ID:                         id,
		MembershipID:               membershipID,
		CollaborationID:            mem.CollaborationID,
	}
	b.analysisTemplates.Put(tmpl)
	if len(tags) > 0 {
		b.tagsByArn[tmpl.Arn] = maps.Clone(tags)
	}

	return tmpl, nil
}

func (b *InMemoryBackend) GetAnalysisTemplate(
	membershipID, templateID string,
) (*AnalysisTemplate, error) {
	b.mu.RLock("GetAnalysisTemplate")
	defer b.mu.RUnlock()
	tmpl, ok := b.analysisTemplates.Get(membershipKey(membershipID, templateID))
	if !ok {
		return nil, ErrNotFound
	}

	return tmpl, nil
}

func (b *InMemoryBackend) ListAnalysisTemplates(
	membershipID, maxResults, nextToken string,
) ([]*AnalysisTemplateSummary, string, error) {
	b.mu.RLock("ListAnalysisTemplates")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listItems(
		b.analysisTemplatesByMembership.Get(membershipID),
		nil,
		toAnalysisTemplateSummary,
		func(a, c *AnalysisTemplateSummary) bool {
			return a.ID < c.ID
		},
		maxResults, nextToken,
	)

	return page, next, nil
}

func (b *InMemoryBackend) UpdateAnalysisTemplate(
	membershipID, templateID, description string,
) (*AnalysisTemplate, error) {
	b.mu.Lock("UpdateAnalysisTemplate")
	defer b.mu.Unlock()
	tmpl, ok := b.analysisTemplates.Get(membershipKey(membershipID, templateID))
	if !ok {
		return nil, ErrNotFound
	}
	tmpl.Description = description
	tmpl.UpdateTime = b.now()

	return tmpl, nil
}

func (b *InMemoryBackend) DeleteAnalysisTemplate(membershipID, templateID string) error {
	b.mu.Lock("DeleteAnalysisTemplate")
	defer b.mu.Unlock()
	key := membershipKey(membershipID, templateID)
	tmpl, ok := b.analysisTemplates.Get(key)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, tmpl.Arn)
	b.analysisTemplates.Delete(key)

	return nil
}

func (b *InMemoryBackend) GetCollaborationAnalysisTemplate(
	collaborationID, templateArn string,
) (*AnalysisTemplate, error) {
	b.mu.RLock("GetCollaborationAnalysisTemplate")
	defer b.mu.RUnlock()
	var found *AnalysisTemplate
	b.analysisTemplates.Range(func(t *AnalysisTemplate) bool {
		if t.CollaborationID == collaborationID && t.Arn == templateArn {
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

func (b *InMemoryBackend) ListCollaborationAnalysisTemplates(
	collaborationID, maxResults, nextToken string,
) ([]*CollaborationAnalysisTemplateSummary, string, error) {
	b.mu.RLock("ListCollaborationAnalysisTemplates")
	defer b.mu.RUnlock()
	collab, ok := b.collaborations.Get(collaborationID)
	if !ok {
		return nil, "", ErrNotFound
	}
	page, next := listNestedItems(
		b.analysisTemplates.All(),
		func(t *AnalysisTemplate) bool { return t.CollaborationID == collaborationID },
		func(t *AnalysisTemplate) *CollaborationAnalysisTemplateSummary {
			return toCollaborationAnalysisTemplateSummary(t, collab.CreatorAccountID)
		},
		func(a, c *CollaborationAnalysisTemplateSummary) bool {
			return a.ID < c.ID
		},
		maxResults, nextToken,
	)

	return page, next, nil
}

func (b *InMemoryBackend) BatchGetCollaborationAnalysisTemplate(
	collaborationID string,
	templateArns []string,
) ([]*AnalysisTemplate, []BatchError, error) {
	b.mu.RLock("BatchGetCollaborationAnalysisTemplate")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, nil, ErrNotFound
	}
	all := b.analysisTemplates.All()
	var results []*AnalysisTemplate
	var errors []BatchError
	for _, arnStr := range templateArns {
		found := false
		for _, t := range all {
			if t.CollaborationID == collaborationID && t.Arn == arnStr {
				results = append(results, t)
				found = true

				break
			}
		}
		if !found {
			errors = append(
				errors,
				BatchError{Arn: arnStr, Code: errCodeNotFound, Message: errMsgNotFound},
			)
		}
	}

	return results, errors, nil
}
