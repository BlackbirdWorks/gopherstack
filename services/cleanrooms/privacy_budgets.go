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
		ID:                              t.ID,
		MembershipID:                    t.MembershipID,
		CollaborationID:                 t.CollaborationID,
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
	collab, _ := b.collaborations.Get(mem.CollaborationID)
	var collabArn string
	if collab != nil {
		collabArn = collab.Arn
	}
	tmpl := &PrivacyBudgetTemplate{
		PrivacyBudgetTemplateIdentifier: id,
		Arn:                             b.privacyBudgetTemplateARN(membershipID, id),
		CollaborationArn:                collabArn,
		CollaborationIdentifier:         mem.CollaborationID,
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
		CollaborationID:                 mem.CollaborationID,
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
			return a.ID < c.ID
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

// privacyBudgetTypeDifferentialPrivacy is the only PrivacyBudgetType this
// backend models a real budget for (types.PrivacyBudgetTypeDifferentialPrivacy).
// ACCESS_BUDGET (types.PrivacyBudgetTypeAccessBudget) is a real, separate
// budget kind this backend does not model -- see PARITY.md gaps.
const privacyBudgetTypeDifferentialPrivacy = "DIFFERENTIAL_PRIVACY"

// dpAggregationScaleFactor converts an epsilon/usersNoisePerQuery pair into a
// deterministic aggregation-count budget. AWS's real formula is proprietary
// and undocumented in the SDK; this is a documented, order-preserving
// approximation (more epsilon or less noise-per-query yields a larger
// budget), matching this codebase's established approximation pattern (e.g.
// cloudwatch's StatisticSet percentile synthesis) -- not a claim of numeric
// parity with real AWS.
const dpAggregationScaleFactor = 100

// differentialPrivacyAggregationTypes are the 5 real
// DifferentialPrivacyAggregationType enum values (types/enums.go).
func differentialPrivacyAggregationTypes() []string {
	return []string{"AVG", "COUNT", "COUNT_DISTINCT", "SUM", "STDDEV"}
}

// asInt64 converts a decoded-JSON numeric value (float64 from encoding/json,
// or occasionally an int/int64 when constructed directly by a test) to int64.
func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}

// extractDPEpsilonNoise pulls epsilon/usersNoisePerQuery out of a
// PrivacyBudgetTemplateParametersInput-shaped map
// ({"differentialPrivacy": {"epsilon": N, "usersNoisePerQuery": M}}, the
// real wire shape verified against
// awsRestjson1_serializeDocumentDifferentialPrivacyTemplateParametersInput).
func extractDPEpsilonNoise(parameters map[string]any) (int64, int64, bool) {
	dp, isMap := parameters["differentialPrivacy"].(map[string]any)
	if !isMap {
		return 0, 0, false
	}

	epsilon, epsilonOK := asInt64(dp["epsilon"])
	noise, noiseOK := asInt64(dp["usersNoisePerQuery"])

	return epsilon, noise, epsilonOK && noiseOK
}

// dpMaxAggregationCount derives the per-aggregation-type budget from
// epsilon/usersNoisePerQuery (see dpAggregationScaleFactor).
func dpMaxAggregationCount(epsilon, usersNoisePerQuery int64) int64 {
	if usersNoisePerQuery <= 0 || epsilon <= 0 {
		return 0
	}

	return epsilon * dpAggregationScaleFactor / usersNoisePerQuery
}

// differentialPrivacyBudgetPayload builds the real DifferentialPrivacyPrivacyBudget
// wire shape ({"differentialPrivacy": {"epsilon": N, "aggregations": [...]}}, see
// awsRestjson1_deserializeDocumentDifferentialPrivacyPrivacyBudget). No query-time
// consumption tracking exists in this backend (StartProtectedQuery's differentialPrivacy
// parameter is not modeled, see PARITY.md), so remainingCount always equals maxCount --
// a fresh, unconsumed budget, not a fabricated partial one.
func differentialPrivacyBudgetPayload(epsilon, usersNoisePerQuery int64) map[string]any {
	maxCount := dpMaxAggregationCount(epsilon, usersNoisePerQuery)

	aggregations := make([]map[string]any, 0, len(differentialPrivacyAggregationTypes()))
	for _, t := range differentialPrivacyAggregationTypes() {
		aggregations = append(aggregations, map[string]any{
			"type":           t,
			"maxCount":       maxCount,
			"remainingCount": maxCount,
		})
	}

	return map[string]any{
		"differentialPrivacy": map[string]any{
			"epsilon":      epsilon,
			"aggregations": aggregations,
		},
	}
}

// toPrivacyBudget builds a real PrivacyBudgetSummary (see PrivacyBudget doc
// comment) for a differential-privacy template. Returns nil for a template
// whose Parameters don't carry a well-formed differentialPrivacy epsilon/
// usersNoisePerQuery pair (e.g. an ACCESS_BUDGET template, or one with only
// niche fields set) rather than fabricating one.
func toPrivacyBudget(t *PrivacyBudgetTemplate) *PrivacyBudget {
	if t.PrivacyBudgetType != privacyBudgetTypeDifferentialPrivacy {
		return nil
	}

	epsilon, noise, ok := extractDPEpsilonNoise(t.Parameters)
	if !ok {
		return nil
	}

	return &PrivacyBudget{
		Budget:                   differentialPrivacyBudgetPayload(epsilon, noise),
		ID:                       t.ID,
		PrivacyBudgetTemplateArn: t.Arn,
		PrivacyBudgetTemplateID:  t.ID,
		CollaborationArn:         t.CollaborationArn,
		CollaborationID:          t.CollaborationID,
		MembershipArn:            t.MembershipArn,
		MembershipID:             t.MembershipID,
		PrivacyBudgetType:        t.PrivacyBudgetType,
		CreateTime:               t.CreateTime,
		UpdateTime:               t.UpdateTime,
	}
}

func (b *InMemoryBackend) ListPrivacyBudgets(
	membershipID, privacyBudgetType, _, _ string,
) ([]*PrivacyBudget, string, error) {
	b.mu.RLock("ListPrivacyBudgets")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}

	budgets := make([]*PrivacyBudget, 0)
	for _, t := range b.privacyBudgetTemplatesByMembership.Get(membershipID) {
		if privacyBudgetType != "" && t.PrivacyBudgetType != privacyBudgetType {
			continue
		}

		if pb := toPrivacyBudget(t); pb != nil {
			budgets = append(budgets, pb)
		}
	}

	return budgets, "", nil
}

func (b *InMemoryBackend) ListCollaborationPrivacyBudgets(
	collaborationID, privacyBudgetType, _, _ string,
) ([]*PrivacyBudget, string, error) {
	b.mu.RLock("ListCollaborationPrivacyBudgets")
	defer b.mu.RUnlock()
	if _, ok := b.collaborations.Get(collaborationID); !ok {
		return nil, "", ErrNotFound
	}

	budgets := make([]*PrivacyBudget, 0)
	b.privacyBudgetTemplates.Range(func(t *PrivacyBudgetTemplate) bool {
		if t.CollaborationID != collaborationID {
			return true
		}

		if privacyBudgetType != "" && t.PrivacyBudgetType != privacyBudgetType {
			return true
		}

		if pb := toPrivacyBudget(t); pb != nil {
			budgets = append(budgets, pb)
		}

		return true
	})

	return budgets, "", nil
}

func (b *InMemoryBackend) GetCollaborationPrivacyBudgetTemplate(
	collaborationID, templateID string,
) (*PrivacyBudgetTemplate, error) {
	b.mu.RLock("GetCollaborationPrivacyBudgetTemplate")
	defer b.mu.RUnlock()
	var found *PrivacyBudgetTemplate
	b.privacyBudgetTemplates.Range(func(t *PrivacyBudgetTemplate) bool {
		if t.CollaborationID == collaborationID && t.ID == templateID {
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
		func(t *PrivacyBudgetTemplate) bool { return t.CollaborationID == collaborationID },
		toPrivacyBudgetTemplateSummary,
		func(a, c *PrivacyBudgetTemplateSummary) bool {
			return a.ID < c.ID
		},
		maxResults, nextToken,
	)

	return page, next, nil
}

// PreviewPrivacyImpact computes the DifferentialPrivacyPrivacyImpact shape
// (aggregations: [{type, maxCount}]) from the requested epsilon/
// usersNoisePerQuery using the same dpMaxAggregationCount approximation
// ListPrivacyBudgets uses -- this is a preview, so it never mutates any
// stored budget.
func (b *InMemoryBackend) PreviewPrivacyImpact(
	membershipID string,
	parameters map[string]any,
) (map[string]any, error) {
	b.mu.RLock("PreviewPrivacyImpact")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, ErrNotFound
	}

	// PreviewPrivacyImpactParametersInput serializes the identical
	// {"differentialPrivacy": {"epsilon", "usersNoisePerQuery"}} shape as the
	// template parameters (verified against
	// awsRestjson1_serializeDocumentDifferentialPrivacyPreviewParametersInput).
	epsilon, noise, ok := extractDPEpsilonNoise(parameters)
	if !ok {
		return nil, fmt.Errorf(
			"%w: parameters.differentialPrivacy.{epsilon,usersNoisePerQuery} are required", ErrValidation,
		)
	}

	maxCount := dpMaxAggregationCount(epsilon, noise)
	aggregations := make([]map[string]any, 0, len(differentialPrivacyAggregationTypes()))

	for _, t := range differentialPrivacyAggregationTypes() {
		aggregations = append(aggregations, map[string]any{"type": t, "maxCount": maxCount})
	}

	return map[string]any{
		"privacyImpact": map[string]any{
			"differentialPrivacy": map[string]any{"aggregations": aggregations},
		},
	}, nil
}
