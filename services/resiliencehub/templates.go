package resiliencehub

import (
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// resolveTemplateLocked resolves templateArn to its stored
// RecommendationTemplate. Callers must hold b.mu.
func (b *InMemoryBackend) resolveTemplateLocked(templateArn string) (*RecommendationTemplate, bool) {
	id, ok := resourceIDFromARN(templateArn, ":recommendation-template/")
	if !ok {
		return nil, false
	}

	return b.templates.Get(id)
}

// CreateRecommendationTemplate creates a new recommendation template.
// Mechanically real (the template record itself is genuine, retrievable,
// deletable state) but its content is honestly trivial: since this backend
// never generates real SOP/alarm/test/component recommendations (see
// recommendations.go), RecommendationIDs/RecommendationTypes are always
// empty and TemplatesLocation is a synthetic, documented placeholder --
// no actual S3 object is written (see PARITY.md's "Recommendation families"
// gap; s3 write-through is flagged there as a valid future enhancement, out
// of scope for this pass). This op is mechanically trivial (no real
// analysis to package), so it completes synchronously rather than through
// the Pending/InProgress timer the other four async families use.
func (b *InMemoryBackend) CreateRecommendationTemplate(
	req *createRecommendationTemplateRequest,
) (*RecommendationTemplate, error) {
	if req.Name == "" {
		return nil, validationError("name is required")
	}

	if req.AssessmentArn == "" {
		return nil, validationError("assessmentArn is required")
	}

	b.mu.Lock("CreateRecommendationTemplate")
	defer b.mu.Unlock()

	asmt, ok := b.resolveAssessmentLocked(req.AssessmentArn)
	if !ok {
		return nil, notFoundError(resourceAssessment, req.AssessmentArn)
	}

	format := req.Format
	if format == "" {
		format = TemplateFormatCfnJSON
	}

	id := newRecommendationTemplateID()
	t := tags.New("resiliencehub.template." + id + ".tags")
	t.Merge(req.Tags)

	now := time.Now().UTC()
	bucket := req.BucketName

	tmpl := &RecommendationTemplate{
		ARN:                 b.RecommendationTemplateARN(id),
		AssessmentArn:       asmt.ARN,
		Name:                req.Name,
		Format:              format,
		AppArn:              asmt.AppArn,
		Status:              AsyncStatusSuccess,
		StartTime:           now,
		EndTime:             now,
		RecommendationTypes: cloneStrs(req.RecommendationTypes),
		RecommendationIDs:   cloneStrs(req.RecommendationIDs),
		Tags:                t,
	}

	if bucket != "" {
		tmpl.TemplatesLocation = &S3Location{Bucket: bucket, Prefix: "resiliencehub/" + id + "/"}
	}

	b.templates.Put(tmpl)

	return tmpl.clone(), nil
}

// DeleteRecommendationTemplate deletes the template identified by
// templateArn. Unlike every other Create*/Delete*/Update* op in this
// service, this one does NOT accept ConflictException (confirmed by
// re-reading its own deserializeOpErrorDeleteRecommendationTemplate switch
// in the SDK -- see PARITY.md), so this deletion is never rejected for a
// conflicting state.
func (b *InMemoryBackend) DeleteRecommendationTemplate(templateArn string) (string, error) {
	b.mu.Lock("DeleteRecommendationTemplate")
	defer b.mu.Unlock()

	t, ok := b.resolveTemplateLocked(templateArn)
	if !ok {
		return "", notFoundError(resourceRecommendationTemplate, templateArn)
	}

	status := t.Status

	if t.Tags != nil {
		t.Tags.Close()
	}

	b.templates.Delete(templateKeyFn(t))

	return status, nil
}

// listTemplatesFilter holds ListRecommendationTemplates' optional filters.
type listTemplatesFilter struct {
	assessmentArn string
	name          string
	templateArn   string
	statuses      []string
	reverseOrder  bool
}

func matchesTemplateFilter(t *RecommendationTemplate, f listTemplatesFilter) bool {
	if f.assessmentArn != "" && t.AssessmentArn != f.assessmentArn {
		return false
	}

	if f.name != "" && t.Name != f.name {
		return false
	}

	if f.templateArn != "" && t.ARN != f.templateArn {
		return false
	}

	if len(f.statuses) > 0 && !containsStr(f.statuses, t.Status) {
		return false
	}

	return true
}

// ListRecommendationTemplates returns a page of templates matching f.
func (b *InMemoryBackend) ListRecommendationTemplates(
	f listTemplatesFilter,
	token string,
	limit int,
) page.Page[*RecommendationTemplate] {
	b.mu.RLock("ListRecommendationTemplates")
	defer b.mu.RUnlock()

	all := b.templates.Snapshot()
	filtered := make([]*RecommendationTemplate, 0, len(all))

	for _, t := range all {
		if matchesTemplateFilter(t, f) {
			filtered = append(filtered, t)
		}
	}

	// ListRecommendationTemplatesInput.ReverseOrder: "The default is to sort
	// by ascending startTime" (api_op_ListRecommendationTemplates.go,
	// resiliencehub@v1.38.3) -- sort by StartTime explicitly rather than
	// relying on b.templates.Snapshot()'s key order.
	sort.Slice(filtered, func(i, j int) bool {
		if f.reverseOrder {
			return filtered[i].StartTime.After(filtered[j].StartTime)
		}

		return filtered[i].StartTime.Before(filtered[j].StartTime)
	})

	return page.New(filtered, token, limit, defaultPageLimit)
}
