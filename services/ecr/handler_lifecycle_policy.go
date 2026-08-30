package ecr

import (
	"context"
	"encoding/base64"
	"slices"
)

// lifecyclePolicyResultView is the JSON representation shared by
// DeleteLifecyclePolicy, GetLifecyclePolicy, and PutLifecyclePolicy.
// lastEvaluatedAt is serialised as a Unix epoch float64 (seconds) so that the
// AWS SDK v2 deserialiser, which expects a JSON Number for timestamp fields,
// can decode it correctly (mirrors repositoryView's createdAt convention).
type lifecyclePolicyResultView struct {
	LifecyclePolicyText string  `json:"lifecyclePolicyText"`
	RepositoryName      string  `json:"repositoryName"`
	RegistryID          string  `json:"registryId,omitempty"`
	LastEvaluatedAt     float64 `json:"lastEvaluatedAt"`
}

func toLifecyclePolicyResultView(r *LifecyclePolicyResult) *lifecyclePolicyResultView {
	if r == nil {
		return nil
	}

	return &lifecyclePolicyResultView{
		LifecyclePolicyText: r.LifecyclePolicyText,
		RepositoryName:      r.RepositoryName,
		RegistryID:          r.RegistryID,
		LastEvaluatedAt:     float64(r.LastEvaluatedAt.Unix()),
	}
}

// lifecyclePolicyPreviewRuleActionView is the JSON representation of a
// lifecycle preview entry's action (real AWS type: LifecyclePolicyRuleAction).
type lifecyclePolicyPreviewRuleActionView struct {
	Type string `json:"type,omitempty"`
}

// lifecyclePolicyPreviewEntryView is the JSON representation of a single
// per-image lifecycle preview entry (real AWS wire name:
// LifecyclePolicyPreviewResult — renamed here to avoid colliding with
// gopherstack's top-level preview-request type).
type lifecyclePolicyPreviewEntryView struct {
	Action              lifecyclePolicyPreviewRuleActionView `json:"action"`
	ImageDigest         string                               `json:"imageDigest,omitempty"`
	StorageClass        string                               `json:"storageClass,omitempty"`
	ImageTags           []string                             `json:"imageTags,omitempty"`
	ImagePushedAt       float64                              `json:"imagePushedAt"`
	AppliedRulePriority int                                  `json:"appliedRulePriority"`
}

// lifecyclePolicyPreviewSummaryView is the JSON representation of the preview
// summary (real AWS type: LifecyclePolicyPreviewSummary).
type lifecyclePolicyPreviewSummaryView struct {
	ExpiringImageTotalCount int `json:"expiringImageTotalCount"`
}

// lifecyclePolicyPreviewView is the JSON representation returned by
// GetLifecyclePolicyPreview. NextToken supports the real API's MaxResults/
// NextToken pagination (see handleGetLifecyclePolicyPreview).
type lifecyclePolicyPreviewView struct {
	LifecyclePolicyText string                            `json:"lifecyclePolicyText"`
	RepositoryName      string                            `json:"repositoryName"`
	RegistryID          string                            `json:"registryId,omitempty"`
	Status              string                            `json:"status"`
	NextToken           string                            `json:"nextToken,omitempty"`
	PreviewResults      []lifecyclePolicyPreviewEntryView `json:"previewResults"`
	Summary             lifecyclePolicyPreviewSummaryView `json:"summary"`
}

// lifecyclePolicyPreviewStartView is the JSON representation returned by
// StartLifecyclePolicyPreview. Confirmed by direct diff of
// StartLifecyclePolicyPreviewOutput /
// awsAwsjson11_deserializeOpDocumentStartLifecyclePolicyPreviewOutput in
// aws-sdk-go-v2/service/ecr@v1.60.4: unlike GetLifecyclePolicyPreview,
// Start's response does NOT include previewResults/summary/nextToken --
// Start only kicks off the (synchronously, in gopherstack) computed preview;
// results must be retrieved separately via GetLifecyclePolicyPreview. The
// previous shared-view implementation leaked previewResults/summary into
// Start's response, an invented-field bug independent of (but adjacent to)
// the Filter/ImageIds/MaxResults/NextToken gap this pass closes.
type lifecyclePolicyPreviewStartView struct {
	LifecyclePolicyText string `json:"lifecyclePolicyText"`
	RepositoryName      string `json:"repositoryName"`
	RegistryID          string `json:"registryId,omitempty"`
	Status              string `json:"status"`
}

func toLifecyclePolicyPreviewStartView(p *LifecyclePolicyPreviewResult) *lifecyclePolicyPreviewStartView {
	if p == nil {
		return nil
	}

	return &lifecyclePolicyPreviewStartView{
		LifecyclePolicyText: p.LifecyclePolicyText,
		RepositoryName:      p.RepositoryName,
		RegistryID:          p.RegistryID,
		Status:              p.Status,
	}
}

func toLifecyclePolicyPreviewView(p *LifecyclePolicyPreviewResult) *lifecyclePolicyPreviewView {
	if p == nil {
		return nil
	}

	results := make([]lifecyclePolicyPreviewEntryView, 0, len(p.PreviewResults))
	for _, e := range p.PreviewResults {
		results = append(results, lifecyclePolicyPreviewEntryView{
			Action:              lifecyclePolicyPreviewRuleActionView{Type: e.ActionType},
			ImageDigest:         e.ImageDigest,
			StorageClass:        e.StorageClass,
			ImageTags:           e.ImageTags,
			ImagePushedAt:       float64(e.ImagePushedAt.Unix()),
			AppliedRulePriority: e.AppliedRulePriority,
		})
	}

	return &lifecyclePolicyPreviewView{
		LifecyclePolicyText: p.LifecyclePolicyText,
		RepositoryName:      p.RepositoryName,
		RegistryID:          p.RegistryID,
		Status:              p.Status,
		PreviewResults:      results,
		Summary:             lifecyclePolicyPreviewSummaryView{ExpiringImageTotalCount: len(results)},
	}
}

// deleteLifecyclePolicyInput is the request body for DeleteLifecyclePolicy.
type deleteLifecyclePolicyInput struct {
	RepositoryName string `json:"repositoryName"`
	RegistryID     string `json:"registryId,omitempty"`
}

func (h *Handler) handleDeleteLifecyclePolicy(
	ctx context.Context,
	in *deleteLifecyclePolicyInput,
) (*lifecyclePolicyResultView, error) {
	result, err := h.Backend.DeleteLifecyclePolicy(ctx, in.RepositoryName)
	if err != nil {
		return nil, err
	}

	return toLifecyclePolicyResultView(result), nil
}

type getLifecyclePolicyInput struct {
	RepositoryName string `json:"repositoryName"`
	RegistryID     string `json:"registryId,omitempty"`
}

func (h *Handler) handleGetLifecyclePolicy(
	ctx context.Context,
	in *getLifecyclePolicyInput,
) (*lifecyclePolicyResultView, error) {
	result, err := h.Backend.GetLifecyclePolicy(ctx, in.RepositoryName)
	if err != nil {
		return nil, err
	}

	return toLifecyclePolicyResultView(result), nil
}

// lifecyclePolicyPreviewFilterInput is the request body for
// GetLifecyclePolicyPreview's optional "filter" field (real AWS type:
// LifecyclePolicyPreviewFilter, which carries only tagStatus).
type lifecyclePolicyPreviewFilterInput struct {
	TagStatus string `json:"tagStatus,omitempty"`
}

// getLifecyclePolicyPreviewInput is the request body for
// GetLifecyclePolicyPreview. Filter/MaxResults/NextToken and ImageIds are
// mutually exclusive per the real API doc ("This option cannot be used when
// you specify images with imageIds"); see handleGetLifecyclePolicyPreview.
type getLifecyclePolicyPreviewInput struct {
	RepositoryName string                             `json:"repositoryName"`
	RegistryID     string                             `json:"registryId,omitempty"`
	NextToken      string                             `json:"nextToken,omitempty"`
	Filter         *lifecyclePolicyPreviewFilterInput `json:"filter,omitempty"`
	ImageIDs       []ImageIdentifier                  `json:"imageIds,omitempty"`
	MaxResults     int                                `json:"maxResults,omitempty"`
}

func (h *Handler) handleGetLifecyclePolicyPreview(
	ctx context.Context,
	in *getLifecyclePolicyPreviewInput,
) (*lifecyclePolicyPreviewView, error) {
	preview, err := h.Backend.GetLifecyclePolicyPreview(ctx, in.RepositoryName)
	if err != nil {
		return nil, err
	}

	entries := preview.PreviewResults

	var nextToken string
	if len(in.ImageIDs) > 0 {
		entries = filterLifecyclePreviewEntriesByImageIDs(entries, in.ImageIDs)
	} else {
		tagStatus := ""
		if in.Filter != nil {
			tagStatus = in.Filter.TagStatus
		}

		entries = filterLifecyclePreviewEntriesByTagStatus(entries, tagStatus)
		entries, nextToken = paginateLifecyclePreviewEntries(entries, in.NextToken, in.MaxResults)
	}

	cp := *preview
	cp.PreviewResults = entries

	view := toLifecyclePolicyPreviewView(&cp)
	view.NextToken = nextToken

	return view, nil
}

// filterLifecyclePreviewEntriesByImageIDs keeps only entries matching one of
// ids by digest or tag (mirrors DescribeImages' imageIds semantics).
func filterLifecyclePreviewEntriesByImageIDs(
	entries []LifecyclePolicyPreviewEntry, ids []ImageIdentifier,
) []LifecyclePolicyPreviewEntry {
	out := make([]LifecyclePolicyPreviewEntry, 0, len(entries))

	for _, e := range entries {
		if lifecyclePreviewEntryMatchesAnyImageID(e, ids) {
			out = append(out, e)
		}
	}

	return out
}

func lifecyclePreviewEntryMatchesAnyImageID(e LifecyclePolicyPreviewEntry, ids []ImageIdentifier) bool {
	for _, id := range ids {
		if id.ImageDigest != "" && id.ImageDigest == e.ImageDigest {
			return true
		}

		if id.ImageTag != "" {
			if slices.Contains(e.ImageTags, id.ImageTag) {
				return true
			}
		}
	}

	return false
}

// filterLifecyclePreviewEntriesByTagStatus applies the real API's
// filter.tagStatus (TAGGED/UNTAGGED/ANY, default ANY), reusing the same
// tagged/untagged semantics as DescribeImages/ListImages (passesTagFilter).
func filterLifecyclePreviewEntriesByTagStatus(
	entries []LifecyclePolicyPreviewEntry, tagStatus string,
) []LifecyclePolicyPreviewEntry {
	if tagStatus == "" {
		return entries
	}

	out := make([]LifecyclePolicyPreviewEntry, 0, len(entries))

	for _, e := range entries {
		if passesTagFilter(len(e.ImageTags) > 0, tagStatus) {
			out = append(out, e)
		}
	}

	return out
}

// paginateLifecyclePreviewEntries applies nextToken/maxResults cursor-based
// pagination, matching the base64(imageDigest)-cursor convention used by
// DescribeImages/ListImages elsewhere in this package. AWS defaults
// maxResults to 100 when unset.
func paginateLifecyclePreviewEntries(
	entries []LifecyclePolicyPreviewEntry, nextToken string, maxResults int,
) ([]LifecyclePolicyPreviewEntry, string) {
	if nextToken != "" {
		entries = advanceToDigestCursor(entries, nextToken)
	}

	if maxResults <= 0 {
		maxResults = 100
	}

	if len(entries) <= maxResults {
		return entries, ""
	}

	next := base64.StdEncoding.EncodeToString([]byte(entries[maxResults].ImageDigest))

	return entries[:maxResults], next
}

// advanceToDigestCursor decodes nextToken and returns entries from the item
// whose ImageDigest matches it onward. entries is sorted by ImagePushedAt,
// not by digest, so an undecodable or unmatched cursor -- a stale token
// whose image was deleted, or a tampered one -- has no valid resume point:
// this returns no items rather than restarting at page one.
func advanceToDigestCursor(
	entries []LifecyclePolicyPreviewEntry, nextToken string,
) []LifecyclePolicyPreviewEntry {
	decoded, err := base64.StdEncoding.DecodeString(nextToken)
	if err != nil {
		return entries[:0]
	}

	cursor := string(decoded)

	for i, e := range entries {
		if e.ImageDigest == cursor {
			return entries[i:]
		}
	}

	return entries[:0]
}

// putLifecyclePolicyInput is the request body for PutLifecyclePolicy.
type putLifecyclePolicyInput struct {
	RepositoryName      string `json:"repositoryName"`
	LifecyclePolicyText string `json:"lifecyclePolicyText"`
	RegistryID          string `json:"registryId,omitempty"`
}

func (h *Handler) handlePutLifecyclePolicy(
	ctx context.Context,
	in *putLifecyclePolicyInput,
) (*lifecyclePolicyResultView, error) {
	result, err := h.Backend.PutLifecyclePolicy(ctx, in.RepositoryName, in.LifecyclePolicyText)
	if err != nil {
		return nil, err
	}

	return toLifecyclePolicyResultView(result), nil
}

func (h *Handler) handleStartLifecyclePolicyPreview(
	ctx context.Context,
	in *putLifecyclePolicyInput,
) (*lifecyclePolicyPreviewStartView, error) {
	preview, err := h.Backend.StartLifecyclePolicyPreview(ctx, in.RepositoryName, in.LifecyclePolicyText)
	if err != nil {
		return nil, err
	}

	return toLifecyclePolicyPreviewStartView(preview), nil
}
