package ecr

import (
	"context"
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

// lifecyclePolicyPreviewView is the JSON representation shared by
// GetLifecyclePolicyPreview and StartLifecyclePolicyPreview.
type lifecyclePolicyPreviewView struct {
	LifecyclePolicyText string                            `json:"lifecyclePolicyText"`
	RepositoryName      string                            `json:"repositoryName"`
	RegistryID          string                            `json:"registryId,omitempty"`
	Status              string                            `json:"status"`
	PreviewResults      []lifecyclePolicyPreviewEntryView `json:"previewResults"`
	Summary             lifecyclePolicyPreviewSummaryView `json:"summary"`
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

func (h *Handler) handleGetLifecyclePolicyPreview(
	ctx context.Context,
	in *getLifecyclePolicyInput,
) (*lifecyclePolicyPreviewView, error) {
	preview, err := h.Backend.GetLifecyclePolicyPreview(ctx, in.RepositoryName)
	if err != nil {
		return nil, err
	}

	return toLifecyclePolicyPreviewView(preview), nil
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
) (*lifecyclePolicyPreviewView, error) {
	preview, err := h.Backend.StartLifecyclePolicyPreview(ctx, in.RepositoryName, in.LifecyclePolicyText)
	if err != nil {
		return nil, err
	}

	return toLifecyclePolicyPreviewView(preview), nil
}
