package codebuild

import (
	"context"
	"fmt"
)

type createWebhookInput struct {
	ManualCreation         *bool                   `json:"manualCreation,omitempty"`
	PullRequestBuildPolicy *PullRequestBuildPolicy `json:"pullRequestBuildPolicy,omitempty"`
	ScopeConfiguration     *ScopeConfiguration     `json:"scopeConfiguration,omitempty"`
	ProjectName            string                  `json:"projectName"`
	BranchFilter           string                  `json:"branchFilter,omitempty"`
	BuildType              string                  `json:"buildType,omitempty"`
	FilterGroups           [][]WebhookFilter       `json:"filterGroups,omitempty"`
}

type createWebhookOutput struct {
	Webhook *Webhook `json:"webhook"`
}

func (h *Handler) handleCreateWebhook(
	_ context.Context,
	in *createWebhookInput,
) (*createWebhookOutput, error) {
	if in.ProjectName == "" {
		return nil, fmt.Errorf("%w: projectName is required", errInvalidRequest)
	}

	w, err := h.Backend.CreateWebhook(in.ProjectName, in.BranchFilter, in.BuildType, in.FilterGroups, WebhookConfig{
		ManualCreation:         in.ManualCreation,
		PullRequestBuildPolicy: in.PullRequestBuildPolicy,
		ScopeConfiguration:     in.ScopeConfiguration,
	})
	if err != nil {
		return nil, err
	}

	return &createWebhookOutput{Webhook: w}, nil
}

type deleteWebhookInput struct {
	ProjectName string `json:"projectName"`
}

type deleteWebhookOutput struct{}

func (h *Handler) handleDeleteWebhook(_ context.Context, in *deleteWebhookInput) (*deleteWebhookOutput, error) {
	if in.ProjectName == "" {
		return nil, fmt.Errorf("%w: projectName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteWebhook(in.ProjectName); err != nil {
		return nil, err
	}

	return &deleteWebhookOutput{}, nil
}

type updateWebhookInput struct {
	PullRequestBuildPolicy *PullRequestBuildPolicy `json:"pullRequestBuildPolicy,omitempty"`
	ProjectName            string                  `json:"projectName"`
	BranchFilter           string                  `json:"branchFilter,omitempty"`
	BuildType              string                  `json:"buildType,omitempty"`
	FilterGroups           [][]WebhookFilter       `json:"filterGroups,omitempty"`
	RotateSecret           bool                    `json:"rotateSecret,omitempty"`
}

type updateWebhookOutput struct {
	Webhook *Webhook `json:"webhook"`
}

func (h *Handler) handleUpdateWebhook(_ context.Context, in *updateWebhookInput) (*updateWebhookOutput, error) {
	if in.ProjectName == "" {
		return nil, fmt.Errorf("%w: projectName is required", errInvalidRequest)
	}

	w, err := h.Backend.UpdateWebhook(in.ProjectName, in.BranchFilter, in.BuildType, in.FilterGroups, WebhookConfig{
		PullRequestBuildPolicy: in.PullRequestBuildPolicy,
		RotateSecret:           in.RotateSecret,
	})
	if err != nil {
		return nil, err
	}

	return &updateWebhookOutput{Webhook: w}, nil
}
