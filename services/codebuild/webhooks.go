package codebuild

import (
	"time"

	"github.com/google/uuid"
)

const webhookStatusActive = "ACTIVE"

func (b *InMemoryBackend) buildWebhookURL(projectName string) string {
	return "https://codebuild." + b.region + ".amazonaws.com/webhooks/" + projectName
}

// WebhookConfig holds the configurable, additive fields accepted by
// CreateWebhook/UpdateWebhook beyond branchFilter/buildType/filterGroups.
type WebhookConfig struct {
	ManualCreation         *bool
	PullRequestBuildPolicy *PullRequestBuildPolicy
	ScopeConfiguration     *ScopeConfiguration
	RotateSecret           bool
}

// CreateWebhook creates a webhook for a CodeBuild project.
//
// Real AWS surfaces the created webhook back on the project itself (the
// Project.Webhook field returned by BatchGetProjects/GetProject), so the new
// webhook is mirrored onto the project record here as well as stored in the
// webhooks table. This emulator doesn't perform a real GitHub/GitLab/Bitbucket
// round-trip, so webhook creation always succeeds immediately with status
// ACTIVE (matching the real terminal state a client would eventually observe).
func (b *InMemoryBackend) CreateWebhook(
	projectName, branchFilter, buildType string, filterGroups [][]WebhookFilter, cfg WebhookConfig,
) (*Webhook, error) {
	b.mu.Lock("CreateWebhook")
	defer b.mu.Unlock()

	p, ok := b.projects.Get(projectName)
	if !ok {
		return nil, ErrNotFound
	}

	if b.webhooks.Has(projectName) {
		return nil, ErrAlreadyExists
	}

	now := float64(time.Now().Unix())
	w := &Webhook{
		ProjectName:            projectName,
		URL:                    b.buildWebhookURL(projectName),
		PayloadURL:             b.buildWebhookURL(projectName),
		BranchFilter:           branchFilter,
		BuildType:              buildType,
		FilterGroups:           filterGroups,
		ManualCreation:         cfg.ManualCreation,
		PullRequestBuildPolicy: cfg.PullRequestBuildPolicy,
		ScopeConfiguration:     cfg.ScopeConfiguration,
		Secret:                 uuid.NewString(),
		LastModifiedSecret:     now,
		Status:                 webhookStatusActive,
	}
	b.webhooks.Put(w)

	out := *w
	projCopy := out
	p.Webhook = &projCopy

	return &out, nil
}

// DeleteWebhook removes the webhook for a project.
func (b *InMemoryBackend) DeleteWebhook(projectName string) error {
	b.mu.Lock("DeleteWebhook")
	defer b.mu.Unlock()

	if !b.webhooks.Delete(projectName) {
		return ErrNotFound
	}

	if p, ok := b.projects.Get(projectName); ok {
		p.Webhook = nil
	}

	return nil
}

// UpdateWebhook updates the branchFilter, buildType, filterGroups and
// additive fields of an existing webhook. rotateSecret regenerates Secret and
// bumps LastModifiedSecret, matching real AWS's rotateSecret request field.
func (b *InMemoryBackend) UpdateWebhook(
	projectName, branchFilter, buildType string, filterGroups [][]WebhookFilter, cfg WebhookConfig,
) (*Webhook, error) {
	b.mu.Lock("UpdateWebhook")
	defer b.mu.Unlock()

	w, ok := b.webhooks.Get(projectName)
	if !ok {
		return nil, ErrNotFound
	}

	w.BranchFilter = branchFilter
	w.BuildType = buildType

	if filterGroups != nil {
		w.FilterGroups = filterGroups
	}

	if cfg.PullRequestBuildPolicy != nil {
		w.PullRequestBuildPolicy = cfg.PullRequestBuildPolicy
	}

	if cfg.RotateSecret {
		w.Secret = uuid.NewString()
		w.LastModifiedSecret = float64(time.Now().Unix())
	}

	out := *w

	if p, projOK := b.projects.Get(projectName); projOK {
		projCopy := out
		p.Webhook = &projCopy
	}

	return &out, nil
}
