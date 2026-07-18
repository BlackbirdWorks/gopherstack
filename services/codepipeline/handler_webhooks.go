package codepipeline

import (
	"context"
	"fmt"
)

// maxResultsCapWebhooks is the per-operation pagination cap for ListWebhooks.
const maxResultsCapWebhooks int32 = 60

// validWebhookAuth returns true if a is a valid webhook Authentication value.
func validWebhookAuth(a string) bool {
	return a == "" || a == WebhookAuthGitHubHMAC || a == WebhookAuthIP ||
		a == WebhookAuthUnauthenticated
}

type listWebhooksInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

// webhookDefinitionView is the AWS-spec shape for a webhook definition inside ListWebhooks.
type webhookDefinitionView struct {
	AuthenticationConfiguration WebhookAuthConfig `json:"authenticationConfiguration,omitzero"`
	Name                        string            `json:"name"`
	TargetPipeline              string            `json:"targetPipeline"`
	TargetAction                string            `json:"targetAction"`
	Authentication              string            `json:"authentication,omitempty"`
	Filters                     []WebhookFilter   `json:"filters"`
}

// webhookListEntry is the AWS-spec outer envelope returned per webhook in ListWebhooks.
type webhookListEntry struct {
	URL                      string                `json:"url,omitempty"`
	ARN                      string                `json:"arn,omitempty"`
	LastTriggered            string                `json:"lastTriggered,omitempty"`
	Definition               webhookDefinitionView `json:"definition"`
	RegisteredWithThirdParty bool                  `json:"registeredWithThirdParty"`
}

type listWebhooksOutput struct {
	NextToken string             `json:"NextToken,omitempty"`
	Webhooks  []webhookListEntry `json:"webhooks"`
}

func (h *Handler) handleListWebhooks(
	ctx context.Context,
	in *listWebhooksInput,
) (*listWebhooksOutput, error) {
	webhooks := h.Backend.ListWebhooks(ctx)
	entries := make([]webhookListEntry, len(webhooks))

	for i, wh := range webhooks {
		filters := wh.Filters
		if filters == nil {
			filters = []WebhookFilter{}
		}

		entries[i] = webhookListEntry{
			Definition: webhookDefinitionView{
				Name:                        wh.Name,
				TargetPipeline:              wh.TargetPipeline,
				TargetAction:                wh.TargetAction,
				Authentication:              wh.Authentication,
				Filters:                     filters,
				AuthenticationConfiguration: wh.AuthenticationConfiguration,
			},
			URL:                      wh.URL,
			ARN:                      wh.ARN,
			LastTriggered:            wh.LastTriggered,
			RegisteredWithThirdParty: wh.RegisteredWithThirdParty,
		}
	}

	page, nextToken, err := cpPaginate(entries, in.NextToken, in.MaxResults, maxResultsCapWebhooks)
	if err != nil {
		return nil, err
	}

	return &listWebhooksOutput{NextToken: nextToken, Webhooks: page}, nil
}

type putWebhookInput struct {
	Webhook struct {
		AuthenticationConfiguration WebhookAuthConfig `json:"authenticationConfiguration,omitzero"`
		Name                        string            `json:"name"`
		TargetPipeline              string            `json:"targetPipeline"`
		TargetAction                string            `json:"targetAction"`
		Authentication              string            `json:"authentication,omitempty"`
		Filters                     []WebhookFilter   `json:"filters,omitempty"`
	} `json:"webhook"`
	Tags []Tag `json:"tags"`
}

type putWebhookOutput struct {
	Webhook webhookListEntry `json:"webhook"`
}

func (h *Handler) handlePutWebhook(
	ctx context.Context,
	in *putWebhookInput,
) (*putWebhookOutput, error) {
	if in.Webhook.Name == "" {
		return nil, fmt.Errorf("%w: webhook name is required", errInvalidRequest)
	}

	if in.Webhook.Authentication != "" && !validWebhookAuth(in.Webhook.Authentication) {
		return nil, fmt.Errorf("%w: invalid authentication %q; must be %s, %s, or %s",
			ErrValidation, in.Webhook.Authentication,
			WebhookAuthGitHubHMAC, WebhookAuthIP, WebhookAuthUnauthenticated)
	}

	wh, err := h.Backend.PutWebhook(ctx, &Webhook{
		Name:                        in.Webhook.Name,
		TargetPipeline:              in.Webhook.TargetPipeline,
		TargetAction:                in.Webhook.TargetAction,
		Authentication:              in.Webhook.Authentication,
		Filters:                     in.Webhook.Filters,
		AuthenticationConfiguration: in.Webhook.AuthenticationConfiguration,
	})
	if err != nil {
		return nil, err
	}

	whFilters := wh.Filters
	if whFilters == nil {
		whFilters = []WebhookFilter{}
	}

	return &putWebhookOutput{
		Webhook: webhookListEntry{
			Definition: webhookDefinitionView{
				Name:                        wh.Name,
				TargetPipeline:              wh.TargetPipeline,
				TargetAction:                wh.TargetAction,
				Authentication:              wh.Authentication,
				Filters:                     whFilters,
				AuthenticationConfiguration: wh.AuthenticationConfiguration,
			},
			URL:                      wh.URL,
			ARN:                      wh.ARN,
			RegisteredWithThirdParty: wh.RegisteredWithThirdParty,
		},
	}, nil
}

type registerWebhookInput struct {
	WebhookName string `json:"webhookName"`
}

func (h *Handler) handleRegisterWebhookWithThirdParty(
	ctx context.Context,
	in *registerWebhookInput,
) (*emptyOut, error) {
	if err := h.Backend.RegisterWebhookWithThirdParty(ctx, in.WebhookName); err != nil {
		return nil, err
	}

	return &emptyOut{}, nil
}

type deleteWebhookInput struct {
	Name string `json:"name"`
}

type deleteWebhookOutput struct{}

func (h *Handler) handleDeleteWebhook(
	ctx context.Context,
	in *deleteWebhookInput,
) (*deleteWebhookOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteWebhook(ctx, in.Name); err != nil {
		return nil, err
	}

	return &deleteWebhookOutput{}, nil
}

type deregisterWebhookWithThirdPartyInput struct {
	WebhookName string `json:"webhookName"`
}

type deregisterWebhookWithThirdPartyOutput struct{}

func (h *Handler) handleDeregisterWebhookWithThirdParty(
	ctx context.Context,
	in *deregisterWebhookWithThirdPartyInput,
) (*deregisterWebhookWithThirdPartyOutput, error) {
	if err := h.Backend.DeregisterWebhookWithThirdParty(ctx, in.WebhookName); err != nil {
		return nil, err
	}

	return &deregisterWebhookWithThirdPartyOutput{}, nil
}
