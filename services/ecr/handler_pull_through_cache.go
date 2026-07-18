package ecr

import (
	"context"
	"encoding/base64"
)

// createPullThroughCacheRuleInput is the request body for CreatePullThroughCacheRule.
type createPullThroughCacheRuleInput struct {
	EcrRepositoryPrefix      string `json:"ecrRepositoryPrefix"`
	UpstreamRegistryURL      string `json:"upstreamRegistryUrl"`
	CredentialArn            string `json:"credentialArn,omitempty"`
	CustomRoleArn            string `json:"customRoleArn,omitempty"`
	UpstreamRegistry         string `json:"upstreamRegistry,omitempty"`
	UpstreamRepositoryPrefix string `json:"upstreamRepositoryPrefix,omitempty"`
}

type createPullThroughCacheRuleOutput struct {
	EcrRepositoryPrefix      string `json:"ecrRepositoryPrefix"`
	UpstreamRegistryURL      string `json:"upstreamRegistryUrl"`
	CredentialArn            string `json:"credentialArn,omitempty"`
	CustomRoleArn            string `json:"customRoleArn,omitempty"`
	UpstreamRegistry         string `json:"upstreamRegistry,omitempty"`
	UpstreamRepositoryPrefix string `json:"upstreamRepositoryPrefix,omitempty"`
	RegistryID               string `json:"registryId"`
	CreatedAt                int64  `json:"createdAt"`
	UpdatedAt                int64  `json:"updatedAt,omitempty"`
}

func (h *Handler) handleCreatePullThroughCacheRule(
	ctx context.Context,
	in *createPullThroughCacheRuleInput,
) (*createPullThroughCacheRuleOutput, error) {
	rule, err := h.Backend.CreatePullThroughCacheRule(
		ctx,
		in.EcrRepositoryPrefix,
		in.UpstreamRegistryURL,
		in.CredentialArn,
		in.UpstreamRegistry,
		in.CustomRoleArn,
		in.UpstreamRepositoryPrefix,
	)
	if err != nil {
		return nil, err
	}

	return &createPullThroughCacheRuleOutput{
		EcrRepositoryPrefix:      rule.EcrRepositoryPrefix,
		UpstreamRegistryURL:      rule.UpstreamRegistryURL,
		CredentialArn:            rule.CredentialArn,
		CustomRoleArn:            rule.CustomRoleArn,
		UpstreamRegistry:         rule.UpstreamRegistry,
		UpstreamRepositoryPrefix: rule.UpstreamRepositoryPrefix,
		RegistryID:               rule.RegistryID,
		CreatedAt:                rule.CreatedAt.Unix(),
		UpdatedAt:                rule.UpdatedAt.Unix(),
	}, nil
}

type describePullThroughCacheRulesInput struct {
	NextToken             string   `json:"nextToken,omitempty"`
	EcrRepositoryPrefixes []string `json:"ecrRepositoryPrefixes,omitempty"`
	MaxResults            int      `json:"maxResults,omitempty"`
}

type describePullThroughCacheRulesOutput struct {
	NextToken             string                             `json:"nextToken,omitempty"`
	PullThroughCacheRules []createPullThroughCacheRuleOutput `json:"pullThroughCacheRules"`
}

func (h *Handler) handleDescribePullThroughCacheRules(
	ctx context.Context,
	in *describePullThroughCacheRulesInput,
) (*describePullThroughCacheRulesOutput, error) {
	rules, err := h.Backend.DescribePullThroughCacheRules(ctx, in.EcrRepositoryPrefixes)
	if err != nil {
		return nil, err
	}

	// Apply nextToken cursor: token is base64(ecrRepositoryPrefix) of the first rule on this page.
	if in.NextToken != "" && len(in.EcrRepositoryPrefixes) == 0 {
		decoded, decErr := base64.StdEncoding.DecodeString(in.NextToken)
		if decErr == nil {
			cursorPrefix := string(decoded)
			start := 0
			for i, r := range rules {
				if r.EcrRepositoryPrefix == cursorPrefix {
					start = i

					break
				}
			}

			rules = rules[start:]
		}
	}

	// Apply maxResults page limit; emit opaque token = base64(next prefix).
	var nextToken string
	if in.MaxResults > 0 && len(rules) > in.MaxResults {
		nextToken = base64.StdEncoding.EncodeToString(
			[]byte(rules[in.MaxResults].EcrRepositoryPrefix),
		)
		rules = rules[:in.MaxResults]
	}

	out := make([]createPullThroughCacheRuleOutput, 0, len(rules))
	for _, rule := range rules {
		out = append(out, createPullThroughCacheRuleOutput{
			EcrRepositoryPrefix:      rule.EcrRepositoryPrefix,
			UpstreamRegistryURL:      rule.UpstreamRegistryURL,
			CredentialArn:            rule.CredentialArn,
			CustomRoleArn:            rule.CustomRoleArn,
			UpstreamRegistry:         rule.UpstreamRegistry,
			UpstreamRepositoryPrefix: rule.UpstreamRepositoryPrefix,
			RegistryID:               rule.RegistryID,
			CreatedAt:                rule.CreatedAt.Unix(),
			UpdatedAt:                rule.UpdatedAt.Unix(),
		})
	}

	return &describePullThroughCacheRulesOutput{
		PullThroughCacheRules: out,
		NextToken:             nextToken,
	}, nil
}

// deletePullThroughCacheRuleInput is the request body for DeletePullThroughCacheRule.
type deletePullThroughCacheRuleInput struct {
	EcrRepositoryPrefix string `json:"ecrRepositoryPrefix"`
	RegistryID          string `json:"registryId,omitempty"`
}

type deletePullThroughCacheRuleOutput struct {
	EcrRepositoryPrefix string `json:"ecrRepositoryPrefix"`
	UpstreamRegistryURL string `json:"upstreamRegistryUrl"`
	RegistryID          string `json:"registryId"`
	CreatedAt           int64  `json:"createdAt"`
}

func (h *Handler) handleDeletePullThroughCacheRule(
	ctx context.Context,
	in *deletePullThroughCacheRuleInput,
) (*deletePullThroughCacheRuleOutput, error) {
	rule, err := h.Backend.DeletePullThroughCacheRule(ctx, in.EcrRepositoryPrefix)
	if err != nil {
		return nil, err
	}

	return &deletePullThroughCacheRuleOutput{
		EcrRepositoryPrefix: rule.EcrRepositoryPrefix,
		UpstreamRegistryURL: rule.UpstreamRegistryURL,
		RegistryID:          rule.RegistryID,
		CreatedAt:           rule.CreatedAt.Unix(),
	}, nil
}

type updatePullThroughCacheRuleInput struct {
	EcrRepositoryPrefix string `json:"ecrRepositoryPrefix"`
	CredentialArn       string `json:"credentialArn,omitempty"`
	CustomRoleArn       string `json:"customRoleArn,omitempty"`
	RegistryID          string `json:"registryId,omitempty"`
}

func (h *Handler) handleUpdatePullThroughCacheRule(
	ctx context.Context,
	in *updatePullThroughCacheRuleInput,
) (*createPullThroughCacheRuleOutput, error) {
	rule, err := h.Backend.UpdatePullThroughCacheRule(
		ctx,
		in.EcrRepositoryPrefix,
		in.CredentialArn,
		in.CustomRoleArn,
	)
	if err != nil {
		return nil, err
	}

	return &createPullThroughCacheRuleOutput{
		EcrRepositoryPrefix:      rule.EcrRepositoryPrefix,
		UpstreamRegistryURL:      rule.UpstreamRegistryURL,
		CredentialArn:            rule.CredentialArn,
		CustomRoleArn:            rule.CustomRoleArn,
		UpstreamRegistry:         rule.UpstreamRegistry,
		UpstreamRepositoryPrefix: rule.UpstreamRepositoryPrefix,
		RegistryID:               rule.RegistryID,
		CreatedAt:                rule.CreatedAt.Unix(),
		UpdatedAt:                rule.UpdatedAt.Unix(),
	}, nil
}

type validatePullThroughCacheRuleInput struct {
	EcrRepositoryPrefix string `json:"ecrRepositoryPrefix"`
	RegistryID          string `json:"registryId,omitempty"`
}

func (h *Handler) handleValidatePullThroughCacheRule(
	ctx context.Context,
	in *validatePullThroughCacheRuleInput,
) (*ValidatePullThroughCacheRuleResult, error) {
	return h.Backend.ValidatePullThroughCacheRule(ctx, in.EcrRepositoryPrefix)
}
