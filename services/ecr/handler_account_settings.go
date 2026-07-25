package ecr

import (
	"context"
	"encoding/base64"
)

type accountSettingInput struct {
	Name  string `json:"name"`
	Value string `json:"value,omitempty"`
}

func (h *Handler) handleGetAccountSetting(
	ctx context.Context,
	in *accountSettingInput,
) (*accountSettingInput, error) {
	value, err := h.Backend.GetAccountSetting(ctx, in.Name)
	if err != nil {
		return nil, err
	}

	return &accountSettingInput{Name: in.Name, Value: value}, nil
}

func (h *Handler) handlePutAccountSetting(
	ctx context.Context,
	in *accountSettingInput,
) (*accountSettingInput, error) {
	value, err := h.Backend.PutAccountSetting(ctx, in.Name, in.Value)
	if err != nil {
		return nil, err
	}

	return &accountSettingInput{Name: in.Name, Value: value}, nil
}

type pullTimeUpdateExclusionInput struct {
	PrincipalArn string `json:"principalArn"`
}

type registerPullTimeUpdateExclusionOutput struct {
	PrincipalArn string `json:"principalArn"`
	CreatedAt    int64  `json:"createdAt,omitempty"`
}

func (h *Handler) handleRegisterPullTimeUpdateExclusion(
	ctx context.Context,
	in *pullTimeUpdateExclusionInput,
) (*registerPullTimeUpdateExclusionOutput, error) {
	exclusion, err := h.Backend.RegisterPullTimeUpdateExclusion(ctx, in.PrincipalArn)
	if err != nil {
		return nil, err
	}

	return &registerPullTimeUpdateExclusionOutput{
		CreatedAt:    exclusion.CreatedAt.Unix(),
		PrincipalArn: exclusion.PrincipalArn,
	}, nil
}

func (h *Handler) handleDeregisterPullTimeUpdateExclusion(
	ctx context.Context,
	in *pullTimeUpdateExclusionInput,
) (*registerPullTimeUpdateExclusionOutput, error) {
	exclusion, err := h.Backend.DeregisterPullTimeUpdateExclusion(ctx, in.PrincipalArn)
	if err != nil {
		return nil, err
	}

	return &registerPullTimeUpdateExclusionOutput{
		CreatedAt:    exclusion.CreatedAt.Unix(),
		PrincipalArn: exclusion.PrincipalArn,
	}, nil
}

type listPullTimeUpdateExclusionsInput struct {
	NextToken  string `json:"nextToken,omitempty"`
	MaxResults int    `json:"maxResults,omitempty"`
}

type listPullTimeUpdateExclusionsOutput struct {
	NextToken                string   `json:"nextToken,omitempty"`
	PullTimeUpdateExclusions []string `json:"pullTimeUpdateExclusions"`
}

func (h *Handler) handleListPullTimeUpdateExclusions(
	ctx context.Context,
	in *listPullTimeUpdateExclusionsInput,
) (*listPullTimeUpdateExclusionsOutput, error) {
	exclusions, err := h.Backend.ListPullTimeUpdateExclusions(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(exclusions))
	for _, exclusion := range exclusions {
		out = append(out, exclusion.PrincipalArn)
	}

	page, nextToken := paginatePullTimeUpdateExclusions(out, in.NextToken, in.MaxResults)

	return &listPullTimeUpdateExclusionsOutput{PullTimeUpdateExclusions: page, NextToken: nextToken}, nil
}

// paginatePullTimeUpdateExclusions applies the real API's maxResults/nextToken
// cursor-based pagination (opaque base64(principalArn) cursor, matching the
// convention used elsewhere in this package) over the already-sorted
// (PrincipalArn-ascending) exclusion list. AWS defaults maxResults to 100
// when unset.
func paginatePullTimeUpdateExclusions(arns []string, nextToken string, maxResults int) ([]string, string) {
	if nextToken != "" {
		if decoded, err := base64.StdEncoding.DecodeString(nextToken); err == nil {
			cursor := string(decoded)

			for i, arn := range arns {
				if arn == cursor {
					arns = arns[i:]

					break
				}
			}
		}
	}

	if maxResults <= 0 {
		maxResults = 100
	}

	if len(arns) <= maxResults {
		return arns, ""
	}

	return arns[:maxResults], base64.StdEncoding.EncodeToString([]byte(arns[maxResults]))
}
