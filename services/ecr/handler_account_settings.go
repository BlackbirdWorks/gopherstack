package ecr

import (
	"context"
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

type listPullTimeUpdateExclusionsOutput struct {
	PullTimeUpdateExclusions []string `json:"pullTimeUpdateExclusions"`
}

func (h *Handler) handleListPullTimeUpdateExclusions(
	ctx context.Context,
	_ *emptyInput,
) (*listPullTimeUpdateExclusionsOutput, error) {
	exclusions, err := h.Backend.ListPullTimeUpdateExclusions(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(exclusions))
	for _, exclusion := range exclusions {
		out = append(out, exclusion.PrincipalArn)
	}

	return &listPullTimeUpdateExclusionsOutput{PullTimeUpdateExclusions: out}, nil
}
