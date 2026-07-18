package codebuild

import (
	"context"
	"fmt"
)

type deleteSourceCredentialsInput struct {
	Arn string `json:"arn"`
}

type deleteSourceCredentialsOutput struct {
	Arn string `json:"arn"`
}

func (h *Handler) handleDeleteSourceCredentials(
	_ context.Context,
	in *deleteSourceCredentialsInput,
) (*deleteSourceCredentialsOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: arn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteSourceCredentials(in.Arn); err != nil {
		return nil, err
	}

	return &deleteSourceCredentialsOutput{Arn: in.Arn}, nil
}

type importSourceCredentialsInput struct {
	AuthType   string `json:"authType"`
	ServerType string `json:"serverType"`
	Token      string `json:"token"`
	Username   string `json:"username"`
}

type importSourceCredentialsOutput struct {
	Arn string `json:"arn"`
}

func (h *Handler) handleImportSourceCredentials(
	_ context.Context,
	in *importSourceCredentialsInput,
) (*importSourceCredentialsOutput, error) {
	if in.Token == "" {
		return nil, fmt.Errorf("%w: token is required", errInvalidRequest)
	}

	arnStr, err := h.Backend.ImportSourceCredentials(in.AuthType, in.ServerType, in.Token)
	if err != nil {
		return nil, err
	}

	return &importSourceCredentialsOutput{Arn: arnStr}, nil
}

type listSourceCredentialsInput struct{}

type listSourceCredentialsOutput struct {
	SourceCredentialsInfos []map[string]any `json:"sourceCredentialsInfos"`
}

func (h *Handler) handleListSourceCredentials(
	_ context.Context,
	_ *listSourceCredentialsInput,
) (*listSourceCredentialsOutput, error) {
	creds := h.Backend.ListSourceCredentials()
	infos := make([]map[string]any, 0, len(creds))
	for _, c := range creds {
		infos = append(infos, map[string]any{
			"arn":        c.Arn,
			"serverType": c.ServerType,
			"authType":   c.AuthType,
		})
	}

	return &listSourceCredentialsOutput{SourceCredentialsInfos: infos}, nil
}
