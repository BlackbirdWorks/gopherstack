package acmpca

import (
	"context"
	"encoding/json"
)

type listPermissionsInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	NextToken               string `json:"NextToken"`
	MaxResults              int    `json:"MaxResults"`
}

type permissionOutput struct {
	CertificateAuthorityArn string   `json:"CertificateAuthorityArn"`
	Policy                  string   `json:"Policy,omitempty"`
	Principal               string   `json:"Principal"`
	SourceAccount           string   `json:"SourceAccount,omitempty"`
	Actions                 []string `json:"Actions"`
	CreatedAt               int64    `json:"CreatedAt,omitempty"`
}

type listPermissionsOutput struct {
	NextToken   string             `json:"NextToken,omitempty"`
	Permissions []permissionOutput `json:"Permissions"`
}

type createPermissionInput struct {
	CertificateAuthorityArn string   `json:"CertificateAuthorityArn"`
	Principal               string   `json:"Principal"`
	SourceAccount           string   `json:"SourceAccount"`
	Actions                 []string `json:"Actions"`
}

type createPermissionOutput struct{}

type deletePermissionInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	Principal               string `json:"Principal"`
	SourceAccount           string `json:"SourceAccount"`
}

type deletePermissionOutput struct{}

func (h *Handler) jsonListPermissions(ctx context.Context, body []byte) (any, error) {
	var input listPermissionsInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	p, err := h.Backend.ListPermissions(ctx, input.CertificateAuthorityArn, input.NextToken, input.MaxResults)
	if err != nil {
		return nil, err
	}

	permissions := make([]permissionOutput, 0, len(p.Data))
	for _, permission := range p.Data {
		out := permissionOutput{
			Actions:                 copyStringSlice(permission.Actions),
			CertificateAuthorityArn: permission.CertificateAuthorityArn,
			Policy:                  permission.Policy,
			Principal:               permission.Principal,
			SourceAccount:           permission.SourceAccount,
		}
		if !permission.CreatedAt.IsZero() {
			out.CreatedAt = permission.CreatedAt.Unix()
		}
		permissions = append(permissions, out)
	}

	return &listPermissionsOutput{
		NextToken:   p.Next,
		Permissions: permissions,
	}, nil
}

func (h *Handler) jsonCreatePermission(ctx context.Context, body []byte) (any, error) {
	var input createPermissionInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if _, err := h.Backend.CreatePermission(
		ctx,
		input.CertificateAuthorityArn,
		input.Principal,
		input.SourceAccount,
		input.Actions,
	); err != nil {
		return nil, err
	}

	return &createPermissionOutput{}, nil
}

func (h *Handler) jsonDeletePermission(ctx context.Context, body []byte) (any, error) {
	var input deletePermissionInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.DeletePermission(
		ctx,
		input.CertificateAuthorityArn,
		input.Principal,
		input.SourceAccount,
	); err != nil {
		return nil, err
	}

	return &deletePermissionOutput{}, nil
}

func copyStringSlice(values []string) []string {
	return append([]string(nil), values...)
}
