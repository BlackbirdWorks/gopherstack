package transfer

import (
	"context"
	"fmt"
)

type importSSHPublicKeyInput struct {
	ServerID         string `json:"ServerId"`
	UserName         string `json:"UserName"`
	SSHPublicKeyBody string `json:"SshPublicKeyBody"`
}

type importSSHPublicKeyOutput struct {
	ServerID       string `json:"ServerId"`
	SSHPublicKeyID string `json:"SshPublicKeyId"`
	UserName       string `json:"UserName"`
}

func (h *Handler) handleImportSSHPublicKey(
	_ context.Context,
	in *importSSHPublicKeyInput,
) (*importSSHPublicKeyOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	k, err := h.Backend.ImportSSHPublicKey(in.ServerID, in.UserName, in.SSHPublicKeyBody)
	if err != nil {
		return nil, err
	}

	return &importSSHPublicKeyOutput{
		ServerID:       k.ServerID,
		SSHPublicKeyID: k.SSHPublicKeyID,
		UserName:       k.UserName,
	}, nil
}

type deleteSSHPublicKeyInput struct {
	ServerID       string `json:"ServerId"`
	UserName       string `json:"UserName"`
	SSHPublicKeyID string `json:"SshPublicKeyId"`
}

func (h *Handler) handleDeleteSSHPublicKey(
	_ context.Context,
	in *deleteSSHPublicKeyInput,
) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	if in.SSHPublicKeyID == "" {
		return nil, fmt.Errorf("%w: SSHPublicKeyID is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteSSHPublicKey(in.ServerID, in.UserName, in.SSHPublicKeyID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}
