package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func toUserPoolReplicaWireType(r *UserPoolReplica) userPoolReplicaWireType {
	return userPoolReplicaWireType{
		RegionName:  r.RegionName,
		Role:        r.Role,
		Status:      r.Status,
		UserPoolArn: r.ARN,
	}
}

func (h *Handler) handleCreateUserPoolReplica(
	_ context.Context,
	in *createUserPoolReplicaInput,
) (*createUserPoolReplicaOutput, error) {
	replica, err := h.Backend.CreateUserPoolReplica(in.UserPoolID, in.RegionName, in.UserPoolTags)
	if err != nil {
		return nil, err
	}

	wire := toUserPoolReplicaWireType(replica)

	return &createUserPoolReplicaOutput{UserPoolReplica: &wire}, nil
}

func (h *Handler) handleDeleteUserPoolReplica(
	_ context.Context,
	in *deleteUserPoolReplicaInput,
) (*deleteUserPoolReplicaOutput, error) {
	replica, err := h.Backend.DeleteUserPoolReplica(in.UserPoolID, in.RegionName)
	if err != nil {
		return nil, err
	}

	wire := toUserPoolReplicaWireType(replica)

	return &deleteUserPoolReplicaOutput{UserPoolReplica: &wire}, nil
}

func (h *Handler) handleUpdateUserPoolReplica(
	_ context.Context,
	in *updateUserPoolReplicaInput,
) (*updateUserPoolReplicaOutput, error) {
	replica, err := h.Backend.UpdateUserPoolReplica(in.UserPoolID, in.RegionName, in.Status)
	if err != nil {
		return nil, err
	}

	wire := toUserPoolReplicaWireType(replica)

	return &updateUserPoolReplicaOutput{UserPoolReplica: &wire}, nil
}

func (h *Handler) handleListUserPoolReplicas(
	_ context.Context,
	in *listUserPoolReplicasInput,
) (*listUserPoolReplicasOutput, error) {
	replicas, err := h.Backend.ListUserPoolReplicas(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	out := make([]userPoolReplicaWireType, len(replicas))
	for i, r := range replicas {
		out[i] = toUserPoolReplicaWireType(r)
	}

	return &listUserPoolReplicasOutput{UserPoolReplicas: out}, nil
}

func (h *Handler) userPoolReplicasOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateUserPoolReplica": service.WrapOp(h.handleCreateUserPoolReplica),
		"DeleteUserPoolReplica": service.WrapOp(h.handleDeleteUserPoolReplica),
		"UpdateUserPoolReplica": service.WrapOp(h.handleUpdateUserPoolReplica),
		"ListUserPoolReplicas":  service.WrapOp(h.handleListUserPoolReplicas),
	}
}
