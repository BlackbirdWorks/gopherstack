package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func toResourceServerType(rs *ResourceServer) resourceServerAccurateType {
	scopes := make([]resourceServerScopeType, len(rs.Scopes))
	for i, s := range rs.Scopes {
		scopes[i] = resourceServerScopeType(s)
	}

	return resourceServerAccurateType{
		UserPoolID: rs.UserPoolID,
		Identifier: rs.Identifier,
		Name:       rs.Name,
		Scopes:     scopes,
	}
}

func toBackendScopes(scopes []resourceServerScopeType) []ResourceServerScope {
	out := make([]ResourceServerScope, len(scopes))
	for i, s := range scopes {
		out[i] = ResourceServerScope(s)
	}

	return out
}

func (h *Handler) handleCreateResourceServerAccurate(
	_ context.Context,
	in *createResourceServerAccurateInput,
) (*createResourceServerAccurateOutput, error) {
	rs, err := h.Backend.CreateResourceServer(in.UserPoolID, in.Identifier, in.Name, toBackendScopes(in.Scopes))
	if err != nil {
		return nil, err
	}

	return &createResourceServerAccurateOutput{ResourceServer: toResourceServerType(rs)}, nil
}

func (h *Handler) handleDescribeResourceServerAccurate(
	_ context.Context,
	in *describeResourceServerAccurateInput,
) (*describeResourceServerAccurateOutput, error) {
	rs, err := h.Backend.DescribeResourceServer(in.UserPoolID, in.Identifier)
	if err != nil {
		return nil, err
	}

	return &describeResourceServerAccurateOutput{ResourceServer: toResourceServerType(rs)}, nil
}

func (h *Handler) handleListResourceServersAccurate(
	_ context.Context,
	in *listResourceServersAccurateInput,
) (*listResourceServersAccurateOutput, error) {
	servers, err := h.Backend.ListResourceServers(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	out := make([]resourceServerAccurateType, len(servers))
	for i, rs := range servers {
		out[i] = toResourceServerType(rs)
	}

	return &listResourceServersAccurateOutput{ResourceServers: out}, nil
}

func (h *Handler) handleUpdateResourceServerAccurate(
	_ context.Context,
	in *updateResourceServerAccurateInput,
) (*updateResourceServerAccurateOutput, error) {
	rs, err := h.Backend.UpdateResourceServer(in.UserPoolID, in.Identifier, in.Name, toBackendScopes(in.Scopes))
	if err != nil {
		return nil, err
	}

	return &updateResourceServerAccurateOutput{ResourceServer: toResourceServerType(rs)}, nil
}

func (h *Handler) handleDeleteResourceServerAccurate(
	_ context.Context,
	in *deleteResourceServerAccurateInput,
) (*deleteResourceServerAccurateOutput, error) {
	if err := h.Backend.DeleteResourceServer(in.UserPoolID, in.Identifier); err != nil {
		return nil, err
	}

	return &deleteResourceServerAccurateOutput{}, nil
}

func (h *Handler) handleCreateResourceServer(
	_ context.Context,
	in *createResourceServerInput,
) (*createResourceServerOutput, error) {
	return &createResourceServerOutput{
		ResourceServer: &resourceServerType{UserPoolID: in.UserPoolID, Identifier: in.Identifier, Name: in.Name},
	}, nil
}

func (h *Handler) handleDeleteResourceServer(
	_ context.Context,
	_ *deleteResourceServerInput,
) (*deleteResourceServerOutput, error) {
	return &deleteResourceServerOutput{}, nil
}

func (h *Handler) handleDescribeResourceServer(
	_ context.Context,
	in *describeResourceServerInput,
) (*describeResourceServerOutput, error) {
	return &describeResourceServerOutput{
		ResourceServer: &resourceServerType{UserPoolID: in.UserPoolID, Identifier: in.Identifier},
	}, nil
}

func (h *Handler) handleListResourceServers(
	_ context.Context,
	_ *listResourceServersInput,
) (*listResourceServersOutput, error) {
	return &listResourceServersOutput{ResourceServers: []resourceServerType{}}, nil
}

func (h *Handler) handleUpdateResourceServer(
	_ context.Context,
	in *updateResourceServerInput,
) (*updateResourceServerOutput, error) {
	return &updateResourceServerOutput{
		ResourceServer: &resourceServerType{UserPoolID: in.UserPoolID, Identifier: in.Identifier, Name: in.Name},
	}, nil
}

func (h *Handler) resourceServersOpsA() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateResourceServer":   service.WrapOp(h.handleCreateResourceServer),
		"DeleteResourceServer":   service.WrapOp(h.handleDeleteResourceServer),
		"DescribeResourceServer": service.WrapOp(h.handleDescribeResourceServer),
		"ListResourceServers":    service.WrapOp(h.handleListResourceServers),
		"UpdateResourceServer":   service.WrapOp(h.handleUpdateResourceServer),
	}
}

func (h *Handler) resourceServersOpsB() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateResourceServer:   wrapAccuracy(h.handleCreateResourceServerAccurate),
		opDescribeResourceServer: wrapAccuracy(h.handleDescribeResourceServerAccurate),
		opListResourceServers:    wrapAccuracy(h.handleListResourceServersAccurate),
		opUpdateResourceServer:   wrapAccuracy(h.handleUpdateResourceServerAccurate),
		opDeleteResourceServer:   wrapAccuracy(h.handleDeleteResourceServerAccurate),
	}
}
