package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// resourceServersPageSize is this backend's default page size for
// ListResourceServers; real AWS doesn't document an exact default, so this
// is chosen generously (larger than any realistic per-pool resource-server
// count) so pagination only activates when a caller explicitly requests a
// smaller MaxResults.
const resourceServersPageSize = 100

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

	pg := page.New(servers, in.NextToken, in.MaxResults, resourceServersPageSize)

	out := make([]resourceServerAccurateType, len(pg.Data))
	for i, rs := range pg.Data {
		out[i] = toResourceServerType(rs)
	}

	return &listResourceServersAccurateOutput{ResourceServers: out, NextToken: pg.Next}, nil
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

func (h *Handler) resourceServersOpsB() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateResourceServer:   wrapAccuracy(h.handleCreateResourceServerAccurate),
		opDescribeResourceServer: wrapAccuracy(h.handleDescribeResourceServerAccurate),
		opListResourceServers:    wrapAccuracy(h.handleListResourceServersAccurate),
		opUpdateResourceServer:   wrapAccuracy(h.handleUpdateResourceServerAccurate),
		opDeleteResourceServer:   wrapAccuracy(h.handleDeleteResourceServerAccurate),
	}
}
