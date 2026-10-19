package dashboard

import (
	"context"
	"net/http"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"google.golang.org/protobuf/proto"
)

const (
	gcpIAMProxyPrefix = "/dashboard/api/v1/gcp/iam"

	iamPredefinedRolesSegs = 1
	iamParentSegs          = 2
	iamCollectionSegs      = 3
	iamItemSegs            = 4
	iamKeysSegs            = 5
	iamKeySegs             = 6
	iamKindIdx             = 2
	iamKeysIdx             = 4
	iamRolesKind           = "roles"
	iamAccountsKind        = "serviceAccounts"
	iamKeysKind            = "keys"
	iamVerbEnable          = "enable"
	iamVerbDisable         = "disable"
	iamVerbUndelete        = "undelete"
	iamVerbSignBlob        = "signBlob"
)

// registerGCPIAMRoutes exposes Cloud IAM service accounts, keys and roles to the SPA as the REST
// JSON shapes of the IAM admin API, calling the backend in-process.
func (h *DashboardHandler) registerGCPIAMRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPIAMOps; ops != nil {
		route = (&gcpIAMAPI{iam: ops.API()}).route
	}

	h.registerGCPRESTProxy(gcpIAMProxyPrefix, route)
}

type gcpIAMAPI struct {
	iam adminpb.IAMServer
}

func (a *gcpIAMAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)

	switch {
	case r.segs[0] == iamRolesKind && n == iamPredefinedRolesSegs:
		return a.predefinedRoles(ctx, r)
	case r.segs[0] != gcpRESTProjectsSeg || n < iamCollectionSegs:
		return nil, errGCPUnsupported()
	case r.segs[iamKindIdx] == iamRolesKind && n <= iamItemSegs:
		return a.roles(ctx, r)
	case r.segs[iamKindIdx] != iamAccountsKind:
		return nil, errGCPUnsupported()
	case n <= iamItemSegs:
		return a.accounts(ctx, r)
	case n <= iamKeySegs && r.segs[iamKeysIdx] == iamKeysKind:
		return a.keys(ctx, r)
	}

	return nil, errGCPUnsupported()
}

func (a *gcpIAMAPI) predefinedRoles(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if r.method != http.MethodGet || r.verb != "" {
		return nil, errGCPUnsupported()
	}

	return a.iam.ListRoles(ctx, &adminpb.ListRolesRequest{
		PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"), View: adminpb.RoleView_FULL,
	})
}

func (a *gcpIAMAPI) accounts(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if len(r.segs) == iamCollectionSegs {
		return a.accountCollection(ctx, r)
	}

	name := r.name(iamItemSegs)
	post := r.method == http.MethodPost

	switch {
	case r.verb == iamVerbEnable && post:
		return a.iam.EnableServiceAccount(ctx, &adminpb.EnableServiceAccountRequest{Name: name})
	case r.verb == iamVerbDisable && post:
		return a.iam.DisableServiceAccount(ctx, &adminpb.DisableServiceAccountRequest{Name: name})
	case r.verb == iamVerbUndelete && post:
		return a.iam.UndeleteServiceAccount(ctx, &adminpb.UndeleteServiceAccountRequest{Name: name})
	case r.verb == iamVerbSignBlob && post:
		return a.signBlob(ctx, r, name)
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.iam.GetServiceAccount(ctx, &adminpb.GetServiceAccountRequest{Name: name})
	case r.method == http.MethodDelete:
		return a.iam.DeleteServiceAccount(ctx, &adminpb.DeleteServiceAccountRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpIAMAPI) accountCollection(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	parent := r.name(iamParentSegs)

	switch {
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.iam.ListServiceAccounts(ctx, &adminpb.ListServiceAccountsRequest{
			Name: parent, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	case r.method == http.MethodPost:
		req := &adminpb.CreateServiceAccountRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Name = parent

		return a.iam.CreateServiceAccount(ctx, req)
	}

	return nil, errGCPUnsupported()
}

func (a *gcpIAMAPI) signBlob(ctx context.Context, r gcpRESTCall, name string) (proto.Message, error) {
	req := &adminpb.SignBlobRequest{}
	if err := r.unmarshal(req); err != nil {
		return nil, err
	}

	req.Name = name //nolint:staticcheck // deprecated upstream, still the wire field

	return a.iam.SignBlob(ctx, req) //nolint:staticcheck // deprecated upstream, still served
}

func (a *gcpIAMAPI) keys(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if len(r.segs) == iamKeysSegs {
		return a.keyCollection(ctx, r)
	}

	name := r.name(iamKeySegs)
	post := r.method == http.MethodPost

	switch {
	case r.verb == iamVerbEnable && post:
		return a.iam.EnableServiceAccountKey(ctx, &adminpb.EnableServiceAccountKeyRequest{Name: name})
	case r.verb == iamVerbDisable && post:
		return a.iam.DisableServiceAccountKey(ctx, &adminpb.DisableServiceAccountKeyRequest{Name: name})
	case r.verb == "" && r.method == http.MethodDelete:
		return a.iam.DeleteServiceAccountKey(ctx, &adminpb.DeleteServiceAccountKeyRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpIAMAPI) keyCollection(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(iamItemSegs)

	switch {
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.iam.ListServiceAccountKeys(ctx, &adminpb.ListServiceAccountKeysRequest{
			Name: name, KeyTypes: iamKeyTypes(r),
		})
	case r.method == http.MethodPost:
		req := &adminpb.CreateServiceAccountKeyRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Name = name

		return a.iam.CreateServiceAccountKey(ctx, req)
	}

	return nil, errGCPUnsupported()
}

func iamKeyTypes(r gcpRESTCall) []adminpb.ListServiceAccountKeysRequest_KeyType {
	var out []adminpb.ListServiceAccountKeysRequest_KeyType

	for _, v := range r.query["keyTypes"] {
		if t, ok := adminpb.ListServiceAccountKeysRequest_KeyType_value[v]; ok {
			out = append(out, adminpb.ListServiceAccountKeysRequest_KeyType(t))
		}
	}

	return out
}

func (a *gcpIAMAPI) roles(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if len(r.segs) == iamCollectionSegs {
		return a.roleCollection(ctx, r)
	}

	name := r.name(iamItemSegs)

	switch {
	case r.verb == iamVerbUndelete && r.method == http.MethodPost:
		return a.iam.UndeleteRole(ctx, &adminpb.UndeleteRoleRequest{Name: name})
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodDelete:
		return a.iam.DeleteRole(ctx, &adminpb.DeleteRoleRequest{Name: name})
	case r.method == http.MethodPatch:
		return a.updateRole(ctx, r, name)
	}

	return nil, errGCPUnsupported()
}

func (a *gcpIAMAPI) updateRole(ctx context.Context, r gcpRESTCall, name string) (proto.Message, error) {
	role := &adminpb.Role{}
	if err := r.unmarshal(role); err != nil {
		return nil, err
	}

	mask, err := r.mask()
	if err != nil {
		return nil, err
	}

	return a.iam.UpdateRole(ctx, &adminpb.UpdateRoleRequest{Name: name, Role: role, UpdateMask: mask})
}

func (a *gcpIAMAPI) roleCollection(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	parent := r.name(iamParentSegs)

	switch {
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.iam.ListRoles(ctx, &adminpb.ListRolesRequest{
			Parent: parent, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
			View: adminpb.RoleView_FULL, ShowDeleted: r.query.Get("showDeleted") == queryTrue,
		})
	case r.method == http.MethodPost:
		req := &adminpb.CreateRoleRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Parent = parent

		return a.iam.CreateRole(ctx, req)
	}

	return nil, errGCPUnsupported()
}
