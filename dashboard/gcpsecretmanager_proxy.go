package dashboard

import (
	"context"
	"net/http"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/protobuf/proto"
)

const (
	gcpSecretManagerProxyPrefix = "/dashboard/api/v1/gcp/secretmanager"
	gcpSecretsListSegs          = 3
	gcpSecretItemSegs           = 4
	gcpSecretVersionsSegs       = 5
	gcpSecretVersionSegs        = 6
	gcpSecretsKindIdx           = 2
	gcpSecretsVersionsIdx       = 4
	kindSecrets                 = "secrets"
	kindVersions                = "versions"
)

// registerGCPSecretManagerRoutes exposes Secret Manager to the SPA as the REST JSON
// shapes of projects.secrets(.versions), calling the backend in-process.
func (h *DashboardHandler) registerGCPSecretManagerRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPSecretManagerOps; ops != nil {
		route = (&gcpSecretManagerAPI{sm: ops.API()}).route
	}

	h.registerGCPRESTProxy(gcpSecretManagerProxyPrefix, route)
}

type gcpSecretManagerAPI struct {
	sm secretmanagerpb.SecretManagerServiceServer
}

func (a *gcpSecretManagerAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if n < gcpSecretsListSegs || r.segs[0] != gcpRESTProjectsSeg || r.segs[gcpSecretsKindIdx] != kindSecrets {
		return nil, errGCPUnsupported()
	}

	switch n {
	case gcpSecretsListSegs:
		return a.secrets(ctx, r)
	case gcpSecretItemSegs:
		return a.secret(ctx, r)
	case gcpSecretVersionsSegs:
		if r.segs[gcpSecretsVersionsIdx] == kindVersions && r.verb == "" && r.method == http.MethodGet {
			return a.sm.ListSecretVersions(ctx, &secretmanagerpb.ListSecretVersionsRequest{
				Parent: r.name(gcpSecretItemSegs), Filter: r.query.Get("filter"),
				PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
			})
		}
	case gcpSecretVersionSegs:
		if r.segs[gcpSecretsVersionsIdx] == kindVersions {
			return a.version(ctx, r)
		}
	}

	return nil, errGCPUnsupported()
}

func (a *gcpSecretManagerAPI) secrets(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	parent := r.name(gcpSecretsKindIdx)

	switch {
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.sm.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{
			Parent: parent, Filter: r.query.Get("filter"),
			PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	case r.method == http.MethodPost:
		s := &secretmanagerpb.Secret{}
		if err := r.unmarshal(s); err != nil {
			return nil, err
		}

		return a.sm.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
			Parent: parent, SecretId: r.query.Get("secretId"), Secret: s,
		})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpSecretManagerAPI) secret(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(gcpSecretItemSegs)

	switch {
	case r.verb == "addVersion" && r.method == http.MethodPost:
		req := &secretmanagerpb.AddSecretVersionRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Parent = name

		return a.sm.AddSecretVersion(ctx, req)
	case r.verb == "getIamPolicy" && r.method == http.MethodGet:
		return a.sm.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: name})
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.sm.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: name})
	case r.method == http.MethodPatch:
		s := &secretmanagerpb.Secret{}
		if err := r.unmarshal(s); err != nil {
			return nil, err
		}

		mask, err := r.mask()
		if err != nil {
			return nil, err
		}

		s.Name = name

		return a.sm.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{Secret: s, UpdateMask: mask})
	case r.method == http.MethodDelete:
		return a.sm.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{Name: name, Etag: r.query.Get("etag")})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpSecretManagerAPI) version(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(gcpSecretVersionSegs)

	switch {
	case r.verb == "" && r.method == http.MethodGet:
		return a.sm.GetSecretVersion(ctx, &secretmanagerpb.GetSecretVersionRequest{Name: name})
	case r.verb == "access" && r.method == http.MethodGet:
		return a.sm.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: name})
	case r.method != http.MethodPost:
		return nil, errGCPUnsupported()
	}

	switch r.verb {
	case "enable":
		return a.sm.EnableSecretVersion(ctx, &secretmanagerpb.EnableSecretVersionRequest{Name: name})
	case "disable":
		return a.sm.DisableSecretVersion(ctx, &secretmanagerpb.DisableSecretVersionRequest{Name: name})
	case "destroy":
		return a.sm.DestroySecretVersion(ctx, &secretmanagerpb.DestroySecretVersionRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}
