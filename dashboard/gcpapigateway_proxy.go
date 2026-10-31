package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"

	"cloud.google.com/go/apigateway/apiv1/apigatewaypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/blackbirdworks/gopherstack/services/gcpapigateway"
)

const (
	gcpAPIGWPrefix     = "/dashboard/api/v1/gcp/apigateway"
	gcpAPIGWScopeSegs  = 4
	gcpAPIGWListSegs   = 5
	gcpAPIGWItemSegs   = 6
	gcpAPIGWSubSegs    = 7
	gcpAPIGWSubItem    = 8
	gcpAPIGWLocIdx     = 3
	gcpAPIGWKindIdx    = 4
	gcpAPIGWSubKindIdx = 6
	gcpAPIGWKeyParam   = "key"
	gcpAPIGWMaxKey     = 256
	gcpAPIGWInvoke     = "invoke"
	gcpAPIGWAPIs       = "apis"
	gcpAPIGWOps        = "operations"
	gcpAPIGWGlobal     = "global"
	gcpAPIGWMaxBody    = ociAPIGWMaxTryJSON + 2*gcpAPIGWMaxKey
)

var gcpAPIGWIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// registerGCPAPIGatewayRoutes serves the API Gateway page by calling the control plane in-process. Reads cover APIs,
// configs, gateways and operations; the only writes are creating a gateway from an existing config (body rebuilt from
// fixed fields) and deleting a gateway. "Try it" replays one request through the in-process data plane of a selected
// ACTIVE gateway; the destination is never taken from the request and an API key is only ever forwarded as `key`.
func (h *DashboardHandler) registerGCPAPIGatewayRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPAPIGatewayOps; ops != nil {
		px := &gcpAPIGWProxy{api: ops.API(), ops: ops.OperationsAPI(), be: ops.Backend, data: ops.Backend.DataPlane()}
		route = px.route
	}

	h.registerGCPRESTProxy(gcpAPIGWPrefix, route)
}

type gcpAPIGWProxy struct {
	api  apigatewaypb.ApiGatewayServiceServer
	ops  longrunningpb.OperationsServer
	be   *gcpapigateway.InMemoryBackend
	data http.Handler
}

func (a *gcpAPIGWProxy) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if len(r.segs) == 1 && r.segs[0] == gcpRESTProjectsSeg && r.method == http.MethodGet && r.verb == "" {
		return structOf(map[string]any{gcpRESTProjectsSeg: a.be.ProjectIDs()})
	}

	n := len(r.segs)
	if n < gcpAPIGWListSegs || n > gcpAPIGWSubItem || r.segs[0] != gcpRESTProjectsSeg ||
		r.segs[gcpAPIGWScopeSegs-2] != "locations" || !gcpAPIGWIDs(r.segs[1], r.segs[gcpAPIGWLocIdx]) {
		return nil, errGCPUnsupported()
	}

	switch r.segs[gcpAPIGWKindIdx] {
	case gcpAPIGWAPIs:
		return a.apis(ctx, r)
	case "gateways":
		return a.gateways(ctx, r)
	case gcpAPIGWOps:
		return a.operations(ctx, r)
	}

	return nil, errGCPUnsupported()
}

func gcpAPIGWIDs(ids ...string) bool {
	for _, id := range ids {
		if id != "-" && !gcpAPIGWIDRE.MatchString(id) {
			return false
		}
	}

	return true
}

func (a *gcpAPIGWProxy) apis(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if r.method != http.MethodGet || r.verb != "" || (n > gcpAPIGWListSegs && !gcpAPIGWIDs(r.segs[gcpAPIGWListSegs])) {
		return nil, errGCPUnsupported()
	}

	switch {
	case n == gcpAPIGWListSegs:
		return a.api.ListApis(ctx, &apigatewaypb.ListApisRequest{
			Parent: r.name(gcpAPIGWScopeSegs), PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	case n == gcpAPIGWItemSegs:
		return a.api.GetApi(ctx, &apigatewaypb.GetApiRequest{Name: r.name(n)})
	case n == gcpAPIGWSubSegs && r.segs[gcpAPIGWSubKindIdx] == "configs":
		return a.api.ListApiConfigs(ctx, &apigatewaypb.ListApiConfigsRequest{
			Parent: r.name(gcpAPIGWItemSegs), PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	case n == gcpAPIGWSubItem && r.segs[gcpAPIGWSubKindIdx] == "configs" && gcpAPIGWIDs(r.segs[n-1]):
		return a.api.GetApiConfig(ctx, &apigatewaypb.GetApiConfigRequest{
			Name: r.name(n), View: apigatewaypb.GetApiConfigRequest_FULL,
		})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpAPIGWProxy) gateways(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)

	switch {
	case n == gcpAPIGWListSegs && r.verb == "" && r.method == http.MethodGet:
		return a.api.ListGateways(ctx, &apigatewaypb.ListGatewaysRequest{
			Parent: r.name(gcpAPIGWScopeSegs), PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	case n == gcpAPIGWListSegs && r.verb == "" && r.method == http.MethodPost:
		return a.createGateway(ctx, r)
	case n != gcpAPIGWItemSegs || !gcpAPIGWIDs(r.segs[n-1]):
		return nil, errGCPUnsupported()
	case r.verb == "" && r.method == http.MethodGet:
		return a.api.GetGateway(ctx, &apigatewaypb.GetGatewayRequest{Name: r.name(n)})
	case r.verb == "" && r.method == http.MethodDelete:
		return a.api.DeleteGateway(ctx, &apigatewaypb.DeleteGatewayRequest{Name: r.name(n)})
	case r.verb == gcpAPIGWInvoke && r.method == http.MethodPost:
		return a.tryIt(ctx, r)
	}

	return nil, errGCPUnsupported()
}

// createGateway forwards only the display name and the config reference of the caller's body.
func (a *gcpAPIGWProxy) createGateway(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	in := &apigatewaypb.Gateway{}
	if err := r.unmarshal(in); err != nil {
		return nil, err
	}

	id := r.query.Get("gatewayId")
	cfg := in.GetApiConfig()
	prefix := "projects/" + r.segs[1] + "/locations/" + gcpAPIGWGlobal + "/apis/"

	if !gcpAPIGWIDs(id) || !strings.HasPrefix(cfg, prefix) || len(cfg) > len(prefix)+2*gcpAPIGWMaxKey ||
		strings.Count(cfg, "/") != strings.Count(prefix, "/")+2 || strings.Contains(cfg, "..") {
		return nil, status.Error(codes.InvalidArgument, "gatewayId and a project api_config are required")
	}

	return a.api.CreateGateway(ctx, &apigatewaypb.CreateGatewayRequest{
		Parent: r.name(gcpAPIGWScopeSegs), GatewayId: id,
		Gateway: &apigatewaypb.Gateway{DisplayName: in.GetDisplayName(), ApiConfig: cfg},
	})
}

func (a *gcpAPIGWProxy) operations(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if r.method != http.MethodGet || r.verb != "" {
		return nil, errGCPUnsupported()
	}

	switch {
	case n == gcpAPIGWListSegs:
		return a.ops.ListOperations(ctx, &longrunningpb.ListOperationsRequest{
			Name: r.name(gcpAPIGWScopeSegs), PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	case n == gcpAPIGWItemSegs && gcpAPIGWIDs(r.segs[n-1]):
		return a.ops.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: r.name(n)})
	}

	return nil, errGCPUnsupported()
}

type gcpAPIGWTryIn struct {
	APIKey string `json:"apiKey"`
	ociAPIGWTryIn
}

func (a *gcpAPIGWProxy) tryIt(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	bad := status.Error(codes.InvalidArgument, "invalid API Gateway try-it request")

	var in gcpAPIGWTryIn
	if len(r.body) > gcpAPIGWMaxBody || json.Unmarshal(r.body, &in) != nil {
		return nil, bad
	}

	target, ok := gcpAPIGWTryTarget(r.segs[1], r.segs[gcpAPIGWLocIdx], r.segs[len(r.segs)-1], in)
	if !ok {
		return nil, bad
	}

	gw, err := a.api.GetGateway(ctx, &apigatewaypb.GetGatewayRequest{Name: r.name(gcpAPIGWItemSegs)})
	if err != nil {
		return nil, err
	}

	if gw.GetState() != apigatewaypb.Gateway_ACTIVE {
		return nil, status.Error(codes.FailedPrecondition, "Gateway is in state "+gw.GetState().String())
	}

	req := httptest.NewRequestWithContext(ctx, in.Method, target, strings.NewReader(in.Body))

	for _, hd := range in.Headers {
		if ociAPIGWHeaderOK(hd.Name) {
			req.Header.Add(hd.Name, hd.Value)
		}
	}

	rec := httptest.NewRecorder()
	a.data.ServeHTTP(rec, req)

	return structOf(ociAPIGWTryOut(rec, in.APIKey))
}

// gcpAPIGWTryTarget validates the request and returns the path-style data-plane URL of one gateway, with the API key
// (when given) as the only `key` query parameter.
func gcpAPIGWTryTarget(project, location, gateway string, in gcpAPIGWTryIn) (string, bool) {
	rel, ok := ociAPIGWTryPath(in.ociAPIGWTryIn)
	if !ok || !ociAPIGWMethodOK(in.Method) || len(in.Headers) > ociAPIGWMaxHeaders {
		return "", false
	}

	if len(in.Body) > ociAPIGWMaxReqBody || len(in.APIKey) > gcpAPIGWMaxKey ||
		strings.ContainsAny(in.APIKey, "\r\n\x00") {
		return "", false
	}

	path, rawQuery, _ := strings.Cut(rel, "?")
	q, _ := url.ParseQuery(rawQuery)

	if in.APIKey != "" {
		q.Set(gcpAPIGWKeyParam, in.APIKey)
	}

	target := gcpapigateway.PathPrefix + url.PathEscape(project) + "/" + url.PathEscape(location) + "/" +
		url.PathEscape(gateway) + path
	if len(q) > 0 {
		target += "?" + q.Encode()
	}

	return target, true
}

// structOf converts a JSON-compatible value into the protobuf Struct the REST proxy renders.
func structOf(v any) (proto.Message, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, status.Error(codes.Internal, "unreadable response")
	}

	s := &structpb.Struct{}
	if err = (protojson.UnmarshalOptions{}).Unmarshal(raw, s); err != nil {
		return nil, status.Error(codes.Internal, "unreadable response")
	}

	return s, nil
}
