package dashboard

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/blackbirdworks/gopherstack/services/gcpfunctions"
)

const (
	gcpFunctionsProxyPrefix = "/dashboard/api/v1/gcp/functions"

	fnKindIdx   = 4
	fnCollSegs  = 5
	fnItemSegs  = 6
	fnKindFuncs = "functions"
	fnKindRTs   = "runtimes"
	fnVerbUp    = "upload"
	fnVerbGen   = "generateUploadUrl"
	fnVerbCall  = "invoke"
)

func invokeMethodOK(m string) bool {
	switch m {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodHead:
		return true
	}

	return false
}

// registerGCPFunctionsRoutes exposes Cloud Functions to the SPA as REST JSON, calling the backend in-process.
func (h *DashboardHandler) registerGCPFunctionsRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPFunctionsOps; ops != nil {
		route = (&gcpFunctionsAPI{api: ops.API()}).route
	}

	h.registerGCPRESTProxy(gcpFunctionsProxyPrefix, route)
}

type gcpFunctionsAPI struct {
	api *gcpfunctions.API
}

func (a *gcpFunctionsAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if n < fnCollSegs || n > fnItemSegs || r.segs[0] != gcpRESTProjectsSeg ||
		r.segs[schedLocationsIdx] != schedLocationsKind {
		return nil, errGCPUnsupported()
	}

	parent := r.name(fnKindIdx)

	switch {
	case r.segs[fnKindIdx] == fnKindRTs && n == fnCollSegs && r.method == http.MethodGet && r.verb == "":
		return a.api.Functions.ListRuntimes(ctx, &functionspb.ListRuntimesRequest{Parent: parent})
	case r.segs[fnKindIdx] != fnKindFuncs:
		return nil, errGCPUnsupported()
	case n == fnCollSegs:
		return a.collection(ctx, r, parent)
	}

	return a.item(ctx, r)
}

func (a *gcpFunctionsAPI) collection(ctx context.Context, r gcpRESTCall, parent string) (proto.Message, error) {
	switch {
	case r.verb == fnVerbGen && r.method == http.MethodPost:
		return a.api.Functions.GenerateUploadUrl(ctx, &functionspb.GenerateUploadUrlRequest{Parent: parent})
	case r.verb == fnVerbUp && r.method == http.MethodPut:
		return nil, a.api.Upload(ctx, r.query.Get("uploadUrl"), r.body)
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.api.Functions.ListFunctions(ctx, &functionspb.ListFunctionsRequest{
			Parent: parent, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	case r.method == http.MethodPost:
		fn := &functionspb.Function{}
		if err := r.unmarshal(fn); err != nil {
			return nil, err
		}

		return a.api.Functions.CreateFunction(ctx, &functionspb.CreateFunctionRequest{
			Parent: parent, FunctionId: r.query.Get("functionId"), Function: fn,
		})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpFunctionsAPI) item(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(fnItemSegs)

	switch {
	case r.verb == fnVerbCall && r.method == http.MethodPost:
		return a.invoke(ctx, name, r.body)
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.api.Functions.GetFunction(ctx, &functionspb.GetFunctionRequest{Name: name})
	case r.method == http.MethodDelete:
		return a.api.Functions.DeleteFunction(ctx, &functionspb.DeleteFunctionRequest{Name: name})
	case r.method == http.MethodPatch:
		fn := &functionspb.Function{}
		if err := r.unmarshal(fn); err != nil {
			return nil, err
		}

		mask, err := r.mask()
		if err != nil {
			return nil, err
		}

		fn.Name = name

		return a.api.Functions.UpdateFunction(ctx, &functionspb.UpdateFunctionRequest{Function: fn, UpdateMask: mask})
	}

	return nil, errGCPUnsupported()
}

type invokeInput struct {
	Headers map[string]string `json:"headers"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Body    string            `json:"body"`
}

func (a *gcpFunctionsAPI) invoke(ctx context.Context, name string, raw []byte) (proto.Message, error) {
	var in invokeInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid request body: "+err.Error())
	}

	if in.Method == "" {
		in.Method = http.MethodGet
	}

	if !invokeMethodOK(in.Method) {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported method %q", in.Method)
	}

	hdr := http.Header{}
	for k, v := range in.Headers {
		hdr.Set(k, v)
	}

	res, err := a.api.Invoke(ctx, name, gcpfunctions.InvokeRequest{
		Method: in.Method, Path: in.Path, Header: hdr, Body: []byte(in.Body),
	})
	if err != nil {
		return nil, err
	}

	return invokeResult(res)
}

func invokeResult(res *gcpfunctions.InvokeResult) (proto.Message, error) {
	headers := make(map[string]any, len(res.Header))
	for k := range res.Header {
		headers[k] = res.Header.Get(k)
	}

	return structpb.NewStruct(map[string]any{
		"status":  float64(res.Status),
		"headers": headers,
		"body":    string(res.Body),
		"bodyB64": base64.StdEncoding.EncodeToString(res.Body),
	})
}
