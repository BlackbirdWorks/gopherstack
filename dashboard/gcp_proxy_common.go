package dashboard

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	gcpRESTMaxBodyBytes = 1 << 20
	gcpRESTVersion      = "v1"
	gcpRESTProjectsSeg  = "projects"
	gcpRESTNameSegs     = 4
)

// gcpRESTCall is one parsed proxy request: HTTP method, path segments after
// /v1 (colon verbs split into verb), query and raw body.
type gcpRESTCall struct {
	query  url.Values
	method string
	verb   string
	segs   []string
	body   []byte
}

// name joins the first n segments into a resource name.
func (r gcpRESTCall) name(n int) string { return strings.Join(r.segs[:n], "/") }

func (r gcpRESTCall) unmarshal(m proto.Message) error {
	if len(r.body) == 0 {
		return nil
	}

	if err := (protojson.UnmarshalOptions{}).Unmarshal(r.body, m); err != nil {
		return status.Error(codes.InvalidArgument, "invalid request body: "+err.Error())
	}

	return nil
}

func (r gcpRESTCall) pageSize() int32 {
	n, err := strconv.ParseInt(r.query.Get("pageSize"), 10, 32)
	if err != nil || n < 0 {
		return 0
	}

	return int32(n)
}

func (r gcpRESTCall) mask() (*fieldmaskpb.FieldMask, error) {
	fm := &fieldmaskpb.FieldMask{}
	quoted := []byte(strconv.Quote(r.query.Get("updateMask")))
	if err := (protojson.UnmarshalOptions{}).Unmarshal(quoted, fm); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid updateMask: "+err.Error())
	}

	return fm, nil
}

func errGCPUnsupported() error {
	return status.Error(codes.NotFound, "unsupported dashboard proxy request")
}

// parseGCPRESTPath splits an escaped path below the proxy prefix into segments
// (after "v1"), peeling a trailing ":verb" off the last one.
func parseGCPRESTPath(escaped, version string) ([]string, string, bool) {
	if !strings.HasPrefix(escaped, "/") {
		return nil, "", false
	}

	parts := strings.Split(escaped[1:], "/")
	segs := make([]string, 0, len(parts))

	for _, part := range parts {
		s, err := url.PathUnescape(part)
		if err != nil || s == "" || s == "." || s == ".." {
			return nil, "", false
		}

		segs = append(segs, s)
	}

	if segs[0] != version {
		return nil, "", false
	}

	segs = segs[1:]

	if len(segs) == 0 {
		return nil, "", false
	}

	verb := ""
	last := len(segs) - 1

	if i := strings.LastIndex(segs[last], ":"); i > 0 {
		verb = segs[last][i+1:]
		segs[last] = segs[last][:i]
	}

	return segs, verb, true
}

type gcpRESTRoute func(ctx context.Context, call gcpRESTCall) (proto.Message, error)

// registerGCPRESTProxy mounts an in-process JSON proxy; route is nil when the service is not served.
func (h *DashboardHandler) registerGCPRESTProxy(prefix string, route gcpRESTRoute) {
	h.registerGCPRESTProxyVersion(prefix, gcpRESTVersion, route)
}

// registerGCPRESTProxyVersion is registerGCPRESTProxy for APIs whose path starts with a version other than v1.
func (h *DashboardHandler) registerGCPRESTProxyVersion(prefix, version string, route gcpRESTRoute) {
	serve := func(c *echo.Context) error {
		if route == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{keyError: errServiceUnavailable})
		}

		r := c.Request()

		segs, verb, ok := parseGCPRESTPath(strings.TrimPrefix(r.URL.EscapedPath(), prefix), version)
		if !ok {
			return c.String(http.StatusBadRequest, "unsupported GCP proxy request")
		}

		body, err := io.ReadAll(http.MaxBytesReader(c.Response(), r.Body, gcpRESTMaxBodyBytes))
		if err != nil {
			return writeGCPRESTError(c, status.Error(codes.InvalidArgument, "request body too large or unreadable"))
		}

		out, err := route(r.Context(), gcpRESTCall{
			method: r.Method, segs: segs, verb: verb, query: r.URL.Query(), body: body,
		})
		if err != nil {
			return writeGCPRESTError(c, err)
		}

		if out == nil {
			out = &emptypb.Empty{}
		}

		b, err := protojson.Marshal(out)
		if err != nil {
			return writeGCPRESTError(c, status.Error(codes.Internal, err.Error()))
		}

		return c.JSONBlob(http.StatusOK, b)
	}

	for _, method := range []string{
		http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete,
	} {
		h.SubRouter.Add(method, prefix+"/*", serve)
	}
}

func gcpHTTPStatus(code codes.Code) (int, string) {
	switch code {
	case codes.InvalidArgument:
		return http.StatusBadRequest, "INVALID_ARGUMENT"
	case codes.NotFound:
		return http.StatusNotFound, "NOT_FOUND"
	case codes.AlreadyExists:
		return http.StatusConflict, "ALREADY_EXISTS"
	case codes.FailedPrecondition:
		return http.StatusBadRequest, "FAILED_PRECONDITION"
	case codes.PermissionDenied:
		return http.StatusForbidden, "PERMISSION_DENIED"
	case codes.Unimplemented:
		return http.StatusNotImplemented, "UNIMPLEMENTED"
	case codes.OutOfRange:
		return http.StatusBadRequest, "OUT_OF_RANGE"
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests, "RESOURCE_EXHAUSTED"
	default:
		return http.StatusInternalServerError, "INTERNAL"
	}
}

// writeGCPRESTError renders a gRPC status in the Google REST error envelope.
func writeGCPRESTError(c *echo.Context, err error) error {
	st := status.Convert(err)
	httpCode, name := gcpHTTPStatus(st.Code())

	return c.JSON(httpCode, gcpRESTErrorEnvelope{Error: gcpRESTErrorBody{
		Code: httpCode, Message: st.Message(), Status: name,
	}})
}

type gcpRESTErrorBody struct {
	Message string `json:"message"`
	Status  string `json:"status"`
	Code    int    `json:"code"`
}

type gcpRESTErrorEnvelope struct {
	Error gcpRESTErrorBody `json:"error"`
}

// registerConsolePageRoutes mounts the in-process proxies behind the Azure Functions and newer GCP console pages.
func (h *DashboardHandler) registerConsolePageRoutes() {
	h.registerGCPBigtableRoutes()
	h.registerGCPEventarcRoutes()
	h.registerGCPSpannerRoutes()
	h.registerGCPArtifactRegistryRoutes()
	h.registerGCPCloudBuildRoutes()
	h.registerGCPRedisRoutes()
	h.registerGCPMonitoringRoutes()
	h.registerAzureFunctionsRoutes()
	h.registerGCPWorkflowsRoutes()
	h.registerGCPDNSRoutes()
	h.registerGCPDatastoreRoutes()
	h.registerAzureFileRoutes()
	h.registerOCIObjectStorageRoutes()
	h.registerOCIVaultRoutes()
	h.registerOCIStreamingRoutes()
	h.registerOCIIdentityRoutes()
	h.registerOCIFunctionsRoutes()
	h.registerOCICoreRoutes()
	h.registerAzureMonitorRoutes()
	h.registerGCPComputeRoutes()
	h.registerAzureACIRoutes()
	h.registerAzureAppServiceRoutes()
	h.registerGCPContainerRoutes()
	h.registerOCIQueueRoutes()
	h.registerOCINotificationsRoutes()
	h.registerAzureContainerAppsRoutes()
	h.registerAzureVMRoutes()
	h.registerAzureAKSRoutes()
	h.registerOCIContainerEngineRoutes()
	h.registerOCIMySQLRoutes()
	h.registerOCILoggingRoutes()
	h.registerOCIMonitoringRoutes()
	h.registerOCINoSQLRoutes()
	h.registerOCIAPIGatewayRoutes()
	h.registerOCIEventsRoutes()
	h.registerOCILoadBalancerRoutes()
	h.registerOCIArtifactsRoutes()
	h.registerOCIContainerInstancesRoutes()
	h.registerGCPIdentityToolkitRoutes()
	h.registerAzureFlexDBRoutes()
	h.registerGCPAPIGatewayRoutes()
	h.registerGCPBatchRoutes()
	h.registerAzureAPIMRoutes()
}
