package gcpaudit

import (
	"context"
	"strings"
	"time"

	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Policy says which RPCs of one gRPC API are audited.
type Policy struct {
	// NameFunc overrides ResourceName for methods whose resource is not in a top-level name field.
	NameFunc func(req, resp proto.Message) string
	// Data names methods (short, e.g. "Commit") that are Data Access records.
	Data map[string]Kind
	// ResourceType is the monitored resource type recorded; empty uses audited_resource.
	ResourceType string
	// ServiceName is the audit serviceName, e.g. secretmanager.googleapis.com.
	ServiceName string
	// AdminPrefixes are method-name prefixes (Create, Delete, ...) of mutating RPCs.
	AdminPrefixes []string
}

func (p Policy) classify(short string) (Kind, bool) {
	if k, ok := p.Data[short]; ok {
		return k, true
	}

	for _, pre := range p.AdminPrefixes {
		if strings.HasPrefix(short, pre) {
			return Activity, true
		}
	}

	return 0, false
}

// MethodName turns a gRPC full method (/pkg.Service/Method) into the audit methodName (pkg.Service.Method).
func MethodName(fullMethod string) string {
	return strings.ReplaceAll(strings.TrimPrefix(fullMethod, "/"), "/", ".")
}

// UnaryInterceptor records the RPCs selected by p after they run, with their status. Bodies are left out
// so that secret payloads never reach the log.
func (e *Emitter) UnaryInterceptor(p Policy, now func() time.Time) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		resp, err := next(ctx, req)

		kind, ok := p.classify(info.FullMethod[strings.LastIndex(info.FullMethod, "/")+1:])
		if !ok || !e.Enabled() {
			return resp, err
		}

		rm, _ := req.(proto.Message)
		pm, _ := resp.(proto.Message)
		name := ""
		if p.NameFunc != nil {
			name = p.NameFunc(rm, pm)
		}

		if name == "" {
			name = ResourceName(rm, pm)
		}

		rec := Record{
			Project: ProjectOf(name), ServiceName: p.ServiceName, MethodName: MethodName(info.FullMethod),
			ResourceName: name, Kind: kind, Time: now(),
		}

		if p.ResourceType != "" {
			rec.Resource = &monitoredres.MonitoredResource{
				Type: p.ResourceType, Labels: map[string]string{"project_id": rec.Project},
			}
		}

		if err != nil {
			s := grpcstatus.Convert(err)
			rec.Failure = s.Proto()
		}

		e.Emit(ctx, rec)

		return resp, err
	}
}

// ResourceName picks the audited resource: the response's name, else the request's name, database or parent.
func ResourceName(req, resp proto.Message) string {
	if n := stringField(resp, "name"); n != "" && !strings.HasPrefix(n, "operations/") &&
		!strings.Contains(n, "/operations/") {
		return n
	}

	for _, f := range []protoreflect.Name{"name", "database", "parent"} {
		if n := stringField(req, f); n != "" {
			return n
		}
	}

	return ""
}

func stringField(m proto.Message, name protoreflect.Name) string {
	if m == nil {
		return ""
	}

	r := m.ProtoReflect()
	if !r.IsValid() {
		return ""
	}

	fd := r.Descriptor().Fields().ByName(name)
	if fd == nil || fd.Kind() != protoreflect.StringKind || fd.IsList() {
		return ""
	}

	return r.Get(fd).String()
}

// ProjectOf returns the project of a projects/{p}/... resource name, or "".
func ProjectOf(name string) string {
	rest, ok := strings.CutPrefix(name, "projects/")
	if !ok {
		return ""
	}

	p, _, _ := strings.Cut(rest, "/")

	return p
}
