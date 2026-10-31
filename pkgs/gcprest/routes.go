package gcprest

import (
	"errors"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

var (
	errUnsupportedKind = errors.New("unsupported field kind")
	errBadBase64       = errors.New("invalid base64")
)

// target is the gRPC method a route invokes in-process.
type target struct {
	in     protoreflect.MessageDescriptor
	out    protoreflect.MessageDescriptor
	full   string
	stream bool
}

type route struct {
	target   *target
	tmpl     *template
	method   string
	body     string
	respBody string
}

// addServices indexes the http annotations of every service registered on srv.
func (s *Server) addServices(srv *grpc.Server) {
	names := slices.Sorted(maps.Keys(srv.GetServiceInfo()))

	for _, name := range names {
		d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name))
		if err != nil {
			continue
		}

		if svc, ok := d.(protoreflect.ServiceDescriptor); ok {
			s.addService(svc)
		}
	}

	slices.SortStableFunc(s.routes, func(a, b *route) int { return rank(b) - rank(a) })
}

func (s *Server) addService(svc protoreflect.ServiceDescriptor) {
	for i := range svc.Methods().Len() {
		s.addMethod(svc, svc.Methods().Get(i))
	}
}

func (s *Server) addMethod(svc protoreflect.ServiceDescriptor, md protoreflect.MethodDescriptor) {
	rule := httpRule(md)
	if md.IsStreamingClient() || rule == nil {
		return
	}

	t := &target{
		in: md.Input(), out: md.Output(), stream: md.IsStreamingServer(),
		full: "/" + string(svc.FullName()) + "/" + string(md.Name()),
	}

	for _, r := range append([]*annotations.HttpRule{rule}, rule.GetAdditionalBindings()...) {
		if rt := newRoute(r, t); rt != nil {
			s.routes = append(s.routes, rt)
		}

		if svc.FullName() == lroService {
			s.addScopedOperations(r, t)
		}
	}
}

func (s *Server) addScopedOperations(r *annotations.HttpRule, t *target) {
	for _, sr := range scopedOperationRules(r) {
		if rt := newRoute(sr, t); rt != nil {
			s.routes = append(s.routes, rt)
		}
	}
}

const lroService = "google.longrunning.Operations"

// operationScopes are the parent resources whose operations Google APIs bind through their service config
// rather than their protos.
var operationScopes = []string{ //nolint:gochecknoglobals // immutable catalog
	"projects/*/locations/*",
	"projects/*/instances/*",
	"projects/*/instances/*/databases/*",
	"projects/*/instances/*/backups/*",
	"projects/*/instanceConfigs/*",
}

// scopedOperationRules rewrites a google.longrunning.Operations rule onto the resource-scoped operation names
// of operationScopes (any API version). A rule with no such form yields none.
func scopedOperationRules(r *annotations.HttpRule) []*annotations.HttpRule {
	method, path := ruleVerb(r)
	out := make([]*annotations.HttpRule, 0, len(operationScopes)+1)

	for _, scope := range append([]string{""}, operationScopes...) {
		scoped := strings.Replace(path, "/v1/{name=operations/**}", "/*/{name="+scope+"/operations/*}", 1)
		if scope == "" {
			if scoped = strings.Replace(path, "/v1/{name=operations/**}", "/*/{name=operations/**}", 1); scoped == path {
				continue
			}
		} else if scoped == path {
			scoped = strings.Replace(path, "/v1/{name=operations}", "/*/{name="+scope+"}/operations", 1)
		}

		if scoped == path {
			return nil
		}

		rule := &annotations.HttpRule{Body: r.GetBody(), ResponseBody: r.GetResponseBody()}

		switch method {
		case http.MethodGet:
			rule.Pattern = &annotations.HttpRule_Get{Get: scoped}
		case http.MethodDelete:
			rule.Pattern = &annotations.HttpRule_Delete{Delete: scoped}
		case http.MethodPost:
			rule.Pattern = &annotations.HttpRule_Post{Post: scoped}
		default:
			return nil
		}

		out = append(out, rule)
	}

	return out
}

// rank puts verb-suffixed templates first so ".../x:verb" is never swallowed by ".../{name}", then templates that
// end in a literal segment (".../tagKeys/namespaced") ahead of the variable ones that would also match them; the
// rest keep proto declaration order, which is how Google frontends resolve overlapping templates.
func rank(r *route) int {
	if r.tmpl.verb != "" {
		return 2 //nolint:mnd // verb templates sort first
	}

	last := len(r.tmpl.elems) - 1
	if last >= 0 && r.tmpl.elems[last].kind == elemLiteral &&
		!slices.ContainsFunc(r.tmpl.vars, func(v variable) bool { return v.start <= last && last < v.end }) {
		return 1
	}

	return 0
}

func httpRule(md protoreflect.MethodDescriptor) *annotations.HttpRule {
	opts, ok := md.Options().(*descriptorpb.MethodOptions)
	if !ok || opts == nil {
		return nil
	}

	rule, _ := proto.GetExtension(opts, annotations.E_Http).(*annotations.HttpRule)

	return rule
}

func newRoute(r *annotations.HttpRule, t *target) *route {
	method, path := ruleVerb(r)
	if method == "" {
		return nil
	}

	tmpl, err := parseTemplate(path)
	if err != nil {
		return nil
	}

	return &route{target: t, tmpl: tmpl, method: method, body: r.GetBody(), respBody: r.GetResponseBody()}
}

func ruleVerb(r *annotations.HttpRule) (string, string) {
	switch p := r.GetPattern().(type) {
	case *annotations.HttpRule_Get:
		return http.MethodGet, p.Get
	case *annotations.HttpRule_Put:
		return http.MethodPut, p.Put
	case *annotations.HttpRule_Post:
		return http.MethodPost, p.Post
	case *annotations.HttpRule_Delete:
		return http.MethodDelete, p.Delete
	case *annotations.HttpRule_Patch:
		return http.MethodPatch, p.Patch
	case *annotations.HttpRule_Custom:
		return strings.ToUpper(p.Custom.GetKind()), p.Custom.GetPath()
	default:
		return "", ""
	}
}

// match finds the first route accepting the request method and path.
func (s *Server) match(r *http.Request) (*route, []binding, bool) {
	raw := r.URL.EscapedPath()

	for _, rt := range s.routes {
		if rt.method != r.Method {
			continue
		}

		if segs, ok := splitPath(raw, rt.tmpl.verb); ok {
			if b, ok2 := rt.tmpl.match(segs); ok2 {
				return rt, b, true
			}
		}
	}

	return nil, nil, false
}

func splitPath(escaped, verb string) ([]string, bool) {
	if verb != "" {
		var ok bool
		if escaped, ok = strings.CutSuffix(escaped, ":"+verb); !ok {
			return nil, false
		}
	}

	parts := strings.Split(strings.TrimPrefix(escaped, "/"), "/")
	for i, p := range parts {
		u, err := url.PathUnescape(p)
		if err != nil {
			return nil, false
		}

		parts[i] = u
	}

	return parts, true
}
