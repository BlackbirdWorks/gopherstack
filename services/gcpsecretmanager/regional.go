package gcpsecretmanager

import (
	"context"
	"net"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	regionalHostPrefix = "secretmanager."
	regionalHostSuffix = ".rep.googleapis.com"
)

// endpointLocation extracts <loc> from a secretmanager.<loc>.rep.googleapis.com authority.
func endpointLocation(authority string) string {
	host := authority
	if h, _, err := net.SplitHostPort(authority); err == nil {
		host = h
	}

	loc, ok := strings.CutPrefix(host, regionalHostPrefix)
	if !ok {
		return ""
	}

	loc, ok = strings.CutSuffix(loc, regionalHostSuffix)
	if !ok || loc == "" || strings.Contains(loc, ".") {
		return ""
	}

	return loc
}

// resourceLocation returns the location of the request's resource, or "" for a global one.
func resourceLocation(req any) string {
	msg, ok := req.(proto.Message)
	if !ok {
		return ""
	}

	m := msg.ProtoReflect()

	for _, field := range []string{fieldName, "parent", "resource"} {
		fd := m.Descriptor().Fields().ByName(protoreflect.Name(field))
		if fd == nil {
			continue
		}

		parts := strings.Split(m.Get(fd).String(), "/")
		for i := 0; i+1 < len(parts); i++ {
			if parts[i] == segLocations {
				return parts[i+1]
			}
		}

		return ""
	}

	if fd := m.Descriptor().Fields().ByName("secret"); fd != nil && m.Has(fd) {
		return resourceLocation(m.Get(fd).Message().Interface())
	}

	return ""
}

// regionalInterceptor rejects requests sent to a regional endpoint for a resource in another location.
func regionalInterceptor(
	ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler,
) (any, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for _, a := range md.Get(":authority") {
			if endpoint := endpointLocation(a); endpoint != "" && resourceLocation(req) != endpoint {
				return nil, errInvalid("The regional endpoint %s only serves resources in location %q.", a, endpoint)
			}
		}
	}

	return next(ctx, req)
}
