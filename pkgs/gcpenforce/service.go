package gcpenforce

import (
	"strings"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	// metaHost is the default_host of google.iam.v1.IAMPolicy, which every API embeds rather than owns.
	metaHost            = "iam-meta-api.googleapis.com"
	hostResourceManager = "cloudresourcemanager.googleapis.com"
	hostServiceUsage    = "serviceusage.googleapis.com"
)

// exemptPrefixes are proto packages never enforced: operations, the control plane itself, and gRPC internals.
//
//nolint:gochecknoglobals // immutable lookup table
var exemptPrefixes = []string{
	"google.longrunning.", "google.cloud.resourcemanager.", "google.api.serviceusage.", "grpc.",
}

// splitMethod splits "/pkg.Service/Method" into the service full name and the method.
func splitMethod(full string) (string, string) {
	svc, method, _ := strings.Cut(strings.TrimPrefix(full, "/"), "/")

	return svc, method
}

// Exempt reports whether a gRPC method is outside enforcement.
func Exempt(fullMethod string) bool {
	svc, _ := splitMethod(fullMethod)

	for _, p := range exemptPrefixes {
		if strings.HasPrefix(svc, p) {
			return true
		}
	}

	return false
}

// ServiceForMethod resolves "/google.pubsub.v1.Publisher/Publish" to "pubsub.googleapis.com" from the service's
// google.api.default_host option, falling back to fallback for services without one (google.iam.v1.IAMPolicy).
func ServiceForMethod(fullMethod, fallback string) string {
	svc, _ := splitMethod(fullMethod)

	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(svc))
	if err == nil {
		if sd, ok := d.(protoreflect.ServiceDescriptor); ok {
			if opts, ok := sd.Options().(*descriptorpb.ServiceOptions); ok && opts != nil {
				if host, _ := proto.GetExtension(opts, annotations.E_DefaultHost).(string); host != "" && host != metaHost {
					return trimPort(host)
				}
			}
		}
	}

	return fallback
}

func trimPort(host string) string {
	h, _, _ := strings.Cut(host, ":")

	return h
}

// exemptHost reports whether an API host is the control plane.
func exemptHost(host string) bool { return host == hostResourceManager || host == hostServiceUsage }

// OperationsPath reports whether an HTTP path addresses long-running operations.
func OperationsPath(path string) bool {
	return strings.HasSuffix(path, "/operations") || strings.Contains(path, "/operations/")
}
