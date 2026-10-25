// Package sdkwire calls AWS service APIs through the Smithy schema-driven
// protocol codecs, taking and returning JSON-shaped values, so callers need
// no generated SDK client.
//
// Clients whose pinned version ships a schemas package use it directly; the
// rest use models extracted from their generated clients (legacy/). After an
// SDK bump run `go run ./cmd/sfnschemagen` and then `go run ./cmd/ccwiregen`
// (the Cloud Control wire structs in internal/app) from the repo root; parity
// tests in this package check every operation against the real clients.
package sdkwire

import (
	"sync"

	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/aws/smithy-go"
)

// Op is one service operation.
type Op struct {
	Schema *smithy.OperationSchema
	// Token names the idempotency-token member the SDK fills when absent.
	Token string
	// Sanitize names input members the SDK cuts to their last "/" segment.
	Sanitize []string
}

// Service describes one AWS service's wire protocol and operations.
type Service struct {
	Protocol smithyhttp.ClientProtocol
	Ops      map[string]*Op
	// Headers are sent on every request, as the SDK's service customizations do.
	Headers     map[string]string
	SigningName string
	// UserAgentAPI is the "api/<service>#<version>" SDK marker routers key on.
	UserAgentAPI string
}

var registry = map[string]func() *Service{} //nolint:gochecknoglobals // filled by the generated init

func register(key string, build func() *Service) {
	registry[key] = sync.OnceValue(build)
}

// Lookup returns the service registered under key.
func Lookup(key string) (*Service, bool) {
	build, ok := registry[key]
	if !ok {
		return nil, false
	}

	return build(), true
}

// Keys lists the registered service keys.
func Keys() []string {
	keys := make([]string, 0, len(registry))
	for k := range registry {
		keys = append(keys, k)
	}

	return keys
}
