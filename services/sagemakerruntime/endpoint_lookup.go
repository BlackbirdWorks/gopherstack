package sagemakerruntime

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// endpointStatusInService is the real AWS SDK v2 sagemaker
// types.EndpointStatusInService enum literal. Duplicated here (rather than
// importing the sagemaker types package) because it is the only value this
// package needs to compare against.
const endpointStatusInService = "InService"

// EndpointLookup is the minimal read surface into services/sagemaker's
// endpoint registry needed to validate an EndpointName (and its InService
// status) before invoking it. *sagemaker.InMemoryBackend already satisfies
// this interface via its exported, lock-safe DescribeEndpoint method -- no
// change to services/sagemaker is required. See provider.go's
// wireEndpointLookup for how the two services get connected at server
// start-up, following the precedent established by
// services/cloudwatchlogs/provider.go's s3HandlerProvider/wireExportSink.
type EndpointLookup interface {
	DescribeEndpoint(ctx context.Context, name string) (*sagemaker.Endpoint, error)
}

// SetEndpointLookup wires the backend to the emulated SageMaker service's
// endpoint registry, enabling EndpointName validation on invoke calls. When
// left unset (the default for a bare NewInMemoryBackend, e.g. in unit tests
// that never go through Provider.Init), validateEndpoint is a no-op success
// -- this preserves this service's pre-existing behaviour of accepting any
// EndpointName, matching a standalone gopherstack instance where the
// SageMaker control-plane service isn't wired into the same process.
func (b *InMemoryBackend) SetEndpointLookup(lookup EndpointLookup) {
	b.mu.Lock("SetEndpointLookup")
	defer b.mu.Unlock()

	b.endpointLookup = lookup
}

// validateEndpoint checks endpointName against the wired SageMaker endpoint
// registry, if any. It returns ("", true) when there is nothing to check (no
// registry wired) or when endpointName resolves to an InService endpoint.
// Otherwise it returns the ValidationError message real AWS returns both for
// a truly unknown EndpointName and for one that has not yet reached
// InService (e.g. still Creating): AWS's runtime routing table only serves
// InService endpoints, so from an InvokeEndpoint caller's perspective the
// two cases are indistinguishable -- confirmed against the documented
// message "An error occurred (ValidationError) when calling the
// InvokeEndpoint operation: Endpoint <name> of account <account> not
// found.", which real-world reports also observe for endpoints still stuck
// in Creating.
func (b *InMemoryBackend) validateEndpoint(ctx context.Context, endpointName string) (string, bool) {
	b.mu.RLock("validateEndpoint")
	lookup := b.endpointLookup
	accountID := b.accountID
	b.mu.RUnlock()

	if lookup == nil {
		return "", true
	}

	ep, err := lookup.DescribeEndpoint(ctx, endpointName)
	if err != nil || ep == nil || ep.EndpointStatus != endpointStatusInService {
		return fmt.Sprintf("Endpoint %s of account %s not found.", endpointName, accountID), false
	}

	return "", true
}
