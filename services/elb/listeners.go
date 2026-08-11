package elb

import (
	"context"
	"fmt"
)

// validateListenerCertificates checks every HTTPS/SSL listener's
// SSLCertificateId against the wired CertificateResolver. Callers must hold
// b.mu. A nil certResolver (the default) accepts every certificate id
// unvalidated.
func (b *InMemoryBackend) validateListenerCertificates(ctx context.Context, listeners []Listener) error {
	if b.certResolver == nil {
		return nil
	}

	for _, l := range listeners {
		if l.SSLCertificateID == "" {
			continue
		}

		if !b.certResolver.ResolveCertificate(ctx, l.SSLCertificateID) {
			return fmt.Errorf("%w: %s", ErrCertificateNotFound, l.SSLCertificateID)
		}
	}

	return nil
}

// CreateLoadBalancerListeners adds listeners to an existing load balancer.
// Idempotent: if a listener on the same port already exists with identical settings,
// it is a no-op. Returns DuplicateListener if the port is in use with different settings.
func (b *InMemoryBackend) CreateLoadBalancerListeners(ctx context.Context, name string, listeners []Listener) error {
	b.mu.Lock("CreateLoadBalancerListeners")
	defer b.mu.Unlock()

	lb, ok := b.lbs.Get(lbTableKey(getRegion(ctx, b.region), name))
	if !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	if certErr := b.validateListenerCertificates(ctx, listeners); certErr != nil {
		return certErr
	}

	// Real AWS's CreateLoadBalancerListeners typed-error switch recognizes
	// InvalidConfigurationRequest (not ValidationError) for this op (see
	// deserializers.go's awsAwsquery_deserializeOpErrorCreateLoadBalancerListeners).
	const maxListeners = 100
	if len(lb.Listeners)+len(listeners) > maxListeners {
		return fmt.Errorf("%w: classic-listeners limit of %d exceeded", ErrInvalidConfiguration, maxListeners)
	}

	existing := make(map[int32]*Listener, len(lb.Listeners))
	for i := range lb.Listeners {
		existing[lb.Listeners[i].LoadBalancerPort] = &lb.Listeners[i]
	}

	// Validate all incoming listeners: port conflict with different config = DuplicateListener.
	seen := make(map[int32]bool, len(listeners))
	for _, l := range listeners {
		ex, portTaken := existing[l.LoadBalancerPort]
		if portTaken {
			if ex.Protocol != l.Protocol || ex.InstancePort != l.InstancePort ||
				ex.InstanceProtocol != l.InstanceProtocol {
				return fmt.Errorf(
					"%w: conflicting listener on port %d",
					ErrDuplicateListener,
					l.LoadBalancerPort,
				)
			}
			// Exact match: idempotent no-op.
			continue
		}

		if seen[l.LoadBalancerPort] {
			return fmt.Errorf("%w: duplicate port %d in request", ErrDuplicateListener, l.LoadBalancerPort)
		}

		seen[l.LoadBalancerPort] = true
	}

	for _, l := range listeners {
		if _, alreadyExists := existing[l.LoadBalancerPort]; !alreadyExists {
			lb.Listeners = append(lb.Listeners, l)
		}
	}

	return nil
}

// DeleteLoadBalancerListeners removes listeners by port from an existing load balancer.
func (b *InMemoryBackend) DeleteLoadBalancerListeners(ctx context.Context, name string, ports []int32) error {
	b.mu.Lock("DeleteLoadBalancerListeners")
	defer b.mu.Unlock()

	lb, ok := b.lbs.Get(lbTableKey(getRegion(ctx, b.region), name))
	if !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	remove := make(map[int32]bool, len(ports))
	for _, p := range ports {
		remove[p] = true
	}

	kept := lb.Listeners[:0]
	for _, l := range lb.Listeners {
		if !remove[l.LoadBalancerPort] {
			kept = append(kept, l)
		}
	}

	lb.Listeners = kept

	return nil
}

// SetLoadBalancerListenerSSLCertificate sets the SSL certificate for an existing listener.
func (b *InMemoryBackend) SetLoadBalancerListenerSSLCertificate(
	ctx context.Context, name string, port int32, certID string,
) error {
	b.mu.Lock("SetLoadBalancerListenerSSLCertificate")
	defer b.mu.Unlock()

	lb, ok := b.lbs.Get(lbTableKey(getRegion(ctx, b.region), name))
	if !ok {
		return fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	if b.certResolver != nil && !b.certResolver.ResolveCertificate(ctx, certID) {
		return fmt.Errorf("%w: %s", ErrCertificateNotFound, certID)
	}

	for i := range lb.Listeners {
		if lb.Listeners[i].LoadBalancerPort == port {
			proto := lb.Listeners[i].Protocol
			if proto != protoHTTPS && proto != protoSSL {
				return fmt.Errorf(
					"%w: SSL certificate can only be set on HTTPS or SSL listeners (port %d has protocol %s)",
					ErrInvalidConfiguration, port, proto,
				)
			}

			lb.Listeners[i].SSLCertificateID = certID

			return nil
		}
	}

	return fmt.Errorf("%w: no listener on port %d", ErrListenerNotFound, port)
}

// listenerProtocolForPort returns the protocol of the listener on the given port,
// or an empty string if no matching listener exists.
func listenerProtocolForPort(lb *LoadBalancer, port int32) string {
	for i := range lb.Listeners {
		if lb.Listeners[i].LoadBalancerPort == port {
			return lb.Listeners[i].Protocol
		}
	}

	return ""
}
