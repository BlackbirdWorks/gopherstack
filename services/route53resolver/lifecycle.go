package route53resolver

import (
	"fmt"
	"net/netip"
	"time"
)

const (
	statusCreating = "CREATING"
)

func (b *InMemoryBackend) now() time.Time {
	if b.clock != nil {
		return b.clock()
	}

	return time.Now()
}

// SetLifecycleDelay sets how long resolver endpoints and rule associations stay
// CREATING/UPDATING before settling. Zero (the default) settles instantly.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

// SetClock overrides the backend clock for deterministic lifecycle tests; nil restores time.Now.
func (b *InMemoryBackend) SetClock(clock func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.clock = clock
}

func (b *InMemoryBackend) transitionDeadline() time.Time {
	if b.lifecycleDelay <= 0 {
		return time.Time{}
	}

	return b.now().Add(b.lifecycleDelay)
}

func (b *InMemoryBackend) beginEndpointTransition(ep *ResolverEndpoint, status string) {
	ep.transitionStatus = status
	ep.transitionUntil = b.transitionDeadline()
}

// endpointView copies ep with the transitional status applied while its deadline is in the future.
func (b *InMemoryBackend) endpointView(ep *ResolverEndpoint) *ResolverEndpoint {
	cp := cloneEndpoint(ep)

	if !ep.transitionUntil.IsZero() && b.now().Before(ep.transitionUntil) {
		cp.Status = ep.transitionStatus
	}

	return cp
}

func (b *InMemoryBackend) assocView(a *ResolverRuleAssociation) *ResolverRuleAssociation {
	cp := *a

	if !a.creatingUntil.IsZero() && b.now().Before(a.creatingUntil) {
		cp.Status = statusCreating
	}

	return &cp
}

func validateIPLiteral(field, value string, v6 bool) error {
	if value == "" {
		return nil
	}

	addr, err := netip.ParseAddr(value)
	if err != nil || addr.Is6() != v6 || addr.Is4In6() {
		return fmt.Errorf("%w: %s %q is not a valid IP address", ErrInvalidParameter, field, value)
	}

	return nil
}

func validateEndpointIPs(ips []IPAddress) error {
	for _, ip := range ips {
		if err := validateIPLiteral("Ip", ip.IP, false); err != nil {
			return err
		}

		if err := validateIPLiteral("Ipv6", ip.Ipv6, true); err != nil {
			return err
		}
	}

	return nil
}

func validateTargetIPs(targets []TargetIP) error {
	for _, t := range targets {
		if err := validateIPLiteral("Ip", t.IP, false); err != nil {
			return err
		}

		if err := validateIPLiteral("Ipv6", t.Ipv6, true); err != nil {
			return err
		}
	}

	return nil
}
