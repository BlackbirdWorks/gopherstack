package globalaccelerator

import (
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// byoipSettled maps each transitional state to the state it reaches once the dwell elapses.
var byoipSettled = map[string]string{ //nolint:gochecknoglobals // immutable state-machine table
	byoipPendingProvisioning:   byoipReady,
	byoipPendingAdvertising:    byoipAdvertising,
	byoipPendingWithdrawing:    byoipReady,
	byoipPendingDeprovisioning: byoipDeprovisioned,
}

func byoipEffectiveState(c *ByoipCidr, now time.Time) string {
	if next, ok := byoipSettled[c.State]; ok && !now.Before(c.PendingUntil) {
		return next
	}

	return c.State
}

func (b *InMemoryBackend) viewByoipLocked(c *ByoipCidr) *ByoipCidr {
	cp := cloneByoip(c)
	cp.State = byoipEffectiveState(c, b.now())

	return cp
}

func (b *InMemoryBackend) byoipLocked(cidr string) (*ByoipCidr, error) {
	c, ok := b.byoip.Get(cidr)
	if !ok {
		return nil, fail(ErrByoipCidrNotFound, "BYOIP range %s not found", cidr)
	}

	c.State = byoipEffectiveState(c, b.now())

	return c, nil
}

func (b *InMemoryBackend) enterByoipLocked(c *ByoipCidr, state, msg string) {
	now := b.now()
	c.State, c.PendingUntil = state, b.pendingUntil(now)
	c.Events = append(c.Events, Event{Message: msg, Timestamp: now})
}

// ProvisionByoipCidr starts provisioning an address range.
func (b *InMemoryBackend) ProvisionByoipCidr(cidr, message, signature string) (*ByoipCidr, error) {
	if err := validateCIDR(cidr); err != nil {
		return nil, err
	}

	if message == "" || signature == "" {
		return nil, fail(ErrInvalidArgument, "cidrAuthorizationContext needs a message and a signature")
	}

	b.mu.Lock("ProvisionByoipCidr")
	defer b.mu.Unlock()

	c, ok := b.byoip.Get(cidr)
	if ok {
		c.State = byoipEffectiveState(c, b.now())

		if c.State != byoipDeprovisioned {
			return nil, fail(ErrConflict, "BYOIP range %s is already provisioned", cidr)
		}
	} else {
		c = &ByoipCidr{Cidr: cidr}
		b.byoip.Put(c)
	}

	b.enterByoipLocked(c, byoipPendingProvisioning, "Provisioning started")

	return b.viewByoipLocked(c), nil
}

func (b *InMemoryBackend) transitionByoip(cidr, from, to, msg string) (*ByoipCidr, error) {
	c, err := b.byoipLocked(cidr)
	if err != nil {
		return nil, err
	}

	if c.State != from {
		return nil, fail(ErrIncorrectCidrState, "BYOIP range %s is %s, expected %s", cidr, c.State, from)
	}

	b.enterByoipLocked(c, to, msg)

	return b.viewByoipLocked(c), nil
}

// AdvertiseByoipCidr starts advertising a READY range.
func (b *InMemoryBackend) AdvertiseByoipCidr(cidr string) (*ByoipCidr, error) {
	b.mu.Lock("AdvertiseByoipCidr")
	defer b.mu.Unlock()

	return b.transitionByoip(cidr, byoipReady, byoipPendingAdvertising, "Advertising started")
}

// WithdrawByoipCidr stops advertising an ADVERTISING range.
func (b *InMemoryBackend) WithdrawByoipCidr(cidr string) (*ByoipCidr, error) {
	b.mu.Lock("WithdrawByoipCidr")
	defer b.mu.Unlock()

	return b.transitionByoip(cidr, byoipAdvertising, byoipPendingWithdrawing, "Withdrawal started")
}

// DeprovisionByoipCidr releases a READY range that no accelerator uses.
func (b *InMemoryBackend) DeprovisionByoipCidr(cidr string) (*ByoipCidr, error) {
	b.mu.Lock("DeprovisionByoipCidr")
	defer b.mu.Unlock()

	if c, err := b.byoipLocked(cidr); err == nil && c.State == byoipReady && b.cidrInUseLocked(c.Cidr) {
		return nil, fail(ErrIncorrectCidrState, "BYOIP range %s has addresses assigned to accelerators", cidr)
	}

	return b.transitionByoip(cidr, byoipReady, byoipPendingDeprovisioning, "Deprovisioning started")
}

func (b *InMemoryBackend) cidrInUseLocked(cidr string) bool {
	inUse := false

	b.accelerators.Range(func(a *Accelerator) bool {
		inUse = slices.ContainsFunc(a.IPv4, func(ip string) bool { return addrInCIDR(ip, cidr) })

		return !inUse
	})

	return inUse
}

// ListByoipCidrs pages the provisioned ranges.
func (b *InMemoryBackend) ListByoipCidrs(token string, maxResults *int32) (page.Page[*ByoipCidr], error) {
	b.mu.RLock("ListByoipCidrs")
	defer b.mu.RUnlock()

	all := b.byoip.All()
	slices.SortFunc(all, func(x, y *ByoipCidr) int { return compareStrings(x.Cidr, y.Cidr) })

	pg, err := paginate(all, token, maxResults)
	if err != nil {
		return pg, err
	}

	for i, c := range pg.Data {
		pg.Data[i] = b.viewByoipLocked(c)
	}

	return pg, nil
}
