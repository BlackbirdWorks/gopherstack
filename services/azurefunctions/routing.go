package azurefunctions

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"math"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	routingName   = "x-ms-routing-name"
	routingSelf   = "self"
	routingMaxAge = 3600
	percentScale  = 100
)

// RoutingRule sends a share of a production site's traffic to one of its slots.
type RoutingRule struct {
	Slot    string
	Percent float64
}

// SetSlotHooks wires what the data plane asks ARM about slots: the traffic routing rules of a production site
// and what to do after a deployment (auto swap).
func (b *InMemoryBackend) SetSlotHooks(
	rules func(app string) []RoutingRule, onDeploy func(ctx context.Context, name string),
) {
	b.mu.Lock("SetSlotHooks")
	defer b.mu.Unlock()

	b.rules, b.onDeploy = rules, onDeploy
}

func (b *InMemoryBackend) slotExists(app, slot string) bool {
	b.mu.RLock("slotExists")
	defer b.mu.RUnlock()

	_, ok := b.apps[strs.Fold(SlotName(app, slot))]

	return ok
}

func randomPercent() float64 {
	var buf [4]byte

	_, _ = rand.Read(buf[:])

	return float64(binary.BigEndian.Uint32(buf[:])) / float64(math.MaxUint32) * percentScale
}

func pickRule(rules []RoutingRule) string {
	pick, sum := randomPercent(), 0.0

	for _, r := range rules {
		if sum += r.Percent; pick < sum {
			return r.Slot
		}
	}

	return routingSelf
}

// routeSlot picks the app (production or one of its slots) that serves a request to production's hostname: the
// x-ms-routing-name query parameter or cookie, else a random pick by the traffic rules, which is then pinned for
// an hour with a cookie (Learn: Set up staging environments, "Route production traffic").
func (b *InMemoryBackend) routeSlot(w http.ResponseWriter, r *http.Request, name string) string {
	if strings.Contains(name, SlotSep) {
		return name
	}

	b.mu.RLock("routeSlot")
	rulesFn := b.rules
	b.mu.RUnlock()

	var rules []RoutingRule
	if rulesFn != nil {
		rules = rulesFn(name)
	}

	choice, pin := r.URL.Query().Get(routingName), false
	if choice != "" {
		pin = true
	} else if c, err := r.Cookie(routingName); err == nil {
		choice = c.Value
	} else if len(rules) > 0 {
		choice, pin = pickRule(rules), true
	}

	target := name

	if choice != "" && !strings.EqualFold(choice, routingSelf) {
		if !b.slotExists(name, choice) {
			return name
		}

		target = SlotName(name, choice)
	} else if choice == "" {
		return name
	}

	if pin {
		//nolint:gosec // plain HTTP listener: Secure follows the request scheme
		http.SetCookie(w, &http.Cookie{
			Name: routingName, Value: choice, Path: "/", MaxAge: routingMaxAge, HttpOnly: true,
			SameSite: http.SameSiteLaxMode, Secure: r.TLS != nil,
		})
	}

	return target
}

// afterDeploy tells ARM a deployment finished so an auto swap slot can swap into its target.
func (b *InMemoryBackend) afterDeploy(name string) {
	b.mu.RLock("afterDeploy")
	fn, ctx := b.onDeploy, b.runCtx
	b.mu.RUnlock()

	if fn != nil && strings.Contains(name, SlotSep) {
		b.wg.Go(func() { fn(ctx, name) })
	}
}
