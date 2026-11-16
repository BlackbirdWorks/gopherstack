package azurearm

import (
	"context"
	"fmt"
	"strings"
)

const (
	webFieldAutoSwap    = "autoSwapSlotName"
	webFieldExperiments = "experiments"
	webFieldRampUpRules = "rampUpRules"
)

// RampUpRule routes a share of a site's production traffic to one of its slots (siteConfig.experiments.rampUpRules).
type RampUpRule struct {
	Name    string
	Percent float64
}

// checkAutoSwap rejects an autoSwapSlotName that Azure does not accept: only a slot of a function app can swap
// automatically (Learn: Set up staging environments, "Configure auto swap": not supported for Linux web apps),
// into production or another existing slot.
func (p *WebProvider) checkAutoSwap(s *storedWebSite, cfg map[string]any) error {
	target, _ := cfg[webFieldAutoSwap].(string)
	if target == "" {
		return nil
	}

	bad := func(format string, args ...any) error {
		return badWebRequest(fmt.Errorf("%w: %s", ErrWebInvalidSiteConfig, fmt.Sprintf(format, args...)))
	}

	switch {
	case s.Slot == "":
		return bad("autoSwapSlotName applies to a deployment slot, not the production site")
	case !isFunctionAppKind(s.Kind):
		return bad("auto swap is not supported in web apps on Linux or Web App for Containers")
	case strings.EqualFold(target, s.Slot):
		return bad("autoSwapSlotName cannot be the slot itself")
	}

	if parent, ok := p.sites[webSiteKey(s.Name)]; ok {
		if _, found := slotRecord(parent, target); !found {
			return bad("autoSwapSlotName %q is not a slot of this app", target)
		}
	}

	return nil
}

// AutoSwap swaps the slot dpName into its autoSwapSlotName after a deployment, if one is configured.
func (r *Registry) AutoSwap(ctx context.Context, dpName string) error {
	p, ok := r.ProviderNamed(namespaceMicrosoftWeb)
	if !ok {
		return nil
	}

	wp, ok := p.(*WebProvider)
	if !ok {
		return nil
	}

	return wp.autoSwap(ctx, dpName)
}

func (p *WebProvider) autoSwap(ctx context.Context, dpName string) error {
	p.mu.Lock("autoSwap")
	defer p.mu.Unlock()

	for _, parent := range p.sites {
		for _, slot := range parent.Slots {
			if !strings.EqualFold(slot.dpName(), dpName) {
				continue
			}

			targetName, _ := slot.Configs[webConfigWeb][webFieldAutoSwap].(string)
			if targetName == "" || slot.Pending != nil {
				return nil
			}

			target, found := slotRecord(parent, targetName)
			if !found || target == slot {
				return nil
			}

			sd := newSlotSides(parent, slot, target)
			if err := p.prepareSwap(ctx, sd); err != nil {
				return err
			}

			return p.completeSwap(sd)
		}
	}

	return nil
}

// SiteRampUpRules returns the traffic routing rules of the production site dpName.
func (r *Registry) SiteRampUpRules(dpName string) []RampUpRule {
	p, ok := r.ProviderNamed(namespaceMicrosoftWeb)
	if !ok {
		return nil
	}

	wp, ok := p.(*WebProvider)
	if !ok {
		return nil
	}

	wp.idCache.mu.RLock("SiteRampUpRules")
	defer wp.idCache.mu.RUnlock()

	return wp.idCache.rules[strings.ToLower(dpName)]
}

func rampUpRulesOf(s *storedWebSite) []RampUpRule {
	rules, _ := mapOf(s.Configs[webConfigWeb][webFieldExperiments])[webFieldRampUpRules].([]any)
	out := make([]RampUpRule, 0, len(rules))

	for _, raw := range rules {
		m := mapOf(raw)
		name, _ := m["name"].(string)

		if pct, ok := m["reroutePercentage"].(float64); ok && name != "" {
			out = append(out, RampUpRule{Name: name, Percent: pct})
		}
	}

	return out
}
