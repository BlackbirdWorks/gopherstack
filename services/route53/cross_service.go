package route53

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// recoveryControlSiblings is matched structurally against *CLI, which cannot be imported here.
type recoveryControlSiblings interface {
	GetRoute53RecoveryControlConfigHandler() service.Registerable
}

type routingControlStates interface {
	RoutingControlIsOn(arn string) (bool, bool)
}

// SetAppConfig records service.AppContext.Config so the recovery-control sibling is resolved at call time.
func (b *InMemoryBackend) SetAppConfig(cfg any) { b.appConfig = cfg }

// routingControlState returns (on, found) for the routing control, resolving the sibling at call time.
func (b *InMemoryBackend) routingControlState(arn string) (bool, bool) {
	s, ok := b.appConfig.(recoveryControlSiblings)
	if !ok {
		return false, false
	}

	st, ok := s.GetRoute53RecoveryControlConfigHandler().(routingControlStates)
	if !ok {
		return false, false
	}

	return st.RoutingControlIsOn(arn)
}

// effectiveStatus is the stored status, except for RECOVERY_CONTROL checks whose status follows the routing control.
func (b *InMemoryBackend) effectiveStatus(hc *HealthCheck) string {
	if hc.Config.Type != HealthCheckTypeRecoveryControl || hc.Config.RoutingControlArn == "" {
		return hc.Status
	}

	on, found := b.routingControlState(hc.Config.RoutingControlArn)
	if !found {
		return hc.Status
	}

	if on {
		return defaultHealthStatus
	}

	return healthUnhealthy
}

// RecoveryControlHealthCheckIDs returns the IDs of RECOVERY_CONTROL health checks bound to the routing control.
func (b *InMemoryBackend) RecoveryControlHealthCheckIDs(routingControlArn string) []string {
	b.mu.RLock("RecoveryControlHealthCheckIDs")
	defer b.mu.RUnlock()

	var ids []string

	for _, hc := range b.healthChecks.All() {
		if hc.Config.Type == HealthCheckTypeRecoveryControl &&
			strings.EqualFold(hc.Config.RoutingControlArn, routingControlArn) {
			ids = append(ids, hc.ID)
		}
	}

	return ids
}

// RecoveryControlHealthCheckIDs returns the IDs of RECOVERY_CONTROL health checks bound to the routing control.
func (h *Handler) RecoveryControlHealthCheckIDs(routingControlArn string) []string {
	if bk, ok := h.Backend.(interface{ RecoveryControlHealthCheckIDs(string) []string }); ok {
		return bk.RecoveryControlHealthCheckIDs(routingControlArn)
	}

	return nil
}
