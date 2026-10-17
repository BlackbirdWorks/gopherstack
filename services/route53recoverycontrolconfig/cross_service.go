package route53recoverycontrolconfig

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// siblingServices is matched structurally against *CLI, which cannot be imported here.
type siblingServices interface {
	GetRoute53Handler() service.Registerable
}

type healthCheckLister interface {
	RecoveryControlHealthCheckIDs(routingControlArn string) []string
}

// SetAppConfig records service.AppContext.Config so siblings are resolved at call time, after every provider exists.
func (b *InMemoryBackend) SetAppConfig(cfg any) { b.appConfig = cfg }

// AssociatedHealthCheckIDs lists the Route 53 RECOVERY_CONTROL health checks bound to a routing control.
func (b *InMemoryBackend) AssociatedHealthCheckIDs(routingControlArn string) ([]string, error) {
	b.mu.RLock("AssociatedHealthCheckIDs")
	_, err := b.getControl(routingControlArn)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	s, ok := b.appConfig.(siblingServices)
	if !ok {
		return nil, nil
	}

	l, ok := s.GetRoute53Handler().(healthCheckLister)
	if !ok {
		return nil, nil
	}

	return l.RecoveryControlHealthCheckIDs(routingControlArn), nil
}
