package mwaa

import "context"

// CreateWebLoginToken validates that the environment exists and is AVAILABLE,
// then returns a JWT-shaped web login token and the environment's webserver hostname.
// AWS returns ResourceNotFoundException when the environment is in any non-AVAILABLE state.
func (b *InMemoryBackend) CreateWebLoginToken(ctx context.Context, envName string) (string, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("CreateWebLoginToken")
	defer b.mu.RUnlock()

	env, ok := b.environments.Get(regionKey(region, envName))
	if !ok {
		return "", "", ErrEnvironmentNotFound
	}

	if env.Status != envStatusAvailable {
		return "", "", ErrEnvironmentNotFound
	}

	return generateMWAAToken(envName, "web"), webserverHostname(env.WebserverURL), nil
}
