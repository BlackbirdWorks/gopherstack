package signin

import (
	"context"
	"regexp"
	"strings"
)

const (
	scopeAccount      = "ACCOUNT"
	scopeOrganization = "ORGANIZATION"
	scopeRoot         = "ROOT"
)

var targetPattern = regexp.MustCompile(`^(\d{12}|o-[a-z0-9]{10}|r-[0-9a-z]{4,32})$`)

// ConsoleAuthorization is the state returned by the console authorization operations.
type ConsoleAuthorization struct {
	Scope    string
	TargetID string
	Enabled  bool
}

// resolveTarget validates targetID, defaulting to the caller's account, and returns it with its scope.
func (b *InMemoryBackend) resolveTarget(ctx context.Context, targetID string) (string, string, error) {
	c := b.callerOf(ctx)
	if c.anon {
		return "", "", accessDenied(oauthInsufficientPerms, 403, "anonymous callers cannot manage console authorization")
	}

	if targetID == "" {
		targetID = c.account
	}

	if !targetPattern.MatchString(targetID) {
		return "", "", validation("targetId must be a 12-digit account ID, an organization ID or a root ID")
	}

	switch {
	case strings.HasPrefix(targetID, "o-"):
		return targetID, scopeOrganization, nil
	case strings.HasPrefix(targetID, "r-"):
		return targetID, scopeRoot, nil
	case targetID != c.account:
		return "", "", accessDenied(oauthInsufficientPerms, 403, "not authorized to configure account "+targetID)
	}

	return targetID, scopeAccount, nil
}

// PutConsoleAuthorizationConfiguration enables console authorization for the target.
func (b *InMemoryBackend) PutConsoleAuthorizationConfiguration(ctx context.Context, targetID string) (*ConsoleAuthorization, error) {
	target, scope, err := b.resolveTarget(ctx, targetID)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("PutConsoleAuthorizationConfiguration")
	defer b.mu.Unlock()

	b.consoleConfigs.Put(&ConsoleConfig{TargetID: target, Scope: scope, Enabled: true})

	return &ConsoleAuthorization{Scope: scope, TargetID: target, Enabled: true}, nil
}

// GetConsoleAuthorizationConfiguration returns the target's configuration.
func (b *InMemoryBackend) GetConsoleAuthorizationConfiguration(ctx context.Context, targetID string) (*ConsoleAuthorization, error) {
	target, _, err := b.resolveTarget(ctx, targetID)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetConsoleAuthorizationConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.consoleConfigs.Get(target)
	if !ok {
		return nil, notFound("no console authorization configuration for " + target)
	}

	return &ConsoleAuthorization{Scope: cfg.Scope, TargetID: cfg.TargetID, Enabled: cfg.Enabled}, nil
}

// DeleteConsoleAuthorizationConfiguration removes the target's configuration.
func (b *InMemoryBackend) DeleteConsoleAuthorizationConfiguration(ctx context.Context, targetID string) (*ConsoleAuthorization, error) {
	target, _, err := b.resolveTarget(ctx, targetID)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteConsoleAuthorizationConfiguration")
	defer b.mu.Unlock()

	cfg, ok := b.consoleConfigs.Get(target)
	if !ok {
		return nil, notFound("no console authorization configuration for " + target)
	}

	b.consoleConfigs.Delete(target)

	return &ConsoleAuthorization{Scope: cfg.Scope, TargetID: cfg.TargetID, Enabled: false}, nil
}
