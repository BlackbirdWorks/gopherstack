package resourceexplorer2

import (
	"context"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// OrgDirectory is the organization membership view the Organizations backend provides. A backend wired with an
// OrgAccess that also implements it validates organization scopes and member listings against it.
type OrgDirectory interface {
	// ManagementAccount returns the management account; ok is false when no organization exists.
	ManagementAccount() (account string, ok bool)
	IsMember(account string) bool
	IsDelegatedAdmin(principal, account string) bool
	// ScopeExists reports whether an organization or organizational-unit ARN names a live entity.
	ScopeExists(scopeARN string) bool
}

func accessDenied(format string, args ...any) error {
	return restroute.NewError(http.StatusForbidden, "AccessDeniedException", format, args...)
}

func (b *InMemoryBackend) orgDirectory() OrgDirectory {
	b.mu.RLock("orgDirectory")
	org := b.org
	b.mu.RUnlock()

	d, _ := org.(OrgDirectory)

	return d
}

func isAccountRootScope(scope, account string) bool {
	return scope == arn.BuildGlobal("iam", "", account, "root")
}

// validateScope checks organization and OU scopes against the organization; account roots need no lookup.
func (b *InMemoryBackend) validateScope(scope, account string) error {
	if isAccountRootScope(scope, account) {
		return nil
	}

	dir := b.orgDirectory()
	if dir == nil {
		return nil
	}

	if !strings.HasPrefix(scope, "arn:") || !dir.ScopeExists(scope) {
		return invalid(
			"Scope %q is not the root of this account or an organization or organizational unit that exists",
			scope,
		)
	}

	return nil
}

// authorizeMemberListing lets only the management account or a delegated administrator with access enabled in.
func (b *InMemoryBackend) authorizeMemberListing(ctx context.Context) error {
	dir := b.orgDirectory()
	if dir == nil {
		return nil
	}

	caller := b.callerAccount(ctx)

	mgmt, ok := dir.ManagementAccount()
	if !ok {
		return accessDenied("the account is not part of an organization")
	}

	if caller == mgmt {
		return nil
	}

	if dir.IsDelegatedAdmin(servicePrincipal, caller) && b.serviceAccessEnabled() {
		return nil
	}

	return accessDenied(
		"only the management account or a delegated administrator with service access enabled can list member indexes",
	)
}

func (b *InMemoryBackend) serviceAccessEnabled() bool {
	b.mu.RLock("serviceAccessEnabled")
	org := b.org
	b.mu.RUnlock()

	return org != nil && org.ServiceAccessEnabled(servicePrincipal)
}
