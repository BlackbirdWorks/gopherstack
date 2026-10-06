package redshiftdata

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

// callerIdentity is the IAM role and session behind a request; both are empty when unauthenticated.
type callerIdentity struct {
	role    string
	session string
}

func callerFromContext(ctx context.Context) callerIdentity {
	m, ok := awsmeta.Key.Get(ctx)
	if !ok || m == nil || m.Principal == nil {
		return callerIdentity{}
	}

	p := m.Principal
	if p.Kind == awsmeta.PrincipalKindAssumedRole {
		role := p.Arn
		if i := strings.LastIndex(role, "/"); i >= 0 {
			role = role[:i]
		}

		return callerIdentity{role: role, session: p.SessionName}
	}

	return callerIdentity{role: p.Arn}
}

// sees applies ListStatements/ListSessions RoleLevel: true (default) matches the caller's
// role, false also requires the same session. Unattributed records stay visible.
func (c callerIdentity) sees(owner, ownerSession string, roleLevel *bool) bool {
	if c.role == "" || owner == "" {
		return true
	}

	if owner != c.role {
		return false
	}

	return roleLevel == nil || *roleLevel || ownerSession == c.session
}
