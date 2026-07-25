package glue

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrIllegalSessionState is returned by GetSessionEndpoint when the target
// session is not in a state that can serve a Spark Connect endpoint,
// mirroring AWS's IllegalSessionStateException (confirmed in
// deserializers.go's awsAwsjson11_deserializeOpErrorGetSessionEndpoint error
// switch).
var ErrIllegalSessionState = awserr.New("IllegalSessionStateException", awserr.ErrConflict)

// glueResourceTypeJob and glueResourceTypeSession are GetDashboardUrlInput's
// two documented ResourceType values (types.GlueResourceType). The wire
// operation/field names use AWS's "Url" spelling; the Go method below is
// named GetDashboardURL per Go's URL-initialism convention (revive).
const (
	glueResourceTypeJob     = "JOB"
	glueResourceTypeSession = "SESSION"
)

// sessionEndpointTokenTTL is how long a GetSessionEndpoint auth token remains
// valid. AWS does not document a fixed lifetime; one hour matches the
// lifetime this backend already uses elsewhere for similarly short-lived
// mock credentials.
const sessionEndpointTokenTTL = time.Hour

// SessionEndpoint carries the Spark Connect endpoint URL and authentication
// token for an interactive session, field-diffed against
// types.SessionEndpoint.
type SessionEndpoint struct {
	URL                     string  `json:"Url"`
	AuthToken               string  `json:"AuthToken"`
	AuthTokenExpirationTime float64 `json:"AuthTokenExpirationTime"`
}

// GetDashboardURL returns the Spark monitoring dashboard URL for a JOB or
// SESSION resource. This backend has no real Glue Studio console, so the URL
// is a deterministic mock value -- the same modeling choice already used
// throughout this package for other console/network-facing fields that have
// no real infrastructure behind them (e.g. DevEndpoint's
// YarnEndpointAddress/PrivateAddress/PublicAddress; see PARITY.md). What IS
// real is the existence check: the named JOB/SESSION must actually exist in
// this backend, so a caller referencing an unknown resource still gets the
// documented EntityNotFoundException rather than a URL for nothing.
func (b *InMemoryBackend) GetDashboardURL(resourceID, resourceType, requestOrigin string) (string, error) {
	b.mu.RLock("GetDashboardURL")
	defer b.mu.RUnlock()

	switch resourceType {
	case glueResourceTypeJob:
		if !b.jobs.Has(resourceID) {
			return "", fmt.Errorf("job %q not found: %w", resourceID, ErrNotFound)
		}
	case glueResourceTypeSession:
		if !b.sessions.Has(resourceID) {
			return "", fmt.Errorf("session %q not found: %w", resourceID, ErrNotFound)
		}
	default:
		return "", fmt.Errorf(
			"%w: ResourceType must be %s or %s", ErrValidation, glueResourceTypeJob, glueResourceTypeSession,
		)
	}

	origin := requestOrigin
	if origin == "" {
		origin = "console"
	}

	url := fmt.Sprintf(
		"https://%s.console.aws.amazon.com/gluestudio/home?region=%s#/monitoring/%s/%s?origin=%s",
		b.region, b.region, strings.ToLower(resourceType), resourceID, origin,
	)

	return url, nil
}

// GetSessionEndpoint returns the Spark Connect endpoint for an interactive
// session. The session must exist and must not have been stopped/be
// stopping; this backend does not model a real Spark Connect listener, so the
// URL/token are deterministic mock values (same modeling choice as
// GetDashboardURL above), but the existence and state checks are real.
func (b *InMemoryBackend) GetSessionEndpoint(sessionID string) (*SessionEndpoint, error) {
	b.mu.RLock("GetSessionEndpoint")
	defer b.mu.RUnlock()

	s, ok := b.sessions.Get(sessionID)
	if !ok {
		return nil, fmt.Errorf("session %q not found: %w", sessionID, ErrNotFound)
	}

	if s.Status == stateStopped || s.Status == stateStopping {
		return nil, fmt.Errorf(
			"%w: session %q is not in a usable state (status=%s)", ErrIllegalSessionState, sessionID, s.Status,
		)
	}

	url := fmt.Sprintf("sc://%s.session.glue.%s.amazonaws.com:443/;x-session-id=%s", sessionID, b.region, sessionID)

	return &SessionEndpoint{
		URL:                     url,
		AuthToken:               "sct-" + uuid.NewString(),
		AuthTokenExpirationTime: float64(time.Now().Add(sessionEndpointTokenTTL).Unix()),
	}, nil
}
