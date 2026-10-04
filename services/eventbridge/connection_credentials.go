package eventbridge

import (
	"errors"
	"fmt"
)

// ErrConnectionNotAuthorized means the connection is not in the AUTHORIZED state.
var ErrConnectionNotAuthorized = errors.New("connection is not AUTHORIZED")

// ResolveConnectionAuth returns a copy of an AUTHORIZED connection's un-masked auth and invocation parameters.
func (b *InMemoryBackend) ResolveConnectionAuth(connectionARN string) (*ResolvedAPIDestination, error) {
	region := arnRegion(connectionARN)
	if region == "" {
		region = b.region
	}

	name := arnResourceName(connectionARN, "connection")
	if name == "" {
		return nil, fmt.Errorf("%w: invalid connection ARN", ErrNotFound)
	}

	b.mu.RLock("ResolveConnectionAuth")
	defer b.mu.RUnlock()

	table := b.connections[region]
	if table == nil {
		return nil, fmt.Errorf("%w: connection %s not found", ErrNotFound, name)
	}

	conn, ok := table.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: connection %s not found", ErrNotFound, name)
	}

	if conn.ConnectionState != "AUTHORIZED" {
		return nil, fmt.Errorf("%w: connection %s is %s", ErrConnectionNotAuthorized, name, conn.ConnectionState)
	}

	cp := *conn
	cp.authSecret = cloneConnectionAuthParameters(conn.authSecret)

	resolved := &ResolvedAPIDestination{}
	applyConnectionAuthToResolved(resolved, &cp)

	return resolved, nil
}
