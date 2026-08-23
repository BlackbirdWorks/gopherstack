package securityhub

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) connectorV2ARN(id string) string {
	return arn.Build("securityhub", b.region, b.accountID, fmt.Sprintf("connector-v2/%s", id))
}

// clone deep-copies c's map fields. Tags is created aliased to
// b.tags[ConnectorArn] (same map object, see CreateConnectorV2), and
// TagResource/UntagResource mutate that map in place under lock -- a
// shallow "cp := *c" leaves the returned copy's Tags field pointing at that
// live, mutable map.
func (c *ConnectorV2) clone() *ConnectorV2 {
	cp := *c
	cp.Provider = maps.Clone(c.Provider)
	cp.Tags = maps.Clone(c.Tags)

	return &cp
}

func (b *InMemoryBackend) CreateConnectorV2(
	name, description string,
	provider map[string]any,
	tags map[string]string,
) (*ConnectorV2, error) {
	b.mu.Lock("CreateConnectorV2")
	defer b.mu.Unlock()

	b.connectorV2Seq++
	id := fmt.Sprintf("connector-v2-%d", b.connectorV2Seq)
	arn := b.connectorV2ARN(id)
	now := time.Now().UTC().Format(time.RFC3339)

	c := &ConnectorV2{
		ConnectorId:     id,
		ConnectorArn:    arn,
		Name:            name,
		Description:     description,
		CreatedAt:       now,
		UpdatedAt:       now,
		ConnectorStatus: "ACTIVE",
		Provider:        provider,
		Tags:            tags,
	}
	b.connectorsV2.Put(c)

	if len(tags) > 0 {
		b.tags[arn] = tags
	}

	return c.clone(), nil
}

func (b *InMemoryBackend) GetConnectorV2(connectorID string) (*ConnectorV2, error) {
	b.mu.RLock("GetConnectorV2")
	defer b.mu.RUnlock()

	c, ok := b.connectorsV2.Get(connectorID)
	if !ok {
		for _, conn := range b.connectorsV2.All() {
			if conn.ConnectorArn == connectorID {
				return conn.clone(), nil
			}
		}

		return nil, ErrNotFound
	}

	return c.clone(), nil
}

func (b *InMemoryBackend) ListConnectorsV2(nextToken string, maxResults int) ([]*ConnectorV2, string) {
	b.mu.RLock("ListConnectorsV2")
	defer b.mu.RUnlock()

	snap := b.connectorsV2.All()
	all := make([]*ConnectorV2, 0, len(snap))

	for _, c := range snap {
		all = append(all, c.clone())
	}

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

func (b *InMemoryBackend) UpdateConnectorV2(
	connectorID, name, description string,
	provider map[string]any,
) (*ConnectorV2, error) {
	b.mu.Lock("UpdateConnectorV2")
	defer b.mu.Unlock()

	var target *ConnectorV2

	if c, ok := b.connectorsV2.Get(connectorID); ok {
		target = c
	} else {
		for _, conn := range b.connectorsV2.All() {
			if conn.ConnectorArn == connectorID {
				target = conn

				break
			}
		}
	}

	if target == nil {
		return nil, ErrNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)

	if name != "" {
		target.Name = name
	}

	if description != "" {
		target.Description = description
	}

	if provider != nil {
		target.Provider = provider
	}

	target.UpdatedAt = now

	return target.clone(), nil
}

func (b *InMemoryBackend) DeleteConnectorV2(connectorID string) error {
	b.mu.Lock("DeleteConnectorV2")
	defer b.mu.Unlock()

	if b.connectorsV2.Delete(connectorID) {
		return nil
	}

	for _, c := range b.connectorsV2.All() {
		if c.ConnectorArn == connectorID {
			b.connectorsV2.Delete(c.ConnectorId)

			return nil
		}
	}

	return ErrNotFound
}

// RegisterConnectorV2 completes the OAuth 2.0 authorization-code flow the
// real RegisterConnectorV2Input carries: AuthCode and AuthState, nothing
// else (securityhub@v1.75.4 api_op_RegisterConnectorV2.go:26-40) -- there is
// no ConnectorId input member at all. AuthState's on-wire content is opaque
// to any real AWS client: it is minted server-side, handed back verbatim by
// the OAuth provider, and only this backend ever inspects it. This backend's
// convention is that AuthState IS the connector ID it was minted for, so
// decoding it back to a connector is a direct lookup rather than a guess at
// AWS's internal encoding. AuthCode is accepted (real clients must send it)
// but not persisted: nothing in ConnectorV2 models it, and no RegisterConnectorV2
// output field echoes it back either.
func (b *InMemoryBackend) RegisterConnectorV2(_, authState string) (*ConnectorV2, error) {
	b.mu.Lock("RegisterConnectorV2")
	defer b.mu.Unlock()

	var target *ConnectorV2

	if c, ok := b.connectorsV2.Get(authState); ok {
		target = c
	} else {
		for _, conn := range b.connectorsV2.All() {
			if conn.ConnectorArn == authState {
				target = conn

				break
			}
		}
	}

	if target == nil {
		return nil, ErrNotFound
	}

	target.ConnectorStatus = "REGISTERED"
	target.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	return target.clone(), nil
}

func (b *InMemoryBackend) CreateTicketV2(connectorID, findingMetadataUID, mode string) (*TicketV2, error) {
	b.mu.Lock("CreateTicketV2")
	defer b.mu.Unlock()

	if _, ok := b.connectorsV2.Get(connectorID); !ok {
		found := false

		for _, conn := range b.connectorsV2.All() {
			if conn.ConnectorArn == connectorID {
				found = true

				break
			}
		}

		if !found {
			return nil, ErrNotFound
		}
	}

	b.ticketV2Seq++
	id := fmt.Sprintf("ticket-v2-%d", b.ticketV2Seq)
	now := time.Now().UTC().Format(time.RFC3339)

	t := &TicketV2{
		TicketId:           id,
		ConnectorId:        connectorID,
		FindingMetadataUid: findingMetadataUID,
		Mode:               mode,
		CreatedAt:          now,
	}
	b.ticketsV2.Put(t)

	return t, nil
}
