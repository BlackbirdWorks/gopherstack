package securityhub

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) connectorV2ARN(id string) string {
	return arn.Build("securityhub", b.region, b.accountID, fmt.Sprintf("connector-v2/%s", id))
}

func (b *InMemoryBackend) ticketV2ARN(seq int) string {
	return arn.Build("securityhub", b.region, b.accountID, fmt.Sprintf("ticket-v2/%d", seq))
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

	return c, nil
}

func (b *InMemoryBackend) GetConnectorV2(connectorID string) (*ConnectorV2, error) {
	b.mu.RLock("GetConnectorV2")
	defer b.mu.RUnlock()

	c, ok := b.connectorsV2.Get(connectorID)
	if !ok {
		for _, conn := range b.connectorsV2.All() {
			if conn.ConnectorArn == connectorID {
				cp := *conn

				return &cp, nil
			}
		}

		return nil, ErrNotFound
	}

	cp := *c

	return &cp, nil
}

func (b *InMemoryBackend) ListConnectorsV2(nextToken string, maxResults int) ([]*ConnectorV2, string) {
	b.mu.RLock("ListConnectorsV2")
	defer b.mu.RUnlock()

	snap := b.connectorsV2.All()
	all := make([]*ConnectorV2, 0, len(snap))

	for _, c := range snap {
		cp := *c
		all = append(all, &cp)
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
	cp := *target

	return &cp, nil
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

func (b *InMemoryBackend) RegisterConnectorV2(connectorID string, provider map[string]any) (*ConnectorV2, error) {
	b.mu.Lock("RegisterConnectorV2")
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
	target.ConnectorStatus = "REGISTERED"

	if provider != nil {
		target.Provider = provider
	}

	target.UpdatedAt = now
	cp := *target

	return &cp, nil
}

func (b *InMemoryBackend) CreateTicketV2(
	ticketConfig map[string]any, //nolint:revive // existing issue.
	tags map[string]string,
) (*TicketV2, error) {
	b.mu.Lock("CreateTicketV2")
	defer b.mu.Unlock()

	b.ticketV2Seq++
	arn := b.ticketV2ARN(b.ticketV2Seq)
	now := time.Now().UTC().Format(time.RFC3339)

	t := &TicketV2{
		TicketConfigurationArn: arn,
		CreatedAt:              now,
	}
	b.ticketsV2.Put(t)

	if len(tags) > 0 {
		b.tags[arn] = tags
	}

	return t, nil
}
