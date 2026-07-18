package cleanrooms

import (
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/google/uuid"
)

func (b *InMemoryBackend) idMappingTableARN(membershipID, id string) string {
	return arn.Build(
		"cleanrooms",
		b.region,
		b.accountID,
		fmt.Sprintf("membership/%s/idmappingtable/%s", membershipID, id),
	)
}

func toIDMappingTableSummary(t *IDMappingTable) *IDMappingTableSummary {
	return &IDMappingTableSummary{
		IDMappingTableIdentifier: t.IDMappingTableIdentifier,
		Arn:                      t.Arn,
		CollaborationArn:         t.CollaborationArn,
		CollaborationIdentifier:  t.CollaborationIdentifier,
		MembershipArn:            t.MembershipArn,
		MembershipIdentifier:     t.MembershipIdentifier,
		Name:                     t.Name,
		CreateTime:               t.CreateTime,
		UpdateTime:               t.UpdateTime,
		ID:                       t.IDMappingTableIdentifier,
		MembershipID:             t.MembershipIdentifier,
		CollaborationID:          t.CollaborationIdentifier,
	}
}

func (b *InMemoryBackend) CreateIDMappingTable(
	membershipID, name, description string,
	inputReferenceConfig map[string]any,
	kmsKeyArn string,
	tags map[string]string,
) (*IDMappingTable, error) {
	if name == "" {
		return nil, ErrValidation
	}
	b.mu.Lock("CreateIDMappingTable")
	defer b.mu.Unlock()
	mem, ok := b.memberships.Get(membershipID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	collab, _ := b.collaborations.Get(mem.CollaborationIdentifier)
	var collabArn string
	if collab != nil {
		collabArn = collab.Arn
	}
	t := &IDMappingTable{
		IDMappingTableIdentifier: id,
		Arn:                      b.idMappingTableARN(membershipID, id),
		CollaborationArn:         collabArn,
		CollaborationIdentifier:  mem.CollaborationIdentifier,
		MembershipArn:            mem.Arn,
		MembershipIdentifier:     membershipID,
		Name:                     name,
		Description:              description,
		InputReferenceConfig:     inputReferenceConfig,
		KmsKeyArn:                kmsKeyArn,
		CreateTime:               ts,
		UpdateTime:               ts,
		Tags:                     tags,
		ID:                       id,
		MembershipID:             membershipID,
		CollaborationID:          mem.CollaborationIdentifier,
	}
	b.idMappingTables.Put(t)
	if len(tags) > 0 {
		b.tagsByArn[t.Arn] = maps.Clone(tags)
	}

	return t, nil
}

func (b *InMemoryBackend) GetIDMappingTable(membershipID, tableID string) (*IDMappingTable, error) {
	b.mu.RLock("GetIDMappingTable")
	defer b.mu.RUnlock()
	t, ok := b.idMappingTables.Get(membershipKey(membershipID, tableID))
	if !ok {
		return nil, ErrNotFound
	}

	return t, nil
}

func (b *InMemoryBackend) ListIDMappingTables(
	membershipID, maxResults, nextToken string,
) ([]*IDMappingTableSummary, string, error) {
	b.mu.RLock("ListIDMappingTables")
	defer b.mu.RUnlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listItems(
		b.idMappingTablesByMembership.Get(membershipID),
		nil,
		toIDMappingTableSummary,
		func(a, c *IDMappingTableSummary) bool {
			return a.IDMappingTableIdentifier < c.IDMappingTableIdentifier
		},
		maxResults, nextToken,
	)

	return page, next, nil
}

func (b *InMemoryBackend) UpdateIDMappingTable(
	membershipID, tableID, description, kmsKeyArn string,
) (*IDMappingTable, error) {
	b.mu.Lock("UpdateIDMappingTable")
	defer b.mu.Unlock()
	t, ok := b.idMappingTables.Get(membershipKey(membershipID, tableID))
	if !ok {
		return nil, ErrNotFound
	}
	if description != "" {
		t.Description = description
	}
	if kmsKeyArn != "" {
		t.KmsKeyArn = kmsKeyArn
	}
	t.UpdateTime = b.now()

	return t, nil
}

func (b *InMemoryBackend) DeleteIDMappingTable(membershipID, tableID string) error {
	b.mu.Lock("DeleteIDMappingTable")
	defer b.mu.Unlock()
	key := membershipKey(membershipID, tableID)
	t, ok := b.idMappingTables.Get(key)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, t.Arn)
	b.idMappingTables.Delete(key)

	return nil
}

func (b *InMemoryBackend) PopulateIDMappingTable(
	membershipID, tableID string,
) (map[string]any, error) {
	b.mu.RLock("PopulateIDMappingTable")
	defer b.mu.RUnlock()
	if _, ok := b.idMappingTables.Get(membershipKey(membershipID, tableID)); !ok {
		return nil, ErrNotFound
	}

	return map[string]any{"mappedJobIdentifier": uuid.NewString()}, nil
}
