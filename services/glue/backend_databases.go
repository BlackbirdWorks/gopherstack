package glue

import (
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// cloneDatabase returns a deep copy of a Database.
func cloneDatabase(db *Database) *Database {
	cp := *db
	cp.Tags = maps.Clone(db.Tags)

	return &cp
}

// databaseARN returns the ARN for a Glue database.
func (b *InMemoryBackend) databaseARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "database/"+name)
}

// CreateDatabase creates a new Glue database.
func (b *InMemoryBackend) CreateDatabase(
	input DatabaseInput,
	tags map[string]string,
) (*Database, error) {
	b.mu.Lock("CreateDatabase")
	defer b.mu.Unlock()

	if input.Name == "" || len(input.Name) > maxNameLen {
		return nil, ErrValidation
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	if b.databases.Has(input.Name) {
		return nil, ErrAlreadyExists
	}

	db := &Database{
		Name:        input.Name,
		Description: input.Description,
		CatalogID:   b.accountID,
		ARN:         b.databaseARN(input.Name),
		Tags:        maps.Clone(tags),
		CreateTime:  float64(time.Now().Unix()),
	}
	b.databases.Put(db)

	return db, nil
}

// GetDatabase retrieves a Glue database by name.
func (b *InMemoryBackend) GetDatabase(name string) (*Database, error) {
	b.mu.RLock("GetDatabase")
	defer b.mu.RUnlock()

	db, ok := b.databases.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneDatabase(db), nil
}

// GetDatabases returns all Glue databases sorted by name.
func (b *InMemoryBackend) GetDatabases() []*Database {
	b.mu.RLock("GetDatabases")
	defer b.mu.RUnlock()

	src := b.databases.Snapshot()
	out := make([]*Database, 0, len(src))
	for _, db := range src {
		out = append(out, cloneDatabase(db))
	}

	return out
}

// DeleteDatabase deletes a Glue database by name, also removing all its tables and partitions.
func (b *InMemoryBackend) DeleteDatabase(name string) error {
	b.mu.Lock("DeleteDatabase")
	defer b.mu.Unlock()

	if !b.databases.Has(name) {
		return ErrNotFound
	}

	b.databases.Delete(name)

	prefix := name + "|"
	for _, t := range b.tables.Snapshot() {
		if k := tableKey(t.DatabaseName, t.Name); len(k) > len(prefix) && k[:len(prefix)] == prefix {
			b.tables.Delete(k)
		}
	}

	for _, p := range b.partitions.Snapshot() {
		if k := partitionKey(p.DatabaseName, p.TableName, p.Values); len(k) > len(prefix) && k[:len(prefix)] == prefix {
			b.partitions.Delete(k)
		}
	}

	for _, tv := range b.tableVersions.Snapshot() {
		if k := tableVersionEntryKeyFn(tv); len(k) > len(prefix) && k[:len(prefix)] == prefix {
			b.tableVersions.Delete(k)
		}
	}

	return nil
}

// UpdateDatabase updates an existing Glue database.
func (b *InMemoryBackend) UpdateDatabase(name string, input DatabaseInput) error {
	b.mu.Lock("UpdateDatabase")
	defer b.mu.Unlock()

	db, ok := b.databases.Get(name)
	if !ok {
		return ErrNotFound
	}

	db.Description = input.Description

	return nil
}
