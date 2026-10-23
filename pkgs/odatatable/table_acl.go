package odatatable

// MaxSignedIdentifiers is the most stored access policies a table may hold.
const MaxSignedIdentifiers = 5

// AccessPolicy is a stored access policy's time window and permissions.
type AccessPolicy struct {
	Start      string
	Expiry     string
	Permission string
}

// SignedIdentifier is one stored access policy of a table ACL.
type SignedIdentifier struct {
	ID           string
	AccessPolicy AccessPolicy
}

// SetTableACL replaces a table's stored access policies. Returns ErrTableNotFound if the table does not exist.
func (b *InMemoryBackend) SetTableACL(table string, ids []SignedIdentifier) error {
	b.mu.Lock("SetTableACL")
	defer b.mu.Unlock()

	t, ok := b.tables[table]
	if !ok {
		return ErrTableNotFound
	}

	t.ACL = append([]SignedIdentifier(nil), ids...)

	return nil
}

// GetTableACL returns a table's stored access policies. Returns ErrTableNotFound if the table does not exist.
func (b *InMemoryBackend) GetTableACL(table string) ([]SignedIdentifier, error) {
	b.mu.RLock("GetTableACL")
	defer b.mu.RUnlock()

	t, ok := b.tables[table]
	if !ok {
		return nil, ErrTableNotFound
	}

	return append([]SignedIdentifier(nil), t.ACL...), nil
}
