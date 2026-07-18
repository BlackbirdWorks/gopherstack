package verifiedpermissions

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// extractSchemaNamespaces parses a Cedar JSON schema and returns its top-level namespace keys.
func extractSchemaNamespaces(schemaJSON string) []string {
	var top map[string]json.RawMessage

	if err := json.Unmarshal([]byte(schemaJSON), &top); err != nil {
		return []string{}
	}

	ns := collections.SortedKeys(top)

	return ns
}

// PutSchema creates or replaces the schema for a policy store, extracts namespaces, and returns them.
func (b *InMemoryBackend) PutSchema(policyStoreID, schema string) ([]string, error) {
	b.mu.Lock("PutSchema")
	defer b.mu.Unlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	// Validate JSON format.
	if !json.Valid([]byte(schema)) {
		return nil, fmt.Errorf("%w: schema is not valid JSON", ErrValidation)
	}

	namespaces := extractSchemaNamespaces(schema)

	now := time.Now()
	existing, ok := b.schemas.Get(policyStoreID)

	if ok {
		existing.Schema = schema
		existing.LastUpdated = now
		existing.Namespaces = namespaces
	} else {
		b.schemas.Put(&PolicyStoreSchema{
			Schema:        schema,
			CreatedDate:   now,
			LastUpdated:   now,
			Namespaces:    namespaces,
			policyStoreID: policyStoreID,
		})
	}

	return namespaces, nil
}

// GetSchema returns the schema for a policy store.
func (b *InMemoryBackend) GetSchema(policyStoreID string) (*PolicyStoreSchema, error) {
	b.mu.RLock("GetSchema")
	defer b.mu.RUnlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	s, ok := b.schemas.Get(policyStoreID)
	if !ok {
		return nil, fmt.Errorf("%w: no schema found for policy store %s", ErrSchemaNotFound, policyStoreID)
	}

	cp := *s
	if len(s.Namespaces) > 0 {
		cp.Namespaces = make([]string, len(s.Namespaces))
		copy(cp.Namespaces, s.Namespaces)
	}

	return &cp, nil
}
