package opensearch

import (
	"fmt"
	"time"
)

// ServerlessIndex represents an index inside an OpenSearch Serverless
// collection (opensearchserverless@v1.34.4 api_op_{Create,Get,Update,Delete}Index.go).
// This is a distinct resource from the classic-domain DomainIndex
// (indices.go): AOSS scopes an index by collection ID, not domain name, and
// the real opensearchserverless.Client has its own CreateIndex/GetIndex/
// UpdateIndex/DeleteIndex operations, dispatched here via the AOSS
// X-Amz-Target transport (handler_serverless_jsonrpc.go) rather than the
// classic REST-JSON path.
type ServerlessIndex struct {
	IndexSchema      map[string]any `json:"indexSchema,omitempty"`
	CollectionID     string         `json:"collectionId"`
	IndexName        string         `json:"indexName"`
	CreatedDate      float64        `json:"createdDate"`
	LastModifiedDate float64        `json:"lastModifiedDate"`
}

func serverlessIndexKey(collectionID, indexName string) string {
	return collectionID + "#" + indexName
}

func slIndexKeyFn(v *ServerlessIndex) string { return serverlessIndexKey(v.CollectionID, v.IndexName) }

// CreateServerlessIndex creates a new index inside a collection.
func (b *InMemoryBackend) CreateServerlessIndex(
	collectionID, indexName string, indexSchema map[string]any,
) (*ServerlessIndex, error) {
	if collectionID == "" {
		return nil, fmt.Errorf("%w: Id is required", ErrInvalidParameter)
	}

	if indexName == "" {
		return nil, fmt.Errorf("%w: IndexName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateServerlessIndex")
	defer b.mu.Unlock()

	if _, ok := b.serverlessCollectionByIDLocked(collectionID); !ok {
		return nil, fmt.Errorf("%w: collection %s not found", ErrApplicationNotFound, collectionID)
	}

	key := serverlessIndexKey(collectionID, indexName)
	if b.slIndexes.Has(key) {
		return nil, fmt.Errorf("%w: index %s already exists", ErrApplicationAlreadyExists, indexName)
	}

	now := float64(time.Now().Unix())
	idx := &ServerlessIndex{
		CollectionID:     collectionID,
		IndexName:        indexName,
		IndexSchema:      indexSchema,
		CreatedDate:      now,
		LastModifiedDate: now,
	}
	b.slIndexes.Put(idx)

	cp := *idx

	return &cp, nil
}

// GetServerlessIndex retrieves an index by collection ID and name.
func (b *InMemoryBackend) GetServerlessIndex(collectionID, indexName string) (*ServerlessIndex, error) {
	b.mu.RLock("GetServerlessIndex")
	defer b.mu.RUnlock()

	idx, ok := b.slIndexes.Get(serverlessIndexKey(collectionID, indexName))
	if !ok {
		return nil, fmt.Errorf("%w: index %s not found", ErrApplicationNotFound, indexName)
	}

	cp := *idx

	return &cp, nil
}

// UpdateServerlessIndex replaces an existing index's schema.
func (b *InMemoryBackend) UpdateServerlessIndex(
	collectionID, indexName string, indexSchema map[string]any,
) (*ServerlessIndex, error) {
	b.mu.Lock("UpdateServerlessIndex")
	defer b.mu.Unlock()

	key := serverlessIndexKey(collectionID, indexName)

	idx, ok := b.slIndexes.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: index %s not found", ErrApplicationNotFound, indexName)
	}

	idx.IndexSchema = indexSchema
	idx.LastModifiedDate = float64(time.Now().Unix())

	cp := *idx

	return &cp, nil
}

// DeleteServerlessIndex removes an index from a collection.
func (b *InMemoryBackend) DeleteServerlessIndex(collectionID, indexName string) error {
	b.mu.Lock("DeleteServerlessIndex")
	defer b.mu.Unlock()

	key := serverlessIndexKey(collectionID, indexName)
	if !b.slIndexes.Has(key) {
		return fmt.Errorf("%w: index %s not found", ErrApplicationNotFound, indexName)
	}

	b.slIndexes.Delete(key)

	return nil
}
