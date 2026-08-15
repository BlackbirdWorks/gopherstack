package bedrockagent

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"
)

// ---------------------------------------------------------------------------
// Knowledge base CRUD
// ---------------------------------------------------------------------------

// CreateKnowledgeBase creates a new knowledge base.
func (b *InMemoryBackend) CreateKnowledgeBase(
	ctx context.Context, cfg KnowledgeBaseConfig,
) (*KnowledgeBase, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	region := ctxRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.kbsByName[cfg.Name]; exists {
		return nil, fmt.Errorf("%w: knowledge base %q already exists", ErrAlreadyExists, cfg.Name)
	}

	id := b.nextID("kb", &b.kbCounter)
	now := time.Now().UTC()

	kb := &KnowledgeBase{
		KnowledgeBaseID:      id,
		KnowledgeBaseARN:     b.buildKBARN(region, id),
		Name:                 cfg.Name,
		Status:               kbStatusActive,
		Description:          cfg.Description,
		RoleARN:              cfg.RoleARN,
		KBConfiguration:      cfg.KBConfiguration,
		StorageConfiguration: cfg.StorageConfiguration,
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	b.knowledgeBases.Put(kb)
	b.kbsByName[cfg.Name] = id
	b.tags[kb.KnowledgeBaseARN] = maps.Clone(cfg.Tags)

	return kbCopy(kb), nil
}

// GetKnowledgeBase returns a knowledge base.
func (b *InMemoryBackend) GetKnowledgeBase(_ context.Context, kbID string) (*KnowledgeBase, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	kb, ok := b.knowledgeBases.Get(kbID)
	if !ok {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	return kbCopy(kb), nil
}

// UpdateKnowledgeBase updates a knowledge base.
func (b *InMemoryBackend) UpdateKnowledgeBase(
	_ context.Context, kbID string, cfg KnowledgeBaseConfig,
) (*KnowledgeBase, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	kb, ok := b.knowledgeBases.Get(kbID)
	if !ok {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	if cfg.Name != "" {
		kb.Name = cfg.Name
	}

	if cfg.Description != "" {
		kb.Description = cfg.Description
	}

	if cfg.RoleARN != "" {
		kb.RoleARN = cfg.RoleARN
	}

	if cfg.KBConfiguration != nil {
		kb.KBConfiguration = cfg.KBConfiguration
	}

	if cfg.StorageConfiguration != nil {
		kb.StorageConfiguration = cfg.StorageConfiguration
	}

	kb.UpdatedAt = time.Now().UTC()

	return kbCopy(kb), nil
}

// DeleteKnowledgeBase deletes a knowledge base and cascade-cleans every
// resource scoped under it: data sources, and (per data source) ingestion
// jobs and ingested KB documents, plus the KB's own tags and its resource
// policy (bedrock-agent's PutResourcePolicy/GetResourcePolicy/
// DeleteResourcePolicy attach only to knowledge bases -- see
// resource_policy.go). Without this, a deleted-and-recreated KB with the
// same auto-incremented ID space would never actually observe empty child
// collections, and the tags/resourcePolicies maps would keep a permanent
// ghost entry keyed by the now-invalid ARN.
func (b *InMemoryBackend) DeleteKnowledgeBase(_ context.Context, kbID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	kb, ok := b.knowledgeBases.Get(kbID)
	if !ok {
		return fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	delete(b.kbsByName, kb.Name)
	b.knowledgeBases.Delete(kbID)
	delete(b.tags, kb.KnowledgeBaseARN)
	b.resourcePolicies.Delete(kb.KnowledgeBaseARN)

	for _, ds := range slices.Clone(b.dataSourcesByKB.Get(kbID)) {
		b.deleteDataSourceChildrenLocked(kbID, ds.DataSourceID)
		b.dataSources.Delete(dsKey(kbID, ds.DataSourceID))
	}

	return nil
}

// ListKnowledgeBases returns paginated knowledge base summaries.
func (b *InMemoryBackend) ListKnowledgeBases(
	_ context.Context, maxResults int, nextToken string,
) ([]*KnowledgeBaseSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ids := tableIDs(b.knowledgeBases.Snapshot(), func(kb *KnowledgeBase) string { return kb.KnowledgeBaseID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*KnowledgeBaseSummary, 0, len(ids))

	for _, id := range ids {
		kb, _ := b.knowledgeBases.Get(id)
		out = append(out, &KnowledgeBaseSummary{
			KnowledgeBaseID: kb.KnowledgeBaseID,
			Name:            kb.Name,
			Status:          kb.Status,
			Description:     kb.Description,
			UpdatedAt:       kb.UpdatedAt,
		})
	}

	return out, outToken, nil
}

func kbCopy(kb *KnowledgeBase) *KnowledgeBase {
	cp := *kb

	return &cp
}

// ---------------------------------------------------------------------------
// Knowledge base document operations
// ---------------------------------------------------------------------------

func kbDocKey(kbID, dsID, docKey string) string { return kbID + "/" + dsID + "/" + docKey }

// kbDocumentIdentifierKey resolves id to its storage key, or a ValidationException
// if id doesn't carry the sub-object its own DataSourceType requires -- the
// real API has no other way to name a document.
func kbDocumentIdentifierKey(id KBDocumentIdentifier) (string, error) {
	key := id.key()
	if key == "" {
		return "", fmt.Errorf(
			"%w: document identifier requires dataSourceType and a matching custom.id or s3.uri",
			ErrValidation,
		)
	}

	return key, nil
}

// IngestKnowledgeBaseDocuments ingests documents into a knowledge base data source.
func (b *InMemoryBackend) IngestKnowledgeBaseDocuments(
	_ context.Context, kbID, dsID string, docs []KBDocument,
) ([]KBDocumentDetail, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.dataSources.Has(dsKey(kbID, dsID)) {
		return nil, fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	out := make([]KBDocumentDetail, 0, len(docs))

	for _, doc := range docs {
		if _, err := kbDocumentIdentifierKey(doc.Identifier); err != nil {
			return nil, err
		}

		detail := &KBDocumentDetail{
			Identifier:      doc.Identifier,
			KnowledgeBaseID: kbID,
			DataSourceID:    dsID,
			Status:          docStatusIndexed,
		}
		b.kbDocuments.Put(detail)
		out = append(out, *detail)
	}

	return out, nil
}

// GetKnowledgeBaseDocuments retrieves document details.
func (b *InMemoryBackend) GetKnowledgeBaseDocuments(
	_ context.Context, kbID, dsID string, ids []KBDocumentIdentifier,
) ([]KBDocumentDetail, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]KBDocumentDetail, 0, len(ids))

	for _, id := range ids {
		key, err := kbDocumentIdentifierKey(id)
		if err != nil {
			return nil, err
		}

		detail, ok := b.kbDocuments.Get(kbDocKey(kbID, dsID, key))
		if !ok {
			return nil, fmt.Errorf("%w: document not found", ErrNotFound)
		}

		out = append(out, *detail)
	}

	return out, nil
}

// DeleteKnowledgeBaseDocuments deletes documents from a knowledge base data source.
func (b *InMemoryBackend) DeleteKnowledgeBaseDocuments(
	_ context.Context, kbID, dsID string, ids []KBDocumentIdentifier,
) ([]KBDocumentDetail, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]KBDocumentDetail, 0, len(ids))

	for _, id := range ids {
		key, err := kbDocumentIdentifierKey(id)
		if err != nil {
			return nil, err
		}

		storeKey := kbDocKey(kbID, dsID, key)

		detail, ok := b.kbDocuments.Get(storeKey)
		if !ok {
			out = append(out, KBDocumentDetail{
				Identifier:      id,
				KnowledgeBaseID: kbID,
				DataSourceID:    dsID,
				Status:          docStatusNotFound,
			})

			continue
		}

		b.kbDocuments.Delete(storeKey)

		d := *detail
		d.Status = docStatusDeleting
		out = append(out, d)
	}

	return out, nil
}

// ListKnowledgeBaseDocuments returns paginated document details.
func (b *InMemoryBackend) ListKnowledgeBaseDocuments(
	_ context.Context, kbID, dsID string, maxResults int, nextToken string,
) ([]KBDocumentDetail, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	group := b.kbDocumentsByDataSource.Get(dsKey(kbID, dsID))
	keys := tableIDs(group, func(d *KBDocumentDetail) string {
		return kbDocKey(d.KnowledgeBaseID, d.DataSourceID, d.Identifier.key())
	})
	keys, outToken := paginate(keys, nextToken, maxResults)

	out := make([]KBDocumentDetail, 0, len(keys))

	for _, k := range keys {
		d, _ := b.kbDocuments.Get(k)
		out = append(out, *d)
	}

	return out, outToken, nil
}
