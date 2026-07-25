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
// jobs and ingested KB documents, plus the KB's own tags. Without this, a
// deleted-and-recreated KB with the same auto-incremented ID space would
// never actually observe empty child collections, and the tags map would
// keep a permanent ghost entry keyed by the now-invalid ARN.
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

func kbDocKey(kbID, dsID, docID string) string { return kbID + "/" + dsID + "/" + docID }

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
		detail := &KBDocumentDetail{
			DocumentID:      doc.DocID,
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
	_ context.Context, kbID, dsID string, docIDs []string,
) ([]KBDocumentDetail, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]KBDocumentDetail, 0, len(docIDs))

	for _, id := range docIDs {
		detail, ok := b.kbDocuments.Get(kbDocKey(kbID, dsID, id))
		if !ok {
			return nil, fmt.Errorf("%w: document %q not found", ErrNotFound, id)
		}

		out = append(out, *detail)
	}

	return out, nil
}

// DeleteKnowledgeBaseDocuments deletes documents from a knowledge base data source.
func (b *InMemoryBackend) DeleteKnowledgeBaseDocuments(
	_ context.Context, kbID, dsID string, docIDs []string,
) ([]KBDocumentDetail, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]KBDocumentDetail, 0, len(docIDs))

	for _, id := range docIDs {
		key := kbDocKey(kbID, dsID, id)

		detail, ok := b.kbDocuments.Get(key)
		if !ok {
			out = append(out, KBDocumentDetail{
				DocumentID:      id,
				KnowledgeBaseID: kbID,
				DataSourceID:    dsID,
				Status:          "NOT_FOUND",
			})

			continue
		}

		b.kbDocuments.Delete(key)

		d := *detail
		d.Status = "DELETED"
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
		return kbDocKey(d.KnowledgeBaseID, d.DataSourceID, d.DocumentID)
	})
	keys, outToken := paginate(keys, nextToken, maxResults)

	out := make([]KBDocumentDetail, 0, len(keys))

	for _, k := range keys {
		d, _ := b.kbDocuments.Get(k)
		out = append(out, *d)
	}

	return out, outToken, nil
}
