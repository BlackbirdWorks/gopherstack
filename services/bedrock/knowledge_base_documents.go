package bedrock

import (
	"fmt"
	"sort"
	"time"
)

const (
	docStatusIndexed = "INDEXED"
)

func kbDocKey(kbID, dsID, docID string) string { return kbID + "/" + dsID + "/" + docID }

// KBDocumentIdentifier identifies a knowledge base document by data source
// type, matching bedrockagent@v1.58.4 types.DocumentIdentifier (dataSourceType
// plus exactly one of s3.uri / custom.id).
type KBDocumentIdentifier struct {
	DataSourceType string
	S3URI          string
	CustomID       string
}

// id returns the identifier value to use as the document's lookup key.
func (i KBDocumentIdentifier) id() string {
	if i.DataSourceType == "CUSTOM" {
		return i.CustomID
	}

	return i.S3URI
}

// IngestKnowledgeBaseDocuments adds documents to a KB data source.
func (b *InMemoryBackend) IngestKnowledgeBaseDocuments(
	kbID, dsID string,
	identifiers []KBDocumentIdentifier,
) ([]*KnowledgeBaseDocument, error) {
	b.mu.Lock("IngestKnowledgeBaseDocuments")
	defer b.mu.Unlock()

	if _, ok := b.knowledgeBases.Get(kbID); !ok {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	if _, ok := b.dataSources.Get(kbID + "/" + dsID); !ok {
		return nil, fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	docs := make([]*KnowledgeBaseDocument, 0, len(identifiers))

	for _, ident := range identifiers {
		doc := &KnowledgeBaseDocument{
			KnowledgeBaseID: kbID,
			DataSourceID:    dsID,
			DocumentID:      ident.id(),
			DataSourceType:  ident.DataSourceType,
			Status:          docStatusIndexed,
			UpdatedAt:       time.Now().UTC(),
		}
		b.kbDocuments.Put(doc)
		cp := *doc
		docs = append(docs, &cp)
	}

	return docs, nil
}

// ListKnowledgeBaseDocuments returns documents for a KB data source.
func (b *InMemoryBackend) ListKnowledgeBaseDocuments(
	kbID, dsID string,
	maxResults int,
	nextToken string,
) ([]*KnowledgeBaseDocument, string) {
	b.mu.RLock("ListKnowledgeBaseDocuments")
	defer b.mu.RUnlock()

	list := make([]*KnowledgeBaseDocument, 0)

	for _, doc := range b.kbDocuments.All() {
		if doc.KnowledgeBaseID == kbID && doc.DataSourceID == dsID {
			cp := *doc
			list = append(list, &cp)
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].DocumentID < list[j].DocumentID })

	return paginate(list, maxResults, nextToken)
}

// GetKnowledgeBaseDocuments returns selected documents for a KB data source.
func (b *InMemoryBackend) GetKnowledgeBaseDocuments(
	kbID, dsID string,
	identifiers []KBDocumentIdentifier,
) ([]*KnowledgeBaseDocument, error) {
	b.mu.RLock("GetKnowledgeBaseDocuments")
	defer b.mu.RUnlock()

	if _, ok := b.dataSources.Get(kbID + "/" + dsID); !ok {
		return nil, fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	docs := make([]*KnowledgeBaseDocument, 0, len(identifiers))

	for _, ident := range identifiers {
		doc, ok := b.kbDocuments.Get(kbDocKey(kbID, dsID, ident.id()))
		if !ok {
			continue
		}

		cp := *doc
		docs = append(docs, &cp)
	}

	return docs, nil
}

// DeleteKnowledgeBaseDocuments removes documents from a KB data source.
func (b *InMemoryBackend) DeleteKnowledgeBaseDocuments(
	kbID, dsID string,
	identifiers []KBDocumentIdentifier,
) error {
	b.mu.Lock("DeleteKnowledgeBaseDocuments")
	defer b.mu.Unlock()

	for _, ident := range identifiers {
		b.kbDocuments.Delete(kbDocKey(kbID, dsID, ident.id()))
	}

	return nil
}
