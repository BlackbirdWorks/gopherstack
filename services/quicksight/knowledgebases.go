package quicksight

import (
	"maps"
	"sort"
	"time"
)

const (
	knowledgeBaseStatusActive = "ACTIVE"

	// filterKnowledgeBaseName is the SearchKnowledgeBases filter attribute
	// name for matching on a knowledge base's display name (the real API's
	// KNOWLEDGE_BASE_NAME filter).
	filterKnowledgeBaseName = "KNOWLEDGE_BASE_NAME"
)

func knowledgeBaseKey(accountID, knowledgeBaseID string) string {
	return accountID + "/" + knowledgeBaseID
}

// storedKnowledgeBase is the persisted representation of a QuickSight
// knowledge base.
type storedKnowledgeBase struct {
	CreatedAt                    time.Time            `json:"createdAt"`
	UpdatedAt                    time.Time            `json:"updatedAt"`
	Configuration                map[string]any       `json:"configuration,omitempty"`
	AccessControlConfiguration   map[string]any       `json:"accessControlConfiguration,omitempty"`
	MediaExtractionConfiguration map[string]any       `json:"mediaExtractionConfiguration,omitempty"`
	KnowledgeBaseID              string               `json:"knowledgeBaseId"`
	Arn                          string               `json:"arn"`
	Name                         string               `json:"name"`
	Description                  string               `json:"description,omitempty"`
	DataSourceArn                string               `json:"dataSourceArn"`
	Status                       string               `json:"status"`
	PrimaryOwnerArn              string               `json:"primaryOwnerArn,omitempty"`
	Permissions                  []ResourcePermission `json:"permissions,omitempty"`
	DocumentCount                int64                `json:"documentCount"`
	SizeBytes                    int64                `json:"sizeBytes"`
	EmailNotificationOptedIn     bool                 `json:"emailNotificationOptedIn"`
}

func (k *storedKnowledgeBase) toKnowledgeBase() *KnowledgeBase {
	return &KnowledgeBase{
		CreatedAt:                    k.CreatedAt,
		UpdatedAt:                    k.UpdatedAt,
		Configuration:                k.Configuration,
		AccessControlConfiguration:   k.AccessControlConfiguration,
		MediaExtractionConfiguration: k.MediaExtractionConfiguration,
		KnowledgeBaseID:              k.KnowledgeBaseID,
		Arn:                          k.Arn,
		Name:                         k.Name,
		Description:                  k.Description,
		DataSourceArn:                k.DataSourceArn,
		Status:                       k.Status,
		PrimaryOwnerArn:              k.PrimaryOwnerArn,
		Permissions:                  clonePermissions(k.Permissions),
		DocumentCount:                k.DocumentCount,
		SizeBytes:                    k.SizeBytes,
		EmailNotificationOptedIn:     k.EmailNotificationOptedIn,
	}
}

// ---- Knowledge Bases ----

func (b *InMemoryBackend) CreateKnowledgeBase(
	accountID, knowledgeBaseID, name, description, dataSourceArn, primaryOwnerArn string,
	configuration, accessControlConfiguration, mediaExtractionConfiguration map[string]any,
	permissions []ResourcePermission,
	tags map[string]string,
) (*KnowledgeBase, error) {
	if knowledgeBaseID == "" || name == "" || dataSourceArn == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateKnowledgeBase")
	defer b.mu.Unlock()

	key := knowledgeBaseKey(accountID, knowledgeBaseID)
	if b.knowledgeBases.Has(key) {
		return nil, ErrKnowledgeBaseAlreadyExists
	}

	now := time.Now().UTC()
	arn := b.buildARN("knowledge-base", knowledgeBaseID)
	k := &storedKnowledgeBase{
		CreatedAt:                    now,
		UpdatedAt:                    now,
		Configuration:                configuration,
		AccessControlConfiguration:   accessControlConfiguration,
		MediaExtractionConfiguration: mediaExtractionConfiguration,
		KnowledgeBaseID:              knowledgeBaseID,
		Arn:                          arn,
		Name:                         name,
		Description:                  description,
		DataSourceArn:                dataSourceArn,
		Status:                       knowledgeBaseStatusActive,
		PrimaryOwnerArn:              primaryOwnerArn,
		Permissions:                  clonePermissions(permissions),
	}
	b.knowledgeBases.Put(k)

	if len(tags) > 0 {
		b.tags[arn] = maps.Clone(tags)
	}

	return k.toKnowledgeBase(), nil
}

func (b *InMemoryBackend) DescribeKnowledgeBase(accountID, knowledgeBaseID string) (*KnowledgeBase, error) {
	b.mu.RLock("DescribeKnowledgeBase")
	defer b.mu.RUnlock()

	k, ok := b.knowledgeBases.Get(knowledgeBaseKey(accountID, knowledgeBaseID))
	if !ok {
		return nil, ErrKnowledgeBaseNotFound
	}

	return k.toKnowledgeBase(), nil
}

func (b *InMemoryBackend) UpdateKnowledgeBase(
	accountID, knowledgeBaseID, name, description string,
	emailNotificationOptedIn *bool,
	configuration, accessControlConfiguration, mediaExtractionConfiguration map[string]any,
) (*KnowledgeBase, error) {
	b.mu.Lock("UpdateKnowledgeBase")
	defer b.mu.Unlock()

	key := knowledgeBaseKey(accountID, knowledgeBaseID)
	k, ok := b.knowledgeBases.Get(key)
	if !ok {
		return nil, ErrKnowledgeBaseNotFound
	}

	if name != "" {
		k.Name = name
	}
	if description != "" {
		k.Description = description
	}
	if emailNotificationOptedIn != nil {
		k.EmailNotificationOptedIn = *emailNotificationOptedIn
	}
	if configuration != nil {
		k.Configuration = configuration
	}
	if accessControlConfiguration != nil {
		k.AccessControlConfiguration = accessControlConfiguration
	}
	if mediaExtractionConfiguration != nil {
		k.MediaExtractionConfiguration = mediaExtractionConfiguration
	}
	k.UpdatedAt = time.Now().UTC()

	return k.toKnowledgeBase(), nil
}

func (b *InMemoryBackend) DeleteKnowledgeBase(accountID, knowledgeBaseID string) (*KnowledgeBase, error) {
	b.mu.Lock("DeleteKnowledgeBase")
	defer b.mu.Unlock()

	key := knowledgeBaseKey(accountID, knowledgeBaseID)
	k, ok := b.knowledgeBases.Get(key)
	if !ok {
		return nil, ErrKnowledgeBaseNotFound
	}

	delete(b.tags, k.Arn)
	b.knowledgeBases.Delete(key)

	return k.toKnowledgeBase(), nil
}

// BatchDeleteKnowledgeBase deletes each ID independently, mirroring the real
// op's per-item Deleted/Errors partition -- an ID this backend doesn't hold
// is a real per-item failure, not a whole-request error.
func (b *InMemoryBackend) BatchDeleteKnowledgeBase(
	accountID string,
	knowledgeBaseIDs []string,
) ([]KnowledgeBaseDeleteResult, []KnowledgeBaseDeleteError) {
	b.mu.Lock("BatchDeleteKnowledgeBase")
	defer b.mu.Unlock()

	var deleted []KnowledgeBaseDeleteResult
	var failed []KnowledgeBaseDeleteError

	for _, id := range knowledgeBaseIDs {
		key := knowledgeBaseKey(accountID, id)
		k, ok := b.knowledgeBases.Get(key)
		if !ok {
			failed = append(failed, KnowledgeBaseDeleteError{
				KnowledgeBaseID: id,
				ErrorCode:       errResourceNotFound,
				ErrorMessage:    "knowledge base not found",
			})

			continue
		}

		delete(b.tags, k.Arn)
		b.knowledgeBases.Delete(key)
		deleted = append(deleted, KnowledgeBaseDeleteResult{KnowledgeBaseID: k.KnowledgeBaseID, Arn: k.Arn})
	}

	return deleted, failed
}

func (b *InMemoryBackend) ListKnowledgeBases(
	_ string,
	maxResults int32,
	nextToken string,
) ([]*KnowledgeBase, string, error) {
	b.mu.RLock("ListKnowledgeBases")
	defer b.mu.RUnlock()

	all := b.knowledgeBases.All()
	sort.Slice(all, func(i, j int) bool { return all[i].KnowledgeBaseID < all[j].KnowledgeBaseID })

	result, next := paginateKnowledgeBases(all, maxResults, nextToken)

	return result, next, nil
}

func (b *InMemoryBackend) SearchKnowledgeBases(
	_ string,
	filters []SearchFilter,
	maxResults int32,
	nextToken string,
) ([]*KnowledgeBase, string, error) {
	b.mu.RLock("SearchKnowledgeBases")
	defer b.mu.RUnlock()

	var filtered []*storedKnowledgeBase
	for _, k := range b.knowledgeBases.All() {
		if matchesAllNameFilters(k.Name, filters, filterKnowledgeBaseName) {
			filtered = append(filtered, k)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].KnowledgeBaseID < filtered[j].KnowledgeBaseID })

	result, next := paginateKnowledgeBases(filtered, maxResults, nextToken)

	return result, next, nil
}

func paginateKnowledgeBases(
	all []*storedKnowledgeBase,
	maxResults int32,
	nextToken string,
) ([]*KnowledgeBase, string) {
	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		start = len(all)
		for i, k := range all {
			if k.KnowledgeBaseID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].KnowledgeBaseID
	} else {
		end = len(all)
	}

	result := make([]*KnowledgeBase, 0, end-start)
	for _, k := range all[start:end] {
		result = append(result, k.toKnowledgeBase())
	}

	return result, next
}

// ---- Knowledge Base permissions ----

func (b *InMemoryBackend) DescribeKnowledgeBasePermissions(
	accountID, knowledgeBaseID string,
) (*KnowledgeBase, []ResourcePermission, error) {
	b.mu.RLock("DescribeKnowledgeBasePermissions")
	defer b.mu.RUnlock()

	k, ok := b.knowledgeBases.Get(knowledgeBaseKey(accountID, knowledgeBaseID))
	if !ok {
		return nil, nil, ErrKnowledgeBaseNotFound
	}

	return k.toKnowledgeBase(), clonePermissions(k.Permissions), nil
}

func (b *InMemoryBackend) UpdateKnowledgeBasePermissions(
	accountID, knowledgeBaseID string,
	grant, revoke []ResourcePermission,
) (*KnowledgeBase, []ResourcePermission, error) {
	b.mu.Lock("UpdateKnowledgeBasePermissions")
	defer b.mu.Unlock()

	key := knowledgeBaseKey(accountID, knowledgeBaseID)
	k, ok := b.knowledgeBases.Get(key)
	if !ok {
		return nil, nil, ErrKnowledgeBaseNotFound
	}

	k.Permissions = applyGrantRevoke(k.Permissions, grant, revoke)
	k.UpdatedAt = time.Now().UTC()

	return k.toKnowledgeBase(), clonePermissions(k.Permissions), nil
}
