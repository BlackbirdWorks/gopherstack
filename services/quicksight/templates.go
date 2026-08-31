package quicksight

import (
	"maps"
	"slices"
	"sort"
	"strconv"
	"time"
)

const templateAliasLatest = "$LATEST"

// storedTemplateVersion is the persisted representation of one version of a
// QuickSight template.
type storedTemplateVersion struct {
	CreatedTime     time.Time      `json:"createdTime"`
	Definition      map[string]any `json:"definition,omitempty"`
	SourceEntityArn string         `json:"sourceEntityArn,omitempty"`
	Status          string         `json:"status"`
	Description     string         `json:"description,omitempty"`
	VersionNumber   int64          `json:"versionNumber"`
}

// storedTemplate is the persisted representation of a QuickSight template.
type storedTemplate struct {
	CreatedTime     time.Time                        `json:"createdTime"`
	LastUpdatedTime time.Time                        `json:"lastUpdatedTime"`
	TemplateID      string                           `json:"templateId"`
	Arn             string                           `json:"arn"`
	Name            string                           `json:"name"`
	Versions        map[int64]*storedTemplateVersion `json:"versions"`
	Aliases         map[string]int64                 `json:"aliases"`
	Permissions     []ResourcePermission             `json:"permissions"`
	LatestVersion   int64                            `json:"latestVersion"`
}

func (v *storedTemplateVersion) toTemplateVersion(templateArn string) *TemplateVersion {
	return &TemplateVersion{
		CreatedTime:     v.CreatedTime,
		Arn:             templateArn + "/version/" + strconv.FormatInt(v.VersionNumber, 10),
		Status:          v.Status,
		SourceEntityArn: v.SourceEntityArn,
		Description:     v.Description,
		VersionNumber:   v.VersionNumber,
		Definition:      v.Definition,
	}
}

func (t *storedTemplate) versionOrLatest(versionNumber int64) (*storedTemplateVersion, bool) {
	if versionNumber == 0 {
		versionNumber = t.LatestVersion
	}
	v, ok := t.Versions[versionNumber]

	return v, ok
}

func (t *storedTemplate) toTemplate(versionNumber int64) (*Template, bool) {
	v, ok := t.versionOrLatest(versionNumber)
	if !ok {
		return nil, false
	}

	return &Template{
		CreatedTime:     t.CreatedTime,
		LastUpdatedTime: t.LastUpdatedTime,
		TemplateID:      t.TemplateID,
		Arn:             t.Arn,
		Name:            t.Name,
		Version:         v.toTemplateVersion(t.Arn),
		Permissions:     clonePermissions(t.Permissions),
	}, true
}

// ---- Templates ----

func (b *InMemoryBackend) CreateTemplate(
	accountID, templateID, name, sourceEntityArn, versionDescription string,
	definition map[string]any,
	permissions []ResourcePermission,
	tags map[string]string,
) (*Template, error) {
	if templateID == "" || name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateTemplate")
	defer b.mu.Unlock()

	key := templateKey(accountID, templateID)
	if b.templates.Has(key) {
		return nil, ErrTemplateAlreadyExists
	}

	now := time.Now().UTC()
	arn := b.buildARN("template", templateID)
	t := &storedTemplate{
		CreatedTime:     now,
		LastUpdatedTime: now,
		TemplateID:      templateID,
		Arn:             arn,
		Name:            name,
		Versions: map[int64]*storedTemplateVersion{
			1: {
				CreatedTime:     now,
				VersionNumber:   1,
				Status:          statusCreationSuccessful,
				SourceEntityArn: sourceEntityArn,
				Definition:      definition,
				Description:     versionDescription,
			},
		},
		LatestVersion: 1,
		Aliases:       map[string]int64{templateAliasLatest: 1},
		Permissions:   clonePermissions(permissions),
	}
	b.templates.Put(t)

	if len(tags) > 0 {
		b.tags[arn] = maps.Clone(tags)
	}

	result, _ := t.toTemplate(0)

	return result, nil
}

func (b *InMemoryBackend) DescribeTemplate(accountID, templateID string, versionNumber int64) (*Template, error) {
	b.mu.RLock("DescribeTemplate")
	defer b.mu.RUnlock()

	t, ok := b.templates.Get(templateKey(accountID, templateID))
	if !ok {
		return nil, ErrTemplateNotFound
	}

	result, ok := t.toTemplate(versionNumber)
	if !ok {
		return nil, ErrTemplateNotFound
	}

	return result, nil
}

//nolint:dupl // update/delete functions share structure but operate on different stored types
func (b *InMemoryBackend) UpdateTemplate(
	accountID, templateID, name, sourceEntityArn, versionDescription string,
	definition map[string]any,
) (*Template, error) {
	b.mu.Lock("UpdateTemplate")
	defer b.mu.Unlock()

	key := templateKey(accountID, templateID)
	t, ok := b.templates.Get(key)
	if !ok {
		return nil, ErrTemplateNotFound
	}

	if name != "" {
		t.Name = name
	}

	prev, hasPrev := t.Versions[t.LatestVersion]
	if sourceEntityArn == "" && hasPrev {
		sourceEntityArn = prev.SourceEntityArn
	}
	if definition == nil && hasPrev {
		definition = prev.Definition
	}

	nextVersion := t.LatestVersion + 1
	now := time.Now().UTC()
	t.Versions[nextVersion] = &storedTemplateVersion{
		CreatedTime:     now,
		VersionNumber:   nextVersion,
		Status:          statusCreationSuccessful,
		SourceEntityArn: sourceEntityArn,
		Definition:      definition,
		Description:     versionDescription,
	}
	t.LatestVersion = nextVersion
	t.Aliases[templateAliasLatest] = nextVersion
	t.LastUpdatedTime = now

	result, _ := t.toTemplate(0)

	return result, nil
}

func (b *InMemoryBackend) DeleteTemplate(accountID, templateID string, versionNumber int64) error {
	b.mu.Lock("DeleteTemplate")
	defer b.mu.Unlock()

	key := templateKey(accountID, templateID)
	t, ok := b.templates.Get(key)
	if !ok {
		return ErrTemplateNotFound
	}

	if versionNumber == 0 {
		delete(b.tags, t.Arn)
		b.templates.Delete(key)

		return nil
	}

	if _, exists := t.Versions[versionNumber]; !exists {
		return ErrTemplateNotFound
	}
	delete(t.Versions, versionNumber)

	if len(t.Versions) == 0 {
		delete(b.tags, t.Arn)
		b.templates.Delete(key)

		return nil
	}

	if versionNumber == t.LatestVersion {
		var maxVer int64
		for v := range t.Versions {
			if v > maxVer {
				maxVer = v
			}
		}
		t.LatestVersion = maxVer
		t.Aliases[templateAliasLatest] = maxVer
	}

	return nil
}

func (b *InMemoryBackend) allTemplatesLocked(_ string) []*storedTemplate {
	all := b.templates.All()
	sort.Slice(all, func(i, j int) bool { return all[i].TemplateID < all[j].TemplateID })

	return all
}

func (b *InMemoryBackend) ListTemplates(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*Template, string, error) {
	b.mu.RLock("ListTemplates")
	defer b.mu.RUnlock()

	all := b.allTemplatesLocked(accountID)

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		if off, err := decodePageToken(nextToken); err == nil {
			start = off
		}
	}
	// A token issued before items were deleted can name an offset past the
	// current end -- clamp instead of letting all[start:end] panic.
	if start > len(all) {
		start = len(all)
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = encodePageToken(end)
	} else {
		end = len(all)
	}

	result := make([]*Template, 0, end-start)
	for _, t := range all[start:end] {
		tmpl, _ := t.toTemplate(0)
		result = append(result, tmpl)
	}

	return result, next, nil
}

func (b *InMemoryBackend) ListTemplateVersions(
	accountID, templateID string,
	maxResults int32,
	nextToken string,
) ([]*TemplateVersion, string, error) {
	b.mu.RLock("ListTemplateVersions")
	defer b.mu.RUnlock()

	t, ok := b.templates.Get(templateKey(accountID, templateID))
	if !ok {
		return nil, "", ErrTemplateNotFound
	}

	versions := make([]int64, 0, len(t.Versions))
	for v := range t.Versions {
		versions = append(versions, v)
	}
	slices.Sort(versions)

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		start = len(versions)
		if parsed, err := strconv.ParseInt(nextToken, 10, 64); err == nil {
			for i, v := range versions {
				if v == parsed {
					start = i

					break
				}
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(versions) {
		next = strconv.FormatInt(versions[end], 10)
	} else {
		end = len(versions)
	}

	result := make([]*TemplateVersion, 0, end-start)
	for _, v := range versions[start:end] {
		result = append(result, t.Versions[v].toTemplateVersion(t.Arn))
	}

	return result, next, nil
}

// ---- Template permissions ----

func (b *InMemoryBackend) DescribeTemplatePermissions(
	accountID, templateID string,
) (*Template, []ResourcePermission, error) {
	b.mu.RLock("DescribeTemplatePermissions")
	defer b.mu.RUnlock()

	t, ok := b.templates.Get(templateKey(accountID, templateID))
	if !ok {
		return nil, nil, ErrTemplateNotFound
	}

	result, _ := t.toTemplate(0)

	return result, clonePermissions(t.Permissions), nil
}

func (b *InMemoryBackend) UpdateTemplatePermissions(
	accountID, templateID string,
	grant, revoke []ResourcePermission,
) (*Template, []ResourcePermission, error) {
	b.mu.Lock("UpdateTemplatePermissions")
	defer b.mu.Unlock()

	t, ok := b.templates.Get(templateKey(accountID, templateID))
	if !ok {
		return nil, nil, ErrTemplateNotFound
	}

	t.Permissions = applyGrantRevoke(t.Permissions, grant, revoke)
	t.LastUpdatedTime = time.Now().UTC()

	result, _ := t.toTemplate(0)

	return result, clonePermissions(t.Permissions), nil
}

// ---- Template aliases ----

func (b *InMemoryBackend) CreateTemplateAlias(
	accountID, templateID, aliasName string,
	versionNumber int64,
) (*TemplateAlias, error) {
	if aliasName == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateTemplateAlias")
	defer b.mu.Unlock()

	t, ok := b.templates.Get(templateKey(accountID, templateID))
	if !ok {
		return nil, ErrTemplateNotFound
	}

	if _, exists := t.Aliases[aliasName]; exists {
		return nil, ErrTemplateAliasAlreadyExists
	}

	if _, exists := t.Versions[versionNumber]; !exists {
		return nil, ErrTemplateNotFound
	}

	t.Aliases[aliasName] = versionNumber

	return &TemplateAlias{
		AliasName:             aliasName,
		Arn:                   t.Arn + "/alias/" + aliasName,
		TemplateVersionNumber: versionNumber,
	}, nil
}

func (b *InMemoryBackend) DescribeTemplateAlias(accountID, templateID, aliasName string) (*TemplateAlias, error) {
	b.mu.RLock("DescribeTemplateAlias")
	defer b.mu.RUnlock()

	t, ok := b.templates.Get(templateKey(accountID, templateID))
	if !ok {
		return nil, ErrTemplateNotFound
	}

	version, ok := t.Aliases[aliasName]
	if !ok {
		return nil, ErrTemplateAliasNotFound
	}

	return &TemplateAlias{
		AliasName:             aliasName,
		Arn:                   t.Arn + "/alias/" + aliasName,
		TemplateVersionNumber: version,
	}, nil
}

func (b *InMemoryBackend) UpdateTemplateAlias(
	accountID, templateID, aliasName string,
	versionNumber int64,
) (*TemplateAlias, error) {
	b.mu.Lock("UpdateTemplateAlias")
	defer b.mu.Unlock()

	t, ok := b.templates.Get(templateKey(accountID, templateID))
	if !ok {
		return nil, ErrTemplateNotFound
	}

	if _, exists := t.Aliases[aliasName]; !exists {
		return nil, ErrTemplateAliasNotFound
	}

	if _, exists := t.Versions[versionNumber]; !exists {
		return nil, ErrTemplateNotFound
	}

	t.Aliases[aliasName] = versionNumber

	return &TemplateAlias{
		AliasName:             aliasName,
		Arn:                   t.Arn + "/alias/" + aliasName,
		TemplateVersionNumber: versionNumber,
	}, nil
}

func (b *InMemoryBackend) DeleteTemplateAlias(accountID, templateID, aliasName string) error {
	b.mu.Lock("DeleteTemplateAlias")
	defer b.mu.Unlock()

	t, ok := b.templates.Get(templateKey(accountID, templateID))
	if !ok {
		return ErrTemplateNotFound
	}

	if _, exists := t.Aliases[aliasName]; !exists {
		return ErrTemplateAliasNotFound
	}

	delete(t.Aliases, aliasName)

	return nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListTemplateAliases(
	accountID, templateID string,
	maxResults int32,
	nextToken string,
) ([]*TemplateAlias, string, error) {
	b.mu.RLock("ListTemplateAliases")
	defer b.mu.RUnlock()

	t, ok := b.templates.Get(templateKey(accountID, templateID))
	if !ok {
		return nil, "", ErrTemplateNotFound
	}

	names := make([]string, 0, len(t.Aliases))
	for name := range t.Aliases {
		names = append(names, name)
	}
	sort.Strings(names)

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		start = len(names)
		for i, name := range names {
			if name == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(names) {
		next = names[end]
	} else {
		end = len(names)
	}

	result := make([]*TemplateAlias, 0, end-start)
	for _, name := range names[start:end] {
		result = append(result, &TemplateAlias{
			AliasName:             name,
			Arn:                   t.Arn + "/alias/" + name,
			TemplateVersionNumber: t.Aliases[name],
		})
	}

	return result, next, nil
}
