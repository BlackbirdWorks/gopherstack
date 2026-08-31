package quicksight

import (
	"maps"
	"slices"
	"sort"
	"strconv"
	"time"
)

const (
	themeAliasLatest = "$LATEST"
	themeTypeCustom  = "CUSTOM"
	themeTypeAll     = "ALL"
)

// storedThemeVersion is the persisted representation of one version of a
// QuickSight theme.
type storedThemeVersion struct {
	CreatedTime   time.Time      `json:"createdTime"`
	Configuration map[string]any `json:"configuration,omitempty"`
	BaseThemeID   string         `json:"baseThemeId,omitempty"`
	Status        string         `json:"status"`
	Description   string         `json:"description,omitempty"`
	VersionNumber int64          `json:"versionNumber"`
}

// storedTheme is the persisted representation of a QuickSight theme.
type storedTheme struct {
	CreatedTime     time.Time                     `json:"createdTime"`
	LastUpdatedTime time.Time                     `json:"lastUpdatedTime"`
	ThemeID         string                        `json:"themeId"`
	Arn             string                        `json:"arn"`
	Name            string                        `json:"name"`
	Type            string                        `json:"type"`
	Versions        map[int64]*storedThemeVersion `json:"versions"`
	Aliases         map[string]int64              `json:"aliases"`
	Permissions     []ResourcePermission          `json:"permissions"`
	LatestVersion   int64                         `json:"latestVersion"`
}

func (v *storedThemeVersion) toThemeVersion(themeArn string) *ThemeVersion {
	return &ThemeVersion{
		CreatedTime:   v.CreatedTime,
		Arn:           themeArn + "/version/" + strconv.FormatInt(v.VersionNumber, 10),
		Status:        v.Status,
		BaseThemeID:   v.BaseThemeID,
		Description:   v.Description,
		VersionNumber: v.VersionNumber,
		Configuration: v.Configuration,
	}
}

func (t *storedTheme) versionOrLatest(versionNumber int64) (*storedThemeVersion, bool) {
	if versionNumber == 0 {
		versionNumber = t.LatestVersion
	}
	v, ok := t.Versions[versionNumber]

	return v, ok
}

func (t *storedTheme) toTheme(versionNumber int64) (*Theme, bool) {
	v, ok := t.versionOrLatest(versionNumber)
	if !ok {
		return nil, false
	}

	return &Theme{
		CreatedTime:     t.CreatedTime,
		LastUpdatedTime: t.LastUpdatedTime,
		ThemeID:         t.ThemeID,
		Arn:             t.Arn,
		Name:            t.Name,
		Type:            t.Type,
		Version:         v.toThemeVersion(t.Arn),
		Permissions:     clonePermissions(t.Permissions),
	}, true
}

// ---- Themes ----

func (b *InMemoryBackend) CreateTheme(
	accountID, themeID, name, baseThemeID, versionDescription string,
	configuration map[string]any,
	permissions []ResourcePermission,
	tags map[string]string,
) (*Theme, error) {
	if themeID == "" || name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateTheme")
	defer b.mu.Unlock()

	key := themeKey(accountID, themeID)
	if b.themes.Has(key) {
		return nil, ErrThemeAlreadyExists
	}

	now := time.Now().UTC()
	arn := b.buildARN("theme", themeID)
	t := &storedTheme{
		CreatedTime:     now,
		LastUpdatedTime: now,
		ThemeID:         themeID,
		Arn:             arn,
		Name:            name,
		Type:            themeTypeCustom,
		Versions: map[int64]*storedThemeVersion{
			1: {
				CreatedTime:   now,
				VersionNumber: 1,
				Status:        statusCreationSuccessful,
				BaseThemeID:   baseThemeID,
				Configuration: configuration,
				Description:   versionDescription,
			},
		},
		LatestVersion: 1,
		Aliases:       map[string]int64{themeAliasLatest: 1},
		Permissions:   clonePermissions(permissions),
	}
	b.themes.Put(t)

	if len(tags) > 0 {
		b.tags[arn] = maps.Clone(tags)
	}

	result, _ := t.toTheme(0)

	return result, nil
}

func (b *InMemoryBackend) DescribeTheme(accountID, themeID string, versionNumber int64) (*Theme, error) {
	b.mu.RLock("DescribeTheme")
	defer b.mu.RUnlock()

	t, ok := b.themes.Get(themeKey(accountID, themeID))
	if !ok {
		return nil, ErrThemeNotFound
	}

	result, ok := t.toTheme(versionNumber)
	if !ok {
		return nil, ErrThemeNotFound
	}

	return result, nil
}

//nolint:dupl // update/delete functions share structure but operate on different stored types
func (b *InMemoryBackend) UpdateTheme(
	accountID, themeID, name, baseThemeID, versionDescription string,
	configuration map[string]any,
) (*Theme, error) {
	b.mu.Lock("UpdateTheme")
	defer b.mu.Unlock()

	key := themeKey(accountID, themeID)
	t, ok := b.themes.Get(key)
	if !ok {
		return nil, ErrThemeNotFound
	}

	if name != "" {
		t.Name = name
	}

	prev, hasPrev := t.Versions[t.LatestVersion]
	if baseThemeID == "" && hasPrev {
		baseThemeID = prev.BaseThemeID
	}
	if configuration == nil && hasPrev {
		configuration = prev.Configuration
	}

	nextVersion := t.LatestVersion + 1
	now := time.Now().UTC()
	t.Versions[nextVersion] = &storedThemeVersion{
		CreatedTime:   now,
		VersionNumber: nextVersion,
		Status:        statusCreationSuccessful,
		BaseThemeID:   baseThemeID,
		Configuration: configuration,
		Description:   versionDescription,
	}
	t.LatestVersion = nextVersion
	t.Aliases[themeAliasLatest] = nextVersion
	t.LastUpdatedTime = now

	result, _ := t.toTheme(0)

	return result, nil
}

func (b *InMemoryBackend) DeleteTheme(accountID, themeID string, versionNumber int64) error {
	b.mu.Lock("DeleteTheme")
	defer b.mu.Unlock()

	key := themeKey(accountID, themeID)
	t, ok := b.themes.Get(key)
	if !ok {
		return ErrThemeNotFound
	}

	if versionNumber == 0 {
		delete(b.tags, t.Arn)
		b.themes.Delete(key)

		return nil
	}

	if _, exists := t.Versions[versionNumber]; !exists {
		return ErrThemeNotFound
	}
	delete(t.Versions, versionNumber)

	if len(t.Versions) == 0 {
		delete(b.tags, t.Arn)
		b.themes.Delete(key)

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
		t.Aliases[themeAliasLatest] = maxVer
	}

	return nil
}

func (b *InMemoryBackend) allThemesLocked(_ string) []*storedTheme {
	all := b.themes.All()
	sort.Slice(all, func(i, j int) bool { return all[i].ThemeID < all[j].ThemeID })

	return all
}

// ListThemes filters by themeType (the raw "type" query value: "ALL",
// "CUSTOM", or "QUICKSIGHT" per ListThemesInput.Type -- api_op_ListThemes.go
// documents ALL as the default, i.e. no filter). Empty or "ALL" applies no
// filter; anything else must match a theme's own Type exactly.
func (b *InMemoryBackend) ListThemes(
	accountID, themeType string,
	maxResults int32,
	nextToken string,
) ([]*Theme, string, error) {
	b.mu.RLock("ListThemes")
	defer b.mu.RUnlock()

	all := b.allThemesLocked(accountID)
	if themeType != "" && themeType != themeTypeAll {
		filtered := all[:0:0]
		for _, t := range all {
			if t.Type == themeType {
				filtered = append(filtered, t)
			}
		}
		all = filtered
	}

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

	result := make([]*Theme, 0, end-start)
	for _, t := range all[start:end] {
		theme, _ := t.toTheme(0)
		result = append(result, theme)
	}

	return result, next, nil
}

func (b *InMemoryBackend) ListThemeVersions(
	accountID, themeID string,
	maxResults int32,
	nextToken string,
) ([]*ThemeVersion, string, error) {
	b.mu.RLock("ListThemeVersions")
	defer b.mu.RUnlock()

	t, ok := b.themes.Get(themeKey(accountID, themeID))
	if !ok {
		return nil, "", ErrThemeNotFound
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

	result := make([]*ThemeVersion, 0, end-start)
	for _, v := range versions[start:end] {
		result = append(result, t.Versions[v].toThemeVersion(t.Arn))
	}

	return result, next, nil
}

// ---- Theme permissions ----

func (b *InMemoryBackend) DescribeThemePermissions(accountID, themeID string) (*Theme, []ResourcePermission, error) {
	b.mu.RLock("DescribeThemePermissions")
	defer b.mu.RUnlock()

	t, ok := b.themes.Get(themeKey(accountID, themeID))
	if !ok {
		return nil, nil, ErrThemeNotFound
	}

	result, _ := t.toTheme(0)

	return result, clonePermissions(t.Permissions), nil
}

func (b *InMemoryBackend) UpdateThemePermissions(
	accountID, themeID string,
	grant, revoke []ResourcePermission,
) (*Theme, []ResourcePermission, error) {
	b.mu.Lock("UpdateThemePermissions")
	defer b.mu.Unlock()

	t, ok := b.themes.Get(themeKey(accountID, themeID))
	if !ok {
		return nil, nil, ErrThemeNotFound
	}

	t.Permissions = applyGrantRevoke(t.Permissions, grant, revoke)
	t.LastUpdatedTime = time.Now().UTC()

	result, _ := t.toTheme(0)

	return result, clonePermissions(t.Permissions), nil
}

// ---- Theme aliases ----

func (b *InMemoryBackend) CreateThemeAlias(
	accountID, themeID, aliasName string,
	versionNumber int64,
) (*ThemeAlias, error) {
	if aliasName == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateThemeAlias")
	defer b.mu.Unlock()

	t, ok := b.themes.Get(themeKey(accountID, themeID))
	if !ok {
		return nil, ErrThemeNotFound
	}

	if _, exists := t.Aliases[aliasName]; exists {
		return nil, ErrThemeAliasAlreadyExists
	}

	if _, exists := t.Versions[versionNumber]; !exists {
		return nil, ErrThemeNotFound
	}

	t.Aliases[aliasName] = versionNumber

	return &ThemeAlias{
		AliasName:          aliasName,
		Arn:                t.Arn + "/alias/" + aliasName,
		ThemeVersionNumber: versionNumber,
	}, nil
}

func (b *InMemoryBackend) DescribeThemeAlias(accountID, themeID, aliasName string) (*ThemeAlias, error) {
	b.mu.RLock("DescribeThemeAlias")
	defer b.mu.RUnlock()

	t, ok := b.themes.Get(themeKey(accountID, themeID))
	if !ok {
		return nil, ErrThemeNotFound
	}

	version, ok := t.Aliases[aliasName]
	if !ok {
		return nil, ErrThemeAliasNotFound
	}

	return &ThemeAlias{
		AliasName:          aliasName,
		Arn:                t.Arn + "/alias/" + aliasName,
		ThemeVersionNumber: version,
	}, nil
}

func (b *InMemoryBackend) UpdateThemeAlias(
	accountID, themeID, aliasName string,
	versionNumber int64,
) (*ThemeAlias, error) {
	b.mu.Lock("UpdateThemeAlias")
	defer b.mu.Unlock()

	t, ok := b.themes.Get(themeKey(accountID, themeID))
	if !ok {
		return nil, ErrThemeNotFound
	}

	if _, exists := t.Aliases[aliasName]; !exists {
		return nil, ErrThemeAliasNotFound
	}

	if _, exists := t.Versions[versionNumber]; !exists {
		return nil, ErrThemeNotFound
	}

	t.Aliases[aliasName] = versionNumber

	return &ThemeAlias{
		AliasName:          aliasName,
		Arn:                t.Arn + "/alias/" + aliasName,
		ThemeVersionNumber: versionNumber,
	}, nil
}

func (b *InMemoryBackend) DeleteThemeAlias(accountID, themeID, aliasName string) error {
	b.mu.Lock("DeleteThemeAlias")
	defer b.mu.Unlock()

	t, ok := b.themes.Get(themeKey(accountID, themeID))
	if !ok {
		return ErrThemeNotFound
	}

	if _, exists := t.Aliases[aliasName]; !exists {
		return ErrThemeAliasNotFound
	}

	delete(t.Aliases, aliasName)

	return nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListThemeAliases(
	accountID, themeID string,
	maxResults int32,
	nextToken string,
) ([]*ThemeAlias, string, error) {
	b.mu.RLock("ListThemeAliases")
	defer b.mu.RUnlock()

	t, ok := b.themes.Get(themeKey(accountID, themeID))
	if !ok {
		return nil, "", ErrThemeNotFound
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

	result := make([]*ThemeAlias, 0, end-start)
	for _, name := range names[start:end] {
		result = append(result, &ThemeAlias{
			AliasName:          name,
			Arn:                t.Arn + "/alias/" + name,
			ThemeVersionNumber: t.Aliases[name],
		})
	}

	return result, next, nil
}
