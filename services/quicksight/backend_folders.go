package quicksight

import (
	"maps"
	"sort"
	"strings"
	"time"
)

const (
	folderTypeShared     = "SHARED"
	folderTypeRestricted = "RESTRICTED"

	folderMemberTypeDashboard = "DASHBOARD"
	folderMemberTypeAnalysis  = "ANALYSIS"
	folderMemberTypeDataSet   = "DATASET"

	folderArnMarker = ":folder/"

	filterParentFolderArn = "PARENT_FOLDER_ARN"
	filterFolderName      = "FOLDER_NAME"
	filterDashboardName   = "DASHBOARD_NAME"
	filterAnalysisName    = "ANALYSIS_NAME"
	filterDataSetName     = "DATASET_NAME"
	filterDataSourceName  = "DATASOURCE_NAME"

	filterOperatorStringEquals = "StringEquals"
	filterOperatorStringLike   = "StringLike"
)

// matchesNameFilter reports whether name matches a single SearchFilter whose
// Name is nameFilterKey (e.g. "DASHBOARD_NAME"). Filters with any other Name
// are ownership-related (QUICKSIGHT_OWNER, DIRECT_QUICKSIGHT_OWNER, etc.) that
// this in-memory backend doesn't track principals for, so they pass through,
// mirroring folderMatchesFilter's permissive default for unknown filter names.
func matchesNameFilter(name string, filter SearchFilter, nameFilterKey string) bool {
	if filter.Name != nameFilterKey {
		return true
	}

	switch filter.Operator {
	case filterOperatorStringLike:
		return strings.Contains(name, filter.Value)
	default: // StringEquals and unset operators default to equality.
		return name == filter.Value
	}
}

// matchesAllNameFilters reports whether name satisfies every filter in
// filters (AND semantics), evaluated via matchesNameFilter.
func matchesAllNameFilters(name string, filters []SearchFilter, nameFilterKey string) bool {
	for _, filter := range filters {
		if !matchesNameFilter(name, filter, nameFilterKey) {
			return false
		}
	}

	return true
}

// storedFolder is the persisted representation of a QuickSight folder.
type storedFolder struct {
	CreatedTime     time.Time            `json:"createdTime"`
	LastUpdatedTime time.Time            `json:"lastUpdatedTime"`
	FolderID        string               `json:"folderId"`
	Arn             string               `json:"arn"`
	Name            string               `json:"name"`
	FolderType      string               `json:"folderType"`
	ParentFolderArn string               `json:"parentFolderArn"`
	Permissions     []ResourcePermission `json:"permissions"`
}

func (f *storedFolder) toFolder() *Folder {
	return &Folder{
		CreatedTime:     f.CreatedTime,
		LastUpdatedTime: f.LastUpdatedTime,
		FolderID:        f.FolderID,
		Arn:             f.Arn,
		Name:            f.Name,
		FolderType:      f.FolderType,
		ParentFolderArn: f.ParentFolderArn,
		Permissions:     clonePermissions(f.Permissions),
	}
}

// storedFolderMember is the persisted representation of a folder membership.
type storedFolderMember struct {
	FolderID   string `json:"folderId"`
	MemberID   string `json:"memberId"`
	MemberType string `json:"memberType"`
}

func (m *storedFolderMember) toFolderMember() *FolderMember {
	return &FolderMember{
		MemberID:   m.MemberID,
		MemberType: m.MemberType,
	}
}

func isValidFolderType(folderType string) bool {
	switch folderType {
	case folderTypeShared, folderTypeRestricted:
		return true
	}

	return false
}

func isValidFolderMemberType(memberType string) bool {
	switch memberType {
	case folderMemberTypeDashboard, folderMemberTypeAnalysis, folderMemberTypeDataSet:
		return true
	}

	return false
}

func clonePermissions(perms []ResourcePermission) []ResourcePermission {
	if len(perms) == 0 {
		return nil
	}

	out := make([]ResourcePermission, len(perms))
	for i, p := range perms {
		actions := make([]string, len(p.Actions))
		copy(actions, p.Actions)
		out[i] = ResourcePermission{Principal: p.Principal, Actions: actions}
	}

	return out
}

// applyGrantRevoke merges grant into existing (union of actions per principal), then
// removes revoke actions from the result. Principals left with no actions are dropped.
func applyGrantRevoke(existing, grant, revoke []ResourcePermission) []ResourcePermission {
	byPrincipal := make(map[string]map[string]struct{})
	var order []string

	ensure := func(principal string) map[string]struct{} {
		set, ok := byPrincipal[principal]
		if !ok {
			set = make(map[string]struct{})
			byPrincipal[principal] = set
			order = append(order, principal)
		}

		return set
	}

	for _, p := range existing {
		set := ensure(p.Principal)
		for _, a := range p.Actions {
			set[a] = struct{}{}
		}
	}

	for _, p := range grant {
		set := ensure(p.Principal)
		for _, a := range p.Actions {
			set[a] = struct{}{}
		}
	}

	for _, p := range revoke {
		if set, ok := byPrincipal[p.Principal]; ok {
			for _, a := range p.Actions {
				delete(set, a)
			}
		}
	}

	result := make([]ResourcePermission, 0, len(order))
	for _, principal := range order {
		set := byPrincipal[principal]
		if len(set) == 0 {
			continue
		}

		actions := make([]string, 0, len(set))
		for a := range set {
			actions = append(actions, a)
		}
		sort.Strings(actions)

		result = append(result, ResourcePermission{Principal: principal, Actions: actions})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Principal < result[j].Principal })

	return result
}

// folderIDFromArn extracts the FolderId component of a folder ARN, or "" if arn
// does not look like a folder ARN.
func folderIDFromArn(arn string) string {
	idx := strings.LastIndex(arn, folderArnMarker)
	if idx < 0 {
		return ""
	}

	return arn[idx+len(folderArnMarker):]
}

// ---- Folders ----

func (b *InMemoryBackend) CreateFolder(
	accountID, folderID, name, folderType, parentFolderArn string,
	permissions []ResourcePermission,
	tags map[string]string,
) (*Folder, error) {
	if folderID == "" || name == "" {
		return nil, ErrValidation
	}

	if folderType == "" {
		folderType = folderTypeShared
	}
	if !isValidFolderType(folderType) {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateFolder")
	defer b.mu.Unlock()

	key := folderKey(accountID, folderID)
	if _, exists := b.folders[key]; exists {
		return nil, ErrFolderAlreadyExists
	}

	now := time.Now().UTC()
	f := &storedFolder{
		CreatedTime:     now,
		LastUpdatedTime: now,
		FolderID:        folderID,
		Arn:             b.buildARN("folder", folderID),
		Name:            name,
		FolderType:      folderType,
		ParentFolderArn: parentFolderArn,
		Permissions:     clonePermissions(permissions),
	}
	b.folders[key] = f

	if len(tags) > 0 {
		b.tags[f.Arn] = maps.Clone(tags)
	}

	return f.toFolder(), nil
}

func (b *InMemoryBackend) DescribeFolder(accountID, folderID string) (*Folder, error) {
	b.mu.RLock("DescribeFolder")
	defer b.mu.RUnlock()

	f, ok := b.folders[folderKey(accountID, folderID)]
	if !ok {
		return nil, ErrFolderNotFound
	}

	return f.toFolder(), nil
}

func (b *InMemoryBackend) UpdateFolder(accountID, folderID, name string) (*Folder, error) {
	b.mu.Lock("UpdateFolder")
	defer b.mu.Unlock()

	key := folderKey(accountID, folderID)
	f, ok := b.folders[key]
	if !ok {
		return nil, ErrFolderNotFound
	}

	if name != "" {
		f.Name = name
	}
	f.LastUpdatedTime = time.Now().UTC()

	return f.toFolder(), nil
}

func (b *InMemoryBackend) DeleteFolder(accountID, folderID string) error {
	b.mu.Lock("DeleteFolder")
	defer b.mu.Unlock()

	key := folderKey(accountID, folderID)
	f, ok := b.folders[key]
	if !ok {
		return ErrFolderNotFound
	}

	delete(b.tags, f.Arn)
	delete(b.folders, key)

	memberPrefix := accountID + "/" + folderID + "/"
	for k := range b.folderMembers {
		if strings.HasPrefix(k, memberPrefix) {
			delete(b.folderMembers, k)
		}
	}

	return nil
}

func (b *InMemoryBackend) allFoldersLocked(accountID string) []*storedFolder {
	prefix := accountID + "/"
	all := make([]*storedFolder, 0, len(b.folders))
	for k, f := range b.folders {
		if strings.HasPrefix(k, prefix) {
			all = append(all, f)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].FolderID < all[j].FolderID })

	return all
}

func paginateFolders(all []*storedFolder, maxResults int32, nextToken string) ([]*Folder, string) {
	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		if off, err := decodePageToken(nextToken); err == nil {
			start = off
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = encodePageToken(end)
	} else {
		end = len(all)
	}

	result := make([]*Folder, 0, end-start)
	for _, f := range all[start:end] {
		result = append(result, f.toFolder())
	}

	return result, next
}

func (b *InMemoryBackend) ListFolders(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*Folder, string, error) {
	b.mu.RLock("ListFolders")
	defer b.mu.RUnlock()

	result, next := paginateFolders(b.allFoldersLocked(accountID), maxResults, nextToken)

	return result, next, nil
}

func folderMatchesFilter(f *storedFolder, filter FolderSearchFilter) bool {
	var actual string

	switch filter.Name {
	case filterParentFolderArn:
		actual = f.ParentFolderArn
	case filterFolderName:
		actual = f.Name
	default:
		return true
	}

	switch filter.Operator {
	case filterOperatorStringLike:
		return strings.Contains(actual, filter.Value)
	default: // StringEquals and unset operators default to equality.
		return actual == filter.Value
	}
}

func (b *InMemoryBackend) SearchFolders(
	accountID string,
	filters []FolderSearchFilter,
	maxResults int32,
	nextToken string,
) ([]*Folder, string, error) {
	b.mu.RLock("SearchFolders")
	defer b.mu.RUnlock()

	all := b.allFoldersLocked(accountID)

	var filtered []*storedFolder
	for _, f := range all {
		matches := true
		for _, filter := range filters {
			if !folderMatchesFilter(f, filter) {
				matches = false

				break
			}
		}
		if matches {
			filtered = append(filtered, f)
		}
	}

	result, next := paginateFolders(filtered, maxResults, nextToken)

	return result, next, nil
}

// ---- Folder memberships ----

func (b *InMemoryBackend) CreateFolderMembership(
	accountID, folderID, memberID, memberType string,
) (*FolderMember, error) {
	if memberID == "" || !isValidFolderMemberType(memberType) {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateFolderMembership")
	defer b.mu.Unlock()

	if _, ok := b.folders[folderKey(accountID, folderID)]; !ok {
		return nil, ErrFolderNotFound
	}

	m := &storedFolderMember{
		FolderID:   folderID,
		MemberID:   memberID,
		MemberType: memberType,
	}
	b.folderMembers[folderMemberKey(accountID, folderID, memberType, memberID)] = m

	return m.toFolderMember(), nil
}

func (b *InMemoryBackend) DeleteFolderMembership(accountID, folderID, memberID, memberType string) error {
	b.mu.Lock("DeleteFolderMembership")
	defer b.mu.Unlock()

	key := folderMemberKey(accountID, folderID, memberType, memberID)
	if _, ok := b.folderMembers[key]; !ok {
		return ErrFolderMemberNotFound
	}

	delete(b.folderMembers, key)

	return nil
}

func (b *InMemoryBackend) ListFolderMembers(
	accountID, folderID string,
	maxResults int32,
	nextToken string,
) ([]*FolderMember, string, error) {
	b.mu.RLock("ListFolderMembers")
	defer b.mu.RUnlock()

	if _, ok := b.folders[folderKey(accountID, folderID)]; !ok {
		return nil, "", ErrFolderNotFound
	}

	prefix := accountID + "/" + folderID + "/"
	var all []*storedFolderMember
	for k, m := range b.folderMembers {
		if strings.HasPrefix(k, prefix) {
			all = append(all, m)
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].MemberType != all[j].MemberType {
			return all[i].MemberType < all[j].MemberType
		}

		return all[i].MemberID < all[j].MemberID
	})

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, m := range all {
			if m.MemberType+"/"+m.MemberID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].MemberType + "/" + all[end].MemberID
	} else {
		end = len(all)
	}

	result := make([]*FolderMember, 0, end-start)
	for _, m := range all[start:end] {
		result = append(result, m.toFolderMember())
	}

	return result, next, nil
}

// ---- Folder permissions ----

func (b *InMemoryBackend) DescribeFolderPermissions(accountID, folderID string) ([]ResourcePermission, error) {
	b.mu.RLock("DescribeFolderPermissions")
	defer b.mu.RUnlock()

	f, ok := b.folders[folderKey(accountID, folderID)]
	if !ok {
		return nil, ErrFolderNotFound
	}

	return clonePermissions(f.Permissions), nil
}

func (b *InMemoryBackend) UpdateFolderPermissions(
	accountID, folderID string,
	grant, revoke []ResourcePermission,
) ([]ResourcePermission, error) {
	b.mu.Lock("UpdateFolderPermissions")
	defer b.mu.Unlock()

	f, ok := b.folders[folderKey(accountID, folderID)]
	if !ok {
		return nil, ErrFolderNotFound
	}

	f.Permissions = applyGrantRevoke(f.Permissions, grant, revoke)
	f.LastUpdatedTime = time.Now().UTC()

	return clonePermissions(f.Permissions), nil
}

func (b *InMemoryBackend) DescribeFolderResolvedPermissions(
	accountID, folderID string,
) ([]ResourcePermission, error) {
	b.mu.RLock("DescribeFolderResolvedPermissions")
	defer b.mu.RUnlock()

	f, ok := b.folders[folderKey(accountID, folderID)]
	if !ok {
		return nil, ErrFolderNotFound
	}

	result := clonePermissions(f.Permissions)
	parentArn := f.ParentFolderArn
	visited := map[string]bool{folderID: true}

	for parentArn != "" {
		parentID := folderIDFromArn(parentArn)
		if parentID == "" || visited[parentID] {
			break
		}
		visited[parentID] = true

		parent, foundParent := b.folders[folderKey(accountID, parentID)]
		if !foundParent {
			break
		}

		result = applyGrantRevoke(result, parent.Permissions, nil)
		parentArn = parent.ParentFolderArn
	}

	return result, nil
}

// ---- Folders-for-resource ----

// arnFieldCount is the number of colon-delimited fields in a QuickSight
// resource ARN, e.g. "arn:aws:quicksight:us-east-1:123456789012:dashboard/abc".
const arnFieldCount = 6

// resourceArnToMember parses a QuickSight resource ARN into the account ID
// and the folder-member type/ID used to key folder memberships.
func resourceArnToMember(arn string) (string, string, string, bool) {
	parts := strings.SplitN(arn, ":", arnFieldCount)
	if len(parts) != arnFieldCount {
		return "", "", "", false
	}

	accountID := parts[4]

	resType, resID, found := strings.Cut(parts[5], "/")
	if !found {
		return "", "", "", false
	}

	var memberType string
	switch resType {
	case "dashboard":
		memberType = folderMemberTypeDashboard
	case "analysis":
		memberType = folderMemberTypeAnalysis
	case "dataset":
		memberType = folderMemberTypeDataSet
	default:
		return "", "", "", false
	}

	return accountID, memberType, resID, true
}

// ListFoldersForResource lists the ARNs of all folders that resourceArn is a
// member of.
func (b *InMemoryBackend) ListFoldersForResource(
	accountID, resourceArn string,
	maxResults int32,
	nextToken string,
) ([]string, string, error) {
	b.mu.RLock("ListFoldersForResource")
	defer b.mu.RUnlock()

	arnAccountID, memberType, memberID, ok := resourceArnToMember(resourceArn)
	if !ok || arnAccountID != accountID {
		return nil, "", nil
	}

	var folderIDs []string
	prefix := accountID + "/"
	for k, m := range b.folderMembers {
		if strings.HasPrefix(k, prefix) && m.MemberType == memberType && m.MemberID == memberID {
			folderIDs = append(folderIDs, m.FolderID)
		}
	}
	sort.Strings(folderIDs)

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, id := range folderIDs {
			if id == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(folderIDs) {
		next = folderIDs[end]
	} else {
		end = len(folderIDs)
	}

	result := make([]string, 0, end-start)
	for _, id := range folderIDs[start:end] {
		if f, exists := b.folders[folderKey(accountID, id)]; exists {
			result = append(result, f.Arn)
		}
	}

	return result, next, nil
}
