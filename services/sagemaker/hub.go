package sagemaker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

var (
	// ErrHubNotFound is returned when a hub does not exist.
	ErrHubNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrHubAlreadyExists is returned when a hub already exists.
	ErrHubAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrHubNotEmpty is returned when deleting a hub that still contains hub content.
	ErrHubNotEmpty = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrHubContentNotFound is returned when a hub content resource does not exist.
	ErrHubContentNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrHubContentAlreadyExists is returned when a hub content resource already exists.
	ErrHubContentAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

const (
	hubContentStatusAvailable = "Available"
	hubContentTypeModelRef    = "ModelReference"
	defaultHubContentVersion  = "1.0.0"
	defaultDocumentSchemaVer  = "1.0.0"
	presignedURLExpirySeconds = 900
)

// ---------------------------------------------------------------------------
// Hub
// ---------------------------------------------------------------------------

// Hub represents a SageMaker private model hub.
type Hub struct {
	CreationTime      time.Time         `json:"CreationTime"`
	LastModifiedTime  time.Time         `json:"LastModifiedTime"`
	Tags              map[string]string `json:"Tags,omitempty"`
	HubName           string            `json:"HubName"`
	HubArn            string            `json:"HubArn"`
	HubDisplayName    string            `json:"HubDisplayName,omitempty"`
	HubDescription    string            `json:"HubDescription,omitempty"`
	S3OutputPath      string            `json:"S3OutputPath,omitempty"`
	HubStatus         string            `json:"HubStatus"`
	FailureReason     string            `json:"FailureReason,omitempty"`
	HubSearchKeywords []string          `json:"HubSearchKeywords,omitempty"`
}

func cloneHub(h *Hub) *Hub {
	cp := *h
	cp.Tags = maps.Clone(h.Tags)
	cp.HubSearchKeywords = append([]string(nil), h.HubSearchKeywords...)

	return &cp
}

// UpdateHubOptions carries the optional fields accepted by UpdateHub.
// Nil pointers / HasSearchKeywords == false mean "field not present in request".
type UpdateHubOptions struct {
	HubDescription    *string
	HubDisplayName    *string
	HubSearchKeywords []string
	HasSearchKeywords bool
}

// hubsStore returns the region-scoped hub map, creating it lazily.
func (b *InMemoryBackend) hubsStore(r string) *store.Table[Hub] {
	if b.hubs[r] == nil {
		b.hubs[r] = store.Register(b.registry, "hubs:"+r, store.New(func(v *Hub) string { return v.HubName }))
	}

	return b.hubs[r]
}

// hubsStoreRO returns the region-scoped hubs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) hubsStoreRO(r string) *store.Table[Hub] {
	if v := b.hubs[r]; v != nil {
		return v
	}

	return store.New(func(v *Hub) string { return v.HubName })
}

// findHubLocked resolves a hub by name or ARN. Callers must hold b.mu.
func (b *InMemoryBackend) findHubLocked(region, idOrArn string) (*Hub, bool) {
	if h, ok := b.hubsStoreRO(region).Get(idOrArn); ok {
		return h, true
	}

	for _, h := range b.hubsStoreRO(region).All() {
		if h.HubArn == idOrArn {
			return h, true
		}
	}

	return nil, false
}

// CreateHub creates a new private model hub.
func (b *InMemoryBackend) CreateHub(
	ctx context.Context,
	name, description, displayName, s3OutputPath string,
	searchKeywords []string,
	tags map[string]string,
) (*Hub, error) {
	b.mu.Lock("CreateHub")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.hubsStore(region)

	if _, ok := store.Get(name); ok {
		return nil, fmt.Errorf("%w: hub %q already exists", ErrHubAlreadyExists, name)
	}

	hubArn := arn.Build("sagemaker", region, b.accountID, "hub/"+name)
	now := time.Now()

	h := &Hub{
		HubName:           name,
		HubArn:            hubArn,
		HubDescription:    description,
		HubDisplayName:    displayName,
		HubSearchKeywords: append([]string(nil), searchKeywords...),
		S3OutputPath:      s3OutputPath,
		HubStatus:         statusInService,
		CreationTime:      now,
		LastModifiedTime:  now,
		Tags:              mergeTags(nil, tags),
	}
	store.Put(h)

	return cloneHub(h), nil
}

// DescribeHub returns a hub by name or ARN.
func (b *InMemoryBackend) DescribeHub(ctx context.Context, idOrArn string) (*Hub, error) {
	b.mu.RLock("DescribeHub")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	h, ok := b.findHubLocked(region, idOrArn)
	if !ok {
		return nil, fmt.Errorf("%w: hub %q not found", ErrHubNotFound, idOrArn)
	}

	return cloneHub(h), nil
}

// ListHubsParams bundles ListHubs' filter/sort/pagination criteria
// (api_op_ListHubs.go).
type ListHubsParams struct {
	CreationTimeAfter      *time.Time
	CreationTimeBefore     *time.Time
	LastModifiedTimeAfter  *time.Time
	LastModifiedTimeBefore *time.Time
	NameContains           string
	NextToken              string
	SortBy                 string
	SortOrder              string
	MaxResults             int32
}

// ListHubs returns hubs matching params, sorted per params.SortBy (default
// HubName, ties broken by HubName) / params.SortOrder (default Ascending —
// neither default is documented by AWS for this op, so the pre-existing
// unconditional HubName-ascending behavior is kept as the fallback), capped
// at params.MaxResults.
func (b *InMemoryBackend) ListHubs(ctx context.Context, params ListHubsParams) ([]*Hub, string) {
	b.mu.RLock("ListHubs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.hubsStoreRO(region)
	list := make([]*Hub, 0, store.Len())

	for _, v := range store.All() {
		if params.NameContains != "" && !strings.Contains(v.HubName, params.NameContains) {
			continue
		}

		if params.CreationTimeAfter != nil && !v.CreationTime.After(*params.CreationTimeAfter) {
			continue
		}

		if params.CreationTimeBefore != nil && !v.CreationTime.Before(*params.CreationTimeBefore) {
			continue
		}

		if params.LastModifiedTimeAfter != nil && !v.LastModifiedTime.After(*params.LastModifiedTimeAfter) {
			continue
		}

		if params.LastModifiedTimeBefore != nil && !v.LastModifiedTime.Before(*params.LastModifiedTimeBefore) {
			continue
		}

		list = append(list, cloneHub(v))
	}

	desc := strings.EqualFold(params.SortOrder, sortOrderDescending)
	sort.Slice(list, func(i, j int) bool {
		less := hubSortLess(list[i], list[j], params.SortBy)
		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

// hubSortLess orders two hubs by sortBy — one of HubSortBy's real values
// (HubName, CreationTime, HubStatus, AccountIdOwner; types/enums.go:3929-3944).
// AccountIdOwner has no distinguishing order in this single-account-per-region
// backend, so (like every other key) it falls through to the HubName tiebreak.
func hubSortLess(a, b *Hub, sortBy string) bool {
	switch sortBy {
	case keyCreationTime:
		if !a.CreationTime.Equal(b.CreationTime) {
			return a.CreationTime.Before(b.CreationTime)
		}
	case "HubStatus":
		if a.HubStatus != b.HubStatus {
			return a.HubStatus < b.HubStatus
		}
	}

	return a.HubName < b.HubName
}

// UpdateHub updates the mutable fields of a hub.
func (b *InMemoryBackend) UpdateHub(ctx context.Context, idOrArn string, opts UpdateHubOptions) (*Hub, error) {
	b.mu.Lock("UpdateHub")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	h, ok := b.findHubLocked(region, idOrArn)
	if !ok {
		return nil, fmt.Errorf("%w: hub %q not found", ErrHubNotFound, idOrArn)
	}

	if opts.HubDescription != nil {
		h.HubDescription = *opts.HubDescription
	}

	if opts.HubDisplayName != nil {
		h.HubDisplayName = *opts.HubDisplayName
	}

	if opts.HasSearchKeywords {
		h.HubSearchKeywords = append([]string(nil), opts.HubSearchKeywords...)
	}

	h.LastModifiedTime = time.Now()

	return cloneHub(h), nil
}

// DeleteHub deletes a hub. AWS rejects deletion when hub content still exists.
func (b *InMemoryBackend) DeleteHub(ctx context.Context, idOrArn string) error {
	b.mu.Lock("DeleteHub")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	h, ok := b.findHubLocked(region, idOrArn)
	if !ok {
		return fmt.Errorf("%w: hub %q not found", ErrHubNotFound, idOrArn)
	}

	for _, hc := range b.hubContentsStore(region).All() {
		if hc.HubName == h.HubName {
			return fmt.Errorf("%w: hub %q still has hub content and cannot be deleted",
				ErrHubNotEmpty, h.HubName)
		}
	}

	b.hubsStore(region).Delete(h.HubName)

	return nil
}

// ---------------------------------------------------------------------------
// HubContent
// ---------------------------------------------------------------------------

// HubContentDependency describes a single dependency copied alongside hub content.
type HubContentDependency struct {
	DependencyOriginPath string `json:"DependencyOriginPath,omitempty"`
	DependencyCopyPath   string `json:"DependencyCopyPath,omitempty"`
}

// HubContent represents a single versioned resource (model, notebook, or model
// reference) imported into a Hub.
type HubContent struct {
	CreationTime                 time.Time              `json:"CreationTime"`
	LastModifiedTime             time.Time              `json:"LastModifiedTime"`
	Tags                         map[string]string      `json:"Tags,omitempty"`
	HubContentType               string                 `json:"HubContentType"`
	HubContentDisplayName        string                 `json:"HubContentDisplayName,omitempty"`
	HubName                      string                 `json:"HubName"`
	HubArn                       string                 `json:"HubArn"`
	HubContentName               string                 `json:"HubContentName"`
	HubContentArn                string                 `json:"HubContentArn"`
	HubContentVersion            string                 `json:"HubContentVersion"`
	FailureReason                string                 `json:"FailureReason,omitempty"`
	DocumentSchemaVersion        string                 `json:"DocumentSchemaVersion"`
	HubContentStatus             string                 `json:"HubContentStatus"`
	HubContentDescription        string                 `json:"HubContentDescription,omitempty"`
	HubContentMarkdown           string                 `json:"HubContentMarkdown,omitempty"`
	HubContentDocument           string                 `json:"HubContentDocument"`
	SageMakerPublicHubContentArn string                 `json:"SageMakerPublicHubContentArn,omitempty"`
	ReferenceMinVersion          string                 `json:"ReferenceMinVersion,omitempty"`
	SupportStatus                string                 `json:"SupportStatus,omitempty"`
	HubContentDependencies       []HubContentDependency `json:"HubContentDependencies,omitempty"`
	HubContentSearchKeywords     []string               `json:"HubContentSearchKeywords,omitempty"`
}

func cloneHubContent(hc *HubContent) *HubContent {
	cp := *hc
	cp.Tags = maps.Clone(hc.Tags)
	cp.HubContentSearchKeywords = append([]string(nil), hc.HubContentSearchKeywords...)
	cp.HubContentDependencies = append([]HubContentDependency(nil), hc.HubContentDependencies...)

	return &cp
}

// hubContentKey uniquely identifies a hub content resource within a region.
type hubContentKey struct {
	HubName           string
	HubContentType    string
	HubContentName    string
	HubContentVersion string
}

// hubContentsStore returns the region-scoped hub content map, creating it lazily.
func (b *InMemoryBackend) hubContentsStore(r string) *store.Table[HubContent] {
	if b.hubContents[r] == nil {
		b.hubContents[r] = store.Register(b.registry, "hubContents:"+r, store.New(func(v *HubContent) string {
			return hubContentKeyString(
				hubContentKey{
					HubName:           v.HubName,
					HubContentType:    v.HubContentType,
					HubContentName:    v.HubContentName,
					HubContentVersion: v.HubContentVersion,
				},
			)
		}))
	}

	return b.hubContents[r]
}

// hubContentsStoreRO returns the region-scoped hubContents table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) hubContentsStoreRO(r string) *store.Table[HubContent] {
	if v := b.hubContents[r]; v != nil {
		return v
	}

	return store.New(func(v *HubContent) string {
		return hubContentKeyString(
			hubContentKey{
				HubName:           v.HubName,
				HubContentType:    v.HubContentType,
				HubContentName:    v.HubContentName,
				HubContentVersion: v.HubContentVersion,
			},
		)
	})
}

// hubContentARN builds the ARN for a versioned hub content resource.
func hubContentARN(region, accountID string, key hubContentKey) string {
	resource := fmt.Sprintf(
		"hub-content/%s/%s/%s/%s", key.HubName, key.HubContentType, key.HubContentName, key.HubContentVersion,
	)

	return arn.Build("sagemaker", region, accountID, resource)
}

// ImportHubContentInput carries the fields accepted by ImportHubContent.
type ImportHubContentInput struct {
	Tags                     map[string]string
	HubName                  string
	HubContentName           string
	HubContentVersion        string
	HubContentType           string
	DocumentSchemaVersion    string
	HubContentDisplayName    string
	HubContentDescription    string
	HubContentMarkdown       string
	HubContentDocument       string
	SupportStatus            string
	HubContentSearchKeywords []string
}

// ImportHubContent imports a new (or new-version) content resource into a hub.
func (b *InMemoryBackend) ImportHubContent(ctx context.Context, in ImportHubContentInput) (*HubContent, error) {
	b.mu.Lock("ImportHubContent")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	h, ok := b.findHubLocked(region, in.HubName)
	if !ok {
		return nil, fmt.Errorf("%w: hub %q not found", ErrHubNotFound, in.HubName)
	}

	version := in.HubContentVersion
	if version == "" {
		version = defaultHubContentVersion
	}

	hcKey := hubContentKey{
		HubName: h.HubName, HubContentType: in.HubContentType,
		HubContentName: in.HubContentName, HubContentVersion: version,
	}

	store := b.hubContentsStore(region)
	if _, exists := store.Get(hubContentKeyString(hcKey)); exists {
		return nil, fmt.Errorf(
			"%w: hub content %q version %q already exists in hub %q",
			ErrHubContentAlreadyExists, in.HubContentName, version, h.HubName,
		)
	}

	now := time.Now()
	hc := &HubContent{
		HubName:                  h.HubName,
		HubArn:                   h.HubArn,
		HubContentName:           in.HubContentName,
		HubContentArn:            hubContentARN(region, b.accountID, hcKey),
		HubContentVersion:        version,
		HubContentType:           in.HubContentType,
		DocumentSchemaVersion:    in.DocumentSchemaVersion,
		HubContentDisplayName:    in.HubContentDisplayName,
		HubContentDescription:    in.HubContentDescription,
		HubContentMarkdown:       in.HubContentMarkdown,
		HubContentDocument:       in.HubContentDocument,
		SupportStatus:            in.SupportStatus,
		HubContentSearchKeywords: append([]string(nil), in.HubContentSearchKeywords...),
		HubContentStatus:         hubContentStatusAvailable,
		CreationTime:             now,
		LastModifiedTime:         now,
		Tags:                     mergeTags(nil, in.Tags),
	}
	store.Put(hc)

	return cloneHubContent(hc), nil
}

// latestHubContentLocked returns the most recently created version of the named
// content within a hub/type, or false if none exists. Callers must hold b.mu.
func (b *InMemoryBackend) latestHubContentLocked(region, hubName, contentType, contentName string) (*HubContent, bool) {
	var latest *HubContent

	for _, hc := range b.hubContentsStoreRO(region).All() {
		if hc.HubName != hubName || hc.HubContentType != contentType || hc.HubContentName != contentName {
			continue
		}

		if latest == nil || hc.CreationTime.After(latest.CreationTime) {
			latest = hc
		}
	}

	if latest == nil {
		return nil, false
	}

	return latest, true
}

// DescribeHubContent returns a hub content resource. If version is empty, the
// most recently imported version is returned.
func (b *InMemoryBackend) DescribeHubContent(
	ctx context.Context,
	hubName, contentType, contentName, version string,
) (*HubContent, error) {
	b.mu.RLock("DescribeHubContent")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	h, ok := b.findHubLocked(region, hubName)
	if !ok {
		return nil, fmt.Errorf("%w: hub %q not found", ErrHubNotFound, hubName)
	}

	if version != "" {
		key := hubContentKeyString(hubContentKey{
			HubName: h.HubName, HubContentType: contentType,
			HubContentName: contentName, HubContentVersion: version,
		})
		if hc, exists := b.hubContentsStoreRO(region).Get(key); exists {
			return cloneHubContent(hc), nil
		}

		return nil, fmt.Errorf(
			"%w: hub content %q version %q not found in hub %q",
			ErrHubContentNotFound, contentName, version, h.HubName,
		)
	}

	hc, ok := b.latestHubContentLocked(region, h.HubName, contentType, contentName)
	if !ok {
		return nil, fmt.Errorf(
			"%w: hub content %q not found in hub %q", ErrHubContentNotFound, contentName, h.HubName,
		)
	}

	return cloneHubContent(hc), nil
}

// UpdateHubContentOptions bundles the mutable fields accepted by UpdateHubContent.
type UpdateHubContentOptions struct {
	HubContentDescription    string
	HubContentDisplayName    string
	HubContentMarkdown       string
	SupportStatus            string
	HubContentSearchKeywords []string
}

// UpdateHubContent updates the mutable metadata of a specific hub content version.
func (b *InMemoryBackend) UpdateHubContent(
	ctx context.Context,
	hubName, contentType, contentName, version string,
	opts UpdateHubContentOptions,
) (*HubContent, error) {
	b.mu.Lock("UpdateHubContent")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	h, ok := b.findHubLocked(region, hubName)
	if !ok {
		return nil, fmt.Errorf("%w: hub %q not found", ErrHubNotFound, hubName)
	}

	key := hubContentKeyString(hubContentKey{
		HubName: h.HubName, HubContentType: contentType,
		HubContentName: contentName, HubContentVersion: version,
	})

	hc, ok := b.hubContentsStore(region).Get(key)
	if !ok {
		return nil, fmt.Errorf(
			"%w: hub content %q version %q not found in hub %q", ErrHubContentNotFound, contentName, version, h.HubName,
		)
	}

	if opts.HubContentDescription != "" {
		hc.HubContentDescription = opts.HubContentDescription
	}
	if opts.HubContentDisplayName != "" {
		hc.HubContentDisplayName = opts.HubContentDisplayName
	}
	if opts.HubContentMarkdown != "" {
		hc.HubContentMarkdown = opts.HubContentMarkdown
	}
	if opts.SupportStatus != "" {
		hc.SupportStatus = opts.SupportStatus
	}
	if opts.HubContentSearchKeywords != nil {
		hc.HubContentSearchKeywords = append([]string(nil), opts.HubContentSearchKeywords...)
	}
	hc.LastModifiedTime = time.Now()

	return cloneHubContent(hc), nil
}

// UpdateHubContentReference updates the minimum referenced version of a
// ModelReference hub content resource (the most recently created version).
func (b *InMemoryBackend) UpdateHubContentReference(
	ctx context.Context,
	hubName, contentType, contentName, minVersion string,
) (*HubContent, error) {
	b.mu.Lock("UpdateHubContentReference")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	h, ok := b.findHubLocked(region, hubName)
	if !ok {
		return nil, fmt.Errorf("%w: hub %q not found", ErrHubNotFound, hubName)
	}

	hc, ok := b.latestHubContentLocked(region, h.HubName, contentType, contentName)
	if !ok {
		return nil, fmt.Errorf(
			"%w: hub content reference %q not found in hub %q", ErrHubContentNotFound, contentName, h.HubName,
		)
	}

	if minVersion != "" {
		hc.ReferenceMinVersion = minVersion
	}
	hc.LastModifiedTime = time.Now()

	return cloneHubContent(hc), nil
}

// ListHubContentsParams bundles ListHubContents' filter/sort/pagination
// criteria (api_op_ListHubContents.go).
type ListHubContentsParams struct {
	CreationTimeAfter  *time.Time
	CreationTimeBefore *time.Time
	MaxSchemaVersion   string
	NameContains       string
	NextToken          string
	SortBy             string
	SortOrder          string
	MaxResults         int32
}

// hubContentMatchesListParams reports whether hc satisfies
// ListHubContentsParams' NameContains/MaxSchemaVersion/CreationTime-window
// filters (the HubName/HubContentType match is applied by the caller).
func hubContentMatchesListParams(hc *HubContent, params ListHubContentsParams) bool {
	if params.NameContains != "" && !strings.Contains(hc.HubContentName, params.NameContains) {
		return false
	}

	if params.MaxSchemaVersion != "" && compareDottedVersions(hc.DocumentSchemaVersion, params.MaxSchemaVersion) > 0 {
		return false
	}

	if params.CreationTimeAfter != nil && !hc.CreationTime.After(*params.CreationTimeAfter) {
		return false
	}

	if params.CreationTimeBefore != nil && !hc.CreationTime.Before(*params.CreationTimeBefore) {
		return false
	}

	return true
}

// ListHubContents returns the latest surviving version of every distinct
// content name of the given type within a hub, filtered by params, sorted
// per params.SortBy (default HubContentName) / params.SortOrder (default
// Ascending, matching the pre-existing unconditional behavior), capped at
// params.MaxResults.
func (b *InMemoryBackend) ListHubContents(
	ctx context.Context,
	hubName, contentType string,
	params ListHubContentsParams,
) ([]*HubContent, string) {
	b.mu.RLock("ListHubContents")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	latestByName := make(map[string]*HubContent)

	for _, hc := range b.hubContentsStoreRO(region).All() {
		if hc.HubName != hubName || hc.HubContentType != contentType {
			continue
		}

		if !hubContentMatchesListParams(hc, params) {
			continue
		}

		if cur, exists := latestByName[hc.HubContentName]; !exists || hc.CreationTime.After(cur.CreationTime) {
			latestByName[hc.HubContentName] = hc
		}
	}

	list := make([]*HubContent, 0, len(latestByName))
	for _, hc := range latestByName {
		list = append(list, cloneHubContent(hc))
	}

	desc := strings.EqualFold(params.SortOrder, sortOrderDescending)
	sort.Slice(list, func(i, j int) bool {
		less := hubContentSortLess(list[i], list[j], params.SortBy)
		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

// hubContentSortLess orders two hub content entries by sortBy — one of
// HubContentSortBy's real values (HubContentName, CreationTime,
// HubContentStatus; types/enums.go:3833-3847) — ties broken by HubContentName.
func hubContentSortLess(a, b *HubContent, sortBy string) bool {
	switch sortBy {
	case keyCreationTime:
		if !a.CreationTime.Equal(b.CreationTime) {
			return a.CreationTime.Before(b.CreationTime)
		}
	case keyHubContentStatus:
		if a.HubContentStatus != b.HubContentStatus {
			return a.HubContentStatus < b.HubContentStatus
		}
	}

	return a.HubContentName < b.HubContentName
}

// compareDottedVersions compares two "\d{1,4}.\d{1,4}.\d{1,4}"-shaped version
// strings (the pattern shared by HubContentVersion, DocumentSchemaVersion,
// MinVersion, and MaxSchemaVersion) component-by-component as integers,
// returning -1, 0, or 1. A non-numeric or missing component compares as 0
// (equal) so a malformed value degrades to a no-op filter rather than a panic
// or a silently-wrong ordering.
func compareDottedVersions(a, b string) int {
	as := strings.Split(a, ".")
	bs := strings.Split(b, ".")

	for i := range max(len(as), len(bs)) {
		var av, bv int

		if i < len(as) {
			av, _ = strconv.Atoi(as[i])
		}

		if i < len(bs) {
			bv, _ = strconv.Atoi(bs[i])
		}

		if av != bv {
			if av < bv {
				return -1
			}

			return 1
		}
	}

	return 0
}

// ListHubContentVersionsParams bundles ListHubContentVersions'
// filter/sort/pagination criteria (api_op_ListHubContentVersions.go).
type ListHubContentVersionsParams struct {
	CreationTimeAfter  *time.Time
	CreationTimeBefore *time.Time
	MaxSchemaVersion   string
	MinVersion         string
	NextToken          string
	SortBy             string
	SortOrder          string
	MaxResults         int32
}

// ListHubContentVersions returns every version of a named content resource
// matching params, sorted per params.SortBy / params.SortOrder (default
// ascending version order — HubContentSortBy's "HubContentName" key is a
// no-op here since every entry shares the same name, so version order is
// used as the tiebreak/default instead, matching the pre-existing behavior),
// capped at params.MaxResults.
func (b *InMemoryBackend) ListHubContentVersions(
	ctx context.Context,
	hubName, contentType, contentName string,
	params ListHubContentVersionsParams,
) ([]*HubContent, string) {
	b.mu.RLock("ListHubContentVersions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	list := make([]*HubContent, 0)

	for _, hc := range b.hubContentsStoreRO(region).All() {
		if hc.HubName != hubName || hc.HubContentType != contentType || hc.HubContentName != contentName {
			continue
		}

		if params.MaxSchemaVersion != "" &&
			compareDottedVersions(hc.DocumentSchemaVersion, params.MaxSchemaVersion) > 0 {
			continue
		}

		if params.MinVersion != "" && compareDottedVersions(hc.HubContentVersion, params.MinVersion) < 0 {
			continue
		}

		if params.CreationTimeAfter != nil && !hc.CreationTime.After(*params.CreationTimeAfter) {
			continue
		}

		if params.CreationTimeBefore != nil && !hc.CreationTime.Before(*params.CreationTimeBefore) {
			continue
		}

		list = append(list, cloneHubContent(hc))
	}

	desc := strings.EqualFold(params.SortOrder, sortOrderDescending)
	sort.Slice(list, func(i, j int) bool {
		less := hubContentVersionSortLess(list[i], list[j], params.SortBy)
		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

// hubContentVersionSortLess orders two versions of the same named hub
// content by sortBy. "HubContentName" (the default zero value) and
// "HubContentStatus" ties both fall back to version order rather than
// HubContentName, since every item ListHubContentVersions returns shares one
// name.
func hubContentVersionSortLess(a, b *HubContent, sortBy string) bool {
	switch sortBy {
	case keyCreationTime:
		if !a.CreationTime.Equal(b.CreationTime) {
			return a.CreationTime.Before(b.CreationTime)
		}
	case keyHubContentStatus:
		if a.HubContentStatus != b.HubContentStatus {
			return a.HubContentStatus < b.HubContentStatus
		}
	}

	return compareDottedVersions(a.HubContentVersion, b.HubContentVersion) < 0
}

// DeleteHubContent deletes a single version of a hub content resource.
func (b *InMemoryBackend) DeleteHubContent(
	ctx context.Context,
	hubName, contentType, contentName, version string,
) error {
	b.mu.Lock("DeleteHubContent")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	key := hubContentKeyString(hubContentKey{
		HubName: hubName, HubContentType: contentType,
		HubContentName: contentName, HubContentVersion: version,
	})

	store := b.hubContentsStore(region)
	if _, ok := store.Get(key); !ok {
		return fmt.Errorf(
			"%w: hub content %q version %q not found in hub %q",
			ErrHubContentNotFound, contentName, version, hubName,
		)
	}

	store.Delete(key)

	return nil
}

// hubContentARNNameSegments is the minimum number of "/"-separated segments
// ("<hub>/<type>/<name>/...") expected after the "hub-content/" prefix.
const hubContentARNNameSegments = 3

// hubContentARNNameIndex is the index of the content-name segment within those parts.
const hubContentARNNameIndex = 2

// extractHubContentNameFromARN extracts the content name segment from a
// "hub-content/<hub>/<type>/<name>/<version>" resource ARN.
func extractHubContentNameFromARN(hubContentArn string) string {
	_, rest, found := strings.Cut(hubContentArn, "hub-content/")
	if !found {
		return ""
	}

	parts := strings.Split(rest, "/")
	if len(parts) < hubContentARNNameSegments {
		return ""
	}

	return parts[hubContentARNNameIndex]
}

// CreateHubContentReference creates a ModelReference hub content resource that
// points at a public hub's content.
func (b *InMemoryBackend) CreateHubContentReference(
	ctx context.Context,
	hubName, publicHubContentArn, contentName, minVersion string,
	tags map[string]string,
) (*HubContent, error) {
	b.mu.Lock("CreateHubContentReference")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	h, ok := b.findHubLocked(region, hubName)
	if !ok {
		return nil, fmt.Errorf("%w: hub %q not found", ErrHubNotFound, hubName)
	}

	name := contentName
	if name == "" {
		name = extractHubContentNameFromARN(publicHubContentArn)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: could not determine HubContentName from %q", ErrValidation, publicHubContentArn)
	}

	version := minVersion
	if version == "" {
		version = defaultHubContentVersion
	}

	hcKey := hubContentKey{
		HubName: h.HubName, HubContentType: hubContentTypeModelRef,
		HubContentName: name, HubContentVersion: version,
	}

	store := b.hubContentsStore(region)
	if _, exists := store.Get(hubContentKeyString(hcKey)); exists {
		return nil, fmt.Errorf(
			"%w: hub content reference %q already exists in hub %q", ErrHubContentAlreadyExists, name, h.HubName,
		)
	}

	now := time.Now()
	hc := &HubContent{
		HubName:                      h.HubName,
		HubArn:                       h.HubArn,
		HubContentName:               name,
		HubContentArn:                hubContentARN(region, b.accountID, hcKey),
		HubContentVersion:            version,
		HubContentType:               hubContentTypeModelRef,
		DocumentSchemaVersion:        defaultDocumentSchemaVer,
		SageMakerPublicHubContentArn: publicHubContentArn,
		ReferenceMinVersion:          minVersion,
		HubContentStatus:             hubContentStatusAvailable,
		CreationTime:                 now,
		LastModifiedTime:             now,
		Tags:                         mergeTags(nil, tags),
	}
	store.Put(hc)

	return cloneHubContent(hc), nil
}

// DeleteHubContentReference deletes all versions of a ModelReference hub content resource.
func (b *InMemoryBackend) DeleteHubContentReference(
	ctx context.Context,
	hubName, contentType, contentName string,
) error {
	b.mu.Lock("DeleteHubContentReference")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.hubContentsStore(region)

	deleted := false

	for _, hc := range store.All() {
		if hc.HubName == hubName && hc.HubContentType == contentType && hc.HubContentName == contentName {
			store.Delete(hubContentKeyString(hubContentKey{
				HubName: hc.HubName, HubContentType: hc.HubContentType,
				HubContentName: hc.HubContentName, HubContentVersion: hc.HubContentVersion,
			}))

			deleted = true
		}
	}

	if !deleted {
		return fmt.Errorf(
			"%w: hub content reference %q not found in hub %q", ErrHubContentNotFound, contentName, hubName,
		)
	}

	return nil
}

// HubContentPresignedURL is a single presigned URL / local-path pair.
type HubContentPresignedURL struct {
	URL       string
	LocalPath string
}

// presignedURLSignatureBytes is the number of random bytes used to build the
// hex-encoded fake signature in a generated presigned URL.
const presignedURLSignatureBytes = 32

// generatePresignedURL builds a real-shaped (but unsigned) presigned S3 URL.
func generatePresignedURL(region, bucketKey string) string {
	b := make([]byte, presignedURLSignatureBytes)
	_, _ = rand.Read(b)
	sig := hex.EncodeToString(b)

	return fmt.Sprintf(
		"https://sagemaker-hub-content-%s.s3.%s.amazonaws.com/%s"+
			"?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Expires=%d&X-Amz-SignedHeaders=host&X-Amz-Signature=%s",
		region, region, bucketKey, presignedURLExpirySeconds, sig,
	)
}

// PresignedURLAccessConfig mirrors *types.PresignedUrlAccessConfig
// (types/types.go:17716-17729). Accepted and round-tripped into
// CreateHubContentPresignedURLs, but not enforced: this backend has no
// concept of "gated" hub content requiring EULA acceptance to reject
// against, and no independently-resolved S3 URL to validate ExpectedS3URL's
// consistency claim against — disclosed here rather than fabricating either
// check.
type PresignedURLAccessConfig struct {
	ExpectedS3URL string
	AcceptEula    bool
}

// CreateHubContentPresignedURLsParams bundles
// CreateHubContentPresignedURLs' optional request fields
// (api_op_CreateHubContentPresignedUrls.go).
type CreateHubContentPresignedURLsParams struct {
	NextToken    string
	AccessConfig PresignedURLAccessConfig
	MaxResults   int32
}

// CreateHubContentPresignedURLs returns presigned download URLs for the files
// backing a hub content resource. If version is empty, the latest version is
// used. The result is paginated per params.MaxResults (real default: 100,
// api_op_CreateHubContentPresignedUrls.go:34) / params.NextToken.
func (b *InMemoryBackend) CreateHubContentPresignedURLs(
	ctx context.Context,
	hubName, contentType, contentName, version string,
	params CreateHubContentPresignedURLsParams,
) ([]HubContentPresignedURL, string, error) {
	b.mu.RLock("CreateHubContentPresignedURLs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	h, ok := b.findHubLocked(region, hubName)
	if !ok {
		return nil, "", fmt.Errorf("%w: hub %q not found", ErrHubNotFound, hubName)
	}

	var hc *HubContent

	if version != "" {
		key := hubContentKeyString(hubContentKey{
			HubName: h.HubName, HubContentType: contentType,
			HubContentName: contentName, HubContentVersion: version,
		})

		found, exists := b.hubContentsStoreRO(region).Get(key)
		if !exists {
			return nil, "", fmt.Errorf(
				"%w: hub content %q version %q not found in hub %q",
				ErrHubContentNotFound, contentName, version, h.HubName,
			)
		}

		hc = found
	} else {
		found, exists := b.latestHubContentLocked(region, h.HubName, contentType, contentName)
		if !exists {
			return nil, "", fmt.Errorf(
				"%w: hub content %q not found in hub %q", ErrHubContentNotFound, contentName, h.HubName,
			)
		}

		hc = found
	}

	var urls []HubContentPresignedURL

	if len(hc.HubContentDependencies) == 0 {
		key := fmt.Sprintf("%s/%s/%s/model.tar.gz", hc.HubName, hc.HubContentName, hc.HubContentVersion)
		urls = []HubContentPresignedURL{{
			URL:       generatePresignedURL(region, key),
			LocalPath: hc.HubContentName + "/model.tar.gz",
		}}
	} else {
		urls = make([]HubContentPresignedURL, 0, len(hc.HubContentDependencies))
		for _, dep := range hc.HubContentDependencies {
			localPath := dep.DependencyCopyPath
			if localPath == "" {
				localPath = dep.DependencyOriginPath
			}

			key := fmt.Sprintf("%s/%s/%s/%s", hc.HubName, hc.HubContentName, hc.HubContentVersion, localPath)
			urls = append(urls, HubContentPresignedURL{URL: generatePresignedURL(region, key), LocalPath: localPath})
		}
	}

	page, nextToken := paginateSlice(urls, params.NextToken, params.MaxResults)

	return page, nextToken, nil
}
