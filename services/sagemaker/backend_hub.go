package sagemaker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
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
func (b *InMemoryBackend) hubsStore(r string) map[string]*Hub {
	if b.hubs[r] == nil {
		b.hubs[r] = make(map[string]*Hub)
	}

	return b.hubs[r]
}

// findHubLocked resolves a hub by name or ARN. Callers must hold b.mu.
func (b *InMemoryBackend) findHubLocked(region, idOrArn string) (*Hub, bool) {
	if h, ok := b.hubsStore(region)[idOrArn]; ok {
		return h, true
	}

	for _, h := range b.hubsStore(region) {
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

	if _, ok := store[name]; ok {
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
	store[name] = h

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

// ListHubs returns hubs whose name contains nameContains, sorted by name.
func (b *InMemoryBackend) ListHubs(ctx context.Context, nameContains, nextToken string) ([]*Hub, string) {
	b.mu.RLock("ListHubs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.hubsStore(region)

	filtered := make(map[string]*Hub, len(store))

	for k, v := range store {
		if nameContains == "" || strings.Contains(v.HubName, nameContains) {
			filtered[k] = v
		}
	}

	return sagemakerListPaged(filtered, nextToken, cloneHub,
		func(a, bb *Hub) bool { return a.HubName < bb.HubName })
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

	for _, hc := range b.hubContentsStore(region) {
		if hc.HubName == h.HubName {
			return fmt.Errorf("%w: hub %q still has hub content and cannot be deleted",
				ErrHubNotEmpty, h.HubName)
		}
	}

	delete(b.hubsStore(region), h.HubName)

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
func (b *InMemoryBackend) hubContentsStore(r string) map[hubContentKey]*HubContent {
	if b.hubContents[r] == nil {
		b.hubContents[r] = make(map[hubContentKey]*HubContent)
	}

	return b.hubContents[r]
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

	key := hubContentKey{
		HubName: h.HubName, HubContentType: in.HubContentType,
		HubContentName: in.HubContentName, HubContentVersion: version,
	}

	store := b.hubContentsStore(region)
	if _, exists := store[key]; exists {
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
		HubContentArn:            hubContentARN(region, b.accountID, key),
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
	store[key] = hc

	return cloneHubContent(hc), nil
}

// latestHubContentLocked returns the most recently created version of the named
// content within a hub/type, or false if none exists. Callers must hold b.mu.
func (b *InMemoryBackend) latestHubContentLocked(region, hubName, contentType, contentName string) (*HubContent, bool) {
	var latest *HubContent

	for k, hc := range b.hubContentsStore(region) {
		if k.HubName != hubName || k.HubContentType != contentType || k.HubContentName != contentName {
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
		key := hubContentKey{
			HubName: h.HubName, HubContentType: contentType,
			HubContentName: contentName, HubContentVersion: version,
		}
		if hc, exists := b.hubContentsStore(region)[key]; exists {
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

// ListHubContents returns the latest version of every distinct content name of
// the given type within a hub, sorted by name.
func (b *InMemoryBackend) ListHubContents(
	ctx context.Context,
	hubName, contentType, nameContains, nextToken string,
) ([]*HubContent, string) {
	b.mu.RLock("ListHubContents")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	latestByName := make(map[string]*HubContent)

	for k, hc := range b.hubContentsStore(region) {
		if k.HubName != hubName || k.HubContentType != contentType {
			continue
		}

		if nameContains != "" && !strings.Contains(k.HubContentName, nameContains) {
			continue
		}

		if cur, exists := latestByName[k.HubContentName]; !exists || hc.CreationTime.After(cur.CreationTime) {
			latestByName[k.HubContentName] = hc
		}
	}

	return sagemakerListPaged(latestByName, nextToken, cloneHubContent,
		func(a, bb *HubContent) bool { return a.HubContentName < bb.HubContentName })
}

// ListHubContentVersions returns every version of a named content resource,
// sorted by version string.
func (b *InMemoryBackend) ListHubContentVersions(
	ctx context.Context,
	hubName, contentType, contentName, nextToken string,
) ([]*HubContent, string) {
	b.mu.RLock("ListHubContentVersions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	byVersion := make(map[string]*HubContent)

	for k, hc := range b.hubContentsStore(region) {
		if k.HubName != hubName || k.HubContentType != contentType || k.HubContentName != contentName {
			continue
		}

		byVersion[k.HubContentVersion] = hc
	}

	return sagemakerListPaged(byVersion, nextToken, cloneHubContent,
		func(a, bb *HubContent) bool { return a.HubContentVersion < bb.HubContentVersion })
}

// DeleteHubContent deletes a single version of a hub content resource.
func (b *InMemoryBackend) DeleteHubContent(
	ctx context.Context,
	hubName, contentType, contentName, version string,
) error {
	b.mu.Lock("DeleteHubContent")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	key := hubContentKey{
		HubName: hubName, HubContentType: contentType,
		HubContentName: contentName, HubContentVersion: version,
	}

	store := b.hubContentsStore(region)
	if _, ok := store[key]; !ok {
		return fmt.Errorf(
			"%w: hub content %q version %q not found in hub %q",
			ErrHubContentNotFound, contentName, version, hubName,
		)
	}

	delete(store, key)

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

	key := hubContentKey{
		HubName: h.HubName, HubContentType: hubContentTypeModelRef,
		HubContentName: name, HubContentVersion: version,
	}

	store := b.hubContentsStore(region)
	if _, exists := store[key]; exists {
		return nil, fmt.Errorf(
			"%w: hub content reference %q already exists in hub %q", ErrHubContentAlreadyExists, name, h.HubName,
		)
	}

	now := time.Now()
	hc := &HubContent{
		HubName:                      h.HubName,
		HubArn:                       h.HubArn,
		HubContentName:               name,
		HubContentArn:                hubContentARN(region, b.accountID, key),
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
	store[key] = hc

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

	for k := range store {
		if k.HubName == hubName && k.HubContentType == contentType && k.HubContentName == contentName {
			delete(store, k)

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

// CreateHubContentPresignedURLs returns presigned download URLs for the files
// backing a hub content resource. If version is empty, the latest version is used.
func (b *InMemoryBackend) CreateHubContentPresignedURLs(
	ctx context.Context,
	hubName, contentType, contentName, version string,
) ([]HubContentPresignedURL, error) {
	b.mu.RLock("CreateHubContentPresignedURLs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	h, ok := b.findHubLocked(region, hubName)
	if !ok {
		return nil, fmt.Errorf("%w: hub %q not found", ErrHubNotFound, hubName)
	}

	var hc *HubContent

	if version != "" {
		key := hubContentKey{
			HubName: h.HubName, HubContentType: contentType,
			HubContentName: contentName, HubContentVersion: version,
		}

		found, exists := b.hubContentsStore(region)[key]
		if !exists {
			return nil, fmt.Errorf(
				"%w: hub content %q version %q not found in hub %q",
				ErrHubContentNotFound, contentName, version, h.HubName,
			)
		}

		hc = found
	} else {
		found, exists := b.latestHubContentLocked(region, h.HubName, contentType, contentName)
		if !exists {
			return nil, fmt.Errorf(
				"%w: hub content %q not found in hub %q", ErrHubContentNotFound, contentName, h.HubName,
			)
		}

		hc = found
	}

	if len(hc.HubContentDependencies) == 0 {
		key := fmt.Sprintf("%s/%s/%s/model.tar.gz", hc.HubName, hc.HubContentName, hc.HubContentVersion)

		return []HubContentPresignedURL{{
			URL:       generatePresignedURL(region, key),
			LocalPath: hc.HubContentName + "/model.tar.gz",
		}}, nil
	}

	urls := make([]HubContentPresignedURL, 0, len(hc.HubContentDependencies))
	for _, dep := range hc.HubContentDependencies {
		localPath := dep.DependencyCopyPath
		if localPath == "" {
			localPath = dep.DependencyOriginPath
		}

		key := fmt.Sprintf("%s/%s/%s/%s", hc.HubName, hc.HubContentName, hc.HubContentVersion, localPath)
		urls = append(urls, HubContentPresignedURL{URL: generatePresignedURL(region, key), LocalPath: localPath})
	}

	return urls, nil
}
