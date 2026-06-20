package workspaces

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	errResourceNotFound       = "ResourceNotFoundException"
	errInvalidParameterValues = "InvalidParameterValuesException"
	workspaceIDPrefix         = "ws-"
	// AWS workspace IDs use 8 lowercase hex characters after the prefix.
	workspaceIDHexLen     = 8
	stateAvailable        = "AVAILABLE"
	stateAdminMaintenance = "ADMIN_MAINTENANCE"
	stateStopped          = "STOPPED"
	statePending          = "PENDING"
	errMsgNotFound        = "Workspace not found"
	ownerAmazon           = "Amazon"

	// describeWorkspacesMaxResults is the AWS maximum results per page.
	describeWorkspacesMaxResults = 25
	// maxTagsPerResource is the AWS limit for tags per resource.
	maxTagsPerResource = 50
	// maxWorkspacesPerCreate is the AWS limit per CreateWorkspaces call.
	maxWorkspacesPerCreate = 25
	// maxTagKeyLen is the AWS limit for tag key length.
	maxTagKeyLen = 128
	// maxTagValueLen is the AWS limit for tag value length.
	maxTagValueLen = 256
)

// stateRegistered is the registration state for workspace directories.
const stateRegistered = "REGISTERED"

func isValidComputeTypeName(name string) bool {
	switch name {
	case "VALUE", "STANDARD", "PERFORMANCE", "POWER",
		"GRAPHICS", "GRAPHICSPRO", "POWERPRO",
		"GRAPHICS_G4DN", "GRAPHICSPRO_G4DN":
		return true
	}

	return false
}

func isValidRunningMode(mode string) bool {
	return mode == "ALWAYS_ON" || mode == "AUTO_STOP"
}

var (
	// ErrWorkspaceNotFound is returned when a workspace does not exist.
	ErrWorkspaceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrInvalidParameter is returned on invalid input.
	ErrInvalidParameter = awserr.New(errInvalidParameterValues, awserr.ErrInvalidParameter)
)

// storedWorkspace holds a workspace with all persisted fields.
type storedWorkspace struct {
	Properties                  *WorkspaceProperties `json:"properties,omitempty"`
	Tags                        map[string]string    `json:"tags"`
	WorkspaceID                 string               `json:"workspaceId"`
	DirectoryID                 string               `json:"directoryId"`
	UserName                    string               `json:"userName"`
	BundleID                    string               `json:"bundleId"`
	State                       string               `json:"state"`
	ComputerName                string               `json:"computerName"`
	SubnetID                    string               `json:"subnetId"`
	VolumeEncryptionKey         string               `json:"volumeEncryptionKey,omitempty"`
	ErrorCode                   string               `json:"errorCode"`
	ErrorMessage                string               `json:"errorMessage"`
	UserVolumeEncryptionEnabled bool                 `json:"userVolumeEncryptionEnabled"`
	RootVolumeEncryptionEnabled bool                 `json:"rootVolumeEncryptionEnabled"`
}

func (w *storedWorkspace) toWorkspace() *Workspace {
	tags := make(map[string]string)
	maps.Copy(tags, w.Tags)

	var props *WorkspaceProperties
	if w.Properties != nil {
		p := *w.Properties
		props = &p
	}

	return &Workspace{
		WorkspaceID:                 w.WorkspaceID,
		DirectoryID:                 w.DirectoryID,
		UserName:                    w.UserName,
		BundleID:                    w.BundleID,
		State:                       w.State,
		ComputerName:                w.ComputerName,
		SubnetID:                    w.SubnetID,
		VolumeEncryptionKey:         w.VolumeEncryptionKey,
		UserVolumeEncryptionEnabled: w.UserVolumeEncryptionEnabled,
		RootVolumeEncryptionEnabled: w.RootVolumeEncryptionEnabled,
		ErrorCode:                   w.ErrorCode,
		ErrorMessage:                w.ErrorMessage,
		Tags:                        tags,
		Properties:                  props,
	}
}

// backendSnapshot holds serializable backend state.
type backendSnapshot struct {
	Workspaces map[string]*storedWorkspace  `json:"workspaces"`
	Tags       map[string]map[string]string `json:"tags"`
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu                *lockmetrics.RWMutex
	workspaces        map[string]*storedWorkspace  // workspaceID → workspace
	tags              map[string]map[string]string // resourceID → tags
	ipGroups          map[string]*storedIpGroup
	directoryIpGroups map[string]map[string]struct{} //nolint:revive,staticcheck // existing issue.
	connAliases       map[string]*storedConnAlias
	customBundles     map[string]*storedCustomBundle
	images            map[string]*storedImage
	imagePermissions  map[string]map[string]bool
	pools             map[string]*storedPool
	poolSessions      map[string]*storedPoolSession
	connectAddIns     map[string]*storedConnectAddIn
	clientBranding    map[string]*storedClientBranding
	clientProperties  map[string]storedClientProps
	accountLinks      map[string]*storedAccountLink
	appAssociations   map[string]map[string]struct{}
	applications      map[string]*storedApplication
	dirSettings       map[string]*storedDirSettings
	accountConfig     storedAccountConfig
	accountID         string
	region            string
	counter           int
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:                lockmetrics.New("workspaces"),
		workspaces:        make(map[string]*storedWorkspace),
		tags:              make(map[string]map[string]string),
		ipGroups:          make(map[string]*storedIpGroup),
		directoryIpGroups: make(map[string]map[string]struct{}),
		connAliases:       make(map[string]*storedConnAlias),
		customBundles:     make(map[string]*storedCustomBundle),
		images:            make(map[string]*storedImage),
		imagePermissions:  make(map[string]map[string]bool),
		pools:             make(map[string]*storedPool),
		poolSessions:      make(map[string]*storedPoolSession),
		connectAddIns:     make(map[string]*storedConnectAddIn),
		clientBranding:    make(map[string]*storedClientBranding),
		clientProperties:  make(map[string]storedClientProps),
		accountLinks:      make(map[string]*storedAccountLink),
		appAssociations:   make(map[string]map[string]struct{}),
		applications:      make(map[string]*storedApplication),
		dirSettings:       make(map[string]*storedDirSettings),
		accountID:         accountID,
		region:            region,
	}
}

// CreateWorkspace creates a new WorkSpace and returns it.
func (b *InMemoryBackend) CreateWorkspace(spec *WorkspaceCreationSpec) (*Workspace, error) {
	b.mu.Lock("CreateWorkspace")
	defer b.mu.Unlock()

	b.counter++
	workspaceID := fmt.Sprintf("%s%0*x", workspaceIDPrefix, workspaceIDHexLen, b.counter)

	storedTags := make(map[string]string)
	maps.Copy(storedTags, spec.Tags)

	var props *WorkspaceProperties
	if spec.Properties != nil {
		p := *spec.Properties
		props = &p
	}

	w := &storedWorkspace{
		WorkspaceID:                 workspaceID,
		DirectoryID:                 spec.DirectoryID,
		UserName:                    spec.UserName,
		BundleID:                    spec.BundleID,
		SubnetID:                    spec.SubnetID,
		VolumeEncryptionKey:         spec.VolumeEncryptionKey,
		UserVolumeEncryptionEnabled: spec.UserVolumeEncryptionEnabled,
		RootVolumeEncryptionEnabled: spec.RootVolumeEncryptionEnabled,
		State:                       stateAvailable,
		Tags:                        storedTags,
		Properties:                  props,
	}

	b.workspaces[workspaceID] = w
	b.tags[workspaceID] = storedTags

	return w.toWorkspace(), nil
}

// DescribeWorkspaces returns workspaces matching the given filters.
// Results are sorted by WorkspaceId and paginated (max 25 per page, matching AWS).
func (b *InMemoryBackend) DescribeWorkspaces(
	workspaceIDs, directoryIDs, userIDs, bundleIDs []string,
	limit int32, nextToken string,
) ([]*Workspace, string, error) {
	b.mu.RLock("DescribeWorkspaces")
	defer b.mu.RUnlock()

	matched := b.filterWorkspaces(workspaceIDs, directoryIDs, userIDs, bundleIDs)

	sort.Slice(matched, func(i, j int) bool {
		return matched[i].WorkspaceID < matched[j].WorkspaceID
	})

	matched = advanceCursor(matched, nextToken)

	pageSize := resolvePageSize(limit)

	var newToken string

	if len(matched) > pageSize {
		newToken = base64.StdEncoding.EncodeToString([]byte(matched[pageSize].WorkspaceID))
		matched = matched[:pageSize]
	}

	result := make([]*Workspace, 0, len(matched))
	for _, w := range matched {
		result = append(result, w.toWorkspace())
	}

	return result, newToken, nil
}

// filterWorkspaces returns all stored workspaces that match all provided filters.
// Must be called with a read lock held.
func (b *InMemoryBackend) filterWorkspaces(
	workspaceIDs, directoryIDs, userIDs, bundleIDs []string,
) []*storedWorkspace {
	idFilter := buildFilter(workspaceIDs)
	dirFilter := buildFilter(directoryIDs)
	userFilter := buildFilter(userIDs)
	bundleFilter := buildFilter(bundleIDs)

	var matched []*storedWorkspace

	for _, w := range b.workspaces {
		if matchesFilter(idFilter, w.WorkspaceID) &&
			matchesFilter(dirFilter, w.DirectoryID) &&
			matchesFilter(userFilter, w.UserName) &&
			matchesFilter(bundleFilter, w.BundleID) {
			matched = append(matched, w)
		}
	}

	return matched
}

// advanceCursor removes all items that sort before the decoded nextToken cursor.
func advanceCursor(items []*storedWorkspace, nextToken string) []*storedWorkspace {
	if nextToken == "" {
		return items
	}

	cursorBytes, err := base64.StdEncoding.DecodeString(nextToken)
	if err != nil {
		return items
	}

	cursor := string(cursorBytes)

	for i, w := range items {
		if w.WorkspaceID >= cursor {
			return items[i:]
		}
	}

	return nil
}

// resolvePageSize clamps limit to the AWS-allowed range.
func resolvePageSize(limit int32) int {
	if limit <= 0 || int(limit) > describeWorkspacesMaxResults {
		return describeWorkspacesMaxResults
	}

	return int(limit)
}

// validateTagEntry checks a single tag key and value for AWS constraints.
func validateTagEntry(key, value string) error {
	if key == "" {
		return awserr.Newf(errInvalidParameterValues, awserr.ErrInvalidParameter,
			"tag key must not be empty")
	}

	if len(key) > maxTagKeyLen {
		return awserr.Newf(errInvalidParameterValues, awserr.ErrInvalidParameter,
			"tag key exceeds maximum length of %d", maxTagKeyLen)
	}

	if len(value) > maxTagValueLen {
		return awserr.Newf(errInvalidParameterValues, awserr.ErrInvalidParameter,
			"tag value for key %q exceeds maximum length of %d", key, maxTagValueLen)
	}

	return nil
}


// buildFilter converts a string slice to a set for O(1) membership tests.
// An empty result means "no filter" (accept all).
func buildFilter(ids []string) map[string]struct{} {
	if len(ids) == 0 {
		return nil
	}

	f := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		f[id] = struct{}{}
	}

	return f
}

// matchesFilter returns true when filter is empty (no filter) or value is in filter.
func matchesFilter(filter map[string]struct{}, value string) bool {
	if len(filter) == 0 {
		return true
	}

	_, ok := filter[value]

	return ok
}

// GetWorkspacesConnectionStatus returns connection status for the given workspace IDs.
// If no IDs are provided, returns status for all workspaces. AVAILABLE workspaces
// report DISCONNECTED (not yet connected in this emulator); STOPPED workspaces
// report NOT_CONNECTED, matching real AWS behaviour for offline workspaces.
func (b *InMemoryBackend) GetWorkspacesConnectionStatus(workspaceIDs []string) ([]*WorkspaceConnectionStatus, error) {
	b.mu.RLock("GetWorkspacesConnectionStatus")
	defer b.mu.RUnlock()

	connectionStateFor := func(state string) string {
		switch state {
		case stateStopped:
			return "NOT_CONNECTED"
		default:
			return "DISCONNECTED"
		}
	}

	if len(workspaceIDs) == 0 {
		result := make([]*WorkspaceConnectionStatus, 0, len(b.workspaces))

		for _, w := range b.workspaces {
			result = append(result, &WorkspaceConnectionStatus{
				WorkspaceID:       w.WorkspaceID,
				ConnectionState:   connectionStateFor(w.State),
				LastKnownUserTime: time.Time{},
			})
		}

		return result, nil
	}

	result := make([]*WorkspaceConnectionStatus, 0, len(workspaceIDs))

	for _, id := range workspaceIDs {
		w, ok := b.workspaces[id]
		if !ok {
			continue
		}

		result = append(result, &WorkspaceConnectionStatus{
			WorkspaceID:       w.WorkspaceID,
			ConnectionState:   connectionStateFor(w.State),
			LastKnownUserTime: time.Time{},
		})
	}

	return result, nil
}

// ModifyWorkspaceProperties updates and persists mutable properties of a WorkSpace.
// Returns InvalidParameterValuesException for unknown compute type names or running modes.
func (b *InMemoryBackend) ModifyWorkspaceProperties(workspaceID string, props WorkspaceProperties) error {
	if props.ComputeTypeName != "" && !isValidComputeTypeName(props.ComputeTypeName) {
		return awserr.Newf(errInvalidParameterValues, awserr.ErrInvalidParameter,
			"invalid ComputeTypeName: %q", props.ComputeTypeName)
	}

	if props.RunningMode != "" && !isValidRunningMode(props.RunningMode) {
		return awserr.Newf(errInvalidParameterValues, awserr.ErrInvalidParameter,
			"invalid RunningMode: %q, must be ALWAYS_ON or AUTO_STOP", props.RunningMode)
	}

	if props.RunningModeAutoStopTimeoutInMinutes != 0 {
		// AWS requires the timeout to be a multiple of 60 and between 60 and 600.
		t := props.RunningModeAutoStopTimeoutInMinutes
		if t < 60 || t > 600 || t%60 != 0 {
			return awserr.Newf(errInvalidParameterValues, awserr.ErrInvalidParameter,
				"RunningModeAutoStopTimeoutInMinutes must be a multiple of 60 between 60 and 600, got %d", t)
		}
	}

	b.mu.Lock("ModifyWorkspaceProperties")
	defer b.mu.Unlock()

	w, ok := b.workspaces[workspaceID]
	if !ok {
		return ErrWorkspaceNotFound
	}

	p := props
	w.Properties = &p

	return nil
}

// ModifyWorkspaceState updates the administrative state of a WorkSpace.
func (b *InMemoryBackend) ModifyWorkspaceState(workspaceID, state string) error {
	b.mu.Lock("ModifyWorkspaceState")
	defer b.mu.Unlock()

	w, ok := b.workspaces[workspaceID]
	if !ok {
		return ErrWorkspaceNotFound
	}

	if state != stateAvailable && state != stateAdminMaintenance {
		return ErrInvalidParameter
	}

	w.State = state

	return nil
}

// RebootWorkspaces reboots the given workspaces, returning failures for unknown IDs.
func (b *InMemoryBackend) RebootWorkspaces(workspaceIDs []string) ([]FailedRequest, error) {
	b.mu.Lock("RebootWorkspaces")
	defer b.mu.Unlock()

	return b.collectFailures(workspaceIDs, errResourceNotFound, errMsgNotFound), nil
}

// RebuildWorkspaces rebuilds the given workspaces, returning failures for unknown IDs.
func (b *InMemoryBackend) RebuildWorkspaces(workspaceIDs []string) ([]FailedRequest, error) {
	b.mu.Lock("RebuildWorkspaces")
	defer b.mu.Unlock()

	return b.collectFailures(workspaceIDs, errResourceNotFound, errMsgNotFound), nil
}

// StartWorkspaces starts the given workspaces, transitioning STOPPED workspaces to AVAILABLE.
func (b *InMemoryBackend) StartWorkspaces(workspaceIDs []string) ([]FailedRequest, error) {
	b.mu.Lock("StartWorkspaces")
	defer b.mu.Unlock()

	var failures []FailedRequest

	for _, id := range workspaceIDs {
		w, ok := b.workspaces[id]
		if !ok {
			failures = append(failures, FailedRequest{
				WorkspaceID:  id,
				ErrorCode:    errResourceNotFound,
				ErrorMessage: errMsgNotFound,
			})

			continue
		}

		if w.State == stateStopped {
			w.State = stateAvailable
		}
	}

	return failures, nil
}

// StopWorkspaces stops the given workspaces, transitioning them to STOPPED state.
func (b *InMemoryBackend) StopWorkspaces(workspaceIDs []string) ([]FailedRequest, error) {
	b.mu.Lock("StopWorkspaces")
	defer b.mu.Unlock()

	var failures []FailedRequest

	for _, id := range workspaceIDs {
		w, ok := b.workspaces[id]
		if !ok {
			failures = append(failures, FailedRequest{
				WorkspaceID:  id,
				ErrorCode:    errResourceNotFound,
				ErrorMessage: errMsgNotFound,
			})

			continue
		}

		if w.State == stateAvailable {
			w.State = stateStopped
		}
	}

	return failures, nil
}

// TerminateWorkspaces terminates (deletes) the given workspaces, returning failures for unknown IDs.
func (b *InMemoryBackend) TerminateWorkspaces(workspaceIDs []string) ([]FailedRequest, error) {
	b.mu.Lock("TerminateWorkspaces")
	defer b.mu.Unlock()

	var failures []FailedRequest

	for _, id := range workspaceIDs {
		if _, ok := b.workspaces[id]; !ok {
			failures = append(failures, FailedRequest{
				WorkspaceID:  id,
				ErrorCode:    errResourceNotFound,
				ErrorMessage: errMsgNotFound,
			})

			continue
		}

		delete(b.tags, id)
		delete(b.workspaces, id)
	}

	return failures, nil
}

// collectFailures returns FailedRequests for any workspace IDs not found.
// Must be called with a lock held.
func (b *InMemoryBackend) collectFailures(workspaceIDs []string, errCode, errMsg string) []FailedRequest {
	var failures []FailedRequest

	for _, id := range workspaceIDs {
		if _, ok := b.workspaces[id]; !ok {
			failures = append(failures, FailedRequest{
				WorkspaceID:  id,
				ErrorCode:    errCode,
				ErrorMessage: errMsg,
			})
		}
	}

	return failures
}

// CreateTags applies tags to a workspace resource ID.
// Returns InvalidParameterValuesException if tag key/value limits are exceeded
// or if applying the tags would exceed the 50-tag limit per resource.
func (b *InMemoryBackend) CreateTags(resourceID string, tags map[string]string) error {
	for k, v := range tags {
		if err := validateTagEntry(k, v); err != nil {
			return err
		}
	}

	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	existing := b.tags[resourceID]
	// Count distinct keys after merge to enforce 50-tag limit.
	newCount := len(existing)

	for k := range tags {
		if _, exists := existing[k]; !exists {
			newCount++
		}
	}

	if newCount > maxTagsPerResource {
		return awserr.Newf(errInvalidParameterValues, awserr.ErrInvalidParameter,
			"resource %q would exceed maximum tag count of %d", resourceID, maxTagsPerResource)
	}

	if b.tags[resourceID] == nil {
		b.tags[resourceID] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceID], tags)

	// Keep workspace tags in sync so DescribeWorkspaces reflects CreateTags changes.
	if w, ok := b.workspaces[resourceID]; ok {
		if w.Tags == nil {
			w.Tags = make(map[string]string)
		}

		maps.Copy(w.Tags, tags)
	}

	return nil
}

// DeleteTags removes tags from a workspace resource ID.
func (b *InMemoryBackend) DeleteTags(resourceID string, tagKeys []string) error {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	for _, k := range tagKeys {
		delete(b.tags[resourceID], k)

		if w, ok := b.workspaces[resourceID]; ok {
			delete(w.Tags, k)
		}
	}

	return nil
}

// DescribeTags returns tags for a workspace resource ID.
func (b *InMemoryBackend) DescribeTags(resourceID string) (map[string]string, error) {
	b.mu.RLock("DescribeTags")
	defer b.mu.RUnlock()

	result := make(map[string]string)
	maps.Copy(result, b.tags[resourceID])

	return result, nil
}

// DescribeWorkspaceBundles returns workspace bundles, optionally filtered by IDs or owner.
// When no owner is specified, returns both Amazon-owned and account-owned custom bundles.
// When owner is "Amazon", returns only Amazon-owned bundles.
// When owner is an account ID, returns custom bundles for that account.
func (b *InMemoryBackend) DescribeWorkspaceBundles(
	bundleIDs []string, owner string, _ string,
) ([]*WorkspaceBundle, string, error) {
	b.mu.RLock("DescribeWorkspaceBundles")
	defer b.mu.RUnlock()

	var bundles []*WorkspaceBundle

	// Include Amazon bundles unless the caller explicitly requests a specific account.
	if owner == "" || owner == ownerAmazon {
		bundles = append(bundles, hardcodedBundles()...)
	}

	// Include custom bundles when the caller wants all bundles or account-specific bundles.
	if owner != ownerAmazon {
		for _, bun := range b.customBundles {
			bundles = append(bundles, &WorkspaceBundle{
				BundleID:    bun.BundleID,
				Name:        bun.Name,
				Owner:       b.accountID,
				Description: bun.Description,
				ImageID:     bun.ImageID,
				ComputeType: BundleComputeType{Name: bun.ComputeType},
			})
		}
	}

	if len(bundleIDs) > 0 {
		idFilter := buildFilter(bundleIDs)
		filtered := bundles[:0]

		for _, bun := range bundles {
			if matchesFilter(idFilter, bun.BundleID) {
				filtered = append(filtered, bun)
			}
		}

		return filtered, "", nil
	}

	return bundles, "", nil
}

// hardcodedBundles returns the predefined Amazon-owned bundles with full AWS-accurate fields.
func hardcodedBundles() []*WorkspaceBundle {
	return []*WorkspaceBundle{
		{
			BundleID:    "wsb-bh8rsxt14",
			Name:        "Value",
			Owner:       ownerAmazon,
			Description: "Value with Windows 10 and Office 2019",
			ComputeType: BundleComputeType{Name: "VALUE"},
			UserStorage: BundleStorage{Capacity: 10},
			RootStorage: BundleStorage{Capacity: 80},
		},
		{
			BundleID:    "wsb-gm4d5tx2v",
			Name:        "Standard",
			Owner:       ownerAmazon,
			Description: "Standard with Windows 10 and Office 2019",
			ComputeType: BundleComputeType{Name: "STANDARD"},
			UserStorage: BundleStorage{Capacity: 50},
			RootStorage: BundleStorage{Capacity: 80},
		},
		{
			BundleID:    "wsb-b0s22j3d7",
			Name:        "Performance",
			Owner:       ownerAmazon,
			Description: "Performance with Windows 10 and Office 2019",
			ComputeType: BundleComputeType{Name: "PERFORMANCE"},
			UserStorage: BundleStorage{Capacity: 100},
			RootStorage: BundleStorage{Capacity: 80},
		},
		{
			BundleID:    "wsb-clj85qzj1",
			Name:        "Power",
			Owner:       ownerAmazon,
			Description: "Power with Windows 10 and Office 2019",
			ComputeType: BundleComputeType{Name: "POWER"},
			UserStorage: BundleStorage{Capacity: 100},
			RootStorage: BundleStorage{Capacity: 175},
		},
		{
			BundleID:    "wsb-1b5w9hkng",
			Name:        "PowerPro",
			Owner:       ownerAmazon,
			Description: "PowerPro with Windows 10 and Office 2019",
			ComputeType: BundleComputeType{Name: "POWERPRO"},
			UserStorage: BundleStorage{Capacity: 100},
			RootStorage: BundleStorage{Capacity: 175},
		},
	}
}

// DescribeWorkspaceDirectories returns workspace directories matching the given filters.
// Only directories that have been registered via RegisterWorkspaceDirectory are returned.
func (b *InMemoryBackend) DescribeWorkspaceDirectories(
	directoryIDs []string, _ string,
) ([]*WorkspaceDirectory, string, error) {
	b.mu.RLock("DescribeWorkspaceDirectories")
	defer b.mu.RUnlock()

	filter := buildFilter(directoryIDs)
	var result []*WorkspaceDirectory

	for id, ds := range b.dirSettings {
		if !matchesFilter(filter, id) {
			continue
		}

		state := ds.Properties["State"]
		if state == "" {
			state = stateRegistered
		}

		subnetRaw := ds.Properties["SubnetIds"]
		var subnetIDs []string

		if subnetRaw != "" {
			subnetIDs = strings.Split(subnetRaw, ",")
		}

		result = append(result, &WorkspaceDirectory{
			DirectoryID:   id,
			DirectoryName: ds.Properties["DirectoryName"],
			DirectoryType: ds.Properties["DirectoryType"],
			Alias:         ds.Properties["Alias"],
			State:         state,
			SubnetIDs:     subnetIDs,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].DirectoryID < result[j].DirectoryID
	})

	if result == nil {
		result = []*WorkspaceDirectory{}
	}

	return result, "", nil
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.workspaces = make(map[string]*storedWorkspace)
	b.tags = make(map[string]map[string]string)
	b.ipGroups = make(map[string]*storedIpGroup)
	b.directoryIpGroups = make(map[string]map[string]struct{})
	b.connAliases = make(map[string]*storedConnAlias)
	b.customBundles = make(map[string]*storedCustomBundle)
	b.images = make(map[string]*storedImage)
	b.imagePermissions = make(map[string]map[string]bool)
	b.pools = make(map[string]*storedPool)
	b.poolSessions = make(map[string]*storedPoolSession)
	b.connectAddIns = make(map[string]*storedConnectAddIn)
	b.clientBranding = make(map[string]*storedClientBranding)
	b.clientProperties = make(map[string]storedClientProps)
	b.accountLinks = make(map[string]*storedAccountLink)
	b.appAssociations = make(map[string]map[string]struct{})
	b.applications = make(map[string]*storedApplication)
	b.dirSettings = make(map[string]*storedDirSettings)
	b.accountConfig = storedAccountConfig{}
	b.counter = 0
}

// Snapshot serializes the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	data, _ := json.Marshal(backendSnapshot{
		Workspaces: b.workspaces,
		Tags:       b.tags,
	})

	return data
}

// Restore deserializes backend state from a snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	var snap backendSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	if snap.Workspaces != nil {
		b.workspaces = snap.Workspaces
	} else {
		b.workspaces = make(map[string]*storedWorkspace)
	}

	if snap.Tags != nil {
		b.tags = snap.Tags
	} else {
		b.tags = make(map[string]map[string]string)
	}

	return nil
}
