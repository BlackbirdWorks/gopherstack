package workspaces

import (
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	errResourceNotFound       = "ResourceNotFoundException"
	errInvalidParameterValues = "InvalidParameterValuesException"
	workspaceIDPrefix         = "ws-"
	// AWS workspace IDs use 8 lowercase hex characters after the prefix.
	workspaceIDHexLen    = 8
	stateAvailable       = "AVAILABLE"
	stateAdminMaintenance = "ADMIN_MAINTENANCE"
	stateStopped          = "STOPPED"
	statePending          = "PENDING"
)

var (
	// ErrWorkspaceNotFound is returned when a workspace does not exist.
	ErrWorkspaceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrInvalidParameter is returned on invalid input.
	ErrInvalidParameter = awserr.New(errInvalidParameterValues, awserr.ErrInvalidParameter)
)

// storedWorkspace holds a workspace with all persisted fields.
type storedWorkspace struct {
	Properties   *WorkspaceProperties  `json:"properties,omitempty"`
	Tags         map[string]string     `json:"tags"`
	WorkspaceID  string                `json:"workspaceId"`
	DirectoryID  string                `json:"directoryId"`
	UserName     string                `json:"userName"`
	BundleID     string                `json:"bundleId"`
	State        string                `json:"state"`
	ComputerName string                `json:"computerName"`
	SubnetID     string                `json:"subnetId"`
	ErrorCode    string                `json:"errorCode"`
	ErrorMessage string                `json:"errorMessage"`
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
		WorkspaceID:  w.WorkspaceID,
		DirectoryID:  w.DirectoryID,
		UserName:     w.UserName,
		BundleID:     w.BundleID,
		State:        w.State,
		ComputerName: w.ComputerName,
		SubnetID:     w.SubnetID,
		ErrorCode:    w.ErrorCode,
		ErrorMessage: w.ErrorMessage,
		Tags:         tags,
		Properties:   props,
	}
}

// backendSnapshot holds serializable backend state.
type backendSnapshot struct {
	Workspaces map[string]*storedWorkspace  `json:"workspaces"`
	Tags       map[string]map[string]string `json:"tags"`
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu         *lockmetrics.RWMutex
	workspaces map[string]*storedWorkspace  // workspaceID → workspace
	tags       map[string]map[string]string // resourceID → tags
	accountID  string
	region     string
	counter    int
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:         lockmetrics.New("workspaces"),
		workspaces: make(map[string]*storedWorkspace),
		tags:       make(map[string]map[string]string),
		accountID:  accountID,
		region:     region,
	}
}

// CreateWorkspace creates a new WorkSpace and returns it.
func (b *InMemoryBackend) CreateWorkspace(
	userID, directoryID, bundleID string,
	tags map[string]string,
) (*Workspace, error) {
	b.mu.Lock("CreateWorkspace")
	defer b.mu.Unlock()

	b.counter++
	workspaceID := fmt.Sprintf("%s%0*x", workspaceIDPrefix, workspaceIDHexLen, b.counter)

	storedTags := make(map[string]string)
	maps.Copy(storedTags, tags)

	w := &storedWorkspace{
		WorkspaceID: workspaceID,
		DirectoryID: directoryID,
		UserName:    userID,
		BundleID:    bundleID,
		State:       stateAvailable,
		Tags:        storedTags,
	}

	b.workspaces[workspaceID] = w
	b.tags[workspaceID] = storedTags

	return w.toWorkspace(), nil
}

// DescribeWorkspaces returns workspaces matching the given filters.
func (b *InMemoryBackend) DescribeWorkspaces(
	workspaceIDs, directoryIDs, userIDs, bundleIDs []string,
	limit int32, nextToken string,
) ([]*Workspace, string, error) {
	b.mu.RLock("DescribeWorkspaces")
	defer b.mu.RUnlock()

	idFilter := make(map[string]struct{}, len(workspaceIDs))
	for _, id := range workspaceIDs {
		idFilter[id] = struct{}{}
	}

	dirFilter := make(map[string]struct{}, len(directoryIDs))
	for _, id := range directoryIDs {
		dirFilter[id] = struct{}{}
	}

	userFilter := make(map[string]struct{}, len(userIDs))
	for _, id := range userIDs {
		userFilter[id] = struct{}{}
	}

	bundleFilter := make(map[string]struct{}, len(bundleIDs))
	for _, id := range bundleIDs {
		bundleFilter[id] = struct{}{}
	}

	var result []*Workspace

	for _, w := range b.workspaces {
		if len(idFilter) > 0 {
			if _, ok := idFilter[w.WorkspaceID]; !ok {
				continue
			}
		}

		if len(dirFilter) > 0 {
			if _, ok := dirFilter[w.DirectoryID]; !ok {
				continue
			}
		}

		if len(userFilter) > 0 {
			if _, ok := userFilter[w.UserName]; !ok {
				continue
			}
		}

		if len(bundleFilter) > 0 {
			if _, ok := bundleFilter[w.BundleID]; !ok {
				continue
			}
		}

		result = append(result, w.toWorkspace())
	}

	return result, "", nil
}

// GetWorkspacesConnectionStatus returns connection status for the given workspace IDs.
func (b *InMemoryBackend) GetWorkspacesConnectionStatus(workspaceIDs []string) ([]*WorkspaceConnectionStatus, error) {
	b.mu.RLock("GetWorkspacesConnectionStatus")
	defer b.mu.RUnlock()

	result := make([]*WorkspaceConnectionStatus, 0, len(workspaceIDs))

	for _, id := range workspaceIDs {
		w, ok := b.workspaces[id]
		if !ok {
			continue
		}

		result = append(result, &WorkspaceConnectionStatus{
			WorkspaceID:       w.WorkspaceID,
			ConnectionState:   "UNKNOWN",
			LastKnownUserTime: time.Time{},
		})
	}

	return result, nil
}

// ModifyWorkspaceProperties updates and persists mutable properties of a WorkSpace.
func (b *InMemoryBackend) ModifyWorkspaceProperties(workspaceID string, props WorkspaceProperties) error {
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

	return b.collectFailures(workspaceIDs, "ResourceNotFoundException", "Workspace not found"), nil
}

// RebuildWorkspaces rebuilds the given workspaces, returning failures for unknown IDs.
func (b *InMemoryBackend) RebuildWorkspaces(workspaceIDs []string) ([]FailedRequest, error) {
	b.mu.Lock("RebuildWorkspaces")
	defer b.mu.Unlock()

	return b.collectFailures(workspaceIDs, "ResourceNotFoundException", "Workspace not found"), nil
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
				ErrorCode:    "ResourceNotFoundException",
				ErrorMessage: "Workspace not found",
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
				ErrorCode:    "ResourceNotFoundException",
				ErrorMessage: "Workspace not found",
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
				ErrorCode:    "ResourceNotFoundException",
				ErrorMessage: "Workspace not found",
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
func (b *InMemoryBackend) CreateTags(resourceID string, tags map[string]string) error {
	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

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
func (b *InMemoryBackend) DescribeWorkspaceBundles(
	bundleIDs []string, owner string, _ string,
) ([]*WorkspaceBundle, string, error) {
	bundles := hardcodedBundles()

	if len(bundleIDs) > 0 {
		idFilter := make(map[string]struct{}, len(bundleIDs))
		for _, id := range bundleIDs {
			idFilter[id] = struct{}{}
		}

		filtered := bundles[:0]
		for _, bun := range bundles {
			if _, ok := idFilter[bun.BundleID]; ok {
				filtered = append(filtered, bun)
			}
		}

		return filtered, "", nil
	}

	if owner != "" && owner != "Amazon" {
		return []*WorkspaceBundle{}, "", nil
	}

	return bundles, "", nil
}

// hardcodedBundles returns the predefined Amazon-owned bundles.
func hardcodedBundles() []*WorkspaceBundle {
	return []*WorkspaceBundle{
		{
			BundleID:    "wsb-bh8rsxt14",
			Name:        "Value",
			Owner:       "Amazon",
			Description: "Value with Windows 10 and Office 2019",
		},
		{
			BundleID:    "wsb-gm4d5tx2v",
			Name:        "Standard",
			Owner:       "Amazon",
			Description: "Standard with Windows 10 and Office 2019",
		},
		{
			BundleID:    "wsb-b0s22j3d7",
			Name:        "Performance",
			Owner:       "Amazon",
			Description: "Performance with Windows 10 and Office 2019",
		},
	}
}

// DescribeWorkspaceDirectories returns workspace directories matching the given filters.
func (b *InMemoryBackend) DescribeWorkspaceDirectories(
	_ []string, _ string,
) ([]*WorkspaceDirectory, string, error) {
	return []*WorkspaceDirectory{}, "", nil
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
