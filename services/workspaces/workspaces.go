package workspaces

import (
	"context"
	"encoding/base64"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	workspaceIDPrefix = "ws-"
	// AWS workspace IDs use 8 lowercase hex characters after the prefix.
	workspaceIDHexLen     = 8
	stateAvailable        = "AVAILABLE"
	stateAdminMaintenance = "ADMIN_MAINTENANCE"
	stateStopped          = "STOPPED"
	statePending          = "PENDING"
	errMsgNotFound        = "Workspace not found"

	// describeWorkspacesMaxResults is the AWS maximum results per page.
	describeWorkspacesMaxResults = 25
	// maxWorkspacesPerCreate is the AWS limit per CreateWorkspaces call.
	maxWorkspacesPerCreate = 25
)

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

// CreateWorkspace creates a new WorkSpace and returns it.
// Returns InvalidParameterValuesException when spec.DirectoryID is not registered.
func (b *InMemoryBackend) CreateWorkspace(
	ctx context.Context,
	spec *WorkspaceCreationSpec,
) (*Workspace, error) {
	region := b.regionFor(ctx)

	b.mu.Lock("CreateWorkspace")
	defer b.mu.Unlock()

	if !b.dirSettings.Has(spec.DirectoryID) {
		return nil, awserr.Newf(
			"directory %q is not registered", awserr.ErrInvalidParameter, spec.DirectoryID)
	}

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
		Region:                      region,
	}

	b.workspaces.Put(w)
	b.tags[workspaceID] = storedTags

	return w.toWorkspace(), nil
}

// DescribeWorkspaces returns workspaces matching the given filters.
// Results are sorted by WorkspaceId and paginated (max 25 per page, matching AWS).
func (b *InMemoryBackend) DescribeWorkspaces(
	ctx context.Context,
	workspaceIDs, directoryIDs, userIDs, bundleIDs []string,
	limit int32, nextToken string,
) ([]*Workspace, string, error) {
	b.mu.RLock("DescribeWorkspaces")
	defer b.mu.RUnlock()

	matched := b.filterWorkspaces(b.regionFor(ctx), workspaceIDs, directoryIDs, userIDs, bundleIDs)

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
	region string,
	workspaceIDs, directoryIDs, userIDs, bundleIDs []string,
) []*storedWorkspace {
	idFilter := buildFilter(workspaceIDs)
	dirFilter := buildFilter(directoryIDs)
	userFilter := buildFilter(userIDs)
	bundleFilter := buildFilter(bundleIDs)

	var matched []*storedWorkspace

	for _, w := range b.workspaces.All() {
		if region != "" && w.Region != "" && w.Region != region {
			continue
		}

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

// GetWorkspacesConnectionStatus returns connection status for the given workspace IDs.
// If no IDs are provided, returns status for all workspaces. AVAILABLE workspaces
// report DISCONNECTED (not yet connected in this emulator); STOPPED workspaces
// report NOT_CONNECTED, matching real AWS behaviour for offline workspaces.
func (b *InMemoryBackend) GetWorkspacesConnectionStatus(
	workspaceIDs []string,
) ([]*WorkspaceConnectionStatus, error) {
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

	// checkedAt is the timestamp of this connection-status check -- computed
	// once so every WorkSpace in the response reports the same check time,
	// matching a single point-in-time DescribeWorkspacesConnectionStatus call.
	checkedAt := time.Now().UTC()

	if len(workspaceIDs) == 0 {
		result := make([]*WorkspaceConnectionStatus, 0, b.workspaces.Len())

		for _, w := range b.workspaces.All() {
			result = append(result, &WorkspaceConnectionStatus{
				WorkspaceID:                   w.WorkspaceID,
				ConnectionState:               connectionStateFor(w.State),
				ConnectionStateCheckTimestamp: checkedAt,
			})
		}

		return result, nil
	}

	result := make([]*WorkspaceConnectionStatus, 0, len(workspaceIDs))

	for _, id := range workspaceIDs {
		w, ok := b.workspaces.Get(id)
		if !ok {
			continue
		}

		result = append(result, &WorkspaceConnectionStatus{
			WorkspaceID:                   w.WorkspaceID,
			ConnectionState:               connectionStateFor(w.State),
			ConnectionStateCheckTimestamp: checkedAt,
		})
	}

	return result, nil
}

// ModifyWorkspaceProperties updates and persists mutable properties of a WorkSpace.
// Returns InvalidParameterValuesException for unknown compute type names or running modes.
func (b *InMemoryBackend) ModifyWorkspaceProperties(
	workspaceID string,
	props WorkspaceProperties,
) error {
	if props.ComputeTypeName != "" && !isValidComputeTypeName(props.ComputeTypeName) {
		return awserr.Newf(
			"invalid ComputeTypeName: %q", awserr.ErrInvalidParameter, props.ComputeTypeName)
	}

	if props.RunningMode != "" && !isValidRunningMode(props.RunningMode) {
		return awserr.Newf(
			"invalid RunningMode: %q, must be ALWAYS_ON or AUTO_STOP",
			awserr.ErrInvalidParameter, props.RunningMode)
	}

	if props.RunningModeAutoStopTimeoutInMinutes != 0 {
		// AWS requires the timeout to be a multiple of 60 and between 60 and 600.
		t := props.RunningModeAutoStopTimeoutInMinutes
		if t < 60 || t > 600 || t%60 != 0 {
			return awserr.Newf(
				"RunningModeAutoStopTimeoutInMinutes must be a multiple of 60 between 60 and 600, got %d",
				awserr.ErrInvalidParameter,
				t,
			)
		}
	}

	b.mu.Lock("ModifyWorkspaceProperties")
	defer b.mu.Unlock()

	w, ok := b.workspaces.Get(workspaceID)
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

	w, ok := b.workspaces.Get(workspaceID)
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
		w, ok := b.workspaces.Get(id)
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
		w, ok := b.workspaces.Get(id)
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
		if !b.workspaces.Has(id) {
			failures = append(failures, FailedRequest{
				WorkspaceID:  id,
				ErrorCode:    errResourceNotFound,
				ErrorMessage: errMsgNotFound,
			})

			continue
		}

		delete(b.tags, id)
		b.workspaces.Delete(id)
	}

	return failures, nil
}

// collectFailures returns FailedRequests for any workspace IDs not found.
// Must be called with a lock held.
func (b *InMemoryBackend) collectFailures(
	workspaceIDs []string,
	errCode, errMsg string,
) []FailedRequest {
	var failures []FailedRequest

	for _, id := range workspaceIDs {
		if !b.workspaces.Has(id) {
			failures = append(failures, FailedRequest{
				WorkspaceID:  id,
				ErrorCode:    errCode,
				ErrorMessage: errMsg,
			})
		}
	}

	return failures
}

// MigrateWorkspace migrates a workspace to a new bundle.
func (b *InMemoryBackend) MigrateWorkspace( //nolint:nonamedreturns // existing issue.
	sourceWorkspaceID, bundleID string,
) (sourceID, targetID string, err error) {
	b.mu.Lock("MigrateWorkspace")
	defer b.mu.Unlock()

	src, ok := b.workspaces.Get(sourceWorkspaceID)
	if !ok {
		return "", "", ErrWorkspaceNotFound
	}

	b.counter++
	newID := fmt.Sprintf("%s%0*x", workspaceIDPrefix, workspaceIDHexLen, b.counter)

	newWs := &storedWorkspace{
		WorkspaceID: newID,
		DirectoryID: src.DirectoryID,
		UserName:    src.UserName,
		BundleID:    bundleID,
		State:       stateAvailable,
		Tags:        cloneTags(src.Tags),
	}
	b.workspaces.Put(newWs)

	// Terminate old
	b.workspaces.Delete(sourceWorkspaceID)
	delete(b.tags, sourceWorkspaceID)

	return sourceWorkspaceID, newID, nil
}

// RestoreWorkspace restores a WorkSpace from its most recent snapshot. This backend
// does not model snapshots, so the operation is otherwise a no-op beyond existence
// validation, matching real AWS's ResourceNotFoundException for unknown WorkspaceIds.
func (b *InMemoryBackend) RestoreWorkspace(workspaceID string) error {
	b.mu.RLock("RestoreWorkspace")
	defer b.mu.RUnlock()

	if !b.workspaces.Has(workspaceID) {
		return ErrWorkspaceNotFound
	}

	return nil
}

// CreateStandbyWorkspace creates a single standby WorkSpace and returns it in
// PENDING state. Returns InvalidParameterValuesException when spec.DirectoryID
// is not registered, matching the same per-item runtime validation as
// CreateWorkspace. The real StandbyWorkspace request shape carries no
// UserName/BundleId (see StandbyWorkspaceSpec) -- those fields belong to the
// primary WorkSpace, which may live in a different region's backend that this
// in-memory store cannot see, so the created record has no way to inherit
// them; PendingCreateStandbyWorkspacesRequest's real shape doesn't surface
// BundleId at all, and its UserName is left empty for the same reason.
func (b *InMemoryBackend) CreateStandbyWorkspace(
	_ context.Context, spec StandbyWorkspaceSpec,
) (*PendingStandbyWorkspace, error) {
	b.mu.Lock("CreateStandbyWorkspace")
	defer b.mu.Unlock()

	if !b.dirSettings.Has(spec.DirectoryID) {
		return nil, awserr.Newf(
			"directory %q is not registered", awserr.ErrInvalidParameter, spec.DirectoryID)
	}

	id := b.nextID(workspaceIDPrefix)
	tags := cloneTags(spec.Tags)

	w := &storedWorkspace{
		WorkspaceID:         id,
		DirectoryID:         spec.DirectoryID,
		VolumeEncryptionKey: spec.VolumeEncryptionKey,
		State:               statePending,
		Tags:                tags,
	}
	b.workspaces.Put(w)
	b.tags[id] = tags

	return &PendingStandbyWorkspace{
		WorkspaceID: id,
		DirectoryID: spec.DirectoryID,
		State:       statePending,
	}, nil
}
