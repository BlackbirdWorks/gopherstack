package workspaces

import (
	"context"
	"encoding/base64"
	"sort"
	"strings"
)

// directoriesPageSize is the AWS default page size for DescribeWorkspaceDirectories.
const directoriesPageSize = 50

// stateRegistered is the registration state for workspace directories.
const stateRegistered = "REGISTERED"

// DescribeWorkspaceDirectories returns workspace directories matching the given filters.
// Only directories that have been registered via RegisterWorkspaceDirectory are returned.
// Results are sorted by DirectoryID and paginated (max 50 per page, matching AWS).
func (b *InMemoryBackend) DescribeWorkspaceDirectories(
	_ context.Context,
	directoryIDs []string, nextToken string,
) ([]*WorkspaceDirectory, string, error) {
	b.mu.RLock("DescribeWorkspaceDirectories")
	defer b.mu.RUnlock()

	filter := buildFilter(directoryIDs)
	var result []*WorkspaceDirectory

	for _, ds := range b.dirSettings.All() {
		id := ds.DirectoryID
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

	result = advanceDirCursor(result, nextToken)

	var newToken string

	if len(result) > directoriesPageSize {
		newToken = base64.StdEncoding.EncodeToString(
			[]byte(result[directoriesPageSize].DirectoryID),
		)
		result = result[:directoriesPageSize]
	}

	return result, newToken, nil
}

// advanceDirCursor removes all directories that sort before the decoded nextToken cursor.
func advanceDirCursor(dirs []*WorkspaceDirectory, nextToken string) []*WorkspaceDirectory {
	if nextToken == "" {
		return dirs
	}

	cursorBytes, err := base64.StdEncoding.DecodeString(nextToken)
	if err != nil {
		return dirs
	}

	cursor := string(cursorBytes)

	for i, d := range dirs {
		if d.DirectoryID >= cursor {
			return dirs[i:]
		}
	}

	return nil
}

// RegisterWorkspaceDirectory registers a directory and stores subnet IDs.
// Returns ResourceAlreadyExistsException when the directory is already
// registered, matching real AWS: you cannot re-register an already-registered
// directory.
func (b *InMemoryBackend) RegisterWorkspaceDirectory(directoryID string, subnetIDs []string) error {
	b.mu.Lock("RegisterWorkspaceDirectory")
	defer b.mu.Unlock()

	if ds, ok := b.dirSettings.Get(directoryID); ok && ds.Properties["State"] == stateRegistered {
		return errDirectoryAlreadyRegistered
	}

	b.ensureDirSettings(directoryID)

	ds, _ := b.dirSettings.Get(directoryID)
	ds.Properties["State"] = stateRegistered

	if len(subnetIDs) > 0 {
		ds.Properties["SubnetIds"] = strings.Join(subnetIDs, ",")
	}

	return nil
}

// DeregisterWorkspaceDirectory deregisters a directory. Returns
// InvalidResourceStateException when any WorkSpaces are still registered to
// the directory, matching real AWS: "If any WorkSpaces are registered to
// this directory, you must remove them before you can deregister the
// directory" -- this backend never auto-cascade-deletes WorkSpaces on
// deregister, since real AWS doesn't either.
func (b *InMemoryBackend) DeregisterWorkspaceDirectory(directoryID string) error {
	b.mu.Lock("DeregisterWorkspaceDirectory")
	defer b.mu.Unlock()

	for _, w := range b.workspaces.All() {
		if w.DirectoryID == directoryID {
			return errDirectoryHasWorkspaces
		}
	}

	b.dirSettings.Delete(directoryID)
	delete(b.directoryIpGroups, directoryID)

	return nil
}

// ensureDirSettings ensures a storedDirSettings exists for a directory (must hold lock).
func (b *InMemoryBackend) ensureDirSettings(directoryID string) {
	if !b.dirSettings.Has(directoryID) {
		b.dirSettings.Put(&storedDirSettings{
			DirectoryID: directoryID,
			Properties:  make(map[string]string),
		})
	}
}

// isDirectoryRegisteredLocked reports whether directoryID was registered via
// RegisterWorkspaceDirectory -- not merely present in b.dirSettings, since
// ensureDirSettings can create a bare row before State is set. Callers must
// hold b.mu.
func (b *InMemoryBackend) isDirectoryRegisteredLocked(directoryID string) bool {
	ds, ok := b.dirSettings.Get(directoryID)

	return ok && ds.Properties["State"] == stateRegistered
}

// ModifyWorkspaceCreationProperties stores workspace creation properties for
// a registered directory. Returns errDirectoryNotFound for a DirectoryId
// that was never registered, matching real AWS (ResourceNotFoundException is
// in this operation's error list).
func (b *InMemoryBackend) ModifyWorkspaceCreationProperties(
	directoryID string,
	props map[string]string,
) error {
	b.mu.Lock("ModifyWorkspaceCreationProperties")
	defer b.mu.Unlock()

	if !b.isDirectoryRegisteredLocked(directoryID) {
		return errDirectoryNotFound
	}

	ds, _ := b.dirSettings.Get(directoryID)
	for k, v := range props {
		ds.Properties["Creation_"+k] = v
	}

	return nil
}
