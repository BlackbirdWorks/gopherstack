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
func (b *InMemoryBackend) RegisterWorkspaceDirectory(directoryID string, subnetIDs []string) error {
	b.mu.Lock("RegisterWorkspaceDirectory")
	defer b.mu.Unlock()

	b.ensureDirSettings(directoryID)

	ds, _ := b.dirSettings.Get(directoryID)
	ds.Properties["State"] = stateRegistered

	if len(subnetIDs) > 0 {
		ds.Properties["SubnetIds"] = strings.Join(subnetIDs, ",")
	}

	return nil
}

// DeregisterWorkspaceDirectory deregisters a directory.
func (b *InMemoryBackend) DeregisterWorkspaceDirectory(directoryID string) error {
	b.mu.Lock("DeregisterWorkspaceDirectory")
	defer b.mu.Unlock()

	b.dirSettings.Delete(directoryID)

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

// ModifyWorkspaceCreationProperties stores workspace creation properties for a directory.
func (b *InMemoryBackend) ModifyWorkspaceCreationProperties(
	directoryID string,
	props map[string]string,
) error {
	b.mu.Lock("ModifyWorkspaceCreationProperties")
	defer b.mu.Unlock()

	b.ensureDirSettings(directoryID)

	ds, _ := b.dirSettings.Get(directoryID)
	for k, v := range props {
		ds.Properties["Creation_"+k] = v
	}

	return nil
}
