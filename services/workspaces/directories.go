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
			DirectoryID:                    id,
			DirectoryName:                  ds.Properties["DirectoryName"],
			DirectoryType:                  ds.Properties["DirectoryType"],
			Alias:                          ds.Properties["Alias"],
			State:                          state,
			SubnetIDs:                      subnetIDs,
			IPGroupIDs:                     b.directoryIPGroupIDsLocked(id),
			EndpointEncryptionMode:         ds.Properties["EndpointEncryptionMode"],
			CertificateBasedAuthProperties: certBasedAuthPropertiesFromDS(ds),
			SamlProperties:                 samlPropertiesFromDS(ds),
			SelfservicePermissions:         selfservicePermissionsFromDS(ds),
			WorkspaceAccessProperties:      workspaceAccessPropertiesFromDS(ds),
			WorkspaceCreationProperties:    workspaceCreationPropertiesFromDS(ds),
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

// directoryIPGroupIDsLocked returns the sorted IP group IDs associated with
// a directory (real WorkspaceDirectory.IpGroupIds, wire key "ipGroupIds" --
// unusually lowercase-led for this awsjson1.1 API, deserializers.go:18124).
// Caller must hold at least b.mu.RLock.
func (b *InMemoryBackend) directoryIPGroupIDsLocked(directoryID string) []string {
	groups := b.directoryIpGroups[directoryID]
	if len(groups) == 0 {
		return nil
	}

	ids := make([]string, 0, len(groups))
	for gid := range groups {
		ids = append(ids, gid)
	}

	sort.Strings(ids)

	return ids
}

// certBasedAuthPropertiesFromDS reads back what ModifyCertificateBasedAuthProperties
// stored under the "CertAuth_" key prefix. Returns nil (omitted on the wire)
// if the directory was never touched by that op, matching real AWS's
// pointer-typed CertificateBasedAuthProperties member.
func certBasedAuthPropertiesFromDS(ds *storedDirSettings) *CertificateBasedAuthProperties {
	status, hasStatus := ds.Properties["CertAuth_Status"]
	arn, hasArn := ds.Properties["CertAuth_CertificateAuthorityArn"]

	if !hasStatus && !hasArn {
		return nil
	}

	return &CertificateBasedAuthProperties{Status: status, CertificateAuthorityArn: arn}
}

// samlPropertiesFromDS reads back what ModifySamlProperties stored under the
// "Saml_" key prefix. See certBasedAuthPropertiesFromDS for the nil-when-unset rule.
func samlPropertiesFromDS(ds *storedDirSettings) *SamlProperties {
	status, hasStatus := ds.Properties["Saml_Status"]
	url, hasURL := ds.Properties["Saml_UserAccessUrl"]
	relayState, hasRelayState := ds.Properties["Saml_RelayStateParameterName"]

	if !hasStatus && !hasURL && !hasRelayState {
		return nil
	}

	return &SamlProperties{Status: status, UserAccessUrl: url, RelayStateParameterName: relayState}
}

// selfservicePermissionsFromDS reads back what ModifySelfservicePermissions
// stored under the "SelfSvc_" key prefix. See certBasedAuthPropertiesFromDS
// for the nil-when-unset rule.
func selfservicePermissionsFromDS(ds *storedDirSettings) *SelfservicePermissions {
	keys := []string{
		"SelfSvc_RestartWorkspace", "SelfSvc_IncreaseVolumeSize", "SelfSvc_ChangeComputeType",
		"SelfSvc_SwitchRunningMode", "SelfSvc_RebuildWorkspace",
	}
	if !dsHasAnyKey(ds, keys) {
		return nil
	}

	return &SelfservicePermissions{
		RestartWorkspace:   ds.Properties["SelfSvc_RestartWorkspace"],
		IncreaseVolumeSize: ds.Properties["SelfSvc_IncreaseVolumeSize"],
		ChangeComputeType:  ds.Properties["SelfSvc_ChangeComputeType"],
		SwitchRunningMode:  ds.Properties["SelfSvc_SwitchRunningMode"],
		RebuildWorkspace:   ds.Properties["SelfSvc_RebuildWorkspace"],
	}
}

// workspaceAccessPropertiesFromDS reads back what
// ModifyWorkspaceAccessProperties stored under the "Access_" key prefix. See
// certBasedAuthPropertiesFromDS for the nil-when-unset rule.
func workspaceAccessPropertiesFromDS(ds *storedDirSettings) *WorkspaceAccessProperties {
	keys := []string{
		"Access_DeviceTypeWindows", "Access_DeviceTypeOsx", "Access_DeviceTypeWeb",
		"Access_DeviceTypeIos", "Access_DeviceTypeAndroid", "Access_DeviceTypeChromeOs",
		"Access_DeviceTypeZeroClient", "Access_DeviceTypeLinux",
	}
	if !dsHasAnyKey(ds, keys) {
		return nil
	}

	return &WorkspaceAccessProperties{
		DeviceTypeWindows:    ds.Properties["Access_DeviceTypeWindows"],
		DeviceTypeOsx:        ds.Properties["Access_DeviceTypeOsx"],
		DeviceTypeWeb:        ds.Properties["Access_DeviceTypeWeb"],
		DeviceTypeIos:        ds.Properties["Access_DeviceTypeIos"],
		DeviceTypeAndroid:    ds.Properties["Access_DeviceTypeAndroid"],
		DeviceTypeChromeOs:   ds.Properties["Access_DeviceTypeChromeOs"],
		DeviceTypeZeroClient: ds.Properties["Access_DeviceTypeZeroClient"],
		DeviceTypeLinux:      ds.Properties["Access_DeviceTypeLinux"],
	}
}

// workspaceCreationPropertiesFromDS reads back what
// ModifyWorkspaceCreationProperties stored under its "Creation_" key
// prefix. See certBasedAuthPropertiesFromDS for the nil-when-unset rule.
func workspaceCreationPropertiesFromDS(ds *storedDirSettings) *WorkspaceCreationProperties {
	if !dsHasAnyKey(ds, []string{"Creation_DefaultOu", "Creation_CustomSecurityGroupId"}) {
		return nil
	}

	return &WorkspaceCreationProperties{
		DefaultOu:             ds.Properties["Creation_DefaultOu"],
		CustomSecurityGroupId: ds.Properties["Creation_CustomSecurityGroupId"],
	}
}

// dsHasAnyKey reports whether ds.Properties contains at least one of keys.
func dsHasAnyKey(ds *storedDirSettings, keys []string) bool {
	for _, k := range keys {
		if _, ok := ds.Properties[k]; ok {
			return true
		}
	}

	return false
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
func (b *InMemoryBackend) RegisterWorkspaceDirectory(
	directoryID string,
	subnetIDs []string,
	tags map[string]string,
) error {
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

	if len(tags) > 0 {
		b.tags[directoryID] = cloneTags(tags)
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
