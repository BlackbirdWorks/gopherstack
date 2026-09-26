package workspaces

import (
	"context"
	"encoding/base64"
	"sort"
	"strconv"
	"strings"

	directoryservicebackend "github.com/blackbirdworks/gopherstack/services/directoryservice"
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
	directoryIDs []string, directoryNames []string, limit int32, nextToken string,
) ([]*WorkspaceDirectory, string, error) {
	b.mu.RLock("DescribeWorkspaceDirectories")
	defer b.mu.RUnlock()

	idFilter := buildFilter(directoryIDs)
	nameFilter := buildFilter(directoryNames)
	var result []*WorkspaceDirectory

	for _, ds := range b.dirSettings.All() {
		id := ds.DirectoryID
		if !matchesFilter(idFilter, id) || !matchesFilter(nameFilter, ds.Properties["DirectoryName"]) {
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

		var dnsIPs []string
		if dnsRaw := ds.Properties["DnsIpAddresses"]; dnsRaw != "" {
			dnsIPs = strings.Split(dnsRaw, ",")
		}

		result = append(result, &WorkspaceDirectory{
			DirectoryID:                    id,
			DirectoryName:                  ds.Properties["DirectoryName"],
			DirectoryType:                  ds.Properties["DirectoryType"],
			Alias:                          ds.Properties["Alias"],
			CustomerUserName:               ds.Properties["CustomerUserName"],
			State:                          state,
			SubnetIDs:                      subnetIDs,
			DNSIPAddresses:                 dnsIPs,
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

	pageSize := directoriesPageSize
	if limit > 0 && int(limit) < pageSize {
		pageSize = int(limit)
	}

	var newToken string

	if len(result) > pageSize {
		newToken = base64.StdEncoding.EncodeToString(
			[]byte(result[pageSize].DirectoryID),
		)
		result = result[:pageSize]
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
	keys := []string{
		"Creation_DefaultOu", "Creation_CustomSecurityGroupId", "Creation_EnableInternetAccess",
		"Creation_EnableMaintenanceMode", "Creation_UserEnabledAsLocalAdministrator",
	}
	if !dsHasAnyKey(ds, keys) {
		return nil
	}

	return &WorkspaceCreationProperties{
		DefaultOu:                       ds.Properties["Creation_DefaultOu"],
		CustomSecurityGroupId:           ds.Properties["Creation_CustomSecurityGroupId"],
		EnableInternetAccess:            dsBoolPtr(ds, "Creation_EnableInternetAccess"),
		EnableMaintenanceMode:           dsBoolPtr(ds, "Creation_EnableMaintenanceMode"),
		UserEnabledAsLocalAdministrator: dsBoolPtr(ds, "Creation_UserEnabledAsLocalAdministrator"),
	}
}

// dsBoolPtr parses ds.Properties[key] as a bool, returning nil if the key was
// never set (matching the nil-when-unset rule certBasedAuthPropertiesFromDS
// documents) or holds an unparseable value.
func dsBoolPtr(ds *storedDirSettings, key string) *bool {
	v, ok := ds.Properties[key]
	if !ok {
		return nil
	}

	b, err := strconv.ParseBool(v)
	if err != nil {
		return nil
	}

	return &b
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
//
// requestedName is the RegisterWorkspaceDirectoryInput.WorkspaceDirectoryName
// the caller supplied, if any. It is only used as a DirectoryName fallback
// when the Directory Service backend isn't wired or the directory can't be
// found there -- see resolveDirectoryInfo.
func (b *InMemoryBackend) RegisterWorkspaceDirectory(
	directoryID string,
	subnetIDs []string,
	tags map[string]string,
	requestedName string,
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

	b.populateDirectoryInfoLocked(ds, directoryID, requestedName)

	return nil
}

// populateDirectoryInfoLocked fills in DirectoryName and the other
// directory-derived properties from the Directory Service backend, if it's
// reachable and knows about directoryID. Falls back to requestedName for
// DirectoryName alone when it isn't -- real AWS's DirectoryName always comes
// from the AD directory itself, but this backend has no such directory to
// read when Directory Service isn't wired (e.g. a unit test constructing
// InMemoryBackend directly), so a caller-supplied name is the next best
// honest source. Caller must hold b.mu.
func (b *InMemoryBackend) populateDirectoryInfoLocked(ds *storedDirSettings, directoryID, requestedName string) {
	dir, ok := b.resolveDirectoryInfo(directoryID)
	if !ok {
		if requestedName != "" {
			ds.Properties["DirectoryName"] = requestedName
		}

		return
	}

	ds.Properties["DirectoryName"] = dir.Name
	ds.Properties["Alias"] = dir.Alias

	if dirType := workspaceDirectoryType(dir.Type); dirType != "" {
		ds.Properties["DirectoryType"] = dirType
	}

	if len(dir.DNSIPAddrs) > 0 {
		ds.Properties["DnsIpAddresses"] = strings.Join(dir.DNSIPAddrs, ",")
	}

	if dir.ConnectSettings != nil && dir.ConnectSettings.CustomerUserName != "" {
		ds.Properties["CustomerUserName"] = dir.ConnectSettings.CustomerUserName
	}
}

// workspaceDirectoryType maps a Directory Service DirectoryType to the
// WorkspaceDirectoryType wire value. SimpleAD and ADConnector map 1:1 by
// name; MicrosoftAD/SharedMicrosoftAD have no documented, unambiguous
// WorkspaceDirectoryType equivalent (CUSTOMER_MANAGED is undocumented by
// AWS -- see PARITY.md), so this returns "" rather than guess.
func workspaceDirectoryType(t directoryservicebackend.DirectoryType) string {
	switch t {
	case directoryservicebackend.DirectoryTypeSimpleAD:
		return "SIMPLE_AD"
	case directoryservicebackend.DirectoryTypeADConnector:
		return "AD_CONNECTOR"
	default:
		return ""
	}
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
