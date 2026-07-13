package workspaces

import (
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ---------------------------------------------------------------------------
// Stored types
// ---------------------------------------------------------------------------

type ipRuleItem struct {
	IpRule   string `json:"ipRule"` //nolint:revive,staticcheck // existing issue.
	RuleDesc string `json:"ruleDesc"`
}

type storedIpGroup struct { //nolint:revive,staticcheck // existing issue.
	Tags      map[string]string `json:"tags"`
	GroupID   string            `json:"groupId"`
	GroupName string            `json:"groupName"`
	GroupDesc string            `json:"groupDesc"`
	UserRules []ipRuleItem      `json:"userRules"`
}

type connAliasPermission struct {
	AccountID        string `json:"accountId"`
	AllowAssociation bool   `json:"allowAssociation"`
}

type storedConnAlias struct {
	Tags                 map[string]string     `json:"tags"`
	AliasID              string                `json:"aliasId"`
	ConnectionString     string                `json:"connectionString"`
	State                string                `json:"state"`
	OwnerAccountID       string                `json:"ownerAccountId"`
	AssociatedResource   string                `json:"associatedResource"`
	ConnectionIdentifier string                `json:"connectionIdentifier"`
	SharedAccounts       []connAliasPermission `json:"sharedAccounts"`
}

type storedCustomBundle struct {
	Tags        map[string]string `json:"tags"`
	BundleID    string            `json:"bundleId"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	ImageID     string            `json:"imageId"`
	ComputeType string            `json:"computeType"`
}

type storedImage struct {
	Created       time.Time         `json:"created"`
	Tags          map[string]string `json:"tags"`
	ImageID       string            `json:"imageId"`
	Name          string            `json:"name"`
	Description   string            `json:"description"`
	State         string            `json:"state"`
	SourceImageID string            `json:"sourceImageId"`
}

type storedPool struct {
	CreatedAt   time.Time         `json:"createdAt"`
	Tags        map[string]string `json:"tags"`
	PoolID      string            `json:"poolId"`
	PoolArn     string            `json:"poolArn"`
	PoolName    string            `json:"poolName"`
	BundleID    string            `json:"bundleId"`
	DirectoryID string            `json:"directoryId"`
	Description string            `json:"description"`
	State       string            `json:"state"`
}

type storedPoolSession struct {
	SessionID string `json:"sessionId"`
	PoolID    string `json:"poolId"`
	UserID    string `json:"userId"`
}

type storedConnectAddIn struct {
	AddInID    string `json:"addInId"`
	Name       string `json:"name"`
	ResourceID string `json:"resourceId"`
	URL        string `json:"url"`
}

type storedClientBranding struct {
	Platforms  map[string]map[string]any `json:"platforms"`
	ResourceID string                    `json:"resourceId"`
}

type storedClientProps struct {
	ReconnectEnabled string `json:"reconnectEnabled"`
}

type storedAccountLink struct {
	LinkID          string `json:"linkId"`
	Status          string `json:"status"`
	SourceAccountID string `json:"sourceAccountId"`
	TargetAccountID string `json:"targetAccountId"`
}

type storedAccountConfig struct {
	DedicatedTenancySupport string `json:"dedicatedTenancySupport"`
	ManagementCidrRange     string `json:"managementCidrRange"`
}

type storedApplication struct {
	AppID string `json:"appId"`
	Name  string `json:"name"`
	Owner string `json:"owner"`
	State string `json:"state"`
}

type storedDirSettings struct {
	Properties  map[string]string `json:"properties"`
	DirectoryID string            `json:"directoryId"`
}

// ---------------------------------------------------------------------------
// Sentinel errors
// ---------------------------------------------------------------------------

var (
	errIpGroupNotFound = awserr.New( //nolint:revive,staticcheck // existing issue.
		errResourceNotFound,
		awserr.ErrNotFound,
	)

	errConnAliasNotFound   = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	errBundleNotFound      = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	errImageNotFound       = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	errPoolNotFound        = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	errPoolSessionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	errAddInNotFound       = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	errAccountLinkNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (b *InMemoryBackend) nextID(prefix string) string {
	b.counter++

	return fmt.Sprintf("%s%08x", prefix, b.counter)
}

func cloneTags(tags map[string]string) map[string]string {
	out := make(map[string]string, len(tags))
	maps.Copy(out, tags)

	return out
}

// ---------------------------------------------------------------------------
// IP Groups
// ---------------------------------------------------------------------------

// CreateIpGroup creates a new IP group and returns its ID.
func (b *InMemoryBackend) CreateIpGroup( //nolint:revive,staticcheck // existing issue.
	groupName, groupDesc string,
	userRules []ipRuleItem,
	tags map[string]string,
) (string, error) {
	b.mu.Lock("CreateIpGroup")
	defer b.mu.Unlock()

	id := b.nextID("wsipg-")
	rules := make([]ipRuleItem, len(userRules))
	copy(rules, userRules)

	b.ipGroups.Put(&storedIpGroup{
		GroupID:   id,
		GroupName: groupName,
		GroupDesc: groupDesc,
		UserRules: rules,
		Tags:      cloneTags(tags),
	})

	return id, nil
}

// DescribeIpGroups returns IP groups, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeIpGroups( //nolint:revive,staticcheck // existing issue.
	groupIDs []string, _ int32, _ string,
) ([]*storedIpGroup, string, error) {
	b.mu.RLock("DescribeIpGroups")
	defer b.mu.RUnlock()

	filter := buildFilter(groupIDs)
	var result []*storedIpGroup

	for _, g := range b.ipGroups.All() {
		if !matchesFilter(filter, g.GroupID) {
			continue
		}

		cp := *g
		cp.UserRules = make([]ipRuleItem, len(g.UserRules))
		copy(cp.UserRules, g.UserRules)
		result = append(result, &cp)
	}

	if result == nil {
		result = []*storedIpGroup{}
	}

	return result, "", nil
}

// DeleteIPGroup removes an IP group by ID.
func (b *InMemoryBackend) DeleteIPGroup(
	groupID string,
) error {
	b.mu.Lock("DeleteIpGroup")
	defer b.mu.Unlock()

	if !b.ipGroups.Has(groupID) {
		return errIpGroupNotFound
	}

	b.ipGroups.Delete(groupID)

	return nil
}

// AuthorizeIpRules appends rules to an IP group.
func (b *InMemoryBackend) AuthorizeIpRules( //nolint:revive,staticcheck // existing issue.
	groupID string,
	rules []ipRuleItem,
) error {
	b.mu.Lock("AuthorizeIpRules")
	defer b.mu.Unlock()

	g, ok := b.ipGroups.Get(groupID)
	if !ok {
		return errIpGroupNotFound
	}

	g.UserRules = append(g.UserRules, rules...)

	return nil
}

// RevokeIpRules removes rules (by IpRule string) from an IP group.
func (b *InMemoryBackend) RevokeIpRules( //nolint:revive,staticcheck // existing issue.
	groupID string,
	ipRules []string,
) error {
	b.mu.Lock("RevokeIpRules")
	defer b.mu.Unlock()

	g, ok := b.ipGroups.Get(groupID)
	if !ok {
		return errIpGroupNotFound
	}

	revoke := buildFilter(ipRules)
	kept := g.UserRules[:0]

	for _, r := range g.UserRules {
		if !matchesFilter(revoke, r.IpRule) {
			kept = append(kept, r)
		}
	}

	g.UserRules = kept

	return nil
}

// UpdateRulesOfIpGroup replaces all rules in an IP group.
func (b *InMemoryBackend) UpdateRulesOfIpGroup( //nolint:revive,staticcheck // existing issue.
	groupID string,
	rules []ipRuleItem,
) error {
	b.mu.Lock("UpdateRulesOfIpGroup")
	defer b.mu.Unlock()

	g, ok := b.ipGroups.Get(groupID)
	if !ok {
		return errIpGroupNotFound
	}

	cp := make([]ipRuleItem, len(rules))
	copy(cp, rules)
	g.UserRules = cp

	return nil
}

// AssociateIpGroups associates IP groups with a directory.
func (b *InMemoryBackend) AssociateIpGroups( //nolint:revive,staticcheck // existing issue.
	directoryID string,
	groupIDs []string,
) error {
	b.mu.Lock("AssociateIpGroups")
	defer b.mu.Unlock()

	if b.directoryIpGroups[directoryID] == nil {
		b.directoryIpGroups[directoryID] = make(map[string]struct{})
	}

	for _, gid := range groupIDs {
		b.directoryIpGroups[directoryID][gid] = struct{}{}
	}

	return nil
}

// DisassociateIpGroups removes IP group associations from a directory.
func (b *InMemoryBackend) DisassociateIpGroups( //nolint:revive,staticcheck // existing issue.
	directoryID string,
	groupIDs []string,
) error {
	b.mu.Lock("DisassociateIpGroups")
	defer b.mu.Unlock()

	for _, gid := range groupIDs {
		delete(b.directoryIpGroups[directoryID], gid)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Connection Aliases
// ---------------------------------------------------------------------------

// CreateConnectionAlias creates a new connection alias.
func (b *InMemoryBackend) CreateConnectionAlias(
	connectionString string, tags map[string]string,
) (string, error) {
	b.mu.Lock("CreateConnectionAlias")
	defer b.mu.Unlock()

	id := b.nextID("wsca-")
	b.connAliases.Put(&storedConnAlias{
		AliasID:          id,
		ConnectionString: connectionString,
		State:            "CREATED",
		OwnerAccountID:   b.accountID,
		Tags:             cloneTags(tags),
	})

	return id, nil
}

// DescribeConnectionAliases returns connection aliases filtered by IDs or resource.
func (b *InMemoryBackend) DescribeConnectionAliases(
	aliasIDs []string, resourceID string, _ int32, _ string,
) ([]*storedConnAlias, string, error) {
	b.mu.RLock("DescribeConnectionAliases")
	defer b.mu.RUnlock()

	filter := buildFilter(aliasIDs)
	var result []*storedConnAlias

	for _, a := range b.connAliases.All() {
		if !matchesFilter(filter, a.AliasID) {
			continue
		}

		if resourceID != "" && a.AssociatedResource != resourceID {
			continue
		}

		cp := *a
		result = append(result, &cp)
	}

	if result == nil {
		result = []*storedConnAlias{}
	}

	return result, "", nil
}

// DeleteConnectionAlias removes a connection alias.
func (b *InMemoryBackend) DeleteConnectionAlias(aliasID string) error {
	b.mu.Lock("DeleteConnectionAlias")
	defer b.mu.Unlock()

	if !b.connAliases.Has(aliasID) {
		return errConnAliasNotFound
	}

	b.connAliases.Delete(aliasID)

	return nil
}

// AssociateConnectionAlias associates an alias with a resource.
func (b *InMemoryBackend) AssociateConnectionAlias(aliasID, resourceID string) (string, error) {
	b.mu.Lock("AssociateConnectionAlias")
	defer b.mu.Unlock()

	a, ok := b.connAliases.Get(aliasID)
	if !ok {
		return "", errConnAliasNotFound
	}

	a.AssociatedResource = resourceID
	a.ConnectionIdentifier = fmt.Sprintf("wcci-%08x", b.counter)

	return a.ConnectionIdentifier, nil
}

// DisassociateConnectionAlias removes the resource association from an alias.
func (b *InMemoryBackend) DisassociateConnectionAlias(aliasID string) error {
	b.mu.Lock("DisassociateConnectionAlias")
	defer b.mu.Unlock()

	a, ok := b.connAliases.Get(aliasID)
	if !ok {
		return errConnAliasNotFound
	}

	a.AssociatedResource = ""
	a.ConnectionIdentifier = ""

	return nil
}

// DescribeConnectionAliasPermissions returns shared-account permissions for an alias.
func (b *InMemoryBackend) DescribeConnectionAliasPermissions(
	aliasID string, _ int32, _ string,
) (string, []connAliasPermission, string, error) {
	b.mu.RLock("DescribeConnectionAliasPermissions")
	defer b.mu.RUnlock()

	a, ok := b.connAliases.Get(aliasID)
	if !ok {
		return "", nil, "", errConnAliasNotFound
	}

	perms := make([]connAliasPermission, len(a.SharedAccounts))
	copy(perms, a.SharedAccounts)

	return aliasID, perms, "", nil
}

// UpdateConnectionAliasPermission sets the shared-account permission for an alias.
func (b *InMemoryBackend) UpdateConnectionAliasPermission(
	aliasID, accountID string, allowAssociation bool,
) error {
	b.mu.Lock("UpdateConnectionAliasPermission")
	defer b.mu.Unlock()

	a, ok := b.connAliases.Get(aliasID)
	if !ok {
		return errConnAliasNotFound
	}

	for i, p := range a.SharedAccounts {
		if p.AccountID == accountID {
			a.SharedAccounts[i].AllowAssociation = allowAssociation

			return nil
		}
	}

	a.SharedAccounts = append(a.SharedAccounts, connAliasPermission{
		AccountID:        accountID,
		AllowAssociation: allowAssociation,
	})

	return nil
}

// ---------------------------------------------------------------------------
// Custom Bundles
// ---------------------------------------------------------------------------

// CreateWorkspaceBundle creates a custom bundle.
func (b *InMemoryBackend) CreateWorkspaceBundle(
	name, description, imageID, computeType string,
	tags map[string]string,
) (*storedCustomBundle, error) {
	b.mu.Lock("CreateWorkspaceBundle")
	defer b.mu.Unlock()

	id := b.nextID("wsb-")
	bun := &storedCustomBundle{
		BundleID:    id,
		Name:        name,
		Description: description,
		ImageID:     imageID,
		ComputeType: computeType,
		Tags:        cloneTags(tags),
	}
	b.customBundles.Put(bun)

	return bun, nil
}

// DeleteWorkspaceBundle removes a custom bundle.
func (b *InMemoryBackend) DeleteWorkspaceBundle(bundleID string) error {
	b.mu.Lock("DeleteWorkspaceBundle")
	defer b.mu.Unlock()

	if !b.customBundles.Has(bundleID) {
		return errBundleNotFound
	}

	b.customBundles.Delete(bundleID)

	return nil
}

// UpdateWorkspaceBundle updates the image of a custom bundle.
func (b *InMemoryBackend) UpdateWorkspaceBundle(bundleID, imageID string) error {
	b.mu.Lock("UpdateWorkspaceBundle")
	defer b.mu.Unlock()

	bun, ok := b.customBundles.Get(bundleID)
	if !ok {
		return errBundleNotFound
	}

	bun.ImageID = imageID

	return nil
}

// ---------------------------------------------------------------------------
// Images
// ---------------------------------------------------------------------------

func (b *InMemoryBackend) createImageLocked(
	name, description, sourceImageID string,
	tags map[string]string,
) *storedImage {
	id := b.nextID("wsi-")
	img := &storedImage{
		ImageID:       id,
		Name:          name,
		Description:   description,
		State:         "AVAILABLE",
		SourceImageID: sourceImageID,
		Created:       time.Now().UTC(),
		Tags:          cloneTags(tags),
	}
	b.images.Put(img)

	return img
}

// CopyWorkspaceImage copies an image.
func (b *InMemoryBackend) CopyWorkspaceImage(
	name, sourceImageID, _ /*sourceRegion*/, description string,
	tags map[string]string,
) (string, error) {
	b.mu.Lock("CopyWorkspaceImage")
	defer b.mu.Unlock()

	img := b.createImageLocked(name, description, sourceImageID, tags)

	return img.ImageID, nil
}

// CreateWorkspaceImage creates an image from a workspace.
func (b *InMemoryBackend) CreateWorkspaceImage(
	name, description, _ /*workspaceId*/ string,
	tags map[string]string,
) (*storedImage, error) {
	b.mu.Lock("CreateWorkspaceImage")
	defer b.mu.Unlock()

	img := b.createImageLocked(name, description, "", tags)

	return img, nil
}

// DeleteWorkspaceImage removes an image.
func (b *InMemoryBackend) DeleteWorkspaceImage(imageID string) error {
	b.mu.Lock("DeleteWorkspaceImage")
	defer b.mu.Unlock()

	if !b.images.Has(imageID) {
		return errImageNotFound
	}

	b.images.Delete(imageID)
	delete(b.imagePermissions, imageID)

	return nil
}

// ImportWorkspaceImage imports an EC2 image as a workspace image.
func (b *InMemoryBackend) ImportWorkspaceImage(
	ec2ImageID, name, description string, tags map[string]string,
) (string, error) {
	b.mu.Lock("ImportWorkspaceImage")
	defer b.mu.Unlock()

	img := b.createImageLocked(name, description, ec2ImageID, tags)

	return img.ImageID, nil
}

// ImportCustomWorkspaceImage imports a custom workspace image.
func (b *InMemoryBackend) ImportCustomWorkspaceImage(
	name, description string,
) (*storedImage, error) {
	b.mu.Lock("ImportCustomWorkspaceImage")
	defer b.mu.Unlock()

	img := b.createImageLocked(name, description, "", nil)

	return img, nil
}

// CreateUpdatedWorkspaceImage creates an updated version of an existing image.
func (b *InMemoryBackend) CreateUpdatedWorkspaceImage(
	sourceImageID, name, description string, tags map[string]string,
) (string, error) {
	b.mu.Lock("CreateUpdatedWorkspaceImage")
	defer b.mu.Unlock()

	img := b.createImageLocked(name, description, sourceImageID, tags)

	return img.ImageID, nil
}

// DescribeWorkspaceImages returns workspace images, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeWorkspaceImages(
	imageIDs []string, _ /*imageType*/ string, _ int32, _ string,
) ([]*storedImage, string, error) {
	b.mu.RLock("DescribeWorkspaceImages")
	defer b.mu.RUnlock()

	filter := buildFilter(imageIDs)
	var result []*storedImage

	for _, img := range b.images.All() {
		if !matchesFilter(filter, img.ImageID) {
			continue
		}

		cp := *img
		result = append(result, &cp)
	}

	if result == nil {
		result = []*storedImage{}
	}

	return result, "", nil
}

// DescribeWorkspaceImagePermissions returns sharing permissions for an image.
func (b *InMemoryBackend) DescribeWorkspaceImagePermissions(
	imageID string,
) (string, map[string]bool, error) {
	b.mu.RLock("DescribeWorkspaceImagePermissions")
	defer b.mu.RUnlock()

	if !b.images.Has(imageID) {
		return "", nil, errImageNotFound
	}

	perms := make(map[string]bool)
	maps.Copy(perms, b.imagePermissions[imageID])

	return imageID, perms, nil
}

// UpdateWorkspaceImagePermission sets the sharing permission for an image.
func (b *InMemoryBackend) UpdateWorkspaceImagePermission(
	imageID, sharedAccountID string, allowCopy bool,
) error {
	b.mu.Lock("UpdateWorkspaceImagePermission")
	defer b.mu.Unlock()

	if !b.images.Has(imageID) {
		return errImageNotFound
	}

	if b.imagePermissions[imageID] == nil {
		b.imagePermissions[imageID] = make(map[string]bool)
	}

	b.imagePermissions[imageID][sharedAccountID] = allowCopy

	return nil
}

// DescribeCustomWorkspaceImageImport returns import state for a custom image.
func (b *InMemoryBackend) DescribeCustomWorkspaceImageImport(imageID string) (*storedImage, error) {
	b.mu.RLock("DescribeCustomWorkspaceImageImport")
	defer b.mu.RUnlock()

	img, ok := b.images.Get(imageID)
	if !ok {
		return nil, errImageNotFound
	}

	cp := *img

	return &cp, nil
}

// ---------------------------------------------------------------------------
// Pools
// ---------------------------------------------------------------------------

// CreateWorkspacesPool creates a new workspace pool.
func (b *InMemoryBackend) CreateWorkspacesPool(
	poolName, bundleID, directoryID, description string,
	tags map[string]string,
) (*storedPool, error) {
	b.mu.Lock("CreateWorkspacesPool")
	defer b.mu.Unlock()

	id := b.nextID("wsp-")
	arn := fmt.Sprintf(
		"arn:aws:workspaces:%s:%s:workspacespool/%s",
		b.region, b.accountID, id,
	)

	pool := &storedPool{
		PoolID:      id,
		PoolArn:     arn,
		PoolName:    poolName,
		BundleID:    bundleID,
		DirectoryID: directoryID,
		Description: description,
		State:       "RUNNING",
		CreatedAt:   time.Now().UTC(),
		Tags:        cloneTags(tags),
	}
	b.pools.Put(pool)

	return pool, nil
}

// DescribeWorkspacesPools returns pools, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeWorkspacesPools(
	poolIDs []string, _ int32, _ string,
) ([]*storedPool, string, error) {
	b.mu.RLock("DescribeWorkspacesPools")
	defer b.mu.RUnlock()

	filter := buildFilter(poolIDs)
	var result []*storedPool

	for _, p := range b.pools.All() {
		if !matchesFilter(filter, p.PoolID) {
			continue
		}

		cp := *p
		result = append(result, &cp)
	}

	if result == nil {
		result = []*storedPool{}
	}

	return result, "", nil
}

// StartWorkspacesPool transitions a pool to RUNNING.
func (b *InMemoryBackend) StartWorkspacesPool(poolID string) error {
	b.mu.Lock("StartWorkspacesPool")
	defer b.mu.Unlock()

	p, ok := b.pools.Get(poolID)
	if !ok {
		return errPoolNotFound
	}

	p.State = "RUNNING"

	return nil
}

// StopWorkspacesPool transitions a pool to STOPPED.
func (b *InMemoryBackend) StopWorkspacesPool(poolID string) error {
	b.mu.Lock("StopWorkspacesPool")
	defer b.mu.Unlock()

	p, ok := b.pools.Get(poolID)
	if !ok {
		return errPoolNotFound
	}

	p.State = "STOPPED"

	return nil
}

// TerminateWorkspacesPool removes a pool.
func (b *InMemoryBackend) TerminateWorkspacesPool(poolID string) error {
	b.mu.Lock("TerminateWorkspacesPool")
	defer b.mu.Unlock()

	if !b.pools.Has(poolID) {
		return errPoolNotFound
	}

	b.pools.Delete(poolID)

	return nil
}

// UpdateWorkspacesPool updates pool fields.
func (b *InMemoryBackend) UpdateWorkspacesPool(
	poolID, description, bundleID, directoryID string,
) (*storedPool, error) {
	b.mu.Lock("UpdateWorkspacesPool")
	defer b.mu.Unlock()

	p, ok := b.pools.Get(poolID)
	if !ok {
		return nil, errPoolNotFound
	}

	if description != "" {
		p.Description = description
	}

	if bundleID != "" {
		p.BundleID = bundleID
	}

	if directoryID != "" {
		p.DirectoryID = directoryID
	}

	cp := *p

	return &cp, nil
}

// DescribeWorkspacesPoolSessions returns sessions for a pool.
func (b *InMemoryBackend) DescribeWorkspacesPoolSessions(
	poolID, _ /*userID*/ string, _ int32, _ string,
) ([]*storedPoolSession, string, error) {
	b.mu.RLock("DescribeWorkspacesPoolSessions")
	defer b.mu.RUnlock()

	var result []*storedPoolSession

	for _, s := range b.poolSessions.All() {
		if s.PoolID != poolID {
			continue
		}

		cp := *s
		result = append(result, &cp)
	}

	if result == nil {
		result = []*storedPoolSession{}
	}

	return result, "", nil
}

// TerminateWorkspacesPoolSession removes a pool session.
func (b *InMemoryBackend) TerminateWorkspacesPoolSession(sessionID string) error {
	b.mu.Lock("TerminateWorkspacesPoolSession")
	defer b.mu.Unlock()

	if !b.poolSessions.Has(sessionID) {
		return errPoolSessionNotFound
	}

	b.poolSessions.Delete(sessionID)

	return nil
}

// ---------------------------------------------------------------------------
// Directory Registration
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Account
// ---------------------------------------------------------------------------

// DescribeAccount returns account configuration.
func (b *InMemoryBackend) DescribeAccount() storedAccountConfig {
	b.mu.RLock("DescribeAccount")
	defer b.mu.RUnlock()

	cfg := b.accountConfig
	if cfg.DedicatedTenancySupport == "" {
		cfg.DedicatedTenancySupport = "ENABLED"
	}

	return cfg
}

// ModifyAccount updates account configuration.
func (b *InMemoryBackend) ModifyAccount(
	dedicatedTenancyCidr, dedicatedTenancySupport string,
) error {
	b.mu.Lock("ModifyAccount")
	defer b.mu.Unlock()

	if dedicatedTenancyCidr != "" {
		b.accountConfig.ManagementCidrRange = dedicatedTenancyCidr
	}

	if dedicatedTenancySupport != "" {
		b.accountConfig.DedicatedTenancySupport = dedicatedTenancySupport
	}

	return nil
}

// ModifyEndpointEncryptionMode stores the endpoint encryption mode for a directory.
func (b *InMemoryBackend) ModifyEndpointEncryptionMode(directoryID, mode string) error {
	b.mu.Lock("ModifyEndpointEncryptionMode")
	defer b.mu.Unlock()

	b.ensureDirSettings(directoryID)

	ds, _ := b.dirSettings.Get(directoryID)
	ds.Properties["EndpointEncryptionMode"] = mode

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

// ---------------------------------------------------------------------------
// Connect Client Add-Ins
// ---------------------------------------------------------------------------

// CreateConnectClientAddIn creates a new Connect client add-in.
func (b *InMemoryBackend) CreateConnectClientAddIn(name, resourceID, url string) (string, error) {
	b.mu.Lock("CreateConnectClientAddIn")
	defer b.mu.Unlock()

	id := b.nextID("wscai-")
	b.connectAddIns.Put(&storedConnectAddIn{
		AddInID:    id,
		Name:       name,
		ResourceID: resourceID,
		URL:        url,
	})

	return id, nil
}

// DeleteConnectClientAddIn removes a Connect client add-in.
func (b *InMemoryBackend) DeleteConnectClientAddIn(addInID, _ /*resourceId*/ string) error {
	b.mu.Lock("DeleteConnectClientAddIn")
	defer b.mu.Unlock()

	if !b.connectAddIns.Has(addInID) {
		return errAddInNotFound
	}

	b.connectAddIns.Delete(addInID)

	return nil
}

// DescribeConnectClientAddIns returns add-ins for a resource.
func (b *InMemoryBackend) DescribeConnectClientAddIns(
	resourceID string, _ int32, _ string,
) ([]*storedConnectAddIn, string, error) {
	b.mu.RLock("DescribeConnectClientAddIns")
	defer b.mu.RUnlock()

	var result []*storedConnectAddIn

	for _, a := range b.connectAddIns.All() {
		if a.ResourceID != resourceID {
			continue
		}

		cp := *a
		result = append(result, &cp)
	}

	if result == nil {
		result = []*storedConnectAddIn{}
	}

	return result, "", nil
}

// UpdateConnectClientAddIn updates a Connect client add-in.
func (b *InMemoryBackend) UpdateConnectClientAddIn(
	addInID, _ /*resourceId*/, name, url string,
) error {
	b.mu.Lock("UpdateConnectClientAddIn")
	defer b.mu.Unlock()

	a, ok := b.connectAddIns.Get(addInID)
	if !ok {
		return errAddInNotFound
	}

	if name != "" {
		a.Name = name
	}

	if url != "" {
		a.URL = url
	}

	return nil
}

// ---------------------------------------------------------------------------
// Client Branding
// ---------------------------------------------------------------------------

// ImportClientBranding stores branding data for a resource.
func (b *InMemoryBackend) ImportClientBranding(
	resourceID string, platforms map[string]map[string]any,
) error {
	b.mu.Lock("ImportClientBranding")
	defer b.mu.Unlock()

	cb, ok := b.clientBranding.Get(resourceID)
	if !ok {
		cb = &storedClientBranding{
			ResourceID: resourceID,
			Platforms:  make(map[string]map[string]any),
		}
		b.clientBranding.Put(cb)
	}

	maps.Copy(cb.Platforms, platforms)

	return nil
}

// DescribeClientBranding returns branding data for a resource.
func (b *InMemoryBackend) DescribeClientBranding(
	resourceID string,
) (map[string]map[string]any, error) {
	b.mu.RLock("DescribeClientBranding")
	defer b.mu.RUnlock()

	cb, ok := b.clientBranding.Get(resourceID)
	if !ok {
		return map[string]map[string]any{}, nil
	}

	out := make(map[string]map[string]any, len(cb.Platforms))
	maps.Copy(out, cb.Platforms)

	return out, nil
}

// DeleteClientBranding removes branding data for specific platforms.
func (b *InMemoryBackend) DeleteClientBranding(resourceID string, platforms []string) error {
	b.mu.Lock("DeleteClientBranding")
	defer b.mu.Unlock()

	cb, ok := b.clientBranding.Get(resourceID)
	if !ok {
		return nil
	}

	for _, p := range platforms {
		delete(cb.Platforms, p)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Client Properties
// ---------------------------------------------------------------------------

// DescribeClientProperties returns client properties for resource IDs.
func (b *InMemoryBackend) DescribeClientProperties(
	resourceIDs []string,
) (map[string]storedClientProps, error) {
	b.mu.RLock("DescribeClientProperties")
	defer b.mu.RUnlock()

	out := make(map[string]storedClientProps, len(resourceIDs))
	for _, id := range resourceIDs {
		out[id] = b.clientProperties[id]
	}

	return out, nil
}

// ModifyClientProperties sets client properties for a resource.
func (b *InMemoryBackend) ModifyClientProperties(resourceID, reconnectEnabled string) error {
	b.mu.Lock("ModifyClientProperties")
	defer b.mu.Unlock()

	b.clientProperties[resourceID] = storedClientProps{ReconnectEnabled: reconnectEnabled}

	return nil
}

// ---------------------------------------------------------------------------
// Directory modify ops (all store into dirSettings)
// ---------------------------------------------------------------------------

// ModifyCertificateBasedAuthProperties stores certificate auth properties for a directory.
func (b *InMemoryBackend) ModifyCertificateBasedAuthProperties(
	directoryID string,
	props map[string]string,
) error {
	b.mu.Lock("ModifyCertificateBasedAuthProperties")
	defer b.mu.Unlock()

	b.ensureDirSettings(directoryID)

	ds, _ := b.dirSettings.Get(directoryID)
	for k, v := range props {
		ds.Properties["CertAuth_"+k] = v
	}

	return nil
}

// ModifySamlProperties stores SAML properties for a directory.
func (b *InMemoryBackend) ModifySamlProperties(directoryID string, props map[string]string) error {
	b.mu.Lock("ModifySamlProperties")
	defer b.mu.Unlock()

	b.ensureDirSettings(directoryID)

	ds, _ := b.dirSettings.Get(directoryID)
	for k, v := range props {
		ds.Properties["Saml_"+k] = v
	}

	return nil
}

// ModifySelfservicePermissions stores self-service permissions for a directory.
func (b *InMemoryBackend) ModifySelfservicePermissions(
	directoryID string,
	props map[string]string,
) error {
	b.mu.Lock("ModifySelfservicePermissions")
	defer b.mu.Unlock()

	b.ensureDirSettings(directoryID)

	ds, _ := b.dirSettings.Get(directoryID)
	for k, v := range props {
		ds.Properties["SelfSvc_"+k] = v
	}

	return nil
}

// ModifyStreamingProperties stores streaming properties for a directory.
func (b *InMemoryBackend) ModifyStreamingProperties(
	directoryID string,
	props map[string]string,
) error {
	b.mu.Lock("ModifyStreamingProperties")
	defer b.mu.Unlock()

	b.ensureDirSettings(directoryID)

	ds, _ := b.dirSettings.Get(directoryID)
	for k, v := range props {
		ds.Properties["Streaming_"+k] = v
	}

	return nil
}

// ModifyWorkspaceAccessProperties stores workspace access properties for a directory.
func (b *InMemoryBackend) ModifyWorkspaceAccessProperties(
	directoryID string,
	props map[string]string,
) error {
	b.mu.Lock("ModifyWorkspaceAccessProperties")
	defer b.mu.Unlock()

	b.ensureDirSettings(directoryID)

	ds, _ := b.dirSettings.Get(directoryID)
	for k, v := range props {
		ds.Properties["Access_"+k] = v
	}

	return nil
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

// ---------------------------------------------------------------------------
// Account Links
// ---------------------------------------------------------------------------

// CreateAccountLinkInvitation creates an account link invitation.
func (b *InMemoryBackend) CreateAccountLinkInvitation(
	targetAccountID string,
) (*storedAccountLink, error) {
	b.mu.Lock("CreateAccountLinkInvitation")
	defer b.mu.Unlock()

	id := b.nextID("wsal-")
	link := &storedAccountLink{
		LinkID:          id,
		Status:          "PENDING_ACCEPTANCE",
		SourceAccountID: b.accountID,
		TargetAccountID: targetAccountID,
	}
	b.accountLinks.Put(link)

	cp := *link

	return &cp, nil
}

// AcceptAccountLinkInvitation accepts an account link.
func (b *InMemoryBackend) AcceptAccountLinkInvitation(linkID string) (*storedAccountLink, error) {
	b.mu.Lock("AcceptAccountLinkInvitation")
	defer b.mu.Unlock()

	link, ok := b.accountLinks.Get(linkID)
	if !ok {
		return nil, errAccountLinkNotFound
	}

	link.Status = "LINKED"
	cp := *link

	return &cp, nil
}

// RejectAccountLinkInvitation rejects an account link.
func (b *InMemoryBackend) RejectAccountLinkInvitation(linkID string) (*storedAccountLink, error) {
	b.mu.Lock("RejectAccountLinkInvitation")
	defer b.mu.Unlock()

	link, ok := b.accountLinks.Get(linkID)
	if !ok {
		return nil, errAccountLinkNotFound
	}

	link.Status = "REJECTED"
	cp := *link

	return &cp, nil
}

// DeleteAccountLinkInvitation deletes an account link.
func (b *InMemoryBackend) DeleteAccountLinkInvitation(linkID string) (*storedAccountLink, error) {
	b.mu.Lock("DeleteAccountLinkInvitation")
	defer b.mu.Unlock()

	link, ok := b.accountLinks.Get(linkID)
	if !ok {
		return nil, errAccountLinkNotFound
	}

	link.Status = "DELETED"
	cp := *link
	b.accountLinks.Delete(linkID)

	return &cp, nil
}

// GetAccountLink retrieves an account link by ID.
func (b *InMemoryBackend) GetAccountLink(linkID string) (*storedAccountLink, error) {
	b.mu.RLock("GetAccountLink")
	defer b.mu.RUnlock()

	link, ok := b.accountLinks.Get(linkID)
	if !ok {
		return nil, errAccountLinkNotFound
	}

	cp := *link

	return &cp, nil
}

// ListAccountLinks returns account links, optionally filtered by status.
func (b *InMemoryBackend) ListAccountLinks(
	statusFilter string,
	_ int32,
	_ string,
) ([]*storedAccountLink, string, error) {
	b.mu.RLock("ListAccountLinks")
	defer b.mu.RUnlock()

	var result []*storedAccountLink

	for _, link := range b.accountLinks.All() {
		if statusFilter != "" && link.Status != statusFilter {
			continue
		}

		cp := *link
		result = append(result, &cp)
	}

	if result == nil {
		result = []*storedAccountLink{}
	}

	return result, "", nil
}

// ---------------------------------------------------------------------------
// Applications
// ---------------------------------------------------------------------------

// AssociateWorkspaceApplication associates an application with a workspace.
func (b *InMemoryBackend) AssociateWorkspaceApplication(workspaceID, applicationID string) error {
	b.mu.Lock("AssociateWorkspaceApplication")
	defer b.mu.Unlock()

	if b.appAssociations[workspaceID] == nil {
		b.appAssociations[workspaceID] = make(map[string]struct{})
	}

	b.appAssociations[workspaceID][applicationID] = struct{}{}

	return nil
}

// DisassociateWorkspaceApplication removes an application association from a workspace.
func (b *InMemoryBackend) DisassociateWorkspaceApplication(
	workspaceID, applicationID string,
) error {
	b.mu.Lock("DisassociateWorkspaceApplication")
	defer b.mu.Unlock()

	delete(b.appAssociations[workspaceID], applicationID)

	return nil
}

// DeployWorkspaceApplications deploys applications on a workspace (no-op in memory).
func (b *InMemoryBackend) DeployWorkspaceApplications(
	workspaceID string, //nolint:revive // existing issue.
	_ bool,
) ([]map[string]string, error) {
	b.mu.RLock("DeployWorkspaceApplications")
	defer b.mu.RUnlock()

	return []map[string]string{}, nil
}

// DescribeWorkspaceAssociations returns application associations for a workspace.
func (b *InMemoryBackend) DescribeWorkspaceAssociations(
	workspaceID string,
	_ []string,
) ([]map[string]string, error) {
	b.mu.RLock("DescribeWorkspaceAssociations")
	defer b.mu.RUnlock()

	assoc := b.appAssociations[workspaceID]
	result := make([]map[string]string, 0, len(assoc))

	for appID := range assoc {
		result = append(result, map[string]string{
			"WorkspaceId":          workspaceID, //nolint:goconst // existing issue.
			"AssociatedResourceId": appID,
			"AssociationStatus":    "INSTALLED", //nolint:goconst // existing issue.
		})
	}

	return result, nil
}

// DescribeApplicationAssociations returns workspace associations for an application.
func (b *InMemoryBackend) DescribeApplicationAssociations(
	applicationID string, _ []string, _ int32, _ string,
) ([]map[string]string, string, error) {
	b.mu.RLock("DescribeApplicationAssociations")
	defer b.mu.RUnlock()

	var result []map[string]string

	for wsID, apps := range b.appAssociations {
		if _, ok := apps[applicationID]; ok {
			result = append(result, map[string]string{
				"WorkspaceId":          wsID,
				"AssociatedResourceId": applicationID,
				"AssociationStatus":    "INSTALLED",
			})
		}
	}

	if result == nil {
		result = []map[string]string{}
	}

	return result, "", nil
}

// DescribeApplications returns stored applications, filtered by IDs.
func (b *InMemoryBackend) DescribeApplications(
	appIDs []string,
	_ int32,
	_ string,
) ([]*storedApplication, string, error) {
	b.mu.RLock("DescribeApplications")
	defer b.mu.RUnlock()

	filter := buildFilter(appIDs)
	var result []*storedApplication

	for _, app := range b.applications.All() {
		if !matchesFilter(filter, app.AppID) {
			continue
		}

		cp := *app
		result = append(result, &cp)
	}

	if result == nil {
		result = []*storedApplication{}
	}

	return result, "", nil
}

// ---------------------------------------------------------------------------
// Workspace-level ops that operate on existing workspaces
// ---------------------------------------------------------------------------

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

// CreateStandbyWorkspaces creates standby workspaces (returns pending list).
func (b *InMemoryBackend) CreateStandbyWorkspaces(
	_ string, standby []map[string]string,
) ([]map[string]string, []map[string]string, error) {
	b.mu.Lock("CreateStandbyWorkspaces")
	defer b.mu.Unlock()

	pending := make([]map[string]string, 0, len(standby))

	for _, s := range standby {
		b.counter++
		id := fmt.Sprintf("%s%0*x", workspaceIDPrefix, workspaceIDHexLen, b.counter)

		w := &storedWorkspace{
			WorkspaceID: id,
			DirectoryID: s["DirectoryId"],
			UserName:    s["UserName"],
			BundleID:    s["BundleId"],
			State:       statePending,
			Tags:        make(map[string]string),
		}
		b.workspaces.Put(w)

		pending = append(pending, map[string]string{
			"WorkspaceId": id,
			"DirectoryId": w.DirectoryID,
			"UserName":    w.UserName,
			"BundleId":    w.BundleID,
			"State":       w.State,
		})
	}

	return []map[string]string{}, pending, nil
}
