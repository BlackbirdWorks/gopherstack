package directoryservice

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
//
// Directory Service resources are isolated per region: every backend operation
// resolves the caller's region from the request context and operates only on that
// region's nested store. Directories and all of their dependent resources (snapshots,
// trusts, certificates, conditional forwarders, etc.) are inherently single-region —
// their identifiers (d-..., s-..., t-..., c-...) carry no region component, so the
// region is always taken from the request context (falling back to the backend
// default). Cross-region references never occur and isolation is always safe.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

const (
	errEntityNotExistsException     = "EntityDoesNotExistException"
	errEntityAlreadyExistsException = "EntityAlreadyExistsException"
	errClientException              = "ClientException"

	defaultSimpleADLimit    int32 = 10
	defaultMicrosoftADLimit int32 = 20
	defaultSnapshotLimit    int32 = 5
)

var (
	// ErrDirectoryNotFound is returned when a directory does not exist.
	ErrDirectoryNotFound = awserr.New(errEntityNotExistsException, awserr.ErrNotFound)
	// ErrSnapshotNotFound is returned when a snapshot does not exist.
	ErrSnapshotNotFound = awserr.New(errEntityNotExistsException, awserr.ErrNotFound)
	// ErrAliasAlreadyExists is returned when the alias is already taken.
	ErrAliasAlreadyExists = awserr.New(errEntityAlreadyExistsException, awserr.ErrAlreadyExists)
	// ErrInvalidParameter is returned on invalid input.
	ErrInvalidParameter = awserr.New(errClientException, awserr.ErrInvalidParameter)
	// ErrDirectoryLimitExceeded is returned when the directory limit for the region is reached.
	ErrDirectoryLimitExceeded = awserr.New("DirectoryLimitExceededException", awserr.ErrConflict)
	// ErrSnapshotLimitExceeded is returned when the manual snapshot limit for a directory is reached.
	ErrSnapshotLimitExceeded = awserr.New("SnapshotLimitExceededException", awserr.ErrConflict)
	// ErrUnsupportedOperation is returned when an operation is not supported by the directory type.
	ErrUnsupportedOperation = awserr.New("UnsupportedOperationException", awserr.ErrConflict)
)

// storedVpcSettings holds VPC settings for serialization.
type storedVpcSettings struct {
	VpcID             string   `json:"vpcId"`
	SubnetIDs         []string `json:"subnetIds"`
	SecurityGroupIDs  []string `json:"securityGroupIds"`
	AvailabilityZones []string `json:"availabilityZones"`
}

// storedDirectory holds a directory with all fields.
type storedDirectory struct {
	// region is the AWS region this directory belongs to. It is the outer
	// half of the composite key ("region|DirectoryID") used by the backend's
	// flat store.Table[storedDirectory] (see store_setup.go), which replaces
	// the old map[string]map[string]*storedDirectory nesting (outer key =
	// region). Unexported so it never appears in wire responses (those are
	// built by toDirectory, never by marshaling storedDirectory directly),
	// but persistence.go must carry it through a DTO explicitly since
	// json.Marshal never sees unexported fields.
	region      string
	LaunchTime  time.Time          `json:"launchTime"`
	Tags        map[string]string  `json:"tags"`
	VpcSettings *storedVpcSettings `json:"vpcSettings,omitempty"`
	DirectoryID string             `json:"directoryId"`
	Name        string             `json:"name"`
	ShortName   string             `json:"shortName"`
	Description string             `json:"description"`
	Alias       string             `json:"alias"`
	AccessURL   string             `json:"accessUrl"`
	DirType     string             `json:"type"`
	Stage       string             `json:"stage"`
	Size        string             `json:"size"`
	Edition     string             `json:"edition"`
	SsoEnabled  bool               `json:"ssoEnabled"`
}

func (d *storedDirectory) toDirectory() Directory {
	dir := Directory{
		LaunchTime:  d.LaunchTime,
		DirectoryID: d.DirectoryID,
		Name:        d.Name,
		ShortName:   d.ShortName,
		Description: d.Description,
		Alias:       d.Alias,
		AccessURL:   d.AccessURL,
		Type:        DirectoryType(d.DirType),
		Stage:       DirectoryStage(d.Stage),
		Size:        DirectorySize(d.Size),
		Edition:     DirectoryEdition(d.Edition),
		SsoEnabled:  d.SsoEnabled,
	}
	if d.VpcSettings != nil {
		dir.VpcSettings = &DirectoryVpcSettings{
			VpcID:             d.VpcSettings.VpcID,
			SubnetIDs:         d.VpcSettings.SubnetIDs,
			SecurityGroupIDs:  d.VpcSettings.SecurityGroupIDs,
			AvailabilityZones: d.VpcSettings.AvailabilityZones,
		}
	}

	return dir
}

// storedSnapshot holds a snapshot with all fields.
type storedSnapshot struct {
	// region is the AWS region this snapshot belongs to; see
	// storedDirectory.region for the composite-key rationale.
	region      string
	StartTime   time.Time `json:"startTime"`
	SnapshotID  string    `json:"snapshotId"`
	DirectoryID string    `json:"directoryId"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	SnapType    string    `json:"type"`
}

func (s *storedSnapshot) toSnapshot() Snapshot {
	return Snapshot{
		StartTime:   s.StartTime,
		SnapshotID:  s.SnapshotID,
		DirectoryID: s.DirectoryID,
		Name:        s.Name,
		Status:      SnapshotStatus(s.Status),
		Type:        SnapshotType(s.SnapType),
	}
}

// regionKey builds the composite store.Table primary key ("region|id") shared
// by every region-qualified table registered in store_setup.go.
func regionKey(region, id string) string { return region + "|" + id }

// The following *ID helpers build the composite identifier used both as the
// suffix of a table's "region|id" primary key and as the ID carried by that
// table's persistence DTO (see persistence.go); they mirror the ad hoc
// "directoryID:extra" string keys the pre-conversion map-based backend used
// directly as its map keys.
func dsRegionID(directoryID, regionName string) string { return directoryID + ":" + regionName }
func conditionalForwarderID(directoryID, remoteDomainName string) string {
	return directoryID + ":" + remoteDomainName
}
func logSubscriptionID(directoryID, logGroupName string) string {
	return directoryID + ":" + logGroupName
}
func eventTopicID(directoryID, topicName string) string       { return directoryID + ":" + topicName }
func ldapsSettingID(directoryID, ldapsType string) string     { return directoryID + ":" + ldapsType }
func clientAuthSettingID(directoryID, authType string) string { return directoryID + ":" + authType }

// InMemoryBackend implements StorageBackend using in-memory maps, nested per region.
//
// Sixteen resource collections that were previously nested by region (outer
// key = region, e.g. map[string]map[string]*storedTrust) are now each a
// single flat *store.Table keyed by the composite "region|id" string (see
// regionKey above and store_setup.go), with a companion *store.Index grouping
// entries by region for per-region scans. aliases, ipRoutes, dirDataAccess,
// caEnrollment, dirSettings and updateInfoEntries remain raw region-nested
// maps: their values carry no identity of their own to key a store.Table by
// (see store_setup.go's doc comment for the full rationale).
type InMemoryBackend struct {
	registry *store.Registry

	directories         *store.Table[storedDirectory]
	directoriesByRegion *store.Index[storedDirectory]

	snapshots         *store.Table[storedSnapshot]
	snapshotsByRegion *store.Index[storedSnapshot]

	dsRegions         *store.Table[storedRegion]
	dsRegionsByRegion *store.Index[storedRegion]

	schemaExtensions         *store.Table[storedSchemaExtension]
	schemaExtensionsByRegion *store.Index[storedSchemaExtension]

	conditionalForwarders         *store.Table[storedConditionalForwarder]
	conditionalForwardersByRegion *store.Index[storedConditionalForwarder]

	logSubscriptions         *store.Table[storedLogSubscription]
	logSubscriptionsByRegion *store.Index[storedLogSubscription]

	eventTopics         *store.Table[storedEventTopic]
	eventTopicsByRegion *store.Index[storedEventTopic]

	domainControllers         *store.Table[storedDomainController]
	domainControllersByRegion *store.Index[storedDomainController]

	trusts         *store.Table[storedTrust]
	trustsByRegion *store.Index[storedTrust]

	sharedDirectories         *store.Table[storedSharedDirectory]
	sharedDirectoriesByRegion *store.Index[storedSharedDirectory]

	certificates         *store.Table[storedCertificate]
	certificatesByRegion *store.Index[storedCertificate]

	ldapsSettings         *store.Table[storedLDAPSSetting]
	ldapsSettingsByRegion *store.Index[storedLDAPSSetting]

	clientAuthSettings         *store.Table[storedClientAuthSetting]
	clientAuthSettingsByRegion *store.Index[storedClientAuthSetting]

	radiusSettings         *store.Table[storedRadiusSettings]
	radiusSettingsByRegion *store.Index[storedRadiusSettings]

	adAssessments         *store.Table[storedADAssessment]
	adAssessmentsByRegion *store.Index[storedADAssessment]

	hybridADUpdates         *store.Table[storedHybridADUpdate]
	hybridADUpdatesByRegion *store.Index[storedHybridADUpdate]

	// Raw region-nested maps (outer key = AWS region); see the doc comment
	// above for why these six were not converted to store.Table.
	aliases           map[string]map[string]string // region -> alias -> directoryID
	ipRoutes          map[string]map[string][]storedIpRoute
	dirDataAccess     map[string]map[string]bool
	caEnrollment      map[string]map[string]bool
	dirSettings       map[string]map[string][]*storedDirectorySetting
	updateInfoEntries map[string]map[string][]*storedUpdateInfo

	mu        *lockmetrics.RWMutex
	region    string
	accountID string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:          store.NewRegistry(),
		aliases:           make(map[string]map[string]string),
		ipRoutes:          make(map[string]map[string][]storedIpRoute),
		dirDataAccess:     make(map[string]map[string]bool),
		caEnrollment:      make(map[string]map[string]bool),
		dirSettings:       make(map[string]map[string][]*storedDirectorySetting),
		updateInfoEntries: make(map[string]map[string][]*storedUpdateInfo),
		mu:                lockmetrics.New("directoryservice"),
		accountID:         accountID,
		region:            region,
	}
	registerAllTables(b)

	return b
}

// The following accessor helpers replace the old lazy per-region map
// accessors (b.state(region).directories etc.) with store.Table / store.Index
// operations. Callers must still hold b.mu, exactly as before -- store.Table
// performs no locking of its own (see pkgs/store's package doc).

func (b *InMemoryBackend) directoryGet(region, id string) (*storedDirectory, bool) {
	return b.directories.Get(regionKey(region, id))
}

func (b *InMemoryBackend) directoryPut(v *storedDirectory) { b.directories.Put(v) }

func (b *InMemoryBackend) directoryDelete(region, id string) {
	b.directories.Delete(regionKey(region, id))
}

func (b *InMemoryBackend) directoriesInRegion(region string) []*storedDirectory {
	return b.directoriesByRegion.Get(region)
}

func (b *InMemoryBackend) snapshotGet(region, id string) (*storedSnapshot, bool) {
	return b.snapshots.Get(regionKey(region, id))
}

func (b *InMemoryBackend) snapshotPut(v *storedSnapshot) { b.snapshots.Put(v) }

func (b *InMemoryBackend) snapshotDelete(region, id string) {
	b.snapshots.Delete(regionKey(region, id))
}

func (b *InMemoryBackend) snapshotsInRegion(region string) []*storedSnapshot {
	return b.snapshotsByRegion.Get(region)
}

// The following lazy per-region store helpers return the resource map for the
// given region, creating it on first use. Callers must hold b.mu.

func (b *InMemoryBackend) aliasesStore(region string) map[string]string {
	if b.aliases[region] == nil {
		b.aliases[region] = make(map[string]string)
	}

	return b.aliases[region]
}

func (b *InMemoryBackend) ipRoutesStore(region string) map[string][]storedIpRoute {
	if b.ipRoutes[region] == nil {
		b.ipRoutes[region] = make(map[string][]storedIpRoute)
	}

	return b.ipRoutes[region]
}

func (b *InMemoryBackend) dirDataAccessStore(region string) map[string]bool {
	if b.dirDataAccess[region] == nil {
		b.dirDataAccess[region] = make(map[string]bool)
	}

	return b.dirDataAccess[region]
}

func (b *InMemoryBackend) caEnrollmentStore(region string) map[string]bool {
	if b.caEnrollment[region] == nil {
		b.caEnrollment[region] = make(map[string]bool)
	}

	return b.caEnrollment[region]
}

func (b *InMemoryBackend) dirSettingsStore(region string) map[string][]*storedDirectorySetting {
	if b.dirSettings[region] == nil {
		b.dirSettings[region] = make(map[string][]*storedDirectorySetting)
	}

	return b.dirSettings[region]
}

func (b *InMemoryBackend) updateInfoEntriesStore(region string) map[string][]*storedUpdateInfo {
	if b.updateInfoEntries[region] == nil {
		b.updateInfoEntries[region] = make(map[string][]*storedUpdateInfo)
	}

	return b.updateInfoEntries[region]
}

func (b *InMemoryBackend) newDirectoryID() string {
	return fmt.Sprintf("d-%s", uuid.NewString()[:10])
}

func (b *InMemoryBackend) newSnapshotID() string {
	return fmt.Sprintf("s-%s", uuid.NewString()[:10])
}

func (b *InMemoryBackend) defaultAlias(directoryID string) string {
	return directoryID
}

func (b *InMemoryBackend) defaultAccessURL(alias string) string {
	return fmt.Sprintf("%s.awsapps.com", alias)
}

// encodePageToken encodes an integer offset as an opaque base64 token.
func encodePageToken(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// decodePageToken decodes a token produced by encodePageToken.
func decodePageToken(tok string) (int, error) {
	b, err := base64.StdEncoding.DecodeString(tok)
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(string(b))
}

// directoryLifecycleDelay is the time between each stage transition on directory creation.
const directoryLifecycleDelay = 50 * time.Millisecond

// restoreLifecycleDelay is the delay before a restoring directory returns to Active.
const restoreLifecycleDelay = 100 * time.Millisecond

// transitionDirectoryToActive runs the Requested → Creating → Active lifecycle.
// Must be called as a goroutine after the directory has been stored.
func (b *InMemoryBackend) transitionDirectoryToActive(region, dirID string) {
	time.Sleep(directoryLifecycleDelay)

	b.mu.Lock("transitionDirectoryToActive:creating")
	if d, ok := b.directoryGet(region, dirID); ok && d.Stage == string(DirectoryStageRequested) {
		d.Stage = string(DirectoryStageCreating)
	}
	b.mu.Unlock()

	time.Sleep(directoryLifecycleDelay)

	b.mu.Lock("transitionDirectoryToActive:active")
	if d, ok := b.directoryGet(region, dirID); ok && d.Stage == string(DirectoryStageCreating) {
		d.Stage = string(DirectoryStageActive)
	}
	b.mu.Unlock()
}

func (b *InMemoryBackend) newStoredDirectory(
	region, name, shortName, description string,
	dirType DirectoryType,
	size DirectorySize,
	edition DirectoryEdition,
	vpcSettings *DirectoryVpcSettings,
	tags []Tag,
) *storedDirectory {
	id := b.newDirectoryID()
	alias := b.defaultAlias(id)

	d := &storedDirectory{
		region:      region,
		LaunchTime:  time.Now().UTC(),
		DirectoryID: id,
		Name:        name,
		ShortName:   shortName,
		Description: description,
		Alias:       alias,
		AccessURL:   b.defaultAccessURL(alias),
		DirType:     string(dirType),
		Stage:       string(DirectoryStageRequested),
		Size:        string(size),
		Edition:     string(edition),
		Tags:        tagsToMap(tags),
	}
	if vpcSettings != nil {
		d.VpcSettings = &storedVpcSettings{
			VpcID:             vpcSettings.VpcID,
			SubnetIDs:         vpcSettings.SubnetIDs,
			SecurityGroupIDs:  vpcSettings.SecurityGroupIDs,
			AvailabilityZones: vpcSettings.AvailabilityZones,
		}
	}

	return d
}

// CreateDirectory creates a new Simple AD directory.
func (b *InMemoryBackend) CreateDirectory(
	ctx context.Context,
	name, shortName, description, _ string,
	size DirectorySize, vpcSettings *DirectoryVpcSettings, tags []Tag,
) (*Directory, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateDirectory")
	defer b.mu.Unlock()

	if name == "" {
		return nil, ErrInvalidParameter
	}
	if size != DirectorySizeSmall && size != DirectorySizeLarge && size != "" {
		return nil, ErrInvalidParameter
	}

	var count int32
	for _, d := range b.directoriesInRegion(region) {
		if DirectoryType(d.DirType) == DirectoryTypeSimpleAD {
			count++
		}
	}
	if count >= defaultSimpleADLimit {
		return nil, ErrDirectoryLimitExceeded
	}

	d := b.newStoredDirectory(
		region,
		name,
		shortName,
		description,
		DirectoryTypeSimpleAD,
		size,
		"",
		vpcSettings,
		tags,
	)
	b.directoryPut(d)
	b.aliasesStore(region)[d.Alias] = d.DirectoryID

	cp := d.toDirectory()

	go b.transitionDirectoryToActive(region, d.DirectoryID)

	return &cp, nil
}

// CreateMicrosoftAD creates a new Managed Microsoft AD directory.
func (b *InMemoryBackend) CreateMicrosoftAD(
	ctx context.Context,
	name, shortName, description, _ string,
	edition DirectoryEdition, vpcSettings *DirectoryVpcSettings, tags []Tag,
) (*Directory, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateMicrosoftAD")
	defer b.mu.Unlock()

	if name == "" {
		return nil, ErrInvalidParameter
	}
	if edition != DirectoryEditionEnterprise && edition != DirectoryEditionStandard &&
		edition != "" {
		return nil, ErrInvalidParameter
	}

	var count int32
	for _, d := range b.directoriesInRegion(region) {
		if DirectoryType(d.DirType) == DirectoryTypeMicrosoftAD {
			count++
		}
	}
	if count >= defaultMicrosoftADLimit {
		return nil, ErrDirectoryLimitExceeded
	}

	d := b.newStoredDirectory(
		region,
		name,
		shortName,
		description,
		DirectoryTypeMicrosoftAD,
		"",
		edition,
		vpcSettings,
		tags,
	)
	b.directoryPut(d)
	b.aliasesStore(region)[d.Alias] = d.DirectoryID

	cp := d.toDirectory()

	go b.transitionDirectoryToActive(region, d.DirectoryID)

	return &cp, nil
}

// DeleteDirectory deletes a directory and all associated resources.
func (b *InMemoryBackend) DeleteDirectory(ctx context.Context, directoryID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteDirectory")
	defer b.mu.Unlock()

	d, ok := b.directoryGet(region, directoryID)
	if !ok {
		return ErrDirectoryNotFound
	}

	delete(b.aliasesStore(region), d.Alias)
	b.directoryDelete(region, directoryID)
	b.cascadeDeleteDirectory(region, directoryID)

	return nil
}

// DescribeDirectories returns directories, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeDirectories(
	ctx context.Context,
	directoryIDs []string,
	limit int32,
	nextToken string,
) ([]*Directory, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeDirectories")
	defer b.mu.RUnlock()

	var ids []string

	if len(directoryIDs) > 0 {
		for _, id := range directoryIDs {
			if _, ok := b.directoryGet(region, id); !ok {
				return nil, "", ErrDirectoryNotFound
			}
		}
		ids = append([]string(nil), directoryIDs...)
		sort.Strings(ids)
	} else {
		for _, d := range b.directoriesInRegion(region) {
			ids = append(ids, d.DirectoryID)
		}
		sort.Strings(ids)
	}

	start := 0
	if nextToken != "" {
		if n, err := decodePageToken(nextToken); err == nil && n > 0 {
			start = n
		}
	}

	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(ids))

	result := make([]*Directory, 0, end-start)
	for _, id := range ids[start:end] {
		d, _ := b.directoryGet(region, id)
		cp := d.toDirectory()
		result = append(result, &cp)
	}

	var outToken string
	if end < len(ids) {
		outToken = encodePageToken(end)
	}

	return result, outToken, nil
}

// CreateAlias creates an alias for a directory.
func (b *InMemoryBackend) CreateAlias(ctx context.Context, directoryID, alias string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateAlias")
	defer b.mu.Unlock()

	d, ok := b.directoryGet(region, directoryID)
	if !ok {
		return ErrDirectoryNotFound
	}

	aliases := b.aliasesStore(region)
	if _, taken := aliases[alias]; taken {
		return ErrAliasAlreadyExists
	}

	delete(aliases, d.Alias)
	d.Alias = alias
	d.AccessURL = b.defaultAccessURL(alias)
	aliases[alias] = directoryID

	return nil
}

// EnableSso enables single sign-on for a directory.
func (b *InMemoryBackend) EnableSso(ctx context.Context, directoryID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("EnableSso")
	defer b.mu.Unlock()

	d, ok := b.directoryGet(region, directoryID)
	if !ok {
		return ErrDirectoryNotFound
	}

	d.SsoEnabled = true

	return nil
}

// DisableSso disables single sign-on for a directory.
func (b *InMemoryBackend) DisableSso(ctx context.Context, directoryID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DisableSso")
	defer b.mu.Unlock()

	d, ok := b.directoryGet(region, directoryID)
	if !ok {
		return ErrDirectoryNotFound
	}

	d.SsoEnabled = false

	return nil
}

// GetDirectoryLimits returns directory limits for the region.
func (b *InMemoryBackend) GetDirectoryLimits(ctx context.Context) *DirectoryLimits {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetDirectoryLimits")
	defer b.mu.RUnlock()

	var simpleADCount, msADCount, connectedCount int32

	for _, d := range b.directoriesInRegion(region) {
		switch DirectoryType(d.DirType) { //nolint:exhaustive // existing issue.
		case DirectoryTypeSimpleAD:
			simpleADCount++
		case DirectoryTypeMicrosoftAD:
			msADCount++
		case DirectoryTypeADConnector:
			connectedCount++
		}
	}

	return &DirectoryLimits{
		CloudOnlyDirectoriesCurrentCount: simpleADCount,
		CloudOnlyDirectoriesLimit:        defaultSimpleADLimit,
		CloudOnlyDirectoriesLimitReached: simpleADCount >= defaultSimpleADLimit,
		CloudOnlyMicrosoftADCurrentCount: msADCount,
		CloudOnlyMicrosoftADLimit:        defaultMicrosoftADLimit,
		CloudOnlyMicrosoftADLimitReached: msADCount >= defaultMicrosoftADLimit,
		ConnectedDirectoriesCurrentCount: connectedCount,
		ConnectedDirectoriesLimit:        10,                   //nolint:mnd // existing issue.
		ConnectedDirectoriesLimitReached: connectedCount >= 10, //nolint:mnd // existing issue.
	}
}

// CreateSnapshot creates a manual snapshot for a directory.
func (b *InMemoryBackend) CreateSnapshot(
	ctx context.Context,
	directoryID, name string,
) (*Snapshot, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateSnapshot")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return nil, ErrDirectoryNotFound
	}

	var count int32
	for _, s := range b.snapshotsInRegion(region) {
		if s.DirectoryID == directoryID && s.SnapType == string(SnapshotTypeManual) {
			count++
		}
	}
	if count >= defaultSnapshotLimit {
		return nil, ErrSnapshotLimitExceeded
	}

	id := b.newSnapshotID()
	now := time.Now().UTC()

	s := &storedSnapshot{
		region:      region,
		StartTime:   now,
		SnapshotID:  id,
		DirectoryID: directoryID,
		Name:        name,
		Status:      string(SnapshotStatusCompleted),
		SnapType:    string(SnapshotTypeManual),
	}
	b.snapshotPut(s)

	cp := s.toSnapshot()

	return &cp, nil
}

// DeleteSnapshot deletes a snapshot.
func (b *InMemoryBackend) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteSnapshot")
	defer b.mu.Unlock()

	if _, ok := b.snapshotGet(region, snapshotID); !ok {
		return ErrSnapshotNotFound
	}

	b.snapshotDelete(region, snapshotID)

	return nil
}

// DescribeSnapshots returns snapshots filtered by directory and/or snapshot IDs.
func (b *InMemoryBackend) DescribeSnapshots(
	ctx context.Context,
	directoryID string,
	snapshotIDs []string,
	limit int32,
	nextToken string,
) ([]*Snapshot, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeSnapshots")
	defer b.mu.RUnlock()

	// Build filter set for snapshot IDs.
	filterIDs := make(map[string]bool, len(snapshotIDs))
	for _, id := range snapshotIDs {
		filterIDs[id] = true
	}

	ids := make([]string, 0, len(b.snapshotsInRegion(region)))
	for _, snap := range b.snapshotsInRegion(region) {
		if directoryID != "" && snap.DirectoryID != directoryID {
			continue
		}
		if len(filterIDs) > 0 && !filterIDs[snap.SnapshotID] {
			continue
		}
		ids = append(ids, snap.SnapshotID)
	}
	sort.Strings(ids)

	start := 0
	if nextToken != "" {
		if n, err := decodePageToken(nextToken); err == nil && n > 0 {
			start = n
		}
	}

	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(ids))

	result := make([]*Snapshot, 0, end-start)
	for _, id := range ids[start:end] {
		s, _ := b.snapshotGet(region, id)
		cp := s.toSnapshot()
		result = append(result, &cp)
	}

	var outToken string
	if end < len(ids) {
		outToken = encodePageToken(end)
	}

	return result, outToken, nil
}

// GetSnapshotLimits returns snapshot limits for a directory.
func (b *InMemoryBackend) GetSnapshotLimits(
	ctx context.Context,
	directoryID string,
) (*SnapshotLimits, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetSnapshotLimits")
	defer b.mu.RUnlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return nil, ErrDirectoryNotFound
	}

	var count int32
	for _, snap := range b.snapshotsInRegion(region) {
		if snap.DirectoryID == directoryID && snap.SnapType == string(SnapshotTypeManual) {
			count++
		}
	}

	return &SnapshotLimits{
		ManualSnapshotsCurrentCount: count,
		ManualSnapshotsLimit:        defaultSnapshotLimit,
		ManualSnapshotsLimitReached: count >= defaultSnapshotLimit,
	}, nil
}

// RestoreFromSnapshot simulates restoring a directory from a snapshot.
func (b *InMemoryBackend) RestoreFromSnapshot(ctx context.Context, snapshotID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("RestoreFromSnapshot")
	defer b.mu.Unlock()

	snap, ok := b.snapshotGet(region, snapshotID)
	if !ok {
		return ErrSnapshotNotFound
	}

	dir, ok := b.directoryGet(region, snap.DirectoryID)
	if !ok {
		return ErrDirectoryNotFound
	}

	dir.Stage = string(DirectoryStageRestoring)

	dirID := dir.DirectoryID

	go func(region, id string) {
		time.Sleep(restoreLifecycleDelay)

		b.mu.Lock("RestoreFromSnapshot:active")
		if d, exists := b.directoryGet(region, id); exists && d.Stage == string(DirectoryStageRestoring) {
			d.Stage = string(DirectoryStageActive)
		}
		b.mu.Unlock()
	}(region, dirID)

	return nil
}

// AddTagsToResource adds or updates tags on a directory.
func (b *InMemoryBackend) AddTagsToResource(
	ctx context.Context,
	resourceID string,
	tags []Tag,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()

	d, ok := b.directoryGet(region, resourceID)
	if !ok {
		return ErrDirectoryNotFound
	}

	if d.Tags == nil {
		d.Tags = make(map[string]string)
	}

	for _, t := range tags {
		d.Tags[t.Key] = t.Value
	}

	return nil
}

// RemoveTagsFromResource removes tags from a directory.
func (b *InMemoryBackend) RemoveTagsFromResource(
	ctx context.Context,
	resourceID string,
	tagKeys []string,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()

	d, ok := b.directoryGet(region, resourceID)
	if !ok {
		return ErrDirectoryNotFound
	}

	for _, k := range tagKeys {
		delete(d.Tags, k)
	}

	return nil
}

// ListTagsForResource returns tags for a directory with pagination.
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	resourceID string,
	limit int32,
	nextToken string,
) ([]Tag, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	d, ok := b.directoryGet(region, resourceID)
	if !ok {
		return nil, "", ErrDirectoryNotFound
	}

	all := make([]Tag, 0, len(d.Tags))
	for k, v := range d.Tags {
		all = append(all, Tag{Key: k, Value: v})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Key < all[j].Key })

	start := 0
	if nextToken != "" {
		if n, err := decodePageToken(nextToken); err == nil && n > 0 {
			start = n
		}
	}

	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(all))
	result := all[start:end]

	var outToken string
	if end < len(all) {
		outToken = encodePageToken(end)
	}

	return result, outToken, nil
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetAllState()
}

// resetAllState clears every registered table and every raw region-nested
// map, returning the backend to the state NewInMemoryBackend leaves it in
// (minus accountID/region, which callers restore separately). Callers must
// hold b.mu.
func (b *InMemoryBackend) resetAllState() {
	b.registry.ResetAll()
	b.aliases = make(map[string]map[string]string)
	b.ipRoutes = make(map[string]map[string][]storedIpRoute)
	b.dirDataAccess = make(map[string]map[string]bool)
	b.caEnrollment = make(map[string]map[string]bool)
	b.dirSettings = make(map[string]map[string][]*storedDirectorySetting)
	b.updateInfoEntries = make(map[string]map[string][]*storedUpdateInfo)
}

func tagsToMap(tags []Tag) map[string]string {
	if len(tags) == 0 {
		return make(map[string]string)
	}

	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}
