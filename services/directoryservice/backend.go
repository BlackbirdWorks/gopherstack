package directoryservice

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
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

// regionState holds all resources for a single AWS region.
type regionState struct {
	directories           map[string]*storedDirectory
	snapshots             map[string]*storedSnapshot
	aliases               map[string]string // alias → directoryID
	ipRoutes              map[string][]storedIpRoute
	regions               map[string]*storedRegion
	schemaExtensions      map[string]*storedSchemaExtension
	conditionalForwarders map[string]*storedConditionalForwarder
	logSubscriptions      map[string]*storedLogSubscription
	eventTopics           map[string]*storedEventTopic
	domainControllers     map[string]*storedDomainController
	trusts                map[string]*storedTrust
	sharedDirectories     map[string]*storedSharedDirectory
	certificates          map[string]*storedCertificate
	ldapsSettings         map[string]*storedLDAPSSetting
	clientAuthSettings    map[string]*storedClientAuthSetting
	radiusSettings        map[string]*storedRadiusSettings
	dirDataAccess         map[string]bool
	caEnrollment          map[string]bool
	adAssessments         map[string]*storedADAssessment
	dirSettings           map[string][]*storedDirectorySetting
	updateInfoEntries     map[string][]*storedUpdateInfo
	hybridADUpdates       map[string]*storedHybridADUpdate
}

func newRegionState() *regionState {
	return &regionState{
		directories:           make(map[string]*storedDirectory),
		snapshots:             make(map[string]*storedSnapshot),
		aliases:               make(map[string]string),
		ipRoutes:              make(map[string][]storedIpRoute),
		regions:               make(map[string]*storedRegion),
		schemaExtensions:      make(map[string]*storedSchemaExtension),
		conditionalForwarders: make(map[string]*storedConditionalForwarder),
		logSubscriptions:      make(map[string]*storedLogSubscription),
		eventTopics:           make(map[string]*storedEventTopic),
		domainControllers:     make(map[string]*storedDomainController),
		trusts:                make(map[string]*storedTrust),
		sharedDirectories:     make(map[string]*storedSharedDirectory),
		certificates:          make(map[string]*storedCertificate),
		ldapsSettings:         make(map[string]*storedLDAPSSetting),
		clientAuthSettings:    make(map[string]*storedClientAuthSetting),
		radiusSettings:        make(map[string]*storedRadiusSettings),
		dirDataAccess:         make(map[string]bool),
		caEnrollment:          make(map[string]bool),
		adAssessments:         make(map[string]*storedADAssessment),
		dirSettings:           make(map[string][]*storedDirectorySetting),
		updateInfoEntries:     make(map[string][]*storedUpdateInfo),
		hybridADUpdates:       make(map[string]*storedHybridADUpdate),
	}
}

// regionSnapshot is the serializable per-region backend state.
type regionSnapshot struct {
	Directories           map[string]*storedDirectory            `json:"directories"`
	Snapshots             map[string]*storedSnapshot             `json:"snapshots"`
	Aliases               map[string]string                      `json:"aliases"`
	IPRoutes              map[string][]storedIpRoute             `json:"ipRoutes,omitempty"`
	Regions               map[string]*storedRegion               `json:"regions,omitempty"`
	SchemaExtensions      map[string]*storedSchemaExtension      `json:"schemaExtensions,omitempty"`
	ConditionalForwarders map[string]*storedConditionalForwarder `json:"conditionalForwarders,omitempty"`
	LogSubscriptions      map[string]*storedLogSubscription      `json:"logSubscriptions,omitempty"`
	EventTopics           map[string]*storedEventTopic           `json:"eventTopics,omitempty"`
	DomainControllers     map[string]*storedDomainController     `json:"domainControllers,omitempty"`
	Trusts                map[string]*storedTrust                `json:"trusts,omitempty"`
	SharedDirectories     map[string]*storedSharedDirectory      `json:"sharedDirectories,omitempty"`
	Certificates          map[string]*storedCertificate          `json:"certificates,omitempty"`
	LDAPSSettings         map[string]*storedLDAPSSetting         `json:"ldapsSettings,omitempty"`
	ClientAuthSettings    map[string]*storedClientAuthSetting    `json:"clientAuthSettings,omitempty"`
	RadiusSettings        map[string]*storedRadiusSettings       `json:"radiusSettings,omitempty"`
	DirDataAccess         map[string]bool                        `json:"dirDataAccess,omitempty"`
	CAEnrollment          map[string]bool                        `json:"caEnrollment,omitempty"`
	ADAssessments         map[string]*storedADAssessment         `json:"adAssessments,omitempty"`
	DirSettings           map[string][]*storedDirectorySetting   `json:"dirSettings,omitempty"`
	UpdateInfoEntries     map[string][]*storedUpdateInfo         `json:"updateInfoEntries,omitempty"`
	HybridADUpdates       map[string]*storedHybridADUpdate       `json:"hybridADUpdates,omitempty"`
}

// backendSnapshot is the serializable backend state, nested by region.
type backendSnapshot struct {
	Regions map[string]regionSnapshot `json:"regions"`
}

// InMemoryBackend implements StorageBackend using in-memory maps, nested per region.
type InMemoryBackend struct {
	states    map[string]*regionState // region → state
	mu        *lockmetrics.RWMutex
	region    string
	accountID string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:        lockmetrics.New("directoryservice"),
		accountID: accountID,
		region:    region,
		states:    make(map[string]*regionState),
	}
}

// state returns the per-region state for region, lazily creating it.
// Callers must hold b.mu.
func (b *InMemoryBackend) state(region string) *regionState {
	st, ok := b.states[region]
	if !ok {
		st = newRegionState()
		b.states[region] = st
	}

	return st
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
	if d, ok := b.state(region).directories[dirID]; ok && d.Stage == string(DirectoryStageRequested) {
		d.Stage = string(DirectoryStageCreating)
	}
	b.mu.Unlock()

	time.Sleep(directoryLifecycleDelay)

	b.mu.Lock("transitionDirectoryToActive:active")
	if d, ok := b.state(region).directories[dirID]; ok && d.Stage == string(DirectoryStageCreating) {
		d.Stage = string(DirectoryStageActive)
	}
	b.mu.Unlock()
}

func (b *InMemoryBackend) newStoredDirectory(
	name, shortName, description string,
	dirType DirectoryType,
	size DirectorySize,
	edition DirectoryEdition,
	vpcSettings *DirectoryVpcSettings,
	tags []Tag,
) *storedDirectory {
	id := b.newDirectoryID()
	alias := b.defaultAlias(id)

	d := &storedDirectory{
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

	st := b.state(region)

	var count int32
	for _, d := range st.directories {
		if DirectoryType(d.DirType) == DirectoryTypeSimpleAD {
			count++
		}
	}
	if count >= defaultSimpleADLimit {
		return nil, ErrDirectoryLimitExceeded
	}

	d := b.newStoredDirectory(
		name,
		shortName,
		description,
		DirectoryTypeSimpleAD,
		size,
		"",
		vpcSettings,
		tags,
	)
	st.directories[d.DirectoryID] = d
	st.aliases[d.Alias] = d.DirectoryID

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

	st := b.state(region)

	var count int32
	for _, d := range st.directories {
		if DirectoryType(d.DirType) == DirectoryTypeMicrosoftAD {
			count++
		}
	}
	if count >= defaultMicrosoftADLimit {
		return nil, ErrDirectoryLimitExceeded
	}

	d := b.newStoredDirectory(
		name,
		shortName,
		description,
		DirectoryTypeMicrosoftAD,
		"",
		edition,
		vpcSettings,
		tags,
	)
	st.directories[d.DirectoryID] = d
	st.aliases[d.Alias] = d.DirectoryID

	cp := d.toDirectory()

	go b.transitionDirectoryToActive(region, d.DirectoryID)

	return &cp, nil
}

// DeleteDirectory deletes a directory and all associated resources.
func (b *InMemoryBackend) DeleteDirectory(ctx context.Context, directoryID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteDirectory")
	defer b.mu.Unlock()

	st := b.state(region)

	d, ok := st.directories[directoryID]
	if !ok {
		return ErrDirectoryNotFound
	}

	delete(st.aliases, d.Alias)
	delete(st.directories, directoryID)
	cascadeDeleteDirectory(st, directoryID)

	return nil
}

// cascadeDeleteDirectory removes all resources that belong to directoryID from st.
// Must be called with the backend lock held.
func cascadeDeleteDirectory(st *regionState, directoryID string) {
	for id, snap := range st.snapshots {
		if snap.DirectoryID == directoryID {
			delete(st.snapshots, id)
		}
	}

	delete(st.ipRoutes, directoryID)
	delete(st.radiusSettings, directoryID)
	delete(st.dirDataAccess, directoryID)
	delete(st.caEnrollment, directoryID)
	delete(st.dirSettings, directoryID)
	delete(st.updateInfoEntries, directoryID)

	deleteMappedByDir(
		st.regions,
		directoryID,
		func(r *storedRegion) string { return r.DirectoryID },
	)
	deleteMappedByDir(
		st.schemaExtensions,
		directoryID,
		func(e *storedSchemaExtension) string { return e.DirectoryID },
	)
	deleteMappedByDir(
		st.conditionalForwarders,
		directoryID,
		func(f *storedConditionalForwarder) string { return f.DirectoryID },
	)
	deleteMappedByDir(
		st.logSubscriptions,
		directoryID,
		func(s *storedLogSubscription) string { return s.DirectoryID },
	)
	deleteMappedByDir(
		st.eventTopics,
		directoryID,
		func(t *storedEventTopic) string { return t.DirectoryID },
	)
	deleteMappedByDir(
		st.domainControllers,
		directoryID,
		func(d *storedDomainController) string { return d.DirectoryID },
	)
	deleteMappedByDir(st.trusts, directoryID, func(t *storedTrust) string { return t.DirectoryID })
	deleteMappedByDir(
		st.sharedDirectories,
		directoryID,
		func(s *storedSharedDirectory) string { return s.OwnerDirectoryID },
	)
	deleteMappedByDir(
		st.certificates,
		directoryID,
		func(c *storedCertificate) string { return c.DirectoryID },
	)
	deleteMappedByDir(
		st.ldapsSettings,
		directoryID,
		func(l *storedLDAPSSetting) string { return l.DirectoryID },
	)
	deleteMappedByDir(
		st.clientAuthSettings,
		directoryID,
		func(a *storedClientAuthSetting) string { return a.DirectoryID },
	)
	deleteMappedByDir(
		st.adAssessments,
		directoryID,
		func(a *storedADAssessment) string { return a.DirectoryID },
	)
	deleteMappedByDir(
		st.hybridADUpdates,
		directoryID,
		func(h *storedHybridADUpdate) string { return h.DirectoryID },
	)
}

// deleteMappedByDir deletes all entries from m where getDir(v) == directoryID.
func deleteMappedByDir[V any](m map[string]*V, directoryID string, getDir func(*V) string) {
	for key, v := range m {
		if getDir(v) == directoryID {
			delete(m, key)
		}
	}
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

	st := b.state(region)

	var ids []string

	if len(directoryIDs) > 0 {
		for _, id := range directoryIDs {
			if _, ok := st.directories[id]; !ok {
				return nil, "", ErrDirectoryNotFound
			}
		}
		ids = append([]string(nil), directoryIDs...)
		sort.Strings(ids)
	} else {
		ids = make([]string, 0, len(st.directories))
		for id := range st.directories {
			ids = append(ids, id)
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
		d := st.directories[id]
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

	st := b.state(region)

	d, ok := st.directories[directoryID]
	if !ok {
		return ErrDirectoryNotFound
	}

	if _, taken := st.aliases[alias]; taken {
		return ErrAliasAlreadyExists
	}

	delete(st.aliases, d.Alias)
	d.Alias = alias
	d.AccessURL = b.defaultAccessURL(alias)
	st.aliases[alias] = directoryID

	return nil
}

// EnableSso enables single sign-on for a directory.
func (b *InMemoryBackend) EnableSso(ctx context.Context, directoryID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("EnableSso")
	defer b.mu.Unlock()

	d, ok := b.state(region).directories[directoryID]
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

	d, ok := b.state(region).directories[directoryID]
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

	for _, d := range b.state(region).directories {
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

	st := b.state(region)

	if _, ok := st.directories[directoryID]; !ok {
		return nil, ErrDirectoryNotFound
	}

	var count int32
	for _, s := range st.snapshots {
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
		StartTime:   now,
		SnapshotID:  id,
		DirectoryID: directoryID,
		Name:        name,
		Status:      string(SnapshotStatusCompleted),
		SnapType:    string(SnapshotTypeManual),
	}
	st.snapshots[id] = s

	cp := s.toSnapshot()

	return &cp, nil
}

// DeleteSnapshot deletes a snapshot.
func (b *InMemoryBackend) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteSnapshot")
	defer b.mu.Unlock()

	st := b.state(region)

	if _, ok := st.snapshots[snapshotID]; !ok {
		return ErrSnapshotNotFound
	}

	delete(st.snapshots, snapshotID)

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

	st := b.state(region)

	// Build filter set for snapshot IDs.
	filterIDs := make(map[string]bool, len(snapshotIDs))
	for _, id := range snapshotIDs {
		filterIDs[id] = true
	}

	ids := make([]string, 0, len(st.snapshots))
	for id, snap := range st.snapshots {
		if directoryID != "" && snap.DirectoryID != directoryID {
			continue
		}
		if len(filterIDs) > 0 && !filterIDs[id] {
			continue
		}
		ids = append(ids, id)
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
		s := st.snapshots[id]
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

	st := b.state(region)

	if _, ok := st.directories[directoryID]; !ok {
		return nil, ErrDirectoryNotFound
	}

	var count int32
	for _, snap := range st.snapshots {
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

	snap, ok := b.state(region).snapshots[snapshotID]
	if !ok {
		return ErrSnapshotNotFound
	}

	dir, ok := b.state(region).directories[snap.DirectoryID]
	if !ok {
		return ErrDirectoryNotFound
	}

	dir.Stage = string(DirectoryStageRestoring)

	dirID := dir.DirectoryID

	go func(region, id string) {
		time.Sleep(restoreLifecycleDelay)

		b.mu.Lock("RestoreFromSnapshot:active")
		if d, exists := b.state(region).directories[id]; exists && d.Stage == string(DirectoryStageRestoring) {
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

	d, ok := b.state(region).directories[resourceID]
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

	d, ok := b.state(region).directories[resourceID]
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

	d, ok := b.state(region).directories[resourceID]
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

	b.states = make(map[string]*regionState)
}

// BackendSnapshot serializes the backend state to JSON, nested by region.
func (b *InMemoryBackend) BackendSnapshot() []byte {
	b.mu.RLock("BackendSnapshot")
	defer b.mu.RUnlock()

	regions := make(map[string]regionSnapshot, len(b.states))
	for region, st := range b.states {
		regions[region] = regionSnapshot{
			Directories:           st.directories,
			Snapshots:             st.snapshots,
			Aliases:               st.aliases,
			IPRoutes:              st.ipRoutes,
			Regions:               st.regions,
			SchemaExtensions:      st.schemaExtensions,
			ConditionalForwarders: st.conditionalForwarders,
			LogSubscriptions:      st.logSubscriptions,
			EventTopics:           st.eventTopics,
			DomainControllers:     st.domainControllers,
			Trusts:                st.trusts,
			SharedDirectories:     st.sharedDirectories,
			Certificates:          st.certificates,
			LDAPSSettings:         st.ldapsSettings,
			ClientAuthSettings:    st.clientAuthSettings,
			RadiusSettings:        st.radiusSettings,
			DirDataAccess:         st.dirDataAccess,
			CAEnrollment:          st.caEnrollment,
			ADAssessments:         st.adAssessments,
			DirSettings:           st.dirSettings,
			UpdateInfoEntries:     st.updateInfoEntries,
			HybridADUpdates:       st.hybridADUpdates,
		}
	}

	data, _ := json.Marshal(backendSnapshot{Regions: regions})

	return data
}

// restoreRegionState copies non-nil fields from rs into st.
// Complexity is inherent: one branch per region-state field (22 fields, no logic).
//
//nolint:gocognit,cyclop // flat nil-guard per field; no real branching logic
func restoreRegionState(st *regionState, rs regionSnapshot) {
	if rs.Directories != nil {
		st.directories = rs.Directories
	}
	if rs.Snapshots != nil {
		st.snapshots = rs.Snapshots
	}
	if rs.Aliases != nil {
		st.aliases = rs.Aliases
	}
	if rs.IPRoutes != nil {
		st.ipRoutes = rs.IPRoutes
	}
	if rs.Regions != nil {
		st.regions = rs.Regions
	}
	if rs.SchemaExtensions != nil {
		st.schemaExtensions = rs.SchemaExtensions
	}
	if rs.ConditionalForwarders != nil {
		st.conditionalForwarders = rs.ConditionalForwarders
	}
	if rs.LogSubscriptions != nil {
		st.logSubscriptions = rs.LogSubscriptions
	}
	if rs.EventTopics != nil {
		st.eventTopics = rs.EventTopics
	}
	if rs.DomainControllers != nil {
		st.domainControllers = rs.DomainControllers
	}
	if rs.Trusts != nil {
		st.trusts = rs.Trusts
	}
	if rs.SharedDirectories != nil {
		st.sharedDirectories = rs.SharedDirectories
	}
	if rs.Certificates != nil {
		st.certificates = rs.Certificates
	}
	if rs.LDAPSSettings != nil {
		st.ldapsSettings = rs.LDAPSSettings
	}
	if rs.ClientAuthSettings != nil {
		st.clientAuthSettings = rs.ClientAuthSettings
	}
	if rs.RadiusSettings != nil {
		st.radiusSettings = rs.RadiusSettings
	}
	if rs.DirDataAccess != nil {
		st.dirDataAccess = rs.DirDataAccess
	}
	if rs.CAEnrollment != nil {
		st.caEnrollment = rs.CAEnrollment
	}
	if rs.ADAssessments != nil {
		st.adAssessments = rs.ADAssessments
	}
	if rs.DirSettings != nil {
		st.dirSettings = rs.DirSettings
	}
	if rs.UpdateInfoEntries != nil {
		st.updateInfoEntries = rs.UpdateInfoEntries
	}
	if rs.HybridADUpdates != nil {
		st.hybridADUpdates = rs.HybridADUpdates
	}
}

// Restore deserializes backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "directoryservice", data, &snap); err != nil {
		return err
	}

	b.states = make(map[string]*regionState)
	for region, rs := range snap.Regions {
		st := newRegionState()
		restoreRegionState(st, rs)
		b.states[region] = st
	}

	return nil
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
