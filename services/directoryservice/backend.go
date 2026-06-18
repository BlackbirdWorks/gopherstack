package directoryservice

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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
	Directories map[string]*storedDirectory `json:"directories"`
	Snapshots   map[string]*storedSnapshot  `json:"snapshots"`
	Aliases     map[string]string           `json:"aliases"` // alias → directoryID
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
		Stage:       string(DirectoryStageActive),
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

	st := b.state(region)
	d := b.newStoredDirectory(name, shortName, description, DirectoryTypeSimpleAD, size, "", vpcSettings, tags)
	st.directories[d.DirectoryID] = d
	st.aliases[d.Alias] = d.DirectoryID

	cp := d.toDirectory()

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

	st := b.state(region)
	d := b.newStoredDirectory(name, shortName, description, DirectoryTypeMicrosoftAD, "", edition, vpcSettings, tags)
	st.directories[d.DirectoryID] = d
	st.aliases[d.Alias] = d.DirectoryID

	cp := d.toDirectory()

	return &cp, nil
}

// DeleteDirectory deletes a directory.
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

	// Delete associated snapshots.
	for id, snap := range st.snapshots {
		if snap.DirectoryID == directoryID {
			delete(st.snapshots, id)
		}
	}

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
		for i, id := range ids {
			if id == nextToken {
				start = i

				break
			}
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
		outToken = ids[end]
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
func (b *InMemoryBackend) CreateSnapshot(ctx context.Context, directoryID, name string) (*Snapshot, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateSnapshot")
	defer b.mu.Unlock()

	st := b.state(region)

	if _, ok := st.directories[directoryID]; !ok {
		return nil, ErrDirectoryNotFound
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
		for i, id := range ids {
			if id == nextToken {
				start = i

				break
			}
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
		outToken = ids[end]
	}

	return result, outToken, nil
}

// GetSnapshotLimits returns snapshot limits for a directory.
func (b *InMemoryBackend) GetSnapshotLimits(ctx context.Context, directoryID string) (*SnapshotLimits, error) {
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

	b.mu.RLock("RestoreFromSnapshot")
	defer b.mu.RUnlock()

	if _, ok := b.state(region).snapshots[snapshotID]; !ok {
		return ErrSnapshotNotFound
	}

	return nil
}

// AddTagsToResource adds or updates tags on a directory.
func (b *InMemoryBackend) AddTagsToResource(ctx context.Context, resourceID string, tags []Tag) error {
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
func (b *InMemoryBackend) RemoveTagsFromResource(ctx context.Context, resourceID string, tagKeys []string) error {
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

// ListTagsForResource returns tags for a directory.
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	resourceID string,
	_ int32,
	_ string,
) ([]Tag, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	d, ok := b.state(region).directories[resourceID]
	if !ok {
		return nil, "", ErrDirectoryNotFound
	}

	tags := make([]Tag, 0, len(d.Tags))
	for k, v := range d.Tags {
		tags = append(tags, Tag{Key: k, Value: v})
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i].Key < tags[j].Key })

	return tags, "", nil
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
			Directories: st.directories,
			Snapshots:   st.snapshots,
			Aliases:     st.aliases,
		}
	}

	data, _ := json.Marshal(backendSnapshot{Regions: regions})

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

	b.states = make(map[string]*regionState)
	for region, rs := range snap.Regions {
		st := newRegionState()
		if rs.Directories != nil {
			st.directories = rs.Directories
		}
		if rs.Snapshots != nil {
			st.snapshots = rs.Snapshots
		}
		if rs.Aliases != nil {
			st.aliases = rs.Aliases
		}
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
