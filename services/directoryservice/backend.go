package directoryservice

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

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

// storedDirectory holds a directory with all fields.
type storedDirectory struct {
	LaunchTime  time.Time         `json:"launchTime"`
	Tags        map[string]string `json:"tags"`
	DirectoryID string            `json:"directoryId"`
	Name        string            `json:"name"`
	ShortName   string            `json:"shortName"`
	Description string            `json:"description"`
	Alias       string            `json:"alias"`
	AccessURL   string            `json:"accessUrl"`
	DirType     string            `json:"type"`
	Stage       string            `json:"stage"`
	Size        string            `json:"size"`
	Edition     string            `json:"edition"`
	SsoEnabled  bool              `json:"ssoEnabled"`
}

func (d *storedDirectory) toDirectory() Directory {
	return Directory{
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

// backendSnapshot is the serializable backend state.
type backendSnapshot struct {
	Directories map[string]*storedDirectory `json:"directories"`
	Snapshots   map[string]*storedSnapshot  `json:"snapshots"`
	Aliases     map[string]string           `json:"aliases"` // alias → directoryID
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	domainControllers     map[string]*storedDomainController
	adAssessments         map[string]*storedADAssessment
	snapshots             map[string]*storedSnapshot
	aliases               map[string]string
	hybridADUpdates       map[string]*storedHybridADUpdate
	updateInfoEntries     map[string][]*storedUpdateInfo
	ipRoutes              map[string][]storedIpRoute
	regions               map[string]*storedRegion
	schemaExtensions      map[string]*storedSchemaExtension
	conditionalForwarders map[string]*storedConditionalForwarder
	logSubscriptions      map[string]*storedLogSubscription
	eventTopics           map[string]*storedEventTopic
	directories           map[string]*storedDirectory
	sharedDirectories     map[string]*storedSharedDirectory
	mu                    *lockmetrics.RWMutex
	certificates          map[string]*storedCertificate
	ldapsSettings         map[string]*storedLDAPSSetting
	clientAuthSettings    map[string]*storedClientAuthSetting
	radiusSettings        map[string]*storedRadiusSettings
	dirDataAccess         map[string]bool
	caEnrollment          map[string]bool
	trusts                map[string]*storedTrust
	dirSettings           map[string][]*storedDirectorySetting
	region                string
	accountID             string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:                    lockmetrics.New("directoryservice"),
		accountID:             accountID,
		region:                region,
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
	tags []Tag,
) *storedDirectory {
	id := b.newDirectoryID()
	alias := b.defaultAlias(id)

	return &storedDirectory{
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
}

// CreateDirectory creates a new Simple AD directory.
func (b *InMemoryBackend) CreateDirectory(
	name, shortName, description, _ string,
	size DirectorySize, tags []Tag,
) (*Directory, error) {
	b.mu.Lock("CreateDirectory")
	defer b.mu.Unlock()

	if name == "" {
		return nil, ErrInvalidParameter
	}

	d := b.newStoredDirectory(name, shortName, description, DirectoryTypeSimpleAD, size, "", tags)
	b.directories[d.DirectoryID] = d
	b.aliases[d.Alias] = d.DirectoryID

	cp := d.toDirectory()

	return &cp, nil
}

// CreateMicrosoftAD creates a new Managed Microsoft AD directory.
func (b *InMemoryBackend) CreateMicrosoftAD(
	name, shortName, description, _ string,
	edition DirectoryEdition, tags []Tag,
) (*Directory, error) {
	b.mu.Lock("CreateMicrosoftAD")
	defer b.mu.Unlock()

	if name == "" {
		return nil, ErrInvalidParameter
	}

	d := b.newStoredDirectory(name, shortName, description, DirectoryTypeMicrosoftAD, "", edition, tags)
	b.directories[d.DirectoryID] = d
	b.aliases[d.Alias] = d.DirectoryID

	cp := d.toDirectory()

	return &cp, nil
}

// DeleteDirectory deletes a directory.
func (b *InMemoryBackend) DeleteDirectory(directoryID string) error {
	b.mu.Lock("DeleteDirectory")
	defer b.mu.Unlock()

	d, ok := b.directories[directoryID]
	if !ok {
		return ErrDirectoryNotFound
	}

	delete(b.aliases, d.Alias)
	delete(b.directories, directoryID)

	// Delete associated snapshots.
	for id, snap := range b.snapshots {
		if snap.DirectoryID == directoryID {
			delete(b.snapshots, id)
		}
	}

	return nil
}

// DescribeDirectories returns directories, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeDirectories(
	directoryIDs []string,
	limit int32,
	nextToken string,
) ([]*Directory, string, error) {
	b.mu.RLock("DescribeDirectories")
	defer b.mu.RUnlock()

	var ids []string

	if len(directoryIDs) > 0 {
		for _, id := range directoryIDs {
			if _, ok := b.directories[id]; !ok {
				return nil, "", ErrDirectoryNotFound
			}
		}
		ids = append([]string(nil), directoryIDs...)
		sort.Strings(ids)
	} else {
		ids = make([]string, 0, len(b.directories))
		for id := range b.directories {
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
		d := b.directories[id]
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
func (b *InMemoryBackend) CreateAlias(directoryID, alias string) error {
	b.mu.Lock("CreateAlias")
	defer b.mu.Unlock()

	d, ok := b.directories[directoryID]
	if !ok {
		return ErrDirectoryNotFound
	}

	if _, taken := b.aliases[alias]; taken {
		return ErrAliasAlreadyExists
	}

	delete(b.aliases, d.Alias)
	d.Alias = alias
	d.AccessURL = b.defaultAccessURL(alias)
	b.aliases[alias] = directoryID

	return nil
}

// EnableSso enables single sign-on for a directory.
func (b *InMemoryBackend) EnableSso(directoryID string) error {
	b.mu.Lock("EnableSso")
	defer b.mu.Unlock()

	d, ok := b.directories[directoryID]
	if !ok {
		return ErrDirectoryNotFound
	}

	d.SsoEnabled = true

	return nil
}

// DisableSso disables single sign-on for a directory.
func (b *InMemoryBackend) DisableSso(directoryID string) error {
	b.mu.Lock("DisableSso")
	defer b.mu.Unlock()

	d, ok := b.directories[directoryID]
	if !ok {
		return ErrDirectoryNotFound
	}

	d.SsoEnabled = false

	return nil
}

// GetDirectoryLimits returns directory limits for the region.
func (b *InMemoryBackend) GetDirectoryLimits() *DirectoryLimits {
	b.mu.RLock("GetDirectoryLimits")
	defer b.mu.RUnlock()

	var simpleADCount, msADCount, connectedCount int32

	for _, d := range b.directories {
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
func (b *InMemoryBackend) CreateSnapshot(directoryID, name string) (*Snapshot, error) {
	b.mu.Lock("CreateSnapshot")
	defer b.mu.Unlock()

	if _, ok := b.directories[directoryID]; !ok {
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
	b.snapshots[id] = s

	cp := s.toSnapshot()

	return &cp, nil
}

// DeleteSnapshot deletes a snapshot.
func (b *InMemoryBackend) DeleteSnapshot(snapshotID string) error {
	b.mu.Lock("DeleteSnapshot")
	defer b.mu.Unlock()

	if _, ok := b.snapshots[snapshotID]; !ok {
		return ErrSnapshotNotFound
	}

	delete(b.snapshots, snapshotID)

	return nil
}

// DescribeSnapshots returns snapshots filtered by directory and/or snapshot IDs.
func (b *InMemoryBackend) DescribeSnapshots(
	directoryID string,
	snapshotIDs []string,
	limit int32,
	nextToken string,
) ([]*Snapshot, string, error) {
	b.mu.RLock("DescribeSnapshots")
	defer b.mu.RUnlock()

	// Build filter set for snapshot IDs.
	filterIDs := make(map[string]bool, len(snapshotIDs))
	for _, id := range snapshotIDs {
		filterIDs[id] = true
	}

	ids := make([]string, 0, len(b.snapshots))
	for id, snap := range b.snapshots {
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
		s := b.snapshots[id]
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
func (b *InMemoryBackend) GetSnapshotLimits(directoryID string) (*SnapshotLimits, error) {
	b.mu.RLock("GetSnapshotLimits")
	defer b.mu.RUnlock()

	if _, ok := b.directories[directoryID]; !ok {
		return nil, ErrDirectoryNotFound
	}

	var count int32
	for _, snap := range b.snapshots {
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
func (b *InMemoryBackend) RestoreFromSnapshot(snapshotID string) error {
	b.mu.RLock("RestoreFromSnapshot")
	defer b.mu.RUnlock()

	if _, ok := b.snapshots[snapshotID]; !ok {
		return ErrSnapshotNotFound
	}

	return nil
}

// AddTagsToResource adds or updates tags on a directory.
func (b *InMemoryBackend) AddTagsToResource(resourceID string, tags []Tag) error {
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()

	d, ok := b.directories[resourceID]
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
func (b *InMemoryBackend) RemoveTagsFromResource(resourceID string, tagKeys []string) error {
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()

	d, ok := b.directories[resourceID]
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
	resourceID string,
	_ int32,
	_ string,
) ([]Tag, string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	d, ok := b.directories[resourceID]
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

	b.directories = make(map[string]*storedDirectory)
	b.snapshots = make(map[string]*storedSnapshot)
	b.aliases = make(map[string]string)
	b.ipRoutes = make(map[string][]storedIpRoute)
	b.regions = make(map[string]*storedRegion)
	b.schemaExtensions = make(map[string]*storedSchemaExtension)
	b.conditionalForwarders = make(map[string]*storedConditionalForwarder)
	b.logSubscriptions = make(map[string]*storedLogSubscription)
	b.eventTopics = make(map[string]*storedEventTopic)
	b.domainControllers = make(map[string]*storedDomainController)
	b.trusts = make(map[string]*storedTrust)
	b.sharedDirectories = make(map[string]*storedSharedDirectory)
	b.certificates = make(map[string]*storedCertificate)
	b.ldapsSettings = make(map[string]*storedLDAPSSetting)
	b.clientAuthSettings = make(map[string]*storedClientAuthSetting)
	b.radiusSettings = make(map[string]*storedRadiusSettings)
	b.dirDataAccess = make(map[string]bool)
	b.caEnrollment = make(map[string]bool)
	b.adAssessments = make(map[string]*storedADAssessment)
	b.dirSettings = make(map[string][]*storedDirectorySetting)
	b.updateInfoEntries = make(map[string][]*storedUpdateInfo)
	b.hybridADUpdates = make(map[string]*storedHybridADUpdate)
}

// BackendSnapshot serializes the backend state to JSON.
func (b *InMemoryBackend) BackendSnapshot() []byte {
	b.mu.RLock("BackendSnapshot")
	defer b.mu.RUnlock()

	data, _ := json.Marshal(backendSnapshot{
		Directories: b.directories,
		Snapshots:   b.snapshots,
		Aliases:     b.aliases,
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

	if snap.Directories != nil {
		b.directories = snap.Directories
	} else {
		b.directories = make(map[string]*storedDirectory)
	}

	if snap.Snapshots != nil {
		b.snapshots = snap.Snapshots
	} else {
		b.snapshots = make(map[string]*storedSnapshot)
	}

	if snap.Aliases != nil {
		b.aliases = snap.Aliases
	} else {
		b.aliases = make(map[string]string)
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
