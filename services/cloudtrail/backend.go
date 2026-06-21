package cloudtrail

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	// ErrNotFound is returned when the requested resource does not exist.
	ErrNotFound = awserr.New("TrailNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("TrailAlreadyExistsException", awserr.ErrConflict)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)
	// ErrChannelNotFound is returned when a channel is not found.
	ErrChannelNotFound = awserr.New("ChannelNotFoundException", awserr.ErrNotFound)
	// ErrDashboardNotFound is returned when a dashboard is not found.
	ErrDashboardNotFound = awserr.New("DashboardNotFoundException", awserr.ErrNotFound)
	// ErrEventDataStoreNotFound is returned when an event data store is not found.
	ErrEventDataStoreNotFound = awserr.New("EventDataStoreNotFoundException", awserr.ErrNotFound)
	// ErrQueryNotFound is returned when a query is not found.
	ErrQueryNotFound = awserr.New("InactiveQueryException", awserr.ErrNotFound)
	// ErrTerminationProtected is returned when trying to delete a termination-protected resource.
	ErrTerminationProtected = awserr.New("EventDataStoreTerminationProtectedException", awserr.ErrConflict)
	// ErrInsightNotEnabled is returned when GetInsightSelectors is called on a trail with no
	// insight selectors configured. AWS returns InsightNotEnabledException in this case.
	ErrInsightNotEnabled = awserr.New("InsightNotEnabledException", awserr.ErrInvalidParameter)
)

// AdvancedFieldSelector represents a filter condition in an advanced event selector.
// Each field selector specifies a field name and one or more comparison operators.
type AdvancedFieldSelector struct {
	Field         string   `json:"Field"`
	Equals        []string `json:"Equals,omitempty"`
	StartsWith    []string `json:"StartsWith,omitempty"`
	EndsWith      []string `json:"EndsWith,omitempty"`
	NotEquals     []string `json:"NotEquals,omitempty"`
	NotStartsWith []string `json:"NotStartsWith,omitempty"`
	NotEndsWith   []string `json:"NotEndsWith,omitempty"`
}

// AdvancedEventSelector represents an advanced event selector that filters events
// based on field-level conditions. Mutually exclusive with basic EventSelectors.
type AdvancedEventSelector struct {
	Name           string                  `json:"Name,omitempty"`
	FieldSelectors []AdvancedFieldSelector `json:"FieldSelectors"`
}

// LookupAttribute represents a filter attribute for LookupEvents.
type LookupAttribute struct {
	AttributeKey   string `json:"AttributeKey"`
	AttributeValue string `json:"AttributeValue"`
}

// Event represents a recorded management or data event.
type Event struct {
	EventTime   time.Time       `json:"EventTime"`
	EventID     string          `json:"EventId"`
	EventName   string          `json:"EventName"`
	EventSource string          `json:"EventSource"`
	Username    string          `json:"Username,omitempty"`
	ReadOnly    string          `json:"ReadOnly,omitempty"`
	AccessKeyID string          `json:"AccessKeyId,omitempty"`
	Resources   []EventResource `json:"Resources,omitempty"`
}

// EventResource represents a resource associated with a CloudTrail event.
type EventResource struct {
	ResourceName string `json:"ResourceName,omitempty"`
	ResourceType string `json:"ResourceType,omitempty"`
}

// Channel represents a CloudTrail channel resource.
type Channel struct {
	Tags         *tags.Tags    `json:"tags,omitempty"`
	ChannelID    string        `json:"channelId"`
	ChannelARN   string        `json:"channelArn"`
	Name         string        `json:"name"`
	Source       string        `json:"source"`
	Destinations []Destination `json:"destinations,omitempty"`
}

// Destination represents a channel destination.
type Destination struct {
	Type     string `json:"Type"`
	Location string `json:"Location"`
}

// Dashboard represents a CloudTrail dashboard resource.
type Dashboard struct {
	Tags         *tags.Tags `json:"tags,omitempty"`
	DashboardID  string     `json:"dashboardId"`
	DashboardARN string     `json:"dashboardArn"`
	Name         string     `json:"name"`
	Type         string     `json:"type"`
	Status       string     `json:"status"`
}

// EventDataStore represents a CloudTrail event data store resource.
type EventDataStore struct {
	Tags                   *tags.Tags              `json:"tags,omitempty"`
	CreatedTimestamp       time.Time               `json:"createdTimestamp"`
	UpdatedTimestamp       time.Time               `json:"updatedTimestamp"`
	EventDataStoreID       string                  `json:"eventDataStoreId"`
	EventDataStoreARN      string                  `json:"eventDataStoreArn"`
	Name                   string                  `json:"name"`
	Status                 string                  `json:"status"`
	FederationStatus       string                  `json:"federationStatus,omitempty"`
	FederationRoleArn      string                  `json:"federationRoleArn,omitempty"`
	BillingMode            string                  `json:"billingMode,omitempty"`
	KMSKeyID               string                  `json:"kmsKeyId,omitempty"`
	AdvancedEventSelectors []AdvancedEventSelector `json:"advancedEventSelectors,omitempty"`
	InsightSelectors       []InsightSelector       `json:"insightSelectors,omitempty"`
	RetentionPeriod        int32                   `json:"retentionPeriod"`
	MultiRegionEnabled     bool                    `json:"multiRegionEnabled"`
	OrganizationEnabled    bool                    `json:"organizationEnabled"`
	TerminationProtected   bool                    `json:"terminationProtectionEnabled"`
}

// Query represents a CloudTrail query resource.
type Query struct {
	CreationTime      time.Time `json:"creationTime"`
	QueryID           string    `json:"queryId"`
	EventDataStoreARN string    `json:"eventDataStoreArn"`
	QueryString       string    `json:"queryString"`
	QueryStatus       string    `json:"queryStatus"`
	DeliveryS3URI     string    `json:"deliveryS3Uri,omitempty"`
	ErrorMessage      string    `json:"errorMessage,omitempty"`
}

// ResourcePolicy represents a resource-based policy attached to a CloudTrail resource.
type ResourcePolicy struct {
	ResourceARN    string `json:"resourceArn"`
	ResourcePolicy string `json:"resourcePolicy"`
}

// DataResource represents a resource type for event selector data resources.
type DataResource struct {
	Type   string   `json:"Type"`
	Values []string `json:"Values"`
}

// EventSelector represents a CloudTrail event selector.
type EventSelector struct {
	ReadWriteType           string         `json:"ReadWriteType"`
	DataResources           []DataResource `json:"DataResources"`
	IncludeManagementEvents bool           `json:"IncludeManagementEvents"`
}

// Trail represents an AWS CloudTrail trail.
//
// The Tags field is backend-owned. Callers must treat the returned pointer as
// read-only; mutate tags only via AddTags / CreateTrail.
type Trail struct {
	CreationTime               time.Time               `json:"creationTime"`
	StartLoggingTime           *time.Time              `json:"startLoggingTime,omitempty"`
	StopLoggingTime            *time.Time              `json:"stopLoggingTime,omitempty"`
	LatestDeliveryTime         *time.Time              `json:"latestDeliveryTime,omitempty"`
	Tags                       *tags.Tags              `json:"tags,omitempty"`
	KMSKeyID                   string                  `json:"kmsKeyId,omitempty"`
	TrailARN                   string                  `json:"trailArn"`
	S3BucketName               string                  `json:"s3BucketName"`
	S3KeyPrefix                string                  `json:"s3KeyPrefix,omitempty"`
	SnsTopicName               string                  `json:"snsTopicName,omitempty"`
	SnsTopicARN                string                  `json:"snsTopicArn,omitempty"`
	CloudWatchLogsLogGroupARN  string                  `json:"cloudWatchLogsLogGroupArn,omitempty"`
	CloudWatchLogsRoleARN      string                  `json:"cloudWatchLogsRoleArn,omitempty"`
	Region                     string                  `json:"region"`
	Name                       string                  `json:"name"`
	HomeRegion                 string                  `json:"homeRegion"`
	AccountID                  string                  `json:"accountId"`
	EventSelectors             []EventSelector         `json:"eventSelectors,omitempty"`
	AdvancedEventSelectors     []AdvancedEventSelector `json:"advancedEventSelectors,omitempty"`
	InsightSelectors           []InsightSelector       `json:"insightSelectors,omitempty"`
	IncludeGlobalServiceEvents bool                    `json:"includeGlobalServiceEvents"`
	IsMultiRegionTrail         bool                    `json:"isMultiRegionTrail"`
	LogFileValidationEnabled   bool                    `json:"logFileValidationEnabled"`
	IsLogging                  bool                    `json:"isLogging"`
	HasCustomEventSelectors    bool                    `json:"hasCustomEventSelectors"`
	HasInsightSelectors        bool                    `json:"hasInsightSelectors"`
	IsOrganizationTrail        bool                    `json:"isOrganizationTrail"`
}

// Import represents a CloudTrail import resource.
type Import struct {
	CreatedTimestamp time.Time `json:"createdTimestamp"`
	UpdatedTimestamp time.Time `json:"updatedTimestamp"`
	ImportID         string    `json:"importId"`
	ImportSource     string    `json:"importSource,omitempty"`
	ImportStatus     string    `json:"importStatus"`
	Destinations     []string  `json:"destinations,omitempty"`
}

// InsightSelector represents a CloudTrail insight selector.
type InsightSelector struct {
	InsightType string `json:"InsightType"`
}

// InMemoryBackend is the in-memory store for CloudTrail resources.
type InMemoryBackend struct {
	edsByARN         map[string]string
	dashboardsByName map[string]string
	trailsByARN      map[string]string
	channels         map[string]*Channel
	channelsByARN    map[string]string
	channelsByName   map[string]string
	dashboards       map[string]*Dashboard
	queries          map[string]*Query
	edsByName        map[string]string
	eventDataStores  map[string]*EventDataStore
	trails           map[string]*Trail
	mu               *lockmetrics.RWMutex
	dashboardsByARN  map[string]string
	resourcePolicies map[string]*ResourcePolicy
	imports          map[string]*Import
	accountID        string
	region           string
	events           []Event
	channelCounter   int
	dashboardCounter int
	edsCounter       int
	queryCounter     int
	importCounter    int
}

// NewInMemoryBackend creates a new in-memory CloudTrail backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		trails:           make(map[string]*Trail),
		trailsByARN:      make(map[string]string),
		channels:         make(map[string]*Channel),
		channelsByARN:    make(map[string]string),
		channelsByName:   make(map[string]string),
		dashboards:       make(map[string]*Dashboard),
		dashboardsByARN:  make(map[string]string),
		dashboardsByName: make(map[string]string),
		eventDataStores:  make(map[string]*EventDataStore),
		edsByARN:         make(map[string]string),
		edsByName:        make(map[string]string),
		queries:          make(map[string]*Query),
		resourcePolicies: make(map[string]*ResourcePolicy),
		imports:          make(map[string]*Import),
		accountID:        accountID,
		region:           region,
		mu:               lockmetrics.New("cloudtrail"),
	}
}

// Reset clears all state in the backend.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, t := range b.trails {
		t.Tags.Close()
	}
	for _, ch := range b.channels {
		ch.Tags.Close()
	}
	for _, d := range b.dashboards {
		d.Tags.Close()
	}
	for _, eds := range b.eventDataStores {
		eds.Tags.Close()
	}

	b.trails = make(map[string]*Trail)
	b.trailsByARN = make(map[string]string)
	b.channels = make(map[string]*Channel)
	b.channelsByARN = make(map[string]string)
	b.channelsByName = make(map[string]string)
	b.dashboards = make(map[string]*Dashboard)
	b.dashboardsByARN = make(map[string]string)
	b.dashboardsByName = make(map[string]string)
	b.eventDataStores = make(map[string]*EventDataStore)
	b.edsByARN = make(map[string]string)
	b.edsByName = make(map[string]string)
	b.queries = make(map[string]*Query)
	b.resourcePolicies = make(map[string]*ResourcePolicy)
	b.imports = make(map[string]*Import)
	b.events = nil
	b.channelCounter = 0
	b.dashboardCounter = 0
	b.edsCounter = 0
	b.queryCounter = 0
	b.importCounter = 0
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateTrail creates a new CloudTrail trail.
func (b *InMemoryBackend) CreateTrail(
	name, s3BucketName, s3KeyPrefix, snsTopicName,
	cloudWatchLogsLogGroupARN, cloudWatchLogsRoleARN, kmsKeyID string,
	includeGlobalServiceEvents, isMultiRegionTrail, enableLogFileValidation bool,
	kv map[string]string,
) (*Trail, error) {
	b.mu.Lock("CreateTrail")
	defer b.mu.Unlock()

	if _, ok := b.trails[name]; ok {
		return nil, fmt.Errorf("%w: trail %s already exists", ErrAlreadyExists, name)
	}

	trailARN := arn.Build("cloudtrail", b.region, b.accountID, "trail/"+name)
	t := tags.New("cloudtrail.trail." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	var snsTopicARN string
	if snsTopicName != "" {
		snsTopicARN = arn.Build("sns", b.region, b.accountID, snsTopicName)
	}
	trail := &Trail{
		Name:                       name,
		S3BucketName:               s3BucketName,
		S3KeyPrefix:                s3KeyPrefix,
		SnsTopicName:               snsTopicName,
		SnsTopicARN:                snsTopicARN,
		CloudWatchLogsLogGroupARN:  cloudWatchLogsLogGroupARN,
		CloudWatchLogsRoleARN:      cloudWatchLogsRoleARN,
		KMSKeyID:                   kmsKeyID,
		TrailARN:                   trailARN,
		HomeRegion:                 b.region,
		AccountID:                  b.accountID,
		Region:                     b.region,
		IncludeGlobalServiceEvents: includeGlobalServiceEvents,
		IsMultiRegionTrail:         isMultiRegionTrail,
		LogFileValidationEnabled:   enableLogFileValidation,
		IsLogging:                  false,
		CreationTime:               time.Now().UTC(),
		Tags:                       t,
	}
	b.trails[name] = trail
	b.trailsByARN[trailARN] = name
	cp := *trail

	return &cp, nil
}

// GetTrail returns a trail by name or ARN.
func (b *InMemoryBackend) GetTrail(nameOrARN string) (*Trail, error) {
	b.mu.RLock("GetTrail")
	defer b.mu.RUnlock()

	return b.findTrailLocked(nameOrARN)
}

// findTrailLocked looks up a trail by name or ARN (must hold at least a read lock).
func (b *InMemoryBackend) findTrailLocked(nameOrARN string) (*Trail, error) {
	if t, ok := b.trails[nameOrARN]; ok {
		cp := *t
		cp.EventSelectors = copyEventSelectors(t.EventSelectors)

		return &cp, nil
	}
	if name, ok := b.trailsByARN[nameOrARN]; ok {
		if t, found := b.trails[name]; found {
			cp := *t
			cp.EventSelectors = copyEventSelectors(t.EventSelectors)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: trail %s not found", ErrNotFound, nameOrARN)
}

// DescribeTrails returns trails matching the given name list.
// If nameList is empty, all trails are returned.
func (b *InMemoryBackend) DescribeTrails(nameList []string) []*Trail {
	b.mu.RLock("DescribeTrails")
	defer b.mu.RUnlock()

	if len(nameList) == 0 {
		list := make([]*Trail, 0, len(b.trails))
		for _, t := range b.trails {
			cp := *t
			cp.EventSelectors = copyEventSelectors(t.EventSelectors)
			list = append(list, &cp)
		}
		sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

		return list
	}

	list := make([]*Trail, 0, len(nameList))
	for _, name := range nameList {
		t, err := b.findTrailLocked(name)
		if err == nil {
			list = append(list, t)
		}
	}

	return list
}

// UpdateTrail updates an existing trail's configuration.
func (b *InMemoryBackend) UpdateTrail(
	name, s3BucketName, s3KeyPrefix, snsTopicName,
	cloudWatchLogsLogGroupARN, cloudWatchLogsRoleARN, kmsKeyID string,
	includeGlobalServiceEvents, isMultiRegionTrail, enableLogFileValidation *bool,
) (*Trail, error) {
	b.mu.Lock("UpdateTrail")
	defer b.mu.Unlock()

	t, ok := b.trails[name]
	if !ok {
		if trailName, found := b.trailsByARN[name]; found {
			t = b.trails[trailName]
			ok = t != nil
		}
	}

	if !ok || t == nil {
		return nil, fmt.Errorf("%w: trail %s not found", ErrNotFound, name)
	}

	if s3BucketName != "" {
		t.S3BucketName = s3BucketName
	}
	if s3KeyPrefix != "" {
		t.S3KeyPrefix = s3KeyPrefix
	}
	if snsTopicName != "" {
		t.SnsTopicName = snsTopicName
		t.SnsTopicARN = arn.Build("sns", b.region, b.accountID, snsTopicName)
	}
	if cloudWatchLogsLogGroupARN != "" {
		t.CloudWatchLogsLogGroupARN = cloudWatchLogsLogGroupARN
	}
	if cloudWatchLogsRoleARN != "" {
		t.CloudWatchLogsRoleARN = cloudWatchLogsRoleARN
	}
	if kmsKeyID != "" {
		t.KMSKeyID = kmsKeyID
	}
	if includeGlobalServiceEvents != nil {
		t.IncludeGlobalServiceEvents = *includeGlobalServiceEvents
	}
	if isMultiRegionTrail != nil {
		t.IsMultiRegionTrail = *isMultiRegionTrail
	}
	if enableLogFileValidation != nil {
		t.LogFileValidationEnabled = *enableLogFileValidation
	}

	cp := *t
	cp.EventSelectors = copyEventSelectors(t.EventSelectors)

	return &cp, nil
}

// DeleteTrail deletes a trail by name or ARN.
func (b *InMemoryBackend) DeleteTrail(nameOrARN string) error {
	b.mu.Lock("DeleteTrail")
	defer b.mu.Unlock()

	if t, ok := b.trails[nameOrARN]; ok {
		t.Tags.Close()
		delete(b.trailsByARN, t.TrailARN)
		delete(b.trails, nameOrARN)

		return nil
	}
	if name, ok := b.trailsByARN[nameOrARN]; ok {
		if t, exists := b.trails[name]; exists {
			t.Tags.Close()
		}
		delete(b.trailsByARN, nameOrARN)
		delete(b.trails, name)

		return nil
	}

	return fmt.Errorf("%w: trail %s not found", ErrNotFound, nameOrARN)
}

// StartLogging sets the isLogging flag for a trail to true and records the start time.
func (b *InMemoryBackend) StartLogging(nameOrARN string) error {
	b.mu.Lock("StartLogging")
	defer b.mu.Unlock()

	t := b.findByNameOrARNLocked(nameOrARN)
	if t == nil {
		return fmt.Errorf("%w: trail %s not found", ErrNotFound, nameOrARN)
	}
	now := time.Now().UTC()
	t.IsLogging = true
	t.StartLoggingTime = &now
	t.LatestDeliveryTime = &now

	return nil
}

// StopLogging sets the isLogging flag for a trail to false and records the stop time.
func (b *InMemoryBackend) StopLogging(nameOrARN string) error {
	b.mu.Lock("StopLogging")
	defer b.mu.Unlock()

	t := b.findByNameOrARNLocked(nameOrARN)
	if t == nil {
		return fmt.Errorf("%w: trail %s not found", ErrNotFound, nameOrARN)
	}
	now := time.Now().UTC()
	t.IsLogging = false
	t.StopLoggingTime = &now

	return nil
}

// GetTrailStatus returns the full logging status of a trail.
func (b *InMemoryBackend) GetTrailStatus(nameOrARN string) (*Trail, error) {
	b.mu.RLock("GetTrailStatus")
	defer b.mu.RUnlock()

	t := b.findByNameOrARNLocked(nameOrARN)
	if t == nil {
		return nil, fmt.Errorf("%w: trail %s not found", ErrNotFound, nameOrARN)
	}
	cp := *t

	return &cp, nil
}

// PutEventSelectors sets event selectors for a trail. Basic and advanced selectors
// are mutually exclusive: providing AdvancedEventSelectors clears EventSelectors and vice versa.
func (b *InMemoryBackend) PutEventSelectors(
	nameOrARN string,
	selectors []EventSelector,
	advancedSelectors []AdvancedEventSelector,
) (*Trail, error) {
	b.mu.Lock("PutEventSelectors")
	defer b.mu.Unlock()

	t := b.findByNameOrARNLocked(nameOrARN)
	if t == nil {
		return nil, fmt.Errorf("%w: trail %s not found", ErrNotFound, nameOrARN)
	}
	if len(advancedSelectors) > 0 {
		// Advanced selectors replace basic selectors.
		t.AdvancedEventSelectors = copyAdvancedEventSelectors(advancedSelectors)
		t.EventSelectors = nil
		t.HasCustomEventSelectors = true
	} else {
		// Basic selectors replace advanced selectors.
		t.EventSelectors = selectors
		t.AdvancedEventSelectors = nil
		t.HasCustomEventSelectors = len(selectors) > 0
	}
	cp := *t
	cp.EventSelectors = copyEventSelectors(t.EventSelectors)
	cp.AdvancedEventSelectors = copyAdvancedEventSelectors(t.AdvancedEventSelectors)

	return &cp, nil
}

// GetEventSelectors returns both basic and advanced event selectors for a trail.
func (b *InMemoryBackend) GetEventSelectors(
	nameOrARN string,
) (string, []EventSelector, []AdvancedEventSelector, error) {
	b.mu.RLock("GetEventSelectors")
	defer b.mu.RUnlock()

	t := b.findByNameOrARNLocked(nameOrARN)
	if t == nil {
		return "", nil, nil, fmt.Errorf("%w: trail %s not found", ErrNotFound, nameOrARN)
	}

	return t.TrailARN, copyEventSelectors(t.EventSelectors), copyAdvancedEventSelectors(t.AdvancedEventSelectors), nil
}

// AddTags adds tags to a resource by ARN or ID.
func (b *InMemoryBackend) AddTags(resourceID string, kv map[string]string) error {
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	t, err := b.findResourceTagsLocked(resourceID)
	if err != nil {
		return err
	}
	t.Merge(kv)

	return nil
}

// RemoveTags removes tags from a resource by ARN or ID.
func (b *InMemoryBackend) RemoveTags(resourceID string, keys []string) error {
	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	t, err := b.findResourceTagsLocked(resourceID)
	if err != nil {
		return err
	}
	t.DeleteKeys(keys)

	return nil
}

// ListTags returns tags for the given resource ARNs or IDs.
func (b *InMemoryBackend) ListTags(resourceIDs []string) map[string]map[string]string {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	result := make(map[string]map[string]string, len(resourceIDs))
	for _, rid := range resourceIDs {
		t, err := b.findResourceTagsLocked(rid)
		if err == nil {
			result[rid] = t.Clone()
		}
	}

	return result
}

// findResourceTagsLocked returns the tags.Tags for any supported resource type.
// It must be called with at least a read lock held.
func (b *InMemoryBackend) findResourceTagsLocked(resourceID string) (*tags.Tags, error) {
	if t := b.findByNameOrARNLocked(resourceID); t != nil {
		return t.Tags, nil
	}

	id := resourceID
	if mapped, ok := b.channelsByARN[resourceID]; ok {
		id = mapped
	}
	if ch, ok := b.channels[id]; ok {
		return ch.Tags, nil
	}

	id = resourceID
	if mapped, ok := b.dashboardsByARN[resourceID]; ok {
		id = mapped
	}
	if d, ok := b.dashboards[id]; ok {
		return d.Tags, nil
	}

	id = resourceID
	if mapped, ok := b.edsByARN[resourceID]; ok {
		id = mapped
	}
	if eds, ok := b.eventDataStores[id]; ok {
		return eds.Tags, nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
}

// ListTrails returns all trails.
func (b *InMemoryBackend) ListTrails() []*Trail {
	b.mu.RLock("ListTrails")
	defer b.mu.RUnlock()

	list := make([]*Trail, 0, len(b.trails))
	for _, t := range b.trails {
		cp := *t
		cp.EventSelectors = copyEventSelectors(t.EventSelectors)
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].TrailARN < list[j].TrailARN })

	return list
}

// findByNameOrARNLocked looks up a trail by name or ARN without locking.
func (b *InMemoryBackend) findByNameOrARNLocked(nameOrARN string) *Trail {
	if t, ok := b.trails[nameOrARN]; ok {
		return t
	}
	if name, ok := b.trailsByARN[nameOrARN]; ok {
		return b.trails[name]
	}

	return nil
}

func copyEventSelectors(in []EventSelector) []EventSelector {
	if len(in) == 0 {
		return nil
	}
	out := make([]EventSelector, len(in))
	copy(out, in)
	for i, es := range in {
		if es.DataResources != nil {
			out[i].DataResources = make([]DataResource, len(es.DataResources))
			copy(out[i].DataResources, es.DataResources)
		}
	}

	return out
}

func copyAdvancedEventSelectors(in []AdvancedEventSelector) []AdvancedEventSelector {
	if len(in) == 0 {
		return nil
	}
	out := make([]AdvancedEventSelector, len(in))
	for i, aes := range in {
		out[i].Name = aes.Name
		if aes.FieldSelectors != nil {
			out[i].FieldSelectors = make([]AdvancedFieldSelector, len(aes.FieldSelectors))
			for j, fs := range aes.FieldSelectors {
				out[i].FieldSelectors[j] = AdvancedFieldSelector{
					Field:         fs.Field,
					Equals:        copyStringSlice(fs.Equals),
					StartsWith:    copyStringSlice(fs.StartsWith),
					EndsWith:      copyStringSlice(fs.EndsWith),
					NotEquals:     copyStringSlice(fs.NotEquals),
					NotStartsWith: copyStringSlice(fs.NotStartsWith),
					NotEndsWith:   copyStringSlice(fs.NotEndsWith),
				}
			}
		}
	}

	return out
}

func copyStringSlice(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)

	return out
}

// CreateChannel creates a new CloudTrail channel.
func (b *InMemoryBackend) CreateChannel(
	name, source string,
	destinations []Destination,
	kv map[string]string,
) (*Channel, error) {
	b.mu.Lock("CreateChannel")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}
	if _, exists := b.channelsByName[name]; exists {
		return nil, fmt.Errorf("%w: channel %s already exists", ErrAlreadyExists, name)
	}

	b.channelCounter++
	id := fmt.Sprintf("channel-%06d", b.channelCounter)
	channelARN := arn.Build("cloudtrail", b.region, b.accountID, "channel/"+id)
	t := tags.New("cloudtrail.channel." + id + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	ch := &Channel{
		ChannelID:    id,
		ChannelARN:   channelARN,
		Name:         name,
		Source:       source,
		Destinations: destinations,
		Tags:         t,
	}
	b.channels[id] = ch
	b.channelsByARN[channelARN] = id
	b.channelsByName[name] = id

	cp := *ch

	return &cp, nil
}

// DeleteChannel deletes a channel by ID or ARN.
func (b *InMemoryBackend) DeleteChannel(channelIDOrARN string) error {
	b.mu.Lock("DeleteChannel")
	defer b.mu.Unlock()

	id := channelIDOrARN
	if mapped, ok := b.channelsByARN[channelIDOrARN]; ok {
		id = mapped
	}
	ch, ok := b.channels[id]
	if !ok {
		return fmt.Errorf("%w: channel %s not found", ErrChannelNotFound, channelIDOrARN)
	}
	delete(b.channelsByARN, ch.ChannelARN)
	delete(b.channelsByName, ch.Name)
	ch.Tags.Close()
	delete(b.channels, id)

	return nil
}

// CreateDashboard creates a new CloudTrail dashboard.
func (b *InMemoryBackend) CreateDashboard(name, dashType string, kv map[string]string) (*Dashboard, error) {
	b.mu.Lock("CreateDashboard")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}
	if _, exists := b.dashboardsByName[name]; exists {
		return nil, fmt.Errorf("%w: dashboard %s already exists", ErrAlreadyExists, name)
	}

	b.dashboardCounter++
	id := fmt.Sprintf("dashboard-%06d", b.dashboardCounter)
	dashARN := arn.Build("cloudtrail", b.region, b.accountID, "dashboard/"+id)
	t := tags.New("cloudtrail.dashboard." + id + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	d := &Dashboard{
		DashboardID:  id,
		DashboardARN: dashARN,
		Name:         name,
		Type:         dashType,
		Status:       "CREATED",
		Tags:         t,
	}
	b.dashboards[id] = d
	b.dashboardsByARN[dashARN] = id
	b.dashboardsByName[name] = id

	cp := *d

	return &cp, nil
}

// DeleteDashboard deletes a dashboard by ID or ARN.
func (b *InMemoryBackend) DeleteDashboard(dashboardIDOrARN string) error {
	b.mu.Lock("DeleteDashboard")
	defer b.mu.Unlock()

	id := dashboardIDOrARN
	if mapped, ok := b.dashboardsByARN[dashboardIDOrARN]; ok {
		id = mapped
	}
	d, ok := b.dashboards[id]
	if !ok {
		return fmt.Errorf("%w: dashboard %s not found", ErrDashboardNotFound, dashboardIDOrARN)
	}
	delete(b.dashboardsByARN, d.DashboardARN)
	delete(b.dashboardsByName, d.Name)
	d.Tags.Close()
	delete(b.dashboards, id)

	return nil
}

// CreateEventDataStore creates a new CloudTrail event data store.
func (b *InMemoryBackend) CreateEventDataStore(
	name string,
	multiRegionEnabled, organizationEnabled, terminationProtected bool,
	retentionPeriod int32,
	advancedEventSelectors []AdvancedEventSelector,
	billingMode, kmsKeyID string,
	kv map[string]string,
) (*EventDataStore, error) {
	b.mu.Lock("CreateEventDataStore")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}
	if _, exists := b.edsByName[name]; exists {
		return nil, fmt.Errorf("%w: event data store %s already exists", ErrAlreadyExists, name)
	}

	b.edsCounter++
	id := fmt.Sprintf("eds-%06d", b.edsCounter)
	edsARN := arn.Build("cloudtrail", b.region, b.accountID, "eventdatastore/"+id)
	t := tags.New("cloudtrail.eds." + id + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	if billingMode == "" {
		billingMode = "EXTENDABLE_RETENTION_PRICING"
	}
	now := time.Now().UTC()
	eds := &EventDataStore{
		EventDataStoreID:       id,
		EventDataStoreARN:      edsARN,
		Name:                   name,
		Status:                 statusEnabled,
		MultiRegionEnabled:     multiRegionEnabled,
		OrganizationEnabled:    organizationEnabled,
		TerminationProtected:   terminationProtected,
		RetentionPeriod:        retentionPeriod,
		AdvancedEventSelectors: copyAdvancedEventSelectors(advancedEventSelectors),
		BillingMode:            billingMode,
		KMSKeyID:               kmsKeyID,
		FederationStatus:       "DISABLED",
		CreatedTimestamp:       now,
		UpdatedTimestamp:       now,
		Tags:                   t,
	}
	b.eventDataStores[id] = eds
	b.edsByARN[edsARN] = id
	b.edsByName[name] = id

	cp := *eds
	cp.AdvancedEventSelectors = copyAdvancedEventSelectors(eds.AdvancedEventSelectors)

	return &cp, nil
}

// DeleteEventDataStore deletes an event data store by ID or ARN.
// Returns ErrTerminationProtected if termination protection is enabled.
func (b *InMemoryBackend) DeleteEventDataStore(edsIDOrARN string) error {
	b.mu.Lock("DeleteEventDataStore")
	defer b.mu.Unlock()

	id := edsIDOrARN
	if mapped, ok := b.edsByARN[edsIDOrARN]; ok {
		id = mapped
	}
	eds, ok := b.eventDataStores[id]
	if !ok {
		return fmt.Errorf("%w: event data store %s not found", ErrEventDataStoreNotFound, edsIDOrARN)
	}
	if eds.TerminationProtected {
		return fmt.Errorf(
			"%w: event data store %s has termination protection enabled",
			ErrTerminationProtected,
			edsIDOrARN,
		)
	}
	delete(b.edsByARN, eds.EventDataStoreARN)
	delete(b.edsByName, eds.Name)
	eds.Tags.Close()
	delete(b.eventDataStores, id)

	return nil
}

// DeleteResourcePolicy removes the resource-based policy from a CloudTrail resource.
func (b *InMemoryBackend) DeleteResourcePolicy(resourceARN string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if _, ok := b.resourcePolicies[resourceARN]; !ok {
		return fmt.Errorf("%w: resource policy for %s not found", ErrNotFound, resourceARN)
	}
	delete(b.resourcePolicies, resourceARN)

	return nil
}

// DeregisterOrganizationDelegatedAdmin deregisters an organization delegated admin account.
// This is a no-op in the in-memory backend (returns success).
func (b *InMemoryBackend) DeregisterOrganizationDelegatedAdmin(delegatedAdminAccountID string) error {
	if delegatedAdminAccountID == "" {
		return fmt.Errorf("%w: DelegatedAdminAccountId is required", ErrValidation)
	}

	return nil
}

// StartQuery creates a new query against an event data store.
func (b *InMemoryBackend) StartQuery(queryString, edsARN, deliveryS3URI string) (*Query, error) {
	b.mu.Lock("StartQuery")
	defer b.mu.Unlock()

	if queryString == "" {
		return nil, fmt.Errorf("%w: QueryStatement is required", ErrValidation)
	}

	b.queryCounter++
	qid := fmt.Sprintf("query-%06d", b.queryCounter)
	q := &Query{
		QueryID:           qid,
		EventDataStoreARN: edsARN,
		QueryString:       queryString,
		QueryStatus:       "QUEUED",
		DeliveryS3URI:     deliveryS3URI,
		CreationTime:      time.Now().UTC(),
	}
	b.queries[qid] = q

	cp := *q

	return &cp, nil
}

// CancelQuery cancels a running query.
func (b *InMemoryBackend) CancelQuery(queryID string) (*Query, error) {
	b.mu.Lock("CancelQuery")
	defer b.mu.Unlock()

	if queryID == "" {
		return nil, fmt.Errorf("%w: QueryId is required", ErrValidation)
	}

	q, ok := b.queries[queryID]
	if !ok {
		return nil, fmt.Errorf("%w: query %s not found", ErrQueryNotFound, queryID)
	}
	if q.QueryStatus == "FINISHED" || q.QueryStatus == "FAILED" || q.QueryStatus == "CANCELLED" {
		return nil, fmt.Errorf("%w: query %s is already in terminal state %s", ErrValidation, queryID, q.QueryStatus)
	}
	q.QueryStatus = "CANCELLED"
	cp := *q

	return &cp, nil
}

// DescribeQuery returns details about a specific query.
func (b *InMemoryBackend) DescribeQuery(queryID string) (*Query, error) {
	b.mu.RLock("DescribeQuery")
	defer b.mu.RUnlock()

	if queryID == "" {
		return nil, fmt.Errorf("%w: QueryId is required", ErrValidation)
	}

	q, ok := b.queries[queryID]
	if !ok {
		return nil, fmt.Errorf("%w: query %s not found", ErrQueryNotFound, queryID)
	}
	cp := *q

	return &cp, nil
}

// GetEventDataStore returns an event data store by ID or ARN.
func (b *InMemoryBackend) GetEventDataStore(edsIDOrARN string) (*EventDataStore, error) {
	b.mu.RLock("GetEventDataStore")
	defer b.mu.RUnlock()

	id := edsIDOrARN
	if mapped, ok := b.edsByARN[edsIDOrARN]; ok {
		id = mapped
	}
	eds, ok := b.eventDataStores[id]
	if !ok {
		return nil, fmt.Errorf("%w: event data store %s not found", ErrEventDataStoreNotFound, edsIDOrARN)
	}
	cp := *eds

	return &cp, nil
}

// UpdateEventDataStore updates an existing event data store.
func (b *InMemoryBackend) UpdateEventDataStore(
	edsIDOrARN string,
	name string,
	multiRegionEnabled, organizationEnabled, terminationProtected *bool,
	retentionPeriod *int32,
	advancedEventSelectors []AdvancedEventSelector,
	billingMode, kmsKeyID string,
) (*EventDataStore, error) {
	b.mu.Lock("UpdateEventDataStore")
	defer b.mu.Unlock()

	id := edsIDOrARN
	if mapped, ok := b.edsByARN[edsIDOrARN]; ok {
		id = mapped
	}
	eds, ok := b.eventDataStores[id]
	if !ok {
		return nil, fmt.Errorf("%w: event data store %s not found", ErrEventDataStoreNotFound, edsIDOrARN)
	}
	if name != "" && name != eds.Name {
		delete(b.edsByName, eds.Name)
		eds.Name = name
		b.edsByName[name] = id
	}
	if multiRegionEnabled != nil {
		eds.MultiRegionEnabled = *multiRegionEnabled
	}
	if organizationEnabled != nil {
		eds.OrganizationEnabled = *organizationEnabled
	}
	if terminationProtected != nil {
		eds.TerminationProtected = *terminationProtected
	}
	if retentionPeriod != nil {
		eds.RetentionPeriod = *retentionPeriod
	}
	if advancedEventSelectors != nil {
		eds.AdvancedEventSelectors = copyAdvancedEventSelectors(advancedEventSelectors)
	}
	if billingMode != "" {
		eds.BillingMode = billingMode
	}
	if kmsKeyID != "" {
		eds.KMSKeyID = kmsKeyID
	}
	eds.UpdatedTimestamp = time.Now().UTC()
	cp := *eds
	cp.AdvancedEventSelectors = copyAdvancedEventSelectors(eds.AdvancedEventSelectors)

	return &cp, nil
}

// ListEventDataStores returns all event data stores.
func (b *InMemoryBackend) ListEventDataStores() []*EventDataStore {
	b.mu.RLock("ListEventDataStores")
	defer b.mu.RUnlock()

	list := make([]*EventDataStore, 0, len(b.eventDataStores))
	for _, eds := range b.eventDataStores {
		cp := *eds
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].EventDataStoreARN < list[j].EventDataStoreARN })

	return list
}

// RestoreEventDataStore restores a deleted event data store (sets status to ENABLED).
func (b *InMemoryBackend) RestoreEventDataStore(edsIDOrARN string) (*EventDataStore, error) {
	b.mu.Lock("RestoreEventDataStore")
	defer b.mu.Unlock()

	id := edsIDOrARN
	if mapped, ok := b.edsByARN[edsIDOrARN]; ok {
		id = mapped
	}
	eds, ok := b.eventDataStores[id]
	if !ok {
		return nil, fmt.Errorf("%w: event data store %s not found", ErrEventDataStoreNotFound, edsIDOrARN)
	}
	eds.Status = statusEnabled
	eds.UpdatedTimestamp = time.Now().UTC()
	cp := *eds

	return &cp, nil
}

// StartEventDataStoreIngestion starts ingestion for an event data store.
func (b *InMemoryBackend) StartEventDataStoreIngestion(edsIDOrARN string) error {
	b.mu.Lock("StartEventDataStoreIngestion")
	defer b.mu.Unlock()

	id := edsIDOrARN
	if mapped, ok := b.edsByARN[edsIDOrARN]; ok {
		id = mapped
	}
	eds, ok := b.eventDataStores[id]
	if !ok {
		return fmt.Errorf("%w: event data store %s not found", ErrEventDataStoreNotFound, edsIDOrARN)
	}
	eds.Status = statusEnabled
	eds.UpdatedTimestamp = time.Now().UTC()

	return nil
}

// StopEventDataStoreIngestion stops ingestion for an event data store.
func (b *InMemoryBackend) StopEventDataStoreIngestion(edsIDOrARN string) error {
	b.mu.Lock("StopEventDataStoreIngestion")
	defer b.mu.Unlock()

	id := edsIDOrARN
	if mapped, ok := b.edsByARN[edsIDOrARN]; ok {
		id = mapped
	}
	eds, ok := b.eventDataStores[id]
	if !ok {
		return fmt.Errorf("%w: event data store %s not found", ErrEventDataStoreNotFound, edsIDOrARN)
	}
	eds.Status = "STOPPED_INGESTION"
	eds.UpdatedTimestamp = time.Now().UTC()

	return nil
}

// GetChannel returns a channel by ID or ARN.
func (b *InMemoryBackend) GetChannel(channelIDOrARN string) (*Channel, error) {
	b.mu.RLock("GetChannel")
	defer b.mu.RUnlock()

	id := channelIDOrARN
	if mapped, ok := b.channelsByARN[channelIDOrARN]; ok {
		id = mapped
	}
	ch, ok := b.channels[id]
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrChannelNotFound, channelIDOrARN)
	}
	cp := *ch

	return &cp, nil
}

// UpdateChannel updates an existing channel.
func (b *InMemoryBackend) UpdateChannel(channelIDOrARN string, destinations []Destination) (*Channel, error) {
	b.mu.Lock("UpdateChannel")
	defer b.mu.Unlock()

	id := channelIDOrARN
	if mapped, ok := b.channelsByARN[channelIDOrARN]; ok {
		id = mapped
	}
	ch, ok := b.channels[id]
	if !ok {
		return nil, fmt.Errorf("%w: channel %s not found", ErrChannelNotFound, channelIDOrARN)
	}
	if destinations != nil {
		ch.Destinations = destinations
	}
	cp := *ch

	return &cp, nil
}

// ListChannels returns all channels.
func (b *InMemoryBackend) ListChannels() []*Channel {
	b.mu.RLock("ListChannels")
	defer b.mu.RUnlock()

	list := make([]*Channel, 0, len(b.channels))
	for _, ch := range b.channels {
		cp := *ch
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ChannelARN < list[j].ChannelARN })

	return list
}

// GetDashboard returns a dashboard by ID or ARN.
func (b *InMemoryBackend) GetDashboard(dashIDOrARN string) (*Dashboard, error) {
	b.mu.RLock("GetDashboard")
	defer b.mu.RUnlock()

	id := dashIDOrARN
	if mapped, ok := b.dashboardsByARN[dashIDOrARN]; ok {
		id = mapped
	}
	d, ok := b.dashboards[id]
	if !ok {
		return nil, fmt.Errorf("%w: dashboard %s not found", ErrDashboardNotFound, dashIDOrARN)
	}
	cp := *d

	return &cp, nil
}

// UpdateDashboard updates an existing dashboard.
func (b *InMemoryBackend) UpdateDashboard(dashIDOrARN string, name string) (*Dashboard, error) {
	b.mu.Lock("UpdateDashboard")
	defer b.mu.Unlock()

	id := dashIDOrARN
	if mapped, ok := b.dashboardsByARN[dashIDOrARN]; ok {
		id = mapped
	}
	d, ok := b.dashboards[id]
	if !ok {
		return nil, fmt.Errorf("%w: dashboard %s not found", ErrDashboardNotFound, dashIDOrARN)
	}
	if name != "" && name != d.Name {
		delete(b.dashboardsByName, d.Name)
		d.Name = name
		b.dashboardsByName[name] = id
	}
	cp := *d

	return &cp, nil
}

// ListDashboards returns all dashboards.
func (b *InMemoryBackend) ListDashboards() []*Dashboard {
	b.mu.RLock("ListDashboards")
	defer b.mu.RUnlock()

	list := make([]*Dashboard, 0, len(b.dashboards))
	for _, d := range b.dashboards {
		cp := *d
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].DashboardARN < list[j].DashboardARN })

	return list
}

// StartDashboardRefresh triggers a refresh of a dashboard (sets status to REFRESHING).
func (b *InMemoryBackend) StartDashboardRefresh(dashIDOrARN string) (*Dashboard, error) {
	b.mu.Lock("StartDashboardRefresh")
	defer b.mu.Unlock()

	id := dashIDOrARN
	if mapped, ok := b.dashboardsByARN[dashIDOrARN]; ok {
		id = mapped
	}
	d, ok := b.dashboards[id]
	if !ok {
		return nil, fmt.Errorf("%w: dashboard %s not found", ErrDashboardNotFound, dashIDOrARN)
	}
	d.Status = "REFRESHING"
	cp := *d

	return &cp, nil
}

// GetQueryResults returns results for a completed query (stub returns empty rows).
func (b *InMemoryBackend) GetQueryResults(queryID string) (*Query, error) {
	b.mu.RLock("GetQueryResults")
	defer b.mu.RUnlock()

	q, ok := b.queries[queryID]
	if !ok {
		return nil, fmt.Errorf("%w: query %s not found", ErrQueryNotFound, queryID)
	}
	cp := *q

	return &cp, nil
}

// ListQueries returns all queries.
func (b *InMemoryBackend) ListQueries() []*Query {
	b.mu.RLock("ListQueries")
	defer b.mu.RUnlock()

	list := make([]*Query, 0, len(b.queries))
	for _, q := range b.queries {
		cp := *q
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].QueryID < list[j].QueryID })

	return list
}

// StartImport creates an import job.
func (b *InMemoryBackend) StartImport(destinations []string, importSource string) (*Import, error) {
	b.mu.Lock("StartImport")
	defer b.mu.Unlock()

	b.importCounter++
	id := fmt.Sprintf("import-%06d", b.importCounter)
	now := time.Now().UTC()
	imp := &Import{
		ImportID:         id,
		Destinations:     destinations,
		ImportSource:     importSource,
		ImportStatus:     "INITIALIZING",
		CreatedTimestamp: now,
		UpdatedTimestamp: now,
	}
	b.imports[id] = imp
	cp := *imp

	return &cp, nil
}

// GetImport returns an import by ID.
func (b *InMemoryBackend) GetImport(importID string) (*Import, error) {
	b.mu.RLock("GetImport")
	defer b.mu.RUnlock()

	imp, ok := b.imports[importID]
	if !ok {
		return nil, fmt.Errorf("%w: import %s not found", ErrNotFound, importID)
	}
	cp := *imp

	return &cp, nil
}

// ListImports returns all imports.
func (b *InMemoryBackend) ListImports() []*Import {
	b.mu.RLock("ListImports")
	defer b.mu.RUnlock()

	list := make([]*Import, 0, len(b.imports))
	for _, imp := range b.imports {
		cp := *imp
		list = append(list, &cp)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ImportID < list[j].ImportID })

	return list
}

// StopImport stops an in-progress import.
func (b *InMemoryBackend) StopImport(importID string) (*Import, error) {
	b.mu.Lock("StopImport")
	defer b.mu.Unlock()

	imp, ok := b.imports[importID]
	if !ok {
		return nil, fmt.Errorf("%w: import %s not found", ErrNotFound, importID)
	}
	imp.ImportStatus = "STOPPED"
	imp.UpdatedTimestamp = time.Now().UTC()
	cp := *imp

	return &cp, nil
}

// PutInsightSelectors sets insight selectors for a trail, updating HasInsightSelectors.
func (b *InMemoryBackend) PutInsightSelectors(trailNameOrARN string, selectors []InsightSelector) (*Trail, error) {
	b.mu.Lock("PutInsightSelectors")
	defer b.mu.Unlock()

	t := b.findByNameOrARNLocked(trailNameOrARN)
	if t == nil {
		return nil, fmt.Errorf("%w: trail %s not found", ErrNotFound, trailNameOrARN)
	}
	t.InsightSelectors = make([]InsightSelector, len(selectors))
	copy(t.InsightSelectors, selectors)
	t.HasInsightSelectors = len(selectors) > 0

	cp := *t
	cp.InsightSelectors = make([]InsightSelector, len(t.InsightSelectors))
	copy(cp.InsightSelectors, t.InsightSelectors)
	cp.EventSelectors = copyEventSelectors(t.EventSelectors)
	cp.AdvancedEventSelectors = copyAdvancedEventSelectors(t.AdvancedEventSelectors)

	return &cp, nil
}

// GetInsightSelectors returns insight selectors for a trail.
// AWS returns InsightNotEnabledException when no insight selectors are configured.
func (b *InMemoryBackend) GetInsightSelectors(trailNameOrARN string) (string, []InsightSelector, error) {
	b.mu.RLock("GetInsightSelectors")
	defer b.mu.RUnlock()

	t := b.findByNameOrARNLocked(trailNameOrARN)
	if t == nil {
		return "", nil, fmt.Errorf("%w: trail %s not found", ErrNotFound, trailNameOrARN)
	}
	if len(t.InsightSelectors) == 0 {
		return "", nil, fmt.Errorf("%w: trail %s does not have Insights enabled", ErrInsightNotEnabled, trailNameOrARN)
	}
	cp := make([]InsightSelector, len(t.InsightSelectors))
	copy(cp, t.InsightSelectors)

	return t.TrailARN, cp, nil
}

// GetResourcePolicy returns the resource policy for the given ARN.
func (b *InMemoryBackend) GetResourcePolicy(resourceARN string) (*ResourcePolicy, error) {
	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	rp, ok := b.resourcePolicies[resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: resource policy for %s not found", ErrNotFound, resourceARN)
	}
	cp := *rp

	return &cp, nil
}

// PutResourcePolicy sets the resource policy for the given ARN.
func (b *InMemoryBackend) PutResourcePolicy(resourceARN, policy string) *ResourcePolicy {
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	rp := &ResourcePolicy{ResourceARN: resourceARN, ResourcePolicy: policy}
	b.resourcePolicies[resourceARN] = rp
	cp := *rp

	return &cp
}

// RegisterOrganizationDelegatedAdmin is a no-op that registers an org delegated admin.
func (b *InMemoryBackend) RegisterOrganizationDelegatedAdmin(accountID string) error {
	if accountID == "" {
		return fmt.Errorf("%w: MemberAccountId is required", ErrValidation)
	}

	return nil
}

// DisableFederation disables federation for an event data store.
func (b *InMemoryBackend) DisableFederation(edsIDOrARN string) (*EventDataStore, error) {
	b.mu.Lock("DisableFederation")
	defer b.mu.Unlock()

	id := edsIDOrARN
	if mapped, ok := b.edsByARN[edsIDOrARN]; ok {
		id = mapped
	}
	eds, ok := b.eventDataStores[id]
	if !ok {
		return nil, fmt.Errorf("%w: event data store %s not found", ErrEventDataStoreNotFound, edsIDOrARN)
	}
	eds.FederationStatus = "DISABLED"
	eds.FederationRoleArn = ""
	eds.UpdatedTimestamp = time.Now().UTC()
	cp := *eds

	return &cp, nil
}

// EnableFederation enables federation for an event data store, storing the role ARN.
func (b *InMemoryBackend) EnableFederation(edsIDOrARN, federationRoleArn string) (*EventDataStore, error) {
	b.mu.Lock("EnableFederation")
	defer b.mu.Unlock()

	id := edsIDOrARN
	if mapped, ok := b.edsByARN[edsIDOrARN]; ok {
		id = mapped
	}
	eds, ok := b.eventDataStores[id]
	if !ok {
		return nil, fmt.Errorf("%w: event data store %s not found", ErrEventDataStoreNotFound, edsIDOrARN)
	}
	eds.FederationStatus = "ENABLED"
	eds.FederationRoleArn = federationRoleArn
	eds.UpdatedTimestamp = time.Now().UTC()
	cp := *eds

	return &cp, nil
}

// GenerateQuery generates a query for an event data store (stub).
func (b *InMemoryBackend) GenerateQuery(_ []string, requestedQueryMaxResults int32) (*Query, error) {
	b.mu.Lock("GenerateQuery")
	defer b.mu.Unlock()

	b.queryCounter++
	qid := fmt.Sprintf("query-%06d", b.queryCounter)
	q := &Query{
		QueryID:      qid,
		QueryString:  "SELECT * FROM events LIMIT " + strconv.Itoa(int(requestedQueryMaxResults)),
		QueryStatus:  "QUEUED",
		CreationTime: time.Now().UTC(),
	}
	b.queries[qid] = q
	cp := *q

	return &cp, nil
}

// GetEventConfiguration returns event configuration (stub).
func (b *InMemoryBackend) GetEventConfiguration(resourceARN string) map[string]any {
	return map[string]any{
		keyResourceArn:       resourceARN,
		"EventConfiguration": []any{},
	}
}

// PutEventConfiguration sets event configuration (no-op stub).
func (b *InMemoryBackend) PutEventConfiguration(resourceARN string) error {
	if resourceARN == "" {
		return fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	return nil
}

// SearchSampleQueries returns empty sample queries (stub).
func (b *InMemoryBackend) SearchSampleQueries() []map[string]any {
	return []map[string]any{}
}

// ListPublicKeys returns empty public keys (stub).
func (b *InMemoryBackend) ListPublicKeys() []map[string]any {
	return []map[string]any{}
}

// ListInsightsData returns empty insights data (stub).
func (b *InMemoryBackend) ListInsightsData() []map[string]any {
	return []map[string]any{}
}

// ListInsightsMetricData returns empty insights metric data (stub).
func (b *InMemoryBackend) ListInsightsMetricData() []map[string]any {
	return []map[string]any{}
}

// ListImportFailures returns empty import failures (stub).
func (b *InMemoryBackend) ListImportFailures(_ string) []map[string]any {
	return []map[string]any{}
}

// LookupEventsInput holds parameters for a LookupEvents call.
type LookupEventsInput struct {
	StartTime        *time.Time
	EndTime          *time.Time
	NextToken        string
	LookupAttributes []LookupAttribute
	MaxResults       int32
}

// LookupEventsOutput holds the result of a LookupEvents call.
type LookupEventsOutput struct {
	NextToken string
	Events    []Event
}

// RecordEvent stores a management/data event so it can later be returned by
// LookupEvents. The event is assigned an EventID and EventTime if not already set.
func (b *InMemoryBackend) RecordEvent(ev Event) {
	b.mu.Lock("RecordEvent")
	defer b.mu.Unlock()

	if ev.EventID == "" {
		ev.EventID = uuid.NewString()
	}

	if ev.EventTime.IsZero() {
		ev.EventTime = time.Now().UTC()
	}

	b.events = append(b.events, ev)
}

// lookupAttrMatch reports whether an event matches a single lookup attribute.
func lookupAttrMatch(ev Event, attr LookupAttribute) bool {
	switch attr.AttributeKey {
	case "EventId":
		return ev.EventID == attr.AttributeValue
	case "EventName":
		return ev.EventName == attr.AttributeValue
	case "EventSource":
		return ev.EventSource == attr.AttributeValue
	case "Username":
		return ev.Username == attr.AttributeValue
	case "ReadOnly":
		return ev.ReadOnly == attr.AttributeValue
	case "AccessKeyId":
		return ev.AccessKeyID == attr.AttributeValue
	case "ResourceName":
		for _, r := range ev.Resources {
			if r.ResourceName == attr.AttributeValue {
				return true
			}
		}

		return false
	case "ResourceType":
		for _, r := range ev.Resources {
			if r.ResourceType == attr.AttributeValue {
				return true
			}
		}

		return false
	default:
		return false
	}
}

// eventMatchesFilters reports whether an event passes the time range and all
// lookup attributes (AWS ANDs multiple attributes together).
func eventMatchesFilters(ev Event, input LookupEventsInput) bool {
	if input.StartTime != nil && ev.EventTime.Before(*input.StartTime) {
		return false
	}

	if input.EndTime != nil && ev.EventTime.After(*input.EndTime) {
		return false
	}

	for _, attr := range input.LookupAttributes {
		if !lookupAttrMatch(ev, attr) {
			return false
		}
	}

	return true
}

// LookupEvents returns recorded events matching the given filters. Events are
// returned newest-first (matching AWS) and honor StartTime/EndTime, the lookup
// attributes (ANDed together), MaxResults, and NextToken pagination.
func (b *InMemoryBackend) LookupEvents(input LookupEventsInput) LookupEventsOutput {
	b.mu.RLock("LookupEvents")
	defer b.mu.RUnlock()

	matched := make([]Event, 0, len(b.events))

	for _, ev := range b.events {
		if eventMatchesFilters(ev, input) {
			matched = append(matched, ev)
		}
	}

	// Newest-first ordering, as the AWS API returns.
	sort.SliceStable(matched, func(i, j int) bool {
		return matched[i].EventTime.After(matched[j].EventTime)
	})

	// Decode the NextToken (offset into the matched slice).
	start := 0

	if input.NextToken != "" {
		if n, err := strconv.Atoi(input.NextToken); err == nil && n >= 0 && n <= len(matched) {
			start = n
		}
	}

	limit := int(input.MaxResults)
	if limit <= 0 || limit > 50 {
		limit = 50
	}

	end := start + limit

	var nextToken string

	if end < len(matched) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(matched)
	}

	page := make([]Event, 0, end-start)
	if start < len(matched) {
		page = append(page, matched[start:end]...)
	}

	return LookupEventsOutput{Events: page, NextToken: nextToken}
}

// PutEDSInsightSelectors sets insight selectors for an event data store.
func (b *InMemoryBackend) PutEDSInsightSelectors(
	edsIDOrARN string,
	selectors []InsightSelector,
) (*EventDataStore, error) {
	b.mu.Lock("PutEDSInsightSelectors")
	defer b.mu.Unlock()

	id := edsIDOrARN
	if mapped, ok := b.edsByARN[edsIDOrARN]; ok {
		id = mapped
	}
	eds, ok := b.eventDataStores[id]
	if !ok {
		return nil, fmt.Errorf("%w: event data store %s not found", ErrEventDataStoreNotFound, edsIDOrARN)
	}
	eds.InsightSelectors = make([]InsightSelector, len(selectors))
	copy(eds.InsightSelectors, selectors)
	eds.UpdatedTimestamp = time.Now().UTC()
	cp := *eds
	cp.AdvancedEventSelectors = copyAdvancedEventSelectors(eds.AdvancedEventSelectors)
	cp.InsightSelectors = make([]InsightSelector, len(eds.InsightSelectors))
	copy(cp.InsightSelectors, eds.InsightSelectors)

	return &cp, nil
}
