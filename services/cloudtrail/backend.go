package cloudtrail

import (
	"fmt"
	"time"

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
)

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
	Tags                 *tags.Tags `json:"tags,omitempty"`
	CreatedTimestamp     time.Time  `json:"createdTimestamp"`
	UpdatedTimestamp     time.Time  `json:"updatedTimestamp"`
	EventDataStoreID     string     `json:"eventDataStoreId"`
	EventDataStoreARN    string     `json:"eventDataStoreArn"`
	Name                 string     `json:"name"`
	Status               string     `json:"status"`
	RetentionPeriod      int32      `json:"retentionPeriod"`
	MultiRegionEnabled   bool       `json:"multiRegionEnabled"`
	OrganizationEnabled  bool       `json:"organizationEnabled"`
	TerminationProtected bool       `json:"terminationProtectionEnabled"`
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
	CreationTime               time.Time       `json:"creationTime"`
	Tags                       *tags.Tags      `json:"tags,omitempty"`
	KMSKeyID                   string          `json:"kmsKeyId,omitempty"`
	TrailARN                   string          `json:"trailArn"`
	S3BucketName               string          `json:"s3BucketName"`
	S3KeyPrefix                string          `json:"s3KeyPrefix,omitempty"`
	SnsTopicName               string          `json:"snsTopicName,omitempty"`
	SnsTopicARN                string          `json:"snsTopicArn,omitempty"`
	CloudWatchLogsLogGroupARN  string          `json:"cloudWatchLogsLogGroupArn,omitempty"`
	CloudWatchLogsRoleARN      string          `json:"cloudWatchLogsRoleArn,omitempty"`
	Region                     string          `json:"region"`
	Name                       string          `json:"name"`
	HomeRegion                 string          `json:"homeRegion"`
	AccountID                  string          `json:"accountId"`
	EventSelectors             []EventSelector `json:"eventSelectors,omitempty"`
	IncludeGlobalServiceEvents bool            `json:"includeGlobalServiceEvents"`
	IsMultiRegionTrail         bool            `json:"isMultiRegionTrail"`
	LogFileValidationEnabled   bool            `json:"logFileValidationEnabled"`
	IsLogging                  bool            `json:"isLogging"`
	HasCustomEventSelectors    bool            `json:"hasCustomEventSelectors"`
}

// InMemoryBackend is the in-memory store for CloudTrail resources.
type InMemoryBackend struct {
	trails           map[string]*Trail
	trailsByARN      map[string]string
	channels         map[string]*Channel
	dashboards       map[string]*Dashboard
	eventDataStores  map[string]*EventDataStore
	queries          map[string]*Query
	resourcePolicies map[string]*ResourcePolicy
	mu               *lockmetrics.RWMutex
	accountID        string
	region           string
	channelCounter   int
	dashboardCounter int
	edsCounter       int
	queryCounter     int
}

// NewInMemoryBackend creates a new in-memory CloudTrail backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		trails:           make(map[string]*Trail),
		trailsByARN:      make(map[string]string),
		channels:         make(map[string]*Channel),
		dashboards:       make(map[string]*Dashboard),
		eventDataStores:  make(map[string]*EventDataStore),
		queries:          make(map[string]*Query),
		resourcePolicies: make(map[string]*ResourcePolicy),
		accountID:        accountID,
		region:           region,
		mu:               lockmetrics.New("cloudtrail"),
	}
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
	for _, t := range b.trails {
		if t.TrailARN == nameOrARN {
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

// StartLogging sets the isLogging flag for a trail to true.
func (b *InMemoryBackend) StartLogging(nameOrARN string) error {
	b.mu.Lock("StartLogging")
	defer b.mu.Unlock()

	t := b.findByNameOrARNLocked(nameOrARN)
	if t == nil {
		return fmt.Errorf("%w: trail %s not found", ErrNotFound, nameOrARN)
	}
	t.IsLogging = true

	return nil
}

// StopLogging sets the isLogging flag for a trail to false.
func (b *InMemoryBackend) StopLogging(nameOrARN string) error {
	b.mu.Lock("StopLogging")
	defer b.mu.Unlock()

	t := b.findByNameOrARNLocked(nameOrARN)
	if t == nil {
		return fmt.Errorf("%w: trail %s not found", ErrNotFound, nameOrARN)
	}
	t.IsLogging = false

	return nil
}

// GetTrailStatus returns the logging status of a trail.
func (b *InMemoryBackend) GetTrailStatus(nameOrARN string) (bool, error) {
	b.mu.RLock("GetTrailStatus")
	defer b.mu.RUnlock()

	t := b.findByNameOrARNLocked(nameOrARN)
	if t == nil {
		return false, fmt.Errorf("%w: trail %s not found", ErrNotFound, nameOrARN)
	}

	return t.IsLogging, nil
}

// PutEventSelectors sets event selectors for a trail.
func (b *InMemoryBackend) PutEventSelectors(nameOrARN string, selectors []EventSelector) (*Trail, error) {
	b.mu.Lock("PutEventSelectors")
	defer b.mu.Unlock()

	t := b.findByNameOrARNLocked(nameOrARN)
	if t == nil {
		return nil, fmt.Errorf("%w: trail %s not found", ErrNotFound, nameOrARN)
	}
	t.EventSelectors = selectors
	t.HasCustomEventSelectors = len(selectors) > 0
	cp := *t
	cp.EventSelectors = copyEventSelectors(t.EventSelectors)

	return &cp, nil
}

// GetEventSelectors returns event selectors for a trail.
func (b *InMemoryBackend) GetEventSelectors(nameOrARN string) (string, []EventSelector, error) {
	b.mu.RLock("GetEventSelectors")
	defer b.mu.RUnlock()

	t := b.findByNameOrARNLocked(nameOrARN)
	if t == nil {
		return "", nil, fmt.Errorf("%w: trail %s not found", ErrNotFound, nameOrARN)
	}

	return t.TrailARN, copyEventSelectors(t.EventSelectors), nil
}

// AddTags adds tags to a trail resource by ARN.
func (b *InMemoryBackend) AddTags(resourceID string, kv map[string]string) error {
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	t := b.findByNameOrARNLocked(resourceID)
	if t == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
	}
	t.Tags.Merge(kv)

	return nil
}

// RemoveTags removes tags from a trail resource by ARN.
func (b *InMemoryBackend) RemoveTags(resourceID string, keys []string) error {
	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	t := b.findByNameOrARNLocked(resourceID)
	if t == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
	}
	t.Tags.DeleteKeys(keys)

	return nil
}

// ListTags returns tags for the given resource ARNs.
func (b *InMemoryBackend) ListTags(resourceIDs []string) map[string]map[string]string {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	result := make(map[string]map[string]string, len(resourceIDs))
	for _, rid := range resourceIDs {
		t := b.findByNameOrARNLocked(rid)
		if t != nil {
			result[rid] = t.Tags.Clone()
		}
	}

	return result
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

	cp := *ch

	return &cp, nil
}

// DeleteChannel deletes a channel by ID or ARN.
func (b *InMemoryBackend) DeleteChannel(channelIDOrARN string) error {
	b.mu.Lock("DeleteChannel")
	defer b.mu.Unlock()

	for id, ch := range b.channels {
		if id == channelIDOrARN || ch.ChannelARN == channelIDOrARN {
			ch.Tags.Close()
			delete(b.channels, id)

			return nil
		}
	}

	return fmt.Errorf("%w: channel %s not found", ErrNotFound, channelIDOrARN)
}

// CreateDashboard creates a new CloudTrail dashboard.
func (b *InMemoryBackend) CreateDashboard(name, dashType string, kv map[string]string) (*Dashboard, error) {
	b.mu.Lock("CreateDashboard")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
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
		Status:       "CREATING",
		Tags:         t,
	}
	b.dashboards[id] = d

	cp := *d

	return &cp, nil
}

// DeleteDashboard deletes a dashboard by ID or ARN.
func (b *InMemoryBackend) DeleteDashboard(dashboardIDOrARN string) error {
	b.mu.Lock("DeleteDashboard")
	defer b.mu.Unlock()

	for id, d := range b.dashboards {
		if id == dashboardIDOrARN || d.DashboardARN == dashboardIDOrARN {
			d.Tags.Close()
			delete(b.dashboards, id)

			return nil
		}
	}

	return fmt.Errorf("%w: dashboard %s not found", ErrNotFound, dashboardIDOrARN)
}

// CreateEventDataStore creates a new CloudTrail event data store.
func (b *InMemoryBackend) CreateEventDataStore(
	name string,
	multiRegionEnabled, organizationEnabled, terminationProtected bool,
	retentionPeriod int32,
	kv map[string]string,
) (*EventDataStore, error) {
	b.mu.Lock("CreateEventDataStore")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	b.edsCounter++
	id := fmt.Sprintf("eds-%06d", b.edsCounter)
	edsARN := arn.Build("cloudtrail", b.region, b.accountID, "eventdatastore/"+id)
	t := tags.New("cloudtrail.eds." + id + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	now := time.Now().UTC()
	eds := &EventDataStore{
		EventDataStoreID:     id,
		EventDataStoreARN:    edsARN,
		Name:                 name,
		Status:               "CREATED",
		MultiRegionEnabled:   multiRegionEnabled,
		OrganizationEnabled:  organizationEnabled,
		TerminationProtected: terminationProtected,
		RetentionPeriod:      retentionPeriod,
		CreatedTimestamp:     now,
		UpdatedTimestamp:     now,
		Tags:                 t,
	}
	b.eventDataStores[id] = eds

	cp := *eds

	return &cp, nil
}

// DeleteEventDataStore deletes an event data store by ID or ARN.
func (b *InMemoryBackend) DeleteEventDataStore(edsIDOrARN string) error {
	b.mu.Lock("DeleteEventDataStore")
	defer b.mu.Unlock()

	for id, eds := range b.eventDataStores {
		if id == edsIDOrARN || eds.EventDataStoreARN == edsIDOrARN {
			eds.Tags.Close()
			delete(b.eventDataStores, id)

			return nil
		}
	}

	return fmt.Errorf("%w: event data store %s not found", ErrNotFound, edsIDOrARN)
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

	q, ok := b.queries[queryID]
	if !ok {
		return nil, fmt.Errorf("%w: query %s not found", ErrNotFound, queryID)
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

	q, ok := b.queries[queryID]
	if !ok {
		return nil, fmt.Errorf("%w: query %s not found", ErrNotFound, queryID)
	}
	cp := *q

	return &cp, nil
}
