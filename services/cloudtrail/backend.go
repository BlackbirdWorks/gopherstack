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
)

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
	trails    map[string]*Trail
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new in-memory CloudTrail backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		trails:    make(map[string]*Trail),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("cloudtrail"),
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
	trail := &Trail{
		Name:                       name,
		S3BucketName:               s3BucketName,
		S3KeyPrefix:                s3KeyPrefix,
		SnsTopicName:               snsTopicName,
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
		for _, trail := range b.trails {
			if trail.TrailARN == name {
				t = trail

				break
			}
		}
	}

	if t == nil {
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

	if _, ok := b.trails[nameOrARN]; ok {
		delete(b.trails, nameOrARN)

		return nil
	}
	for name, t := range b.trails {
		if t.TrailARN == nameOrARN {
			delete(b.trails, name)

			return nil
		}
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

	for _, t := range b.trails {
		if t.TrailARN == resourceID || t.Name == resourceID {
			t.Tags.Merge(kv)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
}

// RemoveTags removes tags from a trail resource by ARN.
func (b *InMemoryBackend) RemoveTags(resourceID string, keys []string) error {
	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	for _, t := range b.trails {
		if t.TrailARN == resourceID || t.Name == resourceID {
			t.Tags.DeleteKeys(keys)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
}

// ListTags returns tags for the given resource ARNs.
func (b *InMemoryBackend) ListTags(resourceIDs []string) map[string]map[string]string {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	result := make(map[string]map[string]string, len(resourceIDs))
	for _, rid := range resourceIDs {
		for _, t := range b.trails {
			if t.TrailARN == rid || t.Name == rid {
				result[rid] = t.Tags.Clone()

				break
			}
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
	for _, t := range b.trails {
		if t.TrailARN == nameOrARN {
			return t
		}
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
		if len(es.DataResources) > 0 {
			out[i].DataResources = make([]DataResource, len(es.DataResources))
			copy(out[i].DataResources, es.DataResources)
		}
	}

	return out
}
