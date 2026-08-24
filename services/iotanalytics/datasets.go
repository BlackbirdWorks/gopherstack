package iotanalytics

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"time"

	"github.com/google/uuid"
)

// cloneDatasetActions deep-copies a slice of DatasetAction.
func cloneDatasetActions(actions []DatasetAction) []DatasetAction {
	if actions == nil {
		return nil
	}

	cp := make([]DatasetAction, len(actions))
	copy(cp, actions)

	return cp
}

// cloneDatasetTriggers deep-copies a slice of DatasetTrigger.
func cloneDatasetTriggers(triggers []DatasetTrigger) []DatasetTrigger {
	if triggers == nil {
		return nil
	}

	cp := make([]DatasetTrigger, len(triggers))
	copy(cp, triggers)

	return cp
}

// cloneContentDeliveryRules deep-copies a slice of ContentDeliveryRule.
func cloneContentDeliveryRules(rules []ContentDeliveryRule) []ContentDeliveryRule {
	if rules == nil {
		return nil
	}

	cp := make([]ContentDeliveryRule, len(rules))
	copy(cp, rules)

	return cp
}

// cloneLateDataRules deep-copies a slice of LateDataRule.
func cloneLateDataRules(rules []LateDataRule) []LateDataRule {
	if rules == nil {
		return nil
	}

	cp := make([]LateDataRule, len(rules))
	copy(cp, rules)

	return cp
}

// cloneVersioningConfiguration deep-copies a VersioningConfiguration pointer.
func cloneVersioningConfiguration(v *VersioningConfiguration) *VersioningConfiguration {
	if v == nil {
		return nil
	}

	cp := *v

	return &cp
}

// cloneDataset returns a deep copy of d.
func cloneDataset(d *Dataset) *Dataset {
	cp := *d
	cp.Tags = make(map[string]string, len(d.Tags))
	maps.Copy(cp.Tags, d.Tags)
	cp.Actions = cloneDatasetActions(d.Actions)
	cp.Triggers = cloneDatasetTriggers(d.Triggers)
	cp.ContentDeliveryRules = cloneContentDeliveryRules(d.ContentDeliveryRules)
	cp.LateDataRules = cloneLateDataRules(d.LateDataRules)
	cp.VersioningConfiguration = cloneVersioningConfiguration(d.VersioningConfiguration)
	cp.RetentionPeriod = cloneRetentionPeriod(d.RetentionPeriod)

	return &cp
}

// CreateDataset creates a new IoT Analytics dataset.
func (b *InMemoryBackend) CreateDataset(
	ctx context.Context,
	name string,
	tags map[string]string,
	actions []DatasetAction,
	triggers []DatasetTrigger,
	contentDeliveryRules []ContentDeliveryRule,
	versioningConfig *VersioningConfiguration,
	lateDataRules []LateDataRule,
	retentionPeriod *RetentionPeriod,
) (*Dataset, error) {
	if err := validateResourceName(name); err != nil {
		return nil, err
	}

	if err := validateRetentionPeriod(retentionPeriod); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateDataset")
	defer b.mu.Unlock()

	if b.datasets.Has(name) {
		return nil, ErrAlreadyExists
	}

	now := epochSeconds(time.Now())
	arn := resourceARN(ctx, "dataset", name)
	d := &Dataset{
		Name:                    name,
		ARN:                     arn,
		Status:                  statusActive,
		CreationTime:            now,
		LastUpdate:              now,
		Tags:                    make(map[string]string),
		Actions:                 cloneDatasetActions(actions),
		Triggers:                cloneDatasetTriggers(triggers),
		ContentDeliveryRules:    cloneContentDeliveryRules(contentDeliveryRules),
		LateDataRules:           cloneLateDataRules(lateDataRules),
		VersioningConfiguration: cloneVersioningConfiguration(versioningConfig),
		RetentionPeriod:         cloneRetentionPeriod(retentionPeriod),
	}
	maps.Copy(d.Tags, tags)
	b.datasets.Put(d)
	b.tags[arn] = make(map[string]string)
	maps.Copy(b.tags[arn], tags)

	return cloneDataset(d), nil
}

// DescribeDataset returns dataset metadata.
func (b *InMemoryBackend) DescribeDataset(name string) (*Dataset, error) {
	b.mu.RLock("DescribeDataset")
	defer b.mu.RUnlock()

	d, ok := b.datasets.Get(name)
	if !ok {
		return nil, ErrDatasetNotFound
	}

	return cloneDataset(d), nil
}

// UpdateDataset updates a dataset's actions, triggers, and configuration.
func (b *InMemoryBackend) UpdateDataset(
	name string,
	actions []DatasetAction,
	triggers []DatasetTrigger,
	contentDeliveryRules []ContentDeliveryRule,
	versioningConfig *VersioningConfiguration,
	lateDataRules []LateDataRule,
) error {
	b.mu.Lock("UpdateDataset")
	defer b.mu.Unlock()

	d, ok := b.datasets.Get(name)
	if !ok {
		return ErrDatasetNotFound
	}

	d.LastUpdate = epochSeconds(time.Now())

	if actions != nil {
		d.Actions = cloneDatasetActions(actions)
	}

	if triggers != nil {
		d.Triggers = cloneDatasetTriggers(triggers)
	}

	if contentDeliveryRules != nil {
		d.ContentDeliveryRules = cloneContentDeliveryRules(contentDeliveryRules)
	}

	if lateDataRules != nil {
		d.LateDataRules = cloneLateDataRules(lateDataRules)
	}

	if versioningConfig != nil {
		d.VersioningConfiguration = cloneVersioningConfiguration(versioningConfig)
	}

	return nil
}

// DeleteDataset deletes a dataset and its associated content versions.
func (b *InMemoryBackend) DeleteDataset(name string) error {
	b.mu.Lock("DeleteDataset")
	defer b.mu.Unlock()

	d, ok := b.datasets.Get(name)
	if !ok {
		return ErrDatasetNotFound
	}

	delete(b.tags, d.ARN)
	b.datasets.Delete(name)
	delete(b.datasetContents, name)

	return nil
}

// ListDatasets returns all datasets sorted by name.
func (b *InMemoryBackend) ListDatasets() []*Dataset {
	b.mu.RLock("ListDatasets")
	defer b.mu.RUnlock()

	items := b.datasets.Snapshot()
	result := make([]*Dataset, 0, len(items))

	for _, d := range items {
		result = append(result, cloneDataset(d))
	}

	return result
}

// AddDatasetInternal seeds a dataset by name (test helper).
func (b *InMemoryBackend) AddDatasetInternal(name string) *Dataset {
	d, _ := b.CreateDataset(b.svcCtx, name, nil, nil, nil, nil, nil, nil, nil)

	return d
}

// CreateDatasetContent creates a new content version for a dataset. If versionID is
// non-empty, it is used as the new version's VersionID instead of generating a random one
// (AWS docs: "The version ID of the dataset content. To specify versionId for a dataset
// content, the dataset must use a DeltaTimer filter" -- this backend accepts an explicit
// versionId unconditionally rather than requiring a DeltaTimer trigger, since enforcing that
// restriction would require simulating DeltaTimer-driven content generation this backend
// does not otherwise model). A duplicate explicit versionID is rejected with
// ErrAlreadyExists rather than silently overwriting an existing content version.
func (b *InMemoryBackend) CreateDatasetContent(datasetName, versionID string) (*DatasetContent, error) {
	b.mu.Lock("CreateDatasetContent")
	defer b.mu.Unlock()

	if !b.datasets.Has(datasetName) {
		return nil, ErrDatasetNotFound
	}

	contents := b.datasetContents[datasetName]

	if versionID != "" {
		for _, c := range contents {
			if c.VersionID == versionID {
				return nil, ErrAlreadyExists
			}
		}
	} else {
		versionID = uuid.NewString()
	}

	now := epochSeconds(time.Now())
	content := &DatasetContent{
		VersionID:      versionID,
		Status:         statusSucceeded,
		CreationTime:   now,
		CompletionTime: now,
		ScheduleTime:   now,
	}

	if len(contents) >= maxDatasetContents {
		contents = contents[1:]
	}

	b.datasetContents[datasetName] = append(contents, content)

	return content, nil
}

// latestSucceededContent returns the most recently created content version (contents is in
// creation order, oldest first) whose status is SUCCEEDED, or ErrDatasetContentNotFound if
// none match.
func latestSucceededContent(contents []*DatasetContent) (*DatasetContent, error) {
	for _, c := range slices.Backward(contents) {
		if c.Status == statusSucceeded {
			return c, nil
		}
	}

	return nil, ErrDatasetContentNotFound
}

// GetDatasetContent retrieves a specific, latest ($LATEST), or latest-succeeded
// ($LATEST_SUCCEEDED, also the default when versionID is empty) content version of a
// dataset, matching AWS GetDatasetContent versionId semantics.
func (b *InMemoryBackend) GetDatasetContent(datasetName, versionID string) (*DatasetContent, error) {
	b.mu.RLock("GetDatasetContent")
	defer b.mu.RUnlock()

	if !b.datasets.Has(datasetName) {
		return nil, ErrDatasetNotFound
	}

	contents := b.datasetContents[datasetName]
	if len(contents) == 0 {
		return nil, ErrDatasetContentNotFound
	}

	switch versionID {
	case "", latestSucceededVersion:
		return latestSucceededContent(contents)
	case latestVersion:
		return contents[len(contents)-1], nil
	}

	for _, c := range contents {
		if c.VersionID == versionID {
			return c, nil
		}
	}

	return nil, ErrDatasetContentNotFound
}

// ListDatasetContents returns all content versions for a dataset, sorted by creation time
// descending, ties broken by insertion order (most recently created first). CreationTime has
// only second-level resolution (epochSeconds), so content versions created within the same
// test or request burst routinely tie; a plain slices.SortFunc is explicitly documented as
// unstable and would then reorder tied entries arbitrarily between calls, which breaks the
// offset-based pagination in handleListDatasetContents (two calls for page 1 and page 2 could
// disagree on ordering with nothing mutated in between). Reversing to newest-inserted-first
// before a *stable* sort makes ties resolve deterministically in that same direction.
func (b *InMemoryBackend) ListDatasetContents(datasetName string) ([]*DatasetContent, error) {
	b.mu.RLock("ListDatasetContents")
	defer b.mu.RUnlock()

	if !b.datasets.Has(datasetName) {
		return nil, ErrDatasetNotFound
	}

	contents := b.datasetContents[datasetName]
	result := make([]*DatasetContent, len(contents))
	copy(result, contents)
	slices.Reverse(result)

	slices.SortStableFunc(result, func(a, b *DatasetContent) int {
		return cmp.Compare(b.CreationTime, a.CreationTime)
	})

	return result, nil
}

// DeleteDatasetContent deletes a single content version, matching AWS DeleteDatasetContent
// versionId semantics: a specific versionId, $LATEST (the most recently created version
// regardless of status), or $LATEST_SUCCEEDED (the default when versionID is empty --
// the most recently created SUCCEEDED version). Unlike an unqualified "delete all", AWS
// never removes more than one content version per call.
func (b *InMemoryBackend) DeleteDatasetContent(datasetName, versionID string) error {
	b.mu.Lock("DeleteDatasetContent")
	defer b.mu.Unlock()

	if !b.datasets.Has(datasetName) {
		return ErrDatasetNotFound
	}

	contents := b.datasetContents[datasetName]

	switch versionID {
	case "", latestSucceededVersion:
		target, err := latestSucceededContent(contents)
		if err != nil {
			return err
		}

		return b.deleteDatasetContentVersion(datasetName, target.VersionID)
	case latestVersion:
		if len(contents) == 0 {
			return ErrDatasetContentNotFound
		}

		return b.deleteDatasetContentVersion(datasetName, contents[len(contents)-1].VersionID)
	}

	return b.deleteDatasetContentVersion(datasetName, versionID)
}

// deleteDatasetContentVersion removes the content version with the given versionID from
// datasetName's content list. Returns ErrDatasetContentNotFound if no version matches.
func (b *InMemoryBackend) deleteDatasetContentVersion(datasetName, versionID string) error {
	contents := b.datasetContents[datasetName]

	for i, c := range contents {
		if c.VersionID == versionID {
			b.datasetContents[datasetName] = append(contents[:i], contents[i+1:]...)

			return nil
		}
	}

	return ErrDatasetContentNotFound
}
