package quicksight

import (
	"maps"
	"time"
)

// The V2 Topic ops (CreateTopicV2/DescribeTopicV2/UpdateTopicV2/DeleteTopicV2/
// ListTopicsV2/SearchTopicsV2/{Describe,Update}TopicPermissionsV2) act on the
// SAME underlying topic as V1's ops in topics.go, not a parallel resource:
// both Create ops share a TopicId namespace and return ResourceExistsException,
// and Delete/permissions carry no version discriminator (verified against
// aws-sdk-go-v2/service/quicksight@v1.123.1). So both families share
// b.topics keyed by topicKey(accountID, topicID); handler_topics_v2.go routes
// Describe/Delete/List/Search/permissions straight to the V1 backend methods.
//
// The two wire schemas aren't losslessly convertible (V2 drops V1's
// ConfigOptions/Filters/CalculatedFields/NamedEntities/DataAggregation), so
// each family's own fields live on separate storedTopic fields (DataSets vs
// DataSetsV2/DataSetRelations) instead of one clobbering the other; a
// cross-family read honestly returns empty rather than fabricating a
// conversion (see PARITY.md).
//
// UpdateTopicV2Input.Topic is a required, full-replace document (unlike V1's
// per-field leave-unchanged-if-absent convention), so UpdateTopicV2 replaces
// Name/Description/DataSets/DataSetRelations wholesale while
// CustomInstructions/PublishOption (independent optional top-level members)
// keep V1's leave-unchanged-if-absent semantics.

// CreateTopicV2 creates a Q topic (TopicV2Details schema) in the shared
// b.topics store -- see the file doc comment above.
func (b *InMemoryBackend) CreateTopicV2(
	accountID, topicID, name, description, customInstructions string,
	dataSets []map[string]any,
	dataSetRelations []map[string]any,
	tags map[string]string,
) (*Topic, error) {
	if topicID == "" || name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateTopicV2")
	defer b.mu.Unlock()

	key := topicKey(accountID, topicID)
	if b.topics.Has(key) {
		return nil, ErrTopicAlreadyExists
	}

	now := time.Now().UTC()
	t := &storedTopic{
		CreatedTime:           now,
		LastUpdatedTime:       now,
		TopicID:               topicID,
		Arn:                   b.buildARN("topic", topicID),
		Name:                  name,
		Description:           description,
		UserExperienceVersion: topicUserExperienceVersionNewReaderExperience,
		CustomInstructions:    customInstructions,
		DataSetsV2:            dataSets,
		DataSetRelations:      dataSetRelations,
		RefreshSchedules:      make(map[string]*storedTopicRefreshSchedule),
		Refreshes:             make(map[string]*storedTopicRefresh),
		ReviewedAnswers:       make(map[string]*storedTopicReviewedAnswer),
	}
	b.topics.Put(t)

	if len(tags) > 0 {
		b.tags[t.Arn] = maps.Clone(tags)
	}

	return t.toTopic(), nil
}

// UpdateTopicV2 does a full replace of Name/Description/DataSets/
// DataSetRelations and updates CustomInstructions/PublishOption only when
// supplied -- see the file doc comment above.
func (b *InMemoryBackend) UpdateTopicV2(
	accountID, topicID, name, description, customInstructions, publishOption string,
	dataSets []map[string]any,
	dataSetRelations []map[string]any,
) (*Topic, error) {
	if name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("UpdateTopicV2")
	defer b.mu.Unlock()

	t, ok := b.topics.Get(topicKey(accountID, topicID))
	if !ok {
		return nil, ErrTopicNotFound
	}

	t.Name = name
	t.Description = description
	t.DataSetsV2 = dataSets
	t.DataSetRelations = dataSetRelations
	if customInstructions != "" {
		t.CustomInstructions = customInstructions
	}
	if publishOption != "" {
		t.PublishOption = publishOption
	}
	t.LastUpdatedTime = time.Now().UTC()

	return t.toTopic(), nil
}
