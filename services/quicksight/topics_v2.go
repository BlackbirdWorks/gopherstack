package quicksight

import (
	"maps"
	"time"
)

// ---- Topics V2 (Q topics) ----
//
// Design finding: CreateTopicV2/DescribeTopicV2/UpdateTopicV2/DeleteTopicV2/
// ListTopicsV2/SearchTopicsV2/DescribeTopicPermissionsV2/
// UpdateTopicPermissionsV2 operate on the SAME underlying topic resource as
// the V1 Topic operations in topics.go -- not a parallel, disconnected
// resource. Evidence, read directly from aws-sdk-go-v2/service/quicksight@v1.123.1:
//
//  1. CreateTopicInput.TopicId and CreateTopicV2Input.TopicId carry the
//     identical doc comment ("This ID is unique per Amazon Web Services
//     Region for each Amazon Web Services account"), with no mention of a
//     separate V2 ID namespace, and both Create ops return
//     ResourceExistsException (see deserializeOpErrorCreateTopic{,V2}).
//  2. types.TopicUserExperienceVersion -- a field that exists ONLY on the V1
//     TopicDetails/TopicSummary shape -- has exactly two values, LEGACY and
//     NEW_READER_EXPERIENCE. NEW_READER_EXPERIENCE names precisely the
//     reader experience that TopicV2Details' simplified schema
//     (DataSetRelations/TopicV2DataSetReference, no ConfigOptions/Filters/
//     CalculatedFields/NamedEntities) exists to serve -- i.e. this V1-side
//     enum is already the flag that models "this topic uses the V2 schema."
//  3. DescribeTopicPermissionsV2Output/UpdateTopicPermissionsV2Output use
//     the exact same wire shape (Permissions/RequestId/Status/TopicArn/
//     TopicId, all types.ResourcePermission) as V1's
//     DescribeTopicPermissionsOutput/UpdateTopicPermissionsOutput, with no
//     version discriminator anywhere -- permissions are a property of "the
//     topic," not of which schema last wrote it. Likewise Delete: both
//     DeleteTopicInput and DeleteTopicV2Input take only TopicId.
//
// So this backend stores both families in the SAME b.topics collection,
// keyed by the SAME topicKey(accountID, topicID): CreateTopic and
// CreateTopicV2 conflict on a shared TopicId (ResourceExistsException),
// DeleteTopic and DeleteTopicV2 delete the one record, and
// Describe/List/SearchTopics{,V2} and {Describe,Update}TopicPermissions{,V2}
// all read/write that same record -- see handler_topics_v2.go, which routes
// Describe/Delete/List/Search/permissions straight to the V1 backend methods
// in topics.go rather than duplicating them.
//
// Where the two wire schemas are NOT losslessly convertible -- TopicV2Details
// drops V1's ConfigOptions/Filters/CalculatedFields/NamedEntities/
// DataAggregation, and V1's DatasetMetadata has no TopicV2DataSetRelation
// equivalent -- each family's own fields are stored on separate storedTopic
// fields (DataSets vs DataSetsV2/DataSetRelations) instead of one clobbering
// the other on every Create/Update. There is no SDK evidence either schema is
// meant to silently erase the other's data on a cross-family write, and
// guessing that it does would be exactly the kind of unverified claim
// parity-principles.md warns against. A topic created by one family and read
// by the other therefore round-trips its SHARED fields (TopicId/Arn/Name/
// Description/Permissions) for real, while that family's own schema-specific
// fields are honestly empty rather than fabricated -- see PARITY.md's gap
// note for the cross-family DataSets projection this implies.
//
// CreateTopicV2 and UpdateTopicV2 need dedicated backend methods (unlike
// Describe/Delete/List/Search/permissions) because they accept a genuinely
// different parameter set than V1's Create/UpdateTopic: no
// UserExperienceVersion (CreateTopicV2 always sets NEW_READER_EXPERIENCE
// itself -- see topicUserExperienceVersionNewReaderExperience in topics.go),
// no Permissions (neither CreateTopicInput nor CreateTopicV2Input has a
// Permissions field in the real SDK -- permissions are set only via the
// dedicated Update*Permissions* ops for both families), and
// CustomInstructions/DataSetRelations that V1 doesn't have at all.
//
// UpdateTopicV2Input.Topic (TopicV2Details) is REQUIRED and its own Name
// member is REQUIRED -- unlike V1 UpdateTopicInput's per-field optional
// partial-patch convention (topicFieldsFromBody's "empty string means leave
// unchanged" rule), a real V2 client must always resend the full Topic
// document on every update. UpdateTopicV2 therefore does a full replace of
// Name/Description/DataSets/DataSetRelations (including clearing them to
// empty when the caller omits them), while CustomInstructions and
// PublishOption -- independent optional top-level members of
// UpdateTopicV2Input, not nested inside the required Topic document -- keep
// V1's leave-unchanged-if-absent convention.

// CreateTopicV2 creates a Q topic (TopicV2Details schema). It writes to the
// same b.topics collection as CreateTopic (V1) -- see this file's doc
// comment above for why that is correct, not a parallel store.
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

// UpdateTopicV2 replaces a Q topic's Name/Description/DataSets/
// DataSetRelations wholesale (UpdateTopicV2Input.Topic is a required,
// full-replace document -- see this file's doc comment above), and updates
// CustomInstructions/PublishOption only when the caller supplies them (both
// are independent optional top-level members, not part of the required Topic
// document, so they keep leave-unchanged-if-absent semantics).
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
