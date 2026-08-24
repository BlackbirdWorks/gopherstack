package directoryservice

import (
	"context"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// RegisterEventTopic registers an event topic.
func (b *InMemoryBackend) RegisterEventTopic(ctx context.Context, directoryID, topicName string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("RegisterEventTopic")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFound
	}

	// RegisterEventTopic's own error set (deserializers.go) types no
	// already-exists exception, so a re-registration of the same topic must
	// succeed rather than error (inferred, not documented -- unlike
	// codecommit's delete ops, AWS's RegisterEventTopic doc comment says
	// nothing about repeat calls, but the same "no case to report it" logic
	// applies: re-registering just refreshes the subscription).
	b.eventTopicPut(&storedEventTopic{
		region:          region,
		DirectoryID:     directoryID,
		TopicName:       topicName,
		TopicARN:        arn.Build("sns", region, b.accountID, topicName),
		Status:          "Registered",
		CreatedDateTime: time.Now().UTC(),
	})

	return nil
}

// DeregisterEventTopic deregisters an event topic.
func (b *InMemoryBackend) DeregisterEventTopic(ctx context.Context, directoryID, topicName string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeregisterEventTopic")
	defer b.mu.Unlock()

	if _, ok := b.eventTopicGet(region, directoryID, topicName); !ok {
		return ErrDirectoryNotFound
	}

	b.eventTopicDelete(region, directoryID, topicName)

	return nil
}

// DescribeEventTopics returns event topics for a directory.
func (b *InMemoryBackend) DescribeEventTopics(
	ctx context.Context,
	directoryID string,
	topicNames []string,
) ([]EventTopic, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeEventTopics")
	defer b.mu.RUnlock()

	filterSet := make(map[string]bool, len(topicNames))
	for _, n := range topicNames {
		filterSet[n] = true
	}

	var result []EventTopic
	for _, topic := range b.eventTopicsInRegion(region) {
		if directoryID != "" && topic.DirectoryID != directoryID {
			continue
		}
		if len(filterSet) > 0 && !filterSet[topic.TopicName] {
			continue
		}
		result = append(result, EventTopic{
			DirectoryID:     topic.DirectoryID,
			TopicName:       topic.TopicName,
			TopicARN:        topic.TopicARN,
			Status:          topic.Status,
			CreatedDateTime: topic.CreatedDateTime,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].TopicName < result[j].TopicName })

	return result, nil
}
