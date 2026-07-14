package sns

import (
	"fmt"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// GetTopicTags returns tags for the given topic ARN.
func (b *InMemoryBackend) GetTopicTags(arn string) map[string]string {
	b.mu.RLock("GetTopicTags")
	defer b.mu.RUnlock()
	if b.topicTags[arn] == nil {
		return map[string]string{}
	}

	return b.topicTags[arn].Clone()
}

// SetTopicTags stores tags for the given topic ARN.
func (b *InMemoryBackend) SetTopicTags(arn string, kv *svcTags.Tags) {
	b.mu.Lock("SetTopicTags")
	defer b.mu.Unlock()
	if kv == nil {
		return
	}
	if b.topicTags[arn] == nil {
		b.topicTags[arn] = svcTags.New("sns." + arn + ".tags")
	}
	b.topicTags[arn].Merge(kv.Clone())
}

// RemoveTopicTags removes specified tag keys for the given topic ARN.
func (b *InMemoryBackend) RemoveTopicTags(arn string, keys []string) {
	b.mu.Lock("RemoveTopicTags")
	defer b.mu.Unlock()
	if b.topicTags[arn] != nil {
		b.topicTags[arn].DeleteKeys(keys)
	}
}

// TaggedTopics returns a snapshot of all SNS topics with their tags.
// Intended for use by the Resource Groups Tagging API provider.
func (b *InMemoryBackend) TaggedTopics() []TaggedTopicInfo {
	b.mu.RLock("TaggedTopics")
	defer b.mu.RUnlock()

	result := make([]TaggedTopicInfo, 0, b.topics.Len())

	for _, topic := range b.topics.All() {
		topicARN := topic.TopicArn

		var tagMap map[string]string
		if b.topicTags[topicARN] != nil {
			tagMap = b.topicTags[topicARN].Clone()
		}

		result = append(result, TaggedTopicInfo{ARN: topicARN, Tags: tagMap})
	}

	return result
}

// TagTopicByARN applies tags to the SNS topic identified by its ARN.
func (b *InMemoryBackend) TagTopicByARN(topicARN string, newTags map[string]string) error {
	b.mu.Lock("TagTopicByARN")
	defer b.mu.Unlock()

	if !b.topics.Has(topicARN) {
		return fmt.Errorf("%w: topic %s", ErrTopicNotFound, topicARN)
	}

	if b.topicTags[topicARN] == nil {
		b.topicTags[topicARN] = svcTags.New("sns." + topicARN + ".tags")
	}

	b.topicTags[topicARN].Merge(newTags)

	return nil
}

// UntagTopicByARN removes the specified tag keys from the SNS topic identified by its ARN.
func (b *InMemoryBackend) UntagTopicByARN(topicARN string, tagKeys []string) error {
	b.mu.Lock("UntagTopicByARN")
	defer b.mu.Unlock()

	if !b.topics.Has(topicARN) {
		return fmt.Errorf("%w: topic %s", ErrTopicNotFound, topicARN)
	}

	if b.topicTags[topicARN] != nil {
		b.topicTags[topicARN].DeleteKeys(tagKeys)
	}

	return nil
}
