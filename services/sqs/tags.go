package sqs

import "github.com/blackbirdworks/gopherstack/pkgs/tags"

// TagQueue adds or updates tags on a queue.
func (b *InMemoryBackend) TagQueue(input *TagQueueInput) error {
	b.mu.Lock("TagQueue")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.lookupQueueByName(input.Region, name)
	if !ok {
		return ErrQueueNotFound
	}

	if q.Tags == nil {
		q.Tags = tags.New("sqs.queue." + q.Name + ".tags")
	}

	if input.Tags != nil {
		q.Tags.Merge(input.Tags.Clone())
	}

	return nil
}

// UntagQueue removes tags from a queue.
func (b *InMemoryBackend) UntagQueue(input *UntagQueueInput) error {
	b.mu.Lock("UntagQueue")
	defer b.mu.Unlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.lookupQueueByName(input.Region, name)
	if !ok {
		return ErrQueueNotFound
	}

	if q.Tags != nil {
		q.Tags.DeleteKeys(input.TagKeys)
	}

	return nil
}

// ListQueueTags returns the tags for a queue.
func (b *InMemoryBackend) ListQueueTags(input *ListQueueTagsInput) (*ListQueueTagsOutput, error) {
	b.mu.RLock("ListQueueTags")
	defer b.mu.RUnlock()

	name := queueNameFromInput(input.QueueURL)

	q, ok := b.lookupQueueByName(input.Region, name)
	if !ok {
		return nil, ErrQueueNotFound
	}

	if q.Tags == nil {
		return &ListQueueTagsOutput{Tags: tags.New("sqs.queue." + q.Name + ".tags")}, nil
	}

	return &ListQueueTagsOutput{Tags: q.Tags}, nil
}

// TaggedQueueInfo contains a queue's ARN and tag snapshot, for use by the
// Resource Groups Tagging API cross-service listing.
type TaggedQueueInfo struct {
	Tags map[string]string
	ARN  string
}

// TaggedQueues returns a snapshot of all queues with their ARNs and tags.
// Intended for use by the Resource Groups Tagging API provider.
func (b *InMemoryBackend) TaggedQueues() []TaggedQueueInfo {
	b.mu.RLock("TaggedQueues")
	defer b.mu.RUnlock()

	result := make([]TaggedQueueInfo, 0, b.queues.Len())

	for _, q := range b.queues.All() {
		var tagMap map[string]string
		if q.Tags != nil {
			tagMap = q.Tags.Clone()
		}

		result = append(result, TaggedQueueInfo{
			ARN:  q.Attributes[attrQueueArn],
			Tags: tagMap,
		})
	}

	return result
}

// TagQueueByARN applies tags to the queue identified by its ARN.
// Returns ErrQueueNotFound if no queue with that ARN exists.
func (b *InMemoryBackend) TagQueueByARN(queueARN string, newTags map[string]string) error {
	b.mu.Lock("TagQueueByARN")
	defer b.mu.Unlock()

	if q, ok := b.findQueueByARN(queueARN); ok {
		if q.Tags == nil {
			q.Tags = tags.New("sqs.queue." + q.Name + ".tags")
		}

		q.Tags.Merge(newTags)

		return nil
	}

	return ErrQueueNotFound
}

// UntagQueueByARN removes the specified tag keys from the queue identified by its ARN.
// Returns ErrQueueNotFound if no queue with that ARN exists.
func (b *InMemoryBackend) UntagQueueByARN(queueARN string, tagKeys []string) error {
	b.mu.Lock("UntagQueueByARN")
	defer b.mu.Unlock()

	if q, ok := b.findQueueByARN(queueARN); ok {
		if q.Tags != nil {
			q.Tags.DeleteKeys(tagKeys)
		}

		return nil
	}

	return ErrQueueNotFound
}
