package kinesis

import (
	"context"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// ListTagsForResource returns the tags associated with a Kinesis resource
// identified by its ARN -- a stream (tags stored on the stream's internal
// Tags store, set via TagResource) or, per the real op's doc comment ("the
// specified Kinesis resource"), an enhanced fan-out consumer (tags stored on
// Consumer.Tags, set via RegisterStreamConsumer's Tags parameter or
// TagResource against the consumer's own ARN).
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	input *ListTagsForResourceInput,
) (*ListTagsForResourceOutput, error) {
	region := regionFromARNOrCtx(ctx, input.ResourceARN, b.region)

	if sName, cName := consumerInfoFromARN(input.ResourceARN); cName != "" {
		return b.listConsumerTags(region, sName, cName)
	}

	b.mu.RLock("ListTagsForResource")

	streamName := streamNameFromARN(input.ResourceARN)
	stream, ok := b.streams.Get(streamKey(region, streamName))

	if !ok {
		b.mu.RUnlock()

		return nil, ErrStreamNotFound
	}
	stream.mu.RLock("ListTagsForResource.stream")
	b.mu.RUnlock()
	defer stream.mu.RUnlock()

	result := map[string]string{}
	if stream.Tags != nil {
		result = stream.Tags.Clone()
	}

	return &ListTagsForResourceOutput{Tags: result}, nil
}

// listConsumerTags is ListTagsForResource's consumer-ARN path.
func (b *InMemoryBackend) listConsumerTags(
	region, streamName, consumerName string,
) (*ListTagsForResourceOutput, error) {
	b.mu.RLock("ListTagsForResource")

	stream, ok := b.streams.Get(streamKey(region, streamName))
	if !ok {
		b.mu.RUnlock()

		return nil, ErrConsumerNotFound
	}
	stream.mu.RLock("ListTagsForResource.stream")
	b.mu.RUnlock()
	defer stream.mu.RUnlock()

	consumer, ok := stream.Consumers[consumerName]
	if !ok {
		return nil, ErrConsumerNotFound
	}

	result := make(map[string]string, len(consumer.Tags))
	maps.Copy(result, consumer.Tags)

	return &ListTagsForResourceOutput{Tags: result}, nil
}

// TaggedEntry pairs a stream ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's wireTaggingKinesis).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedStreams returns every Kinesis stream that currently has at least one
// tag, across every region this backend holds streams for.
func (b *InMemoryBackend) TaggedStreams() []TaggedEntry {
	b.mu.RLock("TaggedStreams")
	streams := b.streams.All()
	b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(streams))

	for _, s := range streams {
		s.mu.RLock("TaggedStreams.stream")
		var tagMap map[string]string
		if s.Tags != nil {
			tagMap = s.Tags.Clone()
		}
		arn := s.ARN
		s.mu.RUnlock()

		if len(tagMap) == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: arn, Tags: tagMap})
	}

	return out
}

// TagResource adds or updates tags on a Kinesis resource identified by its
// ARN -- a stream (the ARN-based counterpart to AddTagsToStream) or an
// enhanced fan-out consumer.
func (b *InMemoryBackend) TagResource(ctx context.Context, input *TagResourceInput) error {
	region := regionFromARNOrCtx(ctx, input.ResourceARN, b.region)

	if sName, cName := consumerInfoFromARN(input.ResourceARN); cName != "" {
		return b.tagConsumer(region, sName, cName, input.Tags)
	}

	b.mu.RLock("TagResource")

	streamName := streamNameFromARN(input.ResourceARN)
	stream, ok := b.streams.Get(streamKey(region, streamName))

	if !ok {
		b.mu.RUnlock()

		return ErrStreamNotFound
	}
	stream.mu.Lock("TagResource.stream")
	b.mu.RUnlock()
	defer stream.mu.Unlock()

	if stream.Tags == nil {
		stream.Tags = tags.New("kinesis.stream." + streamName + ".tags")
	}

	stream.Tags.Merge(input.Tags)

	return nil
}

// tagConsumer is TagResource's consumer-ARN path.
func (b *InMemoryBackend) tagConsumer(region, streamName, consumerName string, newTags map[string]string) error {
	b.mu.RLock("TagResource")

	stream, ok := b.streams.Get(streamKey(region, streamName))
	if !ok {
		b.mu.RUnlock()

		return ErrConsumerNotFound
	}
	stream.mu.Lock("TagResource.stream")
	b.mu.RUnlock()
	defer stream.mu.Unlock()

	consumer, ok := stream.Consumers[consumerName]
	if !ok {
		return ErrConsumerNotFound
	}

	if consumer.Tags == nil {
		consumer.Tags = make(map[string]string, len(newTags))
	}
	maps.Copy(consumer.Tags, newTags)

	return nil
}

// UntagResource removes tags from a Kinesis resource identified by its ARN --
// a stream (the ARN-based counterpart to RemoveTagsFromStream) or an
// enhanced fan-out consumer.
func (b *InMemoryBackend) UntagResource(ctx context.Context, input *UntagResourceInput) error {
	region := regionFromARNOrCtx(ctx, input.ResourceARN, b.region)

	if sName, cName := consumerInfoFromARN(input.ResourceARN); cName != "" {
		return b.untagConsumer(region, sName, cName, input.TagKeys)
	}

	b.mu.RLock("UntagResource")

	streamName := streamNameFromARN(input.ResourceARN)
	stream, ok := b.streams.Get(streamKey(region, streamName))

	if !ok {
		b.mu.RUnlock()

		return ErrStreamNotFound
	}
	stream.mu.Lock("UntagResource.stream")
	b.mu.RUnlock()
	defer stream.mu.Unlock()

	if stream.Tags != nil {
		stream.Tags.DeleteKeys(input.TagKeys)
	}

	return nil
}

// untagConsumer is UntagResource's consumer-ARN path.
func (b *InMemoryBackend) untagConsumer(region, streamName, consumerName string, keys []string) error {
	b.mu.RLock("UntagResource")

	stream, ok := b.streams.Get(streamKey(region, streamName))
	if !ok {
		b.mu.RUnlock()

		return ErrConsumerNotFound
	}
	stream.mu.Lock("UntagResource.stream")
	b.mu.RUnlock()
	defer stream.mu.Unlock()

	consumer, ok := stream.Consumers[consumerName]
	if !ok {
		return ErrConsumerNotFound
	}

	for _, k := range keys {
		delete(consumer.Tags, k)
	}

	return nil
}
