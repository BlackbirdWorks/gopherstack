package kinesis

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// ListTagsForResource returns the tags associated with a stream identified by its ARN.
// Tags are those stored on the stream's internal Tags store (set via TagResource).
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	input *ListTagsForResourceInput,
) (*ListTagsForResourceOutput, error) {
	region := regionFromARNOrCtx(ctx, input.ResourceARN, b.region)

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

// TagResource adds or updates tags on a stream identified by its ARN.
// This is the ARN-based counterpart to AddTagsToStream.
func (b *InMemoryBackend) TagResource(ctx context.Context, input *TagResourceInput) error {
	region := regionFromARNOrCtx(ctx, input.ResourceARN, b.region)

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

// UntagResource removes tags from a stream identified by its ARN.
// This is the ARN-based counterpart to RemoveTagsFromStream.
func (b *InMemoryBackend) UntagResource(ctx context.Context, input *UntagResourceInput) error {
	region := regionFromARNOrCtx(ctx, input.ResourceARN, b.region)

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
