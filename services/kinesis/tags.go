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
