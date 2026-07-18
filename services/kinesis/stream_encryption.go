package kinesis

import "context"

// StartStreamEncryption enables server-side encryption on a stream.
func (b *InMemoryBackend) StartStreamEncryption(ctx context.Context, input *StartStreamEncryptionInput) error {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.Lock("StartStreamEncryption")
	defer b.mu.Unlock()

	streamName := input.StreamName
	if streamName == "" {
		streamName = streamNameFromARN(input.StreamARN)
	}

	stream, ok := b.streams.Get(streamKey(region, streamName))
	if !ok {
		return ErrStreamNotFound
	}
	stream.mu.Lock("StartStreamEncryption.stream")
	defer stream.mu.Unlock()

	if input.EncryptionType != encryptionTypeKMS {
		return ErrInvalidArgument
	}

	stream.EncryptionType = input.EncryptionType
	stream.KeyID = input.KeyID

	return nil
}

// StopStreamEncryption disables server-side encryption on a stream.
func (b *InMemoryBackend) StopStreamEncryption(ctx context.Context, input *StopStreamEncryptionInput) error {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.Lock("StopStreamEncryption")
	defer b.mu.Unlock()

	streamName := input.StreamName
	if streamName == "" {
		streamName = streamNameFromARN(input.StreamARN)
	}

	stream, ok := b.streams.Get(streamKey(region, streamName))
	if !ok {
		return ErrStreamNotFound
	}
	stream.mu.Lock("StopStreamEncryption.stream")
	defer stream.mu.Unlock()

	stream.EncryptionType = encryptionTypeNone
	stream.KeyID = ""

	return nil
}
