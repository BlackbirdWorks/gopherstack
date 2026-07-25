package kinesis

import (
	"context"
	"errors"
	"regexp"
)

// KMSKeyValidator optionally validates a KMS KeyId against a real KMS backend.
// Implemented by an adapter wrapping the KMS service backend and attached via
// WithKMSValidator (mirrors services/ssm's KMSEncryptor injection pattern --
// see cli.go's wireKinesisKMS). When no validator is wired, StartStreamEncryption
// still enforces the KeyId *shape* AWS documents (UUID / key ARN / alias ARN /
// "alias/..." name) but cannot know whether the key actually exists, is
// disabled, or is pending deletion -- those KMS-specific exceptions require
// real cross-service key state.
type KMSKeyValidator interface {
	// ValidateKMSKey resolves keyID against the KMS backend and returns nil if
	// the key exists and is usable, or one of ErrKMSNotFound/ErrKMSDisabled/
	// ErrKMSInvalidState (kinesis sentinels) describing why it is not.
	ValidateKMSKey(ctx context.Context, keyID string) error
}

// WithKMSValidator attaches a KMSKeyValidator so StartStreamEncryption can
// verify a KeyId resolves to a real, usable KMS key. Returns b for chaining.
func (b *InMemoryBackend) WithKMSValidator(v KMSKeyValidator) *InMemoryBackend {
	b.kmsValidator = v

	return b
}

// kmsKeyIDRe matches the four KeyId shapes AWS's StartStreamEncryption/
// StopStreamEncryption document: a bare key UUID, a key ARN, an alias ARN, or
// an "alias/..." name (including the Kinesis-owned "alias/aws/kinesis").
var kmsKeyIDRe = regexp.MustCompile(
	`^(` +
		`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}` + // key UUID
		`|arn:[^:]+:kms:[^:]*:[^:]*:key/[0-9a-fA-F-]+` + // key ARN
		`|arn:[^:]+:kms:[^:]*:[^:]*:alias/.+` + // alias ARN
		`|alias/.+` + // alias name
		`)$`,
)

// validateKMSKeyIDFormat reports whether keyID matches one of the shapes AWS
// accepts for StartStreamEncryption/StopStreamEncryption's required KeyId.
func validateKMSKeyIDFormat(keyID string) bool {
	return keyID != "" && kmsKeyIDRe.MatchString(keyID)
}

// resolveKMSKey validates a KeyId's shape and, if a validator is wired, its
// existence/usability against the real KMS backend (an in-process call into
// the kms package's own locked backend -- safe to make while holding
// stream.mu since kms never calls back into kinesis). Only called for
// StartStreamEncryption -- StopStreamEncryption requires KeyId to be present
// and well-formed (matching the real SDK's required-field validation) but
// never fails on key state, since disabling encryption is always safe even if
// the key was later disabled or deleted.
func (b *InMemoryBackend) resolveKMSKey(ctx context.Context, keyID string) error {
	if !validateKMSKeyIDFormat(keyID) {
		return errInvalidKMSKeyID
	}

	if b.kmsValidator == nil {
		return nil
	}

	err := b.kmsValidator.ValidateKMSKey(ctx, keyID)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrKMSNotFound), errors.Is(err, ErrKMSDisabled), errors.Is(err, ErrKMSInvalidState):
		return err
	default:
		// An adapter error we don't recognize -- surface it as NotFound rather
		// than silently accepting an unvalidated key.
		return ErrKMSNotFound
	}
}

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

	// KMS key resolution (format + optional cross-service existence/state
	// check) happens last, after the resource-existence and required-field
	// checks above, matching the "stream not found" test expectations that
	// predate KMS validation -- a malformed KeyId against a nonexistent
	// stream still surfaces ResourceNotFoundException, not InvalidArgumentException.
	if err := b.resolveKMSKey(ctx, input.KeyID); err != nil {
		return err
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

	// KeyId is a required field on StopStreamEncryptionInput per the real SDK
	// model even though stopping encryption never needs to look the key up;
	// only its presence/shape is validated here (no KMS backend call).
	if !validateKMSKeyIDFormat(input.KeyID) {
		return errInvalidKMSKeyID
	}

	stream.EncryptionType = encryptionTypeNone
	stream.KeyID = ""

	return nil
}
