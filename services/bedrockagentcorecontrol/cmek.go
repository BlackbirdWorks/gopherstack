package bedrockagentcorecontrol

import (
	"context"
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
)

// SetKeyChecker resolves kmsKeyArn and encryptionKeyArn members against AWS KMS; nil disables the lookup.
func (b *InMemoryBackend) SetKeyChecker(c cmek.KeyChecker) {
	b.mu.Lock("SetKeyChecker")
	defer b.mu.Unlock()

	b.keys = c
}

func (b *InMemoryBackend) keyChecker() cmek.KeyChecker {
	b.mu.RLock("keyChecker")
	defer b.mu.RUnlock()

	return b.keys
}

// keyARNs collects every kmsKeyArn and encryptionKeyArn string nested anywhere in a request.
func keyARNs(v any, out *[]string) {
	switch t := v.(type) {
	case map[string]any:
		for k, m := range t {
			if s, ok := m.(string); ok && (k == "kmsKeyArn" || k == "encryptionKeyArn") && s != "" {
				*out = append(*out, s)

				continue
			}

			keyARNs(m, out)
		}
	case []any:
		for _, m := range t {
			keyARNs(m, out)
		}
	}
}

// checkKeys rejects a request naming a KMS key that does not exist or cannot encrypt with a ValidationException.
func (b *InMemoryBackend) checkKeys(ctx context.Context, in args) error {
	c := b.keyChecker()
	if c == nil {
		return nil
	}

	var arns []string

	keyARNs(map[string]any(in), &arns)

	for _, key := range arns {
		err := c.CheckKey(ctx, key)

		switch {
		case err == nil:
		case errors.Is(err, cmek.ErrNotFound):
			return invalid("KMS key %s does not exist", key)
		case errors.Is(err, cmek.ErrDisabled):
			return invalid("KMS key %s is disabled", key)
		case errors.Is(err, cmek.ErrState):
			return invalid("KMS key %s is in a state that cannot be used for encryption", key)
		case errors.Is(err, cmek.ErrPurpose):
			return invalid("KMS key %s must be a symmetric ENCRYPT_DECRYPT key", key)
		default:
			return err
		}
	}

	return nil
}
