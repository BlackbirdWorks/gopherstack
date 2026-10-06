package rekognition

import "github.com/blackbirdworks/gopherstack/pkgs/idempotency"

// Idempotency returns the bounded ClientRequestToken memo.
func (b *InMemoryBackend) Idempotency() *idempotency.Memo { return b.idem }
