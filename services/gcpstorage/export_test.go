package gcpstorage

// SetStagedLimit lowers the total multipart staging cap.
func (b *InMemoryBackend) SetStagedLimit(n int64) { b.stagedCap = n }
