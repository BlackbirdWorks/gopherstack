package memorydb

// WaitIdle blocks until every container launch, resize and teardown goroutine has finished.
func (b *InMemoryBackend) WaitIdle() { b.dockerWG.Wait() }
