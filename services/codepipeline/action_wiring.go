package codepipeline

import "time"

// SetStackDeployer wires CloudFormation for Deploy/CloudFormation actions.
func (b *InMemoryBackend) SetStackDeployer(d StackDeployer) {
	b.mu.Lock("SetStackDeployer")
	defer b.mu.Unlock()

	b.stackDeployer = d
}

// SetECSDeployer wires ECS for Deploy/ECS actions.
func (b *InMemoryBackend) SetECSDeployer(d ECSDeployer) {
	b.mu.Lock("SetECSDeployer")
	defer b.mu.Unlock()

	b.ecsDeployer = d
}

// SetImageSource wires ECR for Source/ECR actions.
func (b *InMemoryBackend) SetImageSource(s ImageSource) {
	b.mu.Lock("SetImageSource")
	defer b.mu.Unlock()

	b.imageSource = s
}

// SetRepositoryReader wires CodeCommit for Source/CodeCommit actions.
func (b *InMemoryBackend) SetRepositoryReader(r RepositoryReader) {
	b.mu.Lock("SetRepositoryReader")
	defer b.mu.Unlock()

	b.repositoryReader = r
}

// SetClock overrides the time source used for action timeouts; nil restores time.Now.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.clock = now
}

func (b *InMemoryBackend) now() time.Time {
	if b.clock != nil {
		return b.clock().UTC()
	}

	return time.Now().UTC()
}
