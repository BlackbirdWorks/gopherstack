package fsx

// DescribeSharedVpcConfiguration returns the shared VPC configuration.
func (b *InMemoryBackend) DescribeSharedVpcConfiguration() (*SharedVpcConfiguration, error) {
	b.mu.RLock("DescribeSharedVpcConfiguration")
	defer b.mu.RUnlock()

	return &SharedVpcConfiguration{EnableSharedVpcOnFileSystemCreation: b.sharedVpcEnabled}, nil
}

type updateSharedVpcConfigurationInput struct {
	EnableSharedVpcOnFileSystemCreation string `json:"EnableSharedVpcOnFileSystemCreation"`
}

// UpdateSharedVpcConfiguration updates the shared VPC configuration.
func (b *InMemoryBackend) UpdateSharedVpcConfiguration(
	input *updateSharedVpcConfigurationInput,
) (*SharedVpcConfiguration, error) {
	b.mu.Lock("UpdateSharedVpcConfiguration")
	defer b.mu.Unlock()

	b.sharedVpcEnabled = input.EnableSharedVpcOnFileSystemCreation

	return &SharedVpcConfiguration{EnableSharedVpcOnFileSystemCreation: b.sharedVpcEnabled}, nil
}
