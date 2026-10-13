package detective

// DescribeOrganizationConfiguration returns AutoEnable setting for a graph.
func (b *InMemoryBackend) DescribeOrganizationConfiguration(graphARN string) (bool, error) {
	b.mu.RLock("DescribeOrganizationConfiguration")
	defer b.mu.RUnlock()

	if err := b.requireGraphLocked(graphARN); err != nil {
		return false, err
	}

	return b.orgConfigs[graphARN], nil
}

// UpdateOrganizationConfiguration sets the AutoEnable flag for a graph.
func (b *InMemoryBackend) UpdateOrganizationConfiguration(graphARN string, autoEnable bool) error {
	b.mu.Lock("UpdateOrganizationConfiguration")
	defer b.mu.Unlock()

	if err := b.requireGraphLocked(graphARN); err != nil {
		return err
	}

	b.orgConfigs[graphARN] = autoEnable

	return nil
}
