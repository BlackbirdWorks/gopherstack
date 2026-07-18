package detective

// DescribeOrganizationConfiguration returns AutoEnable setting for a graph.
func (b *InMemoryBackend) DescribeOrganizationConfiguration(graphARN string) (bool, error) {
	b.mu.RLock("DescribeOrganizationConfiguration")
	defer b.mu.RUnlock()

	if !b.graphs.Has(graphARN) {
		return false, ErrGraphNotFound
	}

	return b.orgConfigs[graphARN], nil
}

// UpdateOrganizationConfiguration sets the AutoEnable flag for a graph.
func (b *InMemoryBackend) UpdateOrganizationConfiguration(graphARN string, autoEnable bool) error {
	b.mu.Lock("UpdateOrganizationConfiguration")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return ErrGraphNotFound
	}

	b.orgConfigs[graphARN] = autoEnable

	return nil
}
