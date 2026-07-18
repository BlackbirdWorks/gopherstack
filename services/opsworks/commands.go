package opsworks

// DescribeCommands returns commands filtered by deployment, instance, or IDs.
func (b *InMemoryBackend) DescribeCommands(deploymentID, instanceID string, commandIDs []string) ([]*Command, error) {
	b.mu.RLock("DescribeCommands")
	defer b.mu.RUnlock()

	if len(commandIDs) > 0 {
		result := make([]*Command, 0, len(commandIDs))
		for _, id := range commandIDs {
			c, ok := b.commands.Get(id)
			if !ok {
				return nil, ErrCommandNotFound
			}
			result = append(result, c.toCommand())
		}

		return result, nil
	}

	result := make([]*Command, 0)
	for _, c := range b.commands.All() {
		if deploymentID != "" && c.DeploymentID != deploymentID {
			continue
		}
		if instanceID != "" && c.InstanceID != instanceID {
			continue
		}
		result = append(result, c.toCommand())
	}

	return result, nil
}
