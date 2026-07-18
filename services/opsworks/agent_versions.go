package opsworks

// DescribeAgentVersions returns a static list of supported OpsWorks agent versions.
func (b *InMemoryBackend) DescribeAgentVersions(stackID string) ([]*AgentVersion, error) {
	b.mu.RLock("DescribeAgentVersions")
	defer b.mu.RUnlock()

	if stackID != "" {
		if !b.stacks.Has(stackID) {
			return nil, ErrStackNotFound
		}
	}

	return []*AgentVersion{
		{
			ConfigurationManager: &ConfigurationManager{
				Name:    configManagerChef,
				Version: "12",
			},
			Version: "4000-20161221135000",
		},
		{
			ConfigurationManager: &ConfigurationManager{
				Name:    configManagerChef,
				Version: "11.10",
			},
			Version: "4000-20161221135000",
		},
	}, nil
}

// DescribeOperatingSystems returns a static list of supported OpsWorks operating systems.
func (b *InMemoryBackend) DescribeOperatingSystems() ([]*OperatingSystem, error) {
	return []*OperatingSystem{
		{
			ConfigurationManagers: []*ConfigurationManager{
				{Name: configManagerChef, Version: "12"},
				{Name: configManagerChef, Version: "11.10"},
			},
			ID:              "AmazonLinux2",
			Name:            "Amazon Linux 2",
			Type:            osTypeLinux,
			ReportedVersion: "2",
			Supported:       true,
		},
		{
			ConfigurationManagers: []*ConfigurationManager{
				{Name: configManagerChef, Version: "12"},
			},
			ID:              "Ubuntu18.04",
			Name:            "Ubuntu 18.04 LTS",
			Type:            osTypeLinux,
			ReportedVersion: "18.04",
			Supported:       true,
		},
		{
			ConfigurationManagers: []*ConfigurationManager{
				{Name: configManagerChef, Version: "12"},
			},
			ID:              "CentOS7",
			Name:            "CentOS Linux 7",
			Type:            osTypeLinux,
			ReportedVersion: "7",
			Supported:       true,
		},
		{
			ConfigurationManagers: []*ConfigurationManager{
				{Name: configManagerChef, Version: "12.2"},
			},
			ID:              "MicrosoftWindowsServer2019",
			Name:            "Microsoft Windows Server 2019",
			Type:            "Windows",
			ReportedVersion: "2019",
			Supported:       true,
		},
	}, nil
}
