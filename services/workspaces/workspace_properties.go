package workspaces

// ModifyEndpointEncryptionMode stores the endpoint encryption mode for a directory.
func (b *InMemoryBackend) ModifyEndpointEncryptionMode(directoryID, mode string) error {
	b.mu.Lock("ModifyEndpointEncryptionMode")
	defer b.mu.Unlock()

	b.ensureDirSettings(directoryID)

	ds, _ := b.dirSettings.Get(directoryID)
	ds.Properties["EndpointEncryptionMode"] = mode

	return nil
}

// ModifyCertificateBasedAuthProperties stores certificate auth properties for a directory.
func (b *InMemoryBackend) ModifyCertificateBasedAuthProperties(
	directoryID string,
	props map[string]string,
) error {
	b.mu.Lock("ModifyCertificateBasedAuthProperties")
	defer b.mu.Unlock()

	b.ensureDirSettings(directoryID)

	ds, _ := b.dirSettings.Get(directoryID)
	for k, v := range props {
		ds.Properties["CertAuth_"+k] = v
	}

	return nil
}

// ModifySamlProperties stores SAML properties for a directory.
func (b *InMemoryBackend) ModifySamlProperties(directoryID string, props map[string]string) error {
	b.mu.Lock("ModifySamlProperties")
	defer b.mu.Unlock()

	b.ensureDirSettings(directoryID)

	ds, _ := b.dirSettings.Get(directoryID)
	for k, v := range props {
		ds.Properties["Saml_"+k] = v
	}

	return nil
}

// ModifySelfservicePermissions stores self-service permissions for a directory.
func (b *InMemoryBackend) ModifySelfservicePermissions(
	directoryID string,
	props map[string]string,
) error {
	b.mu.Lock("ModifySelfservicePermissions")
	defer b.mu.Unlock()

	b.ensureDirSettings(directoryID)

	ds, _ := b.dirSettings.Get(directoryID)
	for k, v := range props {
		ds.Properties["SelfSvc_"+k] = v
	}

	return nil
}

// ModifyStreamingProperties stores streaming properties for a directory.
func (b *InMemoryBackend) ModifyStreamingProperties(
	directoryID string,
	props map[string]string,
) error {
	b.mu.Lock("ModifyStreamingProperties")
	defer b.mu.Unlock()

	b.ensureDirSettings(directoryID)

	ds, _ := b.dirSettings.Get(directoryID)
	for k, v := range props {
		ds.Properties["Streaming_"+k] = v
	}

	return nil
}

// ModifyWorkspaceAccessProperties stores workspace access properties for a directory.
func (b *InMemoryBackend) ModifyWorkspaceAccessProperties(
	directoryID string,
	props map[string]string,
) error {
	b.mu.Lock("ModifyWorkspaceAccessProperties")
	defer b.mu.Unlock()

	b.ensureDirSettings(directoryID)

	ds, _ := b.dirSettings.Get(directoryID)
	for k, v := range props {
		ds.Properties["Access_"+k] = v
	}

	return nil
}
