package workspaces

// ModifyEndpointEncryptionMode stores the endpoint encryption mode for a
// registered directory. Returns errDirectoryNotFound for a DirectoryId that
// was never registered, matching real AWS (ResourceNotFoundException is in
// this operation's error list).
func (b *InMemoryBackend) ModifyEndpointEncryptionMode(directoryID, mode string) error {
	b.mu.Lock("ModifyEndpointEncryptionMode")
	defer b.mu.Unlock()

	if !b.isDirectoryRegisteredLocked(directoryID) {
		return errDirectoryNotFound
	}

	ds, _ := b.dirSettings.Get(directoryID)
	ds.Properties["EndpointEncryptionMode"] = mode

	return nil
}

// deletableCertBasedAuthPropertyCertificateAuthorityArn is the real
// DeletableCertificateBasedAuthProperty enum value naming the
// CertAuth_CertificateAuthorityArn ds.Properties key.
const deletableCertBasedAuthPropertyCertificateAuthorityArn = "CERTIFICATE_BASED_AUTH_PROPERTIES_" +
	"CERTIFICATE_AUTHORITY_ARN"

// ModifyCertificateBasedAuthProperties stores certificate auth properties
// for a registered directory. See ModifyEndpointEncryptionMode.
func (b *InMemoryBackend) ModifyCertificateBasedAuthProperties(
	directoryID string,
	props map[string]string,
	propertiesToDelete []string,
) error {
	b.mu.Lock("ModifyCertificateBasedAuthProperties")
	defer b.mu.Unlock()

	if !b.isDirectoryRegisteredLocked(directoryID) {
		return errDirectoryNotFound
	}

	ds, _ := b.dirSettings.Get(directoryID)
	for k, v := range props {
		ds.Properties["CertAuth_"+k] = v
	}

	for _, p := range propertiesToDelete {
		if p == deletableCertBasedAuthPropertyCertificateAuthorityArn {
			delete(ds.Properties, "CertAuth_CertificateAuthorityArn")
		}
	}

	return nil
}

// ModifySamlProperties stores SAML properties for a registered directory.
// See ModifyEndpointEncryptionMode.
func (b *InMemoryBackend) ModifySamlProperties(directoryID string, props map[string]string) error {
	b.mu.Lock("ModifySamlProperties")
	defer b.mu.Unlock()

	if !b.isDirectoryRegisteredLocked(directoryID) {
		return errDirectoryNotFound
	}

	ds, _ := b.dirSettings.Get(directoryID)
	for k, v := range props {
		ds.Properties["Saml_"+k] = v
	}

	return nil
}

// ModifySelfservicePermissions stores self-service permissions for a
// registered directory. See ModifyEndpointEncryptionMode.
func (b *InMemoryBackend) ModifySelfservicePermissions(
	directoryID string,
	props map[string]string,
) error {
	b.mu.Lock("ModifySelfservicePermissions")
	defer b.mu.Unlock()

	if !b.isDirectoryRegisteredLocked(directoryID) {
		return errDirectoryNotFound
	}

	ds, _ := b.dirSettings.Get(directoryID)
	for k, v := range props {
		ds.Properties["SelfSvc_"+k] = v
	}

	return nil
}

// ModifyStreamingProperties stores streaming properties for a registered
// directory. See ModifyEndpointEncryptionMode.
func (b *InMemoryBackend) ModifyStreamingProperties(
	directoryID string,
	props map[string]string,
) error {
	b.mu.Lock("ModifyStreamingProperties")
	defer b.mu.Unlock()

	if !b.isDirectoryRegisteredLocked(directoryID) {
		return errDirectoryNotFound
	}

	ds, _ := b.dirSettings.Get(directoryID)
	for k, v := range props {
		ds.Properties["Streaming_"+k] = v
	}

	return nil
}

// ModifyWorkspaceAccessProperties stores workspace access properties for a
// registered directory. See ModifyEndpointEncryptionMode.
func (b *InMemoryBackend) ModifyWorkspaceAccessProperties(
	directoryID string,
	props map[string]string,
) error {
	b.mu.Lock("ModifyWorkspaceAccessProperties")
	defer b.mu.Unlock()

	if !b.isDirectoryRegisteredLocked(directoryID) {
		return errDirectoryNotFound
	}

	ds, _ := b.dirSettings.Get(directoryID)
	for k, v := range props {
		ds.Properties["Access_"+k] = v
	}

	return nil
}
