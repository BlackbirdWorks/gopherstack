package workspaces

import (
	"encoding/json"
	"fmt"
)

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

// samlPropertyKey maps a DeletableSamlProperty enum value to its ds.Properties key.
func samlPropertyKey(p string) string {
	switch p {
	case "SAML_PROPERTIES_USER_ACCESS_URL":
		return "Saml_UserAccessUrl"
	case "SAML_PROPERTIES_RELAY_STATE_PARAMETER_NAME":
		return "Saml_RelayStateParameterName"
	default:
		return ""
	}
}

// ModifySamlProperties stores SAML properties for a registered directory and
// clears the members named in propertiesToDelete. See ModifyEndpointEncryptionMode.
func (b *InMemoryBackend) ModifySamlProperties(
	directoryID string,
	props map[string]string,
	propertiesToDelete []string,
) error {
	b.mu.Lock("ModifySamlProperties")
	defer b.mu.Unlock()

	if !b.isDirectoryRegisteredLocked(directoryID) {
		return errDirectoryNotFound
	}

	ds, _ := b.dirSettings.Get(directoryID)
	for k, v := range props {
		ds.Properties["Saml_"+k] = v
	}

	for _, p := range propertiesToDelete {
		if key := samlPropertyKey(p); key != "" {
			delete(ds.Properties, key)
		}
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

// ModifyStreamingProperties merges streaming properties into a registered
// directory: set members replace, unset members keep their stored value.
func (b *InMemoryBackend) ModifyStreamingProperties(directoryID string, props StreamingProperties) error {
	b.mu.Lock("ModifyStreamingProperties")
	defer b.mu.Unlock()

	if !b.isDirectoryRegisteredLocked(directoryID) {
		return errDirectoryNotFound
	}

	ds, _ := b.dirSettings.Get(directoryID)

	merged := StreamingProperties{}
	if cur := streamingPropertiesFromDS(ds); cur != nil {
		merged = *cur
	}

	if props.StreamingExperiencePreferredProtocol != "" {
		merged.StreamingExperiencePreferredProtocol = props.StreamingExperiencePreferredProtocol
	}

	if props.UserSettings != nil {
		merged.UserSettings = props.UserSettings
	}

	if props.StorageConnectors != nil {
		merged.StorageConnectors = props.StorageConnectors
	}

	if props.GlobalAccelerator != nil {
		merged.GlobalAccelerator = props.GlobalAccelerator
	}

	raw, err := json.Marshal(merged)
	if err != nil {
		return fmt.Errorf("encode streaming properties: %w", err)
	}

	ds.Properties[streamingPropertiesKey] = string(raw)

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
