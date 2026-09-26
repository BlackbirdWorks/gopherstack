package cloudformation

import (
	"fmt"

	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
)

const (
	resTypeIAMSAMLProvider     = "AWS::IAM::SAMLProvider"
	resTypeIAMVirtualMFADevice = "AWS::IAM::VirtualMFADevice"
)

// createIAMExtrasResource handles AWS::IAM::SAMLProvider and
// AWS::IAM::VirtualMFADevice creation. Returns handled=false otherwise.
func (rc *ResourceCreator) createIAMExtrasResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeIAMSAMLProvider:
		id, err := rc.createIAMSAMLProvider(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeIAMVirtualMFADevice:
		id, err := rc.createIAMVirtualMFADevice(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteIAMExtrasResource handles deletion for the types created above.
func (rc *ResourceCreator) deleteIAMExtrasResource(resourceType, physicalID string) (bool, error) {
	if rc.backends.IAM == nil {
		switch resourceType {
		case resTypeIAMSAMLProvider, resTypeIAMVirtualMFADevice:
			return true, nil
		default:
			return false, nil
		}
	}

	switch resourceType {
	case resTypeIAMSAMLProvider:
		return true, ignoreNotFound(
			rc.backends.IAM.Backend.DeleteSAMLProvider(physicalID), iambackend.ErrSAMLProviderNotFound,
		)
	case resTypeIAMVirtualMFADevice:
		return true, ignoreNotFound(
			rc.backends.IAM.Backend.DeleteVirtualMFADevice(physicalID), iambackend.ErrUserNotFound,
		)
	default:
		return false, nil
	}
}

// ---- AWS::IAM::SAMLProvider ----
// Ref returns the ARN (documented). SamlProviderUUID has no backend field
// to stash, so it falls back to the ARN rather than being fabricated.

func (rc *ResourceCreator) createIAMSAMLProvider(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IAM == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	metadata := strProp(props, "SamlMetadataDocument", params, physicalIDs)

	p, err := rc.backends.IAM.Backend.CreateSAMLProvider(name, metadata)
	if err != nil {
		return "", fmt.Errorf("create SAML provider %s: %w", name, err)
	}

	return p.Arn, nil
}

// ---- AWS::IAM::VirtualMFADevice ----
// Ref returns the SerialNumber (documented). Associating the device with
// the Users property isn't performed: real AWS requires an MFA token/code
// to enable a device, which CFN's synchronous create can't supply.

func (rc *ResourceCreator) createIAMVirtualMFADevice(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.IAM == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "VirtualMfaDeviceName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	dev, err := rc.backends.IAM.Backend.CreateVirtualMFADevice(name, strProp(props, "Path", params, physicalIDs))
	if err != nil {
		return "", fmt.Errorf("create virtual MFA device %s: %w", name, err)
	}

	return dev.SerialNumber, nil
}
