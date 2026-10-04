package kms

import (
	"fmt"
	"strings"
)

const (
	storeTypeExternal = "EXTERNAL_KEY_STORE"
	storeTypeCloudHSM = "AWS_CLOUDHSM"

	xksConnPublic = "PUBLIC_ENDPOINT"
	xksConnVPC    = "VPC_ENDPOINT_SERVICE"

	xksPathSuffix = "/kms/xks/v1"
)

func xksInvalid(msg string) error {
	return fmt.Errorf("%w: %s", ErrXksProxyInvalidConfiguration, msg)
}

// validateXksShape checks the endpoint/path/connectivity/service-name relationships.
func validateXksShape(c *XksProxyConfiguration) error {
	if c.Connectivity != xksConnPublic && c.Connectivity != xksConnVPC {
		return xksInvalid("XksProxyConnectivity must be PUBLIC_ENDPOINT or VPC_ENDPOINT_SERVICE")
	}

	if !strings.HasPrefix(c.URIEndpoint, "https://") || len(c.URIEndpoint) == len("https://") ||
		strings.ContainsAny(c.URIEndpoint[len("https://"):], "/\\") {
		return xksInvalid("XksProxyUriEndpoint must begin with https:// and contain no slashes after it")
	}

	if !strings.HasPrefix(c.URIPath, "/") || !strings.HasSuffix(c.URIPath, xksPathSuffix) {
		return xksInvalid("XksProxyUriPath must start with / and end with " + xksPathSuffix)
	}

	if c.Connectivity == xksConnVPC && c.VpcEndpointServiceName == "" {
		return xksInvalid("XksProxyVpcEndpointServiceName is required for VPC_ENDPOINT_SERVICE connectivity")
	}

	if c.Connectivity == xksConnPublic && (c.VpcEndpointServiceName != "" || c.VpcEndpointServiceOwner != "") {
		return xksInvalid("VPC endpoint service settings are only valid for VPC_ENDPOINT_SERVICE connectivity")
	}

	if c.AccessKeyID == "" {
		return xksInvalid("XksProxyAuthenticationCredential with AccessKeyId and RawSecretAccessKey is required")
	}

	return nil
}

// checkXksUniqueLocked enforces the documented uniqueness rules against other stores.
func (b *InMemoryBackend) checkXksUniqueLocked(region, selfID string, c *XksProxyConfiguration) error {
	for _, other := range b.customKeyStoresStore(region).All() {
		o := other.XksProxyConfiguration
		if other.CustomKeyStoreID == selfID || o == nil {
			continue
		}

		if o.URIEndpoint == c.URIEndpoint && o.URIPath == c.URIPath {
			return fmt.Errorf("%w: %s%s", ErrXksProxyURIInUse, c.URIEndpoint, c.URIPath)
		}

		if o.URIEndpoint == c.URIEndpoint && (o.Connectivity != c.Connectivity || c.Connectivity == xksConnVPC) {
			return fmt.Errorf("%w: %s", ErrXksProxyURIEndpointInUse, c.URIEndpoint)
		}

		if c.VpcEndpointServiceName != "" && o.VpcEndpointServiceName == c.VpcEndpointServiceName {
			return fmt.Errorf("%w: %s", ErrXksProxyVPCEndpointServiceInUse, c.VpcEndpointServiceName)
		}
	}

	return nil
}

// buildXksConfig validates the XKS inputs of CreateCustomKeyStore and returns the stored configuration.
func (b *InMemoryBackend) buildXksConfig(
	region, storeType string,
	in *CreateCustomKeyStoreInput,
) (*XksProxyConfiguration, error) {
	hasXks := in.XksProxyAuthenticationCredential != nil || in.XksProxyConnectivity != "" ||
		in.XksProxyURIEndpoint != "" || in.XksProxyURIPath != "" ||
		in.XksProxyVpcEndpointServiceName != "" || in.XksProxyVpcEndpointServiceOwner != ""

	if storeType != storeTypeExternal {
		if hasXks {
			return nil, fmt.Errorf("%w: XKS proxy parameters are only valid for EXTERNAL_KEY_STORE", ErrValidation)
		}

		return nil, nil //nolint:nilnil // CloudHSM stores carry no XKS configuration.
	}

	cfg := &XksProxyConfiguration{
		Connectivity:           in.XksProxyConnectivity,
		URIEndpoint:            in.XksProxyURIEndpoint,
		URIPath:                in.XksProxyURIPath,
		VpcEndpointServiceName: in.XksProxyVpcEndpointServiceName,
	}

	if in.XksProxyAuthenticationCredential != nil {
		cfg.AccessKeyID = in.XksProxyAuthenticationCredential.AccessKeyID
	}

	if cfg.Connectivity == xksConnVPC {
		cfg.VpcEndpointServiceOwner = in.XksProxyVpcEndpointServiceOwner
		if cfg.VpcEndpointServiceOwner == "" {
			cfg.VpcEndpointServiceOwner = b.accountID
		}
	}

	if err := validateXksShape(cfg); err != nil {
		return nil, err
	}

	if err := b.checkXksUniqueLocked(region, "", cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// applyCustomKeyStoreUpdate applies the non-name properties of UpdateCustomKeyStore.
func (b *InMemoryBackend) applyCustomKeyStoreUpdate(
	region string,
	ks *CustomKeyStore,
	in *UpdateCustomKeyStoreInput,
) error {
	if ks.CustomKeyStoreType == storeTypeExternal {
		return b.applyXksUpdate(region, ks, in)
	}

	if in.XksProxyAuthenticationCredential != nil || in.XksProxyConnectivity != nil ||
		in.XksProxyURIEndpoint != nil || in.XksProxyURIPath != nil ||
		in.XksProxyVpcEndpointServiceName != nil || in.XksProxyVpcEndpointServiceOwner != nil {
		return fmt.Errorf("%w: XKS proxy parameters are only valid for EXTERNAL_KEY_STORE", ErrValidation)
	}

	if in.CloudHsmClusterID == nil && in.KeyStorePassword == nil {
		return nil
	}

	if ks.ConnectionState != ConnectionStateDisconnected {
		return fmt.Errorf(
			"%w: custom key store must be DISCONNECTED to change this property; current state: %s",
			ErrCustomKeyStoreInvalidState, ks.ConnectionState,
		)
	}

	if in.KeyStorePassword != nil && (len(*in.KeyStorePassword) < 7 || len(*in.KeyStorePassword) > 32) {
		return fmt.Errorf("%w: KeyStorePassword must be 7 to 32 characters", ErrValidation)
	}

	if in.CloudHsmClusterID != nil {
		ks.CloudHsmClusterID = *in.CloudHsmClusterID
	}

	return nil
}

func (b *InMemoryBackend) applyXksUpdate(region string, ks *CustomKeyStore, in *UpdateCustomKeyStoreInput) error {
	if in.CloudHsmClusterID != nil || in.KeyStorePassword != nil {
		return fmt.Errorf("%w: CloudHSM parameters are only valid for AWS_CLOUDHSM stores", ErrValidation)
	}

	needsDisconnected := in.XksProxyConnectivity != nil || in.XksProxyURIEndpoint != nil ||
		in.XksProxyVpcEndpointServiceName != nil || in.XksProxyVpcEndpointServiceOwner != nil
	if needsDisconnected && ks.ConnectionState != ConnectionStateDisconnected {
		return fmt.Errorf(
			"%w: custom key store must be DISCONNECTED to change this property; current state: %s",
			ErrCustomKeyStoreInvalidState, ks.ConnectionState,
		)
	}

	next := XksProxyConfiguration{}
	if ks.XksProxyConfiguration != nil {
		next = *ks.XksProxyConfiguration
	}

	if in.XksProxyAuthenticationCredential != nil {
		next.AccessKeyID = in.XksProxyAuthenticationCredential.AccessKeyID
	}

	setStr(&next.Connectivity, in.XksProxyConnectivity)
	setStr(&next.URIEndpoint, in.XksProxyURIEndpoint)
	setStr(&next.URIPath, in.XksProxyURIPath)
	setStr(&next.VpcEndpointServiceName, in.XksProxyVpcEndpointServiceName)
	setStr(&next.VpcEndpointServiceOwner, in.XksProxyVpcEndpointServiceOwner)

	if next.Connectivity == xksConnPublic {
		next.VpcEndpointServiceName, next.VpcEndpointServiceOwner = "", ""
	} else if next.VpcEndpointServiceOwner == "" {
		next.VpcEndpointServiceOwner = b.accountID
	}

	if err := validateXksShape(&next); err != nil {
		return err
	}

	if err := b.checkXksUniqueLocked(region, ks.CustomKeyStoreID, &next); err != nil {
		return err
	}

	ks.XksProxyConfiguration = &next

	return nil
}

func setStr(dst *string, src *string) {
	if src != nil {
		*dst = *src
	}
}
