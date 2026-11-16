package azurecontainerregistry

import "errors"

// Backend errors.
var (
	ErrRegistryNotFound = errors.New("azurecontainerregistry: registry not found")
	ErrInvalidName      = errors.New("azurecontainerregistry: invalid registry name")
	ErrAdminDisabled    = errors.New("azurecontainerregistry: admin user is not enabled")
	ErrInvalidCred      = errors.New("azurecontainerregistry: credential name must be password or password2")
	ErrWebhookNotFound  = errors.New("azurecontainerregistry: webhook not found")
	ErrNilAppContext    = errors.New("azurecontainerregistry: nil app context")
	ErrNotFound         = errors.New("azurecontainerregistry: not found")
	ErrDenied           = errors.New("azurecontainerregistry: operation is disabled by the resource's attributes")
	ErrTLSKeyMissing    = errors.New("azurecontainerregistry: --azure-acr-tls-cert set without --azure-acr-tls-key")
	ErrTLSCertMissing   = errors.New("azurecontainerregistry: --azure-acr-tls-key set without --azure-acr-tls-cert")
	errNullEntry        = errors.New("azurecontainerregistry: null snapshot entry")
)

// Scope map and token errors.
var (
	ErrScopeMapNotFound  = errors.New("azurecontainerregistry: scope map not found")
	ErrScopeMapReadOnly  = errors.New("azurecontainerregistry: system scope maps cannot be changed")
	ErrScopeMapInUse     = errors.New("azurecontainerregistry: scope map is used by a token")
	ErrInvalidScopeMap   = errors.New("azurecontainerregistry: invalid scope map action")
	ErrTokenNotFound     = errors.New("azurecontainerregistry: token not found")
	ErrInvalidTokenName  = errors.New("azurecontainerregistry: invalid token password name")
	ErrInvalidTokenState = errors.New("azurecontainerregistry: token status must be enabled or disabled")
)

// Import errors.
var (
	ErrImportSourceNotFound = errors.New("azurecontainerregistry: import source image not found")
	ErrImportTagExists      = errors.New("azurecontainerregistry: import target tag already exists")
	ErrImportRemoteDisabled = errors.New("azurecontainerregistry: importing from external registries is disabled")
	ErrImportAuth           = errors.New("azurecontainerregistry: the source registry refused authentication")
	ErrImportInvalid        = errors.New("azurecontainerregistry: invalid import request")
	ErrImportUpstream       = errors.New("azurecontainerregistry: the source registry returned an error")
)

// ErrInvalidNetworkRule is returned for an IP rule that is not an IPv4 address or CIDR range.
var ErrInvalidNetworkRule = errors.New("azurecontainerregistry: IP rule must be an IPv4 address or CIDR range")

// ErrKeyUnavailable is returned when a customer-managed encryption key cannot be used.
var ErrKeyUnavailable = errors.New("azurecontainerregistry: the customer-managed key is not usable")
