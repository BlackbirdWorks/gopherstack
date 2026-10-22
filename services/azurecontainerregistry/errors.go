package azurecontainerregistry

import "errors"

// Backend errors.
var (
	ErrRegistryNotFound = errors.New("azurecontainerregistry: registry not found")
	ErrInvalidName      = errors.New("azurecontainerregistry: invalid registry name")
	ErrAdminDisabled    = errors.New("azurecontainerregistry: admin user is not enabled")
	ErrInvalidCred      = errors.New("azurecontainerregistry: credential name must be password or password2")
	ErrNilAppContext    = errors.New("azurecontainerregistry: nil app context")
	ErrNotFound         = errors.New("azurecontainerregistry: not found")
	ErrDenied           = errors.New("azurecontainerregistry: operation is disabled by the resource's attributes")
	ErrTLSKeyMissing    = errors.New("azurecontainerregistry: --azure-acr-tls-cert set without --azure-acr-tls-key")
	ErrTLSCertMissing   = errors.New("azurecontainerregistry: --azure-acr-tls-key set without --azure-acr-tls-cert")
	errNullEntry        = errors.New("azurecontainerregistry: null snapshot entry")
)
