package elasticsearch

import "errors"

// Errors returned by the Elasticsearch backend.
var (
	ErrDomainNotFound      = errors.New("ResourceNotFoundException")
	ErrDomainAlreadyExists = errors.New("ResourceAlreadyExistsException")
	// ErrValidation is returned for invalid or missing input parameters.
	ErrValidation = errors.New("ValidationException")
	// ErrInvalidParameter is an alias for ErrValidation kept for compatibility.
	ErrInvalidParameter    = ErrValidation
	ErrConnectionNotFound  = errors.New("ResourceNotFoundException")
	ErrPackageNotFound     = errors.New("ResourceNotFoundException")
	ErrVpcEndpointNotFound = errors.New("ResourceNotFoundException")
	// ErrPackageAlreadyAssociated is returned when AssociatePackage targets a
	// (package, domain) pair that is already associated. AWS returns ConflictException.
	ErrPackageAlreadyAssociated = errors.New("ConflictException")
	// ErrServiceRoleInUse is returned by DeleteElasticsearchServiceRole when
	// a VPC domain still uses the service-linked role. Real AWS: "Role
	// deletion will fail if any existing VPC domains use the role. You must
	// delete any such Elasticsearch domains before deleting the role".
	ErrServiceRoleInUse = errors.New("ValidationException")
)
