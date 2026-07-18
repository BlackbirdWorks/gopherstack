package acmpca

import "errors"

var (
	// ErrCANotFound is returned when a Certificate Authority is not found.
	ErrCANotFound = errors.New("ResourceNotFoundException")
	// ErrCertNotFound is returned when an issued certificate is not found.
	ErrCertNotFound = errors.New("ResourceNotFoundException")
	// ErrInvalidParameter is returned when an invalid parameter is provided.
	ErrInvalidParameter = errors.New("InvalidParameterException")
	// ErrInvalidState is returned when the CA is in an invalid state for the operation.
	ErrInvalidState = errors.New("InvalidStateException")
	// ErrPermissionNotFound is returned when a CA permission is not found.
	ErrPermissionNotFound = errors.New("ResourceNotFoundException")
	// ErrPermissionAlreadyExists is returned when a permission for the same
	// principal/source-account pair already exists on the CA.
	ErrPermissionAlreadyExists = errors.New("PermissionAlreadyExistsException")
	// ErrPolicyNotFound is returned when a CA policy is not found.
	ErrPolicyNotFound = errors.New("ResourceNotFoundException")
	// ErrAuditReportNotFound is returned when a CA audit report is not found.
	ErrAuditReportNotFound = errors.New("ResourceNotFoundException")

	errCAPrivKeyNil    = errors.New("CA private key is nil")
	errDecodeCSRPEM    = errors.New("failed to decode CSR PEM")
	errDecodeCACertPEM = errors.New("failed to decode CA certificate PEM")
)
