package eks

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when an EKS resource is not found.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when an EKS resource already exists.
	ErrAlreadyExists = awserr.New("ResourceInUseException", awserr.ErrConflict)
	// ErrValidation is returned when request input fails validation. The code
	// is "InvalidParameterException" -- "InvalidParameterValueException" does
	// not exist anywhere in aws-sdk-go-v2/service/eks@v1.90.4 (confirmed by
	// grepping the whole module), and every op that models parameter
	// validation in its own deserializeOpError switch uses
	// InvalidParameterException.
	ErrValidation = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)
	// ErrInvalidRequest is for state-conflict validation failures (e.g.
	// cancelling an update whose type/status does not support cancellation)
	// that real AWS EKS reports as InvalidRequestException rather than
	// InvalidParameterValueException -- verified against
	// aws-sdk-go-v2/service/eks's deserializers.go error-code switch, which
	// lists InvalidRequestException as a distinct client-fault shape from
	// InvalidParameterException on ops like CancelUpdate.
	ErrInvalidRequest = awserr.New("InvalidRequestException", awserr.ErrConflict)
)
