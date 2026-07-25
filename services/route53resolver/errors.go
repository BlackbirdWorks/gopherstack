package route53resolver

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	ErrNotFound         = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists    = awserr.New("ResourceExistsException", awserr.ErrAlreadyExists)
	ErrValidation       = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
	ErrInvalidParameter = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)

	// ErrBatchValidation is used for envelope-level required-field validation on
	// BatchCreateFirewallRule/BatchUpdateFirewallRule/BatchDeleteFirewallRule.
	// Unlike every singular Firewall Rule op in this service (which raises
	// InvalidRequestException, see ErrValidation), AWS's own API reference
	// documents ValidationException as the error class for these three batch
	// operations specifically -- verified: the Errors sections of
	// API_route53resolver_Batch{Create,Update,Delete}FirewallRule.html each list
	// AccessDeniedException/InternalServiceErrorException/LimitExceededException/
	// ThrottlingException/ValidationException, with no InvalidRequestException.
	ErrBatchValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)
