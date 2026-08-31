package route53resolver

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	ErrNotFound         = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists    = awserr.New("ResourceExistsException", awserr.ErrAlreadyExists)
	ErrValidation       = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
	ErrInvalidParameter = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)

	// ErrBatchValidation is the bad-request sentinel for the entire
	// Firewall/Outpost op family (FirewallRule(Group)/FirewallDomainList/
	// FirewallConfig/OutpostResolver, plus GetResolverConfig) and for the
	// three Batch*FirewallRule ops. Unlike the singular Resolver* ops in this
	// service (which raise InvalidRequestException, see ErrValidation), every
	// op in this family's own awsAwsjson11_deserializeOpError<Op> switch in
	// the pinned SDK models ValidationException instead -- verified per-op
	// against aws-sdk-go-v2/service/route53resolver@v1.48.4's deserializers.go
	// (gopherstack-6flj/uox6 error-target audit, 2026-08-31; originally scoped
	// to just the three Batch ops, broadened after the same InvalidRequestException
	// mistake was found across the rest of the family).
	ErrBatchValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)
