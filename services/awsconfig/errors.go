package awsconfig

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when a configuration recorder is not found.
	ErrNotFound = awserr.New("NoSuchConfigurationRecorder", awserr.ErrNotFound)
	// ErrNoSuchDeliveryChannel is returned when a delivery channel is not found.
	ErrNoSuchDeliveryChannel = awserr.New("NoSuchDeliveryChannelException", awserr.ErrNotFound)
	// ErrNoSuchConfigRule is returned when a config rule is not found.
	ErrNoSuchConfigRule = awserr.New("NoSuchConfigRuleException", awserr.ErrNotFound)
	// ErrNoSuchAggregator is returned when a configuration aggregator is not found.
	ErrNoSuchAggregator = awserr.New("NoSuchConfigurationAggregatorException", awserr.ErrNotFound)
	// ErrNoSuchConformancePack is returned when a conformance pack is not found.
	ErrNoSuchConformancePack = awserr.New("NoSuchConformancePackException", awserr.ErrNotFound)
	// ErrNoSuchOrganizationConfigRule is returned when an organization config rule is not found.
	ErrNoSuchOrganizationConfigRule = awserr.New("NoSuchOrganizationConfigRuleException", awserr.ErrNotFound)
	// ErrNoSuchOrganizationConformancePack is returned when an org conformance pack is not found.
	// The wire error type is NoSuchOrganizationConformancePackException (verified against
	// aws-sdk-go-v2/service/configservice's DeleteOrganizationConformancePack deserializer).
	ErrNoSuchOrganizationConformancePack = awserr.New(
		"NoSuchOrganizationConformancePackException",
		awserr.ErrNotFound,
	)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("MaxNumberOfConfigurationRecordersExceededException", awserr.ErrAlreadyExists)
	// ErrNoDeliveryChannel is returned when starting a recorder with no delivery channel configured.
	ErrNoDeliveryChannel = awserr.New("NoAvailableDeliveryChannelException", awserr.ErrInvalidParameter)
	// ErrValidation is returned when a required field is missing or invalid.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrResourceNotFound is returned when a referenced resource evaluation does not exist.
	ErrResourceNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
)
