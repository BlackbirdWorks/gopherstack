package redshift

import "errors"

const (
	errClusterSnapshotNotFound = "ClusterSnapshotNotFound"
)

// Error code strings below are verified verbatim against the ErrorCode() method of
// each corresponding fault type in aws-sdk-go-v2/service/redshift@v1.62.3/types/errors.go.
// Real AWS is NOT consistent about the "Fault" suffix -- some fault ErrorCode() values
// include it (e.g. "HsmConfigurationNotFoundFault") and some strip it (e.g.
// "ClusterNotFound" for ClusterNotFoundFault) -- so each entry here was checked
// individually rather than assumed from a pattern. Do not "clean up" a suffix without
// re-checking the SDK source.
var (
	ErrClusterNotFound                = errors.New("ClusterNotFound")
	ErrClusterAlreadyExists           = errors.New("ClusterAlreadyExists")
	ErrInvalidParameter               = errors.New("InvalidParameterValue")
	ErrReservedNodeNotFound           = errors.New("ReservedNodeNotFound")
	ErrReservedNodeAlreadyExists      = errors.New("ReservedNodeAlreadyExists")
	ErrReservedNodeOfferingNotFound   = errors.New("ReservedNodeOfferingNotFound")
	ErrPartnerNotFound                = errors.New("PartnerNotFound")
	ErrDataShareNotFound              = errors.New("InvalidDataShareFault")
	ErrSecurityGroupNotFound          = errors.New("ClusterSecurityGroupNotFound")
	ErrSecurityGroupAlreadyExists     = errors.New("ClusterSecurityGroupAlreadyExists")
	ErrSnapshotNotFound               = errors.New(errClusterSnapshotNotFound)
	ErrSnapshotAlreadyExists          = errors.New("ClusterSnapshotAlreadyExists")
	ErrEndpointAuthNotFound           = errors.New("EndpointAuthorizationNotFound")
	ErrEndpointAuthAlreadyExists      = errors.New("EndpointAuthorizationAlreadyExists")
	ErrResizeNotFound                 = errors.New("ResizeNotFound")
	ErrResizeNotCancellable           = errors.New("InvalidClusterState")
	ErrParameterGroupNotFound         = errors.New("ClusterParameterGroupNotFound")
	ErrParameterGroupAlreadyExists    = errors.New("ClusterParameterGroupAlreadyExists")
	ErrSubnetGroupNotFound            = errors.New("ClusterSubnetGroupNotFoundFault")
	ErrSubnetGroupAlreadyExists       = errors.New("ClusterSubnetGroupAlreadyExists")
	ErrEventSubscriptionNotFound      = errors.New("SubscriptionNotFound")
	ErrEventSubscriptionAlreadyExists = errors.New("SubscriptionAlreadyExist")
	ErrSnapshotCopyGrantNotFound      = errors.New("SnapshotCopyGrantNotFoundFault")
	ErrSnapshotCopyGrantAlreadyExists = errors.New("SnapshotCopyGrantAlreadyExistsFault")
	ErrSnapshotScheduleNotFound       = errors.New("SnapshotScheduleNotFound")
	ErrSnapshotScheduleAlreadyExists  = errors.New("SnapshotScheduleAlreadyExists")
	ErrUsageLimitNotFound             = errors.New("UsageLimitNotFound")
	ErrAuthProfileNotFound            = errors.New("AuthenticationProfileNotFoundFault")
	ErrAuthProfileAlreadyExists       = errors.New("AuthenticationProfileAlreadyExistsFault")
	ErrResourcePolicyNotFound         = errors.New("ResourceNotFoundFault")
	ErrSnapshotCopyAlreadyEnabled     = errors.New("SnapshotCopyAlreadyEnabledFault")
	ErrSnapshotCopyNotEnabled         = errors.New("CopyToRegionDisabledFault")
	ErrHsmClientCertNotFound          = errors.New("HsmClientCertificateNotFoundFault")
	ErrHsmClientCertAlreadyExists     = errors.New("HsmClientCertificateAlreadyExistsFault")
	ErrHsmConfigNotFound              = errors.New("HsmConfigurationNotFoundFault")
	ErrHsmConfigAlreadyExists         = errors.New("HsmConfigurationAlreadyExistsFault")
	ErrScheduledActionNotFound        = errors.New("ScheduledActionNotFound")
	ErrScheduledActionAlreadyExists   = errors.New("ScheduledActionAlreadyExists")
	ErrCustomDomainNotFound           = errors.New("CustomDomainAssociationNotFoundFault")
	ErrCustomDomainAlreadyExists      = errors.New("CustomCnameAssociationFault")
	ErrEndpointAccessNotFound         = errors.New("EndpointNotFound")
	ErrEndpointAccessAlreadyExists    = errors.New("EndpointAlreadyExists")
	ErrIntegrationNotFound            = errors.New("IntegrationNotFoundFault")
	ErrIntegrationAlreadyExists       = errors.New("IntegrationAlreadyExistsFault")
	ErrIdcApplicationNotFound         = errors.New("RedshiftIdcApplicationNotExists")
	ErrIdcApplicationAlreadyExists    = errors.New("RedshiftIdcApplicationAlreadyExists")
	// Qev2IdcApplication fault codes verified against
	// aws-sdk-go-v2/service/redshift@v1.65.0/types/errors.go
	// Qev2IdcApplicationNotExistsFault.ErrorCode() and
	// Qev2IdcApplicationAlreadyExistsFault.ErrorCode() -- these are a distinct
	// fault family from RedshiftIdcApplication's above (no shared "Redshift"
	// prefix), matching Qev2IdcApplication being a distinct resource.
	ErrQev2IdcApplicationNotFound      = errors.New("Qev2IdcApplicationNotExists")
	ErrQev2IdcApplicationAlreadyExists = errors.New("Qev2IdcApplicationAlreadyExists")
)
