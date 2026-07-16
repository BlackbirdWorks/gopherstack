package ec2

import "errors"

// Sentinel errors consolidated from across the backend op-family files.
var (
	// Volumes / network interfaces / subnets (formerly batch1 errors).
	ErrVolumeModificationNotFound         = errors.New("InvalidVolumeModification.NotFound")
	ErrNetworkInterfacePermissionNotFound = errors.New("InvalidPermission.NotFound")
	ErrSubnetCIDRNotFound                 = errors.New(
		"InvalidSubnetCidrBlockAssociationID.NotFound",
	)
)

// Volumes / snapshots / vpc endpoints (formerly batch2 errors).
var (
	ErrEndpointConnectionNotificationNotFound = errors.New(
		"InvalidConnectionNotification.NotFound",
	)
	ErrSnapshotAlreadyLocked   = errors.New("InvalidSnapshot.AlreadyLocked")
	ErrSnapshotNotLocked       = errors.New("InvalidSnapshot.NotLocked")
	ErrReplaceRootTaskNotFound = errors.New("InvalidReplaceRootVolumeTaskID.NotFound")
)

// Instance connect/event-window/capacity (formerly batch3 errors).
var (
	ErrInstanceConnectEndpointNotFound = errors.New("InvalidInstanceConnectEndpointId.NotFound")
	ErrInstanceEventWindowNotFound     = errors.New("InvalidInstanceEventWindowId.NotFound")
	ErrCapacityReservationFull         = errors.New("CapacityReservationFull")
)

// Managed prefix lists / Client VPN / TGW connect / Verified Access (formerly batch4 errors).
var (
	ErrManagedPrefixListNotFound         = errors.New("InvalidPrefixListID.NotFound")
	ErrClientVpnEndpointNotFound         = errors.New("InvalidClientVpnEndpointId.NotFound")
	ErrTransitGatewayConnectNotFound     = errors.New("InvalidTransitGatewayAttachmentID.NotFound")
	ErrTransitGatewayConnectPeerNotFound = errors.New("InvalidTransitGatewayConnectPeerId.NotFound")
	ErrTGWPrefixListRefNotFound          = errors.New("InvalidTransitGatewayPrefixListReferenceId.NotFound")
	ErrVerifiedAccessEndpointNotFound    = errors.New("InvalidVerifiedAccessEndpointId.NotFound")
	ErrVerifiedAccessGroupNotFound       = errors.New("InvalidVerifiedAccessGroupId.NotFound")
	ErrVerifiedAccessInstanceNotFound    = errors.New("InvalidVerifiedAccessInstanceId.NotFound")
	ErrVerifiedAccessTrustProviderNF     = errors.New("InvalidVerifiedAccessTrustProviderId.NotFound")
)

// Traffic mirror / fleet / network insights / carrier gateway / reserved instances (formerly batch5).
var (
	ErrTrafficMirrorFilterNotFound      = errors.New("InvalidTrafficMirrorFilterId.NotFound")
	ErrTrafficMirrorFilterRuleNotFound  = errors.New("InvalidTrafficMirrorFilterRuleId.NotFound")
	ErrTrafficMirrorSessionNotFound     = errors.New("InvalidTrafficMirrorSessionId.NotFound")
	ErrTrafficMirrorTargetNotFound      = errors.New("InvalidTrafficMirrorTargetId.NotFound")
	ErrFleetNotFound                    = errors.New("InvalidFleetId.NotFound")
	ErrNetworkInsightsPathNotFound      = errors.New("InvalidNetworkInsightsPathId.NotFound")
	ErrNetworkInsightsAnalysisNotFound  = errors.New("InvalidNetworkInsightsAnalysisId.NotFound")
	ErrNetworkInsightsAccessScopeNF     = errors.New("InvalidNetworkInsightsAccessScopeId.NotFound")
	ErrNetworkInsightsAccessScopeAnaNF  = errors.New("InvalidNetworkInsightsAccessScopeAnalysisId.NotFound")
	ErrCarrierGatewayNotFound           = errors.New("InvalidCarrierGatewayId.NotFound")
	ErrReservedInstancesListingNotFound = errors.New("InvalidReservedInstancesListingId.NotFound")
)

// ---- Traffic Mirror ----

// TrafficMirrorFilter holds a traffic mirror filter.

// Network ACL / launch template / snapshot / vpc-attr constants (formerly refinement2).
var (
	ErrSnapshotNotFound       = errors.New("InvalidSnapshotID.NotFound")
	ErrNetworkACLNotFound     = errors.New("InvalidNetworkAclID.NotFound")
	ErrLaunchTemplateNotFound = errors.New("InvalidLaunchTemplateID.NotFound")
)

// naclDefaultDenyRuleNumber is the AWS-defined default-deny rule number placed at the end of every NACL.

// Key pairs (formerly refinement3 errors).
var (
	errR3KeyPairNotFound = errors.New("InvalidKeyPair.NotFound")
)

// TGW propagation / interruptible capacity / elastic IP (formerly parity-final errors).
var (
	// ErrTGWPropagationNotFound is returned when disabling a transit gateway
	// route table propagation that was never enabled.
	ErrTGWPropagationNotFound = errors.New("InvalidTransitGatewayRouteTablePropagation.NotFound")

	// ErrInterruptibleAllocationNotFound is returned when updating an
	// interruptible Capacity Reservation allocation that was never created.
	ErrInterruptibleAllocationNotFound = errors.New("InvalidCapacityReservationId.NotFound")

	// ErrPublicIPNotFound is returned when an Elastic IP lookup by public IP
	// address (rather than allocation ID) fails.
	ErrPublicIPNotFound = errors.New("InvalidAddress.NotFound")
)
