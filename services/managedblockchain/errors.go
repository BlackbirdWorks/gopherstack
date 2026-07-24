package managedblockchain

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNetworkNotFound is returned when a network does not exist.
	ErrNetworkNotFound = awserr.New("ResourceNotFoundException: network not found", awserr.ErrNotFound)
	// ErrMemberNotFound is returned when a member does not exist.
	ErrMemberNotFound = awserr.New("ResourceNotFoundException: member not found", awserr.ErrNotFound)
	// ErrResourceNotFound is returned when a resource (network or member) cannot be found by ARN.
	ErrResourceNotFound = awserr.New("ResourceNotFoundException: resource not found", awserr.ErrNotFound)
	// ErrNetworkAlreadyExists is returned when a network already exists.
	ErrNetworkAlreadyExists = awserr.New(
		"ResourceAlreadyExistsException: network already exists",
		awserr.ErrAlreadyExists,
	)
	// ErrMissingNetworkName is returned when the network name is missing.
	ErrMissingNetworkName = errors.New("Name is required for CreateNetwork")
	// ErrMissingMemberName is returned when the member name is missing.
	ErrMissingMemberName = errors.New("Name is required for member configuration")
	// ErrMissingNetworkID is returned when the network ID is missing from a path.
	ErrMissingNetworkID = errors.New("networkId is required")
	// ErrNodeNotFound is returned when a node does not exist.
	ErrNodeNotFound = awserr.New("ResourceNotFoundException: node not found", awserr.ErrNotFound)
	// ErrAccessorNotFound is returned when an accessor does not exist.
	ErrAccessorNotFound = awserr.New("ResourceNotFoundException: accessor not found", awserr.ErrNotFound)
	// ErrProposalNotFound is returned when a proposal does not exist.
	ErrProposalNotFound = awserr.New("ResourceNotFoundException: proposal not found", awserr.ErrNotFound)
	// ErrInvitationNotFound is returned when an invitation does not exist.
	ErrInvitationNotFound = awserr.New("ResourceNotFoundException: invitation not found", awserr.ErrNotFound)
	// ErrMissingMemberID is returned when the member ID is missing for a proposal.
	ErrMissingMemberID = errors.New("MemberId is required for CreateProposal")
	// ErrMissingVoterMemberID is returned when the voter member ID is missing for VoteOnProposal.
	ErrMissingVoterMemberID = errors.New("VoterMemberId is required for VoteOnProposal")
	// ErrMissingNodeMemberID is returned when MemberId is missing for a node operation. Real AWS
	// documents MemberId as "required for Hyperledger Fabric" on every node op (CreateNode's body
	// field, GetNode/ListNodes/DeleteNode/UpdateNode's "memberId" query parameter); gopherstack
	// only emulates Hyperledger Fabric networks, so it is always required here.
	ErrMissingNodeMemberID = errors.New("MemberId is required for Hyperledger Fabric node operations")
	// ErrMissingMemberFrameworkConfig is returned when MemberConfiguration.FrameworkConfiguration
	// is missing. The real aws-sdk-go-v2 client-side validator (validateMemberConfiguration)
	// requires this field on both CreateNetwork's and CreateMember's MemberConfiguration.
	ErrMissingMemberFrameworkConfig = errors.New("FrameworkConfiguration is required for member configuration")
	// ErrMissingMemberFabricConfig is returned when MemberConfiguration.FrameworkConfiguration.Fabric
	// is missing. gopherstack only emulates Hyperledger Fabric networks (see ErrMissingNodeMemberID),
	// so unlike the real API (which permits an empty FrameworkConfiguration for future frameworks),
	// Fabric is always required here.
	ErrMissingMemberFabricConfig = errors.New(
		"FrameworkConfiguration.Fabric is required for Hyperledger Fabric member configuration",
	)
	// ErrMissingMemberAdminUsername is returned when Fabric.AdminUsername is missing. Real AWS's
	// client-side validator (validateMemberFabricConfiguration) requires this field.
	ErrMissingMemberAdminUsername = errors.New("AdminUsername is required for member's Fabric configuration")
	// ErrMissingMemberAdminPassword is returned when Fabric.AdminPassword is missing. Real AWS's
	// client-side validator (validateMemberFabricConfiguration) requires this field.
	ErrMissingMemberAdminPassword = errors.New("AdminPassword is required for member's Fabric configuration")
	// ErrInvalidMemberAdminPassword is returned when Fabric.AdminPassword does not meet the real
	// API's documented length constraint (8-32 characters).
	ErrInvalidMemberAdminPassword = errors.New("AdminPassword must be between 8 and 32 characters")
	// ErrMissingNetworkFabricEdition is returned when FrameworkConfiguration.Fabric is present but
	// Edition is missing. Real AWS's client-side validator (validateNetworkFabricConfiguration)
	// requires this field whenever a Fabric configuration object is supplied.
	ErrMissingNetworkFabricEdition = errors.New(
		"FrameworkConfiguration.Fabric.Edition is required when Fabric is specified",
	)
	// ErrUnsupportedNetworkFramework is returned when Framework is set to a value other than
	// HYPERLEDGER_FABRIC. Real AWS's CreateNetwork documents itself as "Applies only to
	// Hyperledger Fabric" -- new networks (including ETHEREUM, still a valid Framework enum
	// member used elsewhere, e.g. accessors) can no longer be created through this operation.
	ErrUnsupportedNetworkFramework = errors.New("CreateNetworkInput.Framework must be HYPERLEDGER_FABRIC")
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("InvalidRequestException: validation error", awserr.ErrInvalidParameter)
)
