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
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("InvalidRequestException: validation error", awserr.ErrInvalidParameter)
)
