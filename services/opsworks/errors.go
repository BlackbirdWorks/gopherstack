package opsworks

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrStackNotFound is returned when a stack does not exist.
	ErrStackNotFound = awserr.New("Unable to find the requested stack", awserr.ErrNotFound)
	// ErrLayerNotFound is returned when a layer does not exist.
	ErrLayerNotFound = awserr.New("Unable to find the requested layer", awserr.ErrNotFound)
	// ErrInstanceNotFound is returned when an instance does not exist.
	ErrInstanceNotFound = awserr.New("Unable to find the requested instance", awserr.ErrNotFound)
	// ErrAppNotFound is returned when an app does not exist.
	ErrAppNotFound = awserr.New("Unable to find the requested app", awserr.ErrNotFound)
	// ErrDeploymentNotFound is returned when a deployment does not exist.
	ErrDeploymentNotFound = awserr.New("Unable to find the requested deployment", awserr.ErrNotFound)
	// ErrCommandNotFound is returned when a command does not exist.
	ErrCommandNotFound = awserr.New("Unable to find the requested command", awserr.ErrNotFound)
	// ErrUserProfileNotFound is returned when a user profile does not exist.
	ErrUserProfileNotFound = awserr.New("Unable to find the requested user profile", awserr.ErrNotFound)
	// ErrElasticLBNotFound is returned when an ELB is not found.
	ErrElasticLBNotFound = awserr.New(
		"Unable to find the requested Elastic Load Balancing instance",
		awserr.ErrNotFound,
	)
	// ErrElasticIPNotFound is returned when an elastic IP is not found.
	ErrElasticIPNotFound = awserr.New("Unable to find the requested Elastic IP", awserr.ErrNotFound)
	// ErrVolumeNotFound is returned when a volume is not found.
	ErrVolumeNotFound = awserr.New("Unable to find the requested volume", awserr.ErrNotFound)
	// ErrRdsDBInstanceNotFound is returned when an RDS DB instance is not found.
	ErrRdsDBInstanceNotFound = awserr.New("Unable to find the requested Amazon RDS DB instance", awserr.ErrNotFound)
	// ErrEcsClusterNotFound is returned when an ECS cluster is not found.
	ErrEcsClusterNotFound = awserr.New("Unable to find the requested Amazon ECS cluster", awserr.ErrNotFound)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New(
		"Validation failed: one or more request parameters are missing or invalid",
		awserr.ErrInvalidParameter,
	)
)
