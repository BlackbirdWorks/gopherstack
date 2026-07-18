package opsworks

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	errResourceNotFound = "ResourceNotFoundException"
	errValidation       = "ValidationException"
)

var (
	// ErrStackNotFound is returned when a stack does not exist.
	ErrStackNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrLayerNotFound is returned when a layer does not exist.
	ErrLayerNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrInstanceNotFound is returned when an instance does not exist.
	ErrInstanceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrAppNotFound is returned when an app does not exist.
	ErrAppNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrDeploymentNotFound is returned when a deployment does not exist.
	ErrDeploymentNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrCommandNotFound is returned when a command does not exist.
	ErrCommandNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrUserProfileNotFound is returned when a user profile does not exist.
	ErrUserProfileNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrElasticLBNotFound is returned when an ELB is not found.
	ErrElasticLBNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrElasticIPNotFound is returned when an elastic IP is not found.
	ErrElasticIPNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrVolumeNotFound is returned when a volume is not found.
	ErrVolumeNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrRdsDBInstanceNotFound is returned when an RDS DB instance is not found.
	ErrRdsDBInstanceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrEcsClusterNotFound is returned when an ECS cluster is not found.
	ErrEcsClusterNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New(errValidation, awserr.ErrInvalidParameter)
)
