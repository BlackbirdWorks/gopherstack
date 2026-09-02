package stepfunctions

import (
	"errors"
)

var (
	ErrStateMachineAlreadyExists       = errors.New("StateMachineAlreadyExists")
	ErrStateMachineDoesNotExist        = errors.New("StateMachineDoesNotExist")
	ErrStateMachineVersionDoesNotExist = errors.New("StateMachineVersionDoesNotExist")
	ErrStateMachineAliasAlreadyExists  = errors.New("StateMachineAliasAlreadyExists")
	ErrStateMachineAliasDoesNotExist   = errors.New("StateMachineAliasDoesNotExist")
	ErrExecutionAlreadyExists          = errors.New("ExecutionAlreadyExists")
	ErrExecutionDoesNotExist           = errors.New("ExecutionDoesNotExist")
	ErrExecutionNotRedrivable          = errors.New("ExecutionNotRedrivable")
	ErrInvalidDefinition               = errors.New("InvalidDefinition")
	ErrInvalidExecutionType            = errors.New("InvalidExecutionType")
	ErrStateMachineTypeNotSupported    = errors.New("StateMachineTypeNotSupported")
	ErrInvalidRoleArn                  = errors.New("InvalidArn")
	ErrInvalidName                     = errors.New("InvalidName")
	ErrInvalidRoutingConfiguration     = errors.New("InvalidRoutingConfiguration")
	ErrTagPolicyViolation              = errors.New("TagPolicyViolation")
	ErrActivityAlreadyExists           = errors.New("ActivityAlreadyExists")
	ErrActivityDoesNotExist            = errors.New("ActivityDoesNotExist")
	ErrTaskTokenNotFound               = errors.New("TaskTokenNotFound")
	ErrTaskTokenAlreadyExists          = errors.New("TaskTokenAlreadyExists")
	ErrActivityTaskFailed              = errors.New("ActivityTaskFailed")
	ErrHeartbeatTimeout                = errors.New("States.HeartbeatTimeout")
	ErrInvalidExecutionInput           = errors.New("InvalidExecutionInput")
	ErrValidation                      = errors.New("ValidationException")
	ErrMapRunDoesNotExist              = errors.New("MapRunDoesNotExist")
	ErrTooManyTags                     = errors.New("TooManyTags")
)
