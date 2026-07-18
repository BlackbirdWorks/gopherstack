package fis

import "errors"

// ----------------------------------------
// Sentinel errors
// ----------------------------------------

// ErrTemplateNotFound is returned when an experiment template is not found.
var ErrTemplateNotFound = errors.New("ExperimentTemplateNotFound")

// ErrExperimentNotFound is returned when an experiment is not found.
var ErrExperimentNotFound = errors.New("ExperimentNotFound")

// ErrActionNotFound is returned when a FIS action is not found.
var ErrActionNotFound = errors.New("ActionNotFound")

// ErrTargetResourceTypeNotFound is returned when a target resource type is not found.
var ErrTargetResourceTypeNotFound = errors.New("TargetResourceTypeNotFound")

// ErrExperimentNotRunning is returned when trying to stop an experiment that is not running.
var ErrExperimentNotRunning = errors.New("ExperimentNotRunning")

// ErrResourceNotFound is returned when a tagged resource ARN is not known.
var ErrResourceNotFound = errors.New("ResourceNotFound")

// ErrSafetyLeverNotFound is returned when the safety lever ID does not match this account.
var ErrSafetyLeverNotFound = errors.New("SafetyLeverNotFound")

// ErrSafetyLeverEngaged is returned when StartExperiment is blocked by an engaged safety lever.
var ErrSafetyLeverEngaged = errors.New("SafetyLeverEngaged")

// ErrTooManyExperiments is returned when the experiment count would exceed the cap.
var ErrTooManyExperiments = errors.New("ServiceQuotaExceededException")

// ErrValidation is returned when a required field is missing or has an invalid value.
var ErrValidation = errors.New("ValidationException")

// ErrTooManyTags is returned when adding tags would exceed the 50-tag limit.
var ErrTooManyTags = errors.New("TooManyTagsException")

// ErrTargetAccountConfigNotFound is returned when a target account configuration is not found.
var ErrTargetAccountConfigNotFound = errors.New("TargetAccountConfigurationNotFound")
