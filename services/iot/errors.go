package iot

import "errors"

var (
	// ErrThingNotFound is returned when a Thing does not exist.
	ErrThingNotFound = errors.New("thing not found")

	// ErrRuleNotFound is returned when a TopicRule does not exist.
	ErrRuleNotFound = errors.New("topic rule not found")

	// ErrPolicyNotFound is returned when a Policy does not exist.
	ErrPolicyNotFound = errors.New("policy not found")

	// ErrValidation is returned when an input fails validation.
	ErrValidation = errors.New("validation error")

	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = errors.New("resource already exists")

	// ErrVersionConflict is returned when an optimistic-lock version check fails.
	ErrVersionConflict = errors.New("version conflict")

	// ErrDeleteConflict is returned when a resource cannot be deleted due to dependencies.
	ErrDeleteConflict = errors.New("delete conflict")

	// ErrIndexNotFound is returned when a fleet index does not exist.
	ErrIndexNotFound = errors.New("index not found")

	// ErrVersionsLimitExceeded is returned when a policy already has the maximum allowed versions.
	ErrVersionsLimitExceeded = errors.New("versions limit exceeded")

	// ErrShadowNotFound is returned when a Device Shadow does not exist.
	ErrShadowNotFound = errors.New("shadow not found")
)

// ErrThingTypeNotFound is returned when a ThingType does not exist.
var ErrThingTypeNotFound = errors.New("thing type not found")

// ErrThingGroupNotFound is returned when a ThingGroup does not exist.
var ErrThingGroupNotFound = errors.New("thing group not found")

// ErrCertificateNotFound is returned when a Certificate does not exist.
var ErrCertificateNotFound = errors.New("certificate not found")

// ErrCertificateProviderNotFound is returned when a CertificateProvider does not exist.
var ErrCertificateProviderNotFound = errors.New("certificate provider not found")

// ErrTopicRuleDestinationNotFound is returned when a TopicRuleDestination does not exist.
var ErrTopicRuleDestinationNotFound = errors.New("topic rule destination not found")

// ErrPolicyVersionNotFound is returned when a PolicyVersion does not exist.
var ErrPolicyVersionNotFound = errors.New("policy version not found")

// ErrRegistrationTaskNotFound is returned when a bulk thing registration task does not exist.
var ErrRegistrationTaskNotFound = errors.New("thing registration task not found")

// ErrManagedJobTemplateNotFound is returned when a managed job template does not exist.
var ErrManagedJobTemplateNotFound = errors.New("managed job template not found")

// ErrResourceNotFound is returned when a new-op resource is not found.
var ErrResourceNotFound = errors.New("ResourceNotFoundException")
