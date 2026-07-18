package iotdataplane

import (
	"errors"
)

// ErrNoBroker is returned when no MQTT broker has been wired.
var ErrNoBroker = errors.New("no mqtt broker configured")

// ErrShadowNotFound is returned when a thing shadow is not found.
var ErrShadowNotFound = errors.New("shadow not found")

// ErrRetainedMessageNotFound is returned when no retained message exists for a topic.
var ErrRetainedMessageNotFound = errors.New("retained message not found")

// ErrVersionConflict is returned when a shadow update specifies a version
// that does not match the current shadow version (optimistic locking violation).
// The wire error code is "ConflictException" (real AWS iotdataplane exception
// name; see aws-sdk-go-v2/service/iotdataplane/types.ConflictException) --
// there is no "VersionConflictException" in the real API.
var ErrVersionConflict = errors.New("ConflictException")

// ErrValidation is returned for invalid input parameters.
var ErrValidation = errors.New("InvalidRequestException")

// ErrRequestTooLarge is returned when a shadow document exceeds the maximum
// allowed size. Wire error code "RequestEntityTooLargeException" (real AWS
// iotdataplane exception, modeled only for UpdateThingShadow).
var ErrRequestTooLarge = errors.New("RequestEntityTooLargeException")

// ErrConnectionExists is returned when trying to register a clientID that is already connected.
var ErrConnectionExists = errors.New("connection already exists")
