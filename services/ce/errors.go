package ce

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the same name already exists.
	ErrAlreadyExists = awserr.New("ServiceQuotaExceededException", awserr.ErrConflict)
	// ErrValidation is returned when input parameters fail validation. Real AWS CE's
	// documented common-error type for malformed/missing-required-member requests is
	// "ValidationError" (confirmed via the CE API reference's CommonErrors page), not a
	// named per-operation exception -- CE's errors.go typed-exception list (checked
	// against aws-sdk-go-v2/service/costexplorer/types) has no ValidationException or
	// InvalidParameterException entry for any operation in this audit.
	ErrValidation = errors.New("ValidationError")
	// ErrDataUnavailable is returned when queried data is not available for the time range.
	ErrDataUnavailable = awserr.New("DataUnavailableException", awserr.ErrNotFound)
	// ErrUnknownMonitor is returned when a referenced cost anomaly monitor ARN does not
	// exist. Real AWS CE returns this (not the generic ResourceNotFoundException) from
	// every anomaly-monitor op and from any op whose MonitorArnList references a monitor
	// that doesn't exist.
	ErrUnknownMonitor = awserr.New("UnknownMonitorException", awserr.ErrNotFound)
	// ErrUnknownSubscription is returned when a referenced cost anomaly subscription ARN
	// does not exist. Real AWS CE returns this (not the generic ResourceNotFoundException)
	// from every anomaly-subscription op.
	ErrUnknownSubscription = awserr.New("UnknownSubscriptionException", awserr.ErrNotFound)
)
