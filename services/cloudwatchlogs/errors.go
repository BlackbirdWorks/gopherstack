package cloudwatchlogs

import "errors"

var (
	ErrLogGroupNotFound              = errors.New("ResourceNotFoundException")
	ErrLogGroupAlreadyExists         = errors.New("ResourceAlreadyExistsException")
	ErrLogStreamNotFound             = errors.New("ResourceNotFoundException")
	ErrLogStreamAlreadyExist         = errors.New("ResourceAlreadyExistsException")
	ErrSubscriptionFilterNotFound    = errors.New("ResourceNotFoundException")
	ErrSubscriptionFilterLimitExceed = errors.New("LimitExceededException")
	ErrQueryNotFound                 = errors.New("ResourceNotFoundException")
	ErrExportTaskNotFound            = errors.New("ResourceNotFoundException")
	ErrImportTaskNotFound            = errors.New("ResourceNotFoundException")
	ErrValidation                    = errors.New("InvalidParameterException")
	ErrDeliveryNotFound              = errors.New("ResourceNotFoundException")
	ErrLogAnomalyDetectorNotFound    = errors.New("ResourceNotFoundException")
	ErrScheduledQueryNotFound        = errors.New("ResourceNotFoundException")
	ErrMetricFilterNotFound          = errors.New("ResourceNotFoundException")
	ErrQueryDefinitionNotFound       = errors.New("ResourceNotFoundException")
	ErrOperationAborted              = errors.New("OperationAbortedException")
	ErrInvalidOperation              = errors.New("InvalidOperationException")
)

var (
	ErrResourcePolicyNotFound      = errors.New("ResourceNotFoundException")
	ErrDeliveryDestinationNotFound = errors.New("ResourceNotFoundException")
	ErrDeliverySourceNotFound      = errors.New("ResourceNotFoundException")
	ErrDestinationNotFound         = errors.New("ResourceNotFoundException")
	ErrIndexPolicyNotFound         = errors.New("ResourceNotFoundException")
	ErrTransformerNotFound         = errors.New("ResourceNotFoundException")
	ErrIntegrationNotFound         = errors.New("ResourceNotFoundException")
)

var (
	ErrLookupTableNotFound         = errors.New("ResourceNotFoundException")
	ErrLookupTableAlreadyExists    = errors.New("ResourceAlreadyExistsException")
	ErrSyslogConfigurationNotFound = errors.New("ResourceNotFoundException")
)
