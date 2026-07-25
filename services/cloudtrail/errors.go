package cloudtrail

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when the requested resource does not exist.
	ErrNotFound = awserr.New("TrailNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("TrailAlreadyExistsException", awserr.ErrConflict)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)
	// ErrChannelNotFound is returned when a channel is not found.
	ErrChannelNotFound = awserr.New("ChannelNotFoundException", awserr.ErrNotFound)
	// ErrDashboardNotFound is returned when a dashboard is not found.
	ErrDashboardNotFound = awserr.New("DashboardNotFoundException", awserr.ErrNotFound)
	// ErrEventDataStoreNotFound is returned when an event data store is not found.
	ErrEventDataStoreNotFound = awserr.New("EventDataStoreNotFoundException", awserr.ErrNotFound)
	// ErrQueryIDNotFound is returned when a query ID does not exist or does not
	// map to a query (CancelQuery/DescribeQuery/GetQueryResults).
	ErrQueryIDNotFound = awserr.New("QueryIdNotFoundException", awserr.ErrNotFound)
	// ErrQueryInactive is returned when CancelQuery is called on a query that
	// is already in a terminal state (FINISHED/FAILED/TIMED_OUT/CANCELLED).
	ErrQueryInactive = awserr.New("InactiveQueryException", awserr.ErrInvalidParameter)
	// ErrTerminationProtected is returned when trying to delete a termination-protected resource.
	ErrTerminationProtected = awserr.New("EventDataStoreTerminationProtectedException", awserr.ErrConflict)
	// ErrInsightNotEnabled is returned when GetInsightSelectors is called on a trail with no
	// insight selectors configured. AWS returns InsightNotEnabledException in this case.
	ErrInsightNotEnabled = awserr.New("InsightNotEnabledException", awserr.ErrInvalidParameter)
)
