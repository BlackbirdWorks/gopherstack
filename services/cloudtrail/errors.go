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
	// ErrQueryNotFound is returned when a query is not found.
	ErrQueryNotFound = awserr.New("InactiveQueryException", awserr.ErrNotFound)
	// ErrTerminationProtected is returned when trying to delete a termination-protected resource.
	ErrTerminationProtected = awserr.New("EventDataStoreTerminationProtectedException", awserr.ErrConflict)
	// ErrInsightNotEnabled is returned when GetInsightSelectors is called on a trail with no
	// insight selectors configured. AWS returns InsightNotEnabledException in this case.
	ErrInsightNotEnabled = awserr.New("InsightNotEnabledException", awserr.ErrInvalidParameter)
)
