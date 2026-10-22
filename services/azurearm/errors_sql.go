package azurearm

import "errors"

// Sentinel errors of the Microsoft.Sql provider.
var (
	ErrSQLServerNotFound   = errors.New("azurearm: sql server not found")
	ErrSQLChildNotFound    = errors.New("azurearm: sql resource not found")
	ErrSQLNameTaken        = errors.New("azurearm: sql server name is already in use")
	ErrSQLInvalid          = errors.New("azurearm: invalid sql request")
	ErrSQLPasswordNotValid = errors.New("azurearm: sql password does not meet the policy")
	ErrSQLEngine           = errors.New("azurearm: sql engine failure")
	ErrSQLConflict         = errors.New("azurearm: sql resource is in use")
)
