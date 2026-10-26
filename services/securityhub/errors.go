package securityhub

import "errors"

var (
	ErrHubNotEnabled    = errors.New("SecurityHub is not enabled")
	ErrHubAlreadyExists = errors.New("SecurityHub is already enabled")
	ErrNotFound         = errors.New("not found")
	ErrInvalidInput     = errors.New("invalid input")
	ErrAlreadyExists    = errors.New("resource already exists")
	// ErrExportJobRunning is StartExportJobV2's one-RUNNING-job-per-account quota.
	ErrExportJobRunning = errors.New("an export job is already running")
	// ErrExportJobFinished is CancelExportJobV2 on a SUCCEEDED or FAILED job.
	ErrExportJobFinished = errors.New("export job already finished")
	// ErrHubIsAdministrator reports the precondition DisableSecurityHub's doc
	// comment (api_op_DisableSecurityHub.go) states: an account can't
	// disable Security Hub CSPM while it is currently the Security Hub CSPM
	// administrator.
	ErrHubIsAdministrator = errors.New("account is currently the Security Hub administrator")
)
