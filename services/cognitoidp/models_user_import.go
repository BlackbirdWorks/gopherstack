package cognitoidp

import "time"

// UserImportJob represents a bulk user import job.
type UserImportJob struct {
	CreatedAt  time.Time `json:"createdAt"`
	JobID      string    `json:"jobID,omitempty"`
	JobName    string    `json:"jobName,omitempty"`
	UserPoolID string    `json:"userPoolID,omitempty"`
	Status     string    `json:"status,omitempty"` // Created | Pending | InProgress | Stopping ...
}

type userImportJobType struct {
	JobID      string `json:"JobId,omitempty"`
	JobName    string `json:"JobName,omitempty"`
	Status     string `json:"Status,omitempty"`
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type createUserImportJobInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	JobName    string `json:"JobName,omitempty"`
}

type createUserImportJobOutput struct {
	UserImportJob *userImportJobType `json:"UserImportJob,omitempty"`
}

type describeUserImportJobInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	JobID      string `json:"JobId,omitempty"`
}

type describeUserImportJobOutput struct {
	UserImportJob *userImportJobType `json:"UserImportJob,omitempty"`
}

type listUserImportJobsInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type listUserImportJobsOutput struct {
	UserImportJobs []userImportJobType `json:"UserImportJobs,omitempty"`
}

type startUserImportJobInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	JobID      string `json:"JobId,omitempty"`
}

type startUserImportJobOutput struct {
	UserImportJob *userImportJobType `json:"UserImportJob,omitempty"`
}

type stopUserImportJobInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	JobID      string `json:"JobId,omitempty"`
}

type stopUserImportJobOutput struct {
	UserImportJob *userImportJobType `json:"UserImportJob,omitempty"`
}

type getCSVHeaderInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type getCSVHeaderOutput struct {
	UserPoolID string   `json:"UserPoolId,omitempty"`
	CSVHeader  []string `json:"CSVHeader,omitempty"`
}
