package cognitoidp

import "time"

// UserImportJob represents a bulk user import job.
type UserImportJob struct {
	CreatedAt                time.Time `json:"createdAt"`
	StartedAt                time.Time `json:"startedAt,omitzero"`
	CompletedAt              time.Time `json:"completedAt,omitzero"`
	JobID                    string    `json:"jobID,omitempty"`
	JobName                  string    `json:"jobName,omitempty"`
	UserPoolID               string    `json:"userPoolID,omitempty"`
	Status                   string    `json:"status,omitempty"`
	CloudWatchLogsRoleArn    string    `json:"cloudWatchLogsRoleArn,omitempty"`
	PasswordHashingAlgorithm string    `json:"passwordHashingAlgorithm,omitempty"`
	PreSignedURL             string    `json:"preSignedURL,omitempty"`
}

type userImportJobType struct {
	JobID                    string  `json:"JobId,omitempty"`
	JobName                  string  `json:"JobName,omitempty"`
	Status                   string  `json:"Status,omitempty"`
	UserPoolID               string  `json:"UserPoolId,omitempty"`
	CloudWatchLogsRoleArn    string  `json:"CloudWatchLogsRoleArn,omitempty"`
	PasswordHashingAlgorithm string  `json:"PasswordHashingAlgorithm,omitempty"`
	PreSignedURL             string  `json:"PreSignedUrl,omitempty"`
	CreationDate             float64 `json:"CreationDate,omitempty"`
	StartDate                float64 `json:"StartDate,omitempty"`
	CompletionDate           float64 `json:"CompletionDate,omitempty"`
	FailedUsers              int64   `json:"FailedUsers"`
	ImportedUsers            int64   `json:"ImportedUsers"`
	SkippedUsers             int64   `json:"SkippedUsers"`
}

type createUserImportJobInput struct {
	UserPoolID               string `json:"UserPoolId,omitempty"`
	JobName                  string `json:"JobName,omitempty"`
	CloudWatchLogsRoleArn    string `json:"CloudWatchLogsRoleArn,omitempty"`
	PasswordHashingAlgorithm string `json:"PasswordHashingAlgorithm,omitempty"`
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
	UserPoolID      string `json:"UserPoolId,omitempty"`
	PaginationToken string `json:"PaginationToken,omitempty"`
	MaxResults      int    `json:"MaxResults,omitempty"`
}

type listUserImportJobsOutput struct {
	PaginationToken string              `json:"PaginationToken,omitempty"`
	UserImportJobs  []userImportJobType `json:"UserImportJobs,omitempty"`
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
