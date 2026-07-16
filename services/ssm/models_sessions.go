package ssm

// DescribeSessionsInput is the request payload.
type DescribeSessionsInput struct {
	State string `json:"State,omitempty"`
}

// DescribeSessionsOutput is the response payload.
type DescribeSessionsOutput struct{}

// GetAccessTokenInput is the request payload.
type GetAccessTokenInput struct{}

// GetAccessTokenOutput is the response payload.
type GetAccessTokenOutput struct{}

// GetConnectionStatusInput is the request payload.
type GetConnectionStatusInput struct {
	Target string `json:"Target"`
}

// GetConnectionStatusOutput is the response payload.
type GetConnectionStatusOutput struct{}

// ResumeSessionInput is the request payload.
type ResumeSessionInput struct {
	SessionID string `json:"SessionId"`
}

// ResumeSessionOutput is the response payload.
type ResumeSessionOutput struct{}

// StartAccessRequestInput is the request payload.
type StartAccessRequestInput struct{}

// StartAccessRequestOutput is the response payload.
type StartAccessRequestOutput struct{}

// StartSessionInput is the request payload.
type StartSessionInput struct {
	Parameters              map[string][]string `json:"Parameters,omitempty"`
	Target                  string              `json:"Target"`
	DocumentName            string              `json:"DocumentName,omitempty"`
	Reason                  string              `json:"Reason,omitempty"`
	OutputS3BucketName      string              `json:"OutputS3BucketName,omitempty"`
	OutputS3KeyPrefix       string              `json:"OutputS3KeyPrefix,omitempty"`
	CloudWatchLogGroupName  string              `json:"CloudWatchLogGroupName,omitempty"`
	CloudWatchOutputEnabled bool                `json:"CloudWatchOutputEnabled,omitempty"`
}

// StartSessionOutput is the response payload.
type StartSessionOutput struct {
	SessionID  string `json:"SessionId"`
	StreamURL  string `json:"StreamUrl"`
	TokenValue string `json:"TokenValue"`
}

// TerminateSessionInput is the request payload.
type TerminateSessionInput struct {
	SessionID string `json:"SessionId"`
}

// TerminateSessionOutput is the response payload.
type TerminateSessionOutput struct {
	SessionID string `json:"SessionId"`
}

// SessionOutputS3 holds S3 output configuration for a session.
type SessionOutputS3 struct {
	S3BucketName string `json:"S3BucketName,omitempty"`
	S3KeyPrefix  string `json:"S3KeyPrefix,omitempty"`
}

// Session represents an SSM Session Manager session.
type Session struct {
	OutputURL               *SessionOutputS3    `json:"OutputUrl,omitempty"`
	Parameters              map[string][]string `json:"Parameters,omitempty"`
	SessionID               string              `json:"SessionId"`
	Target                  string              `json:"Target"`
	Status                  string              `json:"Status"`
	StreamURL               string              `json:"StreamUrl"`
	TokenValue              string              `json:"TokenValue"`
	Owner                   string              `json:"Owner,omitempty"`
	Reason                  string              `json:"Reason,omitempty"`
	DocumentName            string              `json:"DocumentName,omitempty"`
	CloudWatchLogGroupName  string              `json:"CloudWatchLogGroupName,omitempty"`
	StartDate               float64             `json:"StartDate"`
	EndDate                 float64             `json:"EndDate,omitempty"`
	CloudWatchOutputEnabled bool                `json:"CloudWatchOutputEnabled,omitempty"`
}

// DescribeSessionsOutputFull extends the empty stub output.
type DescribeSessionsOutputFull struct {
	NextToken string    `json:"NextToken,omitempty"`
	Sessions  []Session `json:"Sessions"`
}

// GetConnectionStatusOutputFull has a status field.
type GetConnectionStatusOutputFull struct {
	Target string `json:"Target"`
	Status string `json:"Status"`
}

// GetAccessTokenOutputFull extends the empty stub.
type GetAccessTokenOutputFull struct {
	TokenValue      string `json:"TokenValue"`
	AccessRequestID string `json:"AccessRequestId,omitempty"`
}

// StartAccessRequestOutputFull extends the empty stub.
type StartAccessRequestOutputFull struct {
	AccessRequestID string `json:"AccessRequestId"`
}

// ResumeSessionOutputFull extends the empty stub.
type ResumeSessionOutputFull struct {
	SessionID  string `json:"SessionId"`
	StreamURL  string `json:"StreamUrl"`
	TokenValue string `json:"TokenValue"`
}
