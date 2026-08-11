package ssm

// SessionFilter filters DescribeSessions results. Wire key casing ("key"/
// "value", lowercase) is a deliberate AWS quirk confirmed against
// aws-sdk-go-v2/service/ssm@v1.73.4's serializers.go
// (awsAwsjson11_serializeDocumentSessionFilter) — every other SSM shape uses
// PascalCase field names, but SessionFilter does not.
type SessionFilter struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// DescribeSessionsInput is the request payload.
type DescribeSessionsInput struct {
	State      string          `json:"State"`
	NextToken  string          `json:"NextToken,omitempty"`
	Filters    []SessionFilter `json:"Filters,omitempty"`
	MaxResults int32           `json:"MaxResults,omitempty"`
}

// DescribeSessionsOutput is the response payload.
type DescribeSessionsOutput struct{}

// GetAccessTokenInput is the request payload.
type GetAccessTokenInput struct {
	AccessRequestID string `json:"AccessRequestId"`
}

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

// AccessRequestTarget names a managed node targeted by a just-in-time access
// request. Matches the SDK's generic types.Target{Key,Values} shape.
type AccessRequestTarget struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// StartAccessRequestInput is the request payload.
type StartAccessRequestInput struct {
	Reason  string                `json:"Reason"`
	Targets []AccessRequestTarget `json:"Targets"`
	Tags    []Tag                 `json:"Tags,omitempty"`
}

// StartAccessRequestOutput is the response payload.
type StartAccessRequestOutput struct{}

// StartSessionInput is the request payload.
type StartSessionInput struct {
	Parameters   map[string][]string `json:"Parameters,omitempty"`
	Target       string              `json:"Target"`
	DocumentName string              `json:"DocumentName,omitempty"`
	Reason       string              `json:"Reason,omitempty"`
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

// Session represents an SSM Session Manager session.
//
// OutputUrl (types.SessionManagerOutputUrl, members CloudWatchOutputUrl/
// S3OutputUrl) and Details are both documented "Reserved for future use" in
// aws-sdk-go-v2/service/ssm@v1.73.4's types/types.go — real AWS never
// populates them today, and StartSessionInput has no field that could drive
// them (it only accepts Target/DocumentName/Parameters/Reason). Both are
// therefore omitted here entirely rather than modeled with a
// perpetually-empty struct.
type Session struct {
	Parameters         map[string][]string `json:"Parameters,omitempty"`
	SessionID          string              `json:"SessionId"`
	Target             string              `json:"Target"`
	Status             string              `json:"Status"`
	StreamURL          string              `json:"StreamUrl"`
	TokenValue         string              `json:"TokenValue,omitempty"`
	Owner              string              `json:"Owner,omitempty"`
	Reason             string              `json:"Reason,omitempty"`
	DocumentName       string              `json:"DocumentName,omitempty"`
	AccessType         string              `json:"AccessType,omitempty"`
	MaxSessionDuration string              `json:"MaxSessionDuration,omitempty"`
	StartDate          float64             `json:"StartDate"`
	EndDate            float64             `json:"EndDate,omitempty"`
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

// Credentials mirrors the SDK's types.Credentials (temporary security
// credentials returned for an approved just-in-time access request).
type Credentials struct {
	AccessKeyID     string  `json:"AccessKeyId"`
	SecretAccessKey string  `json:"SecretAccessKey"`
	SessionToken    string  `json:"SessionToken"`
	ExpirationTime  float64 `json:"ExpirationTime"`
}

// GetAccessTokenOutputFull extends the empty stub.
type GetAccessTokenOutputFull struct {
	Credentials         *Credentials `json:"Credentials,omitempty"`
	AccessRequestStatus string       `json:"AccessRequestStatus"`
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

// AccessRequest is a stored just-in-time node access request created by
// StartAccessRequest and consumed by GetAccessToken. Real AWS routes
// approval through configured approvers; this emulator auto-approves every
// request immediately since no approval workflow exists to model, and
// documents that choice rather than leaving GetAccessToken as a dead end.
type AccessRequest struct {
	AccessRequestID string                `json:"AccessRequestId"`
	Reason          string                `json:"Reason"`
	Status          string                `json:"Status"`
	Targets         []AccessRequestTarget `json:"Targets"`
	CreatedAt       float64               `json:"CreatedAt"`
}
