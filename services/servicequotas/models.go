package servicequotas

import "time"

// Request statuses of a quota increase request.
const (
	statusPending     = "PENDING"
	statusCaseOpened  = "CASE_OPENED"
	statusApproved    = "APPROVED"
	statusNotApproved = "NOT_APPROVED"
	statusDenied      = "DENIED"
)

// Utilization report statuses.
const (
	reportPending    = "PENDING"
	reportInProgress = "IN_PROGRESS"
	reportCompleted  = "COMPLETED"
)

const (
	levelAccount  = "ACCOUNT"
	levelResource = "RESOURCE"
	levelAll      = "ALL"

	assocAssociated    = "ASSOCIATED"
	assocDisassociated = "DISASSOCIATED"

	optInEnabled  = "ENABLED"
	optInDisabled = "DISABLED"
)

// Request is a quota increase request.
type Request struct {
	Created      time.Time `json:"created"`
	LastUpdated  time.Time `json:"lastUpdated"`
	CaseOpenedAt time.Time `json:"caseOpenedAt"`
	ID           string    `json:"id"`
	Account      string    `json:"account"`
	Region       string    `json:"region"`
	ServiceCode  string    `json:"serviceCode"`
	QuotaCode    string    `json:"quotaCode"`
	CaseID       string    `json:"caseId,omitempty"`
	Status       string    `json:"status"`
	Requester    string    `json:"requester"`
	DesiredValue float64   `json:"desiredValue"`
	Seq          int64     `json:"seq"`
	CaseAllowed  bool      `json:"caseAllowed"`
}

// AppliedValue overrides a quota's default for one account and Region.
type AppliedValue struct {
	Key         string  `json:"key"`
	Account     string  `json:"account"`
	Region      string  `json:"region"`
	ServiceCode string  `json:"serviceCode"`
	QuotaCode   string  `json:"quotaCode"`
	Value       float64 `json:"value"`
}

// TemplateItem is an entry of the quota request template.
type TemplateItem struct {
	Key          string  `json:"key"`
	Account      string  `json:"account"`
	AwsRegion    string  `json:"awsRegion"`
	ServiceCode  string  `json:"serviceCode"`
	QuotaCode    string  `json:"quotaCode"`
	DesiredValue float64 `json:"desiredValue"`
}

// Association records whether an account's quota request template is associated with its organization.
type Association struct {
	Account string `json:"account"`
	Status  string `json:"status"`
}

// UtilizationEntry is one quota line of a utilization report.
type UtilizationEntry struct {
	ServiceCode string  `json:"serviceCode"`
	QuotaCode   string  `json:"quotaCode"`
	Namespace   string  `json:"namespace,omitempty"`
	Applied     float64 `json:"applied"`
	Default     float64 `json:"default"`
	Utilization float64 `json:"utilization"`
	Adjustable  bool    `json:"adjustable"`
}

// Report is a quota utilization report.
type Report struct {
	Created   time.Time          `json:"created"`
	Generated time.Time          `json:"generated"`
	ID        string             `json:"id"`
	Account   string             `json:"account"`
	Region    string             `json:"region"`
	Status    string             `json:"status"`
	Quotas    []UtilizationEntry `json:"quotas,omitempty"`
	Seq       int64              `json:"seq"`
}

// AutoManagement is the automatic quota management configuration of an account in a Region.
type AutoManagement struct {
	Exclusions      map[string][]string `json:"exclusions,omitempty"`
	Key             string              `json:"key"`
	Account         string              `json:"account"`
	Region          string              `json:"region"`
	OptInLevel      string              `json:"optInLevel"`
	OptInType       string              `json:"optInType"`
	Status          string              `json:"status"`
	NotificationArn string              `json:"notificationArn,omitempty"`
}

// ResourceTags holds the tags of one applied quota.
type ResourceTags struct {
	Tags map[string]string `json:"tags"`
	ARN  string            `json:"arn"`
}
