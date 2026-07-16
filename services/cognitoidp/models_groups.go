package cognitoidp

import "time"

// Group represents a Cognito User Pool group.
type Group struct {
	CreatedAt      time.Time `json:"createdAt"`
	LastModifiedAt time.Time `json:"lastModifiedAt"`
	GroupName      string    `json:"groupName,omitempty"`
	UserPoolID     string    `json:"userPoolId,omitempty"`
	Description    string    `json:"description,omitempty"`
	RoleArn        string    `json:"roleArn,omitempty"`
	Precedence     int32     `json:"precedence,omitempty"`
}

type createGroupInput struct {
	UserPoolID  string `json:"UserPoolId,omitempty"`
	GroupName   string `json:"GroupName,omitempty"`
	Description string `json:"Description,omitempty"`
	Precedence  int32  `json:"Precedence,omitempty"`
}

type createGroupOutput struct {
	Group *groupSummary `json:"Group,omitempty"`
}

type groupSummary struct {
	GroupName    string  `json:"GroupName,omitempty"`
	UserPoolID   string  `json:"UserPoolId,omitempty"`
	Description  string  `json:"Description,omitempty"`
	Precedence   int32   `json:"Precedence,omitempty"`
	CreationDate float64 `json:"CreationDate,omitempty"`
}

type deleteGroupInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	GroupName  string `json:"GroupName,omitempty"`
}

type deleteGroupOutput struct{}

type listGroupsInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type listGroupsOutput struct {
	Groups []*groupSummary `json:"Groups"`
}

type adminAddUserToGroupInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
	GroupName  string `json:"GroupName,omitempty"`
}

type adminAddUserToGroupOutput struct{}

type adminRemoveUserFromGroupInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
	GroupName  string `json:"GroupName,omitempty"`
}

type adminRemoveUserFromGroupOutput struct{}

type adminListGroupsForUserInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
}

type adminListGroupsForUserOutput struct {
	Groups []*groupSummary `json:"Groups"`
}

type listUsersInGroupInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	GroupName  string `json:"GroupName,omitempty"`
}

type listUsersInGroupOutput struct {
	Users []*userSummary `json:"Users,omitempty"`
}

type updateGroupInput struct {
	UserPoolID  string `json:"UserPoolId,omitempty"`
	GroupName   string `json:"GroupName,omitempty"`
	Description string `json:"Description,omitempty"`
	Precedence  int32  `json:"Precedence,omitempty"`
}

type updateGroupOutput struct {
	Group *groupSummary `json:"Group,omitempty"`
}

type getGroupInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	GroupName  string `json:"GroupName,omitempty"`
}

type getGroupOutput struct {
	Group *groupSummary `json:"Group,omitempty"`
}

type createGroupFullInput struct {
	UserPoolID  string `json:"UserPoolId,omitempty"`
	GroupName   string `json:"GroupName,omitempty"`
	Description string `json:"Description,omitempty"`
	RoleArn     string `json:"RoleArn,omitempty"`
	Precedence  int32  `json:"Precedence,omitempty"`
}

type groupFullSummary struct {
	GroupName        string  `json:"GroupName,omitempty"`
	UserPoolID       string  `json:"UserPoolId,omitempty"`
	Description      string  `json:"Description,omitempty"`
	RoleArn          string  `json:"RoleArn,omitempty"`
	Precedence       int32   `json:"Precedence,omitempty"`
	CreationDate     float64 `json:"CreationDate,omitempty"`
	LastModifiedDate float64 `json:"LastModifiedDate,omitempty"`
}

type createGroupFullOutput struct {
	Group *groupFullSummary `json:"Group,omitempty"`
}

type updateGroupFullInput struct {
	UserPoolID  string `json:"UserPoolId,omitempty"`
	GroupName   string `json:"GroupName,omitempty"`
	Description string `json:"Description,omitempty"`
	RoleArn     string `json:"RoleArn,omitempty"`
	Precedence  int32  `json:"Precedence,omitempty"`
}

type updateGroupFullOutput struct {
	Group *groupFullSummary `json:"Group,omitempty"`
}

type getGroupFullInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	GroupName  string `json:"GroupName,omitempty"`
}

type getGroupFullOutput struct {
	Group *groupFullSummary `json:"Group,omitempty"`
}

type listGroupsFullInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	Limit      int    `json:"Limit,omitempty"`
}

type listGroupsFullOutput struct {
	NextToken string              `json:"NextToken,omitempty"`
	Groups    []*groupFullSummary `json:"Groups"`
}

type listUsersInGroupFullInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	GroupName  string `json:"GroupName,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	Limit      int    `json:"Limit,omitempty"`
}

type listUsersInGroupFullOutput struct {
	NextToken string          `json:"NextToken,omitempty"`
	Users     []adminUserJSON `json:"Users,omitempty"`
}
