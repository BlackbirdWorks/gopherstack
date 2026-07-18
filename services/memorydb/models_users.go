package memorydb

import (
	"time"
)

// User represents an in-memory MemoryDB user.
type User struct {
	CreatedAt    time.Time         `json:"createdAt"`
	Tags         map[string]string `json:"tags"`
	ARN          string            `json:"arn"`
	Name         string            `json:"name"`
	Engine       string            `json:"engine"`
	AccessString string            `json:"accessString"`
	Status       string            `json:"status"`
	AuthType     string            `json:"authType"`
	Passwords    []string          `json:"passwords"`
}

type createUserRequest struct {
	AuthenticationMode authenticationModeReq `json:"AuthenticationMode"`
	UserName           string                `json:"UserName"`
	AccessString       string                `json:"AccessString"`
	Tags               []tagEntry            `json:"Tags,omitempty"`
}

type authenticationModeReq struct {
	Type      string   `json:"Type"`
	Passwords []string `json:"Passwords,omitempty"`
}

type describeUserRequest struct {
	MaxResults *int32 `json:"MaxResults,omitempty"`
	UserName   string `json:"UserName,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

type deleteUserRequest struct {
	UserName string `json:"UserName"`
}

type updateUserRequest struct {
	AuthenticationMode *authenticationModeReq `json:"AuthenticationMode,omitempty"`
	UserName           string                 `json:"UserName"`
	AccessString       string                 `json:"AccessString,omitempty"`
}

// -- Parameter group request types -----------------------------------------------

type authenticationObject struct {
	Type          string `json:"Type,omitempty"`
	PasswordCount int32  `json:"PasswordCount,omitempty"`
}

type userObject struct {
	Authentication       *authenticationObject `json:"Authentication,omitempty"`
	ARN                  string                `json:"ARN,omitempty"`
	Name                 string                `json:"Name,omitempty"`
	AccessString         string                `json:"AccessString,omitempty"`
	Status               string                `json:"Status,omitempty"`
	Engine               string                `json:"Engine,omitempty"`
	MinimumEngineVersion string                `json:"MinimumEngineVersion,omitempty"`
	ACLNames             []string              `json:"ACLNames"`
}

// createUserResponse is the response for CreateUser.
type createUserResponse struct {
	User userObject `json:"User"`
}

// describeUserResponse is the response for DescribeUsers.
type describeUserResponse struct {
	NextToken string       `json:"NextToken,omitempty"`
	Users     []userObject `json:"Users"`
}

// updateUserResponse is the response for UpdateUser.
type updateUserResponse struct {
	User userObject `json:"User"`
}

// deleteUserResponse is the response for DeleteUser.
type deleteUserResponse struct {
	User userObject `json:"User"`
}
