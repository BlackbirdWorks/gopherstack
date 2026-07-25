package cognitoidp

// userPoolReplicaWireType mirrors the AWS SDK's types.UserPoolReplicaType
// (RegionName, Role, Status, UserPoolArn -- field-diffed against
// aws-sdk-go-v2/service/cognitoidentityprovider/types.UserPoolReplicaType).
type userPoolReplicaWireType struct {
	RegionName  string `json:"RegionName,omitempty"`
	Role        string `json:"Role,omitempty"`
	Status      string `json:"Status,omitempty"`
	UserPoolArn string `json:"UserPoolArn,omitempty"`
}

type createUserPoolReplicaInput struct {
	UserPoolTags map[string]string `json:"UserPoolTags,omitempty"`
	UserPoolID   string            `json:"UserPoolId,omitempty"`
	RegionName   string            `json:"RegionName,omitempty"`
}

type createUserPoolReplicaOutput struct {
	UserPoolReplica *userPoolReplicaWireType `json:"UserPoolReplica,omitempty"`
}

type deleteUserPoolReplicaInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	RegionName string `json:"RegionName,omitempty"`
}

type deleteUserPoolReplicaOutput struct {
	UserPoolReplica *userPoolReplicaWireType `json:"UserPoolReplica,omitempty"`
}

type updateUserPoolReplicaInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	RegionName string `json:"RegionName,omitempty"`
	Status     string `json:"Status,omitempty"`
}

type updateUserPoolReplicaOutput struct {
	UserPoolReplica *userPoolReplicaWireType `json:"UserPoolReplica,omitempty"`
}

type listUserPoolReplicasInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

type listUserPoolReplicasOutput struct {
	NextToken        string                    `json:"NextToken,omitempty"`
	UserPoolReplicas []userPoolReplicaWireType `json:"UserPoolReplicas,omitempty"`
}
