package cognitoidp

// limitDefinitionWireType mirrors the AWS SDK's types.LimitDefinitionType
// (LimitClass, Attributes -- field-diffed against
// aws-sdk-go-v2/service/cognitoidentityprovider/types.LimitDefinitionType).
type limitDefinitionWireType struct {
	Attributes map[string]string `json:"Attributes,omitempty"`
	LimitClass string            `json:"LimitClass,omitempty"`
}

// limitWireType mirrors the AWS SDK's types.LimitType (LimitDefinition,
// FreeLimitValue, ProvisionedLimitValue).
type limitWireType struct {
	LimitDefinition       limitDefinitionWireType `json:"LimitDefinition"`
	FreeLimitValue        int32                   `json:"FreeLimitValue"`
	ProvisionedLimitValue int32                   `json:"ProvisionedLimitValue"`
}

type getProvisionedLimitInput struct {
	LimitDefinition limitDefinitionWireType `json:"LimitDefinition"`
}

type getProvisionedLimitOutput struct {
	Limit limitWireType `json:"Limit"`
}

type updateProvisionedLimitInput struct {
	LimitDefinition     limitDefinitionWireType `json:"LimitDefinition"`
	RequestedLimitValue int32                   `json:"RequestedLimitValue"`
}

type updateProvisionedLimitOutput struct {
	Limit limitWireType `json:"Limit"`
}
