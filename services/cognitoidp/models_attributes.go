package cognitoidp

import "time"

// SchemaAttribute represents a custom attribute definition for a user pool.
type SchemaAttribute struct {
	Name                     string  `json:"Name,omitempty"`
	AttributeDataType        string  `json:"AttributeDataType,omitempty"`
	StringAttributeMinLength int64   `json:"StringAttributeMinLength,omitempty"`
	StringAttributeMaxLength int64   `json:"StringAttributeMaxLength,omitempty"`
	NumberAttributeMinValue  float64 `json:"NumberAttributeMinValue,omitempty"`
	NumberAttributeMaxValue  float64 `json:"NumberAttributeMaxValue,omitempty"`
	Mutable                  bool    `json:"Mutable,omitempty"`
	Required                 bool    `json:"Required,omitempty"`
	DeveloperOnlyAttribute   bool    `json:"DeveloperOnlyAttribute,omitempty"`
}

// attrVerificationEntry stores a pending attribute verification code.
type attrVerificationEntry struct {
	ExpiresAt time.Time `json:"expiresAt"`
	Code      string    `json:"code,omitempty"`
}

type attributeType struct {
	Name  string `json:"Name,omitempty"`
	Value string `json:"Value,omitempty"`
}

type updateUserAttributesInput struct {
	AccessToken    string          `json:"AccessToken,omitempty"`
	UserAttributes []attributeType `json:"UserAttributes,omitempty"`
}

type updateUserAttributesOutput struct{}

type adminUpdateUserAttributesInput struct {
	UserPoolID     string          `json:"UserPoolId,omitempty"`
	Username       string          `json:"Username,omitempty"`
	UserAttributes []attributeType `json:"UserAttributes,omitempty"`
}

type adminUpdateUserAttributesOutput struct{}

type addCustomAttributesInput struct {
	UserPoolID       string            `json:"UserPoolId,omitempty"`
	CustomAttributes []SchemaAttribute `json:"CustomAttributes,omitempty"`
}

type addCustomAttributesOutput struct{}

type adminDeleteUserAttributesInput struct {
	UserPoolID         string   `json:"UserPoolId,omitempty"`
	Username           string   `json:"Username,omitempty"`
	UserAttributeNames []string `json:"UserAttributeNames,omitempty"`
}

type adminDeleteUserAttributesOutput struct{}

type deleteUserAttributesInput struct {
	AccessToken        string   `json:"AccessToken,omitempty"`
	UserAttributeNames []string `json:"UserAttributeNames,omitempty"`
}

type deleteUserAttributesOutput struct{}

type verifyUserAttributeInput struct {
	AccessToken   string `json:"AccessToken,omitempty"`
	AttributeName string `json:"AttributeName,omitempty"`
	Code          string `json:"Code,omitempty"`
}

type verifyUserAttributeOutput struct{}

type getUserAttributeVerifCodeFullInput struct {
	AccessToken   string `json:"AccessToken,omitempty"`
	AttributeName string `json:"AttributeName,omitempty"`
}

type getUserAttributeVerifCodeFullOutput struct {
	CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
}

type verifyUserAttributeFullInput struct {
	AccessToken   string `json:"AccessToken,omitempty"`
	AttributeName string `json:"AttributeName,omitempty"`
	Code          string `json:"Code,omitempty"`
}

type verifyUserAttributeFullOutput struct{}

type getUserAttributeVerificationCodeInput struct {
	AccessToken   string `json:"AccessToken,omitempty"`
	AttributeName string `json:"AttributeName,omitempty"`
}

type getUserAttributeVerificationCodeOutput struct {
	CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
}
