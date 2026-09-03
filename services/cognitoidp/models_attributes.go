package cognitoidp

import "time"

// SchemaAttribute represents a custom attribute definition for a user pool.
//
// StringAttributeConstraints and NumberAttributeConstraints are nested
// sub-objects on the wire, not flattened top-level fields, and their bounds
// are string-valued even though semantically numeric -- see
// awsAwsjson11_deserializeDocumentSchemaAttributeType and
// ...NumberAttributeConstraintsType/...StringAttributeConstraintsType in
// cognitoidentityprovider@v1.67.4's deserializers.go.
type SchemaAttribute struct {
	StringAttributeConstraints *stringAttributeConstraintsJSON `json:"StringAttributeConstraints,omitempty"`
	NumberAttributeConstraints *numberAttributeConstraintsJSON `json:"NumberAttributeConstraints,omitempty"`
	Name                       string                          `json:"Name,omitempty"`
	AttributeDataType          string                          `json:"AttributeDataType,omitempty"`
	Mutable                    bool                            `json:"Mutable,omitempty"`
	Required                   bool                            `json:"Required,omitempty"`
	DeveloperOnlyAttribute     bool                            `json:"DeveloperOnlyAttribute,omitempty"`
}

// numberAttributeConstraintsJSON mirrors NumberAttributeConstraintsType: MinValue/MaxValue
// are strings on the wire (serializers.go:8993, deserializers.go:24065).
type numberAttributeConstraintsJSON struct {
	MinValue string `json:"MinValue,omitempty"`
	MaxValue string `json:"MaxValue,omitempty"`
}

// stringAttributeConstraintsJSON mirrors StringAttributeConstraintsType: MinLength/MaxLength
// are strings on the wire (serializers.go:9437, deserializers.go:25745).
type stringAttributeConstraintsJSON struct {
	MinLength string `json:"MinLength,omitempty"`
	MaxLength string `json:"MaxLength,omitempty"`
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
