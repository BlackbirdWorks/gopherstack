package app

// CloudFormationSettings configures CloudFormation template validation.
type CloudFormationSettings struct {
	ValidateProperties bool `name:"validate-properties" env:"GOPHERSTACK_CLOUDFORMATION_VALIDATE_PROPERTIES" default:"true" negatable:"" help:"Reject unknown, wrong-typed and missing required resource properties at stack operations, like CloudFormation. Disable to accept any property."` //nolint:lll // config struct tags are intentionally verbose
}

// GetCloudFormationValidateProperties reports whether resource property validation is on.
func (c *CLI) GetCloudFormationValidateProperties() bool { return c.CloudFormation.ValidateProperties }
