package app

import "time"

// DefaultCloudFormationProvisioningDelay is how long each stack step stays *_IN_PROGRESS.
const DefaultCloudFormationProvisioningDelay = 300 * time.Millisecond

// CloudFormationSettings configures CloudFormation template validation and provisioning.
type CloudFormationSettings struct {
	ProvisioningDelay  time.Duration `name:"provisioning-delay" env:"GOPHERSTACK_CFN_PROVISIONING_DELAY" default:"300ms" help:"How long each stack step stays in *_IN_PROGRESS before completing, like real CloudFormation. 0 completes stacks instantly."`                                              //nolint:lll // config struct tags are intentionally verbose
	ValidateProperties bool          `name:"validate-properties" env:"GOPHERSTACK_CLOUDFORMATION_VALIDATE_PROPERTIES" default:"true" negatable:"" help:"Reject unknown, wrong-typed and missing required resource properties at stack operations, like CloudFormation. Disable to accept any property."` //nolint:lll // config struct tags are intentionally verbose
}

// GetCloudFormationValidateProperties reports whether resource property validation is on.
func (c *CLI) GetCloudFormationValidateProperties() bool { return c.CloudFormation.ValidateProperties }

// GetCloudFormationProvisioningDelay returns the per-step *_IN_PROGRESS duration.
func (c *CLI) GetCloudFormationProvisioningDelay() time.Duration {
	return c.CloudFormation.ProvisioningDelay
}
