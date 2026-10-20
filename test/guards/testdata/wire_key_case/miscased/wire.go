package fixture

type ecsTaskParameters struct {
	NetworkConfiguration *networkConfiguration `json:"NetworkConfiguration,omitempty"`
	TaskDefinitionArn    string                `json:"TaskDefinitionArn,omitempty"`
	LaunchType           string                `json:"LaunchType,omitempty"`
}

type networkConfiguration struct {
	AwsvpcConfiguration *awsVpcConfiguration `json:"AwsvpcConfiguration,omitempty"`
}

type awsVpcConfiguration struct {
	Subnets []string `json:"Subnets,omitempty"`
}
