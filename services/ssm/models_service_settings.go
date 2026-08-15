package ssm

// UpdateServiceSettingOutput is the response for UpdateServiceSetting.
type UpdateServiceSettingOutput struct{}

// GetServiceSettingInput is the request payload.
type GetServiceSettingInput struct {
	SettingID string `json:"SettingId"`
}

// GetServiceSettingOutput is the response payload.
type GetServiceSettingOutput struct{}

// ResetServiceSettingInput is the request payload.
type ResetServiceSettingInput struct {
	SettingID string `json:"SettingId"`
}

// ResetServiceSettingOutput is the response payload.
type ResetServiceSettingOutput struct{}

// UpdateServiceSettingInput is the request payload.
type UpdateServiceSettingInput struct {
	SettingID    string `json:"SettingId"`
	SettingValue string `json:"SettingValue"`
}

// ServiceSetting represents an SSM service setting key-value pair.
//
// LastModifiedUser (real member: "The ARN of the last modified user",
// api_op_GetServiceSetting.go's ServiceSetting) is deliberately not
// modeled: this emulator has no caller-identity/SigV4-principal tracking to
// derive a real IAM ARN from (same disclosed gap class as sns's
// ConfirmSubscriptionInput.AuthenticateOnUnsubscribe), so fabricating one
// would be a made-up identity a real client could surface.
type ServiceSetting struct {
	SettingID        string  `json:"SettingId"`
	SettingValue     string  `json:"SettingValue"`
	Status           string  `json:"Status"`
	ARN              string  `json:"ARN,omitempty"`
	LastModifiedDate float64 `json:"LastModifiedDate,omitempty"`
}

// GetServiceSettingOutputFull extends the empty stub output.
type GetServiceSettingOutputFull struct {
	ServiceSetting *ServiceSetting `json:"ServiceSetting,omitempty"`
}

// ResetServiceSettingOutputFull extends the empty stub.
type ResetServiceSettingOutputFull struct {
	ServiceSetting *ServiceSetting `json:"ServiceSetting,omitempty"`
}
