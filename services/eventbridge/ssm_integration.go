package eventbridge

import (
	"context"
	"encoding/json"
)

// ssmParameterPolicyEventSource/DetailType are the real AWS wire values SSM
// uses for Parameter Store policy-action events, confirmed via
// https://docs.aws.amazon.com/systems-manager/latest/userguide/sysman-paramstore-cwe.html
// ("Parameter policy" event pattern example): source "aws.ssm", detail-type
// "Parameter Store Policy Action", detail {"parameter-name":...,
// "policy-type":...}.
const (
	ssmParameterPolicyEventSource     = "aws.ssm"
	ssmParameterPolicyEventDetailType = "Parameter Store Policy Action"
)

// ssmParameterPolicyEventDetail is the JSON shape of a Parameter Store
// policy-action event's "detail" field.
type ssmParameterPolicyEventDetail struct {
	ParameterName string `json:"parameter-name"`
	PolicyType    string `json:"policy-type"`
}

// NotifyParameterPolicyAction implements the services/ssm package's
// ParameterPolicyNotifier interface directly on InMemoryBackend (the same
// direct-implementation pattern as SFNPutEvents in sfn_integration.go),
// translating an SSM Parameter Store policy-action notification into a real
// PutEvents call on the default event bus.
func (b *InMemoryBackend) NotifyParameterPolicyAction(
	ctx context.Context,
	parameterName, policyType string,
) error {
	detail, err := json.Marshal(ssmParameterPolicyEventDetail{
		ParameterName: parameterName,
		PolicyType:    policyType,
	})
	if err != nil {
		return err
	}

	_, err = b.PutEvents(ctx, []EventEntry{
		{
			Source:     ssmParameterPolicyEventSource,
			DetailType: ssmParameterPolicyEventDetailType,
			Detail:     string(detail),
		},
	})

	return err
}
