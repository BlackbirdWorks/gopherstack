package cognitoidp

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/cognitoidp/internal/esdk"
)

// DataKey is a KMS-generated data key for an AWS Encryption SDK message.
type DataKey = esdk.DataKey

const cognitoUserPoolECID = "userpool-id"

// ErrInvalidEmailRoleAccessPolicy is returned when the pool's SES SourceArn identity cannot be used.
var ErrInvalidEmailRoleAccessPolicy = awserr.New("InvalidEmailRoleAccessPolicyException", awserr.ErrInvalidParameter)

const (
	triggerKeyCustomEmailSender = "CustomEmailSender"
	triggerKeyCustomSMSSender   = "CustomSMSSender"
	lambdaConfigKMSKeyID        = "KMSKeyID"
	customSenderTypeKey         = "type"
	customSenderEmailType       = "customEmailSenderRequestV1"
	customSenderSMSType         = "customSMSSenderRequestV1"
)

// Message flows, the suffix of the CustomEmailSender_/CustomSMSSender_ trigger source.
const (
	FlowSignUp          = "SignUp"
	FlowResendCode      = "ResendCode"
	FlowForgotPassword  = "ForgotPassword"
	FlowUpdateAttribute = "UpdateUserAttribute"
	FlowVerifyAttribute = "VerifyUserAttribute"
	FlowAdminCreateUser = "AdminCreateUser"
	FlowAuthentication  = "Authentication"
)

// DataKeyGenerator is the KMS GenerateDataKey call the AWS Encryption SDK message needs.
type DataKeyGenerator interface {
	GenerateDataKey(ctx context.Context, keyID string, encCtx map[string]string) (*DataKey, error)
}

// SetDataKeyGenerator wires the KMS-backed generator used for custom sender triggers.
func (b *InMemoryBackend) SetDataKeyGenerator(g DataKeyGenerator) {
	b.mu.Lock("SetDataKeyGenerator")
	defer b.mu.Unlock()

	b.dataKeys = g
}

// customSender is a rendered CustomEmailSender/CustomSMSSender invocation, minus the encrypted code.
type customSender struct {
	inv         LambdaTriggerInvoker
	enc         DataKeyGenerator
	event       map[string]any
	functionARN string
	keyID       string
	poolID      string
}

func (b *InMemoryBackend) customSenderFor(
	pool *UserPool, user *User, req *MessageRequest, medium string,
) *customSender {
	if b.lambdaInvoker == nil || req.Flow == "" {
		return nil
	}

	key, prefix, reqType := triggerKeyCustomEmailSender, "CustomEmailSender_", customSenderEmailType
	if medium == mediumSMSName {
		key, prefix, reqType = triggerKeyCustomSMSSender, "CustomSMSSender_", customSenderSMSType
	}

	call := b.prepareTrigger(pool, key, prefix+req.Flow, "", user.Username,
		map[string]any{
			customSenderTypeKey:    reqType,
			"clientMetadata":       stringMapToAny(req.ClientMetadata),
			eventKeyUserAttributes: stringMapToAny(user.Attributes),
		}, map[string]any{})
	if call == nil {
		return nil
	}

	keyID, _ := pool.LambdaConfig[lambdaConfigKMSKeyID].(string)

	return &customSender{
		inv: call.inv, enc: b.dataKeys, event: call.event, functionARN: call.functionARN, keyID: keyID, poolID: pool.ID,
	}
}

func (c *customSender) send(ctx context.Context, code string) error {
	if c.enc == nil || c.keyID == "" {
		return fmt.Errorf("%w: custom sender requires a KMS key", ErrCodeDeliveryFailure)
	}

	encCtx := map[string]string{cognitoUserPoolECID: c.poolID}

	dk, err := c.enc.GenerateDataKey(ctx, c.keyID, encCtx)
	if err != nil {
		return fmt.Errorf("%w: generating data key: %w", ErrCodeDeliveryFailure, err)
	}

	blob, err := esdk.Encrypt([]byte(code), encCtx, dk)
	if err != nil {
		return fmt.Errorf("%w: encrypting code: %w", ErrCodeDeliveryFailure, err)
	}

	req, _ := c.event["request"].(map[string]any)
	req["code"] = base64.StdEncoding.EncodeToString(blob)

	if _, err = c.inv.InvokeTrigger(ctx, c.functionARN, c.event); err != nil {
		return fmt.Errorf("%w: custom sender trigger: %w", ErrCodeDeliveryFailure, err)
	}

	return nil
}
