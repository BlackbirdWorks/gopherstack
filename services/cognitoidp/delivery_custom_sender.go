package cognitoidp

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

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

// CodeEncrypter encrypts a one-time code under a KMS key for CustomEmailSender/CustomSMSSender Lambdas.
type CodeEncrypter interface {
	EncryptCode(ctx context.Context, keyID string, plaintext []byte) ([]byte, error)
}

// SetCodeEncrypter wires the KMS-backed encrypter used for custom sender triggers.
func (b *InMemoryBackend) SetCodeEncrypter(e CodeEncrypter) {
	b.mu.Lock("SetCodeEncrypter")
	defer b.mu.Unlock()

	b.codeEncrypter = e
}

// customSender is a rendered CustomEmailSender/CustomSMSSender invocation, minus the encrypted code.
type customSender struct {
	inv         LambdaTriggerInvoker
	enc         CodeEncrypter
	event       map[string]any
	functionARN string
	keyID       string
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
			"clientMetadata":       map[string]any{},
			eventKeyUserAttributes: stringMapToAny(user.Attributes),
		}, map[string]any{})
	if call == nil {
		return nil
	}

	keyID, _ := pool.LambdaConfig[lambdaConfigKMSKeyID].(string)

	return &customSender{
		inv: call.inv, enc: b.codeEncrypter, event: call.event, functionARN: call.functionARN, keyID: keyID,
	}
}

func (c *customSender) send(ctx context.Context, code string) error {
	if c.enc == nil || c.keyID == "" {
		return fmt.Errorf("%w: custom sender requires a KMS key", ErrCodeDeliveryFailure)
	}

	blob, err := c.enc.EncryptCode(ctx, c.keyID, []byte(code))
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
