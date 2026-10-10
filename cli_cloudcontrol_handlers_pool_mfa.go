package main

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	cognitotypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"

	cloudcontrolbackend "github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

const (
	ccMfaSMS      = "SMS_MFA"
	ccMfaSoftware = "SOFTWARE_TOKEN_MFA"
	ccMfaEmail    = "EMAIL_OTP"
)

func ccUserPoolMfaKeys() []string {
	return []string{
		"EnabledMfas", "WebAuthnRelyingPartyID", "WebAuthnUserVerification", "WebAuthnFactorConfiguration",
		"EmailAuthenticationMessage", "EmailAuthenticationSubject",
	}
}

type ccPoolMfa struct {
	WebAuthnRelyingPartyID      *string
	EmailAuthenticationMessage  *string
	EmailAuthenticationSubject  *string
	SmsAuthenticationMessage    *string
	SmsConfiguration            *cognitotypes.SmsConfigurationType
	EmailConfiguration          *cognitotypes.EmailConfigurationType
	MfaConfiguration            string
	WebAuthnUserVerification    string
	WebAuthnFactorConfiguration string
	EnabledMfas                 []string
}

func ccPoolMfaTouched(current, desired map[string]any) bool {
	for _, k := range ccUserPoolMfaKeys() {
		if !reflect.DeepEqual(normalizeJSON(current[k]), normalizeJSON(desired[k])) {
			return true
		}
	}

	return false
}

func (m ccPoolMfa) input(id string) (*cognitoidentityprovider.SetUserPoolMfaConfigInput, error) {
	in := &cognitoidentityprovider.SetUserPoolMfaConfigInput{
		UserPoolId: aws.String(id), MfaConfiguration: cognitotypes.UserPoolMfaType(m.MfaConfiguration),
	}

	for _, mfa := range m.EnabledMfas {
		switch mfa {
		case ccMfaSMS:
			if m.SmsConfiguration == nil {
				return nil, fmt.Errorf(
					"%w: EnabledMfas %s requires SmsConfiguration", cloudcontrolbackend.ErrValidation, mfa,
				)
			}

			in.SmsMfaConfiguration = &cognitotypes.SmsMfaConfigType{
				SmsAuthenticationMessage: m.SmsAuthenticationMessage, SmsConfiguration: m.SmsConfiguration,
			}
		case ccMfaSoftware:
			in.SoftwareTokenMfaConfiguration = &cognitotypes.SoftwareTokenMfaConfigType{Enabled: true}
		case ccMfaEmail:
			if m.EmailConfiguration == nil ||
				m.EmailConfiguration.EmailSendingAccount != cognitotypes.EmailSendingAccountTypeDeveloper {
				return nil, fmt.Errorf(
					"%w: EnabledMfas %s requires EmailConfiguration with EmailSendingAccount DEVELOPER",
					cloudcontrolbackend.ErrValidation, mfa,
				)
			}

			in.EmailMfaConfiguration = &cognitotypes.EmailMfaConfigType{
				Message: m.EmailAuthenticationMessage, Subject: m.EmailAuthenticationSubject,
			}
		default:
			return nil, fmt.Errorf("%w: EnabledMfas value %q must be one of %s, %s, %s",
				cloudcontrolbackend.ErrValidation, mfa, ccMfaSMS, ccMfaSoftware, ccMfaEmail)
		}
	}

	if m.WebAuthnRelyingPartyID != nil || m.WebAuthnUserVerification != "" || m.WebAuthnFactorConfiguration != "" {
		in.WebAuthnConfiguration = &cognitotypes.WebAuthnConfigurationType{
			RelyingPartyId:      m.WebAuthnRelyingPartyID,
			UserVerification:    cognitotypes.UserVerificationType(m.WebAuthnUserVerification),
			FactorConfiguration: cognitotypes.WebAuthnFactorConfigurationType(m.WebAuthnFactorConfiguration),
		}
	}

	return in, nil
}

func ccValidatePoolMfa(desired map[string]any) error {
	m, err := ccDecode[ccPoolMfa](desired)
	if err != nil {
		return err
	}

	_, err = m.input("")

	return err
}

func (h *ccUserPool) applyMfa(ctx context.Context, id string, desired map[string]any) error {
	m, err := ccDecode[ccPoolMfa](desired)
	if err != nil {
		return err
	}

	in, err := m.input(id)
	if err != nil {
		return err
	}

	_, err = h.client.SetUserPoolMfaConfig(ctx, in)

	return ccMapError(err)
}

func (h *ccUserPool) readMfa(ctx context.Context, id string, model map[string]any) {
	out, err := h.client.GetUserPoolMfaConfig(ctx, &cognitoidentityprovider.GetUserPoolMfaConfigInput{
		UserPoolId: aws.String(id),
	})
	if err != nil {
		return
	}

	var enabled []string

	if out.SmsMfaConfiguration != nil {
		enabled = append(enabled, ccMfaSMS)
	}

	if out.SoftwareTokenMfaConfiguration != nil && out.SoftwareTokenMfaConfiguration.Enabled {
		enabled = append(enabled, ccMfaSoftware)
	}

	if e := out.EmailMfaConfiguration; e != nil {
		enabled = append(enabled, ccMfaEmail)

		if e.Message != nil {
			model["EmailAuthenticationMessage"] = aws.ToString(e.Message)
		}

		if e.Subject != nil {
			model["EmailAuthenticationSubject"] = aws.ToString(e.Subject)
		}
	}

	if len(enabled) > 0 {
		slices.Sort(enabled)
		model["EnabledMfas"] = enabled
	}

	if w := out.WebAuthnConfiguration; w != nil {
		if w.RelyingPartyId != nil {
			model["WebAuthnRelyingPartyID"] = aws.ToString(w.RelyingPartyId)
		}

		if w.UserVerification != "" {
			model["WebAuthnUserVerification"] = string(w.UserVerification)
		}

		if w.FactorConfiguration != "" {
			model["WebAuthnFactorConfiguration"] = string(w.FactorConfiguration)
		}
	}
}
