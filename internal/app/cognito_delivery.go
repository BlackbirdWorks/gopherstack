package app

import (
	"context"
	"errors"
	"fmt"
	"net/mail"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cognitoidpbackend "github.com/blackbirdworks/gopherstack/services/cognitoidp"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

const (
	cognitoServicePrincipal = "cognito-idp.amazonaws.com"
	cognitoSMSSessionName   = "CognitoIdpSmsRole"
	sesIdentityVerified     = "Success"
)

// cognitoSMSRoleAssumer checks the pool's SnsCallerArn role is assumable by Cognito through the STS emulator.
type cognitoSMSRoleAssumer struct{ sts *stsbackend.InMemoryBackend }

func (a cognitoSMSRoleAssumer) AssumeSMSRole(_ context.Context, roleARN, _ string) error {
	if _, err := a.sts.AssumeRoleForService(cognitoServicePrincipal, roleARN, cognitoSMSSessionName); err != nil {
		return fmt.Errorf("sts assume sms role: %w", err)
	}

	return nil
}

var errCognitoSenderUnavailable = errors.New("cognito message delivery backend is not running")

// cognitoMessageSender delivers Cognito verification messages through the emulator's SES and SNS.
type cognitoMessageSender struct {
	ses *sesbackend.InMemoryBackend
	sns *snsbackend.InMemoryBackend
}

func (s *cognitoMessageSender) SendEmail(_ context.Context, m cognitoidpbackend.EmailMessage) error {
	if s.ses == nil {
		return fmt.Errorf("ses: %w", errCognitoSenderUnavailable)
	}

	if err := s.checkSourceIdentity(m.SourceARN); err != nil {
		return err
	}

	if m.SourceARN == "" {
		if addr, err := mail.ParseAddress(m.From); err == nil {
			if err = s.ses.VerifyEmailIdentity(addr.Address); err != nil {
				return fmt.Errorf("verifying default sender: %w", err)
			}
		}
	}

	in := sesbackend.SendEmailInput{
		From: m.From, To: []string{m.To}, Subject: m.Subject, BodyHTML: m.HTML, BodyText: m.Text,
		SourceArn: m.SourceARN,
	}
	if m.ReplyTo != "" {
		in.ReplyTo = []string{m.ReplyTo}
	}

	if _, err := s.ses.SendEmail(in); err != nil {
		return fmt.Errorf("ses send: %w", err)
	}

	return nil
}

// checkSourceIdentity requires the SES identity named by a DEVELOPER pool's SourceArn to exist and be verified.
func (s *cognitoMessageSender) checkSourceIdentity(sourceARN string) error {
	if sourceARN == "" {
		return nil
	}

	_, identity, ok := strings.Cut(sourceARN, ":identity/")
	if !ok || identity == "" {
		return fmt.Errorf("%w: SourceArn %q is not an SES identity ARN",
			cognitoidpbackend.ErrInvalidEmailRoleAccessPolicy, sourceARN)
	}

	candidates := []string{identity}
	if _, domain, found := strings.Cut(identity, "@"); found {
		candidates = append(candidates, domain)
	}

	for _, status := range s.ses.GetIdentityVerificationAttributes(candidates) {
		if status == sesIdentityVerified {
			return nil
		}
	}

	return fmt.Errorf("%w: SES identity %q is not verified",
		cognitoidpbackend.ErrInvalidEmailRoleAccessPolicy, identity)
}

func (s *cognitoMessageSender) SendSMS(_ context.Context, m cognitoidpbackend.SMSMessage) error {
	if s.sns == nil {
		return fmt.Errorf("sns: %w", errCognitoSenderUnavailable)
	}

	if _, err := s.sns.PublishSMS(m.PhoneNumber, m.Message); err != nil {
		return fmt.Errorf("sns publish sms: %w", err)
	}

	return nil
}

// wireCognitoMessageDelivery connects Cognito code delivery (SignUp, ForgotPassword, ResendConfirmationCode,
// AdminCreateUser, attribute verification, SMS/email MFA) to SES for email and SNS for SMS.
// An optional STS registration enables SnsCallerArn role assumption before SMS is published.
func wireCognitoMessageDelivery(cognitoReg, sesReg, snsReg service.Registerable, stsReg ...service.Registerable) {
	cognitoH, ok := cognitoReg.(*cognitoidpbackend.Handler)
	if !ok {
		return
	}

	for _, reg := range stsReg {
		if stsH, stsOK := reg.(*stsbackend.Handler); stsOK {
			if bk, bkOK := stsH.Backend.(*stsbackend.InMemoryBackend); bkOK {
				cognitoH.Backend.SetSMSRoleAssumer(cognitoSMSRoleAssumer{sts: bk})
			}
		}
	}

	sender := &cognitoMessageSender{}

	if sesH, sesOK := sesReg.(*sesbackend.Handler); sesOK {
		sender.ses, _ = sesH.Backend.(*sesbackend.InMemoryBackend)
	}

	if snsH, snsOK := snsReg.(*snsbackend.Handler); snsOK {
		sender.sns, _ = snsH.Backend.(*snsbackend.InMemoryBackend)
	}

	if sender.ses == nil && sender.sns == nil {
		return
	}

	cognitoH.Backend.SetMessageSender(sender)
}

// cognitoKMSDataKeys generates the data keys custom-sender codes are encrypted under.
type cognitoKMSDataKeys struct{ kms kmsbackend.StorageBackend }

func (e cognitoKMSDataKeys) GenerateDataKey(
	ctx context.Context, keyID string, encCtx map[string]string,
) (*cognitoidpbackend.DataKey, error) {
	out, err := e.kms.GenerateDataKey(ctx, &kmsbackend.GenerateDataKeyInput{
		KeyID: keyID, KeySpec: "AES_256", EncryptionContext: encCtx,
	})
	if err != nil {
		return nil, fmt.Errorf("kms generate data key: %w", err)
	}

	return &cognitoidpbackend.DataKey{KeyARN: out.KeyID, Plaintext: out.Plaintext, Ciphertext: out.CiphertextBlob}, nil
}

// wireCognitoCodeEncryption lets CustomEmailSender/CustomSMSSender triggers receive KMS-encrypted codes.
func wireCognitoCodeEncryption(cognitoReg, kmsReg service.Registerable) {
	cognitoH, ok := cognitoReg.(*cognitoidpbackend.Handler)
	kmsH, kmsOK := kmsReg.(*kmsbackend.Handler)

	if !ok || !kmsOK {
		return
	}

	cognitoH.Backend.SetDataKeyGenerator(cognitoKMSDataKeys{kms: kmsH.Backend})
}
