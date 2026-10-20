package app

import (
	"context"
	"errors"
	"fmt"
	"net/mail"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cognitoidpbackend "github.com/blackbirdworks/gopherstack/services/cognitoidp"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

const (
	cognitoServicePrincipal = "cognito-idp.amazonaws.com"
	cognitoSMSSessionName   = "CognitoIdpSmsRole"
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
