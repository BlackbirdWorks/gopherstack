package cognitoidp

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ErrCodeDeliveryFailure is returned when a verification or invitation message cannot be delivered.
var ErrCodeDeliveryFailure = awserr.New("CodeDeliveryFailureException", awserr.ErrInvalidParameter)

const (
	defaultEmailSender    = "no-reply@verificationemail.com"
	emailAccountDeveloper = "DEVELOPER"
	codePlaceholder       = "{####}"
	usernamePlaceholder   = "{username}"
	defaultVerifySubject  = "Your verification code"
	defaultVerifyMessage  = "Your verification code is " + codePlaceholder
	defaultInviteSubject  = "Your temporary password"
	defaultInviteMessage  = "Your username is " + usernamePlaceholder +
		" and temporary password is " + codePlaceholder + "."
	defaultMFAMessage  = "Your authentication code is " + codePlaceholder
	mediumSMSName      = "SMS"
	mediumEmailName    = "EMAIL"
	maskStars          = "***"
	phoneVisibleDigits = 4
)

// EmailMessage is one outbound verification email.
type EmailMessage struct {
	From      string
	ReplyTo   string
	SourceARN string
	To        string
	Subject   string
	HTML      string
	Text      string
	PoolID    string
}

// SMSMessage is one outbound verification text message.
type SMSMessage struct {
	PhoneNumber string
	Message     string
	PoolID      string
}

// MessageSender delivers Cognito verification messages; it is wired to SES and SNS at startup.
type MessageSender interface {
	SendEmail(ctx context.Context, m EmailMessage) error
	SendSMS(ctx context.Context, m SMSMessage) error
}

// SetMessageSender wires the sender used for real code delivery; nil keeps codes retrievable only.
func (b *InMemoryBackend) SetMessageSender(s MessageSender) {
	b.mu.Lock("SetMessageSender")
	defer b.mu.Unlock()

	b.messageSender = s
}

// MessageKind selects the message template.
type MessageKind int

// Message kinds.
const (
	MessageVerification MessageKind = iota
	MessageForgotPassword
	MessageInvite
	MessageMFASMS
	MessageMFAEmail
)

// MessageRequest describes one message to deliver to a user.
type MessageRequest struct {
	PoolID        string
	Username      string
	Code          string
	Attribute     string
	Medium        string
	Destination   string
	CustomMessage string
	CustomSubject string
	Mediums       []string
	Kind          MessageKind
}

// MessageResult reports where a message went; Destination is masked as AWS does.
type MessageResult struct {
	Medium        string
	Destination   string
	AttributeName string
	Resolved      bool
}

type deliveryTarget struct {
	medium, dest, attr string
}

// sendJob is a fully rendered message, safe to send without b.mu.
type sendJob struct {
	medium, dest, subject, body, from, replyTo, sourceARN, poolID string
	lenient                                                       bool // quota-limited default sender: failures only log
}

// DeliverMessage resolves the destination for the request and, when a sender is wired, sends it.
func (b *InMemoryBackend) DeliverMessage(ctx context.Context, req *MessageRequest) (MessageResult, error) {
	b.mu.RLock("DeliverMessage")
	res, jobs := b.prepareDeliveryLocked(req)
	sender := b.messageSender
	b.mu.RUnlock()

	if sender == nil {
		return res, nil
	}

	for i := range jobs {
		if err := sendJobTo(ctx, sender, &jobs[i]); err != nil {
			return res, err
		}
	}

	return res, nil
}

func (b *InMemoryBackend) prepareDeliveryLocked(req *MessageRequest) (MessageResult, []sendJob) {
	pool, ok := b.pools.Get(req.PoolID)
	if !ok {
		return MessageResult{}, nil
	}

	user, ok := b.users.Get(userKey(req.PoolID, b.resolveLoginNameLocked(pool, req.Username)))
	if !ok {
		return MessageResult{}, nil
	}

	targets := resolveTargets(pool, user, req)
	if len(targets) == 0 {
		return MessageResult{}, nil
	}

	res := MessageResult{
		Medium: targets[0].medium, Destination: maskDestination(targets[0].medium, targets[0].dest),
		AttributeName: targets[0].attr, Resolved: true,
	}

	jobs := make([]sendJob, 0, len(targets))

	for _, t := range targets {
		subject, body := renderMessage(pool, b.poolMfaConfigs[pool.ID], req, user.Username, t.medium)
		from, replyTo, sourceARN := emailSender(pool)
		jobs = append(jobs, sendJob{
			medium: t.medium, dest: t.dest, subject: subject, body: body,
			from: from, replyTo: replyTo, sourceARN: sourceARN, poolID: pool.ID,
			lenient: t.medium == mediumEmailName && !developerEmail(pool),
		})
	}

	return res, jobs
}

func sendJobTo(ctx context.Context, sender MessageSender, j *sendJob) error {
	var err error

	if j.medium == mediumSMSName {
		err = sender.SendSMS(ctx, SMSMessage{PhoneNumber: j.dest, Message: j.body, PoolID: j.poolID})
	} else {
		err = sender.SendEmail(ctx, EmailMessage{
			From: j.from, ReplyTo: j.replyTo, SourceARN: j.sourceARN, To: j.dest, Subject: j.subject,
			HTML: j.body, Text: j.body, PoolID: j.poolID,
		})
	}

	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "cognitoidp: message delivery failed",
			"pool", j.poolID, "medium", j.medium, "error", err)

		if j.lenient {
			return nil
		}

		return fmt.Errorf("%w: %w", ErrCodeDeliveryFailure, err)
	}

	return nil
}

func developerEmail(pool *UserPool) bool {
	v, _ := pool.EmailConfiguration["EmailSendingAccount"].(string)

	return v == emailAccountDeveloper
}

// emailSender returns the From address, reply-to and SES source ARN the pool's EmailConfiguration selects.
func emailSender(pool *UserPool) (string, string, string) {
	cfg := pool.EmailConfiguration
	str := func(k string) string {
		v, _ := cfg[k].(string)

		return v
	}

	replyTo, sourceARN := str("ReplyToEmailAddress"), str("SourceArn")
	if str("EmailSendingAccount") != emailAccountDeveloper {
		return defaultEmailSender, replyTo, ""
	}

	if from := str("From"); from != "" {
		return from, replyTo, sourceARN
	}

	if _, identity, ok := strings.Cut(sourceARN, ":identity/"); ok && identity != "" {
		if !strings.Contains(identity, "@") {
			identity = "no-reply@" + identity
		}

		return identity, replyTo, sourceARN
	}

	return defaultEmailSender, replyTo, sourceARN
}

func templateString(m map[string]any, key string) string {
	v, _ := m[key].(string)

	return v
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}

	return ""
}

// renderMessage fills the pool's message template (or the CustomMessage trigger's override) for a medium.
func renderMessage(
	pool *UserPool,
	mfa *UserPoolMfaFullConfig,
	req *MessageRequest,
	username, medium string,
) (string, string) {
	var subject, body string

	vt := pool.Settings.VerificationMessageTemplate
	sms := medium == mediumSMSName

	switch req.Kind {
	case MessageInvite:
		it, _ := pool.Settings.AdminCreateUserConfig["InviteMessageTemplate"].(map[string]any)
		if sms {
			body = firstNonEmpty(templateString(it, "SMSMessage"), defaultInviteMessage)
		} else {
			subject = firstNonEmpty(templateString(it, "EmailSubject"), defaultInviteSubject)
			body = firstNonEmpty(templateString(it, "EmailMessage"), defaultInviteMessage)
		}
	case MessageMFASMS, MessageMFAEmail:
		if sms {
			body = firstNonEmpty(mfaSMSMessage(mfa), pool.Settings.SmsAuthenticationMessage, defaultMFAMessage)
		} else {
			subject = firstNonEmpty(emailMFAField(mfa, true), defaultVerifySubject)
			body = firstNonEmpty(emailMFAField(mfa, false), defaultVerifyMessage)
		}
	default:
		if sms {
			body = firstNonEmpty(
				templateString(vt, "SmsMessage"),
				pool.Settings.SmsVerificationMessage,
				defaultVerifyMessage,
			)
		} else {
			subject = firstNonEmpty(
				templateString(vt, "EmailSubject"),
				pool.Settings.EmailVerificationSubject,
				defaultVerifySubject,
			)
			body = firstNonEmpty(
				templateString(vt, "EmailMessage"),
				pool.Settings.EmailVerificationMessage,
				defaultVerifyMessage,
			)
		}
	}

	if req.CustomMessage != "" {
		body = req.CustomMessage
	}

	if req.CustomSubject != "" && !sms {
		subject = req.CustomSubject
	}

	repl := strings.NewReplacer(usernamePlaceholder, username, codePlaceholder, req.Code)

	return repl.Replace(subject), repl.Replace(body)
}

func emailMFAField(mfa *UserPoolMfaFullConfig, subject bool) string {
	if mfa == nil || mfa.EmailMfaConfiguration == nil {
		return ""
	}

	if subject {
		return mfa.EmailMfaConfiguration.Subject
	}

	return mfa.EmailMfaConfiguration.Message
}

func resolveTargets(pool *UserPool, user *User, req *MessageRequest) []deliveryTarget {
	if req.Medium != "" && req.Destination != "" {
		return []deliveryTarget{{medium: req.Medium, dest: req.Destination, attr: req.Attribute}}
	}

	email, phone := user.Attributes[attrEmail], user.Attributes[attrPhoneNumber]
	if req.Attribute != "" && req.Kind == MessageVerification {
		if req.Attribute == attrPhoneNumber {
			return nonEmptyTargets(deliveryTarget{medium: mediumSMSName, dest: phone, attr: attrPhoneNumber})
		}

		return nonEmptyTargets(deliveryTarget{medium: mediumEmailName, dest: email, attr: attrEmail})
	}

	emailT := deliveryTarget{medium: mediumEmailName, dest: email, attr: attrEmail}
	phoneT := deliveryTarget{medium: mediumSMSName, dest: phone, attr: attrPhoneNumber}

	switch req.Kind {
	case MessageMFASMS:
		return nonEmptyTargets(phoneT)
	case MessageMFAEmail:
		return nonEmptyTargets(emailT)
	case MessageInvite:
		return inviteTargets(req.Mediums, emailT, phoneT)
	case MessageForgotPassword:
		if t := recoveryTargets(pool, emailT, phoneT); len(t) > 0 {
			return t[:1]
		}
	case MessageVerification:
	}

	order := []deliveryTarget{emailT, phoneT}
	if slices.Index(pool.AutoVerifiedAttributes, attrPhoneNumber) >= 0 &&
		slices.Index(pool.AutoVerifiedAttributes, attrEmail) < 0 {
		order = []deliveryTarget{phoneT, emailT}
	}

	if t := nonEmptyTargets(order...); len(t) > 0 {
		return t[:1]
	}

	return nil
}

func nonEmptyTargets(ts ...deliveryTarget) []deliveryTarget {
	out := make([]deliveryTarget, 0, len(ts))

	for _, t := range ts {
		if t.dest != "" {
			out = append(out, t)
		}
	}

	return out
}

func inviteTargets(mediums []string, emailT, phoneT deliveryTarget) []deliveryTarget {
	if len(mediums) == 0 {
		mediums = []string{mediumSMSName}
	}

	var out []deliveryTarget

	for _, m := range mediums {
		switch m {
		case mediumEmailName:
			out = append(out, nonEmptyTargets(emailT)...)
		case mediumSMSName:
			out = append(out, nonEmptyTargets(phoneT)...)
		}
	}

	if len(out) == 0 {
		return nonEmptyTargets(emailT, phoneT)[:min(1, len(nonEmptyTargets(emailT, phoneT)))]
	}

	return out
}

// recoveryTargets orders the contact methods by AccountRecoverySetting priority.
func recoveryTargets(pool *UserPool, emailT, phoneT deliveryTarget) []deliveryTarget {
	mechs, _ := pool.AccountRecoverySetting["RecoveryMechanisms"].([]any)

	type prio struct {
		t deliveryTarget
		p int
	}

	var ps []prio

	for _, m := range mechs {
		mm, _ := m.(map[string]any)
		p, _ := mm["Priority"].(float64)

		switch mm["Name"] {
		case "verified_email":
			ps = append(ps, prio{emailT, int(p)})
		case "verified_phone_number":
			ps = append(ps, prio{phoneT, int(p)})
		}
	}

	sort.SliceStable(ps, func(i, j int) bool { return ps[i].p < ps[j].p })

	out := make([]deliveryTarget, 0, len(ps))
	for _, e := range ps {
		out = append(out, nonEmptyTargets(e.t)...)
	}

	return out
}

func maskDestination(medium, dest string) string {
	if medium == mediumSMSName {
		return maskPhone(dest)
	}

	return maskEmail(dest)
}

// sendMFACodeLocked delivers an SMS/email one-time code for a challenge; failures only log, the
// code stays valid on the session. Caller holds b.mu.
func (b *InMemoryBackend) sendMFACodeLocked(pool *UserPool, username, challenge, code string) {
	if b.messageSender == nil {
		return
	}

	kind := MessageMFASMS
	if challenge == challengeEmailOTP {
		kind = MessageMFAEmail
	}

	_, jobs := b.prepareDeliveryLocked(&MessageRequest{PoolID: pool.ID, Username: username, Code: code, Kind: kind})
	for i := range jobs {
		_ = sendJobTo(context.Background(), b.messageSender, &jobs[i])
	}
}

func mfaSMSMessage(mfa *UserPoolMfaFullConfig) string {
	if mfa == nil || mfa.SmsMfaConfiguration == nil {
		return ""
	}

	return mfa.SmsMfaConfiguration.SmsAuthenticationMessage
}
