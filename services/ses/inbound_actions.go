package ses

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/mail"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	maxSNSActionContent = 150 * 1000
	inboundIDLen        = 52
	verdictPass         = "PASS"
	verdictDisabled     = "DISABLED"
	verdictGray         = "GRAY"
)

var (
	errInboundNoStore  = errors.New("ses inbound: no S3 backend wired")
	errInboundNoLambda = errors.New("ses inbound: no Lambda backend wired")
	errInboundNoSNS    = errors.New("ses inbound: no SNS backend wired")
	errInboundTooLarge = errors.New("ses inbound: message exceeds the 150 KB SNS action limit")
)

// inboundEnvelope is one message as received over SMTP.
type inboundEnvelope struct {
	from     string
	clientIP string
	rcpts    []string
	data     []byte
	tls      bool
}

// inboundReply is the SMTP answer to the end of DATA.
type inboundReply struct {
	text string
	code int
}

type sesVerdict struct {
	Status string `json:"status"`
}

type notifHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type notifCommonHeaders struct {
	ReturnPath string   `json:"returnPath"`
	Date       string   `json:"date,omitempty"`
	MessageID  string   `json:"messageId,omitempty"`
	Subject    string   `json:"subject,omitempty"`
	From       []string `json:"from,omitempty"`
	To         []string `json:"to,omitempty"`
}

type notifMail struct {
	Timestamp        string             `json:"timestamp"`
	Source           string             `json:"source"`
	MessageID        string             `json:"messageId"`
	Destination      []string           `json:"destination"`
	Headers          []notifHeader      `json:"headers"`
	CommonHeaders    notifCommonHeaders `json:"commonHeaders"`
	HeadersTruncated bool               `json:"headersTruncated"`
}

type notifAction struct {
	Type            string `json:"type"`
	TopicARN        string `json:"topicArn,omitempty"`
	BucketName      string `json:"bucketName,omitempty"`
	ObjectKeyPrefix string `json:"objectKeyPrefix,omitempty"`
	ObjectKey       string `json:"objectKey,omitempty"`
	Encoding        string `json:"encoding,omitempty"`
	FunctionARN     string `json:"functionArn,omitempty"`
	InvocationType  string `json:"invocationType,omitempty"`
	SMTPReplyCode   string `json:"smtpReplyCode,omitempty"`
	StatusCode      string `json:"statusCode,omitempty"`
	Message         string `json:"message,omitempty"`
	Sender          string `json:"sender,omitempty"`
	OrganizationARN string `json:"organizationArn,omitempty"`
}

type notifReceipt struct {
	Action               notifAction `json:"action"`
	Timestamp            string      `json:"timestamp"`
	SpamVerdict          sesVerdict  `json:"spamVerdict"`
	VirusVerdict         sesVerdict  `json:"virusVerdict"`
	SPFVerdict           sesVerdict  `json:"spfVerdict"`
	DKIMVerdict          sesVerdict  `json:"dkimVerdict"`
	DMARCVerdict         sesVerdict  `json:"dmarcVerdict"`
	DMARCPolicy          string      `json:"dmarcPolicy,omitempty"`
	Recipients           []string    `json:"recipients"`
	ProcessingTimeMillis int         `json:"processingTimeMillis"`
}

type receiptNotification struct {
	NotificationType string       `json:"notificationType"`
	Content          string       `json:"content,omitempty"`
	Receipt          notifReceipt `json:"receipt"`
	Mail             notifMail    `json:"mail"`
}

type lambdaEvent struct {
	Records []lambdaRecord `json:"Records"`
}

type lambdaRecord struct {
	EventSource  string    `json:"eventSource"`
	EventVersion string    `json:"eventVersion"`
	SES          lambdaSES `json:"ses"`
}

type lambdaSES struct {
	Receipt notifReceipt `json:"receipt"`
	Mail    notifMail    `json:"mail"`
}

type ruleFlow int

const (
	flowContinue ruleFlow = iota
	flowStopRule
	flowStopSet
	flowReject
)

// inboundRun is the state shared by every action of one delivery.
type inboundRun struct {
	start   time.Time
	reject  *inboundReply
	exec    *InboundExecution
	b       *InMemoryBackend
	auth    authVerdicts
	msgID   string
	plan    inboundPlan
	matched []string
	mail    notifMail
	env     inboundEnvelope
	rule    ReceiptRule
}

// deliverInbound runs the active rule set against a received message.
func (b *InMemoryBackend) deliverInbound(ctx context.Context, env inboundEnvelope) inboundReply {
	plan := b.inboundSnapshot()
	msgID := awsid.NewN("", inboundIDLen)
	start := time.Now()
	rec := InboundMessage{
		Received: start, MessageID: msgID, Source: env.from, ClientIP: env.clientIP,
		Recipients: env.rcpts, Size: len(env.data), TLS: env.tls,
	}
	reply := inboundReply{code: smtpOK, text: "2.0.0 Ok: queued as " + msgID}

	run := &inboundRun{
		b: b, env: env, plan: plan, msgID: msgID, start: start,
		mail: buildNotifMail(env, msgID, start),
		auth: evaluateAuth(ctx, plan.dns, env),
	}

	rec.SPF, rec.DKIM, rec.DMARC = run.auth.spf, run.auth.dkim, run.auth.dmarc

	if run.auth.dmarc == verdictFail && run.auth.dmarcPolicy == "reject" {
		reply = inboundReply{code: smtpMailboxErr, text: "5.7.1 Message rejected due to DMARC policy"}
		rec.Notes = append(rec.Notes, "dmarc policy reject")
		b.recordInbound(rec)

		return reply
	}

rules:
	for _, rule := range plan.rules {
		var matched []string

		for _, r := range env.rcpts {
			if ruleMatches(rule, r, env.tls) {
				matched = append(matched, r)
			}
		}

		if len(matched) == 0 {
			continue
		}

		run.rule, run.matched = rule, matched
		run.exec = &InboundExecution{Rule: rule.Name, Recipients: matched, ScanEnabled: rule.ScanEnabled}

		flow := run.runActions(ctx)
		rec.Executions = append(rec.Executions, *run.exec)

		switch flow {
		case flowReject:
			reply = *run.reject

			break rules
		case flowStopSet:
			break rules
		case flowContinue, flowStopRule:
		}
	}

	b.recordInbound(rec)

	return reply
}

func (r *inboundRun) runActions(ctx context.Context) ruleFlow {
	for _, a := range r.rule.Actions {
		r.exec.Actions = append(r.exec.Actions, a.Type)

		flow, err := r.runAction(ctx, a)
		if err != nil {
			r.exec.Error = err.Error()
			logger.Load(ctx).WarnContext(ctx, "ses inbound: action failed",
				"rule", r.rule.Name, "action", a.Type, "error", err)
		}

		if flow != flowContinue {
			return flow
		}
	}

	return flowContinue
}

func (r *inboundRun) runAction(ctx context.Context, a ReceiptAction) (ruleFlow, error) {
	switch a.Type {
	case ReceiptActionTypeAddHeader:
		r.env.data = append([]byte(a.HeaderName+": "+a.HeaderValue+"\r\n"), r.env.data...)
	case ReceiptActionTypeS3:
		return flowContinue, r.s3Action(ctx, a)
	case ReceiptActionTypeSNS:
		return flowContinue, r.snsAction(a)
	case ReceiptActionTypeLambda:
		return r.lambdaAction(ctx, a)
	case ReceiptActionTypeBounce:
		return r.bounceAction(a)
	case ReceiptActionTypeStop:
		return flowStopSet, nil
	case ReceiptActionTypeWorkmail:
		r.exec.Notes = append(r.exec.Notes, "workmail: recorded, no WorkMail delivery path")

		return flowContinue, r.notify(a.WorkmailTopicARN, r.notification(
			notifAction{
				Type:            "WorkMail",
				TopicARN:        a.WorkmailTopicARN,
				OrganizationARN: a.WorkmailOrganizationARN,
			},
			"",
		))
	case ReceiptActionTypeConnect:
		r.exec.Notes = append(r.exec.Notes, "connect: recorded, no Connect delivery path")
	}

	return flowContinue, nil
}

func (r *inboundRun) s3Action(ctx context.Context, a ReceiptAction) error {
	if r.plan.store == nil {
		return errInboundNoStore
	}

	key := r.msgID
	if p := strings.TrimSuffix(a.S3KeyPrefix, "/"); p != "" {
		key = p + "/" + r.msgID
	}

	if err := r.putS3(ctx, a, key); err != nil {
		return fmt.Errorf("s3 put %s/%s: %w", a.S3BucketName, key, err)
	}

	return r.notify(a.S3TopicARN, r.notification(notifAction{
		Type: "S3", TopicARN: a.S3TopicARN, BucketName: a.S3BucketName,
		ObjectKeyPrefix: a.S3KeyPrefix, ObjectKey: key,
	}, ""))
}

// putS3 stores the message, envelope-encrypted under the action's KMS key when one is set.
func (r *inboundRun) putS3(ctx context.Context, a ReceiptAction, key string) error {
	if a.S3KMSKeyARN == "" {
		return r.plan.store.PutObject(ctx, a.S3BucketName, key, r.env.data)
	}

	ms, ok := r.plan.store.(InboundMetadataStore)
	if r.plan.kms == nil || !ok {
		return errInboundNoKMS
	}

	encCtx := map[string]string{"aws:ses:message-id": r.msgID, "aws:ses:rule-name": r.rule.Name}

	body, meta, err := sealEnvelope(ctx, r.plan.kms, a.S3KMSKeyARN, encCtx, r.env.data)
	if err != nil {
		return err
	}

	return ms.PutObjectWithMetadata(ctx, a.S3BucketName, key, body, meta)
}

func (r *inboundRun) snsAction(a ReceiptAction) error {
	if len(r.env.data) > maxSNSActionContent {
		return errInboundTooLarge
	}

	enc := a.SNSEncoding
	content := string(r.env.data)

	if enc == "Base64" {
		content = base64.StdEncoding.EncodeToString(r.env.data)
	} else {
		enc = "UTF-8"
	}

	return r.notify(a.SNSTopicARN, r.notification(
		notifAction{Type: "SNS", TopicARN: a.SNSTopicARN, Encoding: enc}, content))
}

func (r *inboundRun) lambdaAction(ctx context.Context, a ReceiptAction) (ruleFlow, error) {
	if r.plan.lambda == nil {
		return flowContinue, errInboundNoLambda
	}

	inv := a.LambdaInvocationType
	if inv == "" {
		inv = "Event"
	}

	n := r.notification(notifAction{
		Type: "Lambda", TopicARN: a.LambdaTopicARN, FunctionARN: a.LambdaFunctionARN, InvocationType: inv,
	}, "")

	payload, err := json.Marshal(lambdaEvent{Records: []lambdaRecord{{
		EventSource: "aws:ses", EventVersion: "1.0", SES: lambdaSES{Receipt: n.Receipt, Mail: n.Mail},
	}}})
	if err != nil {
		return flowContinue, fmt.Errorf("marshal lambda event: %w", err)
	}

	out, _, err := r.plan.lambda.InvokeFunction(ctx, lambdaNameFromARN(a.LambdaFunctionARN), inv, payload)
	if err != nil {
		return flowContinue, fmt.Errorf("invoke %s: %w", a.LambdaFunctionARN, err)
	}

	notifyErr := r.notify(a.LambdaTopicARN, n)
	if inv != "RequestResponse" {
		return flowContinue, notifyErr
	}

	var resp struct {
		Disposition string `json:"disposition"`
	}

	_ = json.Unmarshal(out, &resp)

	switch strings.ToUpper(resp.Disposition) {
	case "STOP_RULE":
		return flowStopRule, notifyErr
	case "STOP_RULE_SET":
		return flowStopSet, notifyErr
	default:
		return flowContinue, notifyErr
	}
}

func (r *inboundRun) bounceAction(a ReceiptAction) (ruleFlow, error) {
	err := r.notify(a.BounceTopicARN, r.notification(notifAction{
		Type: "Bounce", TopicARN: a.BounceTopicARN, SMTPReplyCode: a.SMTPReplyCode,
		StatusCode: a.StatusCode, Message: a.Message, Sender: a.Sender,
	}, ""))

	code, convErr := strconv.Atoi(a.SMTPReplyCode)
	if convErr != nil || code < 400 || code > 599 {
		code = 550
	}

	text := a.Message
	if a.StatusCode != "" {
		text = a.StatusCode + " " + text
	}

	r.reject = &inboundReply{code: code, text: text}

	r.b.sendDSN(r, a, code)

	return flowReject, err
}

func (r *inboundRun) notification(a notifAction, content string) receiptNotification {
	verdict := func(scanned bool) sesVerdict {
		if scanned {
			return sesVerdict{Status: verdictPass}
		}

		return sesVerdict{Status: verdictDisabled}
	}

	return receiptNotification{
		NotificationType: "Received",
		Content:          content,
		Mail:             r.mail,
		Receipt: notifReceipt{
			Timestamp:            time.Now().UTC().Format(inboundTimeFormat),
			ProcessingTimeMillis: int(time.Since(r.start).Milliseconds()),
			Recipients:           r.matched,
			SpamVerdict:          verdict(r.rule.ScanEnabled),
			VirusVerdict:         verdict(r.rule.ScanEnabled),
			SPFVerdict:           sesVerdict{Status: r.auth.spf},
			DKIMVerdict:          sesVerdict{Status: r.auth.dkim},
			DMARCVerdict:         sesVerdict{Status: r.auth.dmarc},
			DMARCPolicy:          r.auth.dmarcPolicy,
			Action:               a,
		},
	}
}

func (r *inboundRun) notify(topicARN string, n receiptNotification) error {
	if topicARN == "" {
		return nil
	}

	if r.plan.sns == nil {
		return errInboundNoSNS
	}

	body, err := json.Marshal(n)
	if err != nil {
		return fmt.Errorf("marshal notification: %w", err)
	}

	if perr := r.plan.sns.PublishToTopic(topicARN, string(body)); perr != nil {
		return fmt.Errorf("publish %s: %w", topicARN, perr)
	}

	return nil
}

func lambdaNameFromARN(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) >= 7 && parts[5] == "function" {
		return parts[6]
	}

	return arn
}

func buildNotifMail(env inboundEnvelope, msgID string, ts time.Time) notifMail {
	headers := parseHeaderBlock(env.data)
	common := notifCommonHeaders{ReturnPath: env.from}

	for _, h := range headers {
		switch strings.ToLower(h.Name) {
		case "from":
			common.From = addressList(h.Value)
		case "to":
			common.To = addressList(h.Value)
		case "date":
			common.Date = h.Value
		case "message-id":
			common.MessageID = h.Value
		case "subject":
			common.Subject = h.Value
		}
	}

	return notifMail{
		Timestamp: ts.UTC().Format(inboundTimeFormat), Source: env.from, MessageID: msgID,
		Destination: env.rcpts, Headers: headers, CommonHeaders: common,
	}
}

func addressList(v string) []string {
	list, err := mail.ParseAddressList(v)
	if err != nil {
		return []string{v}
	}

	out := make([]string, 0, len(list))
	for _, a := range list {
		out = append(out, a.String())
	}

	return out
}

// parseHeaderBlock returns the message headers in order with folded lines joined.
func parseHeaderBlock(data []byte) []notifHeader {
	block, _, _ := strings.Cut(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n\n")

	var out []notifHeader

	for line := range strings.SplitSeq(block, "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}

		if (line[0] == ' ' || line[0] == '\t') && len(out) > 0 {
			out[len(out)-1].Value += " " + strings.TrimSpace(line)

			continue
		}

		if name, val, ok := strings.Cut(line, ":"); ok {
			out = append(out, notifHeader{Name: name, Value: strings.TrimSpace(val)})
		}
	}

	return out
}
