package ses_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"maps"
	"net/smtp"
	"net/textproto"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

type fakeInboundStore struct {
	objs map[string][]byte
	mu   sync.Mutex
}

func (f *fakeInboundStore) PutObject(_ context.Context, bucket, key string, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.objs[bucket+"/"+key] = append([]byte(nil), body...)

	return nil
}

func (f *fakeInboundStore) snapshot() map[string][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make(map[string][]byte, len(f.objs))
	maps.Copy(out, f.objs)

	return out
}

type fakeInboundLambda struct {
	resp     string
	payloads [][]byte
	types    []string
	mu       sync.Mutex
}

func (f *fakeInboundLambda) InvokeFunction(_ context.Context, _, inv string, p []byte) ([]byte, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.payloads = append(f.payloads, p)
	f.types = append(f.types, inv)

	return []byte(f.resp), 200, nil
}

type fakeInboundSNS struct {
	msgs map[string][]string
	mu   sync.Mutex
}

func (f *fakeInboundSNS) PublishToTopic(arn, msg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.msgs[arn] = append(f.msgs[arn], msg)

	return nil
}

const inboundBody = "From: Sender <sender@sender.test>\r\nTo: user@example.com\r\nSubject: Hello\r\n\r\nbody line\r\n"

func TestInboundSMTP(t *testing.T) {
	t.Parallel()

	rule := func(name string, rcpts []string, actions ...ses.ReceiptAction) ses.ReceiptRule {
		return ses.ReceiptRule{Name: name, Enabled: true, Recipients: rcpts, Actions: actions}
	}
	s3a := func(prefix, topic string) ses.ReceiptAction {
		return ses.ReceiptAction{
			Type:         ses.ReceiptActionTypeS3,
			S3BucketName: "inbox",
			S3KeyPrefix:  prefix,
			S3TopicARN:   topic,
		}
	}
	lambdaRR := ses.ReceiptAction{
		Type: ses.ReceiptActionTypeLambda, LambdaFunctionARN: "arn:aws:lambda:us-east-1:123456789012:function:fn",
		LambdaInvocationType: "RequestResponse",
	}

	tests := []struct {
		wantSNS    map[string]int
		name       string
		rcpt       string
		lambdaResp string
		rules      []ses.ReceiptRule
		filters    []ses.ReceiptFilter
		wantRcpt   int
		wantData   int
		wantObjs   int
		useTLS     bool
	}{
		{
			name: "s3 and sns", rcpt: "user@example.com", wantObjs: 1,
			rules: []ses.ReceiptRule{rule("r", []string{"example.com"}, s3a("mail", "arn:t1"),
				ses.ReceiptAction{Type: ses.ReceiptActionTypeSNS, SNSTopicARN: "arn:t2", SNSEncoding: "Base64"})},
			wantSNS: map[string]int{"arn:t1": 1, "arn:t2": 1},
		},
		{
			name: "unverified recipient", rcpt: "user@other.test", wantRcpt: 550,
			rules: []ses.ReceiptRule{rule("r", nil, s3a("", ""))},
		},
		{
			name: "no matching rule", rcpt: "user@example.com", wantRcpt: 550,
			rules: []ses.ReceiptRule{rule("r", []string{"someone@example.com"}, s3a("", ""))},
		},
		{
			name: "subdomain pattern", rcpt: "user@mail.example.com", wantObjs: 1,
			rules: []ses.ReceiptRule{rule("r", []string{".example.com"}, s3a("", ""))},
		},
		{
			name: "bare domain does not match subdomain", rcpt: "user@mail.example.com", wantRcpt: 550,
			rules: []ses.ReceiptRule{rule("r", []string{"example.com"}, s3a("", ""))},
		},
		{
			name: "lambda stop rule", rcpt: "user@example.com", lambdaResp: `{"disposition":"STOP_RULE"}`, wantObjs: 1,
			rules: []ses.ReceiptRule{
				rule("a", nil, lambdaRR, s3a("first", "")),
				rule("b", nil, s3a("second", "")),
			},
		},
		{
			name: "lambda stop rule set", rcpt: "user@example.com", lambdaResp: `{"disposition":"STOP_RULE_SET"}`,
			rules: []ses.ReceiptRule{
				rule("a", nil, lambdaRR, s3a("first", "")),
				rule("b", nil, s3a("second", "")),
			},
		},
		{
			name: "lambda continue", rcpt: "user@example.com", lambdaResp: `{"disposition":"CONTINUE"}`, wantObjs: 2,
			rules: []ses.ReceiptRule{
				rule("a", nil, lambdaRR, s3a("first", "")),
				rule("b", nil, s3a("second", "")),
			},
		},
		{
			name: "stop action", rcpt: "user@example.com", wantObjs: 1,
			rules: []ses.ReceiptRule{
				rule("a", nil, s3a("first", ""), ses.ReceiptAction{Type: ses.ReceiptActionTypeStop}),
				rule("b", nil, s3a("second", "")),
			},
		},
		{
			name: "bounce rejects", rcpt: "user@example.com", wantData: 550,
			rules: []ses.ReceiptRule{rule("a", nil, ses.ReceiptAction{
				Type: ses.ReceiptActionTypeBounce, SMTPReplyCode: "550", StatusCode: "5.1.1",
				Message: "mailbox unavailable", Sender: "mailer@example.com",
			})},
		},
		{
			name: "blocked ip", rcpt: "user@example.com", wantRcpt: -1,
			rules:   []ses.ReceiptRule{rule("a", nil, s3a("", ""))},
			filters: []ses.ReceiptFilter{{Name: "b", Policy: ses.FilterPolicyBlock, CIDR: "127.0.0.0/8"}},
		},
		{
			name: "specific allow beats block", rcpt: "user@example.com", wantObjs: 1,
			rules: []ses.ReceiptRule{rule("a", nil, s3a("", ""))},
			filters: []ses.ReceiptFilter{
				{Name: "b", Policy: ses.FilterPolicyBlock, CIDR: "127.0.0.0/8"},
				{Name: "a", Policy: ses.FilterPolicyAllow, CIDR: "127.0.0.1"},
			},
		},
		{
			name: "tls required without tls", rcpt: "user@example.com", wantRcpt: 550,
			rules: []ses.ReceiptRule{{
				Name: "a", Enabled: true, TLSPolicy: ses.TLSPolicyRequire,
				Actions: []ses.ReceiptAction{s3a("", "")},
			}},
		},
		{
			name: "tls required with starttls", rcpt: "user@example.com", useTLS: true, wantObjs: 1,
			rules: []ses.ReceiptRule{{
				Name: "a", Enabled: true, TLSPolicy: ses.TLSPolicyRequire,
				Actions: []ses.ReceiptAction{s3a("", "")},
			}},
		},
		{
			name: "disabled rule", rcpt: "user@example.com", wantRcpt: 550,
			rules: []ses.ReceiptRule{{Name: "a", Actions: []ses.ReceiptAction{s3a("", "")}}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			store := &fakeInboundStore{objs: map[string][]byte{}}
			lam := &fakeInboundLambda{resp: tc.lambdaResp}
			snsF := &fakeInboundSNS{msgs: map[string][]string{}}
			b.SetInboundObjectStore(store)
			b.SetInboundLambdaInvoker(lam)
			b.SetSNSPublisher(snsF)

			_, err := b.VerifyDomainIdentity("example.com")
			require.NoError(t, err)
			require.NoError(t, b.CreateReceiptRuleSet("rs"))

			after := ""
			for _, r := range tc.rules {
				require.NoError(t, b.CreateReceiptRule("rs", r, after))
				after = r.Name
			}

			for _, f := range tc.filters {
				require.NoError(t, b.CreateReceiptFilter(f))
			}

			require.NoError(t, b.SetActiveReceiptRuleSet("rs"))

			h := ses.NewHandler(b).WithInbound(ses.InboundConfig{Addr: "127.0.0.1:0"})
			require.NoError(t, h.StartWorker(t.Context()))
			t.Cleanup(func() { h.Shutdown(context.Background()) })

			c, err := smtp.Dial(h.InboundAddr())
			if tc.wantRcpt == -1 {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "550")

				return
			}

			require.NoError(t, err)
			defer func() { _ = c.Close() }()

			require.NoError(t, c.Hello("client.test"))

			if tc.useTLS {
				require.NoError(t, c.StartTLS(&tls.Config{
					InsecureSkipVerify: true,
					MinVersion:         tls.VersionTLS12,
				}))
			}

			require.NoError(t, c.Mail("sender@sender.test"))

			if err = c.Rcpt(tc.rcpt); tc.wantRcpt != 0 {
				var te *textproto.Error
				require.ErrorAs(t, err, &te)
				assert.Equal(t, tc.wantRcpt, te.Code)

				return
			}

			require.NoError(t, err)

			w, err := c.Data()
			require.NoError(t, err)
			_, err = w.Write([]byte(inboundBody))
			require.NoError(t, err)

			err = w.Close()
			if tc.wantData != 0 {
				var te *textproto.Error
				require.ErrorAs(t, err, &te)
				assert.Equal(t, tc.wantData, te.Code)

				return
			}

			require.NoError(t, err)
			require.NoError(t, c.Quit())

			objs := store.snapshot()
			assert.Len(t, objs, tc.wantObjs)

			for k, v := range objs {
				assert.Contains(t, k, "inbox/")
				assert.Equal(t, inboundBody, string(v))
			}

			snsF.mu.Lock()
			for arn, n := range tc.wantSNS {
				assert.Len(t, snsF.msgs[arn], n, arn)
			}
			snsF.mu.Unlock()

			assert.Len(t, b.ListInboundMessages(), 1)
		})
	}
}

func TestInboundNotificationShape(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	store := &fakeInboundStore{objs: map[string][]byte{}}
	snsF := &fakeInboundSNS{msgs: map[string][]string{}}
	lam := &fakeInboundLambda{}
	b.SetInboundObjectStore(store)
	b.SetInboundLambdaInvoker(lam)
	b.SetSNSPublisher(snsF)

	require.NoError(t, b.VerifyEmailIdentity("user@example.com"))
	require.NoError(t, b.CreateReceiptRuleSet("rs"))
	require.NoError(t, b.CreateReceiptRule("rs", ses.ReceiptRule{
		Name: "r", Enabled: true, ScanEnabled: true, Recipients: []string{"user@example.com"},
		Actions: []ses.ReceiptAction{
			{Type: ses.ReceiptActionTypeAddHeader, HeaderName: "X-Tag", HeaderValue: "v1"},
			{Type: ses.ReceiptActionTypeSNS, SNSTopicARN: "arn:t", SNSEncoding: "Base64"},
			{
				Type:              ses.ReceiptActionTypeLambda,
				LambdaFunctionARN: "arn:aws:lambda:us-east-1:123456789012:function:fn",
				S3KMSKeyARN:       "ignored",
			},
		},
	}, ""))
	require.NoError(t, b.SetActiveReceiptRuleSet("rs"))

	h := ses.NewHandler(b).WithInbound(ses.InboundConfig{Addr: "127.0.0.1:0"})
	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	require.NoError(t, sendPlain(h.InboundAddr(), "sender@sender.test", "user@example.com", inboundBody))

	snsF.mu.Lock()
	defer snsF.mu.Unlock()

	require.Len(t, snsF.msgs["arn:t"], 1)

	var n struct {
		NotificationType string `json:"notificationType"`
		Content          string `json:"content"`
		Mail             struct {
			Source        string   `json:"source"`
			Destination   []string `json:"destination"`
			CommonHeaders struct {
				Subject    string   `json:"subject"`
				ReturnPath string   `json:"returnPath"`
				From       []string `json:"from"`
			} `json:"commonHeaders"`
			Headers []struct {
				Name  string `json:"name"`
				Value string `json:"value"`
			} `json:"headers"`
		} `json:"mail"`
		Receipt struct {
			Action struct {
				Type     string `json:"type"`
				Encoding string `json:"encoding"`
				TopicARN string `json:"topicArn"`
			} `json:"action"`
			SpamVerdict struct {
				Status string `json:"status"`
			} `json:"spamVerdict"`
			Recipients []string `json:"recipients"`
		} `json:"receipt"`
	}

	require.NoError(t, json.Unmarshal([]byte(snsF.msgs["arn:t"][0]), &n))
	assert.Equal(t, "Received", n.NotificationType)
	assert.Equal(t, "sender@sender.test", n.Mail.Source)
	assert.Equal(t, []string{"user@example.com"}, n.Mail.Destination)
	assert.Equal(t, "Hello", n.Mail.CommonHeaders.Subject)
	assert.Equal(t, "sender@sender.test", n.Mail.CommonHeaders.ReturnPath)
	assert.Equal(t, "SNS", n.Receipt.Action.Type)
	assert.Equal(t, "Base64", n.Receipt.Action.Encoding)
	assert.Equal(t, "PASS", n.Receipt.SpamVerdict.Status)
	assert.Equal(t, []string{"user@example.com"}, n.Receipt.Recipients)

	raw, err := base64.StdEncoding.DecodeString(n.Content)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(string(raw), "X-Tag: v1\r\n"), "AddHeader precedes later actions")
	assert.True(t, bytes.HasSuffix(raw, []byte("body line\r\n")))

	lam.mu.Lock()
	defer lam.mu.Unlock()

	require.Len(t, lam.payloads, 1)
	assert.Equal(t, []string{"Event"}, lam.types)

	var ev struct {
		Records []struct {
			EventSource string `json:"eventSource"`
			SES         struct {
				Receipt struct {
					Action struct {
						Type        string `json:"type"`
						FunctionARN string `json:"functionArn"`
					} `json:"action"`
				} `json:"receipt"`
			} `json:"ses"`
		} `json:"Records"`
	}

	require.NoError(t, json.Unmarshal(lam.payloads[0], &ev))
	require.Len(t, ev.Records, 1)
	assert.Equal(t, "aws:ses", ev.Records[0].EventSource)
	assert.Equal(t, "Lambda", ev.Records[0].SES.Receipt.Action.Type)
}

func TestInboundSizeLimitAndShutdown(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("example.com"))
	require.NoError(t, b.CreateReceiptRuleSet("rs"))
	require.NoError(t, b.CreateReceiptRule("rs", ses.ReceiptRule{Name: "r", Enabled: true}, ""))
	require.NoError(t, b.SetActiveReceiptRuleSet("rs"))

	h := ses.NewHandler(b).WithInbound(ses.InboundConfig{Addr: "127.0.0.1:0", MaxSize: 64})
	require.NoError(t, h.StartWorker(t.Context()))

	err := sendPlain(h.InboundAddr(), "a@sender.test", "u@example.com", strings.Repeat("x", 200))

	var te *textproto.Error
	require.ErrorAs(t, err, &te)
	assert.Equal(t, 552, te.Code)

	addr := h.InboundAddr()
	h.Shutdown(context.Background())
	assert.Empty(t, h.InboundAddr())

	_, derr := smtp.Dial(addr)
	require.Error(t, derr)
}

func sendPlain(addr, from, to, body string) error {
	c, err := smtp.Dial(addr)
	if err != nil {
		return err
	}

	defer func() { _ = c.Close() }()

	if err = c.Mail(from); err != nil {
		return err
	}

	if err = c.Rcpt(to); err != nil {
		return err
	}

	w, err := c.Data()
	if err != nil {
		return err
	}

	if _, err = w.Write([]byte(body)); err != nil {
		return err
	}

	return w.Close()
}
