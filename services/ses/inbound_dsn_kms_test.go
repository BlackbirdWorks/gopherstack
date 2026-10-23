package ses_test

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/mail"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

type fakeKMS struct {
	err  error
	ctxs []map[string]string
	mu   sync.Mutex
}

func (f *fakeKMS) GenerateDataKey(_ context.Context, _ string, c map[string]string) ([]byte, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.ctxs = append(f.ctxs, c)

	return []byte(strings.Repeat("k", 32)), []byte("wrapped-key"), f.err
}

type metaStore struct {
	meta map[string]map[string]string
	fakeInboundStore
}

func (m *metaStore) PutObjectWithMetadata(
	ctx context.Context,
	bucket, key string,
	body []byte,
	meta map[string]string,
) error {
	m.mu.Lock()
	m.meta[bucket+"/"+key] = meta
	m.mu.Unlock()

	return m.PutObject(ctx, bucket, key, body)
}

func TestInboundBounceDSN(t *testing.T) {
	t.Parallel()

	h, _, b := inboundHarness(t, "", ses.ReceiptAction{
		Type: ses.ReceiptActionTypeBounce, SMTPReplyCode: "550", StatusCode: "5.1.1",
		Message: "mailbox unavailable", Sender: "mailer@example.com",
	})

	err := sendPlain(h.InboundAddr(), "sender@sender.test", "user@example.com", inboundBody)
	require.Error(t, err)

	emails := b.ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "mailer@example.com", emails[0].From)
	assert.Equal(t, []string{"sender@sender.test"}, emails[0].To)
	assert.Equal(t, "Delivery Status Notification (Failure)", emails[0].Subject)

	_, raw, _ := strings.Cut(emails[0].BodyText, "\r\n\r\nFrom: ")
	msg, err := mail.ReadMessage(strings.NewReader("From: " + raw))
	require.NoError(t, err)

	mt, params, err := mime.ParseMediaType(msg.Header.Get("Content-Type"))
	require.NoError(t, err)
	assert.Equal(t, "multipart/report", mt)
	assert.Equal(t, "delivery-status", params["report-type"])

	mr := multipart.NewReader(msg.Body, params["boundary"])
	types := map[string]string{}

	for {
		p, perr := mr.NextPart()
		if perr == io.EOF {
			break
		}

		require.NoError(t, perr)

		body, _ := io.ReadAll(p)
		types[p.Header.Get("Content-Type")] = string(body)
	}

	ds := types["message/delivery-status"]
	assert.Contains(t, ds, "Reporting-MTA: dns; inbound-smtp.us-east-1.amazonaws.com")
	assert.Contains(t, ds, "Final-Recipient: rfc822; user@example.com")
	assert.Contains(t, ds, "Action: failed")
	assert.Contains(t, ds, "Status: 5.1.1")
	assert.Contains(t, ds, "Diagnostic-Code: smtp; 550 mailbox unavailable")
	assert.Contains(t, types["text/rfc822-headers"], "Subject: Hello")
}

func TestInboundS3KMS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		kms     *fakeKMS
		name    string
		wantErr string
		noMeta  bool
		wantObj bool
	}{
		{name: "encrypted", kms: &fakeKMS{}, wantObj: true},
		{name: "kms failure", kms: &fakeKMS{err: assert.AnError}, wantErr: "kms generate data key"},
		{name: "store without metadata", kms: &fakeKMS{}, noMeta: true, wantErr: "no KMS backend wired"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ms := &metaStore{objs: map[string][]byte{}, meta: map[string]map[string]string{}}
			b := ses.NewInMemoryBackend()

			if tc.noMeta {
				b.SetInboundObjectStore(&ms.fakeInboundStore)
			} else {
				b.SetInboundObjectStore(ms)
			}

			b.SetInboundKMS(tc.kms)

			_, err := b.VerifyDomainIdentity("example.com")
			require.NoError(t, err)
			require.NoError(t, b.CreateReceiptRuleSet("rs"))
			require.NoError(t, b.CreateReceiptRule("rs", ses.ReceiptRule{
				Name: "r", Enabled: true, Actions: []ses.ReceiptAction{
					{
						Type:         ses.ReceiptActionTypeS3,
						S3BucketName: "inbox",
						S3KMSKeyARN:  "arn:aws:kms:us-east-1:123456789012:key/k",
					},
				},
			}, ""))
			require.NoError(t, b.SetActiveReceiptRuleSet("rs"))

			h := ses.NewHandler(b).WithInbound(ses.InboundConfig{Addr: "127.0.0.1:0"})
			require.NoError(t, h.StartWorker(t.Context()))
			t.Cleanup(func() { h.Shutdown(context.Background()) })

			require.NoError(t, sendPlain(h.InboundAddr(), "sender@sender.test", "user@example.com", inboundBody))

			log := b.ListInboundMessages()
			require.Len(t, log, 1)

			if tc.wantErr != "" {
				assert.Contains(t, log[0].Executions[0].Error, tc.wantErr)
				assert.Empty(t, ms.snapshot())

				return
			}

			objs := ms.snapshot()
			require.Len(t, objs, 1)

			for k, ct := range objs {
				meta := ms.meta[k]
				assert.Equal(t, "AES/GCM/NoPadding", meta["x-amz-cek-alg"])
				assert.Equal(t, "kms", meta["x-amz-wrap-alg"])
				assert.NotContains(t, string(ct), "body line")

				var mat map[string]string
				require.NoError(t, json.Unmarshal([]byte(meta["x-amz-matdesc"]), &mat))
				assert.Equal(t, "r", mat["aws:ses:rule-name"])
				assert.Equal(t, log[0].MessageID, mat["aws:ses:message-id"])

				wrapped, werr := base64.StdEncoding.DecodeString(meta["x-amz-key-v2"])
				require.NoError(t, werr)
				assert.Equal(t, "wrapped-key", string(wrapped))

				var iv []byte
				iv, werr = base64.StdEncoding.DecodeString(meta["x-amz-iv"])
				require.NoError(t, werr)

				blk, werr := aes.NewCipher([]byte(strings.Repeat("k", 32)))
				require.NoError(t, werr)

				gcm, werr := cipher.NewGCM(blk)
				require.NoError(t, werr)

				pt, werr := gcm.Open(nil, iv, ct, nil)
				require.NoError(t, werr)
				assert.Equal(t, inboundBody, string(pt))
			}
		})
	}
}

func TestInboundRegionalListener(t *testing.T) {
	t.Parallel()

	store := &fakeInboundStore{objs: map[string][]byte{}}
	home := ses.NewInMemoryBackend()
	home.SetInboundObjectStore(store)

	h := ses.NewHandler(home).WithInbound(ses.InboundConfig{
		Addr: "127.0.0.1:0", RegionAddrs: ses.ParseInboundRegions("us-west-2=127.0.0.1:0"),
	})
	h.EnableRegions()

	west, ok := h.RegionHandler("us-west-2").Backend.(*ses.InMemoryBackend)
	require.True(t, ok)

	_, err := west.VerifyDomainIdentity("example.com")
	require.NoError(t, err)
	require.NoError(t, west.CreateReceiptRuleSet("rs"))
	require.NoError(t, west.CreateReceiptRule("rs", ses.ReceiptRule{
		Name: "r", Enabled: true,
		Actions: []ses.ReceiptAction{{Type: ses.ReceiptActionTypeS3, S3BucketName: "west"}},
	}, ""))
	require.NoError(t, west.SetActiveReceiptRuleSet("rs"))

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	require.Error(t, sendPlain(h.InboundAddr(), "sender@sender.test", "user@example.com", inboundBody))
	assert.Empty(t, store.snapshot())

	addr := h.InboundRegionAddr("us-west-2")
	require.NotEmpty(t, addr)
	require.NoError(t, sendPlain(addr, "sender@sender.test", "user@example.com", inboundBody))
	assert.Len(t, store.snapshot(), 1)
	assert.Len(t, west.ListInboundMessages(), 1)
	assert.Empty(t, home.ListInboundMessages())
}
