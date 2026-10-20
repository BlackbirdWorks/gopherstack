package app

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"net/smtp"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
)

func putTXT(t *testing.T, c *route53.Client, zoneID, name, value string) {
	t.Helper()

	_, err := c.ChangeResourceRecordSets(t.Context(), &route53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
		ChangeBatch: &r53types.ChangeBatch{Changes: []r53types.Change{{
			Action: r53types.ChangeActionUpsert,
			ResourceRecordSet: &r53types.ResourceRecordSet{
				Name: aws.String(name), Type: r53types.RRTypeTxt, TTL: aws.Int64(60),
				ResourceRecords: []r53types.ResourceRecord{{Value: aws.String(`"` + value + `"`)}},
			},
		}}},
	})
	require.NoError(t, err)
}

func TestSESInboundKMSAndAuthWiring(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ctx := t.Context()
	sesC, s3C, kmsC, r53C := ses.NewFromConfig(fx.cfg), s3.NewFromConfig(fx.cfg), kms.NewFromConfig(fx.cfg), route53.NewFromConfig(fx.cfg)

	key, err := kmsC.CreateKey(ctx, &kms.CreateKeyInput{})
	require.NoError(t, err)

	_, err = s3C.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("inbox")})
	require.NoError(t, err)

	zone, err := r53C.CreateHostedZone(ctx, &route53.CreateHostedZoneInput{
		Name: aws.String("sender.test."), CallerReference: aws.String("ref"),
	})
	require.NoError(t, err)

	zoneID := aws.ToString(zone.HostedZone.Id)
	putTXT(t, r53C, zoneID, "sender.test.", "v=spf1 ip4:127.0.0.0/8 -all")
	putTXT(t, r53C, zoneID, "_dmarc.sender.test.", "v=DMARC1; p=reject")

	_, err = sesC.VerifyDomainIdentity(ctx, &ses.VerifyDomainIdentityInput{Domain: aws.String("example.com")})
	require.NoError(t, err)
	_, err = sesC.CreateReceiptRuleSet(ctx, &ses.CreateReceiptRuleSetInput{RuleSetName: aws.String("rs")})
	require.NoError(t, err)
	_, err = sesC.CreateReceiptRule(ctx, &ses.CreateReceiptRuleInput{
		RuleSetName: aws.String("rs"),
		Rule: &types.ReceiptRule{Name: aws.String("r"), Enabled: true, Actions: []types.ReceiptAction{{
			S3Action: &types.S3Action{BucketName: aws.String("inbox"), KmsKeyArn: key.KeyMetadata.Arn},
		}}},
	})
	require.NoError(t, err)
	_, err = sesC.SetActiveReceiptRuleSet(ctx, &ses.SetActiveReceiptRuleSetInput{RuleSetName: aws.String("rs")})
	require.NoError(t, err)

	sesH, ok := serviceByName(fx.services)["SES"].(*sesbackend.Handler)
	require.True(t, ok)

	sesH.WithInbound(sesbackend.InboundConfig{Addr: "127.0.0.1:0"})
	require.NoError(t, sesH.StartWorker(ctx))
	t.Cleanup(func() { sesH.Shutdown(t.Context()) })

	body := "From: s <sender@sender.test>\r\nTo: user@example.com\r\nSubject: hi\r\n\r\nsecret body\r\n"
	sendInboundMail(t, sesH.InboundAddr(), body)

	sesBk, ok := sesH.Backend.(*sesbackend.InMemoryBackend)
	require.True(t, ok)

	log := sesBk.ListInboundMessages()
	require.Len(t, log, 1)
	assert.Empty(t, log[0].Executions[0].Error)
	assert.Equal(t, "PASS", log[0].SPF)
	assert.Equal(t, "PASS", log[0].DMARC)

	obj, err := s3C.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("inbox"), Key: aws.String(log[0].MessageID)})
	require.NoError(t, err)

	ct := []byte(readAll(t, obj.Body))
	assert.NotContains(t, string(ct), "secret body")

	wrapped, err := base64.StdEncoding.DecodeString(obj.Metadata["x-amz-key-v2"])
	require.NoError(t, err)

	var matdesc map[string]string
	require.NoError(t, json.Unmarshal([]byte(obj.Metadata["x-amz-matdesc"]), &matdesc))

	dec, err := kmsC.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: wrapped, EncryptionContext: matdesc})
	require.NoError(t, err)

	iv, err := base64.StdEncoding.DecodeString(obj.Metadata["x-amz-iv"])
	require.NoError(t, err)

	blk, err := aes.NewCipher(dec.Plaintext)
	require.NoError(t, err)

	gcm, err := cipher.NewGCM(blk)
	require.NoError(t, err)

	pt, err := gcm.Open(nil, iv, ct, nil)
	require.NoError(t, err)
	assert.Equal(t, body, string(pt))
}

func sendInboundMail(t *testing.T, addr, body string) {
	t.Helper()

	c, err := smtp.Dial(addr)
	require.NoError(t, err)

	defer func() { _ = c.Close() }()

	require.NoError(t, c.Mail("sender@sender.test"))
	require.NoError(t, c.Rcpt("user@example.com"))

	w, err := c.Data()
	require.NoError(t, err)

	_, err = w.Write([]byte(body))
	require.NoError(t, err)
	require.NoError(t, w.Close())
}
