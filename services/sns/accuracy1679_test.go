package sns_test

// accuracy1679_test.go — AWS accuracy tests for SNS per issue #1679.
//
// Each section targets a specific gap identified in the issue. Tests are
// intentionally self-contained and avoid coupling to unrelated functionality.

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha1" // AWS SNS SignatureVersion=1 mandates SHA-1
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newA1679Backend(t *testing.T) *sns.InMemoryBackend {
	t.Helper()

	return sns.NewInMemoryBackendWithContext(t.Context(), "000000000000", "us-east-1")
}

func newA1679Handler(t *testing.T) (*sns.Handler, *sns.InMemoryBackend) {
	t.Helper()

	b := newA1679Backend(t)

	return sns.NewHandler(b), b
}

// doSNSRequest sends a form-encoded SNS API request to the handler and returns
// the recorder. It does NOT require NoError on the handler return because some
// tests assert on error responses.
func doSNSRequest(t *testing.T, h *sns.Handler, vals url.Values) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	body := vals.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	_ = h.Handler()(c)

	return rec
}

// snsNotificationEnvelope is the JSON structure of an SNS HTTP/HTTPS notification.
type snsNotificationEnvelope struct {
	Type             string `json:"Type"`
	MessageID        string `json:"MessageId"`
	TopicArn         string `json:"TopicArn"`
	Subject          string `json:"Subject"`
	Message          string `json:"Message"`
	Timestamp        string `json:"Timestamp"`
	SignatureVersion string `json:"SignatureVersion"`
	Signature        string `json:"Signature"`
	SigningCertURL   string `json:"SigningCertURL"`
	UnsubscribeURL   string `json:"UnsubscribeURL"`
}

// parseNotificationEnvelope unmarshals an SNS HTTP notification body.
func parseNotificationEnvelope(t *testing.T, body string) snsNotificationEnvelope {
	t.Helper()

	var env snsNotificationEnvelope
	require.NoError(t, json.Unmarshal([]byte(body), &env), "invalid SNS notification envelope")

	return env
}

// ---------------------------------------------------------------------------
// Issue 4: HTTP/HTTPS notifications carry real RSA signatures
// ---------------------------------------------------------------------------

// TestIssue4_NotificationSignatureNotMock verifies that HTTP notifications
// contain a non-mock Signature field.
func TestIssue4_NotificationSignatureNotMock(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("sig-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	_, err = b.Publish(tp.TopicArn, "hello-sig", "", "", nil)
	require.NoError(t, err)

	select {
	case raw := <-received:
		env := parseNotificationEnvelope(t, raw)
		assert.NotEqual(t, "MOCK-SIGNATURE", env.Signature, "Signature must not be the placeholder")
		assert.NotEmpty(t, env.Signature, "Signature must be non-empty")
		assert.Equal(t, "1", env.SignatureVersion)
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP delivery did not arrive")
	}
}

// TestIssue4_SignatureIsValidRSASHA1 verifies that the Signature field in an
// HTTP notification can be verified using the backend's signing certificate.
func TestIssue4_SignatureIsValidRSASHA1(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("sig-verify-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	_, err = b.Publish(tp.TopicArn, "verify-me", "", "", nil)
	require.NoError(t, err)

	var raw string
	select {
	case raw = <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP delivery did not arrive")
	}

	env := parseNotificationEnvelope(t, raw)

	// Parse the backend's signing certificate.
	certPEM := b.SigningCertPEM()
	require.NotEmpty(t, certPEM)

	block, _ := pem.Decode(certPEM)
	require.NotNil(t, block, "SigningCertPEM must be a valid PEM block")

	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	rsaPub, ok := cert.PublicKey.(*rsa.PublicKey)
	require.True(t, ok, "signing cert must contain an RSA public key")

	// Rebuild the canonical string and verify.
	canonical := sns.CanonicalNotificationStringForTest(
		env.MessageID, env.TopicArn, env.Subject, env.Message, env.Timestamp,
	)

	sigBytes, err := base64.StdEncoding.DecodeString(env.Signature)
	require.NoError(t, err, "Signature must be valid base64")

	h := sha1.Sum([]byte(canonical)) // AWS SNS SignatureVersion=1 mandates SHA-1
	err = rsa.VerifyPKCS1v15(rsaPub, crypto.SHA1, h[:], sigBytes)
	assert.NoError(t, err, "notification signature must verify with the signing cert")
}

// TestIssue4_SubjectIncludedInSignature verifies that the Subject field is
// part of the canonical string when non-empty (AWS spec requirement).
func TestIssue4_SubjectIncludedInSignature(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("subj-sig-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	_, err = b.Publish(tp.TopicArn, "subj-message", "MySubject", "", nil)
	require.NoError(t, err)

	var raw string
	select {
	case raw = <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP delivery did not arrive")
	}

	env := parseNotificationEnvelope(t, raw)
	assert.Equal(t, "MySubject", env.Subject)

	certPEM := b.SigningCertPEM()
	block, _ := pem.Decode(certPEM)
	cert, _ := x509.ParseCertificate(block.Bytes)
	rsaPub := cert.PublicKey.(*rsa.PublicKey)

	canonical := sns.CanonicalNotificationStringForTest(
		env.MessageID, env.TopicArn, env.Subject, env.Message, env.Timestamp,
	)

	sigBytes, _ := base64.StdEncoding.DecodeString(env.Signature)
	h := sha1.Sum([]byte(canonical)) // AWS SNS SignatureVersion=1 mandates SHA-1
	assert.NoError(t, rsa.VerifyPKCS1v15(rsaPub, crypto.SHA1, h[:], sigBytes))
}

// TestIssue4_CertServedAtPEMEndpoint verifies the Handler serves the signing
// certificate at the SimpleNotificationService.pem path.
func TestIssue4_CertServedAtPEMEndpoint(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/SimpleNotificationService.pem", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "-----BEGIN CERTIFICATE-----")

	// Parse to confirm it is a valid DER certificate wrapped in PEM.
	block, _ := pem.Decode([]byte(body))
	require.NotNil(t, block)
	_, err = x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	// Confirm it matches the backend's SigningCertPEM output.
	assert.Equal(t, string(b.SigningCertPEM()), body)
}

// TestIssue4_SigningCertURLConfigurable verifies that SetSigningCertBaseURL
// changes the SigningCertURL embedded in notifications.
func TestIssue4_SigningCertURLConfigurable(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newA1679Backend(t)
	b.SetSigningCertBaseURL("http://mock-sns.local")

	tp, err := b.CreateTopic("cert-url-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	_, err = b.Publish(tp.TopicArn, "cert-url-test", "", "", nil)
	require.NoError(t, err)

	select {
	case raw := <-received:
		env := parseNotificationEnvelope(t, raw)
		assert.Equal(t, "http://mock-sns.local/SimpleNotificationService.pem", env.SigningCertURL)
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP delivery did not arrive")
	}
}

// TestIssue4_RawDeliverySkipsEnvelope confirms that raw delivery mode does not
// wrap in the envelope and therefore does not include a signature.
func TestIssue4_RawDeliverySkipsEnvelope(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("raw-sig-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RawMessageDelivery", "true")
	require.NoError(t, err)

	_, err = b.Publish(tp.TopicArn, "raw-payload", "", "", nil)
	require.NoError(t, err)

	select {
	case raw := <-received:
		// Raw delivery: body is the message itself, not a JSON envelope.
		assert.Equal(t, "raw-payload", raw)
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP delivery did not arrive")
	}
}

// ---------------------------------------------------------------------------
// Issue 1 + 2: FIFO dedup in PublishBatch and ContentBasedDeduplication
// ---------------------------------------------------------------------------

// TestIssue1_PublishBatchFIFODedup verifies deduplication within the 5-minute
// window for FIFO topics using PublishBatch.
func TestIssue1_PublishBatchFIFODedup(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("fifo-batch-dedup.fifo", nil)
	require.NoError(t, err)

	// First batch with dedup ID "d1".
	rec := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e1"},
		"PublishBatchRequestEntries.member.1.Message":                {"msg1"},
		"PublishBatchRequestEntries.member.1.MessageGroupId":         {"g1"},
		"PublishBatchRequestEntries.member.1.MessageDeduplicationId": {"d1"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Id>e1</Id>")
	// No failed entries.
	assert.NotContains(t, rec.Body.String(), "<Code>")

	// Same dedup ID → second batch must treat it as duplicate (success, no re-publish).
	rec2 := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e2"},
		"PublishBatchRequestEntries.member.1.Message":                {"msg1"},
		"PublishBatchRequestEntries.member.1.MessageGroupId":         {"g1"},
		"PublishBatchRequestEntries.member.1.MessageDeduplicationId": {"d1"},
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
	// Duplicate is returned as a success entry per AWS spec.
	assert.Contains(t, rec2.Body.String(), "Successful")
	assert.NotContains(t, rec2.Body.String(), "<Code>")
}

// TestIssue2_ContentBasedDeduplicationBatch verifies that CBD works in
// PublishBatch: explicit MessageDeduplicationId is rejected when CBD enabled.
func TestIssue2_ContentBasedDeduplicationBatch(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("cbd-batch.fifo", map[string]string{
		"ContentBasedDeduplication": "true",
	})
	require.NoError(t, err)

	// Explicit dedup ID with CBD enabled → invalid parameter.
	rec := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e1"},
		"PublishBatchRequestEntries.member.1.Message":                {"some-msg"},
		"PublishBatchRequestEntries.member.1.MessageGroupId":         {"g1"},
		"PublishBatchRequestEntries.member.1.MessageDeduplicationId": {"forbidden"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	// Entry-level failure expected.
	assert.Contains(t, rec.Body.String(), "Failed")
	assert.Contains(t, rec.Body.String(), "InvalidParameter")
}

// TestIssue2_ContentBasedDeduplicationBatchDedup verifies same-body messages
// to a CBD FIFO topic are deduplicated in PublishBatch.
func TestIssue2_ContentBasedDeduplicationBatchDedup(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("cbd-dedup-batch.fifo", map[string]string{
		"ContentBasedDeduplication": "true",
	})
	require.NoError(t, err)

	// First publish.
	rec1 := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e1"},
		"PublishBatchRequestEntries.member.1.Message":        {"dup-body"},
		"PublishBatchRequestEntries.member.1.MessageGroupId": {"g1"},
	})
	assert.Equal(t, http.StatusOK, rec1.Code)
	assert.NotContains(t, rec1.Body.String(), "<Code>")

	// Same body → duplicate within window.
	rec2 := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e2"},
		"PublishBatchRequestEntries.member.1.Message":        {"dup-body"},
		"PublishBatchRequestEntries.member.1.MessageGroupId": {"g1"},
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.NotContains(t, rec2.Body.String(), "<Code>")
}

// TestIssue2_FIFORequiresDedupID verifies that a FIFO topic without CBD
// requires MessageDeduplicationId on PublishBatch entries.
func TestIssue2_FIFORequiresDedupID(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("nodedupid.fifo", nil)
	require.NoError(t, err)

	rec := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e1"},
		"PublishBatchRequestEntries.member.1.Message":        {"msg"},
		"PublishBatchRequestEntries.member.1.MessageGroupId": {"g1"},
		// No MessageDeduplicationId.
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Failed")
	assert.Contains(t, rec.Body.String(), "InvalidParameter")
}

// ---------------------------------------------------------------------------
// Issue 3: KMS master key ID validation
// ---------------------------------------------------------------------------

// TestIssue3_KMSValidAlias verifies that alias-prefixed keys are accepted.
func TestIssue3_KMSValidAlias(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	_, err := b.CreateTopic("kms-alias-topic", map[string]string{
		"KmsMasterKeyId": "alias/aws/sns",
	})
	require.NoError(t, err)
}

// TestIssue3_KMSValidARN verifies that KMS ARNs are accepted.
func TestIssue3_KMSValidARN(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	_, err := b.CreateTopic("kms-arn-topic", map[string]string{
		"KmsMasterKeyId": "arn:aws:kms:us-east-1:000000000000:key/abcdef01-1234-5678-9abc-def012345678",
	})
	require.NoError(t, err)
}

// TestIssue3_KMSInvalidValue verifies that garbage KMS IDs are rejected.
func TestIssue3_KMSInvalidValue(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	_, err := b.CreateTopic("kms-bad-topic", map[string]string{
		"KmsMasterKeyId": "not-a-valid-key-id",
	})
	require.Error(t, err)
}

// TestIssue3_KMSSetTopicAttributeValidation verifies KMS validation via
// SetTopicAttributes.
func TestIssue3_KMSSetTopicAttributeValidation(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("kms-set-attr-topic", nil)
	require.NoError(t, err)

	// Valid alias.
	err = b.SetTopicAttributes(tp.TopicArn, "KmsMasterKeyId", "alias/my-key")
	require.NoError(t, err)

	// Clearing (empty value) is allowed.
	err = b.SetTopicAttributes(tp.TopicArn, "KmsMasterKeyId", "")
	require.NoError(t, err)

	// Invalid value.
	err = b.SetTopicAttributes(tp.TopicArn, "KmsMasterKeyId", "::bad::")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Issue 6: MessageStructure "json" honored in PublishBatch
// ---------------------------------------------------------------------------

// TestIssue6_BatchMessageStructureJSON verifies that each batch entry can
// carry MessageStructure=json and that the per-protocol payload is delivered.
func TestIssue6_BatchMessageStructureJSON(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("batch-json-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)
	_ = sub

	msgJSON := `{"http":"http-specific","default":"fallback"}`

	rec := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e1"},
		"PublishBatchRequestEntries.member.1.Message":          {msgJSON},
		"PublishBatchRequestEntries.member.1.MessageStructure": {"json"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "<Code>")

	select {
	case raw := <-received:
		// The delivered body should be the http-protocol-specific value.
		var env snsNotificationEnvelope
		if json.Unmarshal([]byte(raw), &env) == nil {
			assert.Equal(t, "http-specific", env.Message)
		} else {
			assert.Equal(t, "http-specific", raw)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP delivery did not arrive")
	}
}

// TestIssue6_BatchMessageStructureDefault verifies that batch entries without
// explicit MessageStructure are delivered as plain text.
func TestIssue6_BatchMessageStructureDefault(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("batch-plain-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	rec := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e1"},
		"PublishBatchRequestEntries.member.1.Message": {"plain-text"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "<Code>")

	select {
	case raw := <-received:
		var env snsNotificationEnvelope
		if json.Unmarshal([]byte(raw), &env) == nil {
			assert.Equal(t, "plain-text", env.Message)
		} else {
			assert.Equal(t, "plain-text", raw)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP delivery did not arrive")
	}
}

// ---------------------------------------------------------------------------
// Issue 7 + 13: Filter policy validation and numeric operator pre-validation
// ---------------------------------------------------------------------------

// TestIssue7_FilterPolicyMalformedJSON verifies that a non-JSON filter policy
// is rejected at subscribe time.
func TestIssue7_FilterPolicyMalformedJSON(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("fp-mal-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(tp.TopicArn, "sqs",
		"arn:aws:sqs:us-east-1:000000000000:q", "not-json")
	require.Error(t, err)
}

// TestIssue7_FilterPolicyNotAnObject verifies that an array root is rejected.
func TestIssue7_FilterPolicyNotAnObject(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("fp-array-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(tp.TopicArn, "sqs",
		"arn:aws:sqs:us-east-1:000000000000:q", `["not","an","object"]`)
	require.Error(t, err)
}

// TestIssue7_FilterPolicyConditionNotArray verifies that attribute values must
// be arrays.
func TestIssue7_FilterPolicyConditionNotArray(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("fp-cond-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(tp.TopicArn, "sqs",
		"arn:aws:sqs:us-east-1:000000000000:q",
		`{"attr":"not-an-array"}`)
	require.Error(t, err)
}

// TestIssue7_FilterPolicyUnknownOperator verifies that unknown operators are
// rejected at subscribe time.
func TestIssue7_FilterPolicyUnknownOperator(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("fp-op-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(tp.TopicArn, "sqs",
		"arn:aws:sqs:us-east-1:000000000000:q",
		`{"attr":[{"bogus-operator":"value"}]}`)
	require.Error(t, err)
}

// TestIssue7_FilterPolicyConditionLimit verifies the 150-condition cap.
func TestIssue7_FilterPolicyConditionLimit(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("fp-limit-topic", nil)
	require.NoError(t, err)

	// Build a filter policy with 151 conditions spread across attributes.
	parts := make([]string, 0, 151)
	for i := range 151 {
		parts = append(parts, fmt.Sprintf(`"v%d"`, i))
	}
	policy := fmt.Sprintf(`{"attr":[%s]}`, strings.Join(parts, ","))

	_, err = b.Subscribe(tp.TopicArn, "sqs",
		"arn:aws:sqs:us-east-1:000000000000:q", policy)
	require.Error(t, err)
}

// TestIssue7_FilterPolicyValid verifies that a well-formed policy is accepted.
func TestIssue7_FilterPolicyValid(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("fp-valid-topic", nil)
	require.NoError(t, err)

	policy := `{"color":["red","blue"],"size":[{"numeric":[">=",10]}]}`
	_, err = b.Subscribe(tp.TopicArn, "sqs",
		"arn:aws:sqs:us-east-1:000000000000:q", policy)
	require.NoError(t, err)
}

// TestIssue13_NumericOperandValidatedAtSubscribeTime verifies that a malformed
// numeric operand is rejected at subscribe time, not later.
func TestIssue13_NumericOperandValidatedAtSubscribeTime(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("num-op-topic", nil)
	require.NoError(t, err)

	// Missing numeric value after operator.
	_, err = b.Subscribe(tp.TopicArn, "sqs",
		"arn:aws:sqs:us-east-1:000000000000:q",
		`{"price":[{"numeric":[">"]}]}`)
	require.Error(t, err, "malformed numeric operand must be rejected at subscribe time")
}

// TestIssue13_NumericBadOperatorAtSubscribeTime verifies that an unsupported
// numeric operator is rejected at subscribe time.
func TestIssue13_NumericBadOperatorAtSubscribeTime(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("num-bad-op-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(tp.TopicArn, "sqs",
		"arn:aws:sqs:us-east-1:000000000000:q",
		`{"price":[{"numeric":["??",10]}]}`)
	require.Error(t, err)
}

// TestIssue13_NumericValidOperatorsAccepted verifies all valid operators are
// accepted.
func TestIssue13_NumericValidOperatorsAccepted(t *testing.T) {
	t.Parallel()

	validOps := []string{"=", "<>", ">", ">=", "<", "<="}
	b := newA1679Backend(t)

	for i, op := range validOps {
		tp, err := b.CreateTopic(fmt.Sprintf("num-valid-%d", i), nil)
		require.NoError(t, err)

		policy := fmt.Sprintf(`{"val":[{"numeric":[%q,5]}]}`, op)
		_, err = b.Subscribe(tp.TopicArn, "sqs",
			"arn:aws:sqs:us-east-1:000000000000:q", policy)
		require.NoErrorf(t, err, "operator %q should be accepted", op)
	}
}

// ---------------------------------------------------------------------------
// Issue 8: RedrivePolicy validation
// ---------------------------------------------------------------------------

// TestIssue8_RedrivePolicyValidated verifies that a malformed ARN is rejected.
func TestIssue8_RedrivePolicyValidated(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("rdp-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "http",
		"http://example.com", "")
	require.NoError(t, err)

	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy",
		`{"deadLetterTargetArn":"not-a-valid-arn"}`)
	require.Error(t, err)
}

// TestIssue8_RedrivePolicyRequiresDeadLetterTargetArn verifies the required field.
func TestIssue8_RedrivePolicyRequiresDeadLetterTargetArn(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("rdp-req-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "http", "http://example.com", "")
	require.NoError(t, err)

	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy",
		`{"noDeadLetterKey":"arn:aws:sqs:us-east-1:000000000000:dlq"}`)
	require.Error(t, err)
}

// TestIssue8_RedrivePolicyValidSQSARN verifies that a valid SQS ARN is accepted.
func TestIssue8_RedrivePolicyValidSQSARN(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("rdp-valid-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "http", "http://example.com", "")
	require.NoError(t, err)

	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy",
		`{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:my-dlq"}`)
	require.NoError(t, err)
}

// TestIssue8_RedrivePolicyRoundTrips verifies the policy is persisted and
// returned via GetSubscriptionAttributes.
func TestIssue8_RedrivePolicyRoundTrips(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("rdp-rt-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "http", "http://example.com", "")
	require.NoError(t, err)

	policy := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq"}`
	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy", policy)
	require.NoError(t, err)

	attrs, err := b.GetSubscriptionAttributes(sub.SubscriptionArn)
	require.NoError(t, err)
	assert.Equal(t, policy, attrs["RedrivePolicy"])
}

// ---------------------------------------------------------------------------
// Issue 10: Typed PublishBatch per-entry errors
// ---------------------------------------------------------------------------

// TestIssue10_BatchEntryIDTooLong verifies the InvalidBatchEntryId code.
func TestIssue10_BatchEntryIDTooLong(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("batch-id-long-topic", nil)
	require.NoError(t, err)

	longID := strings.Repeat("x", 81)

	rec := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {longID},
		"PublishBatchRequestEntries.member.1.Message": {"msg"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidBatchEntryId")
}

// TestIssue10_BatchDuplicateIDs verifies the BatchEntryIdsNotDistinct code.
func TestIssue10_BatchDuplicateIDs(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("batch-dup-id-topic", nil)
	require.NoError(t, err)

	rec := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"same-id"},
		"PublishBatchRequestEntries.member.1.Message": {"msg1"},
		"PublishBatchRequestEntries.member.2.Id":      {"same-id"},
		"PublishBatchRequestEntries.member.2.Message": {"msg2"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "BatchEntryIdsNotDistinct")
}

// TestIssue10_BatchTooManyEntries verifies the TooManyEntriesInBatchRequest code.
func TestIssue10_BatchTooManyEntries(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("batch-too-many-topic", nil)
	require.NoError(t, err)

	vals := url.Values{
		"Action":   {"PublishBatch"},
		"TopicArn": {tp.TopicArn},
	}
	for i := 1; i <= sns.ExportedMaxPublishBatchEntries+1; i++ {
		vals.Set(fmt.Sprintf("PublishBatchRequestEntries.member.%d.Id", i),
			fmt.Sprintf("e%d", i))
		vals.Set(fmt.Sprintf("PublishBatchRequestEntries.member.%d.Message", i),
			"msg")
	}

	rec := doSNSRequest(t, h, vals)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "TooManyEntriesInBatchRequest")
}

// TestIssue10_BatchBodyTooLarge verifies the BatchRequestTooLong error.
func TestIssue10_BatchBodyTooLarge(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("batch-large-topic", nil)
	require.NoError(t, err)

	// 256 KiB + 1 byte triggers the limit.
	bigMsg := strings.Repeat("a", 256*1024+1)

	rec := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e1"},
		"PublishBatchRequestEntries.member.1.Message": {bigMsg},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "BatchRequestTooLong")
}

// ---------------------------------------------------------------------------
// Issue 11: Message attribute DataType validation
// ---------------------------------------------------------------------------

// TestIssue11_MessageAttributeInvalidDataType verifies that unsupported
// DataTypes are rejected at publish time.
func TestIssue11_MessageAttributeInvalidDataType(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("attr-dt-topic", nil)
	require.NoError(t, err)

	rec := doSNSRequest(t, h, url.Values{
		"Action":                         {"Publish"},
		"TopicArn":                       {tp.TopicArn},
		"Message":                        {"hello"},
		"MessageAttributes.entry.1.Name": {"myAttr"},
		"MessageAttributes.entry.1.Value.DataType":    {"InvalidType"},
		"MessageAttributes.entry.1.Value.StringValue": {"val"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameter")
}

// TestIssue11_MessageAttributeValidDataTypes verifies all supported types.
func TestIssue11_MessageAttributeValidDataTypes(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("attr-valid-dt-topic", nil)
	require.NoError(t, err)

	for _, dt := range []string{"String", "Number", "Binary", "String.Array", "String.Custom"} {
		attrs := map[string]sns.MessageAttribute{
			"attr1": {DataType: dt, StringValue: "value"},
		}
		_, err = b.Publish(tp.TopicArn, "msg", "", "", attrs)
		require.NoErrorf(t, err, "DataType %q should be accepted", dt)
	}
}

// TestIssue11_MessageAttributeNameTooLong verifies max attribute name length.
func TestIssue11_MessageAttributeNameTooLong(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("attr-long-name-topic", nil)
	require.NoError(t, err)

	longName := strings.Repeat("x", 257)
	attrs := map[string]sns.MessageAttribute{
		longName: {DataType: "String", StringValue: "value"},
	}
	_, err = b.Publish(tp.TopicArn, "msg", "", "", attrs)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Goroutine context-cancellation (optimization: workerSem + svcCtx)
// ---------------------------------------------------------------------------

// TestGoroutineContextCancellationDrains verifies that goroutines waiting on
// the semaphore exit promptly when the context is cancelled rather than
// blocking indefinitely.
func TestGoroutineContextCancellationDrains(t *testing.T) {
	t.Parallel()

	// Use a slow HTTP server to keep delivery slots occupied so the extra goroutines
	// block on the semaphore, exercising the context-cancel escape path.
	slow := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-slow:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	cancelCh := make(chan struct{})
	doneCh := make(chan struct{})

	b := sns.NewInMemoryBackendWithContext(t.Context(), "000000000000", "us-east-1")
	tp, err := b.CreateTopic("ctx-cancel-topic", nil)
	require.NoError(t, err)

	// Subscribe more endpoints than the concurrency cap (8) so goroutines block.
	for range 20 {
		_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
		require.NoError(t, err)
	}

	go func() {
		defer close(doneCh)
		_, _ = b.Publish(tp.TopicArn, "drain-test", "", "", nil)
	}()

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish did not return in time")
	}

	// Unblock the slow server so in-flight goroutines can complete.
	close(cancelCh)
	close(slow)

	// WaitDeliveries must return within a reasonable time.
	waitDone := make(chan struct{})
	go func() {
		b.WaitDeliveries()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		// All delivery goroutines completed.
	case <-time.After(5 * time.Second):
		t.Fatal("WaitDeliveries did not return (possible goroutine leak)")
	}
}

// ---------------------------------------------------------------------------
// Periodic FIFO dedup sweep
// ---------------------------------------------------------------------------

// TestFifoDedupSweepIntervalExported verifies that the sweep interval constant
// is exported and has the expected value.
func TestFifoDedupSweepIntervalExported(t *testing.T) {
	t.Parallel()

	assert.Equal(t, time.Minute, sns.ExportedFifoDedupSweepInterval)
}

// ---------------------------------------------------------------------------
// Canonical notification string
// ---------------------------------------------------------------------------

// TestCanonicalNotificationString_Ordering verifies that fields are sorted
// alphabetically (Message, MessageId, Timestamp, TopicArn, Type).
func TestCanonicalNotificationString_Ordering(t *testing.T) {
	t.Parallel()

	canonical := sns.CanonicalNotificationStringForTest(
		"msg-id", "arn:aws:sns:us-east-1:0:topic", "", "hello", "2026-01-01T00:00:00Z",
	)

	// Fields must appear in alphabetical order.
	msgIdx := strings.Index(canonical, "Message\n")
	msgIDIdx := strings.Index(canonical, "MessageId\n")
	timestampIdx := strings.Index(canonical, "Timestamp\n")
	topicIdx := strings.Index(canonical, "TopicArn\n")
	typeIdx := strings.Index(canonical, "Type\n")

	assert.Less(t, msgIdx, msgIDIdx, "Message before MessageId")
	assert.Less(t, msgIDIdx, timestampIdx, "MessageId before Timestamp")
	assert.Less(t, timestampIdx, topicIdx, "Timestamp before TopicArn")
	assert.Less(t, topicIdx, typeIdx, "TopicArn before Type")
}

// TestCanonicalNotificationString_SubjectIncluded verifies Subject is included
// when non-empty.
func TestCanonicalNotificationString_SubjectIncluded(t *testing.T) {
	t.Parallel()

	canonical := sns.CanonicalNotificationStringForTest(
		"id", "arn", "MySubject", "msg", "ts",
	)
	assert.Contains(t, canonical, "Subject\nMySubject\n")
}

// TestCanonicalNotificationString_SubjectOmitted verifies Subject is omitted
// when empty.
func TestCanonicalNotificationString_SubjectOmitted(t *testing.T) {
	t.Parallel()

	canonical := sns.CanonicalNotificationStringForTest(
		"id", "arn", "", "msg", "ts",
	)
	assert.NotContains(t, canonical, "Subject\n")
}

// ---------------------------------------------------------------------------
// Filter-policy matching correctness (regression suite)
// ---------------------------------------------------------------------------

// TestFilterPolicyAnythingButMatchDelivery verifies "anything-but" delivers
// to subscribers when the attribute does NOT match the exclusion list.
func TestFilterPolicyAnythingButMatchDelivery(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("fp-ab-topic", nil)
	require.NoError(t, err)

	fp := `{"color":[{"anything-but":["red","green"]}]}`
	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, fp)
	require.NoError(t, err)

	// "blue" is not in the exclusion list → should be delivered.
	attrs := map[string]sns.MessageAttribute{
		"color": {DataType: "String", StringValue: "blue"},
	}
	_, err = b.Publish(tp.TopicArn, "blue-msg", "", "", attrs)
	require.NoError(t, err)

	select {
	case raw := <-received:
		var env snsNotificationEnvelope
		if json.Unmarshal([]byte(raw), &env) == nil {
			assert.Equal(t, "blue-msg", env.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Message should have been delivered for non-excluded color")
	}
}

// TestFilterPolicyAnythingButNoDelivery verifies "anything-but" blocks
// messages whose attribute matches the exclusion list.
func TestFilterPolicyAnythingButNoDelivery(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("fp-ab-no-topic", nil)
	require.NoError(t, err)

	fp := `{"color":[{"anything-but":["red"]}]}`
	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, fp)
	require.NoError(t, err)

	// "red" matches exclusion → should NOT be delivered.
	attrs := map[string]sns.MessageAttribute{
		"color": {DataType: "String", StringValue: "red"},
	}
	_, err = b.Publish(tp.TopicArn, "red-msg", "", "", attrs)
	require.NoError(t, err)

	select {
	case <-received:
		t.Fatal("Message with excluded color should not have been delivered")
	case <-time.After(300 * time.Millisecond):
		// Expected: no delivery.
	}
}

// TestFilterPolicyPrefixMatch verifies prefix filtering.
func TestFilterPolicyPrefixMatch(t *testing.T) {
	t.Parallel()

	received := make(chan string, 2)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("fp-prefix-topic", nil)
	require.NoError(t, err)

	fp := `{"source":[{"prefix":"order-"}]}`
	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, fp)
	require.NoError(t, err)

	// Matching prefix.
	attrs1 := map[string]sns.MessageAttribute{
		"source": {DataType: "String", StringValue: "order-123"},
	}
	_, err = b.Publish(tp.TopicArn, "order-msg", "", "", attrs1)
	require.NoError(t, err)

	// Non-matching prefix.
	attrs2 := map[string]sns.MessageAttribute{
		"source": {DataType: "String", StringValue: "shipment-123"},
	}
	_, err = b.Publish(tp.TopicArn, "shipment-msg", "", "", attrs2)
	require.NoError(t, err)

	select {
	case raw := <-received:
		var env snsNotificationEnvelope
		if json.Unmarshal([]byte(raw), &env) == nil {
			assert.Equal(t, "order-msg", env.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Prefix-matching message was not delivered")
	}

	// Second message should not arrive.
	select {
	case raw := <-received:
		var env snsNotificationEnvelope
		if json.Unmarshal([]byte(raw), &env) == nil {
			t.Fatalf("non-matching message was delivered: %s", env.Message)
		}
	case <-time.After(300 * time.Millisecond):
		// Expected: no delivery for non-matching.
	}
}

// TestFilterPolicySuffixMatch verifies suffix filtering.
func TestFilterPolicySuffixMatch(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("fp-suffix-topic", nil)
	require.NoError(t, err)

	fp := `{"filename":[{"suffix":".log"}]}`
	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, fp)
	require.NoError(t, err)

	attrs := map[string]sns.MessageAttribute{
		"filename": {DataType: "String", StringValue: "app.log"},
	}
	_, err = b.Publish(tp.TopicArn, "log-msg", "", "", attrs)
	require.NoError(t, err)

	select {
	case raw := <-received:
		var env snsNotificationEnvelope
		if json.Unmarshal([]byte(raw), &env) == nil {
			assert.Equal(t, "log-msg", env.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Suffix-matching message was not delivered")
	}
}

// TestFilterPolicyEqualsIgnoreCase verifies case-insensitive equality.
func TestFilterPolicyEqualsIgnoreCase(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("fp-eic-topic", nil)
	require.NoError(t, err)

	fp := `{"env":[{"equals-ignore-case":"production"}]}`
	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, fp)
	require.NoError(t, err)

	attrs := map[string]sns.MessageAttribute{
		"env": {DataType: "String", StringValue: "PRODUCTION"},
	}
	_, err = b.Publish(tp.TopicArn, "prod-msg", "", "", attrs)
	require.NoError(t, err)

	select {
	case raw := <-received:
		var env snsNotificationEnvelope
		if json.Unmarshal([]byte(raw), &env) == nil {
			assert.Equal(t, "prod-msg", env.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Case-insensitive match was not delivered")
	}
}

// ---------------------------------------------------------------------------
// Concurrent publish safety
// ---------------------------------------------------------------------------

// TestConcurrentPublishSafety verifies that concurrent Publish calls do not
// race on internal backend state.
func TestConcurrentPublishSafety(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("concurrent-topic", nil)
	require.NoError(t, err)

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(i int) {
			defer wg.Done()
			_, _ = b.Publish(tp.TopicArn, fmt.Sprintf("msg-%d", i), "", "", nil)
		}(i)
	}

	wg.Wait()
}

// ---------------------------------------------------------------------------
// HTTPS subscription (treated same as HTTP by the mock)
// ---------------------------------------------------------------------------

// TestHTTPSSubscriptionDelivery verifies that HTTPS subscriptions are delivered
// the same way as HTTP ones.
func TestHTTPSSubscriptionDelivery(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newA1679Backend(t)
	b.SetHTTPDeliveryClient(ts.Client())

	tp, err := b.CreateTopic("https-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(tp.TopicArn, "https", ts.URL, "")
	require.NoError(t, err)

	_, err = b.Publish(tp.TopicArn, "secure-msg", "", "", nil)
	require.NoError(t, err)

	select {
	case raw := <-received:
		env := parseNotificationEnvelope(t, raw)
		assert.Equal(t, "secure-msg", env.Message)
		assert.NotEqual(t, "MOCK-SIGNATURE", env.Signature)
	case <-time.After(2 * time.Second):
		t.Fatal("HTTPS delivery did not arrive")
	}
}

// ---------------------------------------------------------------------------
// Topic attribute round-trips
// ---------------------------------------------------------------------------

// TestTopicAttributeRoundTrips verifies that setting and getting topic
// attributes preserves values for all known configurable attributes.
func TestTopicAttributeRoundTrips(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)

	cases := []struct {
		name  string
		attr  string
		value string
	}{
		{"DisplayName", "DisplayName", "My Test Topic"},
		{"HTTPSuccessFeedbackSampleRate", "HTTPSuccessFeedbackSampleRate", "100"},
		{"SQSSuccessFeedbackRoleArn", "SQSSuccessFeedbackRoleArn",
			"arn:aws:iam::000000000000:role/sqs-feedback"},
		{"LambdaFailureFeedbackRoleArn", "LambdaFailureFeedbackRoleArn",
			"arn:aws:iam::000000000000:role/lambda-feedback"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tp, err := b.CreateTopic("attr-rt-"+strings.ToLower(tc.name), nil)
			require.NoError(t, err)

			err = b.SetTopicAttributes(tp.TopicArn, tc.attr, tc.value)
			require.NoError(t, err)

			attrs, err := b.GetTopicAttributes(tp.TopicArn)
			require.NoError(t, err)
			assert.Equal(t, tc.value, attrs[tc.attr])
		})
	}
}

// ---------------------------------------------------------------------------
// ArchivePolicy attribute acceptance (Issue 12 — attribute storage only)
// ---------------------------------------------------------------------------

// TestIssue12_ArchivePolicyAccepted verifies that the ArchivePolicy attribute
// is accepted and round-trips via GetTopicAttributes.
func TestIssue12_ArchivePolicyAccepted(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("archive-topic", nil)
	require.NoError(t, err)

	policy := `{"MessageRetentionPeriod":30}`
	err = b.SetTopicAttributes(tp.TopicArn, "ArchivePolicy", policy)
	require.NoError(t, err)

	attrs, err := b.GetTopicAttributes(tp.TopicArn)
	require.NoError(t, err)
	assert.Equal(t, policy, attrs["ArchivePolicy"])
}

// TestIssue12_ArchivePolicyOnCreate verifies ArchivePolicy can be set at
// topic creation.
func TestIssue12_ArchivePolicyOnCreate(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	_, err := b.CreateTopic("archive-create-topic", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":7}`,
	})
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// MessageStructure=json for single Publish (regression)
// ---------------------------------------------------------------------------

// TestMessageStructureJSONSinglePublish verifies protocol-specific routing for
// the non-batch Publish action.
func TestMessageStructureJSONSinglePublish(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("json-single-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	msgJSON := `{"http":"http-only","default":"fallback"}`
	rec := doSNSRequest(t, h, url.Values{
		"Action":           {"Publish"},
		"TopicArn":         {tp.TopicArn},
		"Message":          {msgJSON},
		"MessageStructure": {"json"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	select {
	case raw := <-received:
		var env snsNotificationEnvelope
		if json.Unmarshal([]byte(raw), &env) == nil {
			assert.Equal(t, "http-only", env.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP delivery did not arrive")
	}
}

// ---------------------------------------------------------------------------
// Envelope field completeness
// ---------------------------------------------------------------------------

// TestNotificationEnvelopeFields verifies that all required SNS notification
// envelope fields are present and non-empty.
func TestNotificationEnvelopeFields(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("envelope-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	_, err = b.Publish(tp.TopicArn, "envelope-test", "", "", nil)
	require.NoError(t, err)

	select {
	case raw := <-received:
		env := parseNotificationEnvelope(t, raw)
		assert.Equal(t, "Notification", env.Type)
		assert.NotEmpty(t, env.MessageID)
		assert.NotEmpty(t, env.TopicArn)
		assert.NotEmpty(t, env.Message)
		assert.NotEmpty(t, env.Timestamp)
		assert.Equal(t, "1", env.SignatureVersion)
		assert.NotEmpty(t, env.Signature)
		assert.NotEmpty(t, env.SigningCertURL)
		assert.NotEmpty(t, env.UnsubscribeURL)
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP delivery did not arrive")
	}
}

// ---------------------------------------------------------------------------
// Subscription DeliveryPolicy attribute (Issue: per-subscription delivery)
// ---------------------------------------------------------------------------

// TestSubscriptionDeliveryPolicyRoundTrips verifies that a subscription-level
// DeliveryPolicy can be set and retrieved via Get/SetSubscriptionAttributes.
func TestSubscriptionDeliveryPolicyRoundTrips(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("delivery-policy-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:dp-q", "")
	require.NoError(t, err)

	policy := `{"healthyRetryPolicy":{"numRetries":5,"minDelayTarget":10,"maxDelayTarget":30}}`
	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "DeliveryPolicy", policy)
	require.NoError(t, err)

	attrs, err := b.GetSubscriptionAttributes(sub.SubscriptionArn)
	require.NoError(t, err)
	assert.Equal(t, policy, attrs["DeliveryPolicy"])
}

// TestSubscriptionDeliveryPolicyClear verifies clearing DeliveryPolicy by
// setting it to empty string removes it from the attribute map.
func TestSubscriptionDeliveryPolicyClear(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("dp-clear-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:dp-clear-q", "")
	require.NoError(t, err)

	policy := `{"healthyRetryPolicy":{"numRetries":3}}`
	require.NoError(t, b.SetSubscriptionAttributes(sub.SubscriptionArn, "DeliveryPolicy", policy))
	require.NoError(t, b.SetSubscriptionAttributes(sub.SubscriptionArn, "DeliveryPolicy", ""))

	attrs, err := b.GetSubscriptionAttributes(sub.SubscriptionArn)
	require.NoError(t, err)
	assert.Empty(t, attrs["DeliveryPolicy"])
}

// ---------------------------------------------------------------------------
// Subscription ReplayPolicy attribute (Issue: archive replay)
// ---------------------------------------------------------------------------

// TestSubscriptionReplayPolicyRoundTrips verifies that a subscription-level
// ReplayPolicy can be set and retrieved.
func TestSubscriptionReplayPolicyRoundTrips(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("replay-policy-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:rp-q", "")
	require.NoError(t, err)

	policy := `{"replayFromTimestamp":"2024-01-01T00:00:00Z"}`
	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy", policy)
	require.NoError(t, err)

	attrs, err := b.GetSubscriptionAttributes(sub.SubscriptionArn)
	require.NoError(t, err)
	assert.Equal(t, policy, attrs["ReplayPolicy"])
}

// TestSubscriptionReplayPolicyClear verifies clearing ReplayPolicy removes it.
func TestSubscriptionReplayPolicyClear(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("rp-clear-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:rp-clear-q", "")
	require.NoError(t, err)

	policy := `{"replayFromTimestamp":"2024-06-01T00:00:00Z"}`
	require.NoError(t, b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy", policy))
	require.NoError(t, b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy", ""))

	attrs, err := b.GetSubscriptionAttributes(sub.SubscriptionArn)
	require.NoError(t, err)
	assert.Empty(t, attrs["ReplayPolicy"])
}

// TestSubscriptionDeliveryAndReplayPolicyCoexist verifies both attributes can
// be set independently and retrieved together.
func TestSubscriptionDeliveryAndReplayPolicyCoexist(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("coexist-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:coexist-q", "")
	require.NoError(t, err)

	dp := `{"healthyRetryPolicy":{"numRetries":2}}`
	rp := `{"replayFromTimestamp":"2025-01-01T00:00:00Z"}`

	require.NoError(t, b.SetSubscriptionAttributes(sub.SubscriptionArn, "DeliveryPolicy", dp))
	require.NoError(t, b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy", rp))

	attrs, err := b.GetSubscriptionAttributes(sub.SubscriptionArn)
	require.NoError(t, err)
	assert.Equal(t, dp, attrs["DeliveryPolicy"])
	assert.Equal(t, rp, attrs["ReplayPolicy"])
}

// ---------------------------------------------------------------------------
// ListSubscriptionsByTopic pagination
// ---------------------------------------------------------------------------

// TestListSubscriptionsByTopicPagination verifies that ListSubscriptionsByTopic
// correctly paginates when a topic has more than 100 subscriptions.
func TestListSubscriptionsByTopicPagination(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("paginate-by-topic", nil)
	require.NoError(t, err)

	for i := range 115 {
		_, subErr := b.Subscribe(tp.TopicArn, "sqs",
			fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:pbt-q%d", i), "")
		require.NoError(t, subErr)
	}

	page1, tok1, err := b.ListSubscriptionsByTopic(tp.TopicArn, "")
	require.NoError(t, err)
	assert.Len(t, page1, 100)
	assert.NotEmpty(t, tok1)

	page2, tok2, err := b.ListSubscriptionsByTopic(tp.TopicArn, tok1)
	require.NoError(t, err)
	assert.Len(t, page2, 15)
	assert.Empty(t, tok2)

	seen := make(map[string]bool, 115)
	for _, s := range append(page1, page2...) {
		assert.False(t, seen[s.SubscriptionArn], "duplicate subscription in pages")
		seen[s.SubscriptionArn] = true
	}
	assert.Len(t, seen, 115)
}

// TestListSubscriptionsByTopicInvalidToken verifies that ListSubscriptionsByTopic
// returns ErrInvalidParameter for a malformed token.
func TestListSubscriptionsByTopicInvalidToken(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("invalid-token-topic", nil)
	require.NoError(t, err)

	_, _, err = b.ListSubscriptionsByTopic(tp.TopicArn, "!!!not-base64!!!")
	require.ErrorIs(t, err, sns.ErrInvalidParameter)
}

// ---------------------------------------------------------------------------
// FIFO MessageGroupId + MessageDeduplicationId ordering
// ---------------------------------------------------------------------------

// TestFIFOMessageGroupIDRequired verifies that publishing to a FIFO topic
// without MessageGroupId returns the appropriate error.
func TestFIFOMessageGroupIDRequired(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("ordering-topic.fifo", map[string]string{"FifoTopic": "true"})
	require.NoError(t, err)

	rec := doSNSRequest(t, h, url.Values{
		"Action":   {"Publish"},
		"TopicArn": {tp.TopicArn},
		"Message":  {"hello"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MessageGroupId")
}

// TestFIFOMessageDeduplicationIDRequired verifies that FIFO topics without
// ContentBasedDeduplication require an explicit MessageDeduplicationId.
func TestFIFOMessageDeduplicationIDRequired(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("dedup-required-topic.fifo", map[string]string{"FifoTopic": "true"})
	require.NoError(t, err)

	rec := doSNSRequest(t, h, url.Values{
		"Action":         {"Publish"},
		"TopicArn":       {tp.TopicArn},
		"Message":        {"hello"},
		"MessageGroupId": {"g1"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MessageDeduplicationId")
}

// TestFIFOContentBasedDeduplicationForbidsExplicitDedupID verifies that
// providing a MessageDeduplicationId when ContentBasedDeduplication is
// enabled returns an error, matching AWS behaviour.
func TestFIFOContentBasedDeduplicationForbidsExplicitDedupID(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("cbd-topic.fifo", map[string]string{
		"FifoTopic":                 "true",
		"ContentBasedDeduplication": "true",
	})
	require.NoError(t, err)

	rec := doSNSRequest(t, h, url.Values{
		"Action":                 {"Publish"},
		"TopicArn":               {tp.TopicArn},
		"Message":                {"hello"},
		"MessageGroupId":         {"g1"},
		"MessageDeduplicationId": {"explicit-id"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MessageDeduplicationId")
}

// ---------------------------------------------------------------------------
// Subscription all-attributes round-trip via handler
// ---------------------------------------------------------------------------

// TestSubscriptionDeliveryPolicyViaHandler verifies DeliveryPolicy flows
// through the HTTP handler (SetSubscriptionAttributes / GetSubscriptionAttributes).
func TestSubscriptionDeliveryPolicyViaHandler(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("handler-dp-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:handler-dp-q", "")
	require.NoError(t, err)

	policy := `{"healthyRetryPolicy":{"numRetries":5,"minDelayTarget":5,"maxDelayTarget":60}}`
	rec := doSNSRequest(t, h, url.Values{
		"Action":          {"SetSubscriptionAttributes"},
		"SubscriptionArn": {sub.SubscriptionArn},
		"AttributeName":   {"DeliveryPolicy"},
		"AttributeValue":  {policy},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSNSRequest(t, h, url.Values{
		"Action":          {"GetSubscriptionAttributes"},
		"SubscriptionArn": {sub.SubscriptionArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "DeliveryPolicy")
	assert.Contains(t, rec2.Body.String(), "numRetries")
}

// TestSubscriptionReplayPolicyViaHandler verifies ReplayPolicy flows
// through the HTTP handler.
func TestSubscriptionReplayPolicyViaHandler(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("handler-rp-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:handler-rp-q", "")
	require.NoError(t, err)

	policy := `{"replayFromTimestamp":"2025-03-01T00:00:00Z"}`
	rec := doSNSRequest(t, h, url.Values{
		"Action":          {"SetSubscriptionAttributes"},
		"SubscriptionArn": {sub.SubscriptionArn},
		"AttributeName":   {"ReplayPolicy"},
		"AttributeValue":  {policy},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSNSRequest(t, h, url.Values{
		"Action":          {"GetSubscriptionAttributes"},
		"SubscriptionArn": {sub.SubscriptionArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "ReplayPolicy")
	assert.Contains(t, rec2.Body.String(), "replayFromTimestamp")
}
