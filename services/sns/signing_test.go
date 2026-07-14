package sns_test

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestIssue4_NotificationSignatureNotMock verifies that HTTP notifications
// contain a non-mock Signature field.
func TestNotificationSignatureNotMock(t *testing.T) {
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
		assert.Equal(t, "2", env.SignatureVersion)
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP delivery did not arrive")
	}
}

// TestIssue4_SignatureIsValidRSASHA256 verifies that the Signature field in an
// HTTP notification can be verified using the backend's signing certificate.
func TestSignatureIsValidRSASHA256(t *testing.T) {
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

	h := sha256.Sum256([]byte(canonical))
	err = rsa.VerifyPKCS1v15(rsaPub, crypto.SHA256, h[:], sigBytes)
	assert.NoError(t, err, "notification signature must verify with the signing cert")
}

// TestIssue4_SubjectIncludedInSignature verifies that the Subject field is
// part of the canonical string when non-empty (AWS spec requirement).
func TestSubjectIncludedInSignature(t *testing.T) {
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
	h := sha256.Sum256([]byte(canonical))
	assert.NoError(t, rsa.VerifyPKCS1v15(rsaPub, crypto.SHA256, h[:], sigBytes))
}

// TestIssue4_CertServedAtPEMEndpoint verifies the Handler serves the signing
// certificate at the SimpleNotificationService.pem path.
func TestCertServedAtPEMEndpoint(t *testing.T) {
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
func TestSigningCertURLConfigurable(t *testing.T) {
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
func TestRawDeliverySkipsEnvelope(t *testing.T) {
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
		assert.Equal(t, "2", env.SignatureVersion)
		assert.NotEmpty(t, env.Signature)
		assert.NotEmpty(t, env.SigningCertURL)
		assert.NotEmpty(t, env.UnsubscribeURL)
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP delivery did not arrive")
	}
}

// TestNotificationURLsReflectRegion verifies that the SigningCertURL and
// UnsubscribeURL embedded in HTTP notifications derive their region from the
// topic ARN rather than a hardcoded us-east-1 (parity §C).
func TestNotificationURLsReflectRegion(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		region string
	}{
		{name: "us-west-2", region: "us-west-2"},
		{name: "eu-central-1", region: "eu-central-1"},
		{name: "ap-southeast-1", region: "ap-southeast-1"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			received := make(chan string, 1)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				received <- string(body)
				w.WriteHeader(http.StatusOK)
			}))
			defer ts.Close()

			b := sns.NewInMemoryBackendWithContext(t.Context(), "000000000000", tc.region)
			tp, err := b.CreateTopic("region-topic", nil)
			require.NoError(t, err)
			require.Contains(t, tp.TopicArn, ":"+tc.region+":",
				"topic ARN should carry the backend region")

			_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
			require.NoError(t, err)

			_, err = b.Publish(tp.TopicArn, "region-test", "", "", nil)
			require.NoError(t, err)

			select {
			case raw := <-received:
				env := parseNotificationEnvelope(t, raw)
				assert.Contains(t, env.SigningCertURL, "sns."+tc.region+".amazonaws.com",
					"SigningCertURL must reflect the request region")
				assert.Contains(t, env.UnsubscribeURL, "sns."+tc.region+".amazonaws.com",
					"UnsubscribeURL must reflect the request region")
				assert.NotContains(t, env.UnsubscribeURL, "us-east-1")
			case <-time.After(2 * time.Second):
				t.Fatal("HTTP delivery did not arrive")
			}
		})
	}
}

func TestCertURLUsesRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		region string
		name   string
	}{
		{name: "us_east_1", region: "us-east-1"},
		{name: "eu_west_1", region: "eu-west-1"},
		{name: "ap_southeast_2", region: "ap-southeast-2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackendWithConfig("000000000000", tt.region)
			certURL := sns.SigningCertURLForTest(b)

			expected := "https://sns." + tt.region + ".amazonaws.com/SimpleNotificationService.pem"
			assert.Equal(t, expected, certURL, "certURL must use backend region, not hardcoded us-east-1")
			assert.Contains(t, certURL, tt.region, "certURL must embed region %s", tt.region)
		})
	}
}
