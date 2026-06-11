package s3_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// setRequesterPays configures a bucket for Requester-Pays via the handler.
func setRequesterPays(t *testing.T, handler *s3.S3Handler, bucket string) {
	t.Helper()

	body := `<RequestPaymentConfiguration><Payer>Requester</Payer></RequestPaymentConfiguration>`
	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?requestPayment", strings.NewReader(body))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestRequesterPays_Enforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		payerHeader    string
		wantStatus     int
		requesterPays  bool
		wantChargedHdr bool
	}{
		{
			name:          "non_requester_pays_no_header_ok",
			requesterPays: false,
			wantStatus:    http.StatusOK,
		},
		{
			name:          "requester_pays_missing_header_denied",
			requesterPays: true,
			payerHeader:   "",
			wantStatus:    http.StatusForbidden,
		},
		{
			name:           "requester_pays_with_header_ok",
			requesterPays:  true,
			payerHeader:    "requester",
			wantStatus:     http.StatusOK,
			wantChargedHdr: true,
		},
		{
			name:           "requester_pays_header_case_insensitive_ok",
			requesterPays:  true,
			payerHeader:    "Requester",
			wantStatus:     http.StatusOK,
			wantChargedHdr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "rp-bucket")
			mustPutObject(t, backend, "rp-bucket", "obj.txt", []byte("hello"))

			if tt.requesterPays {
				setRequesterPays(t, handler, "rp-bucket")
			}

			req := httptest.NewRequest(http.MethodGet, "/rp-bucket/obj.txt", nil)
			if tt.payerHeader != "" {
				req.Header.Set("X-Amz-Request-Payer", tt.payerHeader)
			}
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantChargedHdr {
				assert.Equal(t, "requester", rec.Header().Get("X-Amz-Request-Charged"))
			}
			if tt.wantStatus == http.StatusForbidden {
				assert.Contains(t, rec.Body.String(), "AccessDenied")
				assert.Contains(t, rec.Body.String(), "Requester Pays")
			}
		})
	}
}

// presignURL builds a presigned GET URL and returns the request, signing with
// the given secret. When tamper is true the signature is corrupted.
func presignedGetRequest(t *testing.T, host, bucket, key, secret string, tamper bool) *http.Request {
	t.Helper()

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	scopeDate := now.Format("20060102")

	const (
		region     = "us-east-1"
		service    = "s3"
		algorithm  = "AWS4-HMAC-SHA256"
		expires    = "3600"
		credSuffix = "/us-east-1/s3/aws4_request"
	)

	credential := "AKIDEXAMPLE/" + scopeDate + credSuffix

	q := url.Values{}
	q.Set("X-Amz-Algorithm", algorithm)
	q.Set("X-Amz-Credential", credential)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", expires)
	q.Set("X-Amz-SignedHeaders", "host")

	rawPath := "/" + bucket + "/" + key

	// Canonical query (sorted, encoded), excluding X-Amz-Signature.
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, testURIEncode(k)+"="+testURIEncode(q.Get(k)))
	}
	canonicalQuery := strings.Join(parts, "&")

	canonicalReq := strings.Join([]string{
		http.MethodGet,
		rawPath,
		canonicalQuery,
		"host:" + host + "\n",
		"host",
		"UNSIGNED-PAYLOAD",
	}, "\n")

	credentialScope := strings.Join([]string{scopeDate, region, service, "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		algorithm,
		amzDate,
		credentialScope,
		testHexSHA256(canonicalReq),
	}, "\n")

	signingKey := testSigningKey(secret, scopeDate, region, service)
	sig := hex.EncodeToString(testHMAC(signingKey, stringToSign))
	if tamper {
		sig = strings.Repeat("0", len(sig))
	}
	q.Set("X-Amz-Signature", sig)

	req := httptest.NewRequest(http.MethodGet, rawPath+"?"+q.Encode(), nil)
	req.Host = host

	return req
}

func TestPresignedSignatureVerification(t *testing.T) {
	t.Parallel()

	const (
		host   = "s3.amazonaws.com"
		bucket = "presign-bucket"
		key    = "obj.txt"
		secret = "test"
	)

	tests := []struct {
		name           string
		wantStatus     int
		enableValidate bool
		tamper         bool
		wrongSecret    bool
	}{
		{
			name:           "validation_off_bad_sig_accepted",
			enableValidate: false,
			tamper:         true,
			wantStatus:     http.StatusOK,
		},
		{
			name:           "validation_on_good_sig_accepted",
			enableValidate: true,
			tamper:         false,
			wantStatus:     http.StatusOK,
		},
		{
			name:           "validation_on_tampered_sig_denied",
			enableValidate: true,
			tamper:         true,
			wantStatus:     http.StatusForbidden,
		},
		{
			name:           "validation_on_wrong_secret_denied",
			enableValidate: true,
			wrongSecret:    true,
			wantStatus:     http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
			handler := s3.NewHandler(backend).WithJanitor(s3.Settings{})
			if tt.enableValidate {
				handler = handler.WithPresignValidation(secret)
			}

			_, err := backend.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)
			mustPutObject(t, backend, bucket, key, []byte("data"))

			signingSecret := secret
			if tt.wrongSecret {
				signingSecret = "wrong-secret"
			}
			req := presignedGetRequest(t, host, bucket, key, signingSecret, tt.tamper)

			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- local SigV4 helpers (mirror the production derivation) -----------------

func testURIEncode(s string) string {
	const unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~"

	var b strings.Builder
	for i := range len(s) {
		c := s[i]
		if strings.IndexByte(unreserved, c) >= 0 {
			b.WriteByte(c)

			continue
		}
		const hexDigits = "0123456789ABCDEF"
		b.WriteByte('%')
		b.WriteByte(hexDigits[c>>4])
		b.WriteByte(hexDigits[c&0x0f])
	}

	return b.String()
}

func testHMAC(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))

	return mac.Sum(nil)
}

func testSigningKey(secret, date, region, service string) []byte {
	kDate := testHMAC([]byte("AWS4"+secret), date)
	kRegion := testHMAC(kDate, region)
	kService := testHMAC(kRegion, service)

	return testHMAC(kService, "aws4_request")
}

func testHexSHA256(s string) string {
	sum := sha256.Sum256([]byte(s))

	return hex.EncodeToString(sum[:])
}
