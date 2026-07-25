package sns

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1" //nolint:gosec // G505: AWS SNS SignatureVersion=1 spec requires SHA-1
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"
)

// certURL returns the URL where certPEM is currently served.
func (s *notificationSigner) certURL() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.certURLValue
}

// setCertURL updates the URL where certPEM is served.
func (s *notificationSigner) setCertURL(u string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.certURLValue = u
}

// newNotificationSigner generates a fresh RSA-2048 key pair and a self-signed
// x.509 certificate. The returned signer is valid for the lifetime of the
// backend instance. region is used to construct the default certURL so the
// embedded SigningCertURL reflects the correct AWS region.
func newNotificationSigner(region string) *notificationSigner {
	key, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	if err != nil {
		// Key generation failure is unrecoverable; panic with a clear message
		// rather than silently falling back to mock signatures.
		panic("sns: failed to generate RSA key for notification signing: " + err.Error())
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Gopherstack SNS Mock"},
			CommonName:   "SimpleNotificationService",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		panic("sns: failed to create self-signed cert: " + err.Error())
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	return &notificationSigner{
		privateKey: key,
		certPEM:    certPEM,
		// certURLValue is set later via SetSigningCertBaseURL when the server address is known.
		// The initial value uses the backend region so URLs are correct before
		// SetSigningCertBaseURL is called (e.g. in non-HTTP test scenarios).
		certURLValue: fmt.Sprintf("https://sns.%s.amazonaws.com/SimpleNotificationService.pem", region),
	}
}

// sign computes the RSA-SHA256 signature of the canonical notification string
// per AWS SNS SignatureVersion=2 and returns it base64-encoded.
func (s *notificationSigner) sign(canonical string) string {
	// codeql[go/insecure-password-hashing] False positive: digital signature, not password hashing.
	h := sha256.Sum256([]byte(canonical))
	sig, err := rsa.SignPKCS1v15(rand.Reader, s.privateKey, crypto.SHA256, h[:])
	if err != nil {
		return "SIGN-ERROR"
	}

	return base64.StdEncoding.EncodeToString(sig)
}

// signSHA1 computes the RSA-SHA1 signature of the canonical notification string
// per AWS SNS SignatureVersion=1 (the AWS default when a topic/subscription does
// not explicitly set SignatureVersion=2) and returns it base64-encoded.
func (s *notificationSigner) signSHA1(canonical string) string {
	//nolint:gosec // G401: AWS SNS SignatureVersion=1 spec requires SHA-1, not a password hash.
	h := sha1.Sum([]byte(canonical)) // codeql[go/insecure-password-hashing] signature, not a password
	sig, err := rsa.SignPKCS1v15(rand.Reader, s.privateKey, crypto.SHA1, h[:])
	if err != nil {
		return "SIGN-ERROR"
	}

	return base64.StdEncoding.EncodeToString(sig)
}

// resolveSignatureVersion normalizes a topic's raw SignatureVersion attribute
// value to the effective AWS SNS signature version: "2" only when the
// attribute is explicitly set to "2"; every other value (including unset/"")
// resolves to "1", matching the real AWS default (SetTopicAttributes API docs:
// "By default, SignatureVersion is set to 1").
func resolveSignatureVersion(attrValue string) string {
	if attrValue == signatureVersion2 {
		return signatureVersion2
	}

	return signatureVersion1
}

// signWithVersion signs canonical using the hash algorithm selected by the
// resolved SignatureVersion ("1" -> SHA1withRSA, anything else -> SHA256withRSA)
// and returns the base64-encoded signature.
func (s *notificationSigner) signWithVersion(canonical, version string) string {
	if version == signatureVersion1 {
		return s.signSHA1(canonical)
	}

	return s.sign(canonical)
}

// canonicalNotificationString builds the string-to-sign for a Notification
// message per the AWS SNS message-signing specification. Fields are included
// in alphabetical order; Subject is omitted when empty.
func canonicalNotificationString(msgID, topicARN, subject, message, timestamp string) string {
	type field struct{ k, v string }
	fields := []field{
		{"Message", message},
		{"MessageId", msgID},
		{"Timestamp", timestamp},
		{topicArnKey, topicARN},
		{"Type", messageTypeNotification},
	}
	if subject != "" {
		fields = append(fields, field{"Subject", subject})
	}

	sort.Slice(fields, func(i, j int) bool { return fields[i].k < fields[j].k })

	var sb strings.Builder
	for _, f := range fields {
		sb.WriteString(f.k)
		sb.WriteByte('\n')
		sb.WriteString(f.v)
		sb.WriteByte('\n')
	}

	return sb.String()
}
