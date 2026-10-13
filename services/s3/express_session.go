package s3

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// s3ExpressSessionTTL matches real S3 Express One Zone: CreateSession
// credentials are scoped to the bucket and expire after 5 minutes
// (s3@v1.111.0 api_op_CreateSession.go doc comment).
const s3ExpressSessionTTL = 5 * time.Minute

// expressSessionKeyBytes/expressSessionSecretBytes/expressSessionTokenBytes
// size the random components of a generated session credential. Lengths are
// cosmetic (no real client parses them) but kept AWS-shaped (16 hex chars is
// short of a real 20-char AKID, this is a mock, not a forgery target).
const (
	expressSessionKeyBytes    = 8
	expressSessionSecretBytes = 20
	expressSessionTokenBytes  = 32
)

// SessionCredentials is the temporary credential set CreateSession issues,
// scoped to one directory bucket.
type SessionCredentials struct {
	Expiration      time.Time
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// SessionEncryption is the default server-side encryption a CreateSession applies to objects stored through it.
type SessionEncryption struct {
	BucketKeyEnabled *bool
	Algorithm        string
	KMSKeyID         string
	KMSContext       string
}

// expressSession is the backend-side record for a live SessionCredentials,
// keyed by AccessKeyID so verifyHeaderAuth can look it up from the
// Authorization header's Credential without also parsing the session token.
type expressSession struct {
	expiresAt time.Time
	enc       SessionEncryption
	bucket    string
	secret    string
	token     string
	readOnly  bool
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return hex.EncodeToString([]byte(strconv.FormatInt(time.Now().UnixNano(), 10)))
	}

	return hex.EncodeToString(b)
}

// CreateSession issues temporary session credentials scoped to bucketName,
// expiring s3ExpressSessionTTL from now. A ReadOnly session is limited to GET/HEAD requests by resolveSigningSecret.
//
// Deliberately does NOT require bucketName to already exist: the pinned SDK
// (s3@v1.111.0) issues an implicit CreateSession as part of its own identity
// resolution for ANY operation on a directory-bucket-shaped name when a
// custom BaseEndpoint is configured -- including CreateBucket itself, before
// the bucket exists. Confirmed with a real client against a dumping
// httptest.Server: CreateBucket's own bindEndpointParams never sets
// DisableS3ExpressSessionAuth, so with a custom endpoint the SDK's endpoint
// ruleset routes it through the session-credential auth scheme regardless
// (case selection differs only when there is no endpoint override, i.e.
// real, unmodified AWS, where CreateBucket needs no session at all). This is
// a structural client-side quirk of driving S3Express-classified operations
// through a custom endpoint, not a real permission gate this emulator has
// any other way to model -- bucket existence is still enforced by every
// actual operation (CreateBucket, PutObject, etc.), just not by this
// bootstrapping step.
func (b *InMemoryBackend) CreateSession(
	ctx context.Context, bucketName string, mode types.SessionMode,
) (SessionCredentials, error) {
	return b.CreateSessionWithEncryption(ctx, bucketName, mode, SessionEncryption{})
}

// CreateSessionWithEncryption is CreateSession with a session-level default SSE configuration.
func (b *InMemoryBackend) CreateSessionWithEncryption(
	_ context.Context, bucketName string, mode types.SessionMode, enc SessionEncryption,
) (SessionCredentials, error) {
	b.sweepExpiredSessions()

	now := time.Now()
	creds := SessionCredentials{
		AccessKeyID:     "ASIAEXPRESS" + randomHex(expressSessionKeyBytes),
		SecretAccessKey: randomHex(expressSessionSecretBytes),
		SessionToken:    randomHex(expressSessionTokenBytes),
		Expiration:      now.Add(s3ExpressSessionTTL),
	}

	b.expressSessions.Set(creds.AccessKeyID, expressSession{
		enc:       enc,
		bucket:    bucketName,
		secret:    creds.SecretAccessKey,
		token:     creds.SessionToken,
		expiresAt: creds.Expiration,
		readOnly:  mode == types.SessionModeReadOnly,
	})

	return creds, nil
}

// sweepExpiredSessions bounds the session store's size: every CreateSession
// call drops any entry that has since expired, so a client that keeps
// requesting new sessions without ever letting them expire in-process cannot
// leak memory beyond one entry per live 5-minute window.
func (b *InMemoryBackend) sweepExpiredSessions() {
	now := time.Now()

	var expired []string
	b.expressSessions.Range(func(k string, v expressSession) bool {
		if now.After(v.expiresAt) {
			expired = append(expired, k)
		}

		return true
	})

	for _, k := range expired {
		b.expressSessions.Delete(k)
	}
}

// ExpressSessionSecret looks up a live (non-expired) S3 Express session by
// its AccessKeyID and session token, returning the bucket it is scoped to
// and its secret key. A missing, mismatched, or expired session is reported
// as not found; an expired entry is also deleted.
func (b *InMemoryBackend) ExpressSessionSecret(accessKeyID, sessionToken string) (string, string, bool) {
	sess, found := b.expressSessions.Get(accessKeyID)
	if !found || sess.token != sessionToken {
		return "", "", false
	}

	if time.Now().After(sess.expiresAt) {
		b.expressSessions.Delete(accessKeyID)

		return "", "", false
	}

	return sess.bucket, sess.secret, true
}

// ExpressSessionReadOnly reports whether the live session for accessKeyID was created with SessionMode ReadOnly.
func (b *InMemoryBackend) ExpressSessionReadOnly(accessKeyID string) bool {
	sess, found := b.expressSessions.Get(accessKeyID)

	return found && sess.readOnly
}

// ExpressSessionEncryption returns the SSE defaults of a live session.
func (b *InMemoryBackend) ExpressSessionEncryption(accessKeyID, sessionToken string) (SessionEncryption, bool) {
	sess, found := b.expressSessions.Get(accessKeyID)
	if !found || sess.token != sessionToken || time.Now().After(sess.expiresAt) {
		return SessionEncryption{}, false
	}

	enc := sess.enc
	if enc.BucketKeyEnabled != nil {
		v := *enc.BucketKeyEnabled
		enc.BucketKeyEnabled = &v
	}

	return enc, true
}

// IsDirectoryBucket reports whether bucket is an S3 Express directory
// bucket. Returns false for general-purpose buckets and for buckets that
// don't exist (existence is the caller's own concern).
func (b *InMemoryBackend) IsDirectoryBucket(bucketName string) bool {
	b.mu.RLock("IsDirectoryBucket")
	defer b.mu.RUnlock()

	bucket, err := b.getBucket(bucketName)
	if err != nil {
		return false
	}

	return bucket.IsDirectoryBucket
}

const (
	headerSessionBucketKey = "X-Amz-Server-Side-Encryption-Bucket-Key-Enabled"
	sseAlgorithmAES256     = "AES256"
	sseAlgorithmKMS        = "aws:kms"
	sseContextARNKey       = "aws:s3:arn"
)

// parseSessionEncryption reads and validates the CreateSession SSE headers (s3@v1.111.0 api_op_CreateSession.go).
func parseSessionEncryption(r *http.Request, bucket string) (SessionEncryption, error) {
	enc := SessionEncryption{
		Algorithm:  r.Header.Get(headerSSEAlgorithm),
		KMSKeyID:   r.Header.Get(headerSSEKMSKeyID),
		KMSContext: r.Header.Get(headerSSEKMSEncryptionContext),
	}

	if v := r.Header.Get(headerSessionBucketKey); v != "" {
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			return enc, fmt.Errorf("%w: invalid %s", ErrInvalidArgument, headerSessionBucketKey)
		}

		enc.BucketKeyEnabled = &parsed
	}

	switch enc.Algorithm {
	case "", sseAlgorithmAES256:
	case sseAlgorithmKMS:
		if enc.KMSKeyID == "" {
			return enc, fmt.Errorf(
				"%w: x-amz-server-side-encryption-aws-kms-key-id is required with aws:kms",
				ErrInvalidArgument,
			)
		}
	default:
		return enc, fmt.Errorf("%w: x-amz-server-side-encryption must be AES256 or aws:kms", ErrInvalidArgument)
	}

	if enc.KMSKeyID != "" && enc.Algorithm != sseAlgorithmKMS {
		return enc, fmt.Errorf("%w: a KMS key id requires x-amz-server-side-encryption: aws:kms", ErrInvalidArgument)
	}

	if enc.KMSContext != "" {
		if err := validateSessionEncryptionContext(enc.KMSContext, bucket); err != nil {
			return enc, err
		}
	}

	return enc, nil
}

// validateSessionEncryptionContext accepts only the default context (the bucket ARN) for directory buckets.
func validateSessionEncryptionContext(encoded, bucket string) error {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return fmt.Errorf("%w: encryption context must be base64-encoded JSON", ErrInvalidArgument)
	}

	var kv map[string]string
	if err = json.Unmarshal(raw, &kv); err != nil {
		return fmt.Errorf("%w: encryption context must be a JSON object of strings", ErrInvalidArgument)
	}

	for k, v := range kv {
		if k != sseContextARNKey || !strings.HasSuffix(v, "bucket/"+bucket) {
			return fmt.Errorf(
				"%w: only the default encryption context (the bucket ARN) is supported for directory buckets",
				ErrInvalidArgument,
			)
		}
	}

	return nil
}

func setSessionEncryptionHeaders(w http.ResponseWriter, enc SessionEncryption) {
	if enc.Algorithm != "" {
		w.Header().Set(headerSSEAlgorithm, enc.Algorithm)
	}

	if enc.KMSKeyID != "" {
		w.Header().Set(headerSSEKMSKeyID, enc.KMSKeyID)
	}

	if enc.KMSContext != "" {
		w.Header().Set(headerSSEKMSEncryptionContext, enc.KMSContext)
	}

	if enc.BucketKeyEnabled != nil {
		w.Header().Set(headerSessionBucketKey, strconv.FormatBool(*enc.BucketKeyEnabled))
	}
}

// applySessionEncryptionDefaults fills absent SSE request headers from the session's CreateSession settings.
func (h *S3Handler) applySessionEncryptionDefaults(r *http.Request, accessKeyID string) {
	token := r.Header.Get(headerAmzSessionToken)
	if token == "" || r.Method != http.MethodPut || r.Header.Get(headerSSEAlgorithm) != "" {
		return
	}

	enc, ok := h.Backend.ExpressSessionEncryption(accessKeyID, token)
	if !ok || enc.Algorithm == "" {
		return
	}

	r.Header.Set(headerSSEAlgorithm, enc.Algorithm)

	if enc.KMSKeyID != "" {
		r.Header.Set(headerSSEKMSKeyID, enc.KMSKeyID)
	}

	if enc.KMSContext != "" {
		r.Header.Set(headerSSEKMSEncryptionContext, enc.KMSContext)
	}
}
