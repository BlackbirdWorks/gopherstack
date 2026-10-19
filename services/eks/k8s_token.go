package eks

import (
	"context"
	"crypto/hmac"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const (
	// IAMTokenPrefix starts every bearer token produced by `aws eks get-token`.
	IAMTokenPrefix = "k8s-aws-v1." //nolint:gosec // public wire prefix, not a credential

	clusterIDHeader  = "X-K8s-Aws-Id"
	sigSignedHeaders = "host;x-k8s-aws-id"
	stsAction        = "GetCallerIdentity"
	sigService       = "sts"
	sigAlgorithm     = "AWS4-HMAC-SHA256"
	sigDateLayout    = "20060102T150405Z"
	tokenMaxExpires  = 60
	tokenMaxAge      = 15 * time.Minute
	tokenMaxSkew     = 5 * time.Minute
	tokenCredParts   = 5
	emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	unsignedPayload  = "UNSIGNED-PAYLOAD"
)

var (
	errTokenMalformed = errors.New("malformed EKS authentication token")
	errTokenExpired   = errors.New("EKS authentication token expired")
	errTokenSignature = errors.New("EKS authentication token signature mismatch")
)

// CredentialIdentity is the principal an AWS access key resolves to.
type CredentialIdentity struct {
	// PrincipalARN is the GetCallerIdentity ARN (user, assumed-role session, or root).
	PrincipalARN string
	// Secret is the access key's secret; empty means the key is unverifiable (the emulator's root fallback).
	Secret string
}

// CredentialResolver resolves an access key (and optional session token) to its principal and secret.
type CredentialResolver interface {
	ResolveCredential(accessKeyID, sessionToken string) (CredentialIdentity, error)
}

type eksToken struct {
	query        url.Values
	signedAt     time.Time
	scheme       string
	host         string
	accessKeyID  string
	region       string
	signature    string
	sessionToken string
}

func decodeTokenURL(raw string) (*url.URL, error) {
	enc, ok := strings.CutPrefix(raw, IAMTokenPrefix)
	if !ok {
		return nil, errTokenMalformed
	}

	dec, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(enc, "="))
	if err != nil {
		return nil, errTokenMalformed
	}

	u, err := url.Parse(string(dec))
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, errTokenMalformed
	}

	return u, nil
}

func tokenWindow(q url.Values) (time.Time, bool) {
	signedAt, err := time.Parse(sigDateLayout, q.Get("X-Amz-Date"))
	if err != nil {
		return time.Time{}, false
	}

	expires, err := strconv.Atoi(q.Get("X-Amz-Expires"))

	return signedAt, err == nil && expires >= 1 && expires <= tokenMaxExpires
}

func parseEKSToken(raw string) (*eksToken, error) {
	u, err := decodeTokenURL(raw)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	if q.Get("Action") != stsAction || q.Get("X-Amz-Algorithm") != sigAlgorithm ||
		q.Get("X-Amz-SignedHeaders") != sigSignedHeaders || q.Get("X-Amz-Signature") == "" {
		return nil, errTokenMalformed
	}

	signedAt, ok := tokenWindow(q)
	if !ok {
		return nil, errTokenMalformed
	}

	parts := strings.Split(q.Get("X-Amz-Credential"), "/")
	if len(parts) != tokenCredParts || parts[0] == "" || parts[1] != signedAt.Format("20060102") ||
		parts[3] != sigService || parts[4] != "aws4_request" {
		return nil, errTokenMalformed
	}

	return &eksToken{
		query: q, signedAt: signedAt, scheme: u.Scheme, host: u.Host, accessKeyID: parts[0], region: parts[2],
		signature: q.Get("X-Amz-Signature"), sessionToken: q.Get("X-Amz-Security-Token"),
	}, nil
}

func (t *eksToken) checkTime(now time.Time) error {
	if now.After(t.signedAt.Add(tokenMaxAge)) {
		return errTokenExpired
	}

	if t.signedAt.After(now.Add(tokenMaxSkew)) {
		return errTokenMalformed
	}

	return nil
}

// verifySignature re-presigns the GetCallerIdentity request with the real SigV4 presigner and compares signatures.
func (t *eksToken) verifySignature(secret, clusterName string) error {
	q := url.Values{}

	for k, v := range t.query {
		switch k {
		case "X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-SignedHeaders",
			"X-Amz-Signature", "X-Amz-Security-Token":
		default:
			q[k] = v
		}
	}

	creds := aws.Credentials{AccessKeyID: t.accessKeyID, SecretAccessKey: secret, SessionToken: t.sessionToken}

	for _, payloadHash := range []string{emptyPayloadHash, unsignedPayload} {
		req := &http.Request{
			Method: http.MethodGet,
			URL:    &url.URL{Scheme: t.scheme, Host: t.host, Path: "/", RawQuery: q.Encode()},
			Header: http.Header{},
		}

		req.Header.Set(clusterIDHeader, clusterName)

		signed, _, err := v4.NewSigner().PresignHTTP(context.Background(), creds, req, payloadHash,
			sigService, t.region, t.signedAt)
		if err != nil {
			return fmt.Errorf("%w: %w", errTokenSignature, err)
		}

		su, err := url.Parse(signed)
		if err != nil {
			return errTokenSignature
		}

		if hmac.Equal([]byte(su.Query().Get("X-Amz-Signature")), []byte(t.signature)) {
			return nil
		}
	}

	return errTokenSignature
}

// authenticateIAMToken validates an `aws eks get-token` bearer for clusterName and returns the caller's principal.
func authenticateIAMToken(
	res CredentialResolver, raw, clusterName string, now time.Time,
) (CredentialIdentity, string, error) {
	t, err := parseEKSToken(raw)
	if err != nil {
		return CredentialIdentity{}, "", err
	}

	if err = t.checkTime(now); err != nil {
		return CredentialIdentity{}, "", err
	}

	if res == nil {
		return CredentialIdentity{}, "", fmt.Errorf("%w: no credential resolver is wired", errTokenSignature)
	}

	id, err := res.ResolveCredential(t.accessKeyID, t.sessionToken)
	if err != nil {
		return CredentialIdentity{}, "", fmt.Errorf("resolve access key: %w", err)
	}

	if id.Secret != "" {
		if err = t.verifySignature(id.Secret, clusterName); err != nil {
			return CredentialIdentity{}, "", err
		}
	}

	return id, t.accessKeyID, nil
}
