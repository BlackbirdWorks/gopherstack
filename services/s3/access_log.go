package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const accessLogDispatchTimeout = 5 * time.Second

// dispatchAccessLog appends an AWS-format access log entry to the target bucket
// when the source bucket has logging configured, giving downstream tooling
// (Athena queries, log analysis tests) a real stream to read rather than just
// storing the LoggingConfig. Real S3 batches and flushes hourly; here one object
// is written per request, in a goroutine so the response isn't held up.
func (h *S3Handler) dispatchAccessLog(
	ctx context.Context,
	r *http.Request,
	sourceBucket, op, key string,
	statusCode int,
	bytesSent int64,
) {
	loggingXML, err := h.Backend.GetBucketLogging(ctx, sourceBucket)
	if err != nil || loggingXML == "" {
		return
	}

	var cfg BucketLoggingStatus
	if uErr := xml.Unmarshal([]byte(loggingXML), &cfg); uErr != nil {
		return
	}
	if cfg.LoggingEnabled == nil || cfg.LoggingEnabled.TargetBucket == "" {
		return
	}

	target := cfg.LoggingEnabled.TargetBucket
	prefix := cfg.LoggingEnabled.TargetPrefix

	// Build the log line outside the goroutine so we capture request state
	// (Method, RemoteAddr, headers) before the handler returns.
	line := formatAccessLogLine(r, sourceBucket, op, key, statusCode, bytesSent)
	logKey := buildAccessLogKey(prefix)

	go func() {
		dispatchCtx, cancel := context.WithTimeout(
			h.notificationDispatchContext(),
			accessLogDispatchTimeout,
		)
		defer cancel()

		if _, putErr := h.Backend.PutObject(dispatchCtx, &awss3.PutObjectInput{
			Bucket:      aws.String(target),
			Key:         aws.String(logKey),
			Body:        bytes.NewReader([]byte(line)),
			ContentType: aws.String("text/plain"),
		}); putErr != nil {
			// Logging failures must never break the source request, but they
			// are worth surfacing for the operator.
			logger.Load(dispatchCtx).WarnContext(
				dispatchCtx,
				"S3 access log dispatch failed",
				"sourceBucket", sourceBucket,
				"targetBucket", target,
				"error", putErr,
			)
		}
	}()
}

// formatAccessLogLine builds a single record in AWS Server Access Log Format.
// We include the fields downstream tooling actually parses; everything else
// is filled with '-' so column counts stay stable.
//
// Field order (space-separated, '-' for unknown):
//
//	bucket-owner src-bucket [timestamp] remote-ip requester request-id
//	operation key request-uri http-status error-code bytes-sent object-size
//	total-time turn-around-time referer user-agent version-id host-id
//	sig-version cipher-suite auth-type host-header tls-version
//	access-point-arn acl-required
func formatAccessLogLine(
	r *http.Request,
	sourceBucket, op, key string,
	statusCode int,
	bytesSent int64,
) string {
	now := time.Now().UTC().Format("[02/Jan/2006:15:04:05 +0000]")
	requestID := newAccessLogID()
	hostID := newAccessLogID()
	remote := r.RemoteAddr
	if remote == "" {
		remote = "-"
	}
	keyField := key
	if keyField == "" {
		keyField = "-"
	}

	requestURI := fmt.Sprintf("%s %s %s", r.Method, r.URL.RequestURI(), r.Proto)
	referer := defaultDash(r.Header.Get("Referer"))
	userAgent := defaultDash(r.Header.Get("User-Agent"))
	host := defaultDash(r.Host)

	// Fields in AWS Server Access Log order; '-' is used for fields the mock
	// doesn't track. host-id is appended last in the printf string itself so
	// the verb count matches.
	return fmt.Sprintf(
		"%s %s %s %s %s %s %s %s \"%s\" %d - %d - - - %s %s - %s SigV4 - AuthHeader %s TLSv1.2 - -\n",
		"gopherstack", // bucket-owner
		sourceBucket,  // src-bucket
		now,           // timestamp
		remote,        // remote-ip
		"-",           // requester (anonymous in the mock)
		requestID,     // request-id
		op,            // operation
		keyField,      // key
		requestURI,    // request-uri
		statusCode,    // http-status
		bytesSent,     // bytes-sent
		referer,       // referer
		userAgent,     // user-agent
		hostID,        // host-id
		host,          // host-header
	)
}

// accessLogIDBytes is the number of random bytes the request-id /
// host-id placeholders consume — 8 bytes → 16 hex chars, matching the
// width of real S3 IDs at this position in the log line.
const accessLogIDBytes = 8

// buildAccessLogKey produces an object key in AWS's
// <prefix>YYYY-MM-DD-HH-MM-SS-<random-hex> pattern. The random suffix means
// concurrent writes don't collide even within the same second.
func buildAccessLogKey(prefix string) string {
	ts := time.Now().UTC().Format("2006-01-02-15-04-05")

	return prefix + ts + "-" + newAccessLogID()
}

// newAccessLogID returns a 16-character hex string used for request-id and
// host-id placeholders. Falls back to a timestamp if crypto/rand fails.
func newAccessLogID() string {
	buf := make([]byte, accessLogIDBytes)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%016x", time.Now().UnixNano())
	}

	return hex.EncodeToString(buf)
}

func defaultDash(s string) string {
	if s == "" {
		return "-"
	}

	return s
}
