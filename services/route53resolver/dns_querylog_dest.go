package route53resolver

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	s3ArnPrefix       = "arn:aws:s3:::"
	logFileWindow     = 5 * time.Minute
	logFileHashLen    = 8
	logFileTimeLayout = "20060102T1504Z"
	firehoseStreamSeg = "deliverystream/"
	arnFields         = 6
)

// QueryLogObjectStore reads and writes the S3 objects query logs are delivered to.
type QueryLogObjectStore interface {
	GetObjectBytes(ctx context.Context, bucket, key string) ([]byte, error)
	PutObjectBytes(ctx context.Context, bucket, key string, data []byte) error
}

// QueryLogStreams accepts the records query logs send to a Firehose delivery stream.
type QueryLogStreams interface {
	PutRecord(ctx context.Context, streamName string, data []byte) error
}

// SetQueryLogDestinations wires the S3 and Firehose backends that query log configs can deliver to.
func (b *InMemoryBackend) SetQueryLogDestinations(objects QueryLogObjectStore, streams QueryLogStreams) {
	b.mu.Lock("SetQueryLogDestinations")
	defer b.mu.Unlock()

	b.logObjects = objects
	b.logStreams = streams
}

func (b *InMemoryBackend) queryLogDestinations() (QueryLogObjectStore, QueryLogStreams) {
	b.mu.RLock("queryLogDestinations")
	defer b.mu.RUnlock()

	return b.logObjects, b.logStreams
}

// s3LogKey is the delivery object for the 5-minute window containing t:
// <prefix>/AWSLogs/<account>/vpcdnsquerylogs/<vpc>/<yyyy>/<mm>/<dd>/<account>_vpcdnsquerylogs_<vpc>_<window>_<hash>.log.gz
func s3LogKey(prefix, account, vpcID, configID string, t time.Time) string {
	t = t.UTC().Truncate(logFileWindow)
	window := t.Format(logFileTimeLayout)
	sum := sha256.Sum256([]byte(configID + "|" + vpcID + "|" + window))
	name := fmt.Sprintf("%s_vpcdnsquerylogs_%s_%s_%s.log.gz", account, vpcID, window, hex.EncodeToString(sum[:])[:logFileHashLen])

	key := fmt.Sprintf("AWSLogs/%s/vpcdnsquerylogs/%s/%s/%s", account, vpcID, t.Format("2006/01/02"), name)
	if prefix = strings.Trim(prefix, "/"); prefix != "" {
		key = prefix + "/" + key
	}

	return key
}

func (r *DNSResolver) deliverToS3(ctx context.Context, e queryLogEntry, cfg *ResolverQueryLogConfig, payload string) {
	objects, _ := r.b.queryLogDestinations()
	if objects == nil {
		return
	}

	bucket, prefix, _ := strings.Cut(strings.TrimPrefix(cfg.DestinationARN, s3ArnPrefix), "/")
	if bucket == "" {
		return
	}

	key := s3LogKey(prefix, r.b.accountID, e.vpcID, cfg.ID, time.Now())

	r.logMu.Lock("deliverToS3")
	defer r.logMu.Unlock()

	var lines []byte

	if existing, err := objects.GetObjectBytes(ctx, bucket, key); err == nil {
		lines = gunzip(existing)
	}

	lines = append(lines, payload...)
	lines = append(lines, '\n')

	if err := objects.PutObjectBytes(ctx, bucket, key, gzipBytes(lines)); err != nil {
		logger.Load(ctx).WarnContext(ctx, "route53resolver: query log delivery to S3 failed",
			"bucket", bucket, "key", key, "error", err)
	}
}

func (r *DNSResolver) deliverToFirehose(ctx context.Context, e queryLogEntry, cfg *ResolverQueryLogConfig, payload string) {
	_, streams := r.b.queryLogDestinations()
	if streams == nil {
		return
	}

	parts := strings.SplitN(cfg.DestinationARN, ":", arnFields)
	if len(parts) != arnFields {
		return
	}

	name, ok := strings.CutPrefix(parts[arnFields-1], firehoseStreamSeg)
	if !ok || name == "" {
		return
	}

	wctx := awsmeta.WithRegion(ctx, e.region)
	if err := streams.PutRecord(wctx, name, []byte(payload+"\n")); err != nil {
		logger.Load(ctx).WarnContext(ctx, "route53resolver: query log delivery to Firehose failed",
			"stream", name, "error", err)
	}
}

func gzipBytes(data []byte) []byte {
	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(data)
	_ = zw.Close()

	return buf.Bytes()
}

func gunzip(data []byte) []byte {
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil
	}

	defer zr.Close()

	out, _ := io.ReadAll(zr)

	return out
}
