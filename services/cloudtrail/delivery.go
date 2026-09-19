package cloudtrail

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

// logFileRecords is the top-level shape of a CloudTrail log file: a single
// "Records" array of individual event details. Documentation-sourced (AWS
// User Guide "CloudTrail log file examples"), not part of the pinned SDK --
// see PARITY.md.
type logFileRecords struct {
	Records []json.RawMessage `json:"Records"`
}

// deliveryTarget is a snapshot of the fields deliverLogFile needs from one
// logging trail, copied out under b.mu so the marshal/gzip/PutObject work
// below can run lock-free without racing a concurrent UpdateTrail.
type deliveryTarget struct {
	trail        *Trail
	s3BucketName string
	s3KeyPrefix  string
}

// deliverLogFile writes ev as a CloudTrail log file to every trail that is
// currently logging and has S3BucketName set, when S3 is wired
// (SetS3Backend). A no-op when S3 is unwired or ev carries no
// CloudTrailEvent detail (e.g. a directly seeded test event bypassing
// RecordManagementEvent) -- matching this repo's unwired-hook-stays-
// permissive convention. Unlike the rest of this backend's mutators, this
// runs the expensive part (gzip + S3 PutObject) WITHOUT b.mu held -- see
// RecordEvent's caller comment -- and only re-takes it briefly to snapshot
// trails and to mark a successful delivery, matching how DescribeTrails/
// UpdateTrail already gate Trail field access on b.mu.
//
// Real AWS batches multiple events per file roughly every 5 minutes; this
// backend delivers one file per recorded event instead of buffering, a
// disclosed simplification (see PARITY.md).
func (b *InMemoryBackend) deliverLogFile(ev Event) {
	if b.s3 == nil || ev.CloudTrailEvent == "" {
		return
	}

	body, err := logFileBody(ev)
	if err != nil {
		return
	}

	b.mu.RLock("deliverLogFile:snapshot")

	targets := make([]deliveryTarget, 0, b.trails.Len())

	for _, t := range b.trails.All() {
		if t.IsLogging && t.S3BucketName != "" {
			targets = append(targets, deliveryTarget{
				trail:        t,
				s3BucketName: t.S3BucketName,
				s3KeyPrefix:  t.S3KeyPrefix,
			})
		}
	}

	b.mu.RUnlock()

	for _, target := range targets {
		input := &sdk_s3.PutObjectInput{
			Bucket: aws.String(target.s3BucketName),
			Key:    aws.String(logFileKey(target.s3KeyPrefix, b.accountID, b.region, ev.EventTime)),
			Body:   bytes.NewReader(body),
		}

		if _, putErr := b.s3.PutObject(context.Background(), input); putErr != nil {
			continue
		}

		now := time.Now().UTC()

		b.mu.Lock("deliverLogFile:markDelivered")
		target.trail.LatestDeliveryTime = &now
		b.mu.Unlock()
	}
}

// gzipWriterPool reuses *gzip.Writer instances across logFileBody calls.
// gzip.NewWriter allocates a full flate compressor (window + Huffman
// tables) every call; RecordEvent invokes this on every mutating API call
// across every registered service, so that allocation dominated both CPU
// and heap in profiling. Reset avoids it.
var gzipWriterPool = sync.Pool{ //nolint:gochecknoglobals // sync.Pool requires package-level allocation
	New: func() any { return gzip.NewWriter(nil) },
}

// logFileBody gzip-compresses a single-record CloudTrail log file body.
func logFileBody(ev Event) ([]byte, error) {
	encoded, err := json.Marshal(
		logFileRecords{Records: []json.RawMessage{json.RawMessage(ev.CloudTrailEvent)}},
	)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	gz, _ := gzipWriterPool.Get().(*gzip.Writer)
	gz.Reset(&buf)

	defer gzipWriterPool.Put(gz)

	if _, writeErr := gz.Write(encoded); writeErr != nil {
		return nil, writeErr
	}
	if closeErr := gz.Close(); closeErr != nil {
		return nil, closeErr
	}

	return buf.Bytes(), nil
}

// logFileKey builds a CloudTrail log file object key. Documentation-sourced
// (AWS User Guide "CloudTrail log file name format":
// "AWSLogs/AccountID/CloudTrail/Region/YYYY/MM/DD/AccountID_CloudTrail_
// Region_YYYYMMDDTHHmmZ_UniqueString.json.gz"), not part of the pinned SDK --
// see PARITY.md.
func logFileKey(s3KeyPrefix, accountID, region string, eventTime time.Time) string {
	ts := eventTime.UTC()
	unique := strings.ReplaceAll(uuid.NewString(), "-", "")[:16]

	key := fmt.Sprintf(
		"AWSLogs/%s/CloudTrail/%s/%04d/%02d/%02d/%s_CloudTrail_%s_%s_%s.json.gz",
		accountID, region, ts.Year(), ts.Month(), ts.Day(),
		accountID, region, ts.Format("20060102T1504Z"), unique,
	)

	if s3KeyPrefix != "" {
		key = strings.TrimSuffix(s3KeyPrefix, "/") + "/" + key
	}

	return key
}
