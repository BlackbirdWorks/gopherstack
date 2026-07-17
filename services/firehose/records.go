package firehose

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// PutRecord appends a record to the delivery stream and flushes if buffer threshold is met.
func (b *InMemoryBackend) PutRecord(ctx context.Context, streamName string, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("%w: record Data must not be empty", ErrValidation)
	}

	if len(data) > maxRecordBytes {
		return fmt.Errorf("%w: record size %d exceeds maximum of %d bytes",
			ErrRecordTooLarge, len(data), maxRecordBytes)
	}

	b.mu.Lock("PutRecord")

	region := getRegionFromContext(ctx, b)

	s, ok := b.streams.Get(regionKey(region, streamName))
	if !ok {
		b.mu.Unlock()

		return fmt.Errorf("%w: stream %s not found", ErrNotFound, streamName)
	}

	if s.DeliveryStreamType != deliveryStreamTypeDirectPut && s.DeliveryStreamType != "" {
		b.mu.Unlock()

		return fmt.Errorf("%w: PutRecord not allowed on %s stream; only DirectPut streams accept direct puts",
			ErrValidation, s.DeliveryStreamType)
	}

	s.Records = append(s.Records, data)
	s.bufferSizeBytes += len(data)
	s.Metrics.TotalRecords++
	s.Metrics.TotalBytes += int64(len(data))
	// If backup mode is enabled, also store a copy in backup records.
	if b.isBackupEnabledLocked(s) {
		s.BackupRecords = append(s.BackupRecords, data)
	}
	snap := b.extractForFlushLocked(s)
	b.updateFlushWatchLocked(region, streamName, s, snap)
	b.mu.Unlock()

	if snap != nil {
		b.deliverSnapshot(b.svcCtx, snap, streamName)
	}

	return nil
}

// updateFlushWatchLocked keeps the interval-flush watch set in sync after buffering
// records: a size-based flush (snap != nil) clears the entry, while remaining buffered
// records for an active destination mark the stream as pending. Must be called with the
// write lock held.
func (b *InMemoryBackend) updateFlushWatchLocked(region, name string, s *DeliveryStream, snap *flushSnapshot) {
	if snap != nil {
		b.clearPendingFlushLocked(region, name)

		return
	}

	if len(s.Records) > 0 && b.hasActiveDestinationLocked(s) {
		b.markPendingFlushLocked(region, name)
	}
}

// PutRecordBatch appends multiple records to the delivery stream and flushes if buffer threshold is met.
func (b *InMemoryBackend) PutRecordBatch(ctx context.Context, streamName string, records [][]byte) (int, error) {
	if len(records) > maxBatchRecords {
		return 0, fmt.Errorf("%w: batch size %d exceeds maximum of %d records",
			ErrBatchTooLarge, len(records), maxBatchRecords)
	}

	totalBytes := 0
	for i, rec := range records {
		if len(rec) == 0 {
			return 0, fmt.Errorf("%w: record %d Data must not be empty", ErrValidation, i)
		}

		if len(rec) > maxRecordBytes {
			return 0, fmt.Errorf("%w: record %d size %d exceeds maximum of %d bytes",
				ErrRecordTooLarge, i, len(rec), maxRecordBytes)
		}
		totalBytes += len(rec)
	}

	if totalBytes > maxBatchBytes {
		return 0, fmt.Errorf("%w: batch payload %d exceeds maximum of %d bytes",
			ErrBatchTooLarge, totalBytes, maxBatchBytes)
	}

	b.mu.Lock("PutRecordBatch")

	region := getRegionFromContext(ctx, b)

	s, ok := b.streams.Get(regionKey(region, streamName))
	if !ok {
		b.mu.Unlock()

		return 0, fmt.Errorf("%w: stream %s not found", ErrNotFound, streamName)
	}

	if s.DeliveryStreamType != deliveryStreamTypeDirectPut && s.DeliveryStreamType != "" {
		b.mu.Unlock()

		return 0, fmt.Errorf("%w: PutRecordBatch not allowed on %s stream; only DirectPut streams accept direct puts",
			ErrValidation, s.DeliveryStreamType)
	}

	backupEnabled := b.isBackupEnabledLocked(s)
	for _, rec := range records {
		s.Records = append(s.Records, rec)
		s.bufferSizeBytes += len(rec)
		s.Metrics.TotalRecords++
		s.Metrics.TotalBytes += int64(len(rec))
		if backupEnabled {
			s.BackupRecords = append(s.BackupRecords, rec)
		}
	}

	snap := b.extractForFlushLocked(s)
	b.updateFlushWatchLocked(region, streamName, s, snap)
	b.mu.Unlock()

	if snap != nil {
		b.deliverSnapshot(b.svcCtx, snap, streamName)
	}

	return 0, nil
}

// recordIDBytes is the number of random bytes used when generating a record identifier.
const recordIDBytes = 16

// newRecordID generates a random hex record identifier.
func newRecordID(ctx context.Context) string {
	b := make([]byte, recordIDBytes)
	if _, err := rand.Read(b); err != nil {
		logger.Load(ctx).WarnContext(ctx,
			"firehose: rand.Read failed; falling back to timestamp-based record ID", "error", err)

		return fmt.Sprintf("rec-%d", time.Now().UnixNano())
	}

	return hex.EncodeToString(b)
}
