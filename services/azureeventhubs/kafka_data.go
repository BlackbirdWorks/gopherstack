package azureeventhubs

import (
	"context"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func (c *kconn) produce(r *kmsg.ProduceRequest) kmsg.Response {
	resp := &kmsg.ProduceResponse{}
	budget := int64(maxBatchInflated)

	for _, t := range r.Topics {
		rt := kmsg.ProduceResponseTopic{Topic: t.Topic}
		ref, found := c.hubRef(t.Topic)

		for _, p := range t.Partitions {
			rp := kmsg.ProduceResponseTopicPartition{
				Partition: p.Partition, BaseOffset: -1, LogAppendTime: -1, LogStartOffset: -1,
			}

			switch {
			case !c.allowed(RightSend, t.Topic):
				rp.ErrorCode = kerrTopicAuthFailed
			case !found:
				rp.ErrorCode = kerrUnknownTopic
			default:
				rp.ErrorCode, rp.BaseOffset = c.appendPartition(ref, p.Partition, p.Records, &budget)
			}

			rt.Partitions = append(rt.Partitions, rp)
		}

		resp.Topics = append(resp.Topics, rt)
	}

	return resp
}

func batchErr(err error) int16 {
	switch {
	case errors.Is(err, errCompression):
		return kerrUnsupportedCompression
	case errors.Is(err, errBatchTooLarge):
		return kerrMessageTooLarge
	}

	return kerrCorruptMessage
}

func (c *kconn) appendPartition(ref HubRef, pid int32, data []byte, budget *int64) (int16, int64) {
	batches, err := splitBatches(data, budget)
	if err != nil {
		return batchErr(err), -1
	}

	base := int64(-1)

	for _, b := range batches {
		if len(b.events) == 0 {
			continue
		}

		code, off := c.appendBatch(ref, pid, b)
		if code != kerrNone {
			return code, -1
		}

		if base < 0 {
			base = off
		}
	}

	if base < 0 {
		info, infoErr := c.k.h.Backend.PartitionInfo(ref, int(pid))
		if infoErr != nil {
			return kerrUnknownTopic, -1
		}

		base = info.LastSequence + 1
	}

	return kerrNone, base
}

func (c *kconn) appendBatch(ref HubRef, pid int32, b producedBatch) (int16, int64) {
	token, code, dup := c.k.prod.begin(ref, pid, b)
	if code != kerrNone {
		return code, -1
	}

	if dup >= 0 {
		return kerrNone, dup
	}

	first, err := c.k.h.Backend.appendAt(ref, int(pid), b.events)

	switch {
	case err == nil:
	case errors.Is(err, ErrEventTooLarge):
		return kerrMessageTooLarge, -1
	case errors.Is(err, ErrPartitionNotFound), errors.Is(err, ErrHubNotFound):
		return kerrUnknownTopic, -1
	default:
		return kerrCorruptMessage, -1
	}

	c.k.prod.commit(token, first)

	return kerrNone, first
}

func (c *kconn) fetch(ctx context.Context, r *kmsg.FetchRequest) kmsg.Response {
	wait := min(time.Duration(r.MaxWaitMillis)*time.Millisecond, kafkaMaxWait)
	if r.MaxWaitMillis <= 0 {
		wait = 0
	}

	maxBytes := int(r.MaxBytes)
	if maxBytes <= 0 || maxBytes > kafkaMaxFetchBytes {
		maxBytes = kafkaMaxFetchBytes
	}

	deadline := time.Now().Add(wait)

	for {
		resp, notifies, ready := c.fetchOnce(r, maxBytes)
		if ready || wait == 0 || time.Until(deadline) <= 0 {
			return resp
		}

		timer := time.NewTimer(time.Until(deadline))

		if !waitAny(ctx, timer.C, notifies) {
			timer.Stop()

			return resp
		}

		timer.Stop()
	}
}

// waitAny blocks until one of the partitions signals, the timer fires or ctx ends; it reports whether to retry.
func waitAny(ctx context.Context, timer <-chan time.Time, notifies []<-chan struct{}) bool {
	wake := make(chan struct{}, 1)
	stop := make(chan struct{})

	defer close(stop)

	for _, n := range notifies {
		go func() {
			select {
			case <-n:
				select {
				case wake <- struct{}{}:
				default:
				}
			case <-stop:
			}
		}()
	}

	select {
	case <-wake:
		return true
	case <-timer:
		return false
	case <-ctx.Done():
		return false
	}
}

func (c *kconn) fetchOnce(r *kmsg.FetchRequest, maxBytes int) (*kmsg.FetchResponse, []<-chan struct{}, bool) {
	resp := &kmsg.FetchResponse{}

	var notifies []<-chan struct{}

	got := false
	left := maxBytes

	for _, t := range r.Topics {
		rt := kmsg.FetchResponseTopic{Topic: t.Topic}
		ref, found := c.hubRef(t.Topic)

		for _, p := range t.Partitions {
			rp := kmsg.FetchResponseTopicPartition{
				Partition: p.Partition, HighWatermark: -1, LastStableOffset: -1, LogStartOffset: -1,
				PreferredReadReplica: -1,
			}

			switch {
			case !c.allowed(RightListen, t.Topic):
				rp.ErrorCode = kerrTopicAuthFailed
			case !found:
				rp.ErrorCode = kerrUnknownTopic
			default:
				n := min(left, int(max(p.PartitionMaxBytes, 1)))
				ns := c.fillPartition(&rp, ref, p, n)
				notifies = append(notifies, ns...)

				if len(rp.RecordBatches) > 0 {
					got = true
					left = max(left-len(rp.RecordBatches), 1)
				}
			}

			if rp.ErrorCode != kerrNone {
				got = true
			}

			rt.Partitions = append(rt.Partitions, rp)
		}

		resp.Topics = append(resp.Topics, rt)
	}

	return resp, notifies, got
}

func (c *kconn) fillPartition(
	rp *kmsg.FetchResponseTopicPartition, ref HubRef, p kmsg.FetchRequestTopicPartition, maxBytes int,
) []<-chan struct{} {
	ls, err := c.k.h.Backend.readLog(ref, int(p.Partition), p.FetchOffset, maxBytes)
	if err != nil {
		rp.ErrorCode = kerrUnknownTopic

		return nil
	}

	rp.HighWatermark, rp.LastStableOffset, rp.LogStartOffset = ls.end, ls.end, ls.begin

	if p.FetchOffset < ls.begin || p.FetchOffset > ls.end {
		rp.ErrorCode = kerrOffsetOutOfRange

		return nil
	}

	batch, encErr := encodeBatch(ls.records)
	if encErr != nil {
		rp.ErrorCode = kerrCorruptMessage

		return nil
	}

	rp.RecordBatches = batch

	return []<-chan struct{}{ls.notify}
}

func (c *kconn) listOffsets(r *kmsg.ListOffsetsRequest) kmsg.Response {
	resp := &kmsg.ListOffsetsResponse{}

	for _, t := range r.Topics {
		rt := kmsg.ListOffsetsResponseTopic{Topic: t.Topic}
		ref, found := c.hubRef(t.Topic)

		for _, p := range t.Partitions {
			rp := kmsg.ListOffsetsResponseTopicPartition{
				Partition: p.Partition, Timestamp: -1, Offset: -1, LeaderEpoch: -1,
			}

			switch {
			case !c.allowed(RightListen, t.Topic) && !c.allowed(RightSend, t.Topic):
				rp.ErrorCode = kerrTopicAuthFailed
			case !found:
				rp.ErrorCode = kerrUnknownTopic
			default:
				off, ts, err := c.k.h.Backend.offsetAt(ref, int(p.Partition), p.Timestamp)
				if err != nil {
					rp.ErrorCode = kerrUnknownTopic

					break
				}

				rp.Offset = off
				if !ts.IsZero() {
					rp.Timestamp = ts.UnixMilli()
				}

				if r.Version == 0 {
					rp.OldStyleOffsets = []int64{off}
				}
			}

			rt.Partitions = append(rt.Partitions, rp)
		}

		resp.Topics = append(resp.Topics, rt)
	}

	return resp
}
