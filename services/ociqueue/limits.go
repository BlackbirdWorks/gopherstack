package ociqueue

import (
	"net/http"
	"strconv"
	"time"
)

// Service limits from https://docs.oracle.com/en-us/iaas/Content/queue/overview.htm.
const (
	maxQueuesPerTenancy  = 10
	maxChannelsPerQueue  = 256
	maxInFlightPerQueue  = 100_000
	maxQueueBytes        = int64(2) << 30
	maxTenancyBytes      = int64(20) << 30
	maxRequestsPerSecond = 1000
	maxBytesPerSecond    = int64(10) << 20
	percentDivisor       = 100
)

type limits struct {
	queues         int
	channels       int
	inFlight       int
	requestsPerSec int
	queueBytes     int64
	tenancyBytes   int64
	bytesPerSec    int64
}

func defaultLimits() limits {
	return limits{
		queues: maxQueuesPerTenancy, channels: maxChannelsPerQueue, inFlight: maxInFlightPerQueue,
		requestsPerSec: maxRequestsPerSecond, queueBytes: maxQueueBytes, tenancyBytes: maxTenancyBytes,
		bytesPerSec: maxBytesPerSecond,
	}
}

func errLimit(msg string) *APIError { return newErr(http.StatusBadRequest, "LimitExceeded", msg) }

func errThrottled(msg string) *APIError {
	return newErr(http.StatusTooManyRequests, "TooManyRequests", msg)
}

// rateWindow counts one API's traffic on one queue within a single clock second.
type rateWindow struct {
	second int64
	reqs   int
	in     int64
	out    int64
}

// SetEnforceRateLimits makes data plane calls answer 429 once a queue's per-second request or byte budget is spent.
func (b *InMemoryBackend) SetEnforceRateLimits(on bool) {
	b.mu.Lock("SetEnforceRateLimits")
	b.enforceRates = on
	b.mu.Unlock()
}

func (b *InMemoryBackend) window(q *Queue, api string) *rateWindow {
	if q.rates == nil {
		q.rates = map[string]*rateWindow{}
	}

	w := q.rates[api]
	if w == nil {
		w = &rateWindow{}
		q.rates[api] = w
	}

	if s := b.nowUTC().Unix(); s != w.second {
		*w = rateWindow{second: s}
	}

	return w
}

// throttle charges one request and its ingress bytes to api and rejects it when the second's budget is spent.
func (b *InMemoryBackend) throttle(q *Queue, api string, ingress int64) *APIError {
	if !b.enforceRates {
		return nil
	}

	w := b.window(q, api)

	switch {
	case w.reqs >= b.lim.requestsPerSec:
		return errThrottled(
			"The queue exceeded " + strconv.Itoa(b.lim.requestsPerSec) + " " + api + " requests per second",
		)
	case w.in+ingress > b.lim.bytesPerSec, w.out >= b.lim.bytesPerSec:
		return errThrottled("The queue exceeded its per-second data throughput for " + api)
	}

	w.reqs++
	w.in += ingress

	return nil
}

func (b *InMemoryBackend) chargeEgress(q *Queue, api string, n int64) {
	if b.enforceRates {
		b.window(q, api).out += n
	}
}

// dataQueueFor resolves a queue for a data plane call and charges it against the rate limits.
func (b *InMemoryBackend) dataQueueFor(id, api string, ingress int64) (*Queue, *APIError) {
	q, aerr := b.dataQueue(id)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = b.throttle(q, api, ingress); aerr != nil {
		return nil, aerr
	}

	return q, nil
}

func (q *Queue) eachStored(fn func(*Message)) {
	for _, ln := range q.lanes() {
		for _, m := range *ln.msgs {
			fn(m)
		}

		for _, m := range *ln.dlq {
			fn(m)
		}
	}
}

func (q *Queue) storedCount() int {
	n := 0

	for _, ln := range q.lanes() {
		n += len(*ln.msgs) + len(*ln.dlq)
	}

	return n
}

func (q *Queue) storedBytes() int64 {
	var n int64

	q.eachStored(func(m *Message) { n += int64(len(m.Content)) })

	return n
}

func (q *Queue) inFlight(now time.Time) int {
	n := 0

	for _, ln := range q.lanes() {
		for _, m := range *ln.msgs {
			if m.VisibleAfter.After(now) {
				n++
			}
		}
	}

	return n
}

// mayExceed reports whether count stored messages plus extra bytes could pass limit, so cheap queues skip the scan.
func mayExceed(count int, extra, limit int64) bool {
	return int64(count)*maxContentBytes+extra > limit
}

type putTotals struct {
	channels map[string]int64
	bytes    int64
}

func totalsOf(in putMessagesIn) putTotals {
	t := putTotals{channels: map[string]int64{}}

	for _, e := range in.Messages {
		n := int64(len(*e.Content))
		t.bytes += n

		if e.Metadata != nil {
			t.channels[*e.Metadata.ChannelID] += n
		}
	}

	return t
}

func (b *InMemoryBackend) tenancyBytes() int64 {
	var n int64

	for _, q := range b.queues {
		if q.State != stateDeleted {
			n += q.storedBytes()
		}
	}

	return n
}

func (b *InMemoryBackend) tenancyMayExceed(extra int64) bool {
	count := 0

	for _, q := range b.queues {
		if q.State != stateDeleted {
			count += q.storedCount()
		}
	}

	return mayExceed(count, extra, b.lim.tenancyBytes)
}

// admitPut applies the storage, channel and per-channel limits to a publish; the caller holds the write lock.
func (b *InMemoryBackend) admitPut(q *Queue, in putMessagesIn) *APIError {
	t := totalsOf(in)
	count := q.storedCount()

	if mayExceed(count, t.bytes, b.lim.queueBytes) && q.storedBytes()+t.bytes > b.lim.queueBytes {
		return errLimit(
			"The queue would exceed its storage limit of " + strconv.FormatInt(b.lim.queueBytes, 10) + " bytes",
		)
	}

	if b.tenancyMayExceed(t.bytes) && b.tenancyBytes()+t.bytes > b.lim.tenancyBytes {
		return errLimit(
			"The tenancy would exceed its queue storage limit of " + strconv.FormatInt(
				b.lim.tenancyBytes,
				10,
			) + " bytes",
		)
	}

	if len(t.channels) == 0 {
		return nil
	}

	held := map[string]int64{}
	if count+len(t.channels) > b.lim.channels ||
		(q.ChannelLimit < percentDivisor && mayExceed(count, t.bytes, b.channelBytes(q))) {
		q.eachStored(func(m *Message) {
			if m.Channel != "" {
				held[m.Channel] += int64(len(m.Content))
			}
		})
	}

	return b.checkChannels(q, t, held)
}

func (b *InMemoryBackend) channelBytes(q *Queue) int64 {
	return b.lim.queueBytes * int64(q.ChannelLimit) / percentDivisor
}

func (b *InMemoryBackend) checkChannels(q *Queue, t putTotals, held map[string]int64) *APIError {
	distinct := len(held)

	for ch, n := range t.channels {
		if _, ok := held[ch]; !ok {
			distinct++
		}

		if held[ch]+n > b.channelBytes(q) {
			return errLimit("Channel " + ch + " would exceed its channelConsumptionLimit of " +
				strconv.Itoa(q.ChannelLimit) + " percent of the queue storage")
		}
	}

	if distinct > b.lim.channels {
		return errLimit("A queue holds at most " + strconv.Itoa(b.lim.channels) + " channels")
	}

	return nil
}

func (b *InMemoryBackend) checkQueueCount() *APIError {
	n := 0

	for _, q := range b.queues {
		if q.State != stateDeleted {
			n++
		}
	}

	if n >= b.lim.queues {
		return errLimit("The tenancy already has " + strconv.Itoa(n) + " queues in this region; the service limit is " +
			strconv.Itoa(b.lim.queues))
	}

	return nil
}

// inFlightRoom is how many more messages the queue may hand out before it holds maxInFlightPerQueue in flight.
func (b *InMemoryBackend) inFlightRoom(q *Queue, now time.Time) (int, *APIError) {
	if q.storedCount() < b.lim.inFlight {
		return b.lim.inFlight, nil
	}

	room := b.lim.inFlight - q.inFlight(now)
	if room <= 0 {
		return 0, errLimit("The queue already has " + strconv.Itoa(b.lim.inFlight) + " messages in flight")
	}

	return room, nil
}
