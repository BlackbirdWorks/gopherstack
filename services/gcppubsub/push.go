package gcppubsub

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	pushTimeout    = 30 * time.Second
	pushBatch      = 10
	pushMinBackoff = time.Second
)

type pushMessage struct {
	Attributes  map[string]string `json:"attributes,omitempty"`
	Data        string            `json:"data"`
	MessageID   string            `json:"messageId"`
	MessageIDSn string            `json:"message_id"`
	PublishTime string            `json:"publishTime"`
	PublishSn   string            `json:"publish_time"`
	OrderingKey string            `json:"orderingKey,omitempty"`
}

type pushEnvelope struct {
	Subscription    string      `json:"subscription"`
	Message         pushMessage `json:"message"`
	DeliveryAttempt int32       `json:"deliveryAttempt,omitempty"`
}

type pushJob struct {
	sub      string
	endpoint string
	cfg      *pubsubpb.PushConfig
	msgs     []*pubsubpb.ReceivedMessage
}

// RunPush delivers messages of push subscriptions until ctx is cancelled, then waits for in-flight requests.
func (b *InMemoryBackend) RunPush(ctx context.Context) {
	client := httpclient.New(pushTimeout)

	var wg sync.WaitGroup

	defer wg.Wait()

	for {
		next := b.dispatchPush(ctx, client, &wg)

		var timeout <-chan time.Time

		timer := time.NewTimer(time.Hour)

		if !next.IsZero() {
			timer.Reset(max(time.Until(next), time.Millisecond))
			timeout = timer.C
		}

		select {
		case <-ctx.Done():
			timer.Stop()

			return
		case <-b.pushWake:
		case <-timeout:
		}

		timer.Stop()
	}
}

func (b *InMemoryBackend) dispatchPush(ctx context.Context, client *http.Client, wg *sync.WaitGroup) time.Time {
	var (
		jobs []pushJob
		next time.Time
	)

	b.mu.Lock("DispatchPush")

	for name, s := range b.subs {
		endpoint := s.pb.GetPushConfig().GetPushEndpoint()
		if endpoint == "" || s.pushing || s.pb.GetDetached() || s.expired(b.now()) {
			continue
		}

		res := b.leaseLocked(s, leaseReq{
			maxMsgs: pushBatch, owner: "push", deadline: time.Duration(s.pb.GetAckDeadlineSeconds()) * time.Second,
		})
		if !res.nextWake.IsZero() && (next.IsZero() || res.nextWake.Before(next)) {
			next = res.nextWake
		}

		if len(res.msgs) == 0 {
			continue
		}

		s.pushing = true

		jobs = append(jobs, pushJob{sub: name, endpoint: endpoint, cfg: s.pb.GetPushConfig(), msgs: res.msgs})
	}

	b.mu.Unlock()

	for _, job := range jobs {
		wg.Go(func() { b.runPushJob(ctx, client, job) })
	}

	return next
}

func (b *InMemoryBackend) runPushJob(ctx context.Context, client *http.Client, job pushJob) {
	var acked, failed []string

	for _, rm := range job.msgs {
		if b.sendPush(ctx, client, job, rm) {
			acked = append(acked, rm.GetAckId())
		} else {
			failed = append(failed, rm.GetAckId())
		}
	}

	if ctx.Err() != nil {
		failed = append(failed, acked...)
		acked = nil
	}

	if len(acked) > 0 {
		_, _, _ = b.Acknowledge(job.sub, acked)
	}

	b.mu.Lock("PushDone")

	if s, ok := b.subs[job.sub]; ok {
		s.pushing = false
		now := b.now()

		for _, id := range failed {
			if m, held := s.acks[id]; held {
				s.release(m, now)
				m.NotBefore = now.Add(max(s.backoff(m.Attempts), pushMinBackoff))
			}
		}

		b.notifyLocked(s)
	}

	b.mu.Unlock()
	b.signalPush()
}

func (b *InMemoryBackend) sendPush(
	ctx context.Context,
	client *http.Client,
	job pushJob,
	rm *pubsubpb.ReceivedMessage,
) bool {
	m := rm.GetMessage()
	pub := m.GetPublishTime().AsTime().UTC().Format(time.RFC3339Nano)

	var (
		body    []byte
		headers = map[string]string{}
	)

	if nw := job.cfg.GetNoWrapper(); nw != nil {
		body = m.GetData()
		headers["Content-Type"] = "application/octet-stream"

		if nw.GetWriteMetadata() {
			headers["X-Goog-Pubsub-Subscription-Name"] = job.sub
			headers["X-Goog-Pubsub-Message-Id"] = m.GetMessageId()
			headers["X-Goog-Pubsub-Publish-Time"] = pub
			headers["X-Goog-Pubsub-Ordering-Key"] = m.GetOrderingKey()

			for k, v := range m.GetAttributes() {
				headers["X-Goog-Pubsub-Message-Attribute-"+k] = v
			}
		}
	} else {
		env := pushEnvelope{
			Subscription:    job.sub,
			DeliveryAttempt: rm.GetDeliveryAttempt(),
			Message: pushMessage{
				Attributes: m.GetAttributes(), Data: base64.StdEncoding.EncodeToString(m.GetData()),
				MessageID: m.GetMessageId(), MessageIDSn: m.GetMessageId(),
				PublishTime: pub, PublishSn: pub, OrderingKey: m.GetOrderingKey(),
			},
		}

		var err error
		if body, err = json.Marshal(env); err != nil {
			return false
		}

		headers["Content-Type"] = "application/json"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, job.endpoint, bytes.NewReader(body))
	if err != nil {
		return false
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		logger.Load(ctx).DebugContext(ctx, "gcppubsub: push delivery failed", "subscription", job.sub, "error", err)

		return false
	}

	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent, http.StatusProcessing:
		return true
	default:
		return false
	}
}
