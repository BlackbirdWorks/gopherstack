package sns_test

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/events"
	"github.com/blackbirdworks/gopherstack/services/sns"
)

func verifyNotificationSig(
	t *testing.T, pub *rsa.PublicKey, version, msgID, topic, message, ts, sig string,
) {
	t.Helper()

	canonical := sns.CanonicalNotificationStringForTest(msgID, topic, "subj", message, ts)

	raw, err := base64.StdEncoding.DecodeString(sig)
	require.NoError(t, err)

	if version == "2" {
		h := sha256.Sum256([]byte(canonical))
		assert.NoError(t, rsa.VerifyPKCS1v15(pub, crypto.SHA256, h[:], raw))

		return
	}

	h := sha1.Sum([]byte(canonical))
	assert.NoError(t, rsa.VerifyPKCS1v15(pub, crypto.SHA1, h[:], raw))
}

func TestPublishSignaturesVerifyOnEveryChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
	}{
		{name: "sha1", version: "1"},
		{name: "sha256", version: "2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			const publishes = 6

			var mu sync.Mutex

			httpEnvs := make(chan snsNotificationEnvelope, publishes*2)
			ts := newNotificationServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)

				var env snsNotificationEnvelope
				if json.Unmarshal(body, &env) == nil {
					httpEnvs <- env
				}

				w.WriteHeader(http.StatusOK)
			}))
			defer ts.Close()

			b := newA1679Backend(t)

			var sqsEvents []*events.SNSPublishedEvent

			emitter := events.NewInMemoryEmitter[*events.SNSPublishedEvent]()
			emitter.Subscribe(func(_ context.Context, ev *events.SNSPublishedEvent) error {
				mu.Lock()
				defer mu.Unlock()

				sqsEvents = append(sqsEvents, ev)

				return nil
			})
			b.SetPublishEmitter(emitter)

			tp, err := b.CreateTopic("sig-channels", map[string]string{"SignatureVersion": tt.version})
			require.NoError(t, err)

			_, err = b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q", "")
			require.NoError(t, err)

			for range 2 {
				_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
				require.NoError(t, err)
			}

			var wg sync.WaitGroup

			msg := `{"default":"def-body","http":"http-body"}`

			for range publishes {
				wg.Go(func() {
					_, pErr := b.Publish(tp.TopicArn, msg, "subj", "json", nil)
					assert.NoError(t, pErr)
				})
			}

			wg.Wait()

			pub := parseSigningCertPublicKey(t, b)

			mu.Lock()
			defer mu.Unlock()

			require.Len(t, sqsEvents, publishes)

			for _, ev := range sqsEvents {
				assert.Equal(t, tt.version, ev.SignatureVersion)
				verifyNotificationSig(
					t, pub, tt.version, ev.MessageID, ev.TopicARN, ev.Message, ev.Timestamp, ev.Signature,
				)

				for _, sub := range ev.Subscriptions {
					if sub.Protocol != "sqs" {
						continue
					}

					assert.Equal(t, "def-body", sub.Body(ev.Message))
					verifyNotificationSig(t, pub, tt.version, ev.MessageID, ev.TopicARN,
						sub.Body(ev.Message), ev.Timestamp, sub.SignatureFor(ev.Signature))
				}
			}

			for range publishes * 2 {
				select {
				case env := <-httpEnvs:
					assert.Equal(t, "http-body", env.Message)
					assert.Equal(t, tt.version, env.SignatureVersion)
					verifyNotificationSig(
						t, pub, tt.version, env.MessageID, env.TopicArn, env.Message, env.Timestamp, env.Signature,
					)
				case <-time.After(5 * time.Second):
					t.Fatal("HTTP delivery did not arrive")
				}
			}
		})
	}
}
