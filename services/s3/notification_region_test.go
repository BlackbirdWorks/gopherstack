package s3_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

type regionCapturePublisher struct {
	regions []string
	mu      sync.Mutex
}

func (p *regionCapturePublisher) PublishS3Event(ctx context.Context, _, _, _ string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.regions = append(p.regions, awsmeta.Region(ctx))
}

func TestNotificationDispatch_UsesBucketRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "home", region: "us-east-1"},
		{name: "other", region: "eu-west-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				handler, _ := newTestHandler(t)
				mustCreateBucketInRegion(t, handler, "notif-region", tc.region)

				meta := func(r *http.Request) *http.Request {
					return r.WithContext(awsmeta.Set(r.Context(), &awsmeta.Metadata{
						Region: tc.region, Account: awsmeta.DefaultAccount,
					}))
				}

				cfg := `<NotificationConfiguration><EventBridgeConfiguration/>` +
					`<QueueConfiguration><Id>q1</Id><Queue>arn:aws:sqs:` + tc.region + `:000000000000:q</Queue>` +
					`<Event>s3:ObjectCreated:*</Event></QueueConfiguration></NotificationConfiguration>`
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, meta(httptest.NewRequest(
					http.MethodPut, "/notif-region?notification", strings.NewReader(cfg))))
				require.Equal(t, http.StatusOK, rec.Code)

				queue := &captureQueue{}
				bus := &regionCapturePublisher{}
				handler.SetNotificationDispatcher(s3.NewNotificationDispatcher(
					&s3.NotificationTargets{SQSSender: queue, EventBridgePublisher: bus}, "us-east-1"))

				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, meta(httptest.NewRequest(
					http.MethodPut, "/notif-region/k", strings.NewReader("x"))))
				require.Equal(t, http.StatusOK, rec.Code)

				synctest.Wait()

				queue.mu.Lock()
				defer queue.mu.Unlock()
				bus.mu.Lock()
				defer bus.mu.Unlock()

				require.Len(t, queue.messages, 1)
				assert.Contains(t, queue.messages[0], `"awsRegion":"`+tc.region+`"`)
				assert.Equal(t, []string{tc.region}, bus.regions)
			})
		})
	}
}
