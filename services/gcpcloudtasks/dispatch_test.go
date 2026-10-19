package gcpcloudtasks_test

import (
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/services/gcpcloudtasks"
)

func TestDispatchDeliversRequest(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(200))
	q := e.queue(t, "deliver").GetName()

	created := e.task(t, q, func(task *cloudtaskspb.Task) {
		task.MessageType = &cloudtaskspb.Task_HttpRequest{HttpRequest: &cloudtaskspb.HttpRequest{
			Url:        tg.srv.URL + "/hook?x=1",
			HttpMethod: cloudtaskspb.HttpMethod_PUT,
			Headers:    map[string]string{"X-Custom": "yes", "Content-Type": "text/plain"},
			Body:       []byte("hello"),
		}}
	})

	e.dispatch()
	require.Equal(t, 1, tg.count())

	h := tg.hit(0)
	assert.Equal(t, "PUT", h.method)
	assert.Equal(t, "/hook?x=1", h.path)
	assert.Equal(t, "hello", string(h.body))
	assert.Equal(t, "yes", h.header.Get("X-Custom"))
	assert.Equal(t, "text/plain", h.header.Get("Content-Type"))
	assert.Equal(t, "Google-Cloud-Tasks", h.header.Get("User-Agent"))
	assert.Equal(t, "deliver", h.header.Get("X-Cloudtasks-Queuename"))
	assert.Equal(
		t,
		created.GetName()[strings.LastIndex(created.GetName(), "/")+1:],
		h.header.Get("X-Cloudtasks-Taskname"),
	)
	assert.Equal(t, "0", h.header.Get("X-Cloudtasks-Taskretrycount"))

	_, err := e.getTask(t, created.GetName())
	assert.Equal(t, codes.NotFound, status.Code(err), "task is deleted after a 2xx")
}

func TestDispatchDefaultsPostAndContentType(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(204))
	q := e.queue(t, "defaults").GetName()

	e.task(t, q, func(task *cloudtaskspb.Task) {
		task.MessageType = &cloudtaskspb.Task_HttpRequest{HttpRequest: &cloudtaskspb.HttpRequest{
			Url: tg.srv.URL, Body: []byte("raw"),
		}}
	})
	e.dispatch()

	require.Equal(t, 1, tg.count())
	assert.Equal(t, "POST", tg.hit(0).method)
	assert.Equal(t, "application/octet-stream", tg.hit(0).header.Get("Content-Type"))
}

func TestDispatchScheduleTime(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(200))
	q := e.queue(t, "scheduled").GetName()

	e.task(t, q, httpTask(tg.srv.URL), func(task *cloudtaskspb.Task) {
		task.ScheduleTime = tsAfter(e, time.Minute)
	})

	e.dispatch()
	assert.Zero(t, tg.count())

	e.clk.advance(time.Minute)
	e.dispatch()
	assert.Equal(t, 1, tg.count())
}

func TestDispatchRetriesWithBackoff(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, func(n int) int {
		if n < 3 {
			return 500
		}

		return 200
	})
	q := e.queue(t, "retry", fastRetry(5)).GetName()
	task := e.task(t, q, httpTask(tg.srv.URL))

	e.dispatch()
	require.Equal(t, 1, tg.count())

	got, err := e.getTask(t, task.GetName())
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.GetDispatchCount())
	assert.EqualValues(t, 1, got.GetResponseCount())
	assert.EqualValues(t, codes.Internal, got.GetLastAttempt().GetResponseStatus().GetCode())
	assert.NotNil(t, got.GetFirstAttempt().GetDispatchTime())
	assert.Equal(t, e.clk.now().Add(time.Second), got.GetScheduleTime().AsTime(), "first backoff is min_backoff")

	e.clk.advance(999 * time.Millisecond)
	e.dispatch()
	assert.Equal(t, 1, tg.count(), "not due yet")

	e.clk.advance(time.Millisecond)
	e.dispatch()
	require.Equal(t, 2, tg.count())

	got, err = e.getTask(t, task.GetName())
	require.NoError(t, err)
	assert.Equal(t, e.clk.now().Add(2*time.Second), got.GetScheduleTime().AsTime(), "second backoff doubles")

	e.clk.advance(2 * time.Second)
	e.dispatch()
	require.Equal(t, 3, tg.count())
	assert.Equal(t, "2", tg.hit(2).header.Get("X-Cloudtasks-Taskretrycount"))
	assert.Equal(t, "2", tg.hit(2).header.Get("X-Cloudtasks-Taskexecutioncount"))

	_, err = e.getTask(t, task.GetName())
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestDispatchStopsAfterMaxAttempts(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(503))
	q := e.queue(t, "maxattempts", fastRetry(2)).GetName()
	task := e.task(t, q, httpTask(tg.srv.URL))

	for range 5 {
		e.dispatch()
		e.clk.advance(time.Minute)
	}

	assert.Equal(t, 2, tg.count())

	_, err := e.getTask(t, task.GetName())
	assert.Equal(t, codes.NotFound, status.Code(err), "task is dropped once attempts are exhausted")
}

func TestDispatchTaskRetryConfigOverridesQueue(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(500))
	q := e.queue(t, "taskretry", fastRetry(10)).GetName()

	e.task(t, q, httpTask(tg.srv.URL), func(task *cloudtaskspb.Task) {
		task.RetryConfig = &cloudtaskspb.RetryConfig{MaxAttempts: 1}
	})
	e.dispatch()
	e.clk.advance(time.Hour)
	e.dispatch()

	assert.Equal(t, 1, tg.count())
}

func TestDispatchMaxRetryDuration(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(500))
	q := e.queue(t, "duration", func(q *cloudtaskspb.Queue) {
		fastRetry(-1)(q)
		q.RetryConfig.MaxRetryDuration = durationpb.New(5 * time.Second)
	}).GetName()

	e.task(t, q, httpTask(tg.srv.URL))

	for range 10 {
		e.dispatch()
		e.clk.advance(2 * time.Second)
	}

	assert.Less(t, tg.count(), 10)
	assert.Positive(t, tg.count())

	it := e.client.ListTasks(t.Context(), &cloudtaskspb.ListTasksRequest{Parent: q})
	_, err := it.Next()
	assert.Error(t, err, "task dropped after max_retry_duration")
}

func TestDispatchStatusMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		http int
		want codes.Code
	}{
		{"not found", 404, codes.NotFound},
		{"forbidden", 403, codes.PermissionDenied},
		{"too many", 429, codes.ResourceExhausted},
		{"unavailable", 503, codes.Unavailable},
		{"redirect is failure", 302, codes.Unknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			tg := newTarget(t, always(tt.http))
			q := e.queue(t, "status", fastRetry(5)).GetName()
			task := e.task(t, q, httpTask(tg.srv.URL))

			e.dispatch()

			got, err := e.getTask(t, task.GetName())
			require.NoError(t, err, "non-2xx keeps the task for retry")
			assert.EqualValues(t, tt.want, got.GetLastAttempt().GetResponseStatus().GetCode())
			assert.NotNil(t, got.GetLastAttempt().GetResponseTime())
		})
	}
}

func TestDispatchUnreachableTarget(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(200))
	url := tg.srv.URL
	tg.srv.Close()

	q := e.queue(t, "unreachable", fastRetry(5)).GetName()
	task := e.task(t, q, httpTask(url))

	e.dispatch()

	got, err := e.getTask(t, task.GetName())
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.GetDispatchCount())
	assert.Zero(t, got.GetResponseCount())
	assert.EqualValues(t, codes.Unavailable, got.GetLastAttempt().GetResponseStatus().GetCode())
}

func TestPauseResumePurgeRunTask(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(200))
	q := e.queue(t, "control").GetName()

	_, err := e.client.PauseQueue(t.Context(), &cloudtaskspb.PauseQueueRequest{Name: q})
	require.NoError(t, err)

	a := e.task(t, q, httpTask(tg.srv.URL))
	e.task(t, q, httpTask(tg.srv.URL))

	e.dispatch()
	assert.Zero(t, tg.count(), "paused queue does not dispatch")

	ran, err := e.client.RunTask(t.Context(), &cloudtaskspb.RunTaskRequest{Name: a.GetName()})
	require.NoError(t, err)
	assert.EqualValues(t, 1, ran.GetDispatchCount())

	e.handler.Backend.Drain()
	assert.Equal(t, 1, tg.count(), "RunTask dispatches even when paused")

	_, err = e.getTask(t, a.GetName())
	assert.Equal(t, codes.NotFound, status.Code(err))

	purged, err := e.client.PurgeQueue(t.Context(), &cloudtaskspb.PurgeQueueRequest{Name: q})
	require.NoError(t, err)
	assert.NotNil(t, purged.GetPurgeTime())

	_, err = e.client.ResumeQueue(t.Context(), &cloudtaskspb.ResumeQueueRequest{Name: q})
	require.NoError(t, err)

	e.dispatch()
	assert.Equal(t, 1, tg.count(), "purged tasks are never dispatched")

	_, err = e.client.RunTask(t.Context(), &cloudtaskspb.RunTaskRequest{Name: a.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))

	e.task(t, q, httpTask(tg.srv.URL))
	e.dispatch()
	assert.Equal(t, 2, tg.count(), "resumed queue dispatches again")
}

func TestPausedQueueDispatchesAfterResume(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(200))
	q := e.queue(t, "resume").GetName()

	_, err := e.client.PauseQueue(t.Context(), &cloudtaskspb.PauseQueueRequest{Name: q})
	require.NoError(t, err)

	e.task(t, q, httpTask(tg.srv.URL))
	e.dispatch()
	require.Zero(t, tg.count())

	_, err = e.client.ResumeQueue(t.Context(), &cloudtaskspb.ResumeQueueRequest{Name: q})
	require.NoError(t, err)

	e.dispatch()
	assert.Equal(t, 1, tg.count())
}

func TestDispatchRateLimit(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(200))
	q := e.queue(t, "rate", func(q *cloudtaskspb.Queue) {
		q.RateLimits = &cloudtaskspb.RateLimits{MaxDispatchesPerSecond: 1}
	}).GetName()

	for range 3 {
		e.task(t, q, httpTask(tg.srv.URL))
	}

	e.dispatch()
	assert.Equal(t, 1, tg.count(), "burst of one")

	e.dispatch()
	assert.Equal(t, 1, tg.count(), "no tokens without time passing")

	e.clk.advance(time.Second)
	e.dispatch()
	assert.Equal(t, 2, tg.count())

	e.clk.advance(time.Second)
	e.dispatch()
	assert.Equal(t, 3, tg.count())
}

func TestDispatchConcurrencyLimit(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	tg := newBlockingTarget(t, started, release)
	q := e.queue(t, "conc", func(q *cloudtaskspb.Queue) {
		q.RateLimits = &cloudtaskspb.RateLimits{MaxConcurrentDispatches: 1}
	}).GetName()

	e.task(t, q, httpTask(tg.URL))
	e.task(t, q, httpTask(tg.URL))

	e.handler.Backend.DispatchDue()
	<-started

	e.handler.Backend.DispatchDue()
	assert.Empty(t, started, "second task waits for the in-flight attempt")

	close(release)
	e.handler.Backend.Drain()

	e.dispatch()
	<-started
}

func TestOIDCAndOAuthTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		auth     func(*cloudtaskspb.HttpRequest)
		name     string
		contains string
	}{
		{
			name: "oidc",
			auth: func(r *cloudtaskspb.HttpRequest) {
				r.AuthorizationHeader = &cloudtaskspb.HttpRequest_OidcToken{OidcToken: &cloudtaskspb.OidcToken{
					ServiceAccountEmail: "sa@p.iam.gserviceaccount.com", Audience: "my-audience",
				}}
			},
		},
		{
			name: "oauth",
			auth: func(r *cloudtaskspb.HttpRequest) {
				r.AuthorizationHeader = &cloudtaskspb.HttpRequest_OauthToken{OauthToken: &cloudtaskspb.OAuthToken{
					ServiceAccountEmail: "sa@p.iam.gserviceaccount.com",
				}}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			tg := newTarget(t, always(200))
			q := e.queue(t, "auth").GetName()

			e.task(t, q, func(task *cloudtaskspb.Task) {
				r := &cloudtaskspb.HttpRequest{Url: tg.srv.URL}
				tt.auth(r)
				task.MessageType = &cloudtaskspb.Task_HttpRequest{HttpRequest: r}
			})
			e.dispatch()

			require.Equal(t, 1, tg.count())
			authz := tg.hit(0).header.Get("Authorization")
			require.True(t, strings.HasPrefix(authz, "Bearer "))

			if tt.name == "oidc" {
				claims := decodeJWT(t, strings.TrimPrefix(authz, "Bearer "))
				assert.Equal(t, "my-audience", claims["aud"])
				assert.Equal(t, "sa@p.iam.gserviceaccount.com", claims["email"])
				assert.Equal(t, "unsigned-test-token", claims["gopherstack"])
			} else {
				assert.Contains(t, authz, "gopherstack-unsigned-oauth")
			}
		})
	}
}

func TestQueueHTTPTargetOverrides(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(200))
	host := strings.TrimPrefix(tg.srv.URL, "http://")
	hostOnly, portStr, _ := strings.Cut(host, ":")

	var port int64

	for _, c := range portStr {
		port = port*10 + int64(c-'0')
	}

	scheme := cloudtaskspb.UriOverride_HTTP
	q := e.queue(t, "override", func(q *cloudtaskspb.Queue) {
		q.HttpTarget = &cloudtaskspb.HttpTarget{
			HttpMethod: cloudtaskspb.HttpMethod_PATCH,
			UriOverride: &cloudtaskspb.UriOverride{
				Scheme: &scheme, Host: &hostOnly, Port: &port,
				PathOverride:  &cloudtaskspb.PathOverride{Path: "/overridden"},
				QueryOverride: &cloudtaskspb.QueryOverride{QueryParams: "a=b"},
			},
			HeaderOverrides: []*cloudtaskspb.HttpTarget_HeaderOverride{
				{Header: &cloudtaskspb.HttpTarget_Header{Key: "X-Forced", Value: "1"}},
			},
		}
	}).GetName()

	e.task(t, q, httpTask("https://unreachable.invalid/original"))
	e.dispatch()

	require.Equal(t, 1, tg.count())
	assert.Equal(t, "PATCH", tg.hit(0).method)
	assert.Equal(t, "/overridden?a=b", tg.hit(0).path)
	assert.Equal(t, "1", tg.hit(0).header.Get("X-Forced"))
}

func TestAppEngineDispatch(t *testing.T) {
	t.Parallel()

	appTask := func(task *cloudtaskspb.Task) {
		task.MessageType = &cloudtaskspb.Task_AppEngineHttpRequest{
			AppEngineHttpRequest: &cloudtaskspb.AppEngineHttpRequest{
				RelativeUri: "/work", Body: []byte("b"), Headers: map[string]string{"X-A": "1"},
			},
		}
	}

	t.Run("not configured", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		q := e.queue(t, "ae-off", fastRetry(5)).GetName()
		task := e.task(t, q, appTask)

		e.dispatch()

		got, err := e.getTask(t, task.GetName())
		require.NoError(t, err)
		assert.EqualValues(t, codes.FailedPrecondition, got.GetLastAttempt().GetResponseStatus().GetCode())
		assert.Contains(t, got.GetLastAttempt().GetResponseStatus().GetMessage(), "not dispatchable")
		assert.Zero(t, got.GetResponseCount())
	})

	t.Run("configured", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		tg := newTarget(t, always(200))
		e.handler.Backend.SetAppEngineBaseURL(tg.srv.URL)

		q := e.queue(t, "ae-on").GetName()
		e.task(t, q, appTask)
		e.dispatch()

		require.Equal(t, 1, tg.count())
		assert.Equal(t, "/work", tg.hit(0).path)
		assert.Equal(t, "POST", tg.hit(0).method)
		assert.Equal(t, "1", tg.hit(0).header.Get("X-A"))
		assert.Equal(t, "ae-on", tg.hit(0).header.Get("X-Appengine-Queuename"))
	})
}

func TestBackoffSchedule(t *testing.T) {
	t.Parallel()

	rc := &cloudtaskspb.RetryConfig{
		MinBackoff: durationpb.New(10 * time.Second), MaxBackoff: durationpb.New(300 * time.Second), MaxDoublings: 3,
	}
	want := []int{10, 20, 40, 80, 160, 240, 300, 300}

	for i, w := range want {
		assert.Equal(t, time.Duration(w)*time.Second, gcpcloudtasks.Backoff(rc, int32(i+1)), "failure %d", i+1)
	}
}

func TestBatchRPCsUnimplemented(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	q := e.queue(t, "batch").GetName()

	_, err := e.client.BatchDeleteTasks(
		t.Context(),
		&cloudtaskspb.BatchDeleteTasksRequest{Parent: q, Names: []string{q + "/tasks/x"}},
	)
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}
