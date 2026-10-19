package gcpscheduler_test

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestRunJobDeliversHTTP(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(200))

	j := e.job(t, "run", func(j *schedulerpb.Job) {
		j.Target = &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{
			Uri:        tg.srv.URL + "/hook?x=1",
			HttpMethod: schedulerpb.HttpMethod_PUT,
			Headers:    map[string]string{"X-Custom": "yes", "Content-Type": "text/plain"},
			Body:       []byte("hello"),
		}}
	})

	e.clk.advance(time.Second)

	ran, err := e.client.RunJob(t.Context(), &schedulerpb.RunJobRequest{Name: j.GetName()})
	require.NoError(t, err)
	assert.True(t, e.clk.now().Equal(ran.GetLastAttemptTime().AsTime()))

	e.handler.Backend.Drain()
	require.Equal(t, 1, tg.count())

	h := tg.hit(0)
	assert.Equal(t, "PUT", h.method)
	assert.Equal(t, "/hook?x=1", h.path)
	assert.Equal(t, "hello", string(h.body))
	assert.Equal(t, "yes", h.header.Get("X-Custom"))
	assert.Equal(t, "text/plain", h.header.Get("Content-Type"))
	assert.Equal(t, "Google-Cloud-Scheduler", h.header.Get("User-Agent"))
	assert.Equal(t, "true", h.header.Get("X-Cloudscheduler"))
	assert.Equal(t, "run", h.header.Get("X-Cloudscheduler-Jobname"))
	assert.Equal(t, "2030-01-15T10:07:31Z", h.header.Get("X-Cloudscheduler-Scheduletime"))

	got := e.get(t, j.GetName())
	assert.EqualValues(t, code.Code_OK, got.GetStatus().GetCode())
	assert.NotNil(t, got.GetScheduleTime(), "RunJob does not disturb the cron schedule")
}

func TestHTTPDefaults(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(204))
	j := e.job(t, "defaults", func(j *schedulerpb.Job) {
		j.Target = &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{
			Uri: tg.srv.URL, Body: []byte("raw"),
		}}
	})

	_, err := e.client.RunJob(t.Context(), &schedulerpb.RunJobRequest{Name: j.GetName()})
	require.NoError(t, err)
	e.handler.Backend.Drain()

	require.Equal(t, 1, tg.count())
	assert.Equal(t, "POST", tg.hit(0).method)
	assert.Equal(t, "application/octet-stream", tg.hit(0).header.Get("Content-Type"))
}

func TestHTTPAuthHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		auth   func(*schedulerpb.HttpTarget)
		verify func(t *testing.T, authz string)
		name   string
	}{
		{
			name: "oidc", auth: func(h *schedulerpb.HttpTarget) {
				h.AuthorizationHeader = &schedulerpb.HttpTarget_OidcToken{OidcToken: &schedulerpb.OidcToken{
					ServiceAccountEmail: "sa@p.iam.gserviceaccount.com", Audience: "my-aud",
				}}
			},
			verify: func(t *testing.T, authz string) {
				t.Helper()
				claims := decodeJWT(t, authz[len("Bearer "):])
				assert.Equal(t, "my-aud", claims["aud"])
				assert.Equal(t, "sa@p.iam.gserviceaccount.com", claims["email"])
				assert.Equal(t, "unsigned-test-token", claims["gopherstack"])
			},
		},
		{
			name: "oauth", auth: func(h *schedulerpb.HttpTarget) {
				h.AuthorizationHeader = &schedulerpb.HttpTarget_OauthToken{OauthToken: &schedulerpb.OAuthToken{
					ServiceAccountEmail: "sa@p.iam.gserviceaccount.com",
				}}
			},
			verify: func(t *testing.T, authz string) {
				t.Helper()
				assert.Contains(t, authz, "Bearer gopherstack-unsigned-oauth.")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			tg := newTarget(t, always(200))
			j := e.job(t, "auth", func(j *schedulerpb.Job) {
				h := &schedulerpb.HttpTarget{Uri: tg.srv.URL}
				tt.auth(h)
				j.Target = &schedulerpb.Job_HttpTarget{HttpTarget: h}
			})

			_, err := e.client.RunJob(t.Context(), &schedulerpb.RunJobRequest{Name: j.GetName()})
			require.NoError(t, err)
			e.handler.Backend.Drain()

			require.Equal(t, 1, tg.count())
			tt.verify(t, tg.hit(0).header.Get("Authorization"))
		})
	}
}

func TestCronFiring(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(200))
	j := e.job(t, "cron", httpJob(tg.srv.URL), withSchedule("*/5 * * * *"))
	assert.True(t, time.Date(2030, 1, 15, 10, 10, 0, 0, time.UTC).Equal(j.GetScheduleTime().AsTime()))

	e.dispatch()
	assert.Equal(t, 0, tg.count(), "not due yet")

	e.clk.advance(2*time.Minute + 30*time.Second)
	e.dispatch()
	require.Equal(t, 1, tg.count())
	assert.Equal(t, "2030-01-15T10:10:00Z", tg.hit(0).header.Get("X-Cloudscheduler-Scheduletime"))

	got := e.get(t, j.GetName())
	assert.True(t, time.Date(2030, 1, 15, 10, 15, 0, 0, time.UTC).Equal(got.GetScheduleTime().AsTime()))
	assert.True(t, e.clk.now().Equal(got.GetLastAttemptTime().AsTime()))

	e.dispatch()
	assert.Equal(t, 1, tg.count(), "fires once per slot")

	e.clk.advance(time.Hour)
	e.dispatch()
	assert.Equal(t, 2, tg.count(), "missed slots collapse into one fire")
}

func TestCronTimeZoneFiring(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(200))
	e.job(t, "tz", httpJob(tg.srv.URL), withSchedule("0 12 * * *"),
		func(j *schedulerpb.Job) { j.TimeZone = "Asia/Tokyo" })

	e.clk.advance(16*time.Hour + 53*time.Minute) // 03:00 UTC on the 16th = noon Tokyo
	e.dispatch()
	assert.Equal(t, 1, tg.count())
	assert.Equal(t, "2030-01-16T03:00:00Z", tg.hit(0).header.Get("X-Cloudscheduler-Scheduletime"))
}

func TestPausedJobDoesNotFire(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(200))
	j := e.job(t, "paused", httpJob(tg.srv.URL), withSchedule("* * * * *"))

	_, err := e.client.PauseJob(t.Context(), &schedulerpb.PauseJobRequest{Name: j.GetName()})
	require.NoError(t, err)

	e.clk.advance(10 * time.Minute)
	e.dispatch()
	assert.Equal(t, 0, tg.count())

	_, err = e.client.ResumeJob(t.Context(), &schedulerpb.ResumeJobRequest{Name: j.GetName()})
	require.NoError(t, err)

	e.dispatch()
	assert.Equal(t, 0, tg.count(), "resume does not replay missed slots")

	e.clk.advance(time.Minute)
	e.dispatch()
	assert.Equal(t, 1, tg.count())
}

func TestRunJobWhilePausedStillRuns(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(200))
	j := e.job(t, "pausedrun", httpJob(tg.srv.URL))

	_, err := e.client.PauseJob(t.Context(), &schedulerpb.PauseJobRequest{Name: j.GetName()})
	require.NoError(t, err)

	_, err = e.client.RunJob(t.Context(), &schedulerpb.RunJobRequest{Name: j.GetName()})
	require.NoError(t, err)
	e.handler.Backend.Drain()
	assert.Equal(t, 1, tg.count())
	assert.Equal(t, schedulerpb.Job_PAUSED, e.get(t, j.GetName()).GetState())
}

func retryCfg(count int32, maxDur time.Duration) func(*schedulerpb.Job) {
	return func(j *schedulerpb.Job) {
		j.RetryConfig = &schedulerpb.RetryConfig{
			RetryCount:         count,
			MaxRetryDuration:   durationpb.New(maxDur),
			MinBackoffDuration: durationpb.New(10 * time.Second),
			MaxBackoffDuration: durationpb.New(40 * time.Second),
			MaxDoublings:       2,
		}
	}
}

func TestRetryCountBackoff(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(503))
	j := e.job(t, "retry", httpJob(tg.srv.URL), withSchedule("0 0 1 1 *"), retryCfg(3, 0))

	_, err := e.client.RunJob(t.Context(), &schedulerpb.RunJobRequest{Name: j.GetName()})
	require.NoError(t, err)
	e.handler.Backend.Drain()
	require.Equal(t, 1, tg.count())

	got := e.get(t, j.GetName())
	assert.EqualValues(t, code.Code_UNAVAILABLE, got.GetStatus().GetCode())

	for i, wait := range []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second} {
		e.clk.advance(wait - time.Second)
		e.dispatch()
		require.Equal(t, 1+i, tg.count(), "retry %d not due yet", i+1)

		e.clk.advance(time.Second)
		e.dispatch()
		require.Equal(t, 2+i, tg.count(), "retry %d fires after %s", i+1, wait)
	}

	e.clk.advance(time.Hour)
	e.dispatch()
	assert.Equal(t, 4, tg.count(), "retry_count 3 means 4 attempts in total")
}

func TestNoRetryByDefault(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(500))
	j := e.job(t, "noretry", httpJob(tg.srv.URL), withSchedule("0 0 1 1 *"))

	_, err := e.client.RunJob(t.Context(), &schedulerpb.RunJobRequest{Name: j.GetName()})
	require.NoError(t, err)
	e.handler.Backend.Drain()

	e.clk.advance(time.Hour)
	e.dispatch()
	assert.Equal(t, 1, tg.count())
	assert.EqualValues(t, code.Code_INTERNAL, e.get(t, j.GetName()).GetStatus().GetCode())
}

func TestRetrySucceedsClearsRetry(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, func(n int) int {
		if n < 2 {
			return http.StatusInternalServerError
		}

		return http.StatusOK
	})
	j := e.job(t, "recover", httpJob(tg.srv.URL), withSchedule("0 0 1 1 *"), retryCfg(5, 0))

	_, err := e.client.RunJob(t.Context(), &schedulerpb.RunJobRequest{Name: j.GetName()})
	require.NoError(t, err)
	e.handler.Backend.Drain()

	e.clk.advance(10 * time.Second)
	e.dispatch()
	require.Equal(t, 2, tg.count())
	assert.EqualValues(t, code.Code_OK, e.get(t, j.GetName()).GetStatus().GetCode())

	e.clk.advance(time.Hour)
	e.dispatch()
	assert.Equal(t, 2, tg.count(), "no further retries after success")
}

func TestMaxRetryDuration(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(500))
	j := e.job(t, "dur", httpJob(tg.srv.URL), withSchedule("0 0 1 1 *"), retryCfg(0, 45*time.Second))

	_, err := e.client.RunJob(t.Context(), &schedulerpb.RunJobRequest{Name: j.GetName()})
	require.NoError(t, err)
	e.handler.Backend.Drain()

	for range 20 {
		e.clk.advance(10 * time.Second)
		e.dispatch()
	}

	// attempts at t=0, 10, 30 (10s then 20s backoff), then 70 is past the 45s window.
	assert.Equal(t, 4, tg.count())
}

func TestCronFirePreemptsRetry(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(500))
	j := e.job(t, "preempt", httpJob(tg.srv.URL), withSchedule("*/5 * * * *"), retryCfg(5, 0))

	e.clk.advance(2*time.Minute + 30*time.Second)
	e.dispatch()
	require.Equal(t, 1, tg.count())

	e.clk.advance(5 * time.Minute)
	e.dispatch()
	assert.Equal(t, 2, tg.count())

	assert.EqualValues(t, code.Code_INTERNAL, e.get(t, j.GetName()).GetStatus().GetCode())
}

func TestPauseCancelsPendingRetry(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tg := newTarget(t, always(500))
	j := e.job(t, "pauseretry", httpJob(tg.srv.URL), withSchedule("0 0 1 1 *"), retryCfg(5, 0))

	_, err := e.client.RunJob(t.Context(), &schedulerpb.RunJobRequest{Name: j.GetName()})
	require.NoError(t, err)
	e.handler.Backend.Drain()

	_, err = e.client.PauseJob(t.Context(), &schedulerpb.PauseJobRequest{Name: j.GetName()})
	require.NoError(t, err)
	_, err = e.client.ResumeJob(t.Context(), &schedulerpb.ResumeJobRequest{Name: j.GetName()})
	require.NoError(t, err)

	e.clk.advance(time.Minute)
	e.dispatch()
	assert.Equal(t, 1, tg.count())
}

func TestAppEngineTarget(t *testing.T) {
	t.Parallel()

	tg := newTarget(t, always(200))

	tests := []struct {
		name    string
		baseURL string
		wantHit bool
		want    code.Code
	}{
		{"configured", "SERVER", true, code.Code_OK},
		{"not configured", "", false, code.Code_FAILED_PRECONDITION},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			before := tg.count()

			if tt.baseURL == "SERVER" {
				e.handler.Backend.SetAppEngineBaseURL(tg.srv.URL + "/")
			}

			j := e.job(t, "ae", func(j *schedulerpb.Job) {
				j.Target = &schedulerpb.Job_AppEngineHttpTarget{AppEngineHttpTarget: &schedulerpb.AppEngineHttpTarget{
					HttpMethod:       schedulerpb.HttpMethod_POST,
					RelativeUri:      "/tasks/run",
					Body:             []byte("payload"),
					AppEngineRouting: &schedulerpb.AppEngineRouting{Host: "svc.example.test"},
				}}
			})

			_, err := e.client.RunJob(t.Context(), &schedulerpb.RunJobRequest{Name: j.GetName()})
			require.NoError(t, err)
			e.handler.Backend.Drain()

			assert.EqualValues(t, tt.want, e.get(t, j.GetName()).GetStatus().GetCode())

			if tt.wantHit {
				h := tg.hit(before)
				assert.Equal(t, "/tasks/run", h.path)
				assert.Equal(t, "payload", string(h.body))
				assert.Equal(t, "svc.example.test", h.header.Get("X-Appengine-Routing-Host"))
			}
		})
	}
}

type fakePublisher struct {
	err  error
	reqs []*pubsubpb.PublishRequest
	mu   sync.Mutex
}

func (f *fakePublisher) Publish(req *pubsubpb.PublishRequest) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.reqs = append(f.reqs, req)

	return []string{"1"}, f.err
}

func TestPubsubTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pub     *fakePublisher
		name    string
		want    code.Code
		noWired bool
	}{
		{pub: &fakePublisher{}, name: "delivered", want: code.Code_OK},
		{
			pub:  &fakePublisher{err: status.Error(codes.NotFound, "no topic")},
			name: "topic missing",
			want: code.Code_NOT_FOUND,
		},
		{name: "publisher not wired", noWired: true, want: code.Code_FAILED_PRECONDITION},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			if !tt.noWired {
				e.handler.Backend.SetPublisher(tt.pub)
			}

			j := e.job(t, "ps", func(j *schedulerpb.Job) {
				j.Target = &schedulerpb.Job_PubsubTarget{PubsubTarget: &schedulerpb.PubsubTarget{
					TopicName:  "projects/test-project/topics/events",
					Data:       []byte("tick"),
					Attributes: map[string]string{"k": "v"},
				}}
			})

			_, err := e.client.RunJob(t.Context(), &schedulerpb.RunJobRequest{Name: j.GetName()})
			require.NoError(t, err)
			e.handler.Backend.Drain()

			assert.EqualValues(t, tt.want, e.get(t, j.GetName()).GetStatus().GetCode())

			if tt.noWired {
				return
			}

			require.Len(t, tt.pub.reqs, 1)

			req := tt.pub.reqs[0]
			assert.Equal(t, "projects/test-project/topics/events", req.GetTopic())

			msg := req.GetMessages()[0]
			assert.Equal(t, "tick", string(msg.GetData()))
			assert.Equal(t, "v", msg.GetAttributes()["k"])
			assert.Equal(t, j.GetName(), msg.GetAttributes()["googclient_schedulejobname"])
			assert.Equal(t, "2030-01-15T10:07:30Z", msg.GetAttributes()["googclient_scheduletime"])
		})
	}
}

func TestDeleteDuringAttemptDropsOutcome(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	srv := newBlockingServer(t, started, release)
	j := e.job(t, "inflight", httpJob(srv.URL))

	_, err := e.client.RunJob(t.Context(), &schedulerpb.RunJobRequest{Name: j.GetName()})
	require.NoError(t, err)

	<-started
	require.NoError(t, e.client.DeleteJob(t.Context(), &schedulerpb.DeleteJobRequest{Name: j.GetName()}))
	close(release)
	e.handler.Backend.Drain()

	_, err = e.client.GetJob(t.Context(), &schedulerpb.GetJobRequest{Name: j.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestShutdownAbortsInFlight(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	srv := newBlockingServer(t, started, release)
	j := e.job(t, "abort", httpJob(srv.URL))

	_, err := e.client.RunJob(t.Context(), &schedulerpb.RunJobRequest{Name: j.GetName()})
	require.NoError(t, err)

	<-started
	e.handler.Shutdown(context.Background())
	close(release)
}
