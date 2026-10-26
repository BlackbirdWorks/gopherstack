package gcpscheduler_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	locationpb "google.golang.org/genproto/googleapis/cloud/location"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestCreateJobDefaults(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	j := e.job(t, "defaults", httpJob("https://example.test/x"), withSchedule("0 9 * * *"))

	assert.Equal(t, schedulerpb.Job_ENABLED, j.GetState())
	assert.Equal(t, "Etc/UTC", j.GetTimeZone())
	assert.True(t, time.Date(2030, 1, 16, 9, 0, 0, 0, time.UTC).Equal(j.GetScheduleTime().AsTime()))
	assert.True(t, epoch().Equal(j.GetUserUpdateTime().AsTime()))
	assert.Nil(t, j.GetLastAttemptTime())

	rc := j.GetRetryConfig()
	assert.EqualValues(t, 0, rc.GetRetryCount())
	assert.Equal(t, 5*time.Second, rc.GetMinBackoffDuration().AsDuration())
	assert.Equal(t, time.Hour, rc.GetMaxBackoffDuration().AsDuration())
	assert.EqualValues(t, 5, rc.GetMaxDoublings())
}

func TestCreateJobTimeZoneSchedule(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	j := e.job(t, "tz", httpJob("https://example.test/x"), withSchedule("0 9 * * *"),
		func(j *schedulerpb.Job) { j.TimeZone = "America/New_York" })

	assert.True(t, time.Date(2030, 1, 15, 14, 0, 0, 0, time.UTC).Equal(j.GetScheduleTime().AsTime()))
}

func TestCreateJobAutoName(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	j, err := e.client.CreateJob(t.Context(), &schedulerpb.CreateJobRequest{
		Parent: location,
		Job: &schedulerpb.Job{
			Schedule: "* * * * *",
			Target:   &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{Uri: "https://example.test"}},
		},
	})
	require.NoError(t, err)
	assert.Contains(t, j.GetName(), location+"/jobs/")
}

func TestCreateJobErrors(t *testing.T) {
	t.Parallel()

	good := func(mut func(*schedulerpb.Job)) *schedulerpb.Job {
		j := &schedulerpb.Job{
			Name: location + "/jobs/j", Schedule: "* * * * *",
			Target: &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{Uri: "https://example.test"}},
		}
		if mut != nil {
			mut(j)
		}

		return j
	}

	tests := []struct {
		job    *schedulerpb.Job
		name   string
		parent string
	}{
		{good(nil), "bad parent", "projects/p"},
		{nil, "missing job", location},
		{good(func(j *schedulerpb.Job) {
			j.Name = "projects/test-project/locations/europe-west1/jobs/j"
		}), "name under other parent", location},
		{good(func(j *schedulerpb.Job) { j.Name = location + "/jobs/bad id" }), "bad id", location},
		{good(func(j *schedulerpb.Job) { j.Schedule = "" }), "no schedule", location},
		{good(func(j *schedulerpb.Job) { j.Schedule = "every 6 hours mon,wed,fri" }), "bad cron", location},
		{good(func(j *schedulerpb.Job) { j.TimeZone = "Mars/Olympus" }), "bad zone", location},
		{good(func(j *schedulerpb.Job) { j.Target = nil }), "no target", location},
		{good(func(j *schedulerpb.Job) {
			j.Target = &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{Uri: "/x"}}
		}), "relative http uri", location},
		{good(func(j *schedulerpb.Job) {
			j.Target = &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{
				Uri: "https://example.test", HttpMethod: schedulerpb.HttpMethod_GET, Body: []byte("x"),
			}}
		}), "get with body", location},
		{good(func(j *schedulerpb.Job) {
			j.Target = &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{
				Uri:                 "https://example.test",
				AuthorizationHeader: &schedulerpb.HttpTarget_OidcToken{OidcToken: &schedulerpb.OidcToken{}},
			}}
		}), "oidc without account", location},
		{good(func(j *schedulerpb.Job) {
			j.Target = &schedulerpb.Job_PubsubTarget{
				PubsubTarget: &schedulerpb.PubsubTarget{TopicName: "t", Data: []byte("x")},
			}
		}), "bad topic", location},
		{good(func(j *schedulerpb.Job) {
			j.Target = &schedulerpb.Job_PubsubTarget{
				PubsubTarget: &schedulerpb.PubsubTarget{TopicName: "projects/p/topics/t"},
			}
		}), "empty pubsub message", location},
		{good(func(j *schedulerpb.Job) {
			j.Target = &schedulerpb.Job_AppEngineHttpTarget{
				AppEngineHttpTarget: &schedulerpb.AppEngineHttpTarget{RelativeUri: "x"},
			}
		}), "relative uri without slash", location},
		{
			good(func(j *schedulerpb.Job) { j.AttemptDeadline = durationpb.New(time.Second) }),
			"deadline too short",
			location,
		},
		{good(func(j *schedulerpb.Job) {
			j.AttemptDeadline = durationpb.New(time.Hour)
		}), "deadline too long for http", location},
		{good(func(j *schedulerpb.Job) {
			j.RetryConfig = &schedulerpb.RetryConfig{RetryCount: 6}
		}), "retry count too high", location},
		{good(func(j *schedulerpb.Job) {
			j.RetryConfig = &schedulerpb.RetryConfig{
				MinBackoffDuration: durationpb.New(time.Minute), MaxBackoffDuration: durationpb.New(time.Second),
			}
		}), "min backoff above max", location},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			_, err := e.client.CreateJob(t.Context(), &schedulerpb.CreateJobRequest{Parent: tt.parent, Job: tt.job})
			assert.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
		})
	}
}

func TestCreateJobDuplicate(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.job(t, "dup", httpJob("https://example.test"))

	_, err := e.client.CreateJob(t.Context(), &schedulerpb.CreateJobRequest{
		Parent: location,
		Job: &schedulerpb.Job{
			Name: location + "/jobs/dup", Schedule: "* * * * *",
			Target: &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{Uri: "https://x.test"}},
		},
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
}

func TestGetDeleteJob(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	j := e.job(t, "gone", httpJob("https://example.test"))
	assert.Equal(t, j.GetName(), e.get(t, j.GetName()).GetName())

	require.NoError(t, e.client.DeleteJob(t.Context(), &schedulerpb.DeleteJobRequest{Name: j.GetName()}))

	_, err := e.client.GetJob(t.Context(), &schedulerpb.GetJobRequest{Name: j.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))

	err = e.client.DeleteJob(t.Context(), &schedulerpb.DeleteJobRequest{Name: j.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.client.GetJob(t.Context(), &schedulerpb.GetJobRequest{Name: "garbage"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	e.job(t, "gone", httpJob("https://example.test"))
}

func TestListJobsPagination(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	for i := range 5 {
		e.job(t, fmt.Sprintf("job-%d", i), httpJob("https://example.test"))
	}

	otherLoc := "projects/test-project/locations/europe-west1"
	_, err := e.client.CreateJob(t.Context(), &schedulerpb.CreateJobRequest{
		Parent: otherLoc,
		Job: &schedulerpb.Job{
			Name: otherLoc + "/jobs/elsewhere", Schedule: "* * * * *",
			Target: &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{Uri: "https://x.test"}},
		},
	})
	require.NoError(t, err)

	it := e.client.ListJobs(t.Context(), &schedulerpb.ListJobsRequest{Parent: location, PageSize: 2})

	var names []string

	for {
		j, nerr := it.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		names = append(names, j.GetName())
	}

	require.Len(t, names, 5)
	assert.Equal(t, location+"/jobs/job-0", names[0])
	assert.Equal(t, location+"/jobs/job-4", names[4])
}

func TestUpdateJobMask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, got *schedulerpb.Job)
		name  string
		job   *schedulerpb.Job
		paths []string
		code  codes.Code
	}{
		{
			name: "description only", paths: []string{"description"},
			job: &schedulerpb.Job{Description: "new", Schedule: "0 0 * * *"},
			check: func(t *testing.T, got *schedulerpb.Job) {
				t.Helper()
				assert.Equal(t, "new", got.GetDescription())
				assert.Equal(t, "*/5 * * * *", got.GetSchedule(), "unmasked field untouched")
			},
		},
		{
			name: "schedule recomputes schedule_time", paths: []string{"schedule"},
			job: &schedulerpb.Job{Schedule: "0 12 * * *"},
			check: func(t *testing.T, got *schedulerpb.Job) {
				t.Helper()
				assert.True(t, time.Date(2030, 1, 15, 12, 0, 0, 0, time.UTC).Equal(got.GetScheduleTime().AsTime()))
			},
		},
		{
			name: "time zone", paths: []string{"schedule", "time_zone"},
			job: &schedulerpb.Job{Schedule: "0 12 * * *", TimeZone: "Asia/Tokyo"},
			check: func(t *testing.T, got *schedulerpb.Job) {
				t.Helper()
				assert.True(t, time.Date(2030, 1, 16, 3, 0, 0, 0, time.UTC).Equal(got.GetScheduleTime().AsTime()))
			},
		},
		{
			name: "nested retry field", paths: []string{"retry_config.retry_count"},
			job: &schedulerpb.Job{RetryConfig: &schedulerpb.RetryConfig{RetryCount: 3}},
			check: func(t *testing.T, got *schedulerpb.Job) {
				t.Helper()
				assert.EqualValues(t, 3, got.GetRetryConfig().GetRetryCount())
				assert.EqualValues(t, 5, got.GetRetryConfig().GetMaxDoublings())
			},
		},
		{
			name: "switch target", paths: []string{"pubsub_target", "http_target"},
			job: &schedulerpb.Job{Target: &schedulerpb.Job_PubsubTarget{
				PubsubTarget: &schedulerpb.PubsubTarget{TopicName: "projects/p/topics/t", Data: []byte("d")},
			}},
			check: func(t *testing.T, got *schedulerpb.Job) {
				t.Helper()
				assert.Equal(t, "projects/p/topics/t", got.GetPubsubTarget().GetTopicName())
				assert.Nil(t, got.GetHttpTarget())
			},
		},
		{name: "state is immutable", paths: []string{"state"}, job: &schedulerpb.Job{}, code: codes.InvalidArgument},
		{name: "name is immutable", paths: []string{"name"}, job: &schedulerpb.Job{}, code: codes.InvalidArgument},
		{name: "unknown field", paths: []string{"nope"}, job: &schedulerpb.Job{}, code: codes.InvalidArgument},
		{
			name: "bad value rejected", paths: []string{"schedule"}, job: &schedulerpb.Job{Schedule: "bogus"},
			code: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			orig := e.job(t, "upd", httpJob("https://example.test"))
			e.clk.advance(time.Minute)

			tt.job.Name = orig.GetName()
			got, err := e.client.UpdateJob(t.Context(), &schedulerpb.UpdateJobRequest{
				Job: tt.job, UpdateMask: &fieldmaskpb.FieldMask{Paths: tt.paths},
			})

			if tt.code != codes.OK {
				assert.Equal(t, tt.code, status.Code(err), "%v", err)
				assert.Equal(t, "*/5 * * * *", e.get(t, orig.GetName()).GetSchedule(), "failed update changes nothing")

				return
			}

			require.NoError(t, err)
			tt.check(t, got)
			assert.True(t, epoch().Add(time.Minute).Equal(got.GetUserUpdateTime().AsTime()))
		})
	}
}

func TestUpdateJobMissingAndEmptyMask(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	_, err := e.client.UpdateJob(t.Context(), &schedulerpb.UpdateJobRequest{
		Job: &schedulerpb.Job{Name: location + "/jobs/none"},
	})
	assert.Equal(t, codes.NotFound, status.Code(err))

	orig := e.job(t, "full", httpJob("https://example.test"), func(j *schedulerpb.Job) { j.Description = "old" })
	got, err := e.client.UpdateJob(t.Context(), &schedulerpb.UpdateJobRequest{Job: &schedulerpb.Job{
		Name: orig.GetName(), Schedule: "0 1 * * *",
		Target: &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{Uri: "https://other.test"}},
	}})
	require.NoError(t, err)
	assert.Empty(t, got.GetDescription(), "empty mask replaces every mutable field")
	assert.Equal(t, "https://other.test", got.GetHttpTarget().GetUri())
}

func TestPauseResume(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	j := e.job(t, "pr", httpJob("https://example.test"), withSchedule("0 9 * * *"))
	ctx := t.Context()

	same, err := e.client.ResumeJob(ctx, &schedulerpb.ResumeJobRequest{Name: j.GetName()})
	require.NoError(t, err, "the Terraform provider resumes every new job")
	assert.Equal(t, schedulerpb.Job_ENABLED, same.GetState())

	paused, err := e.client.PauseJob(ctx, &schedulerpb.PauseJobRequest{Name: j.GetName()})
	require.NoError(t, err)
	assert.Equal(t, schedulerpb.Job_PAUSED, paused.GetState())
	assert.Nil(t, paused.GetScheduleTime())

	_, err = e.client.PauseJob(ctx, &schedulerpb.PauseJobRequest{Name: j.GetName()})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "pause needs ENABLED")

	e.clk.advance(24 * time.Hour)

	resumed, err := e.client.ResumeJob(ctx, &schedulerpb.ResumeJobRequest{Name: j.GetName()})
	require.NoError(t, err)
	assert.Equal(t, schedulerpb.Job_ENABLED, resumed.GetState())
	assert.True(t, time.Date(2030, 1, 17, 9, 0, 0, 0, time.UTC).Equal(resumed.GetScheduleTime().AsTime()))

	ops := []func(string) error{
		func(n string) error {
			_, opErr := e.client.PauseJob(ctx, &schedulerpb.PauseJobRequest{Name: n})

			return opErr
		},
		func(n string) error {
			_, opErr := e.client.ResumeJob(ctx, &schedulerpb.ResumeJobRequest{Name: n})

			return opErr
		},
		func(n string) error {
			_, opErr := e.client.RunJob(ctx, &schedulerpb.RunJobRequest{Name: n})

			return opErr
		},
	}

	for _, op := range ops {
		assert.Equal(t, codes.NotFound, status.Code(op(location+"/jobs/none")))
	}
}

func TestLocations(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	loc, err := e.client.GetLocation(t.Context(), &locationpb.GetLocationRequest{Name: location})
	require.NoError(t, err)
	assert.Equal(t, "us-central1", loc.GetLocationId())
	assert.Equal(t, "Iowa", loc.GetDisplayName())
	assert.Equal(t, "us-central1", loc.GetLabels()["cloud.googleapis.com/region"])

	_, err = e.client.GetLocation(t.Context(), &locationpb.GetLocationRequest{
		Name: "projects/test-project/locations/mars-north1",
	})
	assert.Equal(t, codes.NotFound, status.Code(err))

	it := e.client.ListLocations(
		t.Context(),
		&locationpb.ListLocationsRequest{Name: "projects/test-project", PageSize: 7},
	)

	var ids []string

	for {
		l, nerr := it.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		ids = append(ids, l.GetLocationId())
	}

	assert.Greater(t, len(ids), 30)
	assert.Contains(t, ids, "europe-west1")

	_, err = e.client.ListLocations(t.Context(), &locationpb.ListLocationsRequest{Name: "x"}).Next()
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
