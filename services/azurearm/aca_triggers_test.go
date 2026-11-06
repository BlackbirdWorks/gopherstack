package azurearm_test

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appcontainers/armappcontainers/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
)

const noSkip = -time.Second

type trigTimer struct {
	fn   func()
	d    time.Duration
	stop bool
}

func (t *trigTimer) C() <-chan time.Time { return nil }

func (t *trigTimer) Stop() bool {
	t.stop = true

	return true
}

// trigClock is a settable clock whose timers fire only when the test says so.
type trigClock struct {
	now    time.Time
	timers []*trigTimer
	mu     sync.Mutex
}

func (c *trigClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *trigClock) NewTimer(time.Duration) gcpscale.Timer { return &trigTimer{stop: true} }

func (c *trigClock) AfterFunc(d time.Duration, f func()) gcpscale.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()

	t := &trigTimer{fn: f, d: d}
	c.timers = append(c.timers, t)

	return t
}

// advance moves the clock and fires the live timers except those with the skipped delay (pool idle timers).
func (c *trigClock) advance(d, skip time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	timers := c.timers
	c.timers = nil
	c.mu.Unlock()

	for _, t := range timers {
		if !t.stop && t.d != skip {
			t.fn()
		}
	}
}

type trigMeters struct {
	sb, q map[string]int
	mu    sync.Mutex
}

func (m *trigMeters) ServiceBusMessages(queue, _, _ string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.sb[queue], nil
}

func (m *trigMeters) StorageQueueMessages(queue string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.q[queue], nil
}

func trigJob(
	envID, image string,
	trigger armappcontainers.TriggerType,
	retry, timeout int32,
	cfgFn func(*armappcontainers.JobConfiguration),
) armappcontainers.Job {
	cfg := &armappcontainers.JobConfiguration{
		TriggerType: &trigger, ReplicaTimeout: &timeout, ReplicaRetryLimit: &retry,
	}
	cfgFn(cfg)

	return armappcontainers.Job{
		Location: new("local"),
		Properties: &armappcontainers.JobProperties{
			EnvironmentID: &envID, Configuration: cfg,
			Template: &armappcontainers.JobTemplate{Containers: []*armappcontainers.Container{{
				Name: new("work"), Image: &image,
				Resources: &armappcontainers.ContainerResources{CPU: new(0.25), Memory: new("0.5Gi")},
			}}},
		},
	}
}

func (s *acaStack) putJobModel(t *testing.T, name string, j armappcontainers.Job) error {
	t.Helper()

	poller, err := s.factory.NewJobsClient().BeginCreateOrUpdate(t.Context(), "rg", name, j, nil)
	if err != nil {
		return err
	}

	_, err = poller.PollUntilDone(t.Context(), pollFast())

	return err
}

func (s *acaStack) execStatuses(t *testing.T, job string) []armappcontainers.JobExecutionRunningState {
	t.Helper()

	execs := s.executions(t, job)
	out := make([]armappcontainers.JobExecutionRunningState, 0, len(execs))

	for _, e := range execs {
		out = append(out, *e.Properties.Status)
	}

	return out
}

func TestACA_ScheduleTrigger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cron        string
		steps       []time.Duration
		wantExecs   int
		wantStarted int
		parallelism int32
	}{
		{
			name:        "every-five-minutes",
			cron:        "*/5 * * * *",
			parallelism: 1,
			steps:       []time.Duration{4 * time.Minute},
			wantExecs:   0,
		},
		{
			name:        "fires-once",
			cron:        "*/5 * * * *",
			parallelism: 1,
			steps:       []time.Duration{5 * time.Minute},
			wantExecs:   1,
			wantStarted: 1,
		},
		{
			name:        "fires-twice",
			cron:        "*/5 * * * *",
			parallelism: 1,
			steps:       []time.Duration{5 * time.Minute, 5 * time.Minute},
			wantExecs:   2,
			wantStarted: 2,
		},
		{
			name:        "parallelism",
			cron:        "0 * * * *",
			parallelism: 3,
			steps:       []time.Duration{time.Hour},
			wantExecs:   1,
			wantStarted: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACAStack(t)
			clock := &trigClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			s.apps.SetClock(clock)
			s.rt.scripts["job-long"] = nil
			envID := *s.mustEnv(t, "rg", "env1").ID

			job := trigJob(envID, "job-long", armappcontainers.TriggerTypeSchedule, 0, 600,
				func(c *armappcontainers.JobConfiguration) {
					c.ScheduleTriggerConfig = &armappcontainers.JobConfigurationScheduleTriggerConfig{
						CronExpression: &tc.cron, Parallelism: &tc.parallelism, ReplicaCompletionCount: new(int32(1)),
					}
				})
			require.NoError(t, s.putJobModel(t, "cron", job))

			for _, step := range tc.steps {
				clock.advance(step, noSkip)
			}

			assert.Len(t, s.executions(t, "cron"), tc.wantExecs)
			assert.Len(t, s.rt.started(), tc.wantStarted)
		})
	}
}

func TestACA_ScheduleCronValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, cron string
		ok         bool
	}{
		{name: "valid-names", cron: "0 9 * JAN MON-FRI", ok: true},
		{name: "valid-step-range", cron: "0-30/10 */2 1,15 * *", ok: true},
		{name: "minute-out-of-range", cron: "60 * * * *"},
		{name: "bad-step", cron: "*/0 * * * *"},
		{name: "four-fields", cron: "* * * *"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACAStack(t)
			envID := *s.mustEnv(t, "rg", "env1").ID

			err := s.putJobModel(t, "cron", trigJob(envID, "img", armappcontainers.TriggerTypeSchedule, 0, 60,
				func(c *armappcontainers.JobConfiguration) {
					c.ScheduleTriggerConfig = &armappcontainers.JobConfigurationScheduleTriggerConfig{
						CronExpression: &tc.cron,
					}
				}))
			if tc.ok {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
		})
	}
}

func TestACA_EventTriggerQueues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		metadata  map[string]any
		sb        map[string]int
		q         map[string]int
		name      string
		scaler    string
		wantExecs int
		maxExecs  int32
	}{
		{
			name: "servicebus-ceil", scaler: "azure-servicebus", metadata: map[string]any{
				"queueName": "jobs", "messageCount": "5",
			},
			sb: map[string]int{"jobs": 12}, maxExecs: 10, wantExecs: 3,
		},
		{
			name: "servicebus-capped", scaler: "azure-servicebus", metadata: map[string]any{
				"queueName": "jobs", "messageCount": "1",
			},
			sb: map[string]int{"jobs": 12}, maxExecs: 2, wantExecs: 2,
		},
		{
			name: "storage-queue", scaler: "azure-queue",
			metadata: map[string]any{"queueName": "work", "queueLength": 2},
			q:        map[string]int{"work": 5}, maxExecs: 10, wantExecs: 3,
		},
		{
			name: "empty-queue", scaler: "azure-queue",
			metadata: map[string]any{"queueName": "work", "queueLength": 1},
			q:        map[string]int{}, maxExecs: 10, wantExecs: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACAStack(t)
			clock := &trigClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			s.apps.SetClock(clock)

			meters := &trigMeters{sb: tc.sb, q: tc.q}
			s.apps.SetQueueMeters(meters)

			envID := *s.mustEnv(t, "rg", "env1").ID

			require.NoError(
				t,
				s.putJobModel(t, "ev", trigJob(envID, "job-long", armappcontainers.TriggerTypeEvent, 0, 600,
					func(c *armappcontainers.JobConfiguration) {
						c.EventTriggerConfig = &armappcontainers.JobConfigurationEventTriggerConfig{
							Scale: &armappcontainers.JobScale{
								MaxExecutions: &tc.maxExecs, PollingInterval: new(int32(15)),
								Rules: []*armappcontainers.JobScaleRule{
									{Name: new("q"), Type: &tc.scaler, Metadata: tc.metadata},
								},
							},
						}
					})),
			)

			clock.advance(15*time.Second, noSkip)
			assert.Len(t, s.executions(t, "ev"), tc.wantExecs)

			clock.advance(15*time.Second, noSkip)
			assert.Len(t, s.executions(t, "ev"), tc.wantExecs, "running executions are not started again")
		})
	}
}

func TestACA_JobRetryTimeoutCompletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		want        armappcontainers.JobExecutionRunningState
		exits       []int
		advance     time.Duration
		retry       int32
		timeout     int32
		parallelism int32
		stopsRest   bool
	}{
		{
			name: "retry-then-succeed", exits: []int{1, 1, 0}, retry: 2, timeout: 600, parallelism: 1,
			want: armappcontainers.JobExecutionRunningStateSucceeded,
		},
		{
			name: "retries-exhausted", exits: []int{1, 1, 0}, retry: 1, timeout: 600, parallelism: 1,
			want: armappcontainers.JobExecutionRunningStateFailed,
		},
		{
			name: "timeout", exits: nil, retry: 0, timeout: 10, parallelism: 1, advance: 11 * time.Second,
			want: armappcontainers.JobExecutionRunningStateFailed, stopsRest: true,
		},
		{
			name: "completion-count", exits: []int{0}, retry: 0, timeout: 600, parallelism: 2,
			want: armappcontainers.JobExecutionRunningStateSucceeded, stopsRest: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACAStack(t)
			clock := &trigClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			s.apps.SetClock(clock)
			s.rt.scripts["job-x"] = tc.exits
			envID := *s.mustEnv(t, "rg", "env1").ID

			require.NoError(
				t,
				s.putJobModel(
					t,
					"jobx",
					trigJob(envID, "job-x", armappcontainers.TriggerTypeManual, tc.retry, tc.timeout,
						func(c *armappcontainers.JobConfiguration) {
							c.ManualTriggerConfig = &armappcontainers.JobConfigurationManualTriggerConfig{
								Parallelism: &tc.parallelism, ReplicaCompletionCount: new(int32(1)),
							}
						}),
				),
			)

			s.startJob(t, "jobx")

			require.Eventually(t, func() bool {
				clock.advance(tc.advance, noSkip)

				st := s.execStatuses(t, "jobx")

				return len(st) == 1 && st[0] == tc.want
			}, 10*time.Second, 5*time.Millisecond)

			if tc.stopsRest {
				require.Eventually(t, func() bool { return s.rt.live() <= 1 }, 10*time.Second, 5*time.Millisecond)
			}
		})
	}
}

func TestACA_ExecutionHistoryCap(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	clock := &trigClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	s.apps.SetClock(clock)
	s.rt.scripts["job-done"] = make([]int, 105)
	envID := *s.mustEnv(t, "rg", "env1").ID

	require.NoError(t, s.putJobModel(t, "hist", trigJob(envID, "job-done", armappcontainers.TriggerTypeSchedule, 0, 600,
		func(c *armappcontainers.JobConfiguration) {
			c.ScheduleTriggerConfig = &armappcontainers.JobConfigurationScheduleTriggerConfig{
				CronExpression: new("* * * * *"),
			}
		})))

	for range 103 {
		clock.advance(time.Minute, noSkip)
		require.Eventually(t, func() bool {
			clock.advance(0, noSkip)

			return !slices.Contains(s.execStatuses(t, "hist"), armappcontainers.JobExecutionRunningStateRunning)
		}, 10*time.Second, time.Millisecond)
	}

	assert.LessOrEqual(t, len(s.executions(t, "hist")), 101)
	assert.Greater(t, len(s.executions(t, "hist")), 95)
}

func TestACA_QueueScaleRules(t *testing.T) {
	t.Parallel()

	sbRule := func(q string) *armappcontainers.ScaleRule {
		return &armappcontainers.ScaleRule{
			Name: new("sb"), Custom: &armappcontainers.CustomScaleRule{
				Type: new(
					"azure-servicebus",
				), Metadata: map[string]*string{"queueName": new(q), "messageCount": new("5")},
			},
		}
	}
	stRule := &armappcontainers.ScaleRule{
		Name: new(
			"st",
		), AzureQueue: &armappcontainers.QueueScaleRule{QueueName: new("work"), QueueLength: new(int32(10))},
	}

	tests := []struct {
		rule *armappcontainers.ScaleRule
		sb   map[string]int
		q    map[string]int
		name string
		want []int
		max  int32
	}{
		{
			name: "servicebus-steps",
			rule: sbRule("jobs"),
			max:  20,
			sb:   map[string]int{"jobs": 100},
			want: []int{1, 4, 8, 16},
		},
		{name: "max-honoured", rule: sbRule("jobs"), max: 3, sb: map[string]int{"jobs": 100}, want: []int{1, 3, 3}},
		{name: "azure-queue", rule: stRule, max: 10, q: map[string]int{"work": 25}, want: []int{1, 3, 3}},
		{name: "empty-queue-stays", rule: sbRule("jobs"), max: 10, sb: map[string]int{}, want: []int{1, 1}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACAStack(t)
			clock := &trigClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			s.apps.SetClock(clock)

			meters := &trigMeters{sb: tc.sb, q: tc.q}
			s.apps.SetQueueMeters(meters)

			env := s.mustEnv(t, "rg", "env1")
			s.mustApp(t, "rg", "worker", *env.ID, acaApp{
				noIng: true,
				scale: &armappcontainers.Scale{
					MinReplicas: new(int32(0)), MaxReplicas: &tc.max, CooldownPeriod: new(int32(3600)),
					Rules: []*armappcontainers.ScaleRule{tc.rule},
				},
			})

			assert.Equal(t, tc.want[0], s.rt.live(), "a new revision starts one replica")

			for _, want := range tc.want[1:] {
				clock.advance(30*time.Second, 3600*time.Second)
				assert.Equal(t, want, s.rt.live())
			}
		})
	}
}
