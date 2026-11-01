package azurearm_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appcontainers/armappcontainers/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type acaJob struct {
	trigger     armappcontainers.TriggerType
	image       string
	cron        string
	parallelism int32
	timeout     int32
}

func (o acaJob) model(envID string) armappcontainers.Job {
	image := o.image
	if image == "" {
		image = "busybox:1"
	}

	trigger := o.trigger
	if trigger == "" {
		trigger = armappcontainers.TriggerTypeManual
	}

	timeout := o.timeout
	if timeout == 0 {
		timeout = 60
	}

	cfg := &armappcontainers.JobConfiguration{TriggerType: &trigger, ReplicaTimeout: &timeout}

	switch trigger {
	case armappcontainers.TriggerTypeSchedule:
		cfg.ScheduleTriggerConfig = &armappcontainers.JobConfigurationScheduleTriggerConfig{CronExpression: &o.cron}
	case armappcontainers.TriggerTypeEvent:
		cfg.EventTriggerConfig = &armappcontainers.JobConfigurationEventTriggerConfig{}
	case armappcontainers.TriggerTypeManual:
		cfg.ManualTriggerConfig = &armappcontainers.JobConfigurationManualTriggerConfig{}
		if o.parallelism > 0 {
			cfg.ManualTriggerConfig.Parallelism = &o.parallelism
		}
	}

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

func (s *acaStack) putJob(t *testing.T, name, envID string, o acaJob) (*armappcontainers.Job, error) {
	t.Helper()

	poller, err := s.factory.NewJobsClient().BeginCreateOrUpdate(t.Context(), "rg", name, o.model(envID), nil)
	if err != nil {
		return nil, err
	}

	res, err := poller.PollUntilDone(t.Context(), pollFast())
	if err != nil {
		return nil, err
	}

	return &res.Job, nil
}

func (s *acaStack) executions(t *testing.T, job string) []*armappcontainers.JobExecution {
	t.Helper()

	var out []*armappcontainers.JobExecution

	pager := s.factory.NewJobsExecutionsClient().NewListPager("rg", job, nil)
	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		out = append(out, page.Value...)
	}

	return out
}

func (s *acaStack) startJob(t *testing.T, rg, job string) string {
	t.Helper()

	poller, err := s.factory.NewJobsClient().BeginStart(t.Context(), rg, job, nil)
	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	return *res.Name
}

func TestACA_JobLifecycleAndExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		image  string
		status armappcontainers.JobExecutionRunningState
		exit   []int
	}{
		{name: "succeeds", image: "job-ok", exit: []int{0}, status: armappcontainers.JobExecutionRunningStateSucceeded},
		{name: "fails", image: "job-bad", exit: []int{3}, status: armappcontainers.JobExecutionRunningStateFailed},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACAStack(t)
			s.rt.scripts[tc.image] = tc.exit
			envID := *s.mustEnv(t, "rg", "env1").ID

			job, err := s.putJob(t, "batch", envID, acaJob{image: tc.image})
			require.NoError(t, err)
			assert.Equal(t, armappcontainers.JobProvisioningStateSucceeded, *job.Properties.ProvisioningState)
			assert.Equal(t, armappcontainers.TriggerTypeManual, *job.Properties.Configuration.TriggerType)

			exec := s.startJob(t, "rg", "batch")
			assert.Regexp(t, `^batch-[0-9a-f]{7}$`, exec)

			require.Eventually(t, func() bool {
				list := s.executions(t, "batch")

				return len(list) == 1 && *list[0].Properties.Status == tc.status
			}, 10*time.Second, 5*time.Millisecond)

			list := s.executions(t, "batch")
			assert.Equal(t, exec, *list[0].Name)
			assert.NotNil(t, list[0].Properties.StartTime)
			assert.NotNil(t, list[0].Properties.EndTime)
			assert.Equal(t, tc.image, *list[0].Properties.Template.Containers[0].Image)

			var env []string

			for _, sp := range s.rt.started() {
				env = sp.Env
			}

			assert.Contains(t, env, "CONTAINER_APP_JOB_NAME=batch")
			assert.Contains(t, env, "CONTAINER_APP_JOB_EXECUTION_NAME="+exec)
		})
	}
}

func TestACA_JobStopAndOverride(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	envID := *s.mustEnv(t, "rg", "env1").ID

	_, err := s.putJob(t, "long", envID, acaJob{image: "job-long"})
	require.NoError(t, err)

	poller, err := s.factory.NewJobsClient().
		BeginStart(t.Context(), "rg", "long", &armappcontainers.JobsClientBeginStartOptions{
			Template: &armappcontainers.JobExecutionTemplate{Containers: []*armappcontainers.JobExecutionContainer{
				{Name: new("work"), Image: new("job-override")},
			}},
		})
	require.NoError(t, err)

	started, err := poller.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	list := s.executions(t, "long")
	require.Len(t, list, 1)
	assert.Equal(t, armappcontainers.JobExecutionRunningStateRunning, *list[0].Properties.Status)
	assert.Equal(t, "job-override", *list[0].Properties.Template.Containers[0].Image)
	assert.Equal(t, 1, s.rt.live())

	stop, err := s.factory.NewJobsClient().BeginStopExecution(t.Context(), "rg", "long", *started.Name, nil)
	require.NoError(t, err)

	_, err = stop.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	list = s.executions(t, "long")
	assert.Equal(t, armappcontainers.JobExecutionRunningStateStopped, *list[0].Properties.Status)
	assert.Equal(t, 0, s.rt.live())

	bad, err := s.factory.NewJobsClient().
		BeginStart(t.Context(), "rg", "long", &armappcontainers.JobsClientBeginStartOptions{
			Template: &armappcontainers.JobExecutionTemplate{Containers: []*armappcontainers.JobExecutionContainer{
				{Name: new("nonexistent"), Image: new("x")},
			}},
		})
	if err == nil {
		_, err = bad.PollUntilDone(t.Context(), pollFast())
	}

	_, status := respCode(t, err)
	assert.Equal(t, http.StatusBadRequest, status)

	_, err = s.factory.NewJobsClient().BeginStart(t.Context(), "rg", "ghost", nil)
	_, status = respCode(t, err)
	assert.Equal(t, http.StatusNotFound, status)
}

func TestACA_JobParallelismRunsReplicas(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	envID := *s.mustEnv(t, "rg", "env1").ID

	_, err := s.putJob(t, "fan", envID, acaJob{image: "job-fan", parallelism: 3})
	require.NoError(t, err)

	s.startJob(t, "rg", "fan")
	assert.Equal(t, 3, s.rt.live())

	stop, err := s.factory.NewJobsClient().BeginStopMultipleExecutions(t.Context(), "rg", "fan", nil)
	require.NoError(t, err)

	res, err := stop.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Len(t, res.Value, 1)
	assert.Equal(t, 0, s.rt.live())
}

func TestACA_JobValidationAndCascade(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts acaJob
	}{
		{name: "schedule-without-cron", opts: acaJob{trigger: armappcontainers.TriggerTypeSchedule}},
		{
			name: "schedule-with-six-fields",
			opts: acaJob{trigger: armappcontainers.TriggerTypeSchedule, cron: "* * * * * *"},
		},
		{name: "bad-trigger", opts: acaJob{trigger: "Whenever"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACAStack(t)
			envID := *s.mustEnv(t, "rg", "env1").ID

			_, err := s.putJob(t, "bad-job", envID, tc.opts)
			code, status := respCode(t, err)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidRequestParameterWithDetails", code)
		})
	}

	t.Run("schedule-and-event-store-config", func(t *testing.T) {
		t.Parallel()

		s := newACAStack(t)
		envID := *s.mustEnv(t, "rg", "env1").ID

		sched, err := s.putJob(
			t,
			"cron",
			envID,
			acaJob{trigger: armappcontainers.TriggerTypeSchedule, cron: "*/5 * * * *"},
		)
		require.NoError(t, err)
		assert.Equal(t, "*/5 * * * *", *sched.Properties.Configuration.ScheduleTriggerConfig.CronExpression)

		_, err = s.putJob(t, "evt", envID, acaJob{trigger: armappcontainers.TriggerTypeEvent})
		require.NoError(t, err)

		s.rt.scripts["job-run"] = nil

		_, err = s.putJob(t, "gone", envID, acaJob{image: "job-run"})
		require.NoError(t, err)
		s.startJob(t, "rg", "gone")
		assert.Equal(t, 1, s.rt.live())

		status, _ := s.raw(t, http.MethodPut, "", "/subscriptions/"+aciSub+"/resourcegroups/rg?api-version=2021-04-01")
		require.Less(t, status, 300)

		status, _ = s.raw(
			t,
			http.MethodDelete,
			"",
			"/subscriptions/"+aciSub+"/resourcegroups/rg?api-version=2021-04-01",
		)
		require.Less(t, status, 300)

		_, err = s.factory.NewJobsClient().Get(t.Context(), "rg", "gone", nil)
		_, code := respCode(t, err)
		assert.Equal(t, http.StatusNotFound, code)
		assert.Equal(t, 0, s.rt.live())
	})
}
