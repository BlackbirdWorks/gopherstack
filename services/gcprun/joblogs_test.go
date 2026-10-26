package gcprun_test

import (
	"testing"

	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ltype "google.golang.org/genproto/googleapis/logging/type"
)

func TestJobLogCapture(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tasks     int32
		lines     []fakeLine
		wantTexts []string
	}{
		{
			name: "one task", tasks: 1,
			lines:     []fakeLine{{text: "start"}, {text: "boom", stderr: true}},
			wantTexts: []string{"start", "boom"},
		},
		{name: "two tasks", tasks: 2, lines: []fakeLine{{text: "hi"}}, wantTexts: []string{"hi", "hi"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, ch := logEnv(t, tt.lines...)
			job := e.createJob(t, "etl", jobSpec("example/etl:1", tt.tasks, tt.tasks, 0))

			op, err := e.jobs.RunJob(t.Context(), &runpb.RunJobRequest{Name: job.GetName()})
			require.NoError(t, err)

			ex, err := op.Wait(t.Context())
			require.NoError(t, err)

			want := len(tt.wantTexts)
			seenTasks := map[string]bool{}

			for range want {
				got := nextEntry(t, ch)

				assert.Equal(t, "cloud_run_job", got.GetResource().GetType())
				assert.Equal(t, map[string]string{
					"project_id": "test-project", "job_name": "etl", "location": "us-central1",
				}, got.GetResource().GetLabels())
				assert.Equal(t, lastPart(ex.GetName()), got.GetLabels()["execution_name"])
				assert.Equal(t, "0", got.GetLabels()["task_attempt"])
				assert.NotEmpty(t, got.GetLabels()["instanceId"])
				assert.Contains(t, []string{
					"projects/test-project/logs/run.googleapis.com%2Fstdout",
					"projects/test-project/logs/run.googleapis.com%2Fstderr",
				}, got.GetLogName())

				if got.GetTextPayload() == "boom" {
					assert.Equal(t, ltype.LogSeverity_ERROR, got.GetSeverity())
				}

				seenTasks[got.GetLabels()["task_index"]] = true
			}

			assert.Len(t, seenTasks, int(tt.tasks))
			assert.Zero(t, e.rt.streams.Load(), "capture ends with the task")
		})
	}
}

func lastPart(s string) string {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return s[i+1:]
		}
	}

	return s
}
