package glue_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestJobRun_SucceededReportsExecutionTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		wait time.Duration
		want int
	}{
		{name: "sub second", wait: time.Second, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := glue.NewInMemoryBackend("000000000000", "us-east-1")
				t.Cleanup(b.Close)

				_, err := b.CreateJob(glue.Job{
					Name: "j", Role: "arn:aws:iam::000000000000:role/glue", Command: glue.JobCommand{Name: "glueetl"},
				})
				require.NoError(t, err)

				run, err := b.StartJobRun("j", nil)
				require.NoError(t, err)

				time.Sleep(tt.wait)

				got, err := b.GetJobRun("j", run.ID)
				require.NoError(t, err)
				require.Equal(t, "SUCCEEDED", got.JobRunState)
				assert.GreaterOrEqual(t, got.ExecutionTime, tt.want)
			})
		})
	}
}
