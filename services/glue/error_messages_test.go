package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPIErrors_CodeAndMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(c *gluesdk.Client) error
		name     string
		wantCode string
		wantMsg  string
	}{
		{
			name: "database_missing",
			call: func(c *gluesdk.Client) error {
				_, err := c.GetDatabase(t.Context(), &gluesdk.GetDatabaseInput{Name: aws.String("nope")})

				return err
			},
			wantCode: "EntityNotFoundException", wantMsg: "Entity Not Found",
		},
		{
			name: "database_duplicate",
			call: func(c *gluesdk.Client) error {
				for range 2 {
					_, err := c.CreateDatabase(t.Context(), &gluesdk.CreateDatabaseInput{
						DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String("d")},
					})
					if err != nil {
						return err
					}
				}

				return nil
			},
			wantCode: "AlreadyExistsException", wantMsg: "Database already exists.",
		},
		{
			name: "worker_type_prefix_stripped",
			call: func(c *gluesdk.Client) error {
				_, err := c.CreateJob(t.Context(), &gluesdk.CreateJobInput{
					Name: aws.String("j"),
					Role: aws.String("r"),
					Command: &gluetypes.JobCommand{
						Name:           aws.String("glueetl"),
						ScriptLocation: aws.String("s3://b/s.py"),
					},
					WorkerType:      "Z.9X",
					NumberOfWorkers: aws.Int32(2),
				})

				return err
			},
			wantCode: "InvalidInputException", wantMsg: `invalid WorkerType "Z.9X"`,
		},
		{
			name: "trigger_schedule_missing",
			call: func(c *gluesdk.Client) error {
				_, err := c.CreateTrigger(t.Context(), &gluesdk.CreateTriggerInput{
					Name: aws.String("t"), Type: gluetypes.TriggerTypeScheduled,
					Actions: []gluetypes.Action{{JobName: aws.String("j")}},
				})

				return err
			},
			wantCode: "InvalidInputException", wantMsg: "Schedule must be a cron expression",
		},
		{
			name: "trigger_schedule_malformed",
			call: func(c *gluesdk.Client) error {
				_, err := c.CreateTrigger(t.Context(), &gluesdk.CreateTriggerInput{
					Name: aws.String("t"), Type: gluetypes.TriggerTypeScheduled, Schedule: aws.String("bad"),
					Actions: []gluetypes.Action{{JobName: aws.String("j")}},
				})

				return err
			},
			wantCode: "InvalidInputException", wantMsg: "Schedule must be a cron expression",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call(newTestGlueClient(t, newTestHandler(t)))
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.Contains(t, apiErr.ErrorMessage(), tt.wantMsg)
		})
	}
}
