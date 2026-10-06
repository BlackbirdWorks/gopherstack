package rds_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeEvents_StartEndTime covers the StartTime/EndTime interval
// (rds api_op_DescribeEvents.go:48-53,111-116).
func TestDescribeEvents_StartEndTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		start time.Duration
		end   time.Duration
		want  bool
	}{
		{name: "window_includes", start: -time.Hour, end: time.Hour, want: true},
		{name: "window_in_future", start: time.Hour, end: 2 * time.Hour},
		{name: "window_in_past", start: -2 * time.Hour, end: -time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestRDSClient(t, newTestRDSHandler(t))
			_, err := client.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
				DBInstanceIdentifier: aws.String("db-interval"),
				DBInstanceClass:      aws.String("db.t3.micro"),
				Engine:               aws.String("mysql"),
			})
			require.NoError(t, err)

			now := time.Now()
			out, err := client.DescribeEvents(t.Context(), &rdssdk.DescribeEventsInput{
				StartTime: aws.Time(now.Add(tt.start)),
				EndTime:   aws.Time(now.Add(tt.end)),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, len(out.Events) > 0)
		})
	}
}
