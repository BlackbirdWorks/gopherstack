package gcpscheduler_test

import (
	"testing"
	"time"

	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpscheduler"
)

func TestAppEngineCronNext(t *testing.T) {
	t.Parallel()

	utc := func(m time.Month, d, h, min int) time.Time { return time.Date(2030, m, d, h, min, 0, 0, time.UTC) }
	base := utc(time.January, 15, 10, 7) // a Tuesday

	tests := []struct {
		from time.Time
		want time.Time
		name string
		expr string
		zone string
	}{
		{base, utc(1, 15, 10, 10), "every 5 minutes", "every 5 minutes", "UTC"},
		{base, utc(1, 15, 10, 30), "every 30 mins", "every 30 mins", "UTC"},
		{utc(1, 15, 13, 58), utc(1, 15, 14, 0), "from to is inclusive", "every 5 minutes from 10:00 to 14:00", "UTC"},
		{utc(1, 15, 14, 0), utc(1, 16, 10, 0), "from to wraps to next day", "every 5 minutes from 10:00 to 14:00", "UTC"},
		{base, utc(1, 15, 12, 0), "every 2 hours synchronized", "every 2 hours synchronized", "UTC"},
		{utc(1, 15, 16, 30), utc(1, 16, 8, 0), "hours from to", "every 1 hours from 08:00 to 16:00", "UTC"},
		{base, utc(1, 16, 0, 0), "every day", "every day 00:00", "UTC"},
		{base, utc(1, 21, 9, 0), "every monday", "every monday 09:00", "UTC"},
		{base, utc(2, 5, 4, 0), "1st and 3rd tuesday", "1st,3rd tuesday of month 04:00", "UTC"},
		{base, utc(1, 16, 9, 0), "ordinal words", "2nd,third wednesday of month 09:00", "UTC"},
		{base, utc(2, 11, 0, 0), "ordinal with weekday list", "2nd monday,thu", "UTC"},
		{base, utc(3, 13, 17, 0), "ordinal of one month", "2nd wednesday of march 17:00", "UTC"},
		{base, utc(1, 22, 9, 0), "month days", "1,8,15,22 of month 09:00", "UTC"},
		{base, utc(9, 2, 9, 0), "ordinal of month list", "1st monday of sep,oct,nov 09:00", "UTC"},
		{base, utc(4, 1, 0, 0), "month day of month list", "1 of jan,april,july,oct 00:00", "UTC"},
		{base, utc(1, 15, 14, 0), "time zone", "every day 09:00", "America/New_York"},
		{base, utc(1, 15, 10, 8), "unix-cron still works", "* * * * *", "UTC"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := gcpscheduler.CronNext(tt.expr, tt.zone, tt.from)
			require.NoError(t, err)
			assert.True(t, tt.want.Equal(got), "want %s got %s", tt.want, got)
		})
	}
}

func TestAppEngineCronInvalid(t *testing.T) {
	t.Parallel()

	tests := []struct{ name, expr string }{
		{"mixed interval and days", "every 6 hours mon,wed,fri"},
		{"zero interval", "every 0 minutes"},
		{"interval without every", "5 minutes"},
		{"hour out of range", "every 5 minutes from 25:00 to 26:00"},
		{"to before from", "every 5 minutes from 14:00 to 10:00"},
		{"from without to", "every 5 minutes from 10:00"},
		{"unknown day", "every funday 09:00"},
		{"daily without every", "monday 09:00"},
		{"month days without of", "1,8 09:00"},
		{"unknown month", "2nd monday of foo"},
		{"bad time", "every monday 9am"},
		{"trailing words", "every monday 09:00 please"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := gcpscheduler.CronNext(tt.expr, "UTC", time.Now())
			require.Error(t, err)
		})
	}
}

func TestCreateJobAcceptsAppEngineCron(t *testing.T) {
	t.Parallel()

	b := gcpscheduler.NewInMemoryBackend()
	now := time.Date(2030, 1, 15, 10, 7, 30, 0, time.UTC)
	b.SetClock(func() time.Time { return now })

	out, err := b.CreateJob(&schedulerpb.CreateJobRequest{Parent: location, Job: &schedulerpb.Job{
		Name: location + "/jobs/ae", Schedule: "every monday 09:00", TimeZone: "UTC",
		Target: &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{Uri: "https://target.test/x"}},
	}})
	require.NoError(t, err)
	assert.True(t, time.Date(2030, 1, 21, 9, 0, 0, 0, time.UTC).Equal(out.GetScheduleTime().AsTime()))

	_, err = b.CreateJob(&schedulerpb.CreateJobRequest{Parent: location, Job: &schedulerpb.Job{
		Name: location + "/jobs/bad", Schedule: "every 6 hours mon,wed,fri",
		Target: &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{Uri: "https://target.test/x"}},
	}})
	require.Error(t, err)
}
