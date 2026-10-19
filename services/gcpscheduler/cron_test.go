package gcpscheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpscheduler"
)

func TestCronNext(t *testing.T) {
	t.Parallel()

	base := time.Date(2030, 1, 15, 10, 7, 30, 0, time.UTC) // a Tuesday

	tests := []struct {
		from time.Time
		want time.Time
		name string
		expr string
		zone string
	}{
		{base, time.Date(2030, 1, 15, 10, 8, 0, 0, time.UTC), "every minute", "* * * * *", "UTC"},
		{
			time.Date(2030, 1, 15, 10, 7, 0, 0, time.UTC),
			time.Date(2030, 1, 16, 10, 7, 0, 0, time.UTC),
			"strictly after",
			"7 10 * * *",
			"UTC",
		},
		{base, time.Date(2030, 1, 15, 10, 15, 0, 0, time.UTC), "step", "*/15 * * * *", "UTC"},
		{base, time.Date(2030, 1, 15, 11, 0, 0, 0, time.UTC), "range with step", "0-30/10 11 * * *", "UTC"},
		{base, time.Date(2030, 1, 15, 10, 20, 0, 0, time.UTC), "list", "5,20,40 10 * * *", "UTC"},
		{base, time.Date(2030, 1, 16, 9, 0, 0, 0, time.UTC), "daily next day", "0 9 * * *", "UTC"},
		{base, time.Date(2030, 1, 21, 9, 0, 0, 0, time.UTC), "weekday number", "0 9 * * 1", "UTC"},
		{base, time.Date(2030, 1, 16, 9, 0, 0, 0, time.UTC), "weekday name range", "0 9 * * mon-fri", "UTC"},
		{base, time.Date(2030, 1, 20, 9, 0, 0, 0, time.UTC), "sunday as 7", "0 9 * * 7", "UTC"},
		{base, time.Date(2030, 3, 1, 0, 0, 0, 0, time.UTC), "month name", "0 0 1 mar *", "UTC"},
		{base, time.Date(2030, 1, 20, 0, 0, 0, 0, time.UTC), "dom or dow when both set", "0 0 20 * mon", "UTC"},
		{base, time.Date(2032, 2, 29, 0, 0, 0, 0, time.UTC), "leap day", "0 0 29 2 *", "UTC"},
		{base, time.Date(2030, 1, 15, 11, 0, 0, 0, time.UTC), "macro hourly", "@hourly", "UTC"},
		{base, time.Date(2030, 1, 16, 0, 0, 0, 0, time.UTC), "macro daily", "@daily", "UTC"},
		{base, time.Date(2030, 1, 20, 0, 0, 0, 0, time.UTC), "macro weekly", "@weekly", "UTC"},
		{base, time.Date(2030, 1, 15, 14, 0, 0, 0, time.UTC), "zone offset", "0 9 * * *", "America/New_York"},
		{base, time.Date(2030, 1, 16, 0, 0, 0, 0, time.UTC), "zone ahead of utc", "0 9 * * *", "Asia/Tokyo"},
		{
			time.Date(2030, 3, 9, 15, 0, 0, 0, time.UTC),
			time.Date(2030, 3, 10, 13, 0, 0, 0, time.UTC),
			"dst spring forward",
			"0 9 * * *",
			"America/New_York",
		},
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

func TestCronInvalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
	}{
		{"empty", ""},
		{"too few fields", "* * * *"},
		{"too many fields", "* * * * * *"},
		{"minute out of range", "60 * * * *"},
		{"hour out of range", "0 24 * * *"},
		{"dom zero", "0 0 0 * *"},
		{"month 13", "0 0 1 13 *"},
		{"dow 8", "0 0 * * 8"},
		{"reversed range", "30-10 * * * *"},
		{"zero step", "*/0 * * * *"},
		{"junk step", "*/x * * * *"},
		{"junk value", "a * * * *"},
		{"app engine syntax", "every 5 minutes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := gcpscheduler.CronNext(tt.expr, "UTC", time.Now())
			require.Error(t, err)
		})
	}
}

func TestCronNeverFires(t *testing.T) {
	t.Parallel()

	got, err := gcpscheduler.CronNext("0 0 31 2 *", "UTC", time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	assert.True(t, got.IsZero())
}

func TestCronDSTGapFiresOncePerDay(t *testing.T) {
	t.Parallel()

	first, err := gcpscheduler.CronNext("30 2 * * *", "America/New_York", time.Date(2030, 3, 10, 5, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	second, err := gcpscheduler.CronNext("30 2 * * *", "America/New_York", first)
	require.NoError(t, err)

	assert.True(t, time.Date(2030, 3, 11, 6, 30, 0, 0, time.UTC).Equal(second), "got %s", second)
}
