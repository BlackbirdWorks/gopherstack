package gcpmonitoring

import (
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/genproto/googleapis/type/calendarperiod"
)

func TestSLOPeriodStart(t *testing.T) {
	t.Parallel()

	day := func(y int, m time.Month, d, h int) time.Time { return time.Date(y, m, d, h, 0, 0, 0, time.UTC) }

	tests := []struct {
		now    time.Time
		want   time.Time
		name   string
		period calendarperiod.CalendarPeriod
	}{
		{day(2026, 11, 11, 15), day(2026, 11, 9, 0), "week starts monday", calendarperiod.CalendarPeriod_WEEK},
		{
			day(2026, 1, 2, 9), day(2025, 12, 29, 0), "fortnight week 1 starts iso week 1",
			calendarperiod.CalendarPeriod_FORTNIGHT,
		},
		{
			day(2026, 1, 8, 9), day(2025, 12, 29, 0), "fortnight week 2 of the first",
			calendarperiod.CalendarPeriod_FORTNIGHT,
		},
		{day(2026, 1, 14, 9), day(2026, 1, 12, 0), "fortnight third iso week", calendarperiod.CalendarPeriod_FORTNIGHT},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			slo := &monitoringpb.ServiceLevelObjective{
				Period: &monitoringpb.ServiceLevelObjective_CalendarPeriod{CalendarPeriod: tt.period},
			}

			assert.Equal(t, tt.want, sloPeriodStart(slo, tt.now))
		})
	}
}
