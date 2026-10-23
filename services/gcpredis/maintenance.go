package gcpredis

import (
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/redis/apiv1/redispb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultMaintenanceLen = time.Hour
	scheduleDeadline      = 7 * 24 * time.Hour
	daysPerWeek           = 7
)

// RescheduleMaintenance sets when the next maintenance runs.
func (b *InMemoryBackend) RescheduleMaintenance(
	in *redispb.RescheduleMaintenanceRequest,
) (*longrunningpb.Operation, error) {
	now := b.now()

	b.mu.Lock("RescheduleMaintenance")
	rec, n, err := b.readyLocked(in.GetName())

	if err == nil {
		var sched *redispb.MaintenanceSchedule

		if sched, err = buildSchedule(in, rec.pb.GetMaintenancePolicy(), now); err == nil {
			rec.pb.MaintenanceSchedule = sched
		}
	}
	b.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return b.begin(n.parent(), "reschedule", n.full(), b.settle(n.full(), nil))
}

func buildSchedule(
	in *redispb.RescheduleMaintenanceRequest, mp *redispb.MaintenancePolicy, now time.Time,
) (*redispb.MaintenanceSchedule, error) {
	var (
		start time.Time
		span  = defaultMaintenanceLen
	)

	switch in.GetRescheduleType() {
	case redispb.RescheduleMaintenanceRequest_IMMEDIATE:
		start = now
	case redispb.RescheduleMaintenanceRequest_NEXT_AVAILABLE_WINDOW:
		start, span = nextWindow(mp, now)
	case redispb.RescheduleMaintenanceRequest_SPECIFIC_TIME:
		if in.GetScheduleTime() == nil {
			return nil, errInvalid("schedule_time is required for SPECIFIC_TIME.")
		}

		if start = in.GetScheduleTime().AsTime(); !start.After(now) {
			return nil, errInvalid("schedule_time must be in the future.")
		}
	default:
		return nil, errInvalid("reschedule_type is required.")
	}

	return &redispb.MaintenanceSchedule{
		StartTime:            timestamppb.New(start),
		EndTime:              timestamppb.New(start.Add(span)),
		ScheduleDeadlineTime: timestamppb.New(start.Add(scheduleDeadline)),
	}, nil
}

// nextWindow finds the next occurrence of the policy's weekly window, or the start of the next UTC day.
func nextWindow(mp *redispb.MaintenancePolicy, now time.Time) (time.Time, time.Duration) {
	windows := mp.GetWeeklyMaintenanceWindow()
	if len(windows) == 0 {
		return now.UTC().Truncate(hoursPerDay * time.Hour).Add(hoursPerDay * time.Hour), defaultMaintenanceLen
	}

	w := windows[0]
	span := defaultMaintenanceLen

	if d := w.GetDuration(); d != nil {
		span = d.AsDuration()
	}

	day := time.Weekday(int(w.GetDay()) % daysPerWeek)
	clock := time.Duration(
		w.GetStartTime().GetHours(),
	)*time.Hour + time.Duration(
		w.GetStartTime().GetMinutes(),
	)*time.Minute
	midnight := now.UTC().Truncate(hoursPerDay * time.Hour)

	for i := range daysPerWeek + 1 {
		cand := midnight.AddDate(0, 0, i).Add(clock)
		if cand.Weekday() == day && cand.After(now) {
			return cand, span
		}
	}

	return midnight.AddDate(0, 0, daysPerWeek).Add(clock), span
}
