package main

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// cwInsightLogSource feeds CloudWatch Logs events to Contributor Insights rules.
type cwInsightLogSource struct {
	logs *cwlogsbackend.InMemoryBackend
}

func (s cwInsightLogSource) InsightEvents(
	region string, patterns []string, start, end time.Time,
) []cwbackend.InsightLogEvent {
	evs := s.logs.ContributorEvents(region, patterns, start.UnixMilli(), end.UnixMilli())
	out := make([]cwbackend.InsightLogEvent, len(evs))

	for i, ev := range evs {
		out[i] = cwbackend.InsightLogEvent{Timestamp: time.UnixMilli(ev.Timestamp).UTC(), Message: ev.Message}
	}

	return out
}

// wireCWInsightRuleLogs lets CloudWatch Contributor Insights rules read CloudWatch Logs events.
func wireCWInsightRuleLogs(cwlogsReg, cwReg service.Registerable) {
	logsH, ok := cwlogsReg.(*cwlogsbackend.Handler)
	if !ok {
		return
	}

	logsBk, bkOk := logsH.Backend.(*cwlogsbackend.InMemoryBackend)
	cwH, cwOk := cwReg.(*cwbackend.Handler)

	if bkOk && cwOk {
		cwH.SetLogEventSource(cwInsightLogSource{logs: logsBk})
	}
}
