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

// TransformedInsightEvents runs each event through its log group's transformer; groups without one pass unchanged.
func (s cwInsightLogSource) TransformedInsightEvents(
	region string, patterns []string, start, end time.Time,
) []cwbackend.InsightLogEvent {
	evs := s.logs.ContributorEvents(region, patterns, start.UnixMilli(), end.UnixMilli())
	out := make([]cwbackend.InsightLogEvent, len(evs))
	byGroup := make(map[string][]int)

	for i, ev := range evs {
		out[i] = cwbackend.InsightLogEvent{Timestamp: time.UnixMilli(ev.Timestamp).UTC(), Message: ev.Message}
		byGroup[ev.LogGroup] = append(byGroup[ev.LogGroup], i)
	}

	for group, idx := range byGroup {
		processors, ok := s.logs.TransformerProcessors(group)
		if !ok {
			continue
		}

		messages := make([]string, len(idx))
		for j, i := range idx {
			messages[j] = out[i].Message
		}

		for j, res := range cwlogsbackend.ApplyTransformer(messages, processors) {
			out[idx[j]].Message = res.TransformedEventMessage
		}
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
