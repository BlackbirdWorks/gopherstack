package xray

import (
	"context"
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type samplingStatisticSummaryView struct {
	RuleName     string  `json:"RuleName"`
	RequestCount int32   `json:"RequestCount"`
	SampledCount int32   `json:"SampledCount"`
	BorrowCount  int32   `json:"BorrowCount"`
	Timestamp    float64 `json:"Timestamp"`
}

type getSamplingStatisticSummariesInput struct {
	NextToken string `json:"NextToken"`
}

func (h *Handler) handleGetSamplingStatisticSummaries(_ context.Context, body []byte) ([]byte, error) {
	var in getSamplingStatisticSummariesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	summaries := h.Backend.GetSamplingStatisticSummaries()
	views := make([]samplingStatisticSummaryView, 0, len(summaries))

	for _, s := range summaries {
		views = append(views, samplingStatisticSummaryView{
			RuleName:     s.RuleName,
			RequestCount: s.RequestCount,
			SampledCount: s.SampledCount,
			BorrowCount:  s.BorrowCount,
			Timestamp:    float64(s.Timestamp.Unix()),
		})
	}

	pg := page.New(views, in.NextToken, 0, defaultSamplingStatsPageSize)

	return json.Marshal(map[string]any{
		"SamplingStatisticSummaries": pg.Data,
		keyNextToken:                 pg.Next,
	})
}

type samplingStatisticsDocumentInput struct {
	RuleName     string `json:"RuleName"`
	ClientID     string `json:"ClientId"`
	RequestCount int32  `json:"RequestCount"`
	SampledCount int32  `json:"SampledCount"`
	BorrowCount  int32  `json:"BorrowCount"`
}

type getSamplingTargetsInput struct {
	SamplingStatisticsDocuments []samplingStatisticsDocumentInput `json:"SamplingStatisticsDocuments"`
}

type samplingTargetDocumentView struct {
	RuleName          string  `json:"RuleName"`
	ReservoirQuotaTTL float64 `json:"ReservoirQuotaTTL"`
	FixedRate         float64 `json:"FixedRate"`
	ReservoirQuota    int32   `json:"ReservoirQuota"`
	Interval          int32   `json:"Interval"`
}

type unprocessedStatisticsView struct {
	RuleName  string `json:"RuleName"`
	ErrorCode string `json:"ErrorCode"`
	Message   string `json:"Message"`
}

func (h *Handler) handleGetSamplingTargets(_ context.Context, body []byte) ([]byte, error) {
	var in getSamplingTargetsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	docs := make([]SamplingStatisticsDocument, 0, len(in.SamplingStatisticsDocuments))
	for _, d := range in.SamplingStatisticsDocuments {
		docs = append(docs, SamplingStatisticsDocument(d))
	}

	targets, unprocessed := h.Backend.GetSamplingTargets(docs)

	targetViews := make([]samplingTargetDocumentView, 0, len(targets))
	for _, t := range targets {
		targetViews = append(targetViews, samplingTargetDocumentView{
			RuleName:          t.RuleName,
			FixedRate:         t.FixedRate,
			ReservoirQuota:    t.ReservoirSize,
			ReservoirQuotaTTL: float64(t.ReservoirQuotaTTL.Unix()),
			Interval:          samplingTargetInterval,
		})
	}

	unprocessedViews := make([]unprocessedStatisticsView, 0, len(unprocessed))
	for _, u := range unprocessed {
		unprocessedViews = append(unprocessedViews, unprocessedStatisticsView(u))
	}

	lastMod := h.Backend.LastRuleModification()
	var lastModTS float64

	if !lastMod.IsZero() {
		lastModTS = float64(lastMod.Unix())
	} else {
		lastModTS = float64(time.Now().Unix())
	}

	return json.Marshal(map[string]any{
		"SamplingTargetDocuments": targetViews,
		"UnprocessedStatistics":   unprocessedViews,
		"LastRuleModification":    lastModTS,
	})
}
