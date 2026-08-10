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

type samplingBoostStatisticsDocumentInput struct {
	RuleName            string `json:"RuleName"`
	ServiceName         string `json:"ServiceName"`
	TotalCount          int32  `json:"TotalCount"`
	AnomalyCount        int32  `json:"AnomalyCount"`
	SampledAnomalyCount int32  `json:"SampledAnomalyCount"`
}

type getSamplingTargetsInput struct {
	SamplingStatisticsDocuments      []samplingStatisticsDocumentInput      `json:"SamplingStatisticsDocuments"`
	SamplingBoostStatisticsDocuments []samplingBoostStatisticsDocumentInput `json:"SamplingBoostStatisticsDocuments"`
}

// samplingBoostView is the wire shape for SamplingTargetDocument.SamplingBoost.
// It is declared but never populated in samplingTargetDocumentView: AWS's
// boost-trigger algorithm is unpublished, so gopherstack does not compute a
// boost rate (see GetSamplingTargets in sampling_statistics.go). A boost
// document for a known rule is accepted (not reported in
// UnprocessedBoostStatistics) but produces no observable SamplingBoost --
// an honest "accepted, no engine behind it" gap, not a fabricated value.
type samplingBoostView struct {
	BoostRate    float64 `json:"BoostRate"`
	BoostRateTTL float64 `json:"BoostRateTTL"`
}

type samplingTargetDocumentView struct {
	SamplingBoost     *samplingBoostView `json:"SamplingBoost,omitempty"` // always nil, see samplingBoostView doc
	RuleName          string             `json:"RuleName"`
	ReservoirQuotaTTL float64            `json:"ReservoirQuotaTTL"`
	FixedRate         float64            `json:"FixedRate"`
	ReservoirQuota    int32              `json:"ReservoirQuota"`
	Interval          int32              `json:"Interval"`
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

	boostDocs := make([]SamplingBoostStatisticsDocument, 0, len(in.SamplingBoostStatisticsDocuments))
	for _, d := range in.SamplingBoostStatisticsDocuments {
		boostDocs = append(boostDocs, SamplingBoostStatisticsDocument(d))
	}

	targets, unprocessed, unprocessedBoost := h.Backend.GetSamplingTargets(docs, boostDocs)

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

	unprocessedBoostViews := make([]unprocessedStatisticsView, 0, len(unprocessedBoost))
	for _, u := range unprocessedBoost {
		unprocessedBoostViews = append(unprocessedBoostViews, unprocessedStatisticsView(u))
	}

	lastMod := h.Backend.LastRuleModification()
	var lastModTS float64

	if !lastMod.IsZero() {
		lastModTS = float64(lastMod.Unix())
	} else {
		lastModTS = float64(time.Now().Unix())
	}

	return json.Marshal(map[string]any{
		"SamplingTargetDocuments":    targetViews,
		"UnprocessedStatistics":      unprocessedViews,
		"UnprocessedBoostStatistics": unprocessedBoostViews,
		"LastRuleModification":       lastModTS,
	})
}
