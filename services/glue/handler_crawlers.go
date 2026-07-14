package glue

import (
	"context"
	"fmt"
)

type createCrawlerInput struct {
	Tags          map[string]string `json:"Tags,omitempty"`
	Name          string            `json:"Name"`
	Role          string            `json:"Role"`
	DatabaseName  string            `json:"DatabaseName"`
	Description   string            `json:"Description,omitempty"`
	Schedule      string            `json:"Schedule,omitempty"`
	Configuration string            `json:"Configuration,omitempty"`
	TablePrefix   string            `json:"TablePrefix,omitempty"`
	Classifiers   []string          `json:"Classifiers,omitempty"`
	Targets       CrawlerTarget     `json:"Targets,omitzero"`
}

func (h *Handler) handleCreateCrawler(_ context.Context, in *createCrawlerInput) (*emptyOutput, error) {
	_, err := h.Backend.CreateCrawlerWithOptions(in.Name, in.Role, in.DatabaseName, in.Targets, in.Tags, CrawlerOptions{
		Description:   in.Description,
		Schedule:      in.Schedule,
		Configuration: in.Configuration,
		TablePrefix:   in.TablePrefix,
		Classifiers:   in.Classifiers,
	})
	if err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type getCrawlerInput struct {
	Name string `json:"Name"`
}

type getCrawlerOutput struct {
	Crawler *Crawler `json:"Crawler"`
}

func (h *Handler) handleGetCrawler(_ context.Context, in *getCrawlerInput) (*getCrawlerOutput, error) {
	c, err := h.Backend.GetCrawler(in.Name)
	if err != nil {
		return nil, err
	}

	return &getCrawlerOutput{Crawler: c}, nil
}

type getCrawlersInput struct{}

type getCrawlersOutput struct {
	Crawlers []*Crawler `json:"Crawlers"`
}

func (h *Handler) handleGetCrawlers(_ context.Context, _ *getCrawlersInput) (*getCrawlersOutput, error) {
	crawlers := h.Backend.GetCrawlers()

	return &getCrawlersOutput{Crawlers: crawlers}, nil
}

type updateCrawlerInput struct {
	Name          string        `json:"Name"`
	Role          string        `json:"Role"`
	DatabaseName  string        `json:"DatabaseName"`
	Description   string        `json:"Description,omitempty"`
	Schedule      string        `json:"Schedule,omitempty"`
	Configuration string        `json:"Configuration,omitempty"`
	TablePrefix   string        `json:"TablePrefix,omitempty"`
	Classifiers   []string      `json:"Classifiers,omitempty"`
	Targets       CrawlerTarget `json:"Targets,omitzero"`
}

func (h *Handler) handleUpdateCrawler(_ context.Context, in *updateCrawlerInput) (*emptyOutput, error) {
	err := h.Backend.UpdateCrawlerWithOptions(in.Name, in.Role, in.DatabaseName, in.Targets, CrawlerOptions{
		Description:   in.Description,
		Schedule:      in.Schedule,
		Configuration: in.Configuration,
		TablePrefix:   in.TablePrefix,
		Classifiers:   in.Classifiers,
	})
	if err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type deleteCrawlerInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteCrawler(_ context.Context, in *deleteCrawlerInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteCrawler(in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type batchGetCrawlersInput struct {
	CrawlerNames []string `json:"CrawlerNames"`
}

type batchGetCrawlersOutput struct {
	Crawlers         []*Crawler `json:"Crawlers"`
	CrawlersNotFound []string   `json:"CrawlersNotFound"`
}

func (h *Handler) handleBatchGetCrawlers(
	_ context.Context,
	in *batchGetCrawlersInput,
) (*batchGetCrawlersOutput, error) {
	found, missing := h.Backend.BatchGetCrawlers(in.CrawlerNames)

	return &batchGetCrawlersOutput{Crawlers: found, CrawlersNotFound: missing}, nil
}

type listCrawlersInput struct{}

type listCrawlersOutput struct {
	CrawlerNames []string `json:"CrawlerNames"`
}

func (h *Handler) handleListCrawlers(
	_ context.Context,
	_ *listCrawlersInput,
) (*listCrawlersOutput, error) {
	names := h.Backend.ListCrawlers()

	return &listCrawlersOutput{CrawlerNames: names}, nil
}

type startCrawlerInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleStartCrawler(_ context.Context, in *startCrawlerInput) (*emptyOutput, error) {
	if err := h.Backend.StartCrawler(in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type stopCrawlerInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleStopCrawler(_ context.Context, in *stopCrawlerInput) (*emptyOutput, error) {
	if err := h.Backend.StopCrawler(in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type updateCrawlerScheduleInput struct {
	CrawlerName string `json:"CrawlerName"`
	Schedule    string `json:"Schedule"`
}

func (h *Handler) handleUpdateCrawlerSchedule(_ context.Context, in *updateCrawlerScheduleInput) (*emptyOutput, error) {
	if err := h.Backend.UpdateCrawlerSchedule(in.CrawlerName, in.Schedule); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type startCrawlerScheduleInput struct {
	CrawlerName string `json:"CrawlerName"`
}

func (h *Handler) handleStartCrawlerSchedule(_ context.Context, in *startCrawlerScheduleInput) (*emptyOutput, error) {
	if err := h.Backend.StartCrawlerSchedule(in.CrawlerName); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type stopCrawlerScheduleInput struct {
	CrawlerName string `json:"CrawlerName"`
}

func (h *Handler) handleStopCrawlerSchedule(_ context.Context, in *stopCrawlerScheduleInput) (*emptyOutput, error) {
	if err := h.Backend.StopCrawlerSchedule(in.CrawlerName); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// getCrawlerMetricsInput holds input for GetCrawlerMetrics.
type getCrawlerMetricsInput struct {
	CrawlerNameList []string `json:"CrawlerNameList"`
}

// getCrawlerMetricsOutput holds the result for GetCrawlerMetrics.
type getCrawlerMetricsOutput struct {
	CrawlerMetricsList []*CrawlerMetrics `json:"CrawlerMetricsList"`
}

func (h *Handler) handleGetCrawlerMetrics(
	_ context.Context,
	in *getCrawlerMetricsInput,
) (*getCrawlerMetricsOutput, error) {
	metrics := h.Backend.GetCrawlerMetrics(in.CrawlerNameList)

	return &getCrawlerMetricsOutput{CrawlerMetricsList: metrics}, nil
}

// defaultListCrawlsLimit matches the real API's default page size (max 100).
const defaultListCrawlsLimit = 20

// listCrawlsInput holds input for ListCrawls.
type listCrawlsInput struct {
	CrawlerName string `json:"CrawlerName"`
	NextToken   string `json:"NextToken,omitempty"`
	MaxResults  int32  `json:"MaxResults,omitempty"`
}

// crawlHistoryOut is a single crawl-history entry.
type crawlHistoryOut struct {
	CrawlID   string  `json:"CrawlId,omitempty"`
	State     string  `json:"State,omitempty"`
	Summary   string  `json:"Summary,omitempty"`
	StartTime float64 `json:"StartTime,omitempty"`
	EndTime   float64 `json:"EndTime,omitempty"`
}

// listCrawlsOutput holds the result for ListCrawls.
type listCrawlsOutput struct {
	NextToken string            `json:"NextToken,omitempty"`
	Crawls    []crawlHistoryOut `json:"Crawls"`
}

func (h *Handler) handleListCrawls(_ context.Context, in *listCrawlsInput) (*listCrawlsOutput, error) {
	if in.CrawlerName == "" {
		return nil, fmt.Errorf("%w: CrawlerName is required", ErrValidation)
	}

	hist, err := h.Backend.ListCrawls(in.CrawlerName)
	if err != nil {
		return nil, err
	}

	limit := int(in.MaxResults)
	if limit <= 0 || limit > 100 {
		limit = defaultListCrawlsLimit
	}

	page, next := paginateSlice(hist, in.NextToken, limit)

	out := make([]crawlHistoryOut, 0, len(page))
	for _, e := range page {
		out = append(out, crawlHistoryOut{
			CrawlID:   e.CrawlID,
			State:     e.State,
			Summary:   e.Summary,
			StartTime: e.StartTime,
			EndTime:   e.EndTime,
		})
	}

	return &listCrawlsOutput{Crawls: out, NextToken: next}, nil
}
