package ce

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type anomalyMonitorInput struct {
	MonitorName      string `json:"MonitorName"`
	MonitorType      string `json:"MonitorType"`
	MonitorDimension string `json:"MonitorDimension"`
}

type createAnomalyMonitorInput struct {
	AnomalyMonitor anomalyMonitorInput `json:"AnomalyMonitor"`
	ResourceTags   []resourceTag       `json:"ResourceTags"`
}

type createAnomalyMonitorOutput struct {
	MonitorArn string `json:"MonitorArn"`
}

func (h *Handler) handleCreateAnomalyMonitor(
	_ context.Context,
	in *createAnomalyMonitorInput,
) (*createAnomalyMonitorOutput, error) {
	if in.AnomalyMonitor.MonitorName == "" {
		return nil, fmt.Errorf("%w: MonitorName is required", ErrValidation)
	}

	if in.AnomalyMonitor.MonitorType == "" {
		return nil, fmt.Errorf("%w: MonitorType is required", ErrValidation)
	}

	mon, err := h.Backend.CreateAnomalyMonitor(
		in.AnomalyMonitor.MonitorName,
		in.AnomalyMonitor.MonitorType,
		in.AnomalyMonitor.MonitorDimension,
		resourceTagsToMap(in.ResourceTags),
	)
	if err != nil {
		return nil, err
	}

	return &createAnomalyMonitorOutput{MonitorArn: mon.MonitorARN}, nil
}

type deleteAnomalyMonitorInput struct {
	MonitorArn string `json:"MonitorArn"`
}

type deleteAnomalyMonitorOutput struct{}

func (h *Handler) handleDeleteAnomalyMonitor(
	_ context.Context,
	in *deleteAnomalyMonitorInput,
) (*deleteAnomalyMonitorOutput, error) {
	if in.MonitorArn == "" {
		return nil, fmt.Errorf("%w: MonitorArn is required", ErrValidation)
	}

	if err := h.Backend.DeleteAnomalyMonitor(in.MonitorArn); err != nil {
		return nil, err
	}

	return &deleteAnomalyMonitorOutput{}, nil
}

type getAnomalyMonitorsInput struct {
	NextPageToken  string   `json:"NextPageToken"`
	MonitorArnList []string `json:"MonitorArnList"`
	MaxResults     int      `json:"MaxResults"`
}

type anomalyMonitorSummary struct {
	CreationDate     *string `json:"CreationDate,omitempty"`
	LastUpdatedDate  *string `json:"LastUpdatedDate,omitempty"`
	MonitorArn       string  `json:"MonitorArn"`
	MonitorName      string  `json:"MonitorName"`
	MonitorType      string  `json:"MonitorType"`
	MonitorDimension string  `json:"MonitorDimension,omitempty"`
}

type getAnomalyMonitorsOutput struct {
	NextPageToken   string                  `json:"NextPageToken,omitempty"`
	AnomalyMonitors []anomalyMonitorSummary `json:"AnomalyMonitors"`
}

func (h *Handler) handleGetAnomalyMonitors(
	_ context.Context,
	in *getAnomalyMonitorsInput,
) (*getAnomalyMonitorsOutput, error) {
	monitors, nextToken, err := h.Backend.GetAnomalyMonitors(in.MonitorArnList, in.MaxResults, in.NextPageToken)
	if err != nil {
		return nil, err
	}

	items := make([]anomalyMonitorSummary, 0, len(monitors))

	for _, mon := range monitors {
		s := anomalyMonitorSummary{
			MonitorArn:       mon.MonitorARN,
			MonitorName:      mon.MonitorName,
			MonitorType:      mon.MonitorType,
			MonitorDimension: mon.MonitorDimension,
		}

		if !mon.CreationDate.IsZero() {
			v := mon.CreationDate.Format("2006-01-02")
			s.CreationDate = &v
		}

		if !mon.LastUpdatedDate.IsZero() {
			v := mon.LastUpdatedDate.Format("2006-01-02")
			s.LastUpdatedDate = &v
		}

		items = append(items, s)
	}

	return &getAnomalyMonitorsOutput{AnomalyMonitors: items, NextPageToken: nextToken}, nil
}

type updateAnomalyMonitorInput struct {
	MonitorArn  string `json:"MonitorArn"`
	MonitorName string `json:"MonitorName"`
}

type updateAnomalyMonitorOutput struct {
	MonitorArn string `json:"MonitorArn"`
}

func (h *Handler) handleUpdateAnomalyMonitor(
	_ context.Context,
	in *updateAnomalyMonitorInput,
) (*updateAnomalyMonitorOutput, error) {
	// Real AWS only requires MonitorArn here (see
	// aws-sdk-go-v2/service/costexplorer's UpdateAnomalyMonitorInput: MonitorName is
	// optional -- "Specify the fields that you want to update. Omitted fields are
	// unchanged"). Requiring MonitorName too would reject valid requests a real client
	// can make.
	if in.MonitorArn == "" {
		return nil, fmt.Errorf("%w: MonitorArn is required", ErrValidation)
	}

	mon, err := h.Backend.UpdateAnomalyMonitor(in.MonitorArn, in.MonitorName)
	if err != nil {
		return nil, err
	}

	return &updateAnomalyMonitorOutput{MonitorArn: mon.MonitorARN}, nil
}

type subscriberInput struct {
	Address string `json:"Address"`
	Type    string `json:"Type"`
	Status  string `json:"Status"`
}

type anomalySubscriptionInput struct {
	SubscriptionName string            `json:"SubscriptionName"`
	Frequency        string            `json:"Frequency"`
	MonitorArnList   []string          `json:"MonitorArnList"`
	Subscribers      []subscriberInput `json:"Subscribers"`
	Threshold        float64           `json:"Threshold"`
}

type createAnomalySubscriptionInput struct {
	ResourceTags        []resourceTag            `json:"ResourceTags"`
	AnomalySubscription anomalySubscriptionInput `json:"AnomalySubscription"`
}

type createAnomalySubscriptionOutput struct {
	SubscriptionArn string `json:"SubscriptionArn"`
}

func (h *Handler) handleCreateAnomalySubscription(
	_ context.Context,
	in *createAnomalySubscriptionInput,
) (*createAnomalySubscriptionOutput, error) {
	if in.AnomalySubscription.SubscriptionName == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrValidation)
	}

	if in.AnomalySubscription.MonitorArnList == nil {
		return nil, fmt.Errorf("%w: MonitorArnList is required", ErrValidation)
	}

	if in.AnomalySubscription.Subscribers == nil {
		return nil, fmt.Errorf("%w: Subscribers is required", ErrValidation)
	}

	if in.AnomalySubscription.Frequency == "" {
		return nil, fmt.Errorf("%w: Frequency is required", ErrValidation)
	}

	subs := make([]Subscriber, 0, len(in.AnomalySubscription.Subscribers))
	for _, s := range in.AnomalySubscription.Subscribers {
		subs = append(subs, Subscriber(s))
	}

	sub, err := h.Backend.CreateAnomalySubscription(
		in.AnomalySubscription.SubscriptionName,
		in.AnomalySubscription.Frequency,
		in.AnomalySubscription.MonitorArnList,
		subs,
		in.AnomalySubscription.Threshold,
		resourceTagsToMap(in.ResourceTags),
	)
	if err != nil {
		return nil, err
	}

	return &createAnomalySubscriptionOutput{SubscriptionArn: sub.SubscriptionARN}, nil
}

type deleteAnomalySubscriptionInput struct {
	SubscriptionArn string `json:"SubscriptionArn"`
}

type deleteAnomalySubscriptionOutput struct{}

func (h *Handler) handleDeleteAnomalySubscription(
	_ context.Context,
	in *deleteAnomalySubscriptionInput,
) (*deleteAnomalySubscriptionOutput, error) {
	if in.SubscriptionArn == "" {
		return nil, fmt.Errorf("%w: SubscriptionArn is required", ErrValidation)
	}

	if err := h.Backend.DeleteAnomalySubscription(in.SubscriptionArn); err != nil {
		return nil, err
	}

	return &deleteAnomalySubscriptionOutput{}, nil
}

type getAnomalySubscriptionsInput struct {
	MonitorArn          string   `json:"MonitorArn"`
	NextPageToken       string   `json:"NextPageToken"`
	SubscriptionArnList []string `json:"SubscriptionArnList"`
	MaxResults          int      `json:"MaxResults"`
}

type anomalySubscriptionSummary struct {
	SubscriptionArn  string            `json:"SubscriptionArn"`
	SubscriptionName string            `json:"SubscriptionName"`
	AccountID        string            `json:"AccountId,omitempty"`
	Frequency        string            `json:"Frequency"`
	MonitorArnList   []string          `json:"MonitorArnList"`
	Subscribers      []subscriberInput `json:"Subscribers"`
	Threshold        float64           `json:"Threshold,omitempty"`
}

type getAnomalySubscriptionsOutput struct {
	NextPageToken        string                       `json:"NextPageToken,omitempty"`
	AnomalySubscriptions []anomalySubscriptionSummary `json:"AnomalySubscriptions"`
}

func (h *Handler) handleGetAnomalySubscriptions(
	_ context.Context,
	in *getAnomalySubscriptionsInput,
) (*getAnomalySubscriptionsOutput, error) {
	subs, nextToken, err := h.Backend.GetAnomalySubscriptions(
		in.SubscriptionArnList, in.MonitorArn, in.MaxResults, in.NextPageToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]anomalySubscriptionSummary, 0, len(subs))

	for _, sub := range subs {
		subscribers := make([]subscriberInput, 0, len(sub.Subscribers))
		for _, s := range sub.Subscribers {
			subscribers = append(subscribers, subscriberInput(s))
		}

		items = append(items, anomalySubscriptionSummary{
			SubscriptionArn:  sub.SubscriptionARN,
			SubscriptionName: sub.SubscriptionName,
			AccountID:        sub.AccountID,
			MonitorArnList:   sub.MonitorARNList,
			Frequency:        sub.Frequency,
			Threshold:        sub.Threshold,
			Subscribers:      subscribers,
		})
	}

	return &getAnomalySubscriptionsOutput{AnomalySubscriptions: items, NextPageToken: nextToken}, nil
}

type updateAnomalySubscriptionInput struct {
	SubscriptionArn  string            `json:"SubscriptionArn"`
	Frequency        string            `json:"Frequency"`
	SubscriptionName string            `json:"SubscriptionName"`
	MonitorArnList   []string          `json:"MonitorArnList"`
	Subscribers      []subscriberInput `json:"Subscribers"`
	Threshold        float64           `json:"Threshold"`
}

type updateAnomalySubscriptionOutput struct {
	SubscriptionArn string `json:"SubscriptionArn"`
}

func (h *Handler) handleUpdateAnomalySubscription(
	_ context.Context,
	in *updateAnomalySubscriptionInput,
) (*updateAnomalySubscriptionOutput, error) {
	if in.SubscriptionArn == "" {
		return nil, fmt.Errorf("%w: SubscriptionArn is required", ErrValidation)
	}

	subs := make([]Subscriber, 0, len(in.Subscribers))
	for _, s := range in.Subscribers {
		subs = append(subs, Subscriber(s))
	}

	sub, err := h.Backend.UpdateAnomalySubscription(
		in.SubscriptionArn, in.Frequency, in.SubscriptionName,
		in.MonitorArnList, subs, in.Threshold,
	)
	if err != nil {
		return nil, err
	}

	return &updateAnomalySubscriptionOutput{SubscriptionArn: sub.SubscriptionARN}, nil
}

type anomalyDateInterval struct {
	StartDate string `json:"StartDate"`
	EndDate   string `json:"EndDate"`
}

type getAnomaliesInput struct {
	DateInterval  anomalyDateInterval `json:"DateInterval"`
	MonitorArn    string              `json:"MonitorArn"`
	Feedback      string              `json:"Feedback"`
	TotalImpact   map[string]any      `json:"TotalImpact"`
	NextPageToken string              `json:"NextPageToken"`
	MaxResults    int                 `json:"MaxResults"`
}

type anomalyImpact struct {
	MaxImpact             float64 `json:"MaxImpact"`
	TotalImpact           float64 `json:"TotalImpact"`
	TotalActualSpend      float64 `json:"TotalActualSpend"`
	TotalExpectedSpend    float64 `json:"TotalExpectedSpend"`
	TotalImpactPercentage float64 `json:"TotalImpactPercentage"`
}

type anomalySummary struct {
	AnomalyID        string             `json:"AnomalyId"`
	AnomalyStartDate string             `json:"AnomalyStartDate"`
	AnomalyEndDate   string             `json:"AnomalyEndDate"`
	DimensionValue   string             `json:"DimensionValue"`
	MonitorArn       string             `json:"MonitorArn"`
	SubscriptionArn  string             `json:"SubscriptionArn,omitempty"`
	Feedback         string             `json:"Feedback,omitempty"`
	RootCauses       []AnomalyRootCause `json:"RootCauses,omitempty"`
	Impact           anomalyImpact      `json:"Impact"`
	AnomalyScore     AnomalyScore       `json:"AnomalyScore"`
}

type getAnomaliesOutput struct {
	NextPageToken string           `json:"NextPageToken,omitempty"`
	Anomalies     []anomalySummary `json:"Anomalies"`
}

func (h *Handler) handleGetAnomalies(
	_ context.Context,
	in *getAnomaliesInput,
) (*getAnomaliesOutput, error) {
	if in.DateInterval.StartDate == "" {
		return nil, fmt.Errorf("%w: DateInterval.StartDate is required", ErrValidation)
	}

	anomalies, nextToken := h.Backend.GetAnomalies(
		in.MonitorArn, in.Feedback,
		in.DateInterval.StartDate, in.DateInterval.EndDate,
		in.MaxResults, in.NextPageToken,
	)
	items := make([]anomalySummary, 0, len(anomalies))

	for _, a := range anomalies {
		items = append(items, anomalySummary{
			AnomalyID:        a.AnomalyID,
			AnomalyStartDate: a.AnomalyStartDate,
			AnomalyEndDate:   a.AnomalyEndDate,
			DimensionValue:   a.DimensionValue,
			MonitorArn:       a.MonitorARN,
			SubscriptionArn:  a.SubscriptionARN,
			AnomalyScore:     a.AnomalyScore,
			Impact: anomalyImpact{
				MaxImpact:             a.TotalImpact,
				TotalImpact:           a.TotalImpact,
				TotalActualSpend:      a.TotalImpact * anomalyActualSpendMultiplier,
				TotalExpectedSpend:    a.TotalImpact * anomalyExpectedSpendMultiplier,
				TotalImpactPercentage: anomalyImpactPercentage,
			},
			Feedback:   a.FeedbackType,
			RootCauses: a.RootCauses,
		})
	}

	return &getAnomaliesOutput{Anomalies: items, NextPageToken: nextToken}, nil
}

type provideAnomalyFeedbackInput struct {
	AnomalyID string `json:"AnomalyId"`
	Feedback  string `json:"Feedback"`
}

type provideAnomalyFeedbackOutput struct {
	AnomalyID string `json:"AnomalyId"`
}

func (h *Handler) handleProvideAnomalyFeedback(
	_ context.Context,
	in *provideAnomalyFeedbackInput,
) (*provideAnomalyFeedbackOutput, error) {
	if in.AnomalyID == "" {
		return nil, fmt.Errorf("%w: AnomalyId is required", ErrValidation)
	}

	if err := h.Backend.ProvideAnomalyFeedback(in.AnomalyID, in.Feedback); err != nil {
		return nil, err
	}

	return &provideAnomalyFeedbackOutput{
		AnomalyID: in.AnomalyID,
	}, nil
}

// buildAnomalyOps returns the anomaly-family op dispatch entries.
func (h *Handler) buildAnomalyOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateAnomalyMonitor": service.WrapOp(
			h.handleCreateAnomalyMonitor,
		),
		"DeleteAnomalyMonitor": service.WrapOp(
			h.handleDeleteAnomalyMonitor,
		),
		"GetAnomalyMonitors": service.WrapOp(
			h.handleGetAnomalyMonitors,
		),
		"UpdateAnomalyMonitor": service.WrapOp(
			h.handleUpdateAnomalyMonitor,
		),
		"CreateAnomalySubscription": service.WrapOp(
			h.handleCreateAnomalySubscription,
		),
		"DeleteAnomalySubscription": service.WrapOp(
			h.handleDeleteAnomalySubscription,
		),
		"GetAnomalySubscriptions": service.WrapOp(
			h.handleGetAnomalySubscriptions,
		),
		"UpdateAnomalySubscription": service.WrapOp(
			h.handleUpdateAnomalySubscription,
		),
		"GetAnomalies": service.WrapOp(h.handleGetAnomalies),
		"ProvideAnomalyFeedback": service.WrapOp(
			h.handleProvideAnomalyFeedback,
		),
	}
}
