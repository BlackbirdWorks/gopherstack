package budgets

import "context"

type (
	notificationReq struct {
		Subscriber   Subscriber   `json:"Subscriber"`
		AccountID    string       `json:"AccountId"`
		BudgetName   string       `json:"BudgetName"`
		Notification Notification `json:"Notification"`
		Subscribers  []Subscriber `json:"Subscribers"`
	}

	updateNotificationReq struct {
		AccountID       string       `json:"AccountId"`
		BudgetName      string       `json:"BudgetName"`
		NewNotification Notification `json:"NewNotification"`
		OldNotification Notification `json:"OldNotification"`
	}

	updateSubscriberReq struct {
		NewSubscriber Subscriber   `json:"NewSubscriber"`
		OldSubscriber Subscriber   `json:"OldSubscriber"`
		AccountID     string       `json:"AccountId"`
		BudgetName    string       `json:"BudgetName"`
		Notification  Notification `json:"Notification"`
	}

	listNotificationsReq struct {
		pageReq
		AccountID  string `json:"AccountId"`
		BudgetName string `json:"BudgetName"`
	}

	listNotificationsResp struct {
		NextToken     string         `json:"NextToken,omitempty"`
		Notifications []Notification `json:"Notifications"`
	}

	listSubscribersReq struct {
		pageReq
		AccountID    string       `json:"AccountId"`
		BudgetName   string       `json:"BudgetName"`
		Notification Notification `json:"Notification"`
	}

	listSubscribersResp struct {
		NextToken   string       `json:"NextToken,omitempty"`
		Subscribers []Subscriber `json:"Subscribers"`
	}

	accountPageReq struct {
		pageReq
		AccountID string `json:"AccountId"`
	}

	accountNotificationsResp struct {
		NextToken                     string                          `json:"NextToken,omitempty"`
		BudgetNotificationsForAccount []BudgetNotificationsForAccount `json:"BudgetNotificationsForAccount"`
	}
)

func registerNotificationOps(ops map[string]opFunc) {
	ops["CreateNotification"] = bind((*Handler).createNotification)
	ops["DeleteNotification"] = bind((*Handler).deleteNotification)
	ops["UpdateNotification"] = bind((*Handler).updateNotification)
	ops["DescribeNotificationsForBudget"] = bind((*Handler).describeNotifications)
	ops["DescribeBudgetNotificationsForAccount"] = bind((*Handler).describeAccountNotifications)
	ops["CreateSubscriber"] = bind((*Handler).createSubscriber)
	ops["DeleteSubscriber"] = bind((*Handler).deleteSubscriber)
	ops["UpdateSubscriber"] = bind((*Handler).updateSubscriber)
	ops["DescribeSubscribersForNotification"] = bind((*Handler).describeSubscribers)
}

func (h *Handler) createNotification(ctx context.Context, in *notificationReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.CreateNotification(ctx, in.AccountID, in.BudgetName, in.Notification, in.Subscribers)
}

func (h *Handler) deleteNotification(_ context.Context, in *notificationReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.DeleteNotification(in.AccountID, in.BudgetName, in.Notification)
}

func (h *Handler) updateNotification(ctx context.Context, in *updateNotificationReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.UpdateNotification(
		ctx, in.AccountID, in.BudgetName, in.OldNotification, in.NewNotification)
}

func (h *Handler) describeNotifications(ctx context.Context, in *listNotificationsReq) (*listNotificationsResp, error) {
	pg, err := h.Backend.ListNotifications(ctx, in.AccountID, in.BudgetName, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listNotificationsResp{Notifications: pg.Data, NextToken: pg.Next}, nil
}

func (h *Handler) describeAccountNotifications(
	ctx context.Context, in *accountPageReq,
) (*accountNotificationsResp, error) {
	pg, err := h.Backend.ListBudgetNotificationsForAccount(ctx, in.AccountID, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &accountNotificationsResp{BudgetNotificationsForAccount: pg.Data, NextToken: pg.Next}, nil
}

func (h *Handler) createSubscriber(_ context.Context, in *notificationReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.CreateSubscriber(in.AccountID, in.BudgetName, in.Notification, in.Subscriber)
}

func (h *Handler) deleteSubscriber(_ context.Context, in *notificationReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.DeleteSubscriber(in.AccountID, in.BudgetName, in.Notification, in.Subscriber)
}

func (h *Handler) updateSubscriber(_ context.Context, in *updateSubscriberReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.UpdateSubscriber(
		in.AccountID, in.BudgetName, in.Notification, in.OldSubscriber, in.NewSubscriber)
}

func (h *Handler) describeSubscribers(_ context.Context, in *listSubscribersReq) (*listSubscribersResp, error) {
	pg, err := h.Backend.ListSubscribers(in.AccountID, in.BudgetName, in.Notification, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listSubscribersResp{Subscribers: pg.Data, NextToken: pg.Next}, nil
}
