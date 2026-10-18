package budgets

import "context"

type (
	createActionReq struct {
		ActionThreshold  *ActionThreshold `json:"ActionThreshold"`
		Definition       *Definition      `json:"Definition"`
		AccountID        string           `json:"AccountId"`
		ActionType       string           `json:"ActionType"`
		ApprovalModel    string           `json:"ApprovalModel"`
		BudgetName       string           `json:"BudgetName"`
		ExecutionRoleArn string           `json:"ExecutionRoleArn"`
		NotificationType string           `json:"NotificationType"`
		Subscribers      []Subscriber     `json:"Subscribers"`
		ResourceTags     []ResourceTag    `json:"ResourceTags"`
	}

	actionRef struct {
		AccountID  string `json:"AccountId"`
		ActionID   string `json:"ActionId"`
		BudgetName string `json:"BudgetName"`
	}

	createActionResp struct {
		AccountID  string `json:"AccountId"`
		ActionID   string `json:"ActionId"`
		BudgetName string `json:"BudgetName"`
	}

	actionResp struct {
		AccountID  string `json:"AccountId"`
		BudgetName string `json:"BudgetName"`
		Action     Action `json:"Action"`
	}

	updateActionReq struct {
		ActionThreshold  *ActionThreshold `json:"ActionThreshold"`
		Definition       *Definition      `json:"Definition"`
		AccountID        string           `json:"AccountId"`
		ActionID         string           `json:"ActionId"`
		ApprovalModel    string           `json:"ApprovalModel"`
		BudgetName       string           `json:"BudgetName"`
		ExecutionRoleArn string           `json:"ExecutionRoleArn"`
		NotificationType string           `json:"NotificationType"`
		Subscribers      []Subscriber     `json:"Subscribers"`
	}

	updateActionResp struct {
		AccountID  string `json:"AccountId"`
		BudgetName string `json:"BudgetName"`
		NewAction  Action `json:"NewAction"`
		OldAction  Action `json:"OldAction"`
	}

	executeActionReq struct {
		AccountID     string `json:"AccountId"`
		ActionID      string `json:"ActionId"`
		BudgetName    string `json:"BudgetName"`
		ExecutionType string `json:"ExecutionType"`
	}

	listActionsReq struct {
		pageReq
		AccountID  string `json:"AccountId"`
		BudgetName string `json:"BudgetName"`
	}

	listActionsResp struct {
		NextToken string   `json:"NextToken,omitempty"`
		Actions   []Action `json:"Actions"`
	}

	listHistoriesReq struct {
		pageReq
		TimePeriod *TimePeriod `json:"TimePeriod"`
		AccountID  string      `json:"AccountId"`
		ActionID   string      `json:"ActionId"`
		BudgetName string      `json:"BudgetName"`
	}

	listHistoriesResp struct {
		NextToken       string          `json:"NextToken,omitempty"`
		ActionHistories []ActionHistory `json:"ActionHistories"`
	}
)

func registerActionOps(ops map[string]opFunc) {
	ops["CreateBudgetAction"] = bind((*Handler).createAction)
	ops["DescribeBudgetAction"] = bind((*Handler).describeAction)
	ops["UpdateBudgetAction"] = bind((*Handler).updateAction)
	ops["DeleteBudgetAction"] = bind((*Handler).deleteAction)
	ops["ExecuteBudgetAction"] = bind((*Handler).executeAction)
	ops["DescribeBudgetActionsForBudget"] = bind((*Handler).describeActionsForBudget)
	ops["DescribeBudgetActionsForAccount"] = bind((*Handler).describeActionsForAccount)
	ops["DescribeBudgetActionHistories"] = bind((*Handler).describeActionHistories)
}

func (h *Handler) createAction(_ context.Context, in *createActionReq) (*createActionResp, error) {
	id, err := h.Backend.CreateBudgetAction(in.AccountID, in.BudgetName, Action{
		ActionThreshold: in.ActionThreshold, Definition: in.Definition, ActionType: in.ActionType,
		ApprovalModel: in.ApprovalModel, ExecutionRoleArn: in.ExecutionRoleArn,
		NotificationType: in.NotificationType, Subscribers: in.Subscribers,
	}, in.ResourceTags)
	if err != nil {
		return nil, err
	}

	return &createActionResp{AccountID: in.AccountID, ActionID: id, BudgetName: in.BudgetName}, nil
}

func (h *Handler) describeAction(ctx context.Context, in *actionRef) (*actionResp, error) {
	a, err := h.Backend.GetAction(ctx, in.AccountID, in.BudgetName, in.ActionID)
	if err != nil {
		return nil, err
	}

	return &actionResp{Action: *a, AccountID: in.AccountID, BudgetName: in.BudgetName}, nil
}

func (h *Handler) updateAction(_ context.Context, in *updateActionReq) (*updateActionResp, error) {
	oldA, newA, err := h.Backend.UpdateBudgetAction(in.AccountID, in.BudgetName, in.ActionID, ActionPatch{
		ActionThreshold: in.ActionThreshold, Definition: in.Definition, Subscribers: in.Subscribers,
		ApprovalModel: in.ApprovalModel, ExecutionRoleArn: in.ExecutionRoleArn, NotificationType: in.NotificationType,
	})
	if err != nil {
		return nil, err
	}

	return &updateActionResp{
		NewAction:  *newA,
		OldAction:  *oldA,
		AccountID:  in.AccountID,
		BudgetName: in.BudgetName,
	}, nil
}

func (h *Handler) deleteAction(_ context.Context, in *actionRef) (*actionResp, error) {
	a, err := h.Backend.DeleteBudgetAction(in.AccountID, in.BudgetName, in.ActionID)
	if err != nil {
		return nil, err
	}

	return &actionResp{Action: *a, AccountID: in.AccountID, BudgetName: in.BudgetName}, nil
}

func (h *Handler) executeAction(ctx context.Context, in *executeActionReq) (*executeActionReq, error) {
	if err := h.Backend.ExecuteBudgetAction(
		ctx,
		in.AccountID,
		in.BudgetName,
		in.ActionID,
		in.ExecutionType,
	); err != nil {
		return nil, err
	}

	return in, nil
}

func (h *Handler) describeActionsForBudget(ctx context.Context, in *listActionsReq) (*listActionsResp, error) {
	pg, err := h.Backend.ListActionsForBudget(ctx, in.AccountID, in.BudgetName, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listActionsResp{Actions: pg.Data, NextToken: pg.Next}, nil
}

func (h *Handler) describeActionsForAccount(ctx context.Context, in *accountPageReq) (*listActionsResp, error) {
	pg, err := h.Backend.ListActionsForAccount(ctx, in.AccountID, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listActionsResp{Actions: pg.Data, NextToken: pg.Next}, nil
}

func (h *Handler) describeActionHistories(_ context.Context, in *listHistoriesReq) (*listHistoriesResp, error) {
	pg, err := h.Backend.ListActionHistories(
		in.AccountID, in.BudgetName, in.ActionID, in.TimePeriod, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listHistoriesResp{ActionHistories: pg.Data, NextToken: pg.Next}, nil
}
