package budgets

import "context"

type emptyResp struct{}

type (
	accountReq struct {
		AccountID  string `json:"AccountId"`
		BudgetName string `json:"BudgetName"`
	}

	pageReq struct {
		MaxResults *int32 `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}

	createBudgetReq struct {
		AccountID                    string                        `json:"AccountId"`
		Budget                       Budget                        `json:"Budget"`
		NotificationsWithSubscribers []NotificationWithSubscribers `json:"NotificationsWithSubscribers"`
		ResourceTags                 []ResourceTag                 `json:"ResourceTags"`
	}

	updateBudgetReq struct {
		AccountID string `json:"AccountId"`
		NewBudget Budget `json:"NewBudget"`
	}

	describeBudgetReq struct {
		AccountID            string `json:"AccountId"`
		BudgetName           string `json:"BudgetName"`
		ShowFilterExpression bool   `json:"ShowFilterExpression"`
	}

	describeBudgetResp struct {
		Budget Budget `json:"Budget"`
	}

	describeBudgetsReq struct {
		pageReq
		AccountID            string `json:"AccountId"`
		ShowFilterExpression bool   `json:"ShowFilterExpression"`
	}

	describeBudgetsResp struct {
		NextToken string   `json:"NextToken,omitempty"`
		Budgets   []Budget `json:"Budgets"`
	}

	performanceReq struct {
		pageReq
		TimePeriod *TimePeriod `json:"TimePeriod"`
		AccountID  string      `json:"AccountId"`
		BudgetName string      `json:"BudgetName"`
	}

	performanceResp struct {
		BudgetPerformanceHistory *PerformanceHistory `json:"BudgetPerformanceHistory"`
		NextToken                string              `json:"NextToken,omitempty"`
	}
)

func registerBudgetOps(ops map[string]opFunc) {
	ops["CreateBudget"] = bind((*Handler).createBudget)
	ops["UpdateBudget"] = bind((*Handler).updateBudget)
	ops["DeleteBudget"] = bind((*Handler).deleteBudget)
	ops["DescribeBudget"] = bind((*Handler).describeBudget)
	ops["DescribeBudgets"] = bind((*Handler).describeBudgets)
	ops["DescribeBudgetPerformanceHistory"] = bind((*Handler).describePerformance)
}

func (h *Handler) createBudget(ctx context.Context, in *createBudgetReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.CreateBudget(
		ctx, in.AccountID, in.Budget, in.NotificationsWithSubscribers, in.ResourceTags)
}

func (h *Handler) updateBudget(ctx context.Context, in *updateBudgetReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.UpdateBudget(ctx, in.AccountID, in.NewBudget)
}

func (h *Handler) deleteBudget(_ context.Context, in *accountReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.DeleteBudget(in.AccountID, in.BudgetName)
}

func (h *Handler) describeBudget(ctx context.Context, in *describeBudgetReq) (*describeBudgetResp, error) {
	bg, err := h.Backend.GetBudget(ctx, in.AccountID, in.BudgetName, in.ShowFilterExpression)
	if err != nil {
		return nil, err
	}

	return &describeBudgetResp{Budget: *bg}, nil
}

func (h *Handler) describeBudgets(ctx context.Context, in *describeBudgetsReq) (*describeBudgetsResp, error) {
	pg, err := h.Backend.ListBudgets(ctx, in.AccountID, in.NextToken, in.MaxResults, in.ShowFilterExpression)
	if err != nil {
		return nil, err
	}

	return &describeBudgetsResp{Budgets: pg.Data, NextToken: pg.Next}, nil
}

func (h *Handler) describePerformance(_ context.Context, in *performanceReq) (*performanceResp, error) {
	ph, next, err := h.Backend.BudgetPerformance(
		in.AccountID,
		in.BudgetName,
		in.TimePeriod,
		in.NextToken,
		in.MaxResults,
	)
	if err != nil {
		return nil, err
	}

	return &performanceResp{BudgetPerformanceHistory: ph, NextToken: next}, nil
}
