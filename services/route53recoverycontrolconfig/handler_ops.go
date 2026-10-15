package route53recoverycontrolconfig

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	keyCluster        = "Cluster"
	keyControlPanel   = "ControlPanel"
	keyRoutingControl = "RoutingControl"
)

type listQuery struct {
	token string
	max   int
}

func listParams(req *restroute.Request) (listQuery, error) {
	m, err := restroute.MaxResultsQuery(req.Query, "MaxResults")
	if err != nil {
		return listQuery{}, err
	}

	return listQuery{token: req.Query.Get("NextToken"), max: m}, nil
}

func (h *Handler) createCluster(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		Tags        map[string]string
		ClusterName string
		ClientToken string
		NetworkType string
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	c, err := h.Backend.CreateCluster(ctx, in.ClusterName, in.NetworkType, in.ClientToken, in.Tags)

	return map[string]any{keyCluster: c}, 0, err
}

func (h *Handler) describeCluster(ctx context.Context, req *restroute.Request) (any, int, error) {
	c, err := h.Backend.DescribeCluster(ctx, req.Params["ClusterArn"])

	return map[string]any{keyCluster: c}, 0, err
}

func (h *Handler) listClusters(ctx context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	items, next, err := h.Backend.ListClusters(ctx, q.token, q.max)

	return pageOut("Clusters", items, next), 0, err
}

func (h *Handler) updateCluster(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct{ ClusterArn, NetworkType string }
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	c, err := h.Backend.UpdateCluster(ctx, in.ClusterArn, in.NetworkType)

	return map[string]any{keyCluster: c}, 0, err
}

func (h *Handler) deleteCluster(_ context.Context, req *restroute.Request) (any, int, error) {
	return nil, 0, h.Backend.DeleteCluster(req.Params["ClusterArn"])
}

func (h *Handler) createControlPanel(_ context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		Tags             map[string]string
		ClusterArn       string
		ControlPanelName string
		ClientToken      string
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	p, err := h.Backend.CreateControlPanel(in.ClusterArn, in.ControlPanelName, in.ClientToken, in.Tags)

	return map[string]any{keyControlPanel: p}, 0, err
}

func (h *Handler) describeControlPanel(_ context.Context, req *restroute.Request) (any, int, error) {
	p, err := h.Backend.DescribeControlPanel(req.Params["ControlPanelArn"])

	return map[string]any{keyControlPanel: p}, 0, err
}

func (h *Handler) listControlPanels(_ context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	items, next, err := h.Backend.ListControlPanels(req.Query.Get("ClusterArn"), q.token, q.max)

	return pageOut("ControlPanels", items, next), 0, err
}

func (h *Handler) updateControlPanel(_ context.Context, req *restroute.Request) (any, int, error) {
	var in struct{ ControlPanelArn, ControlPanelName string }
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	p, err := h.Backend.UpdateControlPanel(in.ControlPanelArn, in.ControlPanelName)

	return map[string]any{keyControlPanel: p}, 0, err
}

func (h *Handler) deleteControlPanel(_ context.Context, req *restroute.Request) (any, int, error) {
	return nil, 0, h.Backend.DeleteControlPanel(req.Params["ControlPanelArn"])
}

func (h *Handler) createRoutingControl(_ context.Context, req *restroute.Request) (any, int, error) {
	var in struct{ ClusterArn, RoutingControlName, ClientToken, ControlPanelArn string }
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	c, err := h.Backend.CreateRoutingControl(in.ClusterArn, in.ControlPanelArn, in.RoutingControlName, in.ClientToken)

	return map[string]any{keyRoutingControl: c}, 0, err
}

func (h *Handler) describeRoutingControl(_ context.Context, req *restroute.Request) (any, int, error) {
	c, err := h.Backend.DescribeRoutingControl(req.Params["RoutingControlArn"])

	return map[string]any{keyRoutingControl: c}, 0, err
}

func (h *Handler) listRoutingControls(_ context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	items, next, err := h.Backend.ListRoutingControls(req.Params["ControlPanelArn"], q.token, q.max)

	return pageOut("RoutingControls", items, next), 0, err
}

func (h *Handler) updateRoutingControl(_ context.Context, req *restroute.Request) (any, int, error) {
	var in struct{ RoutingControlArn, RoutingControlName string }
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	c, err := h.Backend.UpdateRoutingControl(in.RoutingControlArn, in.RoutingControlName)

	return map[string]any{keyRoutingControl: c}, 0, err
}

func (h *Handler) deleteRoutingControl(_ context.Context, req *restroute.Request) (any, int, error) {
	return nil, 0, h.Backend.DeleteRoutingControl(req.Params["RoutingControlArn"])
}

func (h *Handler) listAssociatedHealthChecks(_ context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	ids, err := h.Backend.AssociatedHealthCheckIDs(req.Params["RoutingControlArn"])
	if err != nil {
		return nil, 0, err
	}

	pg, err := restroute.Paginate(ids, q.token, q.max, listDefault, listMax)
	if err != nil {
		return nil, 0, err
	}

	return pageOut("HealthCheckIds", append([]string{}, pg.Data...), pg.Next), 0, nil
}

func (h *Handler) createSafetyRule(_ context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		AssertionRule *wireNewAssertionRule
		GatingRule    *wireNewGatingRule
		Tags          map[string]string
		ClientToken   string
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	spec, err := newRuleSpec(in.AssertionRule, in.GatingRule)
	if err != nil {
		return nil, 0, err
	}

	r, err := h.Backend.CreateSafetyRule(spec, in.ClientToken, in.Tags)
	if err != nil {
		return nil, 0, err
	}

	return ruleBody(r), 0, nil
}

func newRuleSpec(a *wireNewAssertionRule, g *wireNewGatingRule) (*NewRuleSpec, error) {
	if (a == nil) == (g == nil) {
		return nil, restroute.Validation("Specify exactly one of AssertionRule or GatingRule")
	}

	var (
		cfg  *wireNewRuleConfig
		spec NewRuleSpec
		wait *int32
	)

	if a != nil {
		cfg, wait = a.RuleConfig, a.WaitPeriodMs
		spec = NewRuleSpec{Name: a.Name, PanelARN: a.ControlPanelArn, Kind: ruleAssertion, Controls: a.AssertedControls}
	} else {
		cfg, wait = g.RuleConfig, g.WaitPeriodMs
		spec = NewRuleSpec{
			Name:     g.Name,
			PanelARN: g.ControlPanelArn,
			Kind:     ruleGating,
			Controls: g.GatingControls,
			Targets:  g.TargetControls,
		}
	}

	if cfg == nil || cfg.Inverted == nil || cfg.Threshold == nil || wait == nil {
		return nil, restroute.Validation("RuleConfig (Inverted, Threshold, Type) and WaitPeriodMs are required")
	}

	spec.RuleType, spec.Inverted, spec.Threshold, spec.WaitPeriodMs = cfg.Type, *cfg.Inverted, *cfg.Threshold, *wait

	return &spec, nil
}

func ruleBody(r wireRule) map[string]any {
	if r.Gating != nil {
		return map[string]any{"GatingRule": r.Gating}
	}

	return map[string]any{"AssertionRule": r.Assertion}
}

func (h *Handler) describeSafetyRule(_ context.Context, req *restroute.Request) (any, int, error) {
	r, err := h.Backend.DescribeSafetyRule(req.Params["SafetyRuleArn"])
	if err != nil {
		return nil, 0, err
	}

	return ruleBody(r), 0, nil
}

func (h *Handler) listSafetyRules(_ context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	items, next, err := h.Backend.ListSafetyRules(req.Params["ControlPanelArn"], q.token, q.max)

	return pageOut("SafetyRules", items, next), 0, err
}

func (h *Handler) updateSafetyRule(_ context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		AssertionRuleUpdate *wireRuleUpdate
		GatingRuleUpdate    *wireRuleUpdate
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	if (in.AssertionRuleUpdate == nil) == (in.GatingRuleUpdate == nil) {
		return nil, 0, restroute.Validation("Specify exactly one of AssertionRuleUpdate or GatingRuleUpdate")
	}

	kind, u := ruleAssertion, in.AssertionRuleUpdate
	if u == nil {
		kind, u = ruleGating, in.GatingRuleUpdate
	}

	if u.WaitPeriodMs == nil {
		return nil, 0, restroute.Validation("WaitPeriodMs is required")
	}

	r, err := h.Backend.UpdateSafetyRule(kind, u.SafetyRuleArn, u.Name, *u.WaitPeriodMs)
	if err != nil {
		return nil, 0, err
	}

	return ruleBody(r), 0, nil
}

func (h *Handler) deleteSafetyRule(_ context.Context, req *restroute.Request) (any, int, error) {
	return nil, 0, h.Backend.DeleteSafetyRule(req.Params["SafetyRuleArn"])
}

func (h *Handler) getResourcePolicy(_ context.Context, req *restroute.Request) (any, int, error) {
	return nil, 0, h.Backend.requireResource(req.Params["ResourceArn"])
}

func (h *Handler) listTags(_ context.Context, req *restroute.Request) (any, int, error) {
	t, err := h.Backend.ListTagsForResource(req.Params["ResourceArn"])
	if t == nil {
		t = map[string]string{}
	}

	return map[string]any{"Tags": t}, 0, err
}

func (h *Handler) tagResource(_ context.Context, req *restroute.Request) (any, int, error) {
	var in struct{ Tags map[string]string }
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	if len(in.Tags) == 0 {
		return nil, 0, restroute.Validation("Tags is required")
	}

	return nil, 0, h.Backend.TagResource(req.Params["ResourceArn"], in.Tags)
}

func (h *Handler) untagResource(_ context.Context, req *restroute.Request) (any, int, error) {
	keys := req.Query["TagKeys"]
	if len(keys) == 0 {
		return nil, 0, restroute.Validation("TagKeys is required")
	}

	return nil, 0, h.Backend.UntagResource(req.Params["ResourceArn"], keys)
}

func pageOut[T any](key string, items []T, next string) map[string]any {
	if items == nil {
		items = []T{}
	}

	out := map[string]any{key: items}
	if next != "" {
		out["NextToken"] = next
	}

	return out
}
