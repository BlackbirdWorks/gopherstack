package amp

import (
	"context"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	statusAccepted = http.StatusAccepted
	keyWorkspaceID = "workspaceId"
	paramScraperID = "scraperId"
	paramResource  = "resourceArn"
	keyARN         = "arn"
	keyTags        = "tags"
	keyStatus      = "status"
)

type listQuery struct {
	token string
	max   int
}

func listParams(req *restroute.Request) (listQuery, error) {
	m, err := restroute.MaxResultsQuery(req.Query, "maxResults")
	if err != nil {
		return listQuery{}, err
	}

	return listQuery{token: req.Query.Get("nextToken"), max: m}, nil
}

func pageOut[T any](key string, items []T, next string) map[string]any {
	out := map[string]any{key: items}
	if next != "" {
		out["nextToken"] = next
	}

	return out
}

func (h *Handler) workspaceOps(ops map[string]restroute.OpFunc) {
	ops["CreateWorkspace"] = h.createWorkspace
	ops["DescribeWorkspace"] = h.describeWorkspace
	ops["ListWorkspaces"] = h.listWorkspaces
	ops["DeleteWorkspace"] = h.deleteWorkspace
	ops["UpdateWorkspaceAlias"] = h.updateWorkspaceAlias
	ops["DescribeWorkspaceConfiguration"] = h.describeWorkspaceConfiguration
	ops["UpdateWorkspaceConfiguration"] = h.updateWorkspaceConfiguration
}

func (h *Handler) createWorkspace(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		Tags        map[string]string `json:"tags"`
		Alias       string            `json:"alias"`
		ClientToken string            `json:"clientToken"`
		KMSKeyARN   string            `json:"kmsKeyArn"`
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	w, err := h.Backend.CreateWorkspace(ctx, CreateWorkspaceInput{
		Tags: in.Tags, Alias: in.Alias, ClientToken: in.ClientToken, KMSKeyARN: in.KMSKeyARN,
	})
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{
		keyARN: w.Arn, keyWorkspaceID: w.WorkspaceID, keyStatus: w.Status, "kmsKeyArn": omitEmpty(w.KmsKeyArn),
		keyTags: w.Tags,
	}, statusAccepted, nil
}

func omitEmpty(s string) any {
	if s == "" {
		return nil
	}

	return s
}

func (h *Handler) describeWorkspace(ctx context.Context, req *restroute.Request) (any, int, error) {
	w, err := h.Backend.DescribeWorkspace(ctx, req.Params[keyWorkspaceID])

	return map[string]any{"workspace": w}, 0, err
}

func (h *Handler) listWorkspaces(ctx context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	pg, err := h.Backend.ListWorkspaces(ctx, req.Query.Get("alias"), q.token, q.max)
	if err != nil {
		return nil, 0, err
	}

	return pageOut("workspaces", pg.Data, pg.Next), 0, nil
}

func (h *Handler) deleteWorkspace(ctx context.Context, req *restroute.Request) (any, int, error) {
	return nil, statusAccepted, h.Backend.DeleteWorkspace(ctx, req.Params[keyWorkspaceID])
}

func (h *Handler) updateWorkspaceAlias(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		Alias string `json:"alias"`
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	return nil, http.StatusNoContent, h.Backend.UpdateWorkspaceAlias(ctx, req.Params[keyWorkspaceID], in.Alias)
}

func (h *Handler) describeWorkspaceConfiguration(ctx context.Context, req *restroute.Request) (any, int, error) {
	c, err := h.Backend.DescribeWorkspaceConfiguration(ctx, req.Params[keyWorkspaceID])

	return map[string]any{"workspaceConfiguration": c}, 0, err
}

func (h *Handler) updateWorkspaceConfiguration(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		RetentionPeriodInDays         *int32    `json:"retentionPeriodInDays"`
		OutOfOrderTimeWindowInSeconds *int32    `json:"outOfOrderTimeWindowInSeconds"`
		RuleQueryOffsetInSeconds      *int32    `json:"ruleQueryOffsetInSeconds"`
		LimitsPerLabelSet             *[]wLimit `json:"limitsPerLabelSet"`
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	cfg := ConfigInput{
		RetentionPeriodInDays:         in.RetentionPeriodInDays,
		OutOfOrderTimeWindowInSeconds: in.OutOfOrderTimeWindowInSeconds,
		RuleQueryOffsetInSeconds:      in.RuleQueryOffsetInSeconds,
	}

	if in.LimitsPerLabelSet != nil {
		cfg.ReplaceLimits = true

		for _, l := range *in.LimitsPerLabelSet {
			cfg.LimitsPerLabelSet = append(
				cfg.LimitsPerLabelSet,
				LabelSetLimit{LabelSet: l.LabelSet, MaxSeries: l.Limits.MaxSeries},
			)
		}
	}

	st, err := h.Backend.UpdateWorkspaceConfiguration(ctx, req.Params[keyWorkspaceID], cfg)

	return map[string]any{keyStatus: st}, statusAccepted, err
}

func (h *Handler) docOps(ops map[string]restroute.OpFunc) {
	ops["CreateAlertManagerDefinition"] = h.createAlertManager
	ops["PutAlertManagerDefinition"] = h.putAlertManager
	ops["DescribeAlertManagerDefinition"] = h.describeAlertManager
	ops["DeleteAlertManagerDefinition"] = h.deleteAlertManager
	ops["CreateLoggingConfiguration"] = h.createLogging
	ops["UpdateLoggingConfiguration"] = h.updateLogging
	ops["DescribeLoggingConfiguration"] = h.describeLogging
	ops["DeleteLoggingConfiguration"] = h.deleteLogging
	ops["CreateQueryLoggingConfiguration"] = h.createQueryLogging
	ops["UpdateQueryLoggingConfiguration"] = h.updateQueryLogging
	ops["DescribeQueryLoggingConfiguration"] = h.describeQueryLogging
	ops["DeleteQueryLoggingConfiguration"] = h.deleteQueryLogging
	ops["PutResourcePolicy"] = h.putPolicy
	ops["DescribeResourcePolicy"] = h.describePolicy
	ops["DeleteResourcePolicy"] = h.deletePolicy
}

type dataIn struct {
	ClientToken string `json:"clientToken"`
	Data        []byte `json:"data"`
}

func (h *Handler) createAlertManager(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in dataIn
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	st, err := h.Backend.CreateAlertManagerDefinition(ctx, req.Params[keyWorkspaceID], in.ClientToken, in.Data)

	return map[string]any{keyStatus: st}, statusAccepted, err
}

func (h *Handler) putAlertManager(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in dataIn
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	st, err := h.Backend.PutAlertManagerDefinition(ctx, req.Params[keyWorkspaceID], in.Data)

	return map[string]any{keyStatus: st}, statusAccepted, err
}

func (h *Handler) describeAlertManager(ctx context.Context, req *restroute.Request) (any, int, error) {
	d, err := h.Backend.DescribeAlertManagerDefinition(ctx, req.Params[keyWorkspaceID])

	return map[string]any{"alertManagerDefinition": d}, 0, err
}

func (h *Handler) deleteAlertManager(ctx context.Context, req *restroute.Request) (any, int, error) {
	return nil, statusAccepted, h.Backend.DeleteAlertManagerDefinition(ctx, req.Params[keyWorkspaceID])
}

type logGroupIn struct {
	ClientToken string `json:"clientToken"`
	LogGroupArn string `json:"logGroupArn"`
}

func (h *Handler) createLogging(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in logGroupIn
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	st, err := h.Backend.CreateLoggingConfiguration(ctx, req.Params[keyWorkspaceID], in.ClientToken, in.LogGroupArn)

	return map[string]any{keyStatus: st}, statusAccepted, err
}

func (h *Handler) updateLogging(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in logGroupIn
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	st, err := h.Backend.UpdateLoggingConfiguration(ctx, req.Params[keyWorkspaceID], in.LogGroupArn)

	return map[string]any{keyStatus: st}, statusAccepted, err
}

func (h *Handler) describeLogging(ctx context.Context, req *restroute.Request) (any, int, error) {
	l, err := h.Backend.DescribeLoggingConfiguration(ctx, req.Params[keyWorkspaceID])

	return map[string]any{"loggingConfiguration": l}, 0, err
}

func (h *Handler) deleteLogging(ctx context.Context, req *restroute.Request) (any, int, error) {
	return nil, statusAccepted, h.Backend.DeleteLoggingConfiguration(ctx, req.Params[keyWorkspaceID])
}

type queryLogIn struct {
	ClientToken  string                `json:"clientToken"`
	Destinations []wLoggingDestination `json:"destinations"`
}

func (in queryLogIn) destinations() []LoggingDestination {
	out := make([]LoggingDestination, 0, len(in.Destinations))
	for _, d := range in.Destinations {
		out = append(
			out,
			LoggingDestination{LogGroupARN: d.CloudWatchLogs.LogGroupArn, QspThreshold: d.Filters.QspThreshold},
		)
	}

	return out
}

func (h *Handler) createQueryLogging(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in queryLogIn
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	st, err := h.Backend.CreateQueryLoggingConfiguration(
		ctx,
		req.Params[keyWorkspaceID],
		in.ClientToken,
		in.destinations(),
	)

	return map[string]any{keyStatus: st}, statusAccepted, err
}

func (h *Handler) updateQueryLogging(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in queryLogIn
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	st, err := h.Backend.UpdateQueryLoggingConfiguration(ctx, req.Params[keyWorkspaceID], in.destinations())

	return map[string]any{keyStatus: st}, statusAccepted, err
}

func (h *Handler) describeQueryLogging(ctx context.Context, req *restroute.Request) (any, int, error) {
	q, err := h.Backend.DescribeQueryLoggingConfiguration(ctx, req.Params[keyWorkspaceID])

	return map[string]any{"queryLoggingConfiguration": q}, 0, err
}

func (h *Handler) deleteQueryLogging(ctx context.Context, req *restroute.Request) (any, int, error) {
	return nil, statusAccepted, h.Backend.DeleteQueryLoggingConfiguration(ctx, req.Params[keyWorkspaceID])
}

func (h *Handler) putPolicy(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		PolicyDocument string `json:"policyDocument"`
		RevisionID     string `json:"revisionId"`
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	r, err := h.Backend.PutResourcePolicy(ctx, req.Params[keyWorkspaceID], in.PolicyDocument, in.RevisionID)

	return map[string]any{"policyStatus": r.Status, "revisionId": r.RevisionID}, statusAccepted, err
}

func (h *Handler) describePolicy(ctx context.Context, req *restroute.Request) (any, int, error) {
	r, err := h.Backend.DescribeResourcePolicy(ctx, req.Params[keyWorkspaceID])

	return map[string]any{"policyDocument": r.Document, "policyStatus": r.Status, "revisionId": r.RevisionID}, 0, err
}

func (h *Handler) deletePolicy(ctx context.Context, req *restroute.Request) (any, int, error) {
	return nil, statusAccepted, h.Backend.DeleteResourcePolicy(
		ctx,
		req.Params[keyWorkspaceID],
		req.Query.Get("revisionId"),
	)
}

func (h *Handler) tagOps(ops map[string]restroute.OpFunc) {
	ops["TagResource"] = h.tagResource
	ops["UntagResource"] = h.untagResource
	ops["ListTagsForResource"] = h.listTags
	ops["GetDefaultScraperConfiguration"] = h.defaultScraperConfig
}

func (h *Handler) tagResource(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		Tags map[string]string `json:"tags"`
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	return nil, 0, h.Backend.TagResource(ctx, req.Params[paramResource], in.Tags)
}

func (h *Handler) untagResource(ctx context.Context, req *restroute.Request) (any, int, error) {
	return nil, 0, h.Backend.UntagResource(ctx, req.Params[paramResource], req.Query["tagKeys"])
}

func (h *Handler) listTags(ctx context.Context, req *restroute.Request) (any, int, error) {
	t, err := h.Backend.ListTagsForResource(ctx, req.Params[paramResource])

	return map[string]any{keyTags: t}, 0, err
}

func (h *Handler) defaultScraperConfig(_ context.Context, _ *restroute.Request) (any, int, error) {
	return map[string]any{"configuration": []byte(defaultScraperConfig)}, 0, nil
}

func (h *Handler) namespaceOps(ops map[string]restroute.OpFunc) {
	ops["CreateRuleGroupsNamespace"] = h.createNamespace
	ops["PutRuleGroupsNamespace"] = h.putNamespace
	ops["DescribeRuleGroupsNamespace"] = h.describeNamespace
	ops["ListRuleGroupsNamespaces"] = h.listNamespaces
	ops["DeleteRuleGroupsNamespace"] = h.deleteNamespace
}

func nsOut(n wNamespace) map[string]any {
	return map[string]any{keyARN: n.Arn, "name": n.Name, keyStatus: n.Status, keyTags: n.Tags}
}

func (h *Handler) createNamespace(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		Tags        map[string]string `json:"tags"`
		Name        string            `json:"name"`
		ClientToken string            `json:"clientToken"`
		Data        []byte            `json:"data"`
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	n, err := h.Backend.CreateRuleGroupsNamespace(ctx, NamespaceInput{
		Tags:        in.Tags,
		WorkspaceID: req.Params[keyWorkspaceID],
		Name:        in.Name,
		ClientToken: in.ClientToken,
		Data:        in.Data,
	})
	if err != nil {
		return nil, 0, err
	}

	return nsOut(n), statusAccepted, nil
}

func (h *Handler) putNamespace(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in dataIn
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	n, err := h.Backend.PutRuleGroupsNamespace(ctx, NamespaceInput{
		WorkspaceID: req.Params[keyWorkspaceID], Name: req.Params["name"], ClientToken: in.ClientToken, Data: in.Data,
	})
	if err != nil {
		return nil, 0, err
	}

	return nsOut(n), statusAccepted, nil
}

func (h *Handler) describeNamespace(ctx context.Context, req *restroute.Request) (any, int, error) {
	n, err := h.Backend.DescribeRuleGroupsNamespace(ctx, req.Params[keyWorkspaceID], req.Params["name"])

	return map[string]any{"ruleGroupsNamespace": n}, 0, err
}

func (h *Handler) listNamespaces(ctx context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	pg, err := h.Backend.ListRuleGroupsNamespaces(
		ctx,
		req.Params[keyWorkspaceID],
		req.Query.Get("name"),
		q.token,
		q.max,
	)
	if err != nil {
		return nil, 0, err
	}

	return pageOut("ruleGroupsNamespaces", pg.Data, pg.Next), 0, nil
}

func (h *Handler) deleteNamespace(ctx context.Context, req *restroute.Request) (any, int, error) {
	return nil, statusAccepted, h.Backend.DeleteRuleGroupsNamespace(ctx, req.Params[keyWorkspaceID], req.Params["name"])
}
