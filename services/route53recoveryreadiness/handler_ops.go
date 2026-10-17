package route53recoveryreadiness

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
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

func listOut[T any](key string, items []T, next string) map[string]any {
	if items == nil {
		items = []T{}
	}

	out := map[string]any{key: items}
	if next != "" {
		out["nextToken"] = next
	}

	return out
}

type namedBody struct {
	Tags               map[string]string `json:"tags"`
	CellName           string            `json:"cellName"`
	RecoveryGroupName  string            `json:"recoveryGroupName"`
	ResourceSetName    string            `json:"resourceSetName"`
	ResourceSetType    string            `json:"resourceSetType"`
	ReadinessCheckName string            `json:"readinessCheckName"`
	Cells              []string          `json:"cells"`
	Resources          []Resource        `json:"resources"`
}

func decodeBody(req *restroute.Request) (namedBody, error) {
	var in namedBody

	return in, req.Decode(&in)
}

func (h *Handler) createCell(_ context.Context, req *restroute.Request) (any, int, error) {
	in, err := decodeBody(req)
	if err != nil {
		return nil, 0, err
	}

	v, err := h.Backend.CreateCell(in.CellName, in.Cells, in.Tags)

	return v, 0, err
}

func (h *Handler) getCell(_ context.Context, req *restroute.Request) (any, int, error) {
	v, err := h.Backend.GetCell(req.Params["CellName"])

	return v, 0, err
}

func (h *Handler) listCells(_ context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	items, next, err := h.Backend.ListCells(q.token, q.max)

	return listOut("cells", items, next), 0, err
}

func (h *Handler) updateCell(_ context.Context, req *restroute.Request) (any, int, error) {
	in, err := decodeBody(req)
	if err != nil {
		return nil, 0, err
	}

	v, err := h.Backend.UpdateCell(req.Params["CellName"], in.Cells)

	return v, 0, err
}

func (h *Handler) deleteCell(_ context.Context, req *restroute.Request) (any, int, error) {
	return nil, 0, h.Backend.DeleteCell(req.Params["CellName"])
}

func (h *Handler) summaryOut(readiness string, checks []CheckSummary, next string) map[string]any {
	out := listOut("readinessChecks", checks, next)
	out["readiness"] = readiness

	return out
}

func (h *Handler) cellSummary(ctx context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	r, checks, next, err := h.Backend.CellReadinessSummary(ctx, req.Params["CellName"], q.token, q.max)
	if err != nil {
		return nil, 0, err
	}

	return h.summaryOut(r, checks, next), 0, nil
}

func (h *Handler) createGroup(_ context.Context, req *restroute.Request) (any, int, error) {
	in, err := decodeBody(req)
	if err != nil {
		return nil, 0, err
	}

	v, err := h.Backend.CreateRecoveryGroup(in.RecoveryGroupName, in.Cells, in.Tags)

	return v, 0, err
}

func (h *Handler) getGroup(_ context.Context, req *restroute.Request) (any, int, error) {
	v, err := h.Backend.GetRecoveryGroup(req.Params["RecoveryGroupName"])

	return v, 0, err
}

func (h *Handler) listGroups(_ context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	items, next, err := h.Backend.ListRecoveryGroups(q.token, q.max)

	return listOut("recoveryGroups", items, next), 0, err
}

func (h *Handler) updateGroup(_ context.Context, req *restroute.Request) (any, int, error) {
	in, err := decodeBody(req)
	if err != nil {
		return nil, 0, err
	}

	v, err := h.Backend.UpdateRecoveryGroup(req.Params["RecoveryGroupName"], in.Cells)

	return v, 0, err
}

func (h *Handler) deleteGroup(_ context.Context, req *restroute.Request) (any, int, error) {
	return nil, 0, h.Backend.DeleteRecoveryGroup(req.Params["RecoveryGroupName"])
}

func (h *Handler) groupSummary(ctx context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	r, checks, next, err := h.Backend.RecoveryGroupReadinessSummary(
		ctx,
		req.Params["RecoveryGroupName"],
		q.token,
		q.max,
	)
	if err != nil {
		return nil, 0, err
	}

	return h.summaryOut(r, checks, next), 0, nil
}

func (h *Handler) recommendations(_ context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	at, texts, next, err := h.Backend.ArchitectureRecommendations(req.Params["RecoveryGroupName"], q.token, q.max)
	if err != nil {
		return nil, 0, err
	}

	recs := make([]map[string]string, 0, len(texts))
	for _, t := range texts {
		recs = append(recs, map[string]string{"recommendationText": t})
	}

	out := listOut("recommendations", recs, next)
	out["lastAuditTimestamp"] = ts(at)

	return out, 0, nil
}

func (h *Handler) createSet(_ context.Context, req *restroute.Request) (any, int, error) {
	in, err := decodeBody(req)
	if err != nil {
		return nil, 0, err
	}

	v, err := h.Backend.CreateResourceSet(in.ResourceSetName, in.ResourceSetType, in.Resources, in.Tags)

	return v, 0, err
}

func (h *Handler) getSet(_ context.Context, req *restroute.Request) (any, int, error) {
	v, err := h.Backend.GetResourceSet(req.Params["ResourceSetName"])

	return v, 0, err
}

func (h *Handler) listSets(_ context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	items, next, err := h.Backend.ListResourceSets(q.token, q.max)

	return listOut("resourceSets", items, next), 0, err
}

func (h *Handler) updateSet(_ context.Context, req *restroute.Request) (any, int, error) {
	in, err := decodeBody(req)
	if err != nil {
		return nil, 0, err
	}

	v, err := h.Backend.UpdateResourceSet(req.Params["ResourceSetName"], in.ResourceSetType, in.Resources)

	return v, 0, err
}

func (h *Handler) deleteSet(_ context.Context, req *restroute.Request) (any, int, error) {
	return nil, 0, h.Backend.DeleteResourceSet(req.Params["ResourceSetName"])
}

func (h *Handler) createCheck(_ context.Context, req *restroute.Request) (any, int, error) {
	in, err := decodeBody(req)
	if err != nil {
		return nil, 0, err
	}

	v, err := h.Backend.CreateReadinessCheck(in.ReadinessCheckName, in.ResourceSetName, in.Tags)

	return v, 0, err
}

func (h *Handler) getCheck(_ context.Context, req *restroute.Request) (any, int, error) {
	v, err := h.Backend.GetReadinessCheck(req.Params["ReadinessCheckName"])

	return v, 0, err
}

func (h *Handler) listChecks(_ context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	items, next, err := h.Backend.ListReadinessChecks(q.token, q.max)

	return listOut("readinessChecks", items, next), 0, err
}

func (h *Handler) updateCheck(_ context.Context, req *restroute.Request) (any, int, error) {
	in, err := decodeBody(req)
	if err != nil {
		return nil, 0, err
	}

	v, err := h.Backend.UpdateReadinessCheck(req.Params["ReadinessCheckName"], in.ResourceSetName)

	return v, 0, err
}

func (h *Handler) deleteCheck(_ context.Context, req *restroute.Request) (any, int, error) {
	return nil, 0, h.Backend.DeleteReadinessCheck(req.Params["ReadinessCheckName"])
}

func (h *Handler) checkStatus(ctx context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	r, msgs, res, next, err := h.Backend.ReadinessCheckStatus(ctx, req.Params["ReadinessCheckName"], q.token, q.max)
	if err != nil {
		return nil, 0, err
	}

	out := listOut("resources", res, next)
	out["readiness"] = r

	if len(msgs) > 0 {
		out["messages"] = msgs
	}

	return out, 0, nil
}

func (h *Handler) resourceStatus(ctx context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	r, rules, next, err := h.Backend.ReadinessCheckResourceStatus(
		ctx, req.Params["ReadinessCheckName"], req.Params["ResourceIdentifier"], q.token, q.max)
	if err != nil {
		return nil, 0, err
	}

	out := listOut("rules", rules, next)
	out["readiness"] = r

	return out, 0, nil
}

func (h *Handler) createAuth(_ context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		CrossAccountAuthorization string `json:"crossAccountAuthorization"`
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	err := h.Backend.CreateCrossAccountAuthorization(in.CrossAccountAuthorization)

	return map[string]string{"crossAccountAuthorization": in.CrossAccountAuthorization}, 0, err
}

func (h *Handler) listAuths(_ context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	items, next, err := h.Backend.ListCrossAccountAuthorizations(q.token, q.max)

	return listOut("crossAccountAuthorizations", items, next), 0, err
}

func (h *Handler) deleteAuth(_ context.Context, req *restroute.Request) (any, int, error) {
	return nil, 0, h.Backend.DeleteCrossAccountAuthorization(req.Params["CrossAccountAuthorization"])
}

func (h *Handler) listRules(_ context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	items, next, err := h.Backend.ListRules(req.Query.Get("resourceType"), q.token, q.max)

	return listOut("rules", items, next), 0, err
}

func (h *Handler) listTags(_ context.Context, req *restroute.Request) (any, int, error) {
	t, err := h.Backend.ListTagsForResource(req.Params["ResourceArn"])
	if t == nil {
		t = map[string]string{}
	}

	return map[string]any{"tags": t}, 0, err
}

func (h *Handler) tagResource(_ context.Context, req *restroute.Request) (any, int, error) {
	in, err := decodeBody(req)
	if err != nil {
		return nil, 0, err
	}

	if len(in.Tags) == 0 {
		return nil, 0, restroute.Validation("tags is required")
	}

	return nil, 0, h.Backend.TagResource(req.Params["ResourceArn"], in.Tags)
}

func (h *Handler) untagResource(_ context.Context, req *restroute.Request) (any, int, error) {
	keys := req.Query["tagKeys"]
	if len(keys) == 0 {
		return nil, 0, restroute.Validation("tagKeys is required")
	}

	return nil, 0, h.Backend.UntagResource(req.Params["ResourceArn"], keys)
}
