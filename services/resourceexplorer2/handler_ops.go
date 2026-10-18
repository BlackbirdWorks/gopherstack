package resourceexplorer2

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	keyView    = "View"
	keyViewArn = "ViewArn"
)

type listIn struct {
	MaxResults *int32
	NextToken  string
}

func (l listIn) max() int {
	if l.MaxResults == nil {
		return 0
	}

	return int(*l.MaxResults)
}

func withNext(out map[string]any, next string) map[string]any {
	if next != "" {
		out["NextToken"] = next
	}

	return out
}

func listOut[T any](key string, pg page.Page[T]) map[string]any {
	items := pg.Data
	if items == nil {
		items = []T{}
	}

	return withNext(map[string]any{key: items}, pg.Next)
}

func (h *Handler) createIndex(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		Tags        map[string]string
		ClientToken string
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	out, err := h.Backend.CreateIndex(ctx, in.ClientToken, in.Tags)

	return out, 0, err
}

func (h *Handler) getIndex(ctx context.Context, _ *restroute.Request) (any, int, error) {
	out, err := h.Backend.GetIndex(ctx)

	return out, 0, err
}

func (h *Handler) updateIndexType(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct{ Arn, Type string }
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	out, err := h.Backend.UpdateIndexType(ctx, in.Arn, in.Type)

	return out, 0, err
}

func (h *Handler) deleteIndex(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct{ Arn string }
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	out, err := h.Backend.DeleteIndex(ctx, in.Arn)

	return out, 0, err
}

func (h *Handler) listIndexes(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		listIn
		Type    string
		Regions []string
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	pg, err := h.Backend.ListIndexes(ctx, in.Regions, in.Type, in.NextToken, in.max())
	if err != nil {
		return nil, 0, err
	}

	return listOut("Indexes", pg), 0, nil
}

func (h *Handler) listIndexesForMembers(_ context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		listIn
		AccountIDList []string `json:"AccountIdList"`
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	pg, err := h.Backend.ListIndexesForMembers(in.AccountIDList, in.NextToken, in.max())
	if err != nil {
		return nil, 0, err
	}

	return listOut("Indexes", pg), 0, nil
}

func (h *Handler) createView(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in CreateViewInput
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	v, err := h.Backend.CreateView(ctx, &in)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyView: v}, 0, nil
}

func (h *Handler) getView(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct{ ViewArn string }
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	v, tags, err := h.Backend.GetView(ctx, in.ViewArn)
	if err != nil {
		return nil, 0, err
	}

	out := map[string]any{keyView: v}
	if len(tags) > 0 {
		out["Tags"] = tags
	}

	return out, 0, nil
}

func (h *Handler) updateView(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		Filters            *SearchFilter
		IncludedProperties *[]IncludedProperty
		ViewArn            string
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	var props []IncludedProperty
	if in.IncludedProperties != nil {
		props = *in.IncludedProperties
	}

	v, err := h.Backend.UpdateView(ctx, in.ViewArn, in.Filters, props, in.IncludedProperties != nil)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyView: v}, 0, nil
}

func (h *Handler) deleteView(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct{ ViewArn string }
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	a, err := h.Backend.DeleteView(ctx, in.ViewArn)
	if err != nil {
		return nil, 0, err
	}

	return map[string]string{keyViewArn: a}, 0, nil
}

func (h *Handler) listViews(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in listIn
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	pg, err := h.Backend.ListViews(ctx, in.NextToken, in.max())
	if err != nil {
		return nil, 0, err
	}

	return listOut("Views", pg), 0, nil
}

func (h *Handler) batchGetView(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct{ ViewArns []string }
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	views, errs, err := h.Backend.BatchGetView(ctx, in.ViewArns)
	if err != nil {
		return nil, 0, err
	}

	if views == nil {
		views = []ViewWire{}
	}

	if errs == nil {
		errs = []ViewError{}
	}

	return map[string]any{"Views": views, "Errors": errs}, 0, nil
}

func (h *Handler) associateDefaultView(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct{ ViewArn string }
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	a, err := h.Backend.AssociateDefaultView(ctx, in.ViewArn)
	if err != nil {
		return nil, 0, err
	}

	return map[string]string{keyViewArn: a}, 0, nil
}

func (h *Handler) disassociateDefaultView(ctx context.Context, _ *restroute.Request) (any, int, error) {
	h.Backend.DisassociateDefaultView(ctx)

	return nil, 0, nil
}

func (h *Handler) getDefaultView(ctx context.Context, _ *restroute.Request) (any, int, error) {
	out := map[string]string{}
	if a := h.Backend.GetDefaultView(ctx); a != "" {
		out[keyViewArn] = a
	}

	return out, 0, nil
}

func searchOut(r *SearchResult, withCount bool) map[string]any {
	out := map[string]any{"Resources": r.Resources, keyViewArn: r.ViewARN}
	if withCount {
		out["Count"] = map[string]any{"Complete": r.Complete, "TotalResources": r.Total}
	}

	return withNext(out, r.Next)
}

func (h *Handler) search(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		listIn
		QueryString *string
		ViewArn     string
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	if in.QueryString == nil {
		return nil, 0, invalid("QueryString is required")
	}

	r, err := h.Backend.Search(ctx, *in.QueryString, in.ViewArn, in.NextToken, in.max())
	if err != nil {
		return nil, 0, err
	}

	return searchOut(r, true), 0, nil
}

func (h *Handler) listResources(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		listIn
		Filters *SearchFilter
		ViewArn string
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	r, err := h.Backend.ListResources(ctx, in.Filters, in.ViewArn, in.NextToken, in.max())
	if err != nil {
		return nil, 0, err
	}

	return searchOut(r, false), 0, nil
}

func (h *Handler) listSupportedTypes(_ context.Context, req *restroute.Request) (any, int, error) {
	var in listIn
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	pg, err := h.Backend.ListSupportedResourceTypes(in.NextToken, in.max())
	if err != nil {
		return nil, 0, err
	}

	return listOut("ResourceTypes", pg), 0, nil
}

func (h *Handler) listManagedViews(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		listIn
		ServicePrincipal string
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	pg, err := h.Backend.ListManagedViews(h.Backend.callerRegion(ctx), in.ServicePrincipal, in.NextToken, in.max())
	if err != nil {
		return nil, 0, err
	}

	return listOut("ManagedViews", pg), 0, nil
}

func (h *Handler) getManagedView(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct{ ManagedViewArn string }
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	v, err := h.Backend.GetManagedView(awsmeta.Account(ctx), h.Backend.callerRegion(ctx), in.ManagedViewArn)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{"ManagedView": v}, 0, nil
}

func (h *Handler) getAccountConfig(ctx context.Context, _ *restroute.Request) (any, int, error) {
	return map[string]any{"OrgConfiguration": h.Backend.GetAccountLevelServiceConfiguration(ctx)}, 0, nil
}

func (h *Handler) createSetup(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		ViewName          string
		RegionList        []string
		AggregatorRegions []string
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	id, err := h.Backend.CreateSetup(ctx, in.RegionList, in.ViewName, in.AggregatorRegions)
	if err != nil {
		return nil, 0, err
	}

	return map[string]string{"TaskId": id}, 0, nil
}

func (h *Handler) deleteSetup(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		RegionList         []string
		DeleteInAllRegions bool
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	id, err := h.Backend.DeleteSetup(ctx, in.RegionList, in.DeleteInAllRegions)
	if err != nil {
		return nil, 0, err
	}

	return map[string]string{"TaskId": id}, 0, nil
}

func (h *Handler) getSetup(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		listIn
		TaskID string `json:"TaskId"`
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	pg, err := h.Backend.GetSetup(ctx, in.TaskID, in.NextToken, in.max())
	if err != nil {
		return nil, 0, err
	}

	return listOut("Regions", pg), 0, nil
}

func (h *Handler) listTags(ctx context.Context, req *restroute.Request) (any, int, error) {
	t, err := h.Backend.ListTagsForResource(ctx, req.Params["resourceArn"])
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{"Tags": t}, 0, nil
}

func (h *Handler) tagResource(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct{ Tags map[string]string }
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	return nil, 0, h.Backend.TagResource(ctx, req.Params["resourceArn"], in.Tags)
}

func (h *Handler) untagResource(ctx context.Context, req *restroute.Request) (any, int, error) {
	return nil, 0, h.Backend.UntagResource(ctx, req.Params["resourceArn"], req.Query["tagKeys"])
}
