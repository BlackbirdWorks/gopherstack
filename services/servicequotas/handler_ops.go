package servicequotas

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func mapPage[T, W any](pg page.Page[T], conv func(T) W) []W {
	out := make([]W, len(pg.Data))
	for i, v := range pg.Data {
		out[i] = conv(v)
	}

	return out
}

func registerQuotaOps(ops map[string]opFunc) {
	ops["ListServices"] = bind((*Handler).listServices)
	ops["ListServiceQuotas"] = bind((*Handler).listServiceQuotas)
	ops["ListAWSDefaultServiceQuotas"] = bind((*Handler).listDefaultQuotas)
	ops["GetServiceQuota"] = bind((*Handler).getServiceQuota)
	ops["GetAWSDefaultServiceQuota"] = bind((*Handler).getDefaultQuota)
}

func (h *Handler) listServices(_ context.Context, in *listServicesReq) (*listServicesResp, error) {
	pg, err := h.Backend.ListServices(in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listServicesResp{
		NextToken: pg.Next,
		Services: mapPage(pg, func(s ServiceDef) serviceInfoWire {
			return serviceInfoWire{ServiceCode: s.Code, ServiceName: s.Name}
		}),
	}, nil
}

func (h *Handler) listServiceQuotas(ctx context.Context, in *listQuotasReq) (*listQuotasResp, error) {
	pg, err := h.Backend.ListServiceQuotas(
		ctx,
		in.ServiceCode,
		in.QuotaCode,
		in.QuotaAppliedAtLevel,
		in.NextToken,
		in.MaxResults,
	)
	if err != nil {
		return nil, err
	}

	return &listQuotasResp{
		NextToken: pg.Next,
		Quotas:    mapPage(pg, func(v QuotaView) quotaWire { return quotaToWire(v, true) }),
	}, nil
}

func (h *Handler) listDefaultQuotas(ctx context.Context, in *listQuotasReq) (*listQuotasResp, error) {
	pg, err := h.Backend.ListDefaultQuotas(ctx, in.ServiceCode, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listQuotasResp{
		NextToken: pg.Next,
		Quotas:    mapPage(pg, func(v QuotaView) quotaWire { return quotaToWire(v, false) }),
	}, nil
}

func (h *Handler) getServiceQuota(ctx context.Context, in *quotaCodesReq) (*quotaResp, error) {
	v, err := h.Backend.GetServiceQuota(ctx, in.ServiceCode, in.QuotaCode, in.ContextID)
	if err != nil {
		return nil, err
	}

	return &quotaResp{Quota: quotaToWire(v, true)}, nil
}

func (h *Handler) getDefaultQuota(ctx context.Context, in *quotaCodesReq) (*quotaResp, error) {
	v, err := h.Backend.GetDefaultQuota(ctx, in.ServiceCode, in.QuotaCode)
	if err != nil {
		return nil, err
	}

	return &quotaResp{Quota: quotaToWire(v, false)}, nil
}

func registerRequestOps(ops map[string]opFunc) {
	ops["RequestServiceQuotaIncrease"] = bind((*Handler).requestIncrease)
	ops["CreateSupportCase"] = bind((*Handler).createSupportCase)
	ops["GetRequestedServiceQuotaChange"] = bind((*Handler).getRequest)
	ops["ListRequestedServiceQuotaChangeHistory"] = bind((*Handler).listHistory)
	ops["ListRequestedServiceQuotaChangeHistoryByQuota"] = bind((*Handler).listHistoryByQuota)
}

func (h *Handler) requestIncrease(ctx context.Context, in *requestIncreaseReq) (*requestResp, error) {
	v, err := h.Backend.RequestIncrease(
		ctx,
		in.ServiceCode,
		in.QuotaCode,
		in.DesiredValue,
		in.ContextID,
		in.SupportCaseAllowed,
	)
	if err != nil {
		return nil, err
	}

	return &requestResp{RequestedQuota: requestToWire(v)}, nil
}

func (h *Handler) createSupportCase(ctx context.Context, in *requestIDReq) (*emptyResp, error) {
	if in.RequestID == "" {
		return nil, fail(ErrIllegalArgument, "RequestId is required")
	}

	return &emptyResp{}, h.Backend.CreateSupportCase(ctx, in.RequestID)
}

func (h *Handler) getRequest(ctx context.Context, in *requestIDReq) (*requestResp, error) {
	v, err := h.Backend.GetRequest(ctx, in.RequestID)
	if err != nil {
		return nil, err
	}

	return &requestResp{RequestedQuota: requestToWire(v)}, nil
}

func (h *Handler) listHistoryOf(ctx context.Context, f RequestFilter, pr pageReq) (*listRequestsResp, error) {
	pg, err := h.Backend.ListRequests(ctx, f, pr.NextToken, pr.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listRequestsResp{NextToken: pg.Next, RequestedQuotas: mapPage(pg, requestToWire)}, nil
}

func (h *Handler) listHistory(ctx context.Context, in *listHistoryReq) (*listRequestsResp, error) {
	return h.listHistoryOf(ctx, RequestFilter{
		ServiceCode: in.ServiceCode, Status: in.Status, Level: in.QuotaRequestedAtLevel,
	}, in.pageReq)
}

func (h *Handler) listHistoryByQuota(ctx context.Context, in *listHistoryByQuotaReq) (*listRequestsResp, error) {
	if in.ServiceCode == "" || in.QuotaCode == "" {
		return nil, fail(ErrIllegalArgument, "ServiceCode and QuotaCode are required")
	}

	return h.listHistoryOf(ctx, RequestFilter{
		ServiceCode: in.ServiceCode, QuotaCode: in.QuotaCode, Status: in.Status, Level: in.QuotaRequestedAtLevel,
	}, in.pageReq)
}

func registerTemplateOps(ops map[string]opFunc) {
	ops["PutServiceQuotaIncreaseRequestIntoTemplate"] = bind((*Handler).putTemplateItem)
	ops["GetServiceQuotaIncreaseRequestFromTemplate"] = bind((*Handler).getTemplateItem)
	ops["DeleteServiceQuotaIncreaseRequestFromTemplate"] = bind((*Handler).deleteTemplateItem)
	ops["ListServiceQuotaIncreaseRequestsInTemplate"] = bind((*Handler).listTemplateItems)
	ops["AssociateServiceQuotaTemplate"] = bind((*Handler).associateTemplate)
	ops["DisassociateServiceQuotaTemplate"] = bind((*Handler).disassociateTemplate)
	ops["GetAssociationForServiceQuotaTemplate"] = bind((*Handler).getAssociation)
}

func (h *Handler) putTemplateItem(ctx context.Context, in *putTemplateReq) (*templateItemResp, error) {
	v, err := h.Backend.PutTemplateItem(ctx, in.AwsRegion, in.ServiceCode, in.QuotaCode, in.DesiredValue)
	if err != nil {
		return nil, err
	}

	return &templateItemResp{Item: templateToWire(v)}, nil
}

func (h *Handler) getTemplateItem(ctx context.Context, in *templateKeyReq) (*templateItemResp, error) {
	v, err := h.Backend.GetTemplateItem(ctx, in.AwsRegion, in.ServiceCode, in.QuotaCode)
	if err != nil {
		return nil, err
	}

	return &templateItemResp{Item: templateToWire(v)}, nil
}

func (h *Handler) deleteTemplateItem(ctx context.Context, in *templateKeyReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.DeleteTemplateItem(ctx, in.AwsRegion, in.ServiceCode, in.QuotaCode)
}

func (h *Handler) listTemplateItems(ctx context.Context, in *listTemplateReq) (*listTemplateResp, error) {
	pg, err := h.Backend.ListTemplateItems(ctx, in.AwsRegion, in.ServiceCode, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listTemplateResp{NextToken: pg.Next, Items: mapPage(pg, templateToWire)}, nil
}

func (h *Handler) associateTemplate(ctx context.Context, _ *emptyResp) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.AssociateTemplate(ctx)
}

func (h *Handler) disassociateTemplate(ctx context.Context, _ *emptyResp) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.DisassociateTemplate(ctx)
}

func (h *Handler) getAssociation(ctx context.Context, _ *emptyResp) (*associationResp, error) {
	st, err := h.Backend.TemplateAssociation(ctx)
	if err != nil {
		return nil, err
	}

	return &associationResp{ServiceQuotaTemplateAssociationStatus: st}, nil
}

func registerAutoManagementOps(ops map[string]opFunc) {
	ops["StartAutoManagement"] = bind((*Handler).startAutoManagement)
	ops["StopAutoManagement"] = bind((*Handler).stopAutoManagement)
	ops["UpdateAutoManagement"] = bind((*Handler).updateAutoManagement)
	ops["GetAutoManagementConfiguration"] = bind((*Handler).getAutoManagement)
}

func (h *Handler) startAutoManagement(ctx context.Context, in *autoStartReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.StartAutoManagement(ctx, AutoManagementInput{
		OptInLevel: in.OptInLevel, OptInType: in.OptInType, Exclusions: in.ExclusionList,
		NotificationArn: in.NotificationArn,
	})
}

func (h *Handler) stopAutoManagement(ctx context.Context, _ *emptyResp) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.StopAutoManagement(ctx)
}

func (h *Handler) updateAutoManagement(ctx context.Context, in *autoUpdateReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.UpdateAutoManagement(ctx, AutoManagementInput{
		OptInType: in.OptInType, Exclusions: in.ExclusionList, NotificationArn: in.NotificationArn,
	})
}

func (h *Handler) getAutoManagement(ctx context.Context, _ *emptyResp) (*autoGetResp, error) {
	v := h.Backend.GetAutoManagement(ctx)
	resp := &autoGetResp{
		OptInStatus: v.Config.Status, OptInLevel: v.Config.OptInLevel, OptInType: v.Config.OptInType,
		NotificationArn: v.Config.NotificationArn,
	}

	if len(v.Exclusions) > 0 {
		resp.ExclusionList = make(map[string][]quotaInfoWire, len(v.Exclusions))

		for svcCode, entries := range v.Exclusions {
			for _, e := range entries {
				resp.ExclusionList[svcCode] = append(
					resp.ExclusionList[svcCode],
					quotaInfoWire{QuotaCode: e.Code, QuotaName: e.Name},
				)
			}
		}
	}

	return resp, nil
}

func registerReportOps(ops map[string]opFunc) {
	ops["StartQuotaUtilizationReport"] = bind((*Handler).startReport)
	ops["GetQuotaUtilizationReport"] = bind((*Handler).getReport)
}

func (h *Handler) startReport(ctx context.Context, _ *emptyResp) (*startReportResp, error) {
	r, err := h.Backend.StartReport(ctx)
	if err != nil {
		return nil, err
	}

	return &startReportResp{
		Message: "Report generation started", ReportID: r.ID, Status: r.Status,
	}, nil
}

func (h *Handler) getReport(ctx context.Context, in *getReportReq) (*getReportResp, error) {
	rp, err := h.Backend.GetReport(ctx, in.ReportID, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	resp := &getReportResp{ReportID: rp.Report.ID, Status: rp.Report.Status}

	if rp.Report.Status != reportCompleted {
		return resp, nil
	}

	gen := awstime.Epoch(rp.Report.Generated)
	total := int32(rp.Total) //nolint:gosec // bounded by catalog size
	resp.GeneratedAt, resp.TotalCount, resp.NextToken = &gen, &total, rp.Quotas.Next
	resp.Quotas = h.reportQuotas(rp.Quotas.Data)

	return resp, nil
}

func (h *Handler) reportQuotas(entries []UtilizationEntry) []utilizationWire {
	out := make([]utilizationWire, len(entries))

	for i, e := range entries {
		svc, def, _ := h.Backend.catalog.Quota(e.ServiceCode, e.QuotaCode)
		out[i] = utilizationWire{
			AppliedValue: e.Applied, DefaultValue: e.Default, Namespace: e.Namespace,
			QuotaCode: e.QuotaCode, QuotaName: def.Name, ServiceCode: e.ServiceCode, ServiceName: svc.Name,
			Utilization: e.Utilization, Adjustable: e.Adjustable,
		}
	}

	return out
}

func registerTagOps(ops map[string]opFunc) {
	ops["TagResource"] = bind((*Handler).tagResource)
	ops["UntagResource"] = bind((*Handler).untagResource)
	ops["ListTagsForResource"] = bind((*Handler).listTagsForResource)
}

func (h *Handler) tagResource(ctx context.Context, in *tagReq) (*emptyResp, error) {
	m, err := validateTags(in.Tags)
	if err != nil {
		return nil, err
	}

	return &emptyResp{}, h.Backend.TagResource(ctx, in.ResourceARN, m)
}

func (h *Handler) untagResource(ctx context.Context, in *tagReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.UntagResource(ctx, in.ResourceARN, in.TagKeys)
}

func (h *Handler) listTagsForResource(ctx context.Context, in *tagReq) (*listTagsResp, error) {
	m, err := h.Backend.ListTagsForResource(ctx, in.ResourceARN)
	if err != nil {
		return nil, err
	}

	return &listTagsResp{Tags: tags.MapToKV(m)}, nil
}
